from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, List, Optional, Tuple, Final, Dict

import pandas as pd
import requests
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# =========================================================
# 1. CONSTANTS & SYSTEM CONFIGURATION
# =========================================================

class AppConfig:
    """不変の設定値およびシステム定数"""
    TITLE: Final = "APR資産運用管理システム Pro"
    ICON: Final = "🏦"
    JST: Final = timezone(timedelta(hours=9), "JST")
    
    # 計算係数
    RANK_MASTER: Final = "Master"
    RANK_ELITE: Final = "Elite"
    FACTORS: Final = {RANK_MASTER: 0.67, RANK_ELITE: 0.60}
    
    # ステータス・プロジェクト
    PROJECT_PERSONAL: Final = "PERSONAL"
    STATUS_ON: Final = "🟢運用中"
    STATUS_OFF: Final = "🔴停止"
    
    # 複利タイミング
    COMPOUND_DAILY: Final = "daily"
    COMPOUND_MONTHLY: Final = "monthly"
    COMPOUND_NONE: Final = "none"

    # 種別定義
    TYPE_APR: Final = "APR"
    TYPE_LINE: Final = "LINE"
    TYPE_DEPOSIT: Final = "Deposit"
    TYPE_WITHDRAW: Final = "Withdraw"

    # シートヘッダー定義
    HEADERS: Final = {
        "Settings": ["Project_Name", "Net_Factor", "IsCompound", "Compound_Timing", "UpdatedAt_JST", "Active"],
        "Members": ["Project_Name", "PersonName", "Principal", "Line_User_ID", "LINE_DisplayName", "Rank", "IsActive", "CreatedAt_JST", "UpdatedAt_JST"],
        "Ledger": ["Datetime_JST", "Project_Name", "PersonName", "Type", "Amount", "Note", "Evidence_URL", "Line_User_ID", "LINE_DisplayName", "Source"],
        "LineUsers": ["Date", "Time", "Type", "Line_User_ID", "Line_User"],
        "APR_Summary": ["Date_JST", "PersonName", "Total_APR", "APR_Count", "Asset_Ratio", "LINE_DisplayName"]
    }

# =========================================================
# 2. BUSINESS LOGIC ENGINE
# =========================================================

class LogicEngine:
    """計算およびデータ変換のコアロジック"""
    @staticmethod
    def now_jst() -> datetime:
        return datetime.now(AppConfig.JST)

    @staticmethod
    def to_float(v: Any) -> float:
        try:
            s = str(v).replace(",", "").replace("$", "").replace("%", "").strip()
            return float(s) if s else 0.0
        except (ValueError, TypeError): return 0.0

    @staticmethod
    def truthy(v: Any) -> bool:
        if isinstance(v, bool): return v
        return str(v).strip().lower() in ("1", "true", "yes", "はい", "on", "t")

    @staticmethod
    def fmt_usd(x: float) -> str:
        return f"${x:,.2f}"

    @classmethod
    def calculate_apr_distribution(cls, members: pd.DataFrame, apr_rate: float, net_factor: float, project: str) -> pd.DataFrame:
        df = members.copy()
        if project.upper() == AppConfig.PROJECT_PERSONAL:
            df["Factor"] = df["Rank"].apply(lambda r: AppConfig.FACTORS.get(r, AppConfig.FACTORS[AppConfig.RANK_MASTER]))
            df["DailyAPR"] = (df["Principal"] * (apr_rate / 100.0) * df["Factor"]) / 365.0
            df["CalcMode"] = "PERSONAL"
        else:
            total_principal = df["Principal"].sum()
            total_reward = (total_principal * (apr_rate / 100.0) * net_factor) / 365.0
            df["DailyAPR"] = (total_reward / len(df)) if not df.empty else 0.0
            df["Factor"] = net_factor
            df["CalcMode"] = "GROUP_EQUAL"
        return df

# =========================================================
# 3. EXTERNAL INFRASTRUCTURE SERVICES
# =========================================================

class GSheetService:
    def __init__(self, spreadsheet_id: str, namespace: str):
        self.sid = spreadsheet_id
        self.namespace = namespace
        self.gc = self._authorize()
        self.book = self.gc.open_by_key(self.sid)
        self._ensure_sheets()

    def _authorize(self):
        creds = st.secrets["connections"]["gsheets"]["credentials"]
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        return gspread.authorize(Credentials.from_service_account_info(dict(creds), scopes=scopes))

    def _get_sheet_name(self, base: str) -> str:
        return f"{base}__{self.namespace}" if self.namespace != "default" else base

    def _ensure_sheets(self):
        for base, head in AppConfig.HEADERS.items():
            name = self._get_sheet_name(base)
            try: self.book.worksheet(name)
            except gspread.WorksheetNotFound:
                ws = self.book.add_worksheet(title=name, rows=3000, cols=20)
                ws.append_row(head, value_input_option="USER_ENTERED")

    def read_df(self, base: str) -> pd.DataFrame:
        name = self._get_sheet_name(base)
        data = self.book.worksheet(name).get_all_values()
        if not data: return pd.DataFrame(columns=AppConfig.HEADERS[base])
        df = pd.DataFrame(data[1:], columns=data[0])
        return df

    def write_df(self, base: str, df: pd.DataFrame):
        ws = self.book.worksheet(self._get_sheet_name(base))
        ws.clear()
        ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist(), value_input_option="USER_ENTERED")

    def append_row(self, base: str, row: list):
        self.book.worksheet(self._get_sheet_name(base)).append_row(row, value_input_option="USER_ENTERED")

class ExternalAPIs:
    @staticmethod
    def send_line(token: str, user_id: str, text: str, img_url: Optional[str] = None) -> int:
        if not user_id or not token: return 400
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        messages = [{"type": "text", "text": text}]
        if img_url: messages.append({"type": "image", "originalContentUrl": img_url, "previewImageUrl": img_url})
        try:
            r = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json={"to": user_id, "messages": messages}, timeout=20)
            return r.status_code
        except: return 500

# =========================================================
# 4. UI COMPONENTS
# =========================================================

def handle_auth():
    if st.session_state.get("admin_ok"): return
    st.title("🔐 管理者認証")
    admins = st.secrets.get("admin", {}).get("users", [])
    with st.form("login_form"):
        name = st.selectbox("管理者名", [u["name"] for u in admins]) if admins else st.text_input("管理者名")
        pin = st.text_input("PINコード", type="password")
        if st.form_submit_button("ログイン"):
            user = next((u for u in admins if u["name"] == name), None)
            if user and str(user["pin"]) == pin:
                st.session_state.admin_ok, st.session_state.admin_name = True, name
                st.session_state.admin_namespace = user.get("namespace", "default")
                st.rerun()
            else: st.error("認証失敗")
    st.stop()

def render_cash_flow(gs: GSheetService):
    """入出金管理画面"""
    st.subheader("💸 入出金管理")
    members = gs.read_df("Members")
    
    with st.form("cash_form"):
        col1, col2 = st.columns(2)
        person = col1.selectbox("対象メンバー", members["PersonName"].tolist())
        m_info = members[members["PersonName"] == person].iloc[0]
        
        type_act = col2.radio("種別", [AppConfig.TYPE_DEPOSIT, AppConfig.TYPE_WITHDRAW], horizontal=True)
        amount = st.number_input("金額 (USD)", min_value=0.0, step=100.0)
        note = st.text_input("メモ")
        
        if st.form_submit_button("記録を実行"):
            # 元本の更新
            current_p = LogicEngine.to_float(m_info["Principal"])
            new_p = current_p + amount if type_act == AppConfig.TYPE_DEPOSIT else current_p - amount
            
            # Members更新
            members.loc[members["PersonName"] == person, "Principal"] = new_p
            members.loc[members["PersonName"] == person, "UpdatedAt_JST"] = LogicEngine.now_jst().strftime("%Y-%m-%d %H:%M:%S")
            gs.write_df("Members", members)
            
            # Ledger追記
            gs.append_row("Ledger", [
                LogicEngine.now_jst().strftime("%Y-%m-%d %H:%M:%S"), m_info["Project_Name"],
                person, type_act, amount, note, "", m_info["Line_User_ID"], m_info["LINE_DisplayName"], "app"
            ])
            st.success(f"{person} 様の元本を {LogicEngine.fmt_usd(new_p)} に更新しました。")

def render_member_mgmt(gs: GSheetService):
    """メンバー管理（一括編集）"""
    st.subheader("⚙️ メンバー・設定管理")
    tab1, tab2 = st.tabs(["👥 メンバーリスト", "🛠 プロジェクト設定"])
    
    with tab1:
        members = gs.read_df("Members")
        st.info("表内の値を直接編集して「保存」を押してください。")
        edited_df = st.data_editor(members, use_container_width=True, hide_index=True)
        if st.button("メンバー情報を保存"):
            gs.write_df("Members", edited_df)
            st.success("保存完了")

    with tab2:
        settings = gs.read_df("Settings")
        edited_settings = st.data_editor(settings, use_container_width=True, hide_index=True)
        if st.button("設定を保存"):
            gs.write_df("Settings", edited_settings)
            st.success("保存完了")

# =========================================================
# 5. MAIN ENTRY
# =========================================================

def main():
    st.set_page_config(page_title=AppConfig.TITLE, page_icon=AppConfig.ICON, layout="wide")
    handle_auth()
    
    ss_url = st.secrets["connections"]["gsheets"]["spreadsheet"]
    ss_id = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", ss_url).group(1) if "/spreadsheets/d/" in ss_url else ss_url
    gs = GSheetService(ss_id, st.session_state.admin_namespace)

    with st.sidebar:
        st.title(AppConfig.ICON)
        page = st.radio("メニュー", ["📊 ダッシュボード", "📈 APR確定", "💸 入出金管理", "⚙️ メンバー管理"])
        if st.button("🚪 ログアウト"):
            st.session_state.clear()
            st.rerun()

    if page == "📊 ダッシュボード":
        from __main__ import render_dashboard
        render_dashboard(gs)
    elif page == "📈 APR確定":
        from __main__ import render_apr_flow
        render_apr_flow(gs)
    elif page == "💸 入出金管理":
        render_cash_flow(gs)
    elif page == "⚙️ メンバー管理":
        render_member_mgmt(gs)

def render_dashboard(gs):
    st.subheader("📊 ダッシュボード")
    m = gs.read_df("Members")
    m["Principal"] = m["Principal"].apply(LogicEngine.to_float)
    active = m[m["IsActive"].apply(LogicEngine.truthy)]
    c1, c2 = st.columns(2)
    c1.metric("総運用資産", LogicEngine.fmt_usd(active["Principal"].sum()))
    c2.metric("稼働人数", f"{len(active)}名")
    st.divider()
    st.dataframe(active[["Project_Name", "PersonName", "Principal", "Rank"]], use_container_width=True, hide_index=True)

def render_apr_flow(gs):
    st.subheader("📈 APR報酬確定")
    sets = gs.read_df("Settings")
    projs = sets[sets["Active"].apply(LogicEngine.truthy)]["Project_Name"].tolist()
    project = st.selectbox("プロジェクト", projs)
    apr_input = st.number_input("APR (%)", value=0.0, step=0.01, format="%.4f")
    
    if st.button("🚀 実行"):
        m = gs.read_df("Members")
        targets = m[(m["Project_Name"] == project) & (m["IsActive"].apply(LogicEngine.truthy))]
        net_f = LogicEngine.to_float(sets[sets["Project_Name"] == project]["Net_Factor"].iloc[0])
        results = LogicEngine.calculate_apr_distribution(targets, apr_input, net_f, project)
        
        token = st.secrets["line"]["tokens"].get(st.session_state.admin_namespace)
        ts = LogicEngine.now_jst().strftime("%Y-%m-%d %H:%M:%S")
        
        for _, r in results.iterrows():
            gs.append_row("Ledger", [ts, project, r["PersonName"], AppConfig.TYPE_APR, r["DailyAPR"], f"APR:{apr_input}%", "", r["Line_User_ID"], r["LINE_DisplayName"], "app"])
            msg = f"🏦【収益報告】\n{r['PersonName']}様\n本日APR: {apr_input}%\n配当額: {LogicEngine.fmt_usd(r['DailyAPR'])}"
            ExternalAPIs.send_line(token, r["Line_User_ID"], msg)
        st.success("完了")

if __name__ == "__main__":
    main()
