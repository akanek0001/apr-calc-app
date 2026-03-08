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
        """プロジェクト毎の配当計算ロジック"""
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

    @staticmethod
    def extract_ocr_percents(text: str) -> List[float]:
        """OCRテキストから%の数値を抽出"""
        pattern = r"(?i)(?:APR\s*)?(\d+(?:\.\d+)?)\s*%"
        matches = re.findall(pattern, text)
        return sorted(list(set(float(m) for m in matches)), reverse=True)

# =========================================================
# 3. EXTERNAL INFRASTRUCTURE SERVICES
# =========================================================

class GSheetService:
    """Google Sheets 通信および自動初期化"""
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
        df.columns = df.columns.astype(str).str.strip()
        return df

    def write_df(self, base: str, df: pd.DataFrame):
        ws = self.book.worksheet(self._get_sheet_name(base))
        ws.clear()
        ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist(), value_input_option="USER_ENTERED")

    def append_row(self, base: str, row: list):
        self.book.worksheet(self._get_sheet_name(base)).append_row(row, value_input_option="USER_ENTERED")

class ExternalAPIs:
    """LINE / OCR / ImgBB 外部通信"""
    @staticmethod
    def send_line(token: str, user_id: str, text: str, img_url: Optional[str] = None) -> int:
        if not user_id: return 400
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        messages = [{"type": "text", "text": text}]
        if img_url: messages.append({"type": "image", "originalContentUrl": img_url, "previewImageUrl": img_url})
        payload = {"to": user_id, "messages": messages}
        try:
            r = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json=payload, timeout=20)
            return r.status_code
        except: return 500

    @staticmethod
    def upload_imgbb(file_bytes: bytes) -> Optional[str]:
        api_key = st.secrets.get("imgbb", {}).get("api_key")
        if not api_key: return None
        try:
            r = requests.post("https://api.imgbb.com/1/upload", params={"key": api_key}, files={"image": file_bytes}, timeout=30)
            return r.json().get("data", {}).get("url")
        except: return None

# =========================================================
# 4. STREAMLIT UI COMPONENTS
# =========================================================

def handle_auth():
    """認証およびセッション管理"""
    if st.session_state.get("admin_ok"): return
    st.title("🔐 管理者認証")
    admins = st.secrets.get("admin", {}).get("users", [])
    with st.form("login_form"):
        name = st.selectbox("管理者名", [u["name"] for u in admins]) if admins else st.text_input("管理者名")
        pin = st.text_input("PINコード", type="password")
        if st.form_submit_button("ログイン"):
            user = next((u for u in admins if u["name"] == name), None)
            if user and str(user["pin"]) == pin:
                st.session_state.admin_ok = True
                st.session_state.admin_name = name
                st.session_state.admin_namespace = user.get("namespace", "default")
                st.rerun()
            else: st.error("認証失敗")
    st.stop()

def render_dashboard(gs: GSheetService):
    st.subheader("📊 ダッシュボード")
    members = gs.read_df("Members")
    members["Principal"] = members["Principal"].apply(LogicEngine.to_float)
    active = members[members["IsActive"].apply(LogicEngine.truthy)]
    
    c1, c2, c3 = st.columns(3)
    c1.metric("総運用資産", LogicEngine.fmt_usd(active["Principal"].sum()))
    c2.metric("稼働人数", f"{len(active)}名")
    
    st.divider()
    st.markdown("#### プロジェクト別資産")
    st.bar_chart(active.groupby("Project_Name")["Principal"].sum())
    st.dataframe(active[["PersonName", "Project_Name", "Principal", "Rank"]], use_container_width=True, hide_index=True)

def render_apr_flow(gs: GSheetService):
    st.subheader("📈 APR報酬の確定とLINE送信")
    settings = gs.read_df("Settings")
    active_projects = settings[settings["Active"].apply(LogicEngine.truthy)]["Project_Name"].tolist()
    
    project = st.selectbox("プロジェクト選択", active_projects)
    apr_input = st.number_input("本日のAPR (%)", value=0.0, step=0.01, format="%.4f")
    
    uploaded = st.file_uploader("エビデンス画像", type=["jpg", "png"])
    
    if st.button("🚀 報酬確定と一斉送信を実行"):
        with st.spinner("処理中..."):
            # 1. データロード
            members = gs.read_df("Members")
            target_members = members[(members["Project_Name"] == project) & (members["IsActive"].apply(LogicEngine.truthy))]
            
            # 2. 配当計算
            net_f = LogicEngine.to_float(settings[settings["Project_Name"] == project]["Net_Factor"].iloc[0])
            calc_results = LogicEngine.calculate_apr_distribution(target_members, apr_input, net_f, project)
            
            # 3. 画像アップロード
            img_url = ExternalAPIs.upload_imgbb(uploaded.getvalue()) if uploaded else None
            
            # 4. 記録とLINE送信
            token = st.secrets["line"]["tokens"].get(st.session_state.admin_namespace)
            ts = LogicEngine.now_jst().strftime("%Y-%m-%d %H:%M:%S")
            
            for _, r in calc_results.iterrows():
                # 元本更新(Daily複利の場合のみ)
                # (ここに入力に基づく元本更新ロジックを配置可能)
                
                # Ledger記録
                gs.append_row("Ledger", [ts, project, r["PersonName"], AppConfig.TYPE_APR, r["DailyAPR"], f"APR:{apr_input}%", img_url or "", r["Line_User_ID"], r["LINE_DisplayName"], "app"])
                
                # LINE送信
                msg = f"🏦【APR収益報告】\n{r['PersonName']} 様\nプロジェクト: {project}\n本日APR: {apr_input}%\n配当額: {LogicEngine.fmt_usd(r['DailyAPR'])}"
                ExternalAPIs.send_line(token, r["Line_User_ID"], msg, img_url)

            st.success("全ての処理が完了しました。")

# =========================================================
# 5. MAIN ENTRY POINT
# =========================================================

def main():
    st.set_page_config(page_title=AppConfig.TITLE, page_icon=AppConfig.ICON, layout="wide")
    handle_auth()

    # スプレッドシートID抽出
    ss_url = st.secrets["connections"]["gsheets"]["spreadsheet"]
    ss_id = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", ss_url).group(1) if "/spreadsheets/d/" in ss_url else ss_url
    
    # サービス起動
    gs = GSheetService(ss_id, st.session_state.admin_namespace)

    # UIレイアウト
    with st.sidebar:
        st.caption(f"ログイン中: {st.session_state.admin_name}")
        page = st.radio("メニュー", ["📊 ダッシュボード", "📈 APR確定", "⚙️ メンバー管理"])
        if st.button("🚪 ログアウト"):
            st.session_state.clear()
            st.rerun()

    if page == "📊 ダッシュボード": render_dashboard(gs)
    elif page == "📈 APR確定": render_apr_flow(gs)
    else: st.info("管理機能は順次実装中です")

if __name__ == "__main__":
    main()
