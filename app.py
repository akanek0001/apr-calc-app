from __future__ import annotations

import json
import re
import io
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, List, Optional, Tuple, Final, Dict

import pandas as pd
import requests
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from PIL import Image

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
    
    # 種別定義
    TYPE_APR: Final = "APR"
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
# 2. BUSINESS LOGIC ENGINE (OCR / CALC / TEXT)
# =========================================================

class LogicEngine:
    """計算・テキスト解析・OCR候補抽出のコア"""
    @staticmethod
    def now_jst() -> datetime:
        return datetime.now(AppConfig.JST)

    @staticmethod
    def to_float(v: Any) -> float:
        try:
            if not v: return 0.0
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
        df["Principal"] = df["Principal"].apply(cls.to_float)
        
        if project.upper() == AppConfig.PROJECT_PERSONAL:
            df["Factor"] = df["Rank"].apply(lambda r: AppConfig.FACTORS.get(r, AppConfig.FACTORS[AppConfig.RANK_MASTER]))
            df["DailyAPR"] = (df["Principal"] * (apr_rate / 100.0) * df["Factor"]) / 365.0
        else:
            total_principal = df["Principal"].sum()
            total_reward = (total_principal * (apr_rate / 100.0) * net_factor) / 365.0
            df["DailyAPR"] = (total_reward / len(df)) if not df.empty else 0.0
            df["Factor"] = net_factor
        return df

    @staticmethod
    def extract_apr_candidates(text: str) -> List[float]:
        """OCRテキストからAPR（%）と思われる数値をすべて抽出"""
        pattern = r"(\d+(?:\.\d+)?)\s*%"
        matches = re.findall(pattern, text)
        return sorted(list(set(float(m) for m in matches)), reverse=True)

# =========================================================
# 3. EXTERNAL INFRASTRUCTURE SERVICES (GS / LINE / OCR / ImgBB)
# =========================================================

class ExternalServices:
    """外部APIとの通信を集約"""
    @staticmethod
    def upload_imgbb(image_bytes: bytes) -> Optional[str]:
        api_key = st.secrets.get("imgbb", {}).get("api_key")
        if not api_key: return None
        try:
            r = requests.post("https://api.imgbb.com/1/upload", params={"key": api_key}, files={"image": image_bytes}, timeout=30)
            return r.json().get("data", {}).get("url")
        except: return None

    @staticmethod
    def ocr_space(image_bytes: bytes) -> str:
        api_key = st.secrets.get("ocr", {}).get("api_key", "helloworld")
        try:
            files = {"filename": ("image.jpg", image_bytes, "image/jpeg")}
            data = {"apikey": api_key, "language": "eng", "isOverlayRequired": False}
            r = requests.post("https://api.ocr.space/parse/image", files=files, data=data, timeout=30)
            result = r.json()
            return result.get("ParsedResults", [{}])[0].get("ParsedText", "")
        except: return ""

    @staticmethod
    def send_line_push(token: str, user_id: str, text: str, img_url: Optional[str] = None) -> bool:
        if not user_id or not token: return False
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        messages = [{"type": "text", "text": text}]
        if img_url:
            messages.append({"type": "image", "originalContentUrl": img_url, "previewImageUrl": img_url})
        try:
            r = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json={"to": user_id, "messages": messages}, timeout=20)
            return r.status_code == 200
        except: return False

class GSheetService:
    def __init__(self, spreadsheet_id: str, namespace: str):
        self.sid = spreadsheet_id
        self.namespace = namespace
        self.gc = self._auth()
        self.book = self.gc.open_by_key(self.sid)
        self._ensure_sheets()

    def _auth(self):
        creds = st.secrets["connections"]["gsheets"]["credentials"]
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        return gspread.authorize(Credentials.from_service_account_info(dict(creds), scopes=scopes))

    def _get_name(self, base: str) -> str:
        return f"{base}__{self.namespace}" if self.namespace != "default" else base

    def _ensure_sheets(self):
        for base, head in AppConfig.HEADERS.items():
            name = self._get_name(base)
            try: self.book.worksheet(name)
            except:
                ws = self.book.add_worksheet(title=name, rows=5000, cols=20)
                ws.append_row(head, value_input_option="USER_ENTERED")

    def read_df(self, base: str) -> pd.DataFrame:
        name = self._get_name(base)
        data = self.book.worksheet(name).get_all_values()
        if not data: return pd.DataFrame(columns=AppConfig.HEADERS[base])
        return pd.DataFrame(data[1:], columns=data[0])

    def write_df(self, base: str, df: pd.DataFrame):
        ws = self.book.worksheet(self._get_name(base))
        ws.clear()
        ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist(), value_input_option="USER_ENTERED")

    def append_row(self, base: str, row: list):
        self.book.worksheet(self._get_name(base)).append_row(row, value_input_option="USER_ENTERED")

# =========================================================
# 4. UI COMPONENTS (PAGES)
# =========================================================

def render_apr_page(gs: GSheetService):
    st.header("📈 APR報酬確定プロセス")
    
    # 1. 画像アップロード & OCR
    uploaded_file = st.file_uploader("エビデンス画像をアップロード", type=["jpg", "png", "jpeg"])
    ocr_text = ""
    candidates = []
    
    if uploaded_file:
        img_bytes = uploaded_file.getvalue()
        st.image(img_bytes, caption="アップロード画像", width=300)
        
        if st.button("🔍 画像からAPRを解析"):
            with st.spinner("OCR解析中..."):
                ocr_text = ExternalServices.ocr_space(img_bytes)
                candidates = LogicEngine.extract_apr_candidates(ocr_text)
                if candidates:
                    st.session_state.ocr_candidates = candidates
                    st.success(f"APR候補が見つかりました: {candidates}")
                else:
                    st.warning("APR数値を検出できませんでした。手入力してください。")

    # 2. 入力フォーム
    settings = gs.read_df("Settings")
    projs = settings[settings["Active"].apply(LogicEngine.truthy)]["Project_Name"].tolist()
    
    with st.form("apr_confirm_form"):
        col1, col2 = st.columns(2)
        target_proj = col1.selectbox("対象プロジェクト", projs)
        
        default_apr = st.session_state.get("ocr_candidates", [0.0])[0]
        apr_val = col2.number_input("適用APR (%)", value=float(default_apr), step=0.0001, format="%.4f")
        
        submit = st.form_submit_button("🚀 報酬確定とLINE一斉送信")

    if submit and uploaded_file:
        with st.spinner("データ更新および通知送信中..."):
            # A. 画像アップロード
            img_url = ExternalServices.upload_imgbb(uploaded_file.getvalue())
            
            # B. 計算
            m_df = gs.read_df("Members")
            targets = m_df[(m_df["Project_Name"] == target_proj) & (m_df["IsActive"].apply(LogicEngine.truthy))]
            proj_set = settings[settings["Project_Name"] == target_proj].iloc[0]
            
            results = LogicEngine.calculate_apr_distribution(targets, apr_val, LogicEngine.to_float(proj_set["Net_Factor"]), target_proj)
            
            # C. 記録 & LINE送信
            line_token = st.secrets["line"]["tokens"].get(st.session_state.admin_namespace)
            ts = LogicEngine.now_jst().strftime("%Y-%m-%d %H:%M:%S")
            
            for _, r in results.iterrows():
                # Ledger記録
                gs.append_row("Ledger", [ts, target_proj, r["PersonName"], AppConfig.TYPE_APR, r["DailyAPR"], f"APR:{apr_val}%", img_url, r["Line_User_ID"], r["LINE_DisplayName"], "app"])
                
                # LINE送信
                msg = f"🏦【APR収益報告】\n{r['PersonName']} 様\n本日APR: {apr_val}%\n配当額: {LogicEngine.fmt_usd(r['DailyAPR'])}\n元本: {LogicEngine.fmt_usd(LogicEngine.to_float(r['Principal']))}"
                ExternalServices.send_line_push(line_token, r["Line_User_ID"], msg, img_url)
            
            st.success(f"完了: {len(results)}名に通知を送信しました。")

def render_cash_mgmt(gs: GSheetService):
    st.header("💸 入出金・元本管理")
    members = gs.read_df("Members")
    
    with st.expander("➕ 新規入出金記録"):
        with st.form("cash_form"):
            name = st.selectbox("メンバー名", members["PersonName"].tolist())
            m_info = members[members["PersonName"] == name].iloc[0]
            ctype = st.radio("種別", [AppConfig.TYPE_DEPOSIT, AppConfig.TYPE_WITHDRAW], horizontal=True)
            amt = st.number_input("金額 (USD)", min_value=0.0)
            note = st.text_input("備考")
            if st.form_submit_button("記録する"):
                # 元本更新
                curr_p = LogicEngine.to_float(m_info["Principal"])
                new_p = curr_p + amt if ctype == AppConfig.TYPE_DEPOSIT else curr_p - amt
                members.loc[members["PersonName"] == name, "Principal"] = new_p
                members.loc[members["PersonName"] == name, "UpdatedAt_JST"] = LogicEngine.now_jst().strftime("%Y-%m-%d %H:%M:%S")
                
                gs.write_df("Members", members)
                gs.append_row("Ledger", [LogicEngine.now_jst().strftime("%Y-%m-%d %H:%M:%S"), m_info["Project_Name"], name, ctype, amt, note, "", m_info["Line_User_ID"], m_info["LINE_DisplayName"], "app"])
                st.success("更新完了")
                st.rerun()

# =========================================================
# 5. MAIN CONTROL FLOW
# =========================================================

def handle_auth():
    if st.session_state.get("admin_ok"): return
    st.title("🔐 Login")
    admins = st.secrets.get("admin", {}).get("users", [])
    with st.form("login"):
        user_name = st.selectbox("Admin", [u["name"] for u in admins])
        pin = st.text_input("PIN", type="password")
        if st.form_submit_button("Login"):
            u = next((x for x in admins if x["name"] == user_name), None)
            if u and str(u["pin"]) == pin:
                st.session_state.admin_ok = True
                st.session_state.admin_name = user_name
                st.session_state.admin_namespace = u.get("namespace", "default")
                st.rerun()
            else: st.error("Invalid PIN")
    st.stop()

def main():
    st.set_page_config(page_title=AppConfig.TITLE, page_icon=AppConfig.ICON, layout="wide")
    handle_auth()
    
    # Spreadsheet ID取得
    raw_id = st.secrets["connections"]["gsheets"]["spreadsheet"]
    ss_id = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", raw_id).group(1) if "/spreadsheets/d/" in raw_id else raw_id
    gs = GSheetService(ss_id, st.session_state.admin_namespace)

    with st.sidebar:
        st.title(f"{AppConfig.ICON} Menu")
        st.write(f"User: {st.session_state.admin_name}")
        page = st.radio("移動先", ["📊 ダッシュボード", "📈 APR確定", "💸 入出金管理", "⚙️ 設定変更"])
        if st.button("Logout"):
            st.session_state.clear()
            st.rerun()

    if page == "📊 ダッシュボード":
        # サマリー表示(略)
        st.write("Dashboard")
    elif page == "📈 APR確定":
        render_apr_page(gs)
    elif page == "💸 入出金管理":
        render_cash_mgmt(gs)
    elif page == "⚙️ 設定変更":
        st.subheader("一括データ編集")
        for sheet in ["Members", "Settings"]:
            st.write(f"### {sheet}")
            df = gs.read_df(sheet)
            edited = st.data_editor(df, key=f"edit_{sheet}", hide_index=True)
            if st.button(f"{sheet}を保存"):
                gs.write_df(sheet, edited)
                st.success("Saved")

if __name__ == "__main__":
    main()
