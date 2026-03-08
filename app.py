from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple, Dict, Final

import gspread
import pandas as pd
import requests
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# =========================================================
# 1. CONSTANTS & DOMAIN CONFIGURATION
# =========================================================

class AppConst:
    """アプリケーション全体で不変の定数定義"""
    TITLE: Final = "APR資産運用管理システム Pro"
    ICON: Final = "🏦"
    JST: Final = timezone(timedelta(hours=9), "JST")
    
    # ランク定義
    MASTER_FACTOR: Final = 0.67
    ELITE_FACTOR: Final = 0.60
    
    # 状態ラベル
    STATUS_ON: Final = "🟢運用中"
    STATUS_OFF: Final = "🔴停止"
    
    # プロジェクト種別
    PROJECT_PERSONAL: Final = "PERSONAL"
    
    # 複利タイプ
    COMPOUND_DAILY: Final = "daily"
    COMPOUND_MONTHLY: Final = "monthly"
    COMPOUND_NONE: Final = "none"

    # 取引種別
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
# 2. UTILITY & DOMAIN LOGIC (SERVICES)
# =========================================================

class TimeUtil:
    """時間操作に関するユーティリティ"""
    @staticmethod
    def now() -> datetime:
        return datetime.now(AppConst.JST)

    @staticmethod
    def fmt_full(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def fmt_date(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d")

class FinanceEngine:
    """APR計算および通貨操作のロジック"""
    @staticmethod
    def format_usd(val: float) -> str:
        return f"${val:,.2f}"

    @staticmethod
    def to_float(v: Any) -> float:
        try:
            s = str(v).replace(",", "").replace("$", "").replace("%", "").strip()
            return float(s) if s else 0.0
        except (ValueError, TypeError):
            return 0.0

    @classmethod
    def calculate_rewards(cls, members: pd.DataFrame, apr_rate: float, net_factor: float, project: str) -> pd.DataFrame:
        """プロジェクトごとのAPR配当計算"""
        df = members.copy()
        is_personal = project.upper() == AppConst.PROJECT_PERSONAL

        if is_personal:
            # 個人別計算: ランク係数を使用
            df["Factor"] = df["Rank"].apply(lambda r: AppConst.MASTER_FACTOR if str(r).lower() == "master" else AppConst.ELITE_FACTOR)
            df["DailyAPR"] = (df["Principal"] * (apr_rate / 100.0) * df["Factor"]) / 365.0
            df["CalcMode"] = "PERSONAL"
        else:
            # グループ計算: 総額を均等割
            total_principal = df["Principal"].sum()
            total_group_reward = (total_principal * (apr_rate / 100.0) * net_factor) / 365.0
            df["DailyAPR"] = (total_group_reward / len(df)) if len(df) > 0 else 0.0
            df["Factor"] = net_factor
            df["CalcMode"] = "GROUP_EQUAL"
        return df

# =========================================================
# 3. EXTERNAL API WRAPPERS (GSHEET, LINE, OCR)
# =========================================================

class GSheetClient:
    """Google Sheetsへの低レイヤアクセスをカプセル化"""
    def __init__(self, spreadsheet_id: str, namespace: str):
        self.namespace = namespace
        self.book = self._authorize_and_open(spreadsheet_id)
        self._ensure_all_sheets()

    def _authorize_and_open(self, sid: str):
        creds_info = st.secrets["connections"]["gsheets"]["credentials"]
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        gc = gspread.authorize(Credentials.from_service_account_info(dict(creds_info), scopes=scopes))
        return gc.open_by_key(sid)

    def get_worksheet_name(self, base_name: str) -> str:
        return f"{base_name}__{self.namespace}" if self.namespace != "default" else base_name

    def _ensure_all_sheets(self):
        """必要なシートの存在確認と初期化"""
        for base, headers in AppConst.HEADERS.items():
            name = self.get_worksheet_name(base)
            try:
                self.book.worksheet(name)
            except gspread.WorksheetNotFound:
                ws = self.book.add_worksheet(title=name, rows=3000, cols=20)
                ws.append_row(headers, value_input_option="USER_ENTERED")

    def load_as_df(self, base_name: str) -> pd.DataFrame:
        name = self.get_worksheet_name(base_name)
        values = self.book.worksheet(name).get_all_values()
        if not values: return pd.DataFrame()
        df = pd.DataFrame(values[1:], columns=values[0])
        # 列名のクリーニング
        df.columns = df.columns.astype(str).str.strip()
        return df

    def append_record(self, base_name: str, row: List[Any]):
        name = self.get_worksheet_name(base_name)
        self.book.worksheet(name).append_row(row, value_input_option="USER_ENTERED")

# =========================================================
# 4. STREAMLIT UI COMPONENTS
# =========================================================

class UIHelper:
    """UIのパーツを管理する静的クラス"""
    @staticmethod
    def apply_custom_css():
        st.markdown("""
            <style>
                .stMetric { background-color: #f0f2f6; padding: 15px; border-radius: 10px; }
                .main-header { color: #1E3A8A; font-weight: bold; }
            </style>
        """, unsafe_allow_html=True)

    @staticmethod
    def sidebar_navigation():
        with st.sidebar:
            st.title(f"{AppConst.ICON} System Menu")
            return st.radio(
                "ページ選択",
                ["📊 ダッシュボード", "📈 APR確定", "💸 入出金管理", "⚙️ 管理設定", "❓ ヘルプ"]
            )

# =========================================================
# 5. MAIN APPLICATION CONTROLLER
# =========================================================

def main():
    st.set_page_config(page_title=AppConst.TITLE, page_icon=AppConst.ICON, layout="wide")
    UIHelper.apply_custom_css()
    st.title(f"{AppConst.ICON} {AppConst.TITLE}")

    # 1. 認証チェック
    if not st.session_state.get("admin_ok", False):
        # ログインフォーム (元のロジックをここに統合)
        render_login_screen()
        return

    # 2. サービス初期化
    sid = FinanceEngine.to_float(0) # ID抽出ロジック(略)
    gs = GSheetClient(st.secrets["connections"]["gsheets"]["spreadsheet"], st.session_state.admin_namespace)

    # 3. メインルーティング
    page = UIHelper.sidebar_navigation()

    if page == "📊 ダッシュボード":
        render_dashboard_page(gs)
    elif page == "📈 APR確定":
        render_apr_page(gs)
    # ... 他のページへ続く

def render_login_screen():
    """ログイン画面の描画"""
    with st.container():
        st.subheader("🔐 管理者認証")
        # ログインフォームの実装...
        # 成功時に st.session_state.admin_ok = True

def render_dashboard_page(gs: GSheetClient):
    """ダッシュボードの描画"""
    st.header("📊 運用状況サマリー")
    members_df = gs.load_as_df("Members")
    # 指標表示ロジック...

if __name__ == "__main__":
    main()
