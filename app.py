from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple, Dict

import gspread
import pandas as pd
import requests
import streamlit as st
from google.oauth2.service_account import Credentials

# =========================================================
# 1. CONSTANTS & CONFIGURATION
# =========================================================

class AppConfig:
    """アプリケーション全体の設定を管理する定数クラス"""
    TITLE = "APR資産運用管理システム Pro"
    ICON = "🏦"
    JST = timezone(timedelta(hours=9), "JST")
    
    # ランク・計算係数
    RANKS = {
        "Master": {"factor": 0.67, "label": "👑 Master (67%)"},
        "Elite": {"factor": 0.60, "label": "🥈 Elite (60%)"}
    }
    
    # ステータス定義
    STATUS_ON = "🟢運用中"
    STATUS_OFF = "🔴停止"
    
    # プロジェクト定数
    PERSONAL_PROJECT = "PERSONAL"
    
    # 複利設定
    COMPOUND = {
        "DAILY": "daily",
        "MONTHLY": "monthly",
        "NONE": "none"
    }

    # トランザクションタイプ
    TYPES = {
        "APR": "APR",
        "LINE": "LINE",
        "DEPOSIT": "Deposit",
        "WITHDRAW": "Withdraw"
    }

    # シート名ベース
    SHEET_BASES = {
        "SETTINGS": "Settings",
        "MEMBERS": "Members",
        "LEDGER": "Ledger",
        "LINE_USERS": "LineUsers",
        "SUMMARY": "APR_Summary"
    }

    # ヘッダー定義
    HEADERS = {
        "SETTINGS": ["Project_Name", "Net_Factor", "IsCompound", "Compound_Timing", "UpdatedAt_JST", "Active"],
        "MEMBERS": ["Project_Name", "PersonName", "Principal", "Line_User_ID", "LINE_DisplayName", "Rank", "IsActive", "CreatedAt_JST", "UpdatedAt_JST"],
        "LEDGER": ["Datetime_JST", "Project_Name", "PersonName", "Type", "Amount", "Note", "Evidence_URL", "Line_User_ID", "LINE_DisplayName", "Source"]
    }

# =========================================================
# 2. CORE DOMAIN LOGIC (SERVICES)
# =========================================================

class Utils:
    """純粋なユーティリティ関数群"""
    @staticmethod
    def now_jst() -> datetime:
        return datetime.now(AppConfig.JST)

    @staticmethod
    def format_usd(val: float) -> str:
        return f"${val:,.2f}"

    @staticmethod
    def to_float(val: Any) -> float:
        if not val: return 0.0
        try:
            return float(str(val).replace(",", "").replace("$", "").replace("%", "").strip())
        except ValueError:
            return 0.0

class FinanceEngine:
    """APR計算等のビジネスロジックを担当"""
    @staticmethod
    def calculate_apr(df: pd.DataFrame, apr_rate: float, net_factor: float, project_name: str) -> pd.DataFrame:
        df = df.copy()
        is_personal = project_name.upper() == AppConfig.PERSONAL_PROJECT
        
        if is_personal:
            df["Factor"] = df["Rank"].apply(lambda r: AppConfig.RANKS.get(r, AppConfig.RANKS["Master"])["factor"])
            df["DailyAPR"] = (df["Principal"] * (apr_rate / 100.0) * df["Factor"]) / 365.0
            df["CalcMode"] = "PERSONAL"
        else:
            total_principal = df["Principal"].sum()
            total_reward = (total_principal * (apr_rate / 100.0) * net_factor) / 365.0
            df["DailyAPR"] = total_reward / len(df) if len(df) > 0 else 0
            df["Factor"] = net_factor
            df["CalcMode"] = "GROUP_EQUAL"
        return df

class GSheetService:
    """Google Sheets通信を抽象化"""
    def __init__(self, spreadsheet_id: str, namespace: str):
        self.sid = spreadsheet_id
        self.ns = namespace
        self._client = self._authenticate()
        self.book = self._client.open_by_key(self.sid)

    def _authenticate(self):
        creds_info = st.secrets["connections"]["gsheets"]["credentials"]
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        return gspread.authorize(Credentials.from_service_account_info(creds_info, scopes=scopes))

    def get_sheet_name(self, base_key: str) -> str:
        base = AppConfig.SHEET_BASES[base_key]
        return f"{base}__{self.ns}" if self.ns != "default" else base

    def load_df(self, base_key: str) -> pd.DataFrame:
        name = self.get_sheet_name(base_key)
        ws = self.book.worksheet(name)
        data = ws.get_all_values()
        if not data: return pd.DataFrame()
        return pd.DataFrame(data[1:], columns=data[0])

    def save_df(self, base_key: str, df: pd.DataFrame):
        name = self.get_sheet_name(base_key)
        ws = self.book.worksheet(name)
        ws.clear()
        ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())

# =========================================================
# 3. UI COMPONENTS
# =========================================================

class Interface:
    """StreamlitのUI描画ロジック"""
    
    @staticmethod
    def render_sidebar():
        """共通サイドバー"""
        with st.sidebar:
            st.title(f"{AppConfig.ICON} Menu")
            st.info(f"User: {st.session_state.get('admin_name', 'Unknown')}")
            if st.button("🚪 ログアウト"):
                st.session_state.clear()
                st.rerun()
            
            return st.radio("ナビゲーション", ["📊 ダッシュボード", "📈 APR確定", "💸 入出金管理", "⚙️ システム設定"])

    @staticmethod
    def display_metrics(total_assets: float, today_apr: float):
        """指標カードの表示"""
        col1, col2 = st.columns(2)
        col1.metric("運用総資産", Utils.format_usd(total_assets))
        col2.metric("本日の発生配当", Utils.format_usd(today_apr))

# =========================================================
# 4. MAIN APPLICATION
# =========================================================

def main():
    st.set_page_config(page_title=AppConfig.TITLE, page_icon=AppConfig.ICON, layout="wide")

    # セッション初期化 (認証等)
    if "admin_ok" not in st.session_state or not st.session_state.admin_ok:
        # ここに認証ロジックを配置（今回は省略）
        st.warning("ログインが必要です")
        return

    # サービスの初期化
    try:
        gs = GSheetService(
            spreadsheet_id=st.secrets["connections"]["gsheets"]["spreadsheet"],
            namespace=st.session_state.admin_namespace
        )
    except Exception as e:
        st.error(f"Google Sheetsとの接続に失敗しました: {e}")
        return

    # メインルーティング
    page = Interface.render_sidebar()

    if page == "📊 ダッシュボード":
        st.header("📊 運用状況サマリー")
        # データのロードと表示処理...
        
    elif page == "📈 APR確定":
        st.header("📈 APR報酬確定プロセス")
        # APR確定ロジック...

    # ... 他のページも同様に整理

if __name__ == "__main__":
    main()
