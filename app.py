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

# =========================================================
# 1. CONSTANTS & SYSTEM CONFIGURATION
# =========================================================

class AppConfig:
    TITLE: Final = "APR資産運用管理システム Pro"
    ICON: Final = "🏦"
    JST: Final = timezone(timedelta(hours=9), "JST")
    
    # ランク係数
    FACTORS: Final = {"Master": 0.67, "Elite": 0.60}
    
    # 種別
    TYPE_APR: Final = "APR"
    TYPE_LINE: Final = "LINE"

    # シートヘッダー
    HEADERS: Final = {
        "Settings": ["Project_Name", "Net_Factor", "IsCompound", "Compound_Timing", "UpdatedAt_JST", "Active"],
        "Members": ["Project_Name", "PersonName", "Principal", "Line_User_ID", "LINE_DisplayName", "Rank", "IsActive", "CreatedAt_JST", "UpdatedAt_JST"],
        "Ledger": ["Datetime_JST", "Project_Name", "PersonName", "Type", "Amount", "Note", "Evidence_URL", "Line_User_ID", "LINE_DisplayName", "Source"],
        "LineUsers": ["Date", "Time", "Type", "Line_User_ID", "Line_User"]
    }

# =========================================================
# 2. EXTERNAL SERVICES (LINE / ImgBB / OCR)
# =========================================================

class ExternalServices:
    """LINE送信および画像関連の外部通信"""
    
    @staticmethod
    def send_line_push(token: str, user_id: str, text: str, img_url: Optional[str] = None) -> bool:
        """LINE Messaging API を使用して個別プッシュ通知を送信"""
        if not user_id or not token:
            return False
            
        url = "https://api.line.me/v2/bot/message/push"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        
        # メッセージ構成
        messages = [{"type": "text", "text": text}]
        if img_url:
            messages.append({
                "type": "image",
                "originalContentUrl": img_url,
                "previewImageUrl": img_url
            })
            
        payload = {"to": user_id, "messages": messages}
        
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=20)
            return r.status_code == 200
        except Exception as e:
            st.error(f"LINE送信エラー: {e}")
            return False

    @staticmethod
    def upload_to_imgbb(image_bytes: bytes) -> Optional[str]:
        """エビデンス画像をImgBBにアップロードしてURLを取得"""
        api_key = st.secrets.get("imgbb", {}).get("api_key")
        if not api_key:
            return None
        
        try:
            url = "https://api.imgbb.com/1/upload"
            r = requests.post(url, params={"key": api_key}, files={"image": image_bytes}, timeout=30)
            return r.json().get("data", {}).get("url")
        except:
            return None

# =========================================================
# 3. APR DETERMINATION & LINE PUSH FLOW
# =========================================================

def render_apr_page(gs: GSheetService):
    st.header("📈 APR確定とLINE通知")
    
    # 1. 設定ロード
    settings = gs.read_df("Settings")
    active_projs = settings[settings["Active"].apply(lambda x: str(x).lower() == 'true')]["Project_Name"].tolist()
    
    # 2. 入力
    project = st.selectbox("対象プロジェクト", active_projs)
    apr_val = st.number_input("適用APR (%)", value=0.0, step=0.0001, format="%.4f")
    uploaded_file = st.file_uploader("エビデンス画像 (LINEに添付されます)", type=["jpg", "png", "jpeg"])
    
    if st.button("🚀 報酬を確定してLINE送信を開始"):
        if not uploaded_file:
            st.warning("エビデンス画像をアップロードしてください。")
            return

        with st.spinner("処理中..."):
            # A. 画像アップロード
            img_url = ExternalServices.upload_to_imgbb(uploaded_file.getvalue())
            
            # B. メンバー取得と配当計算
            m_df = gs.read_df("Members")
            targets = m_df[(m_df["Project_Name"] == project) & (m_df["IsActive"].apply(lambda x: str(x).lower() == 'true'))]
            
            # 係数取得
            net_f = float(settings[settings["Project_Name"] == project].iloc[0]["Net_Factor"])
            
            # C. LINEトークンの取得 (Secretsから管理者ネームスペースに対応するものを選択)
            line_token = st.secrets["line"]["tokens"].get(st.session_state.admin_namespace)
            
            ts = datetime.now(AppConfig.JST).strftime("%Y-%m-%d %H:%M:%S")
            success_count = 0
            
            for _, r in targets.iterrows():
                # 計算ロジック
                p = float(str(r["Principal"]).replace(",", ""))
                factor = AppConfig.FACTORS.get(r["Rank"], 0.67) if project == "PERSONAL" else net_f
                daily_apr = (p * (apr_val / 100.0) * factor) / 365.0
                
                # LINEメッセージ作成
                msg = (
                    f"🏦【収益報告】\n"
                    f"{r['PersonName']} 様\n"
                    f"━━━━━━━━━━━━\n"
                    f"本日APR: {apr_val}%\n"
                    f"配当額: ${daily_apr:,.2f}\n"
                    f"現在元本: ${p:,.2f}\n"
                    f"━━━━━━━━━━━━\n"
                    f"ご確認をお願いいたします。"
                )
                
                # LINE送信実行
                is_sent = ExternalServices.send_line_push(line_token, r["Line_User_ID"], msg, img_url)
                
                # スプレッドシート記録 (Ledger)
                gs.append_row("Ledger", [
                    ts, project, r["PersonName"], AppConfig.TYPE_APR, 
                    daily_apr, f"APR:{apr_val}%", img_url or "", 
                    r["Line_User_ID"], r["LINE_DisplayName"], "System"
                ])
                
                if is_sent:
                    success_count += 1
            
            st.success(f"完了！ {len(targets)}名中 {success_count}名にLINEを送信しました。")

# =========================================================
# 4. MAIN & NAVIGATION
# =========================================================

def main():
    # ... (認証・初期化ロジック) ...
    # サイドバーでページを切り替え
    if page == "📊 ダッシュボード":
        render_dashboard(gs)
    elif page == "📈 APR確定":
        render_apr_page(gs)
    elif page == "❓ ヘルプ":
        render_help_page()
    # ... (他ページ) ...

# (GSheetServiceなどのクラス定義は前回と同様)
