import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理", layout="wide")
st.title("🏦 APR管理システム（1人専用・修正済）")

try:
    # 1. スプレッドシート接続
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read(worksheet="Settings")
    
    # 2. プロジェクト選択
    project_list = df.iloc[:, 0].dropna().astype(str).tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    
    # 3. データの取得
    p_info = df[df.iloc[:, 0] == selected_project].iloc[0]

    # --- 人数を「1」に固定 ---
    fixed_num = 1

    # D列（4番目）から元本を取得
    def to_f(val):
        try:
            clean = str(val).replace(',','').replace('$','').replace('%','').strip()
            return float(clean) if clean else 0.0
        except:
            return 0.0

    principal = to_f(p_info.iloc[3])

    # --- 表示 ---
    st.subheader(f"📊 {selected_project} の状況")
    
    col1, col2 = st.columns(2)
    col1.metric("確定人数", f"{fixed_num} 名")
    col2.metric("運用元本", f"${principal:,.2f}")

    total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.01)
    
    # 収益計算（1日分）
    today_yield = round((principal * (total_apr / 100)) / 365, 4)
    st.metric("本日の収益", f"${today_yield:,.4f}")

    if st.button("収益を保存する"):
        # 履歴用シートに保存
        try:
            hist_df = conn.read(worksheet=selected_project)
        except:
            hist_df = pd.DataFrame()
            
        new_data = pd.DataFrame([{
            "Date": datetime.now().strftime("%Y-%m-%d"),
            "Total_Principal": principal,
            "Yield": today_yield,
            "Paid": "0"
        }])
        
        updated_df = pd.concat([hist_df, new_data], ignore_index=True)
        conn.update(worksheet=selected_project, data=updated_df)
        st.success("保存が完了しました！")
        st.rerun()

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
    st.info("SettingsシートのA列にプロジェクト名、D列に元本が入っているか確認してください。")
