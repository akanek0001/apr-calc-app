import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import requests
import re

# --- 1. 設定 ---
st.set_page_config(page_title="APR運用システム", layout="wide")

# スプレッドシートをCSVとして読み込む関数
def fetch_data(gid):
    url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    csv_url = f"{url}/export?format=csv&gid={gid}"
    return pd.read_csv(csv_url)

# --- 2. メインロジック ---
st.title("💎 APR資産運用管理システム Ultra-Light")

try:
    # Settingsシート(gid=0)の読み込み
    settings_df = fetch_data("0")
    
    # プロジェクト選択
    project_list = settings_df.iloc[:, 0].dropna().unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクト選択", project_list)
    
    # 選択プロジェクトのデータ抽出
    p_idx = settings_df[settings_df.iloc[:, 0] == selected_project].index[0]
    p_info = settings_df.loc[p_idx]
    
    # 人数・元本・名前などの展開
    num = int(p_info.iloc[1])
    names = str(p_info.iloc[6]).split(',')
    principals = [float(x) for x in str(p_info.iloc[3]).split(',')]
    rates = [float(x) for x in str(p_info.iloc[4]).split(',')]

    tab1, tab2, tab3 = st.tabs(["📈 収益報告", "💸 入出金", "👥 ユーザー追加"])

    # --- タブ1: 収益報告 & LINE送信 ---
    with tab1:
        st.subheader("本日の収益報告")
        apr = st.number_input("本日のAPR (%)", value=100.0)
        uploaded_file = st.file_uploader("エビデンス画像", type=['png', 'jpg'])
        
        # 収益計算 (0.77係数)
        yields = [round((p * (apr * 0.77 * rates[i] / 100)) / 365, 4) for i, p in enumerate(principals)]
        
        preview = f"🏦 【{selected_project}】\nAPR: {apr}%\n"
        for i in range(len(names)):
            preview += f"・{names[i]}: +${yields[i]:,.4f}\n"
        st.code(preview)

        if st.button("🚀 LINE送信＆保存", type="primary"):
            # ここでGASのWebhook URLへデータを飛ばす（LINE送信とスプレッドシート保存を一度に実行）
            payload = {
                "type": "report",
                "project": selected_project,
                "message": preview,
                "data": yields
            }
            requests.post(st.secrets["gas_url"], json=payload)
            st.success("送信完了！")

    # --- タブ3: ユーザー追加 ---
    with tab3:
        st.subheader("👥 ユーザー追加登録")
        with st.form("add_user"):
            new_n = st.text_input("名前")
            new_p = st.number_input("元本 ($)", value=0.0)
            if st.form_submit_button("追加を実行"):
                # 新しいリストを作成してGASへ飛ばす
                names.append(new_n)
                principals.append(new_p)
                rates.append(1.0)
                
                payload = {
                    "type": "add_user",
                    "project": selected_project,
                    "names": ",".join(names),
                    "principals": ",".join([str(x) for x in principals]),
                    "rates": ",".join([str(x) for x in rates]),
                    "num": len(names)
                }
                requests.post(st.secrets["gas_url"], json=payload)
                st.success(f"{new_n}様を追加しました。")
                st.rerun()

except Exception as e:
    st.error(f"エラー: {e}")
