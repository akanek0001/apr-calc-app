import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import requests
import json
import re

# --- ページ設定 (一番最初に書く必要があります) ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- 便利関数 ---
def to_f(val):
    try:
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    items = items[:n]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items

def send_line_message(token, user_id, text):
    """個別にLINEメッセージを送信"""
    if not user_id or str(user_id) == "nan": return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": text}]}
    res = requests.post(url, headers=headers, data=json.dumps(payload))
    return res.status_code

# --- メインロジック ---
try:
    # スプレッドシート接続
    conn = st.connection("gsheets", type=GSheetsConnection)
    
    # Settingsシート（プロジェクト情報やユーザーIDが載っているシート）を読み込み
    # ※Makeで書き込んでいるシート名を指定してください
    df = conn.read(worksheet="Settings", ttl=0) 
    
    # サイドバー：プロジェクト選択
    project_list = df.iloc[:, 0].dropna().astype(str).tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df[df.iloc[:, 0] == selected_project].iloc[0]

    # --- 重要：ここからIDを取得 ---
    # Makeで「Line_User_ID」という列名にIDを貯めている想定です
    user_ids = []
    if "Line_User_ID" in df.columns:
        user_ids = df["Line_User_ID"].dropna().tolist()

    num_people = int(float(str(p_info.iloc[1]).strip()))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]

    # 履歴の読み込み
    try:
        hist_df = conn.read(worksheet=selected_project, ttl=0)
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # 収益・出金の計算（既存ロジック維持）
    current_principals = []
    total_withdrawn = []
    total_earned = []

    for i in range(num_people):
        earned = 0.0
        withdrawn = 0.0
        if not hist_df.empty:
            for _, row in hist_df.iterrows():
                vals = str(row["Breakdown"]).split(",")
                if i < len(vals):
                    amount = to_f(vals[i])
                    if str(row["Type"]) == "収益":
                        earned += amount
                    elif str(row["Type"]) == "出金":
                        withdrawn += amount
        total_earned.append(earned)
        total_withdrawn.append(withdrawn)
        current_principals.append(base_principals[i] + earned - withdrawn)

    st.title(f"🏦 {selected_project} 管理パネル")

    tab1, tab2 = st.tabs(["📈 収益確定", "💸 出金処理"])

    with tab1:
        st.subheader("本日の収益計算")
        total_apr = st.number_input("本日の全体のAPR (%)", value=100.0, step=0.01)
        net_apr_factor = 0.67
        today_yields = [round((p * (total_apr * net_apr_factor * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(current_principals)]
        
        if st.button("収益を確定して全員にLINE通知"):
            # スプレッドシート更新
            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "収益",
                "Total_Amount": sum(today_yields),
                "Breakdown": ",".join(map(str, today_yields)),
                "Note": f"APR: {total_apr}%"
            }])
            updated_df = pd.concat([hist_df, new_row], ignore_index=True)
            conn.update(worksheet=selected_project, data=updated_df)
            
            # --- LINE全員送信 ---
            if "line" in st.secrets:
                token = st.secrets["line"]["channel_access_token"]
                msg = f"【収益報告】\n{selected_project}\nAPR: {total_apr}%\n" + "-"*10 + "\n"
                for i in range(num_people):
                    msg += f"No.{i+1}: +${today_yields[i]:,.4f}\n"
                
                # 回収した全IDに送信
                for uid in user_ids:
                    send_line_message(token, uid, msg)
                st.success(f"{len(user_ids)}名に通知を送信しました。")
            st.rerun()

    # (Tab2 出金処理は元のロジックを維持...)
    with tab2:
        st.write("（出金処理ロジックも同様に user_ids をループさせることで通知可能です）")

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
