import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import requests
import json
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")
st.title("🏦 APR管理システム（LINE Messaging API版）")

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

# --- LINE Messaging API送信関数 ---
def send_line_message(token, user_id, text):
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "to": user_id,
        "messages": [{"type": "text", "text": text}]
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    return response.status_code

# --- メインロジック ---
try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read(worksheet="Settings", ttl=0) 
    
    project_list = df.iloc[:, 0].dropna().astype(str).tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df[df.iloc[:, 0] == selected_project].iloc[0]

    raw_num = str(p_info.iloc[1]).strip()
    num_people = int(float(raw_num)) if raw_num and raw_num.replace('.','').isdigit() else 1
    
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    wallet_list = split_val(p_info.iloc[5], num_people)

    try:
        hist_df = conn.read(worksheet=selected_project, ttl=0)
    except:
        hist_df = pd.DataFrame()

    current_principals = []
    for i in range(num_people):
        unpaid_amount = 0.0
        if not hist_df.empty and "Paid_Flags" in hist_df.columns:
            for _, row in hist_df.iterrows():
                flags = str(row["Paid_Flags"]).split(",")
                if i < len(flags) and flags[i].strip() == "0":
                    breakdown = str(row["Breakdown"]).split(",")
                    if i < len(breakdown):
                        unpaid_amount += to_f(breakdown[i])
        current_principals.append(base_principals[i] + unpaid_amount)

    # --- 画面表示 ---
    total_apr = st.number_input("本日の全体のAPR (%)", value=100.0, step=0.01)
    net_apr_factor = 0.67
    today_yields = [round((p * (total_apr * net_apr_factor * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(current_principals)]
    
    st.info(f"💡 33%控除済み（実質 {total_apr * net_apr_factor:.2f}%）")

    # --- 確定・LINE送信ボタン ---
    if st.button("収益を確定してLINEで通知"):
        with st.spinner("処理中..."):
            # A. 履歴を保存
            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Total_Principal": round(sum(current_principals), 2),
                "Breakdown": ", ".join(map(str, today_yields)),
                "Paid_Flags": ",".join(["0"] * num_people)
            }])
            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            conn.update(worksheet=selected_project, data=updated_hist)
            
            # B. LINE通知処理
            if "line" in st.secrets:
                line_token = st.secrets["line"]["channel_access_token"]
                line_user_id = st.secrets["line"]["user_id"]
                
                report_msg = f"【収益報告】\n{selected_project}\n"
                report_msg += f"本日のAPR: {total_apr}%\n"
                report_msg += f"------------------\n"
                for i in range(num_people):
                    report_msg += f"No.{i+1}: ${today_yields[i]:,.4f}\n"
                report_msg += f"------------------\n"
                report_msg += "※33%控除適用済み"
                
                status = send_line_message(line_token, line_user_id, report_msg)
                
                if status == 200:
                    st.success("LINEに通知を送りました！")
                else:
                    st.error(f"LINE送信エラー（Code:{status}）")
            st.rerun()

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
