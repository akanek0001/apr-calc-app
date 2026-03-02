import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import requests
import json
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")
st.title("🏦 APR管理システム（複利・出金管理版）")

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
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": text}]}
    return requests.post(url, headers=headers, data=json.dumps(payload)).status_code

# --- メインロジック ---
try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read(worksheet="Settings", ttl=0) 
    
    project_list = df.iloc[:, 0].dropna().astype(str).tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df[df.iloc[:, 0] == selected_project].iloc[0]

    num_people = int(float(str(p_info.iloc[1]).strip()))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]

    # 履歴の読み込み
    try:
        hist_df = conn.read(worksheet=selected_project, ttl=0)
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # 各メンバーの現在の状況を計算
    current_principals = []
    total_withdrawn = [] # 累計出金額
    total_earned = []    # 累計収益額

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
        # 現在の元本 = 初期元本 + 累計収益 - 累計出金
        current_principals.append(base_principals[i] + earned - withdrawn)

    # --- タブ分け ---
    tab1, tab2 = st.tabs(["📈 本日の収益確定", "💸 出金・精算処理"])

    # --- Tab 1: 収益確定 ---
    with tab1:
        st.subheader("本日の収益計算")
        total_apr = st.number_input("本日の全体のAPR (%)", value=100.0, step=0.01)
        net_apr_factor = 0.67
        today_yields = [round((p * (total_apr * net_apr_factor * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(current_principals)]
        
        st.info(f"💡 33%控除済み（実質 {total_apr * net_apr_factor:.2f}%）")

        # サマリー表示
        cols = st.columns(num_people)
        for i, col in enumerate(cols):
            with col:
                st.metric(f"No.{i+1} 現在の元本", f"${current_principals[i]:,.2f}")
                st.write(f"累計収益: ${total_earned[i]:,.2f}")
                st.write(f"累計出金: ${total_withdrawn[i]:,.2f}")

        if st.button("収益を確定してLINE通知"):
            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "収益",
                "Total_Amount": sum(today_yields),
                "Breakdown": ",".join(map(str, today_yields)),
                "Note": f"APR: {total_apr}%"
            }])
            conn.update(worksheet=selected_project, data=pd.concat([hist_df, new_row], ignore_index=True))
            
            # LINE送信
            if "line" in st.secrets:
                msg = f"【収益報告】\n{selected_project}\nAPR: {total_apr}%\n" + "-"*10 + "\n"
                for i in range(num_people):
                    msg += f"No.{i+1}: +${today_yields[i]:,.4f}\n(元本: ${current_principals[i]+today_yields[i]:,.2f})\n"
                send_line_message(st.secrets["line"]["channel_access_token"], st.secrets["line"]["user_id"], msg)
            st.success("収益を記録しました。")
            st.rerun()

    # --- Tab 2: 出金処理 ---
    with tab2:
        st.subheader("出金・精算の記録")
        target_member = st.selectbox("出金するメンバー", [f"No.{i+1}" for i in range(num_people)])
        member_idx = int(target_member.split(".")[1]) - 1
        
        st.warning(f"{target_member} の出金可能額（複利込）: **${current_principals[member_idx]:,.2f}**")
        withdraw_amount = st.number_input("出金額 ($)", min_value=0.0, max_value=current_principals[member_idx], step=1.0)
        
        if st.button(f"{target_member} の出金を実行する"):
            if withdraw_amount > 0:
                # 全員分の配列を作成（対象者以外は0）
                withdrawals = [0.0] * num_people
                withdrawals[member_idx] = withdraw_amount
                
                new_row = pd.DataFrame([{
                    "Date": datetime.now().strftime("%Y-%m-%d"),
                    "Type": "出金",
                    "Total_Amount": withdraw_amount,
                    "Breakdown": ",".join(map(str, withdrawals)),
                    "Note": f"メンバー{target_member}による出金"
                }])
                conn.update(worksheet=selected_project, data=pd.concat([hist_df, new_row], ignore_index=True))
                
                # LINE送信
                if "line" in st.secrets:
                    msg = f"【出金通知】\n{selected_project}\n{target_member}が ${withdraw_amount:,.2f} を出金しました。"
                    send_line_message(st.secrets["line"]["channel_access_token"], st.secrets["line"]["user_id"], msg)
                
                st.success(f"{target_member} の出金を記録しました。元本が差し引かれます。")
                st.rerun()
            else:
                st.error("金額を入力してください。")

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
