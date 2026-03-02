import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide")
st.title("🏦 APR管理システム（フル機能版）")

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

# --- メインロジック ---
try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read(worksheet="Settings")
    
    project_list = df.iloc[:, 0].dropna().astype(str).tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df[df.iloc[:, 0] == selected_project].iloc[0]

    # 設定の読み込み
    raw_num = str(p_info.iloc[1]).strip()
    num_people = int(float(raw_num)) if raw_num and raw_num.replace('.','').isdigit() else 1
    
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    email_list = split_val(p_info.iloc[2], num_people)
    wallet_list = split_val(p_info.iloc[5], num_people)

    # 履歴の読み込み（複利計算用）
    try:
        hist_df = conn.read(worksheet=selected_project)
    except:
        hist_df = pd.DataFrame()

    # 複利計算ロジック
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
    st.subheader(f"📊 {selected_project} の運用状況")
    total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.01)

    today_yields = [round((p * (total_apr * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(current_principals)]
    
    c1, c2, c3 = st.columns(3)
    c1.metric("確定人数", f"{num_people} 名")
    c2.metric("運用総元本", f"${sum(current_principals):,.2f}")
    c3.metric("本日の総収益", f"${sum(today_yields):,.4f}")

    if st.button("収益を確定してメール送信"):
        # 1. 履歴保存
        new_row = pd.DataFrame([{
            "Date": datetime.now().strftime("%Y-%m-%d"),
            "Total_Principal": round(sum(current_principals), 2),
            "Breakdown": ", ".join(map(str, today_yields)),
            "Paid_Flags": ",".join(["0"] * num_people)
        }])
        updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
        conn.update(worksheet=selected_project, data=updated_hist)
        
        # 2. メール送信処理
        if "gmail" in st.secrets:
            mail_user = st.secrets["gmail"]["user"]
            mail_pass = st.secrets["gmail"]["password"]
            
            for i in range(num_people):
                msg = MIMEText(f"{selected_project}の収益報告です。\n\n本日の収益: ${today_yields[i]}\n現在の運用元本: ${current_principals[i]}\nWallet: {wallet_list[i]}")
                msg['Subject'] = Header(f"【収益報告】{selected_project}", 'utf-8')
                msg['From'] = mail_user
                msg['To'] = email_list[i]
                
                with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                    smtp.login(mail_user, mail_pass)
                    smtp.send_message(msg)
            st.success("履歴を保存し、メールを送信しました！")
        else:
            st.warning("履歴は保存しましたが、メール設定(Secrets)がないため送信をスキップしました。")
        st.rerun()

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
