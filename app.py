import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")
st.title("🏦 APR管理システム（33%控除・実質分配版）")

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
    # 1. スプレッドシート接続（キャッシュ無効化）
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read(worksheet="Settings", ttl=0) 
    
    # 2. プロジェクト選択
    project_list = df.iloc[:, 0].dropna().astype(str).tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df[df.iloc[:, 0] == selected_project].iloc[0]

    # 3. 設定の読み込み
    raw_num = str(p_info.iloc[1]).strip()
    num_people = int(float(raw_num)) if raw_num and raw_num.replace('.','').isdigit() else 1
    
    email_list = split_val(p_info.iloc[2], num_people)
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    wallet_list = split_val(p_info.iloc[5], num_people)

    # 4. 履歴読み込みと複利計算
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
    st.subheader(f"📊 {selected_project} の運用状況")
    
    # 入力
    total_apr = st.number_input("本日の全体のAPR (%)", value=100.0, step=0.01)
    
    # 【重要】33%を差し引いた実質APRを計算のベースにする (100% - 33% = 67%)
    net_apr_factor = 0.67
    
    # 各個人の収益計算
    today_yields = [
        round((p * (total_apr * net_apr_factor * rate_list[i] / 100)) / 365, 4) 
        for i, p in enumerate(current_principals)
    ]
    
    st.info(f"💡 本日のAPR {total_apr}% から 33% を控除（実質 {total_apr * net_apr_factor:.2f}%）して分配計算しています。")

    c1, c2, c3 = st.columns(3)
    c1.metric("確定人数", f"{num_people} 名")
    c2.metric("運用総元本（複利込）", f"${sum(current_principals):,.2f}")
    c3.metric("本日の総分配額", f"${sum(today_yields):,.4f}")

    # 内訳確認用
    with st.expander("メンバー別の詳細を確認（33%控除後）"):
        for i in range(num_people):
            st.write(f"**人目 No.{i+1}**")
            st.write(f"- 元本: ${current_principals[i]:,.2f} / 分配額: ${today_yields[i]:,.4f}")
            st.write(f"- 送信先: {email_list[i]}")

    # --- 確定ボタン ---
    if st.button("収益を確定してメールを送信"):
        with st.spinner("処理中..."):
            # A. 履歴をスプレッドシートに保存
            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Total_Principal": round(sum(current_principals), 2),
                "Breakdown": ", ".join(map(str, today_yields)),
                "Paid_Flags": ",".join(["0"] * num_people)
            }])
            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            conn.update(worksheet=selected_project, data=updated_hist)
            
            # B. メール送信処理
            if "gmail" in st.secrets:
                try:
                    mail_user = st.secrets["gmail"]["user"]
                    mail_pass = st.secrets["gmail"]["password"]
                    
                    for i in range(num_people):
                        target_email = email_list[i]
                        if not target_email or "@" not in target_email:
                            continue

                        mail_content = f"""
{selected_project} の収益報告です（33%控除適用済み）。

■本日の全体のAPR: {total_apr}%
■あなたの本日の分配額: ${today_yields[i]:,.4f}
■現在の運用元本(複利込): ${current_principals[i]:,.2f}
■受取用Wallet: {wallet_list[i]}

※この収益は全体のAPRから33%を差し引いた後の金額です。
※このメールはシステムより自動送信されています。
"""
                        msg = MIMEText(mail_content, 'plain', 'utf-8')
                        msg['Subject'] = Header(f"【収益報告】{selected_project}", 'utf-8')
                        msg['From'] = mail_user
                        msg['To'] = target_email
                        
                        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                            smtp.login(mail_user, mail_pass)
                            smtp.send_message(msg)
                    
                    st.success(f"保存完了！{num_people}名にメールを送信しました。")
                except Exception as mail_err:
                    st.error(f"保存はできましたが、メール送信に失敗しました: {mail_err}")
            else:
                st.warning("保存しましたが、Secrets設定がないためメール送信はスキップされました。")
            
            st.rerun()

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
