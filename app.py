import streamlit as st
import smtplib
from email.utils import formatdate
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime

# --- ページ設定 ---
st.set_page_config(page_title="APR分配報告ツール", page_icon="💰")
st.title("💰 APR収益分配・報告 & 記録")

# --- Googleスプレッドシート接続 ---
try:
    conn = st.connection("gsheets", type=GSheetsConnection)
except Exception as e:
    st.error("Googleスプレッドシートへの接続に失敗しました。Secretsの設定を確認してください。")
    st.stop()

# --- 設定項目（Secretsから自動読み込み） ---
with st.sidebar:
    st.header("メール送信設定")
    default_sender = st.secrets.get("SENDER_EMAIL", "")
    default_password = st.secrets.get("SENDER_PASSWORD", "")
    default_recipients = st.secrets.get("RECIPIENTS", "person1@example.com, person2@example.com, person3@example.com")

    sender_email = st.text_input("あなたのGmailアドレス", value=default_sender)
    sender_password = st.text_input("アプリパスワード", value=default_password, type="password")
    emails_input = st.text_area("報告先メールアドレス", value=default_recipients)
    recipient_emails = [e.strip() for e in emails_input.split(",")]

# --- メイン入力エリア ---
st.subheader("計算データ")
col1, col2 = st.columns(2)
with col1:
    principal = st.number_input("運用元本 ($)", value=14148.0, step=1.0)
with col2:
    today_apr = st.number_input("本日のAPR (%)", value=297.23, step=0.01)

# --- 計算ロジック ---
total_daily_yield = principal * (today_apr / 100) / 365
split_amount = total_daily_yield / 3

st.divider()
st.write("### 送信・記録内容のプレビュー")
st.info(f"全体の本日収益: **${total_daily_yield:,.2f}**\n\n1人あたりの分配額: **${split_amount:,.2f}**")

# --- 送信＆記録ボタン ---
if st.button("メール送信 ＆ シートに記録"):
    try:
        # 1. メール送信
        subject = f"Profit Report: {today_apr}%"
        body = f"Daily Profit Report\n\nTotal: ${total_daily_yield:,.2f}\nEach: ${split_amount:,.2f}\nAPR: {today_apr}%"
        full_message = f"From: {sender_email}\r\nTo: {', '.join(recipient_emails)}\r\nSubject: {subject}\r\nDate: {formatdate(localtime=True)}\r\n\r\n{body}"

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_emails, full_message.encode("ascii", "ignore"))
        
        # 2. スプレッドシート記録
        new_row = pd.DataFrame([{
            "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "APR": today_apr,
            "Total_Yield": round(total_daily_yield, 2),
            "Split_Amount": round(split_amount, 2)
        }])

        # 既存データの取得（空の場合は新規作成）
        try:
            existing_data = conn.read()
            updated_data = pd.concat([existing_data, new_row], ignore_index=True)
        except:
            updated_data = new_row
        
        conn.update(data=updated_data)

        st.success("✅ メール送信とシートへの記録が完了しました！")
        st.balloons()
        
    except Exception as e:
        st.error(f"エラーが発生しました: {e}")
