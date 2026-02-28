import streamlit as st
import smtplib
from email.utils import formatdate

# --- ページ設定 ---
st.set_page_config(page_title="APR分配報告ツール", page_icon="💰")
st.title("💰 APR収益分配・メール報告")
st.write("今日のAPRを入力して、3人にメールで報告します。")

# # --- 設定項目（Secretsから自動読み込み） ---
with st.sidebar:
    st.header("メール送信設定")
    # Secretsがあればそれを使い、なければ空欄にする設定
    default_sender = st.secrets.get("SENDER_EMAIL", "")
    default_password = st.secrets.get("SENDER_PASSWORD", "")
    default_recipients = st.secrets.get("RECIPIENTS", "person1@example.com, person2@example.com, person3@example.com")

    sender_email = st.text_input("あなたのGmailアドレス", value=default_sender)
    sender_password = st.text_input("アプリパスワード (16桁)", value=default_password, type="password")
    emails_input = st.text_area("報告先メールアドレス", value=default_recipients)

# --- メイン入力エリア ---
st.subheader("計算データ")
col1, col2 = st.columns(2)

with col1:
    principal = st.number_input("運用元本 ($)", value=14148.0, step=1.0)
with col2:
    today_apr = st.number_input("本日のAPR (%)", value=297.23, step=0.01)

# 報告先のメールアドレス（デフォルトで3人分）
st.subheader("報告先メールアドレス")
emails_input = st.text_area("カンマ区切りで3人分入力してください", 
                            value="person1@example.com, person2@example.com, person3@example.com")
recipient_emails = [e.strip() for e in emails_input.split(",")]

# --- 計算ロジック ---
total_daily_yield = principal * (today_apr / 100) / 365
split_amount = total_daily_yield / 3

# 結果のプレビュー
st.divider()
st.write("### 送信内容のプレビュー")
st.info(f"全体の本日収益: **${total_daily_yield:,.2f}**\n\n1人あたりの分配額: **${split_amount:,.2f}**")

# --- 送信ボタン ---
if st.button("この内容で3人にメール送信"):
    if not sender_email or not sender_password:
        st.error("送信元の設定（サイドバー）を入力してください。")
    elif len(recipient_emails) < 1:
        st.error("報告先のメールアドレスを入力してください。")
    else:
        try:
            # 先ほど成功した「確実なメール構造」を再現
            subject = f"Profit Report: {today_apr}%"
            body = f"Daily Profit Report\n\nTotal: ${total_daily_yield:,.2f}\nEach: ${split_amount:,.2f}\nAPR: {today_apr}%"
            
            # 手紙の組み立て（ヘッダーと本文を自力で結合）
            full_message = f"From: {sender_email}\r\n"
            full_message += f"To: {', '.join(recipient_emails)}\r\n"
            full_message += f"Subject: {subject}\r\n"
            full_message += f"Date: {formatdate(localtime=True)}\r\n"
            full_message += "\r\n"
            full_message += body

            # 送信実行
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, recipient_emails, full_message.encode("ascii", "ignore"))
            
            st.success("✅ 正常にメールを送信しました！")
            st.balloons() # お祝いの風船
            
        except Exception as e:
            st.error(f"エラーが発生しました: {e}")
