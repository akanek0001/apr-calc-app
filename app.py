import streamlit as st
import smtplib
from email.utils import formatdate
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime

# --- ページ設定 ---
st.set_page_config(page_title="マルチプロジェクトAPR管理", page_icon="🏦")
st.title("🏦 プロジェクト別 APR報告 & 記録")

# --- Googleスプレッドシート接続 ---
try:
    conn = st.connection("gsheets", type=GSheetsConnection)
except Exception as e:
    st.error("Secretsまたはスプレッドシートへの接続を確認してください。")
    st.stop()

# --- プロジェクト設定の読み込み ---
try:
    settings_df = conn.read(worksheet="Settings")
    # すべての列名の前後スペースを消し、小文字に統一して判定しやすくする
    settings_df.columns = settings_df.columns.str.strip()
    
    # もし Project_Name という列名がなければ、1番左の列を使うようにする
    if "Project_Name" in settings_df.columns:
        project_col = "Project_Name"
    else:
        # 安全策：列名が何であれ、1番目の列をプロジェクト名として扱う
        project_col = settings_df.columns[0]
        
    project_list = settings_df[project_col].tolist()
except Exception as e:
    st.error(f"Settingsシートの読み込みに失敗しました: {e}")
    st.stop()

# --- サイドバー：プロジェクト切り替え ---
with st.sidebar:
    st.header("📁 プロジェクト選択")
    project_list = settings_df["Project_Name"].tolist()
    selected_project_name = st.selectbox("運用中のプロジェクトを選択", project_list)
    
    # 選択されたプロジェクトの情報を抽出
    p_info = settings_df[settings_df["Project_Name"] == selected_project_name].iloc[0]
    
    st.divider()
    st.header("📧 メール認証情報")
    # ここは共通の送信元情報をSecretsから取得
    sender_email = st.text_input("送信元Gmail", value=st.secrets.get("SENDER_EMAIL", ""))
    sender_password = st.text_input("アプリパスワード", type="password", value=st.secrets.get("SENDER_PASSWORD", ""))

# --- メイン画面：選択されたプロジェクトの情報を表示 ---
st.subheader(f"📍 現在のプロジェクト: {selected_project_name}")

col_a, col_b = st.columns(2)
with col_a:
    # 設定値からデフォルトを読み込みつつ、微調整も可能
    principal = st.number_input("運用元本 ($)", value=float(p_info["Principal"]), step=100.0)
with col_b:
    num_people = st.number_input("分配人数", value=int(p_info["Num_People"]), min_value=1)

today_apr = st.number_input("本日のAPR (%)", value=297.23, step=0.01)

# --- 計算 ---
total_daily_yield = principal * (today_apr / 100) / 365
split_amount = total_daily_yield / num_people

st.divider()
st.metric(label="本日の総収益", value=f"${total_daily_yield:,.2f}")
st.metric(label=f"1人あたりの配分 ({num_people}名)", value=f"${split_amount:,.2f}")

# 送信先メールアドレスの確認
recipient_emails = [e.strip() for e in str(p_info["Recipients"]).split(",")]
st.write(f"📩 送信先: `{', '.join(recipient_emails)}`")

# --- 実行ボタン ---
if st.button(f"{selected_project_name} の報告を実行"):
    try:
        # 1. メール送信
        subject = f"【{selected_project_name}】Profit Report: {today_apr}%"
        body = f"Project: {selected_project_name}\nTotal Profit: ${total_daily_yield:,.2f}\nSplit: ${split_amount:,.2f}\nAPR: {today_apr}%"
        full_message = f"From: {sender_email}\r\nTo: {', '.join(recipient_emails)}\r\nSubject: {subject}\r\nDate: {formatdate(localtime=True)}\r\n\r\n{body}"
        
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_emails, full_message.encode("utf-8", "ignore"))
        
        # 2. プロジェクト専用のシートに記録
        new_row = pd.DataFrame([{
            "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Principal": principal,
            "APR": today_apr,
            "Total_Yield": round(total_daily_yield, 2),
            "Split_Amount": round(split_amount, 2)
        }])
        
        # プロジェクト名と同じ名前のシートを読み書き
        try:
            existing_data = conn.read(worksheet=selected_project_name)
            updated_data = pd.concat([existing_data, new_row], ignore_index=True)
        except:
            updated_data = new_row
            
        conn.update(worksheet=selected_project_name, data=updated_data)
        
        st.success(f"✅ {selected_project_name} の記録と送信が完了しました！")
        st.balloons()
        
    except Exception as e:
        st.error(f"エラーが発生しました: {e}")
