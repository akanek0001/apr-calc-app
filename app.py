import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import re
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate

# --- 1. ページ基本設定 ---
st.set_page_config(page_title="APR運用管理システム", layout="wide")

# --- 2. 便利関数群 ---
def to_f(val):
    if pd.isna(val): return 0.0
    try:
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "": return ["0"] * n
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

def upload_to_imgbb(file):
    url = "https://api.imgbb.com/1/upload"
    payload = {"key": st.secrets["imgbb"]["api_key"]}
    files = {"image": file.getvalue()}
    res = requests.post(url, payload, files=files)
    return res.json()["data"]["url"] if res.status_code == 200 else None

def send_gmail(subject, body):
    conf = st.secrets["gmail"]
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = conf["user"]
    msg['To'] = conf["user"]
    msg['Date'] = formatdate(localtime=True)
    
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(conf["user"], conf["password"])
        smtp.send_message(msg)

# --- 3. メインシステム ---
st.title("💰 APR運用管理システム (収益報告版)")

try:
    # スプレッドシート読み込み (CSV変換方式)
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    settings_df = pd.read_csv(f"{base_url}/export?format=csv&gid=0")
    
    # プロジェクト選択
    project_list = settings_df.iloc[:, 0].dropna().unique().tolist()
    selected_p = st.sidebar.selectbox("プロジェクト選択", project_list)
    
    # データ抽出
    p_info = settings_df[settings_df.iloc[:, 0] == selected_p].iloc[0]
    num = int(to_f(p_info.iloc[1]))
    names = split_val(p_info.iloc[6], num)
    principals = [to_f(p) for p in split_val(p_info.iloc[3], num)]
    rates = [to_f(r) for r in split_val(p_info.iloc[4], num)]

    # 入力セクション
    st.subheader("📊 今日の収益計算")
    col1, col2 = st.columns(2)
    with col1:
        apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    with col2:
        fee = 0.77
        st.info(f"手数料係数: {fee}")

    # 収益計算
    yields = [(p * (apr/100) * fee * rates[i]) / 365 for i, p in enumerate(principals)]
    
    # 結果表示
    res_df = pd.DataFrame({
        "名前": names,
        "元本 ($)": [f"{p:,.2f}" for p in principals],
        "本日収益 ($)": [f"{y:,.4f}" for y in yields]
    })
    st.table(res_df)

    # 報告セクション
    st.markdown("---")
    st.subheader("🚀 報告・通知")
    
    uploaded_file = st.file_uploader("エビデンス画像をアップロード", type=["png", "jpg", "jpeg"])
    
    if st.button("通知を一斉送信する", type="primary"):
        with st.spinner("処理中..."):
            # 1. メッセージ作成
            report_msg = f"🏦 【{selected_p}】 収益報告\n"
            report_msg += f"本日APR: {apr}%\n"
            report_msg += "------------------\n"
            for i in range(num):
                report_msg += f"・{names[i]}: +${yields[i]:,.4f}\n"
            report_msg += "------------------\n"
            
            img_url = ""
            if uploaded_file:
                img_url = upload_to_imgbb(uploaded_file)
                if img_url: report_msg += f"\n🖼 エビデンス:\n{img_url}"

            # 2. LINE送信
            line_headers = {
                "Authorization": f"Bearer {st.secrets['line']['channel_access_token']}",
                "Content-Type": "application/json"
            }
            line_payload = {"messages": [{"type": "text", "text": report_msg}]}
            requests.post("https://api.line.me/v2/bot/message/broadcast", headers=line_headers, json=line_payload)
            
            # 3. Gmail送信
            send_gmail(f"【{selected_p}】収益報告", report_msg)
            
            st.success("LINEおよびGmailへの送信が完了しました！")
            st.balloons()

except Exception as e:
    st.error(f"接続エラー: {e}")
    st.info("Secretsの設定とスプレッドシートの共有設定を確認してください。")
