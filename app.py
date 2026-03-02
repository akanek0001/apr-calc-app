import streamlit as st
import pandas as pd
import requests
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate
from datetime import datetime
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR運用管理システム", layout="wide")

# --- 補助関数 ---
def to_f(val):
    if pd.isna(val): return 0.0
    try:
        return float(str(val).replace(',','').replace('$','').replace('%','').strip())
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "": return ["-"] * n
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "-")
    return items[:n]

# --- メインロジック ---
st.title("💰 APR運用管理システム")

try:
    # スプレッドシート読み込み
    sheet_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    df = pd.read_csv(f"{sheet_url}/export?format=csv&gid=0")
    
    # プロジェクト選択
    p_list = df.iloc[:, 0].dropna().unique().tolist()
    selected_p = st.sidebar.selectbox("プロジェクト選択", p_list)
    
    # データ抽出
    p_info = df[df.iloc[:, 0] == selected_p].iloc[0]
    num = int(to_f(p_info.iloc[1]))
    names = split_val(p_info.iloc[6], num)
    principals = [to_f(p) for p in split_val(p_info.iloc[3], num)]
    rates = [to_f(r) for r in split_val(p_info.iloc[4], num)]

    st.subheader("📊 収益計算")
    apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    fee = 0.77
    
    yields = [(p * (apr/100) * fee * rates[i]) / 365 for i, p in enumerate(principals)]
    
    res_df = pd.DataFrame({
        "名前": names,
        "元本 ($)": [f"{p:,.2f}" for p in principals],
        "本日収益 ($)": [f"{y:,.4f}" for y in yields]
    })
    st.table(res_df)

    st.markdown("---")
    uploaded_file = st.file_uploader("エビデンス画像", type=["png", "jpg", "jpeg"])
    
    if st.button("LINE・メール一斉送信", type="primary"):
        with st.spinner("送信中..."):
            msg = f"🏦 【{selected_p}】 収益報告\nAPR: {apr}%\n" + "-"*15 + "\n"
            for i in range(num):
                msg += f"・{names[i]}: +${yields[i]:,.4f}\n"
            
            # 画像の処理
            img_url = ""
            if uploaded_file:
                url = "https://api.imgbb.com/1/upload"
                payload = {"key": st.secrets["imgbb"]["api_key"]}
                files = {"image": uploaded_file.getvalue()}
                res = requests.post(url, payload, files=files)
                if res.status_code == 200:
                    img_url = res.json()["data"]["url"]
                    msg += f"\n\n🖼 エビデンス:\n{img_url}"

            # LINE送信
            requests.post("https://api.line.me/v2/bot/message/broadcast", 
                          headers={"Authorization": f"Bearer {st.secrets['line']['channel_access_token']}", "Content-Type": "application/json"},
                          json={"messages": [{"type": "text", "text": msg}]})
            
            # Gmail送信
            conf = st.secrets["gmail"]
            mail = MIMEText(msg)
            mail['Subject'] = f"【{selected_p}】収益報告"
            mail['From'] = conf["user"]
            mail['To'] = conf["user"]
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                smtp.login(conf["user"], conf["password"])
                smtp.send_message(mail)
                
            st.success("送信完了しました！")
            st.balloons()

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
