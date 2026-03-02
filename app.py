import streamlit as st
import pandas as pd
import requests
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate
import re
from datetime import datetime

# --- 1. ページ基本設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide")

# --- 2. 補助関数 ---
def to_f(val):
    if pd.isna(val): return 0.0
    try:
        # カンマや記号を除去して数値化
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "": return ["0"] * n
    # カンマまたはスペースで分割
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

def upload_to_imgbb(file):
    url = "https://api.imgbb.com/1/upload"
    payload = {"key": st.secrets["imgbb"]["api_key"]}
    files = {"image": file.getvalue()}
    try:
        res = requests.post(url, payload, files=files)
        return res.json()["data"]["url"]
    except:
        return None

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

# --- 3. メイン処理 ---
st.title("🏦 APR資産運用管理システム")

try:
    # スプレッドシート読み込み（CSV変換方式）
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    # Settingsシート (gid=0)
    settings_df = pd.read_csv(f"{base_url}/export?format=csv&gid=0")
    
    # プロジェクトリスト取得
    project_list = settings_df.iloc[:, 0].dropna().unique().tolist()
    selected_p = st.sidebar.selectbox("プロジェクト選択", project_list)
    
    # 選択されたプロジェクトの行を特定
    p_info = settings_df[settings_df.iloc[:, 0] == selected_p].iloc[0]
    
    # 各列のデータを取得（以前の仕様に準拠）
    # 1:人数, 2:合計, 3:元本(リスト), 4:比率(リスト), 5:複利フラグ, 6:名前(リスト)
    num = int(to_f(p_info.iloc[1]))
    total_amount = to_f(p_info.iloc[2])
    principals = [to_f(p) for p in split_val(p_info.iloc[3], num)]
    rates = [to_f(r) for r in split_val(p_info.iloc[4], num)]
    is_compound = str(p_info.iloc[5]).lower() == "true"
    names = split_val(p_info.iloc[6], num)

    # 収益入力セクション
    st.subheader(f"📊 {selected_p} 収益計算")
    col1, col2 = st.columns(2)
    with col1:
        apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    with col2:
        fee_rate = 0.77 # 固定係数
        st.write(f"運用タイプ: {'複利モード' if is_compound else '単利モード'}")

    # 収益計算ロジック
    # 収益 = (元本 * (APR / 100) * 0.77 * 分配比率) / 365
    daily_yields = [(p * (apr / 100) * fee_rate * rates[i]) / 365 for i, p in enumerate(principals)]
    total_yield = sum(daily_yields)

    # 結果テーブル表示
    res_df = pd.DataFrame({
        "メンバー": names,
        "元本 ($)": [f"{p:,.2f}" for p in principals],
        "分配比率": rates,
        "本日収益 ($)": [f"{y:,.4f}" for y in daily_yields]
    })
    st.table(res_df)
    st.metric("総収益", f"${total_yield:,.4f}")

    # 報告・送信セクション
    st.markdown("---")
    uploaded_file = st.file_uploader("エビデンス画像をアップロード", type=["png", "jpg", "jpeg"])
    
    if st.button("🚀 LINE & Gmail 通知送信", type="primary"):
        with st.spinner("送信中..."):
            # メッセージ構築
            msg = f"🏦 【{selected_p}】 収益報告\n"
            msg += f"📅 {datetime.now().strftime('%Y/%m/%d')}\n"
            msg += f"📈 本日APR: {apr}%\n"
            msg += "------------------\n"
            for i in range(num):
                msg += f"・{names[i]}: +${daily_yields[i]:,.4f}\n"
            msg += "------------------\n"
            msg += f"💰 合計収益: +${total_yield:,.4f}"

            # 画像アップロード
            img_url = upload_to_imgbb(uploaded_file) if uploaded_file else ""
            if img_url:
                msg += f"\n\n🖼 エビデンス画像:\n{img_url}"

            # LINE送信 (Broadcast API)
            line_headers = {
                "Authorization": f"Bearer {st.secrets['line']['channel_access_token']}",
                "Content-Type": "application/json"
            }
            line_payload = {"messages": [{"type": "text", "text": msg}]}
            requests.post("https://api.line.me/v2/bot/message/broadcast", headers=line_headers, json=line_payload)
            
            # Gmail送信
            send_gmail(f"【{selected_p}】収益報告", msg)
            
            st.success("LINEおよびGmailへの送信が完了しました！")
            st.balloons()

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
