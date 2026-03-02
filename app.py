import streamlit as st
import pandas as pd
import requests
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate
from datetime import datetime
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide")

# --- 当時の正確なデータ処理関数 ---
def to_f(val):
    if pd.isna(val): return 0.0
    try:
        return float(str(val).replace(',','').replace('$','').replace('%','').strip())
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "": return ["-"] * n
    # カンマまたはスペースで分割
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "-")
    return items[:n]

# --- メインロジック ---
st.title("🏦 APR資産運用管理システム")

try:
    # 1. スプレッドシート読み込み (ヘッダーを1行目として認識)
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    # header=0 を明示し、空行を排除
    df = pd.read_csv(f"{base_url}/export?format=csv&gid=0", header=0).dropna(how='all')

    # 2. プロジェクト選択
    p_list = df.iloc[:, 0].dropna().unique().tolist()
    selected_p = st.sidebar.selectbox("プロジェクト選択", p_list)

    # 3. データの抽出 (成功していた時の正確な列インデックス)
    # A:0(Project), B:1(Num), D:3(Principals), E:4(Rates), G:6(Names)
    row = df[df.iloc[:, 0] == selected_p].iloc[0]
    
    num = int(to_f(row.iloc[1]))
    names = split_val(str(row.iloc[6]), num)
    principals = [to_f(x) for x in split_val(str(row.iloc[3]), num)]
    rates = [to_f(x) for x in split_val(str(row.iloc[4]), num)]

    # 4. 収益計算
    st.subheader(f"📊 {selected_p} 収益計算")
    apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 成功時の計算式
    yields = [(p * (apr / 100) * 0.77 * rates[i]) / 365 for i, p in enumerate(principals)]
    total_y = sum(yields)

    # 5. 表示
    res_df = pd.DataFrame({
        "メンバー": names,
        "元本 ($)": [f"{p:,.2f}" for p in principals],
        "分配比率": rates,
        "本日収益 ($)": [f"{y:,.4f}" for y in yields]
    })
    res_df.index = range(1, len(res_df) + 1)
    st.table(res_df)
    st.metric("総収益合計", f"${total_y:,.4f}")

    # 6. 通知セクション (画像は任意)
    st.markdown("---")
    uploaded_file = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])
    
    if st.button("🚀 LINE & Gmail 通知を一斉送信", type="primary"):
        with st.spinner("送信中..."):
            # メッセージ作成
            msg = f"🏦 【{selected_p}】 収益報告\n"
            msg += f"📅 {datetime.now().strftime('%Y/%m/%d %H:%M')}\n"
            msg += f"📈 本日APR: {apr}%\n"
            msg += "------------------\n"
            for i in range(num):
                msg += f"・{names[i]}: +${yields[i]:,.4f}\n"
            msg += "------------------\n"
            msg += f"💰 合計収益: +${total_y:,.4f}"

            # 画像がある場合のみImgBBへ
            if uploaded_file:
                img_res = requests.post(
                    "https://api.imgbb.com/1/upload",
                    data={"key": st.secrets["imgbb"]["api_key"]},
                    files={"image": uploaded_file.getvalue()}
                )
                if img_res.status_code == 200:
                    img_url = img_res.json()["data"]["url"]
                    msg += f"\n\n🖼 エビデンス画像:\n{img_url}"

            # LINE送信
            line_headers = {
                "Authorization": f"Bearer {st.secrets['line']['channel_access_token']}",
                "Content-Type": "application/json"
            }
            requests.post("https://api.line.me/v2/bot/message/broadcast", headers=line_headers, json={"messages": [{"type": "text", "text": msg}]})
            
            # Gmail送信
            g_conf = st.secrets["gmail"]
            mail = MIMEText(msg)
            mail['Subject'] = f"【{selected_p}】収益報告"
            mail['From'], mail['To'] = g_conf["user"], g_conf["user"]
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                smtp.login(g_conf["user"], g_conf["password"])
                smtp.send_message(mail)
            
            st.success("送信完了しました！")
            st.balloons()

except Exception as e:
    st.error(f"エラー: {e}")
