import streamlit as st
import pandas as pd
import requests
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate
from datetime import datetime
import re

# --- 1. ページ基本設定 ---
st.set_page_config(page_title="APR運用管理システム", layout="wide")

# --- 2. 当時のデータ処理関数 ---
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

# --- 3. メインロジック ---
st.title("🏦 APR資産運用管理システム")

try:
    # スプレッドシート読み込み (公開URLを使用)
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    df = pd.read_csv(f"{base_url}/export?format=csv&gid=0")

    # プロジェクト選択
    p_list = df.iloc[:, 0].dropna().unique().tolist()
    selected_p = st.sidebar.selectbox("プロジェクト選択", p_list)

    # 選択されたプロジェクトの行データを取得
    row = df[df.iloc[:, 0] == selected_p].iloc[0]
    
    # 【当時と同じ列インデックス】
    num = int(to_f(row.iloc[1])) # B列: 人数
    names = split_val(row.iloc[6], num) # G列: 名前
    principals = [to_f(x) for x in split_val(row.iloc[3], num)] # D列: 元本
    rates = [to_f(x) for x in split_val(row.iloc[4], num)]      # E列: 比率

    # 4. 収益計算セクション
    st.subheader(f"📊 {selected_p} 収益計算")
    apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 計算式: (元本 * APR% * 0.77 * 比率) / 365
    yields = [(p * (apr / 100) * 0.77 * rates[i]) / 365 for i, p in enumerate(principals)]
    total_y = sum(yields)

    # 結果表示テーブル
    res_df = pd.DataFrame({
        "メンバー": names,
        "元本 ($)": [f"{p:,.2f}" for p in principals],
        "分配比率": rates,
        "本日収益 ($)": [f"{y:,.4f}" for y in yields]
    })
    res_df.index = range(1, len(res_df) + 1)
    st.table(res_df)
    st.metric("総収益合計", f"${total_y:,.4f}")

    # 5. 【修正済】画像任意・LINE送信セクション
    st.markdown("---")
    uploaded_file = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])
    
    if st.button("🚀 LINE & Gmail 通知を一斉送信", type="primary"):
        with st.spinner("通知を送信中..."):
            # A. メッセージの土台作成
            msg = f"🏦 【{selected_p}】 収益報告\n"
            msg += f"📅 {datetime.now().strftime('%Y/%m/%d')}\n"
            msg += f"📈 本日APR: {apr}%\n"
            msg += "------------------\n"
            for i in range(num):
                msg += f"・{names[i]}: +${yields[i]:,.4f}\n"
            msg += "------------------\n"
            msg += f"💰 合計収益: +${total_y:,.4f}"

            # B. 画像がある場合のみImgBBへアップロードしてリンク追加
            if uploaded_file:
                img_res = requests.post(
                    "https://api.imgbb.com/1/upload",
                    data={"key": st.secrets["imgbb"]["api_key"]},
                    files={"image": uploaded_file.getvalue()}
                )
                if img_res.status_code == 200:
                    img_url = img_res.json()["data"]["url"]
                    msg += f"\n\n🖼 エビデンス画像:\n{img_url}"
                else:
                    st.warning("画像のアップロードに失敗しましたが、テキストのみ送信します。")

            # C. LINE送信
            line_headers = {
                "Authorization": f"Bearer {st.secrets['line']['channel_access_token']}",
                "Content-Type": "application/json"
            }
            line_payload = {"messages": [{"type": "text", "text": msg}]}
            requests.post("https://api.line.me/v2/bot/message/broadcast", headers=line_headers, json=line_payload)
            
            # D. Gmail送信
            g_conf = st.secrets["gmail"]
            mail = MIMEText(msg)
            mail['Subject'] = f"【{selected_p}】収益報告"
            mail['From'], mail['To'] = g_conf["user"], g_conf["user"]
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                smtp.login(g_conf["user"], g_conf["password"])
                smtp.send_message(mail)
            
            st.success("送信が完了しました！")
            st.balloons()

except Exception as e:
    st.error(f"エラー: {e}")
