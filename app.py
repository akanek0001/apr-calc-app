import streamlit as st
import pandas as pd
import requests
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide")

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
    # 🔗 スプレッドシート読み込み (当時の設定を厳守)
    sheet_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    # header=0（1行目を項目名として読み飛ばす）が当時の成功設定です
    df = pd.read_csv(f"{sheet_url}/export?format=csv&gid=0", header=0)

    # プロジェクト選択 (A列)
    p_list = df.iloc[:, 0].dropna().unique().tolist()
    selected_p = st.sidebar.selectbox("プロジェクト選択", p_list)

    # データ抽出 (当時の物理インデックスを直撃)
    # A:0(Project), B:1(Num), D:3(Principals), E:4(Rates), G:6(Names)
    p_info = df[df.iloc[:, 0] == selected_p].iloc[0]
    num = int(to_f(p_info.iloc[1]))
    
    # ここが「名前」が正しく出る理由：G列(6)を直視
    names = split_val(str(p_info.iloc[6]), num)
    principals = [to_f(p) for p in split_val(str(p_info.iloc[3]), num)]
    rates = [to_f(r) for r in split_val(str(p_info.iloc[4]), num)]

    st.subheader(f"📊 {selected_p} 収益計算")
    apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 計算式 (元本 * APR% * 0.77 * 比率 / 365)
    yields = [(p * (apr / 100) * 0.77 * rates[i]) / 365 for i, p in enumerate(principals)]
    total_y = sum(yields)

    # テーブル表示
    res_df = pd.DataFrame({
        "メンバー": names,
        "元本 ($)": [f"{p:,.2f}" for p in principals],
        "分配比率": rates,
        "本日収益 ($)": [f"{y:,.4f}" for y in yields]
    })
    res_df.index = range(1, len(res_df) + 1)
    st.table(res_df)
    st.metric("総収益合計", f"${total_y:,.4f}")

    # 通知送信 (画像は「任意」)
    st.markdown("---")
    uploaded_file = st.file_uploader("エビデンス画像 (任意)", type=["png", "jpg", "jpeg"])
    
    if st.button("🚀 LINE・メール一斉送信", type="primary"):
        with st.spinner("送信中..."):
            msg = f"🏦 【{selected_p}】 収益報告\n"
            msg += f"📅 {datetime.now().strftime('%Y/%m/%d %H:%M')}\n"
            msg += f"📈 APR: {apr}%\n"
            msg += "-"*15 + "\n"
            for i in range(num):
                msg += f"・{names[i]}: +${yields[i]:,.4f}\n"
            msg += "-"*15 + "\n"
            msg += f"💰 合計: +${total_y:,.4f}"

            # 画像がある場合のみ実行
            if uploaded_file:
                img_res = requests.post(
                    "https://api.imgbb.com/1/upload",
                    data={"key": st.secrets["imgbb"]["api_key"]},
                    files={"image": uploaded_file.getvalue()}
                )
                if img_res.status_code == 200:
                    img_url = img_res.json()["data"]["url"]
                    msg += f"\n\n🖼 エビデンス:\n{img_url}"

            # LINE & Gmail 送信
            line_headers = {"Authorization": f"Bearer {st.secrets['line']['channel_access_token']}", "Content-Type": "application/json"}
            requests.post("https://api.line.me/v2/bot/message/broadcast", headers=line_headers, json={"messages": [{"type": "text", "text": msg}]})
            
            g_conf = st.secrets["gmail"]
            mail = MIMEText(msg)
            mail['Subject'], mail['From'], mail['To'] = f"【{selected_p}】収益報告", g_conf["user"], g_conf["user"]
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                smtp.login(g_conf["user"], g_conf["password"])
                smtp.send_message(mail)
                
            st.success("全ての送信が完了しました！")

except Exception as e:
    st.error(f"エラー: {e}")
