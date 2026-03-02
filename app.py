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

# --- 2. 堅牢なデータ処理関数 ---
def to_f(val):
    if pd.isna(val): return 0.0
    try:
        s = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(s) if s else 0.0
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "": return ["-"] * n
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "-")
    return items[:n]

# --- 3. メインロジック ---
st.title("🏦 APR資産運用管理システム (高精度版)")

try:
    # スプレッドシート読み込み
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    df = pd.read_csv(f"{base_url}/export?format=csv&gid=0")

    # 列名の正規化（半角全角や空白を無視して探す）
    def find_col(keywords):
        for col in df.columns:
            if any(k in col for k in keywords): return col
        return None

    # プロジェクト選択
    p_col = find_col(["Project", "プロジェクト"]) or df.columns[0]
    p_list = df[p_col].dropna().unique().tolist()
    selected_p = st.sidebar.selectbox("プロジェクト選択", p_list)

    # 選択行のデータ取得
    row = df[df[p_col] == selected_p].iloc[0]

    # 自動列解析（列番号ではなく、意味で取得する）
    num = int(to_f(row[find_col(["Num", "人数"]) or df.columns[1]]))
    names = split_val(row[find_col(["Names", "名前"]) or df.columns[6]], num)
    principals = [to_f(x) for x in split_val(row[find_col(["Principals", "元本"]) or df.columns[3]], num)]
    rates = [to_f(x) for x in split_val(row[find_col(["Rates", "比率"]) or df.columns[4]], num)]

    # 4. 計算入力
    st.subheader(f"📊 {selected_p} 収益報告作成")
    apr = st.number_input("本日のAPR (%)", value=100.0, step=0.01, format="%.2f")
    
    # 収益計算 (元本 * APR% * 0.77 * 比率 / 365)
    yields = [(p * (apr / 100) * 0.77 * rates[i]) / 365 for i, p in enumerate(principals)]
    total_y = sum(yields)

    # 5. 結果表示（視認性を向上）
    res_df = pd.DataFrame({
        "No": range(1, num + 1),
        "メンバー": names,
        "元本 ($)": [f"{p:,.2f}" for p in principals],
        "分配比率": rates,
        "本日収益 ($)": [f"{y:,.4f}" for y in yields]
    }).set_index("No")
    
    st.table(res_df)
    st.metric("総収益合計", f"${total_y:,.4f}")

    # 6. 通知セクション
    st.markdown("---")
    uploaded_file = st.file_uploader("エビデンス画像(任意)", type=["png", "jpg", "jpeg"])
    
    if st.button("🚀 LINE & Gmail 通知を一斉送信", type="primary"):
        with st.spinner("送信中..."):
            msg = f"🏦 【{selected_p}】 収益報告\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n📈 本日APR: {apr}%\n"
            msg += "-"*18 + "\n"
            for i in range(num):
                msg += f"・{names[i]}: +${yields[i]:,.4f}\n"
            msg += "-"*18 + "\n"
            msg += f"💰 合計収益: +${total_y:,.4f}"

            # ImgBBアップロード
            if uploaded_file:
                res = requests.post("https://api.imgbb.com/1/upload", {"key": st.secrets["imgbb"]["api_key"]}, files={"image": uploaded_file.getvalue()})
                if res.status_code == 200:
                    msg += f"\n\n🖼 エビデンス:\n{res.json()['data']['url']}"

            # LINE送信
            requests.post("https://api.line.me/v2/bot/message/broadcast", 
                          headers={"Authorization": f"Bearer {st.secrets['line']['channel_access_token']}", "Content-Type": "application/json"},
                          json={"messages": [{"type": "text", "text": msg}]})
            
            # Gmail送信
            conf = st.secrets["gmail"]
            mail = MIMEText(msg)
            mail['Subject'], mail['From'], mail['To'] = f"【{selected_p}】収益報告", conf["user"], conf["user"]
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                smtp.login(conf["user"], conf["password"])
                smtp.send_message(mail)
            
            st.success("通知送信が完了しました！")
            st.balloons()

except Exception as e:
    st.error(f"致命的エラー: {e}")
    st.info("スプレッドシートの形式を確認してください。")
