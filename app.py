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

# --- ユーティリティ関数 ---
def to_f(val):
    if pd.isna(val): return 0.0
    try:
        # カンマ、ドル記号、%を消して数値へ
        s = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(s) if s else 0.0
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "": return ["0"] * n
    # カンマまたはスペースで分割
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    # 要素が足りない場合は最後の要素で埋める
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

# --- メインロジック ---
st.title("🏦 APR資産運用管理システム")

try:
    # 1. スプレッドシート読み込み (CSV方式)
    # SecretsからURLを取得
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    # Settingsシート(gid=0)を読み込み
    settings_df = pd.read_csv(f"{base_url}/export?format=csv&gid=0")
    
    # 2. プロジェクト選択
    p_list = settings_df.iloc[:, 0].dropna().unique().tolist()
    selected_p = st.sidebar.selectbox("プロジェクト選択", p_list)
    
    # 3. データの抽出 (スプレッドシートの列構造に厳密に合わせる)
    # [0:Project, 1:人数, 2:合計, 3:元本リスト, 4:比率リスト, 5:複利, 6:名前リスト]
    row = settings_df[settings_df.iloc[:, 0] == selected_p].iloc[0]
    
    num = int(to_f(row.iloc[1])) # B列: 人数
    # カンマ区切りの文字列をリストに分解
    names_list = split_val(row.iloc[6], num)       # G列: 名前
    principal_list = [to_f(x) for x in split_val(row.iloc[3], num)] # D列: 元本
    rate_list = [to_f(x) for x in split_val(row.iloc[4], num)]      # E列: 比率
    is_compound = str(row.iloc[5]).lower() == "true" # F列: 複利

    # 4. 収益計算入力
    st.subheader(f"📊 {selected_p} 収益計算")
    st.write(f"運用タイプ: {'複利モード' if is_compound else '単利モード'}")
    
    apr = st.number_input("本日のAPR (%)", value=100.0, step=0.01, format="%.2f")
    
    # 収益計算ロジック
    # 収益 = (各元本 * (APR/100) * 0.77 * 各比率) / 365
    fee_factor = 0.77
    yields = [(p * (apr / 100) * fee_factor * rate_list[i]) / 365 for i, p in enumerate(principal_list)]
    total_yield = sum(yields)

    # 5. 表示用データフレーム作成
    res_df = pd.DataFrame({
        "メンバー": names_list,
        "元本 ($)": [f"{p:,.2f}" for p in principal_list],
        "分配比率": rate_list,
        "本日収益 ($)": [f"{y:,.4f}" for y in yields]
    })
    
    # インデックスを1からにする
    res_df.index = range(1, len(res_df) + 1)
    
    st.table(res_df)
    st.metric("総収益合計", f"${total_yield:,.4f}")

    # 6. 通知セクション
    st.markdown("---")
    uploaded_file = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])
    
    if st.button("🚀 LINE & Gmail 通知を一斉送信", type="primary"):
        with st.spinner("送信中..."):
            # メッセージ作成
            msg = f"🏦 【{selected_p}】 収益報告\n"
            msg += f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            msg += f"📈 本日APR: {apr}%\n"
            msg += "------------------\n"
            for i in range(num):
                msg += f"・{names_list[i]}: +${yields[i]:,.4f}\n"
            msg += "------------------\n"
            msg += f"💰 合計収益: +${total_yield:,.4f}"

            # ImgBBアップロード処理
            img_url = ""
            if uploaded_file:
                try:
                    img_res = requests.post(
                        "https://api.imgbb.com/1/upload", 
                        {"key": st.secrets["imgbb"]["api_key"]}, 
                        files={"image": uploaded_file.getvalue()}
                    )
                    if img_res.status_code == 200:
                        img_url = img_res.json()["data"]["url"]
                        msg += f"\n\n🖼 エビデンス画像:\n{img_url}"
                except:
                    st.warning("画像のアップロードに失敗しましたが、メッセージのみ送信します。")

            # LINE送信
            line_headers = {
                "Authorization": f"Bearer {st.secrets['line']['channel_access_token']}",
                "Content-Type": "application/json"
            }
            requests.post("https://api.line.me/v2/bot/message/broadcast", 
                          headers=line_headers, 
                          json={"messages": [{"type": "text", "text": msg}]})
            
            # Gmail送信
            g_conf = st.secrets["gmail"]
            mail = MIMEText(msg)
            mail['Subject'] = f"【{selected_p}】収益報告"
            mail['From'] = g_conf["user"]
            mail['To'] = g_conf["user"]
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                smtp.login(g_conf["user"], g_conf["password"])
                smtp.send_message(mail)
            
            st.success("LINE通知とメール送信が完了しました！")
            st.balloons()

except Exception as e:
    st.error(f"実行エラー: {e}")
    st.info("スプレッドシートの列が [Project, 人数, 合計元本, 元本リスト, 比率リスト, 複利, 名前リスト] の順になっているか確認してください。")
