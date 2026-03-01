import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", page_icon="🏦", layout="wide")

# --- 共通関数：データ洗浄 ---
def split_val(val, num):
    """B列の人数(num)を絶対とし、それ以上のデータは切り捨てる"""
    if pd.isna(val) or str(val).strip() == "":
        return ["0"] * num
    # カンマまたはスペースで分割
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    # 強制的に人数分でカット
    items = items[:num]
    # 足りない場合は補完
    while len(items) < num:
        items.append(items[-1] if items else "0")
    return items

def safe_float(val, default=0.0):
    try:
        clean_val = str(val).replace('%', '').replace('$', '').replace(',', '').strip()
        return float(clean_val) if clean_val else default
    except:
        return default

def safe_int(val, default=1):
    try:
        # 数字以外の文字（カンマなど）を消して整数化
        clean_val = re.sub(r'\D', '', str(val))
        return int(clean_val) if clean_val else default
    except:
        return default

# --- メイン処理 ---
st.title("🏦 APR管理システム (安定版)")

try:
    # 1. スプレッドシート接続
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings")
    
    # 2. プロジェクト選択 (A列)
    project_list = settings_df.iloc[:, 0].dropna().astype(str).tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    
    # 3. 選択された行のデータを取得
    p_info = settings_df[settings_df.iloc[:, 0] == selected_project].iloc[0]
    
    # --- 列名に頼らず「場所(列番号)」で取得 ---
    # 1列目(B):人数, 3列目(D):元本, 4列目(E):率, 5列目(F):Wallet, 2列目(C):メール
    #num_people = safe_int(p_info.iloc[1], 1)


    
    base_principals = [safe_float(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [safe_float(r, 1.0) for r in split_val(p_info.iloc[4], num_people)]
    wallet_list = split_val(p_info.iloc[5], num_people)
    recipients = split_val(p_info.iloc[2], num_people)
    
    # --- 履歴読み込み ---
    try:
        hist_data = conn.read(worksheet=selected_project)
        if not hist_data.empty:
            hist_data["Date"] = pd.to_datetime(hist_data["Date"])
    except:
        hist_data = pd.DataFrame()

    # --- 表示 ---
    st.subheader(f"📅 記録: {selected_project}")
    total_apr = st.number_input("全体のAPR (%)", value=100.0, step=0.01)

    current_principals = []
    today_yields = []

    for i in range(num_people):
        p_now = base_principals[i] # ひとまず単純計算
        current_principals.append(p_now)
        personal_apr = total_apr * rate_list[i]
        today_yields.append(round((p_now * (personal_apr / 100)) / 365, 4))

    c1, c2, c3 = st.columns(3)
    c1.metric("確定人数", f"{num_people}名")
    c2.metric("総運用元本", f"${sum(current_principals):,.2f}")
    c3.metric("本日の収益", f"${sum(today_yields):,.2f}")

    if st.button("収益を保存"):
        # 保存ロジックのみ実行
        new_row = pd.DataFrame([{
            "Date": datetime.now().strftime("%Y-%m-%d"),
            "Total_Principal": round(sum(current_principals), 2),
            "Breakdown": ", ".join(map(str, today_yields)),
            "Paid_Flags": ",".join(["0"] * num_people)
        }])
        updated_hist = pd.concat([hist_data, new_row], ignore_index=True)
        conn.update(worksheet=selected_project, data=updated_hist)
        st.success("保存しました。")
        st.rerun()

except Exception as e:
    st.error(f"読み込みエラー: {e}")
    st.info("スプレッドシートのタブ名や並び順を確認してください。")
