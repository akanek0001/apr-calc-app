import streamlit as st
import pandas as pd
import requests
import json
import re
from datetime import datetime, timedelta

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- ユーティリティ ---
def to_f(val):
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == "nan": return 0.0
    try:
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == "nan": return ["-"] * n
    items = [x.strip() for x in re.split(r'[,\s\n\r]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "-")
    return items[:n]

st.title("🏦 APR資産運用管理システム")

try:
    # 1. スプレッドシート読み込み (URLはSecretsから)
    # 読み込み時にヘッダーを無視して、確実に全データを取得します
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    
    # 【修正点】Settingsシートを「ヘッダーなし」で読み込み、物理的な位置で判断します
    # gid=0 は通常一番左のタブです
    settings_df = pd.read_csv(f"{base_url}/export?format=csv&gid=0", header=None)

    # 🛠️ デバッグ用：読み込んだシートの先頭を表示（正常に動いたら削除してOK）
    if st.checkbox("【デバッグ】読み込みデータの確認"):
        st.write("読み込まれたデータの先頭5行:", settings_df.head())

    # 1行目が「Project_Name」などの見出しなら、それを除いた2行目以降をデータとします
    if "Project" in str(settings_df.iloc[0, 0]):
        data_df = settings_df.iloc[1:].reset_index(drop=True)
    else:
        data_df = settings_df

    # 2. プロジェクト選択 (A列 = 0)
    project_list = data_df.iloc[:, 0].dropna().unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    
    # 選択された行の取得
    p_info = data_df[data_df.iloc[:, 0] == selected_project].iloc[0]

    # --- 3. 物理的な列番号による抽出（以前の完璧な位置） ---
    # B:1(人数), D:3(元本リスト), E:4(比率リスト), G:6(メンバー名)
    num_people = int(to_f(p_info.iloc[1]))
    
    member_names = split_val(p_info.iloc[6], num_people)
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    
    # F:5(複利設定)
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"]

    # --- メイン画面表示 ---
    st.subheader(f"📊 {selected_project} 本日の計算")
    total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 計算 (手数料0.77係数)
    today_yields = [round((p * (total_apr * 0.77 * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(base_principals)]
    
    res_display = pd.DataFrame({
        "メンバー": member_names,
        "元本 ($)": [f"${p:,.2f}" for p in base_principals],
        "分配比率": rate_list,
        "本日収益 ($)": [f"${y:,.4f}" for y in today_yields]
    })
    res_display.index = range(1, len(res_display) + 1)
    st.table(res_display)

    # 送信ボタンなど（中略）
    if st.button("🚀 LINE一斉送信"):
        st.info("LINE送信ロジックを実行します...")
        # (ここに送信ロジック)

except Exception as e:
    st.error(f"システムエラー: {e}")
    st.info("【確認してください】スプレッドシートの1枚目のタブ（gid=0）が『Settings』になっていますか？")
