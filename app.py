import streamlit as st
import pandas as pd
import requests
import json
import re
from datetime import datetime

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide")

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
    # 1. スプレッドシート読み込み (header=None にして1行目から全てデータとして扱う)
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    df_raw = pd.read_csv(f"{base_url}/export?format=csv&gid=0", header=None)

    # 1行目が項目名（Project_Name等）なら1行飛ばす。そうでなければそのまま使う。
    if "Project" in str(df_raw.iloc[0, 0]):
        data_df = df_raw.iloc[1:].reset_index(drop=True)
    else:
        data_df = df_raw

    # 2. プロジェクト選択 (A列/インデックス0)
    project_list = data_df.iloc[:, 0].dropna().unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    
    # 選択されたプロジェクトの行を抽出
    p_info = data_df[data_df.iloc[:, 0] == selected_project].iloc[0]

    # --- 3. 物理的な列番号で抽出（あなたのシートの並び順） ---
    # B:1(人数), D:3(元本), E:4(比率), F:5(複利), G:6(名前)
    num_people = int(to_f(p_info.iloc[1]))
    member_names = split_val(p_info.iloc[6], num_people)
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"]

    # --- 表示・計算 ---
    st.subheader(f"📊 {selected_project} 本日の計算")
    total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 手数料 0.77 係数
    today_yields = [round((p * (total_apr * 0.77 * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(base_principals)]
    
    res_display = pd.DataFrame({
        "メンバー": member_names,
        "元本 ($)": [f"${p:,.2f}" for p in base_principals],
        "分配比率": rate_list,
        "本日収益 ($)": [f"${y:,.4f}" for y in today_yields]
    })
    res_display.index = range(1, len(res_display) + 1)
    st.table(res_display)
    st.metric("総収益合計", f"${sum(today_yields):,.4f}")

except Exception as e:
    st.error(f"読み込みエラー: {e}")
    st.info("スプレッドシートの1枚目のタブ（gid=0）が正しい内容か、URLが間違っていないか確認してください。")
