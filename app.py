import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime, timedelta
import requests
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム(安定版)", layout="wide")

# --- ユーティリティ ---
def to_f(val):
    if pd.isna(val): return 0.0
    try:
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "": return ["0"] * n
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

st.title("🏦 APR資産運用管理・ユーザー追加機能")

try:
    # データ接続 (キャッシュなしで最新を読み込み)
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings", ttl=0)
    
    if settings_df.empty:
        st.error("スプレッドシートの読み込みに失敗しました。URLを確認してください。")
        st.stop()

    # プロジェクト選択
    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクト選択", project_list)
    
    # 行番号の特定
    p_idx = settings_df[settings_df.iloc[:, 0] == selected_project].index[0]
    p_info = settings_df.loc[p_idx]
    
    # 現データの取得
    num_people = int(to_f(p_info.iloc[1]))
    base_principals = split_val(p_info.iloc[3], num_people)
    rate_list = split_val(p_info.iloc[4], num_people)
    display_names = split_val(str(p_info.iloc[6]), num_people)

    # タブ作成
    tab1, tab2 = st.tabs(["📋 メンバー一覧", "👥 ユーザー追加"])

    with tab1:
        st.write(f"### プロジェクト: {selected_project}")
        st.table(pd.DataFrame({
            "名前": display_names,
            "初期元本": base_principals,
            "配分比率": rate_list
        }))

    with tab2:
        st.subheader("➕ 新規メンバーの追加登録")
        with st.form("add_user_form", clear_on_submit=True):
            new_name = st.text_input("メンバー名")
            new_prin = st.number_input("元本 ($)", min_value=0.0, step=100.0)
            new_rate = st.number_input("比率", value=1.0, step=0.1)
            
            if st.form_submit_button("スプレッドシートを更新"):
                if new_name:
                    # データの結合
                    upd_names = display_names + [new_name]
                    upd_prins = base_principals + [str(new_prin)]
                    upd_rates = rate_list + [str(new_rate)]
                    
                    # 1行丸ごと更新
                    settings_df.at[p_idx, settings_df.columns[1]] = len(upd_names)
                    settings_df.at[p_idx, settings_df.columns[3]] = ",".join(upd_prins)
                    settings_df.at[p_idx, settings_df.columns[4]] = ",".join(upd_rates)
                    settings_df.at[p_idx, settings_df.columns[6]] = ",".join(upd_names)
                    
                    # 保存
                    conn.update(worksheet="Settings", data=settings_df)
                    st.success(f"{new_name} 様を追加しました！")
                    st.rerun()
                else:
                    st.error("名前を入力してください。")

except Exception as e:
    st.error(f"エラー: {e}")
