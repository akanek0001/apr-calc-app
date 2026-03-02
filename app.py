import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import requests
import json
import re

# --- 1. ページ基本設定 ---
st.set_page_config(page_title="APR究極管理システム", layout="wide", page_icon="💎")

# --- 2. 汎用ユーティリティ ---
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

# --- 3. メインシステム ---
st.title("💎 APR資産運用管理システム Ultra")

try:
    # 接続設定 (ttl=0 でキャッシュを無効化し、常に最新のシートを読み込む)
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings", ttl=0)
    line_id_df = conn.read(worksheet="LineID", ttl=60)
    
    # プロジェクトの特定
    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクト選択", project_list)
    
    # 選択したプロジェクトの行インデックスを取得
    p_idx = settings_df[settings_df.iloc[:, 0] == selected_project].index[0]
    p_info = settings_df.loc[p_idx]
    
    # 既存データの展開
    num_people = int(to_f(p_info.iloc[1]))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    display_names = split_val(str(p_info.iloc[6]), num_people)

    # タブ作成
    tab1, tab2, tab3, tab4 = st.tabs(["📈 収益報告", "💸 入出金管理", "🚀 詳細分析", "👥 ユーザー追加・管理"])

    # --- タブ1〜3は既存のまま (中略) ---

    # --- ★タブ4: ユーザー追加機能 (ここが修正箇所) ---
    with tab4:
        st.subheader("👤 運用ユーザーの管理")
        
        # 現在のメンバー表を表示
        df_current = pd.DataFrame({
            "No": range(1, num_people + 1),
            "名前": display_names,
            "初期元本 ($)": base_principals,
            "配分比率": rate_list
        })
        st.table(df_current)
        
        st.markdown("---")
        st.subheader("➕ 新規メンバーをこのプロジェクトに追加")
        
        # 入力フォーム
        with st.form("new_user_form", clear_on_submit=True):
            add_name = st.text_input("メンバー名", placeholder="例：田中 太郎")
            add_principal = st.number_input("初期投資額 ($)", min_value=0.0, step=100.0)
            add_rate = st.number_input("収益配分比率 (通常は1.0)", value=1.0, step=0.1)
            
            submitted = st.form_submit_button("スプレッドシートへ登録")
            
            if submitted:
                if add_name:
                    # 1. 新しいリストを作成
                    new_names = display_names + [add_name]
                    new_principals = base_principals + [add_principal]
                    new_rates = rate_list + [add_rate]
                    new_count = len(new_names)
                    
                    # 2. settings_df の該当行を直接書き換え (列番号に注意)
                    # A:Project, B:Num, C:Total, D:Principals, E:Rates, F:Compound, G:Names
                    settings_df.at[p_idx, settings_df.columns[1]] = new_count
                    settings_df.at[p_idx, settings_df.columns[3]] = ",".join(map(str, new_principals))
                    settings_df.at[p_idx, settings_df.columns[4]] = ",".join(map(str, new_rates))
                    settings_df.at[p_idx, settings_df.columns[6]] = ",".join(new_names)
                    
                    # 3. スプレッドシートを更新
                    conn.update(worksheet="Settings", data=settings_df)
                    
                    st.success(f"✅ {add_name} 様を登録しました。反映をお待ちください...")
                    st.balloons()
                    st.rerun() # 画面を更新して最新リストを表示
                else:
                    st.error("名前を入力してください。")

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
