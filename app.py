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
    # 接続設定 (最新の状態を取得)
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings", ttl=0)
    line_id_df = conn.read(worksheet="LineID", ttl=60)
    
    if settings_df.empty:
        st.error("Settingsシートが空、または読み込めません。")
        st.stop()

    # プロジェクト選択
    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクト選択", project_list)
    
    # 選択したプロジェクトの行インデックスとデータ取得
    p_idx = settings_df[settings_df.iloc[:, 0] == selected_project].index[0]
    p_info = settings_df.loc[p_idx]
    
    # 既存データの展開
    num_people = int(to_f(p_info.iloc[1]))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    display_names = split_val(str(p_info.iloc[6]), num_people)

    # 履歴データの読み込み
    try:
        hist_df = conn.read(worksheet=selected_project, ttl=0)
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # タブ作成
    tab1, tab2, tab3, tab4 = st.tabs(["📈 収益報告", "💸 入出金管理", "🚀 詳細分析", "👥 ユーザー追加・管理"])

    # --- タブ1〜3は前回のロジックを継続 (詳細は省略可) ---
    # (中略: 収益計算やグラフ表示ロジック)

    # --- ★タブ4: ユーザー追加・管理 (確実な更新ロジック) ---
    with tab4:
        st.subheader("👤 運用ユーザーの管理")
        
        # 現在のメンバーを表示
        st.write("📋 現在の登録メンバー")
        member_summary = pd.DataFrame({
            "名前": display_names,
            "初期元本 ($)": base_principals,
            "配分比率": rate_list
        })
        st.table(member_summary)
        
        st.markdown("---")
        st.subheader("➕ 新規メンバーを追加登録")
        
        with st.form("new_user_registration", clear_on_submit=True):
            new_name = st.text_input("メンバー名", placeholder="例：山田 太郎")
            new_principal = st.number_input("初期投資額 ($)", min_value=0.0, step=100.0)
            new_rate = st.number_input("配分比率 (通常1.0)", value=1.0, step=0.1)
            
            save_btn = st.form_submit_button("スプレッドシートを更新して保存")
            
            if save_btn:
                if new_name:
                    # 新しいデータ配列の作成
                    upd_names = display_names + [new_name]
                    upd_principals = base_principals + [new_principal]
                    upd_rates = rate_list + [new_rate]
                    upd_count = len(upd_names)
                    
                    # Settingsシートの該当行を更新
                    # A:Project, B:Num, C:Total, D:Principals, E:Rates, F:Compound, G:Names
                    settings_df.at[p_idx, settings_df.columns[1]] = upd_count
                    settings_df.at[p_idx, settings_df.columns[3]] = ",".join(map(str, upd_principals))
                    settings_df.at[p_idx, settings_df.columns[4]] = ",".join(map(str, upd_rates))
                    settings_df.at[p_idx, settings_df.columns[6]] = ",".join(upd_names)
                    
                    # 書き込み実行
                    conn.update(worksheet="Settings", data=settings_df)
                    
                    st.success(f"✅ {new_name} 様を登録しました。")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("名前を入力してください。")

except Exception as e:
    st.error(f"エラー: {e}")
