import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import re

# Plotlyがインストールされていない場合でも起動できるようにする
try:
    import plotly.express as px
    PLOTLY_AVAILABLE = True 
except ImportError:
    PLOTLY_AVAILABLE = False

# --- 1. ページ基本設定 ---
st.set_page_config(page_title="APR運用管理 Pro", layout="wide", page_icon="🏦")

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
st.title("🏦 APR資産運用管理システム Ultra")

try:
    # 接続 (キャッシュを最小限に)
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings", ttl=0)
    
    if settings_df.empty:
        st.error("Settingsシートが読み込めません。URLと権限を確認してください。")
        st.stop()

    # プロジェクト選択
    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクト選択", project_list)
    
    # 行インデックス特定
    p_idx = settings_df[settings_df.iloc[:, 0] == selected_project].index[0]
    p_info = settings_df.loc[p_idx]
    
    # データ展開
    num_people = int(to_f(p_info.iloc[1]))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    display_names = split_val(str(p_info.iloc[6]), num_people)

    # タブ作成
    tab1, tab2, tab3, tab4 = st.tabs(["📈 収益報告", "💸 入出金管理", "🚀 詳細分析", "👥 ユーザー追加"])

    # --- タブ4: ユーザー追加 (ここがメイン) ---
    with tab4:
        st.subheader("👥 運用メンバーの追加登録")
        
        # 現在のリスト表示
        st.write("現在のメンバー:")
        st.dataframe(pd.DataFrame({"名前": display_names, "初期元本": base_principals, "比率": rate_list}))
        
        with st.form("add_new_member"):
            new_name = st.text_input("追加する名前")
            new_prin = st.number_input("追加時の元本 ($)", min_value=0.0, step=100.0)
            new_rate = st.number_input("分配比率", value=1.0, step=0.1)
            
            if st.form_submit_button("スプレッドシートを更新"):
                if new_name:
                    # データの結合
                    upd_names = display_names + [new_name]
                    upd_prins = base_principals + [new_prin]
                    upd_rates = rate_list + [new_rate]
                    
                    # DataFrameの書き換え
                    settings_df.at[p_idx, settings_df.columns[1]] = len(upd_names)
                    settings_df.at[p_idx, settings_df.columns[3]] = ",".join(map(str, upd_prins))
                    settings_df.at[p_idx, settings_df.columns[4]] = ",".join(map(str, upd_rates))
                    settings_df.at[p_idx, settings_df.columns[6]] = ",".join(upd_names)
                    
                    # シート更新
                    conn.update(worksheet="Settings", data=settings_df)
                    st.success(f"{new_name} 様を追加しました！")
                    st.rerun()
                else:
                    st.error("名前を入力してください")

    # --- タブ3: 分析 (Plotlyが使えない場合はテーブルを表示) ---
    with tab3:
        if PLOTLY_AVAILABLE:
            st.subheader("可視化データ")
            fig = px.pie(values=base_principals, names=display_names, title="元本比率")
            st.plotly_chart(fig)
        else:
            st.warning("ライブラリ(Plotly)の読み込みに失敗したため、簡易表示に切り替えました。")
            st.write("元本比率:")
            st.table(pd.DataFrame({"名前": display_names, "元本": base_principals}))

except Exception as e:
    st.error(f"システムエラー: {e}")
