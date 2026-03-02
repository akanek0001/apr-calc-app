import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide")
st.title("🏦 APR管理システム（人数固定・安定版）")

def safe_float(val):
    try:
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except:
        return 0.0

try:
    # 1. スプレッドシート接続
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read(worksheet="Settings")
    
    # 2. プロジェクト選択
    project_list = df.iloc[:, 0].dropna().astype(str).tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    
    # 3. データの取得
    p_info = df[df.iloc[:, 0] == selected_project].iloc[0]

    # --- 【重要】B列(2列目)の数字を読み取る。失敗したら1名にする ---
    raw_num = str(p_info.iloc[1]).strip()
    num_people = int(float(raw_num)) if raw_num and raw_num.replace('.','').isdigit() else 1

    # --- 【重要】D列(4列目)から元本を取得し、B列の「人数分だけ」を取り出す ---
    raw_p_data = str(p_info.iloc[3])
    # カンマまたはスペースで区切られたデータをリスト化
    p_list_all = [x.strip() for x in re.split(r'[,\s]+', raw_p_data) if x.strip()]
    
    # B列の人数(num_people)の数だけ、先頭からデータを取り出す（足りなければ0で埋める）
    final_principals = []
    for i in range(num_people):
        if i < len(p_list_all):
            final_principals.append(safe_float(p_list_all[i]))
        else:
            final_principals.append(0.0)

    total_principal = sum(final_principals)

    # --- 表示 ---
    st.subheader(f"📊 {selected_project} の状況")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("確定人数 (B列参照)", f"{num_people} 名")
    col2.metric("総運用元本", f"${total_principal:,.2f}")
    
    total_apr = st.number_input("全体のAPR (%)", value=100.0, step=0.01)
    total_yield = round((total_principal * (total_apr / 100)) / 365, 4)
    col3.metric("本日の総収益", f"${total_yield:,.4f}")

    # 内訳の表示（ここを見れば、正しく読み込めているか分かります）
    with st.expander("データ読み込みの内訳（確認用）"):
        st.write(f"B列の人数: {num_people}")
        st.write(f"読み込んだ元本リスト: {final_principals}")

    if st.button("収益データを履歴に保存"):
        try:
            hist_df = conn.read(worksheet=selected_project)
        except:
            hist_df = pd.DataFrame()
            
        new_data = pd.DataFrame([{
            "Date": datetime.now().strftime("%Y-%m-%d"),
            "Total_Principal": total_principal,
            "Breakdown": ", ".join(map(str, [round((p * (total_apr/100))/365, 4) for p in final_principals])),
            "Paid_Flags": ",".join(["0"] * num_people)
        }])
        
        updated_df = pd.concat([hist_df, new_data], ignore_index=True)
        conn.update(worksheet=selected_project, data=updated_df)
        st.success(f"保存完了しました。")
        st.rerun()

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
