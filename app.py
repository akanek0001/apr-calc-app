import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide")
st.title("🏦 APR管理システム（運用復帰版）")

# データ分割用関数
def get_exact_list(val, n):
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    items = items[:n]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items

try:
    # 1. スプレッドシート接続
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read(worksheet="Settings")
    
    # 2. プロジェクト選択
    project_list = df.iloc[:, 0].dropna().astype(str).tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    
    # 3. データの取得
    p_info = df[df.iloc[:, 0] == selected_project].iloc[0]

    # --- B列から「本当の人数」を読み取る ---
    num_val = re.sub(r'\D', '', str(p_info.iloc[1]))
    num_people = int(num_val) if num_val else 1

    # D列から「全員分の元本」を読み取って合計する
    def to_f(val):
        try:
            return float(str(val).replace(',','').replace('$','').strip())
        except:
            return 0.0

    # カンマ区切りの数値をリスト化
    raw_principals = get_exact_list(p_info.iloc[3], num_people)
    principal_list = [to_f(p) for p in raw_principals]
    total_principal = sum(principal_list)

    # --- 表示 ---
    st.subheader(f"📊 {selected_project} の運用状況")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("確定人数", f"{num_people} 名")
    col2.metric("総運用元本", f"${total_principal:,.2f}")
    
    # 平均APRの入力
    total_apr = st.number_input("全体のAPR (%)", value=100.0, step=0.01)
    
    # 全員の収益合計を計算
    total_yield = round((total_principal * (total_apr / 100)) / 365, 4)
    col3.metric("本日の総収益", f"${total_yield:,.4f}")

    # 個別内訳の確認用（デバッグ用）
    with st.expander("メンバー別内訳を確認"):
        for i, p in enumerate(principal_list):
            st.write(f"No.{i+1}: 元本 ${p:,.2f} / 予想収益 ${round((p * (total_apr/100))/365, 4):,.4f}")

    if st.button("収益データを履歴に保存"):
        try:
            hist_df = conn.read(worksheet=selected_project)
        except:
            hist_df = pd.DataFrame()
            
        new_data = pd.DataFrame([{
            "Date": datetime.now().strftime("%Y-%m-%d"),
            "Total_Principal": total_principal,
            "Breakdown": ", ".join(map(str, [round((p * (total_apr/100))/365, 4) for p in principal_list])),
            "Paid_Flags": ",".join(["0"] * num_people)
        }])
        
        updated_df = pd.concat([hist_df, new_data], ignore_index=True)
        conn.update(worksheet=selected_project, data=updated_df)
        st.success(f"{selected_project} の履歴を更新しました！")
        st.rerun()

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
