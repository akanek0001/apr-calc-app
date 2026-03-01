import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime, timedelta

# --- ページ設定 ---
st.set_page_config(page_title="個人元本別・複利管理", page_icon="💰")
st.title("💰 個人元本ベース・APR管理システム")

# --- 接続 ---
try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings")
    settings_df.columns = [str(c).strip() for c in settings_df.columns]
    p_col = settings_df.columns[0]
    project_list = settings_df[p_col].astype(str).tolist()
except Exception as e:
    st.error("スプレッドシートの接続を確認してください。")
    st.stop()

# --- プロジェクト選択 ---
selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
p_info = settings_df[settings_df[p_col] == selected_project].iloc[0]

# --- 個人設定のパース（安全版関数） ---
def split_val(val, num):
    items = [x.strip() for x in str(val).split(",") if x.strip()]
    while len(items) < num: items.append(items[-1] if items else "0")
    return items[:num]

num_people = int(p_info.get("Num_People", 1))
# 個人別の初期元本を取得
base_principals = [float(p) for p in split_val(p_info.get("Individual_Principals", ""), num_people)]
rate_list = [float(r) for r in split_val(p_info.get("Individual_Rates", ""), num_people)]
wallet_list = split_val(p_info.get("Wallet_Addresses", ""), num_people)
cycle_list = [int(c) for c in split_val(p_info.get("Individual_Cycles", ""), num_people)]
comp_list = [c.upper() == "TRUE" for c in split_val(p_info.get("Individual_Compounding", ""), num_people)]

# --- 履歴データの読み込み ---
try:
    hist_data = conn.read(worksheet=selected_project)
    hist_data["Date"] = pd.to_datetime(hist_data["Date"])
except:
    hist_data = pd.DataFrame()

# --- 1. 日次の記録（個人別元本ベース） ---
st.subheader(f"📅 本日の記録: {selected_project}")
total_apr = st.number_input("プロジェクト全体の期待APR (%)", value=100.0, step=0.1)

current_principals = []
today_yields = []

for i in range(num_people):
    # 未払い収益を元本に組み込む（複利設定の場合）
    unpaid_yield = 0.0
    if comp_list[i] and not hist_data.empty:
        for _, row in hist_data.iterrows():
            flags = str(row["Paid_Flags"]).split(",")
            if i < len(flags) and flags[i] == "0":
                unpaid_yield += float(str(row["Breakdown"]).split(",")[i])
    
    # 個人元本 = 初期出資額 + 未払い収益
    p_now = base_principals[i] + unpaid_yield
    current_principals.append(p_now)
    
    # 個人収益 = 個人元本 × (全体APR × 配分率)
    personal_actual_apr = total_apr * rate_list[i]
    daily_y = (p_now * (personal_actual_apr / 100)) / 365
    today_yields.append(round(daily_y, 4))

total_p = sum(current_principals)
total_y = sum(today_yields)

st.write(f"### 現在の総運用額: ${total_p:,.2f}")
st.write(f"### 本日の総収益額: ${total_y:,.2f}")

if st.button("本日の記録を確定する"):
    new_row = pd.DataFrame([{
        "Date": datetime.now().strftime("%Y-%m-%d"),
        "Total_Principal": round(total_p, 2),
        "Breakdown": ", ".join(map(str, today_yields)),
        "Paid_Flags": ",".join(["0"] * num_people)
    }])
    conn.update(worksheet=selected_project, data=pd.concat([hist_data, new_row], ignore_index=True))
    st.success("スプレッドシートに記録しました！")
    st.rerun()

st.divider()

# --- 2. 個人別送金判定（変更なし） ---
st.subheader("🏦 本日の送金対象者")
# （前回のコードと同様の送金判定ロジック）
# ... [省略] ...
