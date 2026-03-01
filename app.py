import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime, timedelta

st.set_page_config(page_title="複利元本推移管理", page_icon="📈")
st.title("📈 複利運用・元本推移管理")

# --- 接続設定 ---
try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings")
    settings_df.columns = [str(c).strip() for c in settings_df.columns]
    p_col = settings_df.columns[0]
    project_list = settings_df[p_col].astype(str).tolist()
except Exception as e:
    st.error("設定シートの読み込みに失敗しました。")
    st.stop()

# --- プロジェクト選択 ---
selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
p_info = settings_df[settings_df[p_col] == selected_project].iloc[0]

# 設定パース用関数
def split_val(val): return [x.strip() for x in str(val).split(",") if x.strip()]

base_principal = float(p_info.get("Principal", 0.0))
num_people = int(p_info.get("Num_People", 1))
apr_list = [float(a) for a in split_val(p_info.get("Individual_APRs", ""))]
comp_list = [c.upper() == "TRUE" for c in split_val(p_info.get("Individual_Compounding", ""))]

# --- 履歴データの読み込み ---
try:
    hist_data = conn.read(worksheet=selected_project)
    hist_data["Date"] = pd.to_datetime(hist_data["Date"])
except:
    hist_data = pd.DataFrame()

# --- 1. 本日の元本組み込み計算 ---
st.subheader("📅 本日の元本と収益")

indiv_base_p = base_principal / num_people
current_principals = []
today_yields = []

for i in range(num_people):
    unpaid_yield = 0.0
    if comp_list[i] and not hist_data.empty:
        # Paid_Flagsが0（未払い）の収益を合計して元本に組み込む
        for _, row in hist_data.iterrows():
            flags = str(row["Paid_Flags"]).split(",")
            if i < len(flags) and flags[i] == "0":
                unpaid_yield += float(str(row["Breakdown"]).split(",")[i])
    
    current_p = indiv_base_p + unpaid_yield
    current_principals.append(current_p)
    
    # 収益計算
    daily_y = (current_p * (apr_list[i] / 100)) / 365
    today_yields.append(round(daily_y, 4))

total_current_p = sum(current_principals)
total_today_yield = sum(today_yields)

st.metric("本日の計算元本合計 (組み込み後)", f"${total_current_p:,.2f}", 
          delta=f"+${total_current_p - base_principal:,.2f} (累計組み込み額)")

if st.button("本日の収益を確定・記録する"):
    new_row = pd.DataFrame([{
        "Date": datetime.now().strftime("%Y-%m-%d"),
        "Principal_Used": round(total_current_p, 2),
        "Compounded_Total": round(total_current_p - base_principal, 2), # 元本の増加分
        "Breakdown": ", ".join(map(str, today_yields)),
        "Total_Yield": round(total_today_yield, 4),
        "Paid_Flags": ",".join(["0"] * num_people)
    }])
    updated_hist = pd.concat([hist_data, new_row], ignore_index=True)
    conn.update(worksheet=selected_project, data=updated_hist)
    st.success("記録完了しました！")
    st.rerun()

st.divider()

# --- 2. 週単位の元本推移確認 (ここが追加ポイント) ---
st.subheader("📊 週単位の元本増加レポート")

if not hist_data.empty:
    # 日付でグルーピング（週単位：月曜始まり）
    report_df = hist_data.copy()
    report_df['Week'] = report_df['Date'].dt.to_period('W').apply(lambda r: r.start_time)
    
    # 週ごとの最終的な元本と、その週の総収益を算出
    weekly_summary = report_df.groupby('Week').agg({
        'Principal_Used': 'last',      # その週の終わりの元本額
        'Total_Yield': 'sum'           # その週に発生した収益合計
    }).sort_index(ascending=False)
    
    # 前週からの増加額を計算
    weekly_summary['Weekly_Increase'] = weekly_summary['Principal_Used'].diff(periods=-1)
    
    # 表示用の整形
    weekly_summary.columns = ["週末時点の元本", "週間の発生収益", "前週比(元本増加)"]
    st.table(weekly_summary.head(12).style.format("${:,.2f}"))
else:
    st.info("データが蓄積されると、ここに週次レポートが表示されます。")
