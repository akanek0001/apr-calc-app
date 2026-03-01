import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.header import Header

# --- ページ設定 ---
st.set_page_config(page_title="APR高度管理システム", page_icon="🏦", layout="wide")
st.title("🏦 プロジェクト別・個人別 APR管理 & 自動報告")

# --- Googleスプレッドシート接続 ---
try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings")
    settings_df.columns = [str(c).strip() for c in settings_df.columns]
    p_col = settings_df.columns[0]
    project_list = settings_df[p_col].astype(str).tolist()
except Exception as e:
    st.error("Settingsシートの読み込みに失敗しました。")
    st.stop()

# --- プロジェクト選択 ---
selected_project = st.sidebar.selectbox("管理するプロジェクトを選択", project_list)
p_info = settings_df[settings_df[p_col] == selected_project].iloc[0]

# --- 解析用関数 ---
def split_val(val, num):
    items = [x.strip() for x in str(val).split(",") if x.strip()]
    while len(items) < num:
        items.append(items[-1] if items else "0")
    return items[:num]

# --- 個別設定のパース ---
num_people = int(p_info.get("Num_People", 1))
base_principals = [float(p) for p in split_val(p_info.get("Individual_Principals", ""), num_people)]
rate_list = [float(r) for r in split_val(p_info.get("Individual_Rates", ""), num_people)]
wallet_list = split_val(p_info.get("Wallet_Addresses", ""), num_people)
cycle_list = [int(c) for c in split_val(p_info.get("Individual_Cycles", ""), num_people)]
comp_list = [c.upper() == "TRUE" for c in split_val(p_info.get("Individual_Compounding", ""), num_people)]
recipients = split_val(p_info.get("Recipients", ""), num_people)

# --- メール送信関数 ---
def send_individual_email(to_email, project_name, personal_yield, total_yield, personal_apr, wallet):
    gmail_user = st.secrets["gmail"]["user"]
    gmail_password = st.secrets["gmail"]["password"]

    subject = f"【収益報告】{project_name} 本日の運用結果"
    body = f"""
本日の「{project_name}」運用収益報告です。

■本日の運用状況
・プロジェクト全体の総収益: ${total_yield:,.4f}
・あなたの本日の収益: ${personal_yield:,.4f}
・適用配分APR: {personal_apr:.2f}%

■現在の設定
・送金先ウォレット: {wallet}

※収益は次回の送金サイクルにまとめて送金されます。
複利設定が有効な場合、この収益は明日の運用元本に組み込まれます。
    """
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"] = gmail_user
    msg["To"] = to_email

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_password)
            server.send_message(msg)
        return True
    except:
        return False

# --- 履歴読み込み ---
try:
    hist_data = conn.read(worksheet=selected_project)
    hist_data["Date"] = pd.to_datetime(hist_data["Date"])
except:
    hist_data = pd.DataFrame()

# --- 1. 日次の収益記録 ---
st.subheader(f"📅 本日の記録: {selected_project}")
total_apr = st.number_input("プロジェクト全体の現在のAPR (%)", value=100.0, step=0.01)

current_principals = []
today_yields = []

for i in range(num_people):
    unpaid_yield = 0.0
    if comp_list[i] and not hist_data.empty:
        for _, row in hist_data.iterrows():
            flags = str(row["Paid_Flags"]).split(",")
            if i < len(flags) and flags[i] == "0":
                unpaid_yield += float(str(row["Breakdown"]).split(",")[i])
    p_now = base_principals[i] + unpaid_yield
    current_principals.append(p_now)
    personal_actual_apr = total_apr * rate_list[i]
    today_yields.append(round((p_now * (personal_actual_apr / 100)) / 365, 4))

col1, col2 = st.columns(2)
col1.metric("総運用元本", f"${sum(current_principals):,.2f}")
col2.metric("本日の総収益", f"${sum(today_yields):,.2f}")

if st.button("本日の収益を確定し、各自にメール送信"):
    # スプレッドシート更新
    new_row = pd.DataFrame([{
        "Date": datetime.now().strftime("%Y-%m-%d"),
        "Total_Principal": round(sum(current_principals), 2),
        "Breakdown": ", ".join(map(str, today_yields)),
        "Paid_Flags": ",".join(["0"] * num_people)
    }])
    conn.update(worksheet=selected_project, data=pd.concat([hist_data, new_row], ignore_index=True))
    
    # メール送信
    success_count = 0
    with st.spinner("メール送信中..."):
        for i in range(num_people):
            if send_individual_email(recipients[i], selected_project, today_yields[i], sum(today_yields), total_apr * rate_list[i], wallet_list[i]):
                success_count += 1
    st.success(f"記録完了！ {success_count} 名にメールを送信しました。")
    st.rerun()

st.divider()

# --- 2. 送金判定 ---
st.subheader("🏦 送金・支払い管理")
payout_rows = []
if not hist_data.empty:
    for i in range(num_people):
        unpaid_indices, person_total, first_date = [], 0.0, None
        for idx, row in hist_data.iterrows():
            flags = str(row["Paid_Flags"]).split(",")
            if i < len(flags) and flags[i] == "0":
                unpaid_indices.append(idx); person_total += float(str(row["Breakdown"]).split(",")[i])
                if first_date is None: first_date = row["Date"]
        if first_date and (datetime.now() - first_date).days >= (cycle_list[i] * 7):
            payout_rows.append({"ID": i, "メンバー": f"No.{i+1}", "送金先": wallet_list[i], "合計額": round(person_total, 2), "Rows": unpaid_indices})

if payout_rows:
    st.table(pd.DataFrame(payout_rows).drop(columns=["ID", "Rows"]))
    if st.button("送金を完了としてマーク"):
        for p in payout_rows:
            for r in p["Rows"]:
                f = str(hist_data.at[r, "Paid_Flags"]).split(",")
                f[p["ID"]] = "1"; hist_data.at[r, "Paid_Flags"] = ",".join(f)
        conn.update(worksheet=selected_project, data=hist_data)
        st.success("ステータスを更新しました。"); st.rerun()
