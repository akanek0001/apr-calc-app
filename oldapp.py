import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR高度管理システム", page_icon="🏦", layout="wide")
st.title("🏦 プロジェクト別・個人別 APR管理 & 自動報告")

# --- 共通関数 ---
def split_val(val, num):
    """カンマまたはスペースで文字列を分割し、指定された人数分を確保する"""
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    while len(items) < num:
        items.append(items[-1] if items else "0")
    return items[:num]

def safe_float(val, default=0.0):
    """文字列を安全に浮動小数点数に変換する（%, $, カンマを除去）"""
    try:
        clean_val = str(val).replace('%', '').replace('$', '').replace(',', '').strip()
        return float(clean_val) if clean_val else default
    except:
        return default

def safe_int(val, default=1):
    """文字列を安全に整数に変換する"""
    try:
        clean_val = re.sub(r'\D', '', str(val))
        return int(clean_val) if clean_val else default
    except:
        return default

# --- Googleスプレッドシート接続 ---
try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings")
    # 列名の前後の空白を削除
    settings_df.columns = [str(c).strip() for c in settings_df.columns]
    p_col = settings_df.columns[0] # 通常 Project_Name
    project_list = settings_df[p_col].astype(str).tolist()
except Exception as e:
    st.error("Settingsシートが見つかりません。シート名を 'Settings' にし、見出しを確認してください。")
    st.stop()

# --- サイドバー：プロジェクト選択 ---
selected_project = st.sidebar.selectbox("管理するプロジェクトを選択", project_list)
p_info = settings_df[settings_df[p_col] == selected_project].iloc[0]

# --- 個別設定のパース（安全版） ---
num_people = safe_int(p_info.get("Num_People", 1))

# 元本、配分率、アドレス、サイクル、複利設定、メールアドレスを個別に取得
base_principals = [safe_float(p) for p in split_val(p_info.get("Individual_Principals", "0"), num_people)]
rate_list = [safe_float(r, 1.0) for r in split_val(p_info.get("Individual_Rates", "1.0"), num_people)]
wallet_list = split_val(p_info.get("Wallet_Addresses", "-"), num_people)
cycle_list = [safe_int(c, 1) for c in split_val(p_info.get("Individual_Cycles", "1"), num_people)]
comp_list = [str(c).upper().strip() == "TRUE" for c in split_val(p_info.get("Individual_Compounding", "TRUE"), num_people)]
recipients = split_val(p_info.get("Recipients", ""), num_people)

# --- メール送信関数 ---
def send_individual_email(to_email, project_name, personal_yield, total_yield, personal_apr, wallet):
    try:
        gmail_user = st.secrets["gmail"]["user"]
        gmail_password = st.secrets["gmail"]["password"]
    except KeyError:
        st.error("SecretsにGmailの設定がありません。")
        return False

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

# --- 履歴データの読み込み ---
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
    # 複利設定かつ未払いデータがある場合、元本に加算
    if comp_list[i] and not hist_data.empty:
        for _, row in hist_data.iterrows():
            flags = str(row["Paid_Flags"]).split(",")
            if i < len(flags) and str(flags[i]).strip() == "0":
                unpaid_yield += safe_float(str(row["Breakdown"]).split(",")[i])
    
    p_now = base_principals[i] + unpaid_yield
    current_principals.append(p_now)
    
    # 収益計算
    personal_actual_apr = total_apr * rate_list[i]
    today_yields.append(round((p_now * (personal_actual_apr / 100)) / 365, 4))

col1, col2, col3 = st.columns(3)
col1.metric("総運用元本 (現在)", f"${sum(current_principals):,.2f}")
col2.metric("本日の総収益", f"${sum(today_yields):,.2f}")
col3.metric("参加人数", f"{num_people} 名")

if st.button("本日の収益を確定し、各自にメール送信"):
    # スプレッドシート履歴更新
    new_row = pd.DataFrame([{
        "Date": datetime.now().strftime("%Y-%m-%d"),
        "Total_Principal": round(sum(current_principals), 2),
        "Breakdown": ", ".join(map(str, today_yields)),
        "Paid_Flags": ",".join(["0"] * num_people)
    }])
    updated_hist = pd.concat([hist_data, new_row], ignore_index=True)
    conn.update(worksheet=selected_project, data=updated_hist)
    
    # メール送信実行
    success_count = 0
    with st.spinner("メール送信中..."):
        for i in range(num_people):
            if send_individual_email(recipients[i], selected_project, today_yields[i], sum(today_yields), total_apr * rate_list[i], wallet_list[i]):
                success_count += 1
    
    st.success(f"記録完了！ {success_count} 名にメールを送信しました。")
    st.rerun()

st.divider()

# --- 2. 個人別・送金タイミング判定 ---
st.subheader("🏦 送金・支払い管理")
payout_rows = []

if not hist_data.empty:
    for i in range(num_people):
        unpaid_indices = []
        person_total = 0.0
        first_unpaid_date = None
        
        for idx, row in hist_data.iterrows():
            flags = str(row["Paid_Flags"]).split(",")
            if i < len(flags) and str(flags[i]).strip() == "0":
                unpaid_indices.append(idx)
                person_total += safe_float(str(row["Breakdown"]).split(",")[i])
                if first_unpaid_date is None:
                    first_unpaid_date = row["Date"]
        
        if first_unpaid_date:
            days_passed = (datetime.now() - first_unpaid_date).days
            if days_passed >= (cycle_list[i] * 7):
                payout_rows.append({
                    "ID": i,
                    "メンバー": f"No.{i+1}",
                    "送金先": wallet_list[i],
                    "合計額": round(person_total, 2),
                    "サイクル": f"{cycle_list[i]}週",
                    "経過日数": f"{days_passed}日",
                    "Rows": unpaid_indices
                })

if payout_rows:
    payout_df = pd.DataFrame(payout_rows).drop(columns=["ID", "Rows"])
    st.warning("⚠️ 送金対象者がいます")
    st.table(payout_df)
    
    if st.button("送金を完了としてマーク（履歴を更新）"):
        for p in payout_rows:
            for r_idx in p["Rows"]:
                flags = str(hist_data.at[r_idx, "Paid_Flags"]).split(",")
                flags[p["ID"]] = "1"
                hist_data.at[r_idx, "Paid_Flags"] = ",".join(flags)
        conn.update(worksheet=selected_project, data=hist_data)
        st.success("支払済みとして更新しました。")
        st.rerun()
else:
    st.info("現在、送金対象のメンバーはいません。")
