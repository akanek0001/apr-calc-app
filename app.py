import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import requests, json, re

import gspread
from google.oauth2.service_account import Credentials

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- Google Sheets 接続 ---
def gs_client():
    cred_info = st.secrets["connections"]["gsheets"]["credentials"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(cred_info, scopes=scopes)
    return gspread.authorize(creds)

def open_sheet():
    spreadsheet_url = st.secrets["connections"]["gsheets"]["spreadsheet"]
    return gs_client().open_by_url(spreadsheet_url)

def ws_to_df(ws):
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=header)

def df_to_ws(ws, df):
    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).fillna("").values.tolist())

# --- ユーティリティ ---
def to_f(val):
    try:
        return float(str(val).replace(',','').replace('$','').replace('%','').strip())
    except:
        return 0.0

def split_val(val, n):
    items = [x.strip() for x in str(val).split(",")]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

def send_line(token, user_id, text):
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json",
               "Authorization": f"Bearer {token}"}
    payload = {"to": user_id,
               "messages": [{"type": "text", "text": text}]}
    requests.post(url, headers=headers, data=json.dumps(payload))

# --- メイン ---
st.title("🏦 APR資産運用管理システム")

try:
    sh = open_sheet()

    settings_df = ws_to_df(sh.worksheet("Settings"))
    settings_df.columns = [str(c).strip() for c in settings_df.columns]
    line_id_df  = ws_to_df(sh.worksheet("LineID"))

    if settings_df.empty:
        st.error("Settingsシートが空です。")
        st.stop()

    project_list = settings_df["Project_Name"].unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)

    p_info = settings_df[settings_df["Project_Name"] == selected_project].iloc[0]

    num_people = int(to_f(p_info["Num_People"]))
    base_principals = [to_f(x) for x in split_val(p_info["IndividualPrincipals"], num_people)]
    rate_list = [to_f(x) for x in split_val(p_info["ProfitRates"], num_people)]
    is_compound = str(p_info["IsCompound"]).upper() in ["TRUE","YES","1","はい"]

    user_ids = line_id_df["LineID"].dropna().tolist()

    # 履歴読み込み
    try:
        hist_ws = sh.worksheet(selected_project)
        hist_df = ws_to_df(hist_ws)
    except:
        hist_ws = sh.add_worksheet(title=selected_project, rows=1000, cols=20)
        hist_df = pd.DataFrame(columns=["Date","Type","Total_Amount","Breakdown","Note"])
        df_to_ws(hist_ws, hist_df)

    tab1, tab2 = st.tabs(["📈 収益確定", "💸 出金"])

    with tab1:
        total_apr = st.number_input("本日のAPR (%)", value=100.0)
        today_yields = [(base_principals[i] * total_apr / 100 / 365) for i in range(num_people)]

        if st.button("収益を保存してLINE送信"):
            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "収益",
                "Total_Amount": sum(today_yields),
                "Breakdown": ",".join(map(str, today_yields)),
                "Note": f"APR:{total_apr}%"
            }])

            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            df_to_ws(hist_ws, updated_hist)

            for uid in user_ids:
                send_line(st.secrets["line"]["channel_access_token"],
                          uid,
                          f"本日の収益: {sum(today_yields):,.2f}")

            st.success("送信完了")

except Exception as e:
    st.error(f"システムエラー: {e}")
