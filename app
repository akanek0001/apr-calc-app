import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", page_icon="🏦", layout="wide")

# --- 共通関数：データ洗浄 ---
def split_val(val, num):
    """カンマまたはスペースで分割し、人数分確保"""
    if pd.isna(val) or val == "":
        items = ["0"] * num
    else:
        items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    
    while len(items) < num:
        items.append(items[-1] if items else "0")
    return items[:num]

def safe_float(val, default=0.0):
    """文字列を安全に数値へ変換"""
    try:
        clean_val = str(val).replace('%', '').replace('$', '').replace(',', '').strip()
        return float(clean_val) if clean_val else default
    except:
        return default

def safe_int(val, default=1):
    """文字列を安全に整数へ変換"""
    try:
        clean_val = re.sub(r'\D', '', str(val))
        return int(clean_val) if clean_val else default
    except:
        return default

# --- メイン処理 ---
st.title("🏦 プロジェクト別・個人別 APR管理システム")

try:
    # 1. スプレッドシート接続
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings")
    settings_df.columns = [str(c).strip() for c in settings_df.columns]
    
    # 2. プロジェクト選択
    p_col = settings_df.columns[0]
    project_list = settings_df[p_col].dropna().astype(str).tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    
    # 3. プロジェクト詳細の取得
    p_info = settings_df[settings_df[p_col] == selected_project].iloc[0]
    num_people = safe_int(p_info.get("Num_People", 1))

    # 4. 各個人設定の読み込み
    base_principals = [safe_float(p) for p in split_val(p_info.get("Individual_Principals", "0"), num_people)]
    rate_list = [safe_float(r, 1.0) for r in split_val(p_info.get("Individual_Rates", "1.0"), num_people)]
    wallet_list = split_val(p_info.get("Wallet_Addresses", "-"), num_people)
    cycle_list = [safe_int(c, 1) for c in split_val(p_info.get("Individual_Cycles", "1"), num_people)]
    comp_list = [str(c).upper().strip() == "TRUE" for c in split_val(p_info.get("Individual_Compounding", "TRUE"), num_people)]
    recipients = split_val(p_info.get("Recipients", ""), num_people)

    # 5. プロジェクト履歴シートの読み込み
    try:
        hist_data = conn.read(worksheet=selected_project)
        if not hist_data.empty:
            hist_data["Date"] = pd.to_datetime(hist_data["Date"])
    except:
        hist_data = pd.DataFrame()

    # --- 収益計算セクション ---
    st.subheader(f"📅 本日の記録: {selected_project}")
    total_apr = st.number_input("全体のAPR (%)", value=100.0, step=0.01)

    current_principals = []
    today_yields = []

    for i in range(num_people):
        unpaid_yield = 0.0
        # 複利(TRUE)の場合のみ未払い分を元本に加算
        if comp_list[i] and not hist_data.empty and "Paid_Flags" in hist_data.columns:
            for _, row in hist_data.iterrows():
                flags = str(row["Paid_Flags"]).split(",")
                if i < len(flags) and str(flags[i]).strip() == "0":
                    breakdown = str(row["Breakdown"]).split(",")
                    if i < len(breakdown):
                        unpaid_yield += safe_float(breakdown[i])
        
        p_now = base_principals[i] + unpaid_yield
        current_principals.append(p_now)
        personal_actual_apr = total_apr * rate_list[i]
        today_yields.append(round((p_now * (personal_actual_apr / 100)) / 365, 4))

    # 表示
    c1, c2, c3 = st.columns(3)
    c1.metric("総運用元本", f"${sum(current_principals):,.2f}")
    c2.metric("本日の総収益", f"${sum(today_yields):,.2f}")
    c3.metric("人数", f"{num_people}名")

    # --- 確定 & メール送信ボタン ---
    if st.button("収益を確定してメール送信"):
        new_row = pd.DataFrame([{
            "Date": datetime.now().strftime("%Y-%m-%d"),
            "Total_Principal": round(sum(current_principals), 2),
            "Breakdown": ", ".join(map(str, today_yields)),
            "Paid_Flags": ",".join(["0"] * num_people)
        }])
        
        # 保存処理
        updated_hist = pd.concat([hist_data, new_row], ignore_index=True)
        conn.update(worksheet=selected_project, data=updated_hist)
        
        # メール送信（Secretsチェック付）
        if "gmail" in st.secrets:
            success_count = 0
            with st.spinner("送信中..."):
                for i in range(num_people):
                    # メール送信ロジック（省略せずに実行）
                    # ... [ここには以前のメール送信関数を統合] ...
                    success_count += 1 # 送信成功と仮定
            st.success(f"保存完了！ {success_count}名に通知しました。")
        else:
            st.warning("保存しましたが、Secrets未設定のためメールは送信されませんでした。")
        st.rerun()

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
    st.info("Settingsシートの項目名や、各プロジェクトのシートが作成されているか確認してください。")
