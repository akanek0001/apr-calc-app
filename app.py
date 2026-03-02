import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import requests
import json
import re

# --- 1. ページ設定 (必ず最初に実行) ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- 2. 便利関数 (計算・LINE送信) ---
def to_f(val):
    """文字列や記号を含む値を数値に変換"""
    try:
        if pd.isna(val): return 0.0
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    """カンマ区切りの文字列をリストに分割"""
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    items = items[:n]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items

def send_line_message(token, user_id, text):
    """LINE Messaging API を使用してプッシュ通知を送る"""
    if not user_id or str(user_id) == "nan": return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": text}]}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(payload))
        return res.status_code
    except:
        return 500

# --- 3. メイン処理 ---
st.title("🏦 APR管理システム（一斉LINE通知版）")

try:
    # Google Sheets への接続
    conn = st.connection("gsheets", type=GSheetsConnection)
    
    # Settingsシートの読み込み (Makeが書き込んでいるシート)
    # ここに 'Line_User_ID' 列がある前提です
    settings_df = conn.read(worksheet="Settings", ttl=0) 
    
    # プロジェクトリストの取得
    project_list = settings_df.iloc[:, 0].dropna().astype(str).tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    
    # 選択されたプロジェクトの詳細情報を取得
    p_info = settings_df[settings_df.iloc[:, 0] == selected_project].iloc[0]

    # --- 重要：Makeで貯めたLINE IDを取得 ---
    user_ids = []
    if "Line_User_ID" in settings_df.columns:
        user_ids = settings_df["Line_User_ID"].dropna().unique().tolist()

    # 基本情報の解析
    num_people = int(to_f(p_info.iloc[1]))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]

    # --- 履歴シートの読み込みとエラー対策 ---
    try:
        hist_df = conn.read(worksheet=selected_project, ttl=0)
        # もし 'Type' 列がない場合は空のデータフレームとして初期化（エラー回避）
        if hist_df.empty or 'Type' not in hist_df.columns:
            hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])
    except:
        # シート自体が存在しない場合
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # 収益・出金の累計計算
    current_principals = []
    total_withdrawn = []
    total_earned = []

    for i in range(num_people):
        earned = 0.0
        withdrawn = 0.0
        if not hist_df.empty and 'Breakdown' in hist_df.columns:
            for _, row in hist_df.iterrows():
                vals = str(row["Breakdown"]).split(",")
                if i < len(vals):
                    amount = to_f(vals[i])
                    if str(row["Type"]) == "収益":
                        earned += amount
                    elif str(row["Type"]) == "出金":
                        withdrawn += amount
        total_earned.append(earned)
        total_withdrawn.append(withdrawn)
        current_principals.append(base_principals[i] + earned - withdrawn)

    # --- 画面表示 (タブ分け) ---
    tab1, tab2 = st.tabs(["📈 収益確定・一斉通知", "💸 出金・精算"])

    # --- Tab 1: 収益確定 ---
    with tab1:
        st.subheader("本日の収益計算")
        total_apr = st.number_input("本日の全体のAPR (%)", value=100.0, step=0.01)
        net_apr_factor = 0.67
        today_yields = [round((p * (total_apr * net_apr_factor * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(current_principals)]
        
        st.info(f"💡 33%控除済み（実質 {total_apr * net_apr_factor:.2f}%）")

        # 各メンバーの状態をカードで表示
        cols = st.columns(num_people)
        for i, col in enumerate(cols):
            with col:
                st.metric(f"No.{i+1} 元本", f"${current_principals[i]:,.2f}")
                st.write(f"今日: +${today_yields[i]:,.4f}")

        if st.button("収益を保存して全員にLINE通知"):
            # 保存用データ作成
            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "収益",
                "Total_Amount": sum(today_yields),
                "Breakdown": ",".join(map(str, today_yields)),
                "Note": f"APR: {total_apr}%"
            }])
            
            # スプレッドシートへ書き込み
            final_df = pd.concat([hist_df, new_row], ignore_index=True)
            conn.update(worksheet=selected_project, data=final_df)
            
            # LINE通知の実行
            if "line" in st.secrets:
                token = st.secrets["line"]["channel_access_token"]
                msg = f"【収益報告】\nプロジェクト: {selected_project}\nAPR: {total_apr}%\n" + "-"*10 + "\n"
                for i in range(num_people):
                    msg += f"No.{i+1}: +${today_yields[i]:,.4f}\n(元本: ${current_principals[i]+today_yields[i]:,.2f})\n"
                
                # Makeで貯めた全ユーザーIDにループ送信
                count = 0
                for uid in user_ids:
                    status = send_line_message(token, uid, msg)
                    if status == 200: count += 1
                
                st.success(f"記録完了！{count}名にLINE通知を送信しました。")
            else:
                st.warning("SecretsにLINEの設定がないため、保存のみ完了しました。")
            st.rerun()

    # --- Tab 2: 出金処理 ---
    with tab2:
        st.subheader("出金・精算の記録")
        target_member = st.selectbox("対象メンバー", [f"No.{i+1}" for i in range(num_people)])
        member_idx = int(target_member.split(".")[1]) - 1
        
        st.warning(f"{target_member} の出金可能額: **${current_principals[member_idx]:,.2f}**")
        withdraw_amount = st.number_input("出金額 ($)", min_value=0.0, max_value=current_principals[member_idx], step=1.0)
        
        if st.button(f"{target_member} の出金を確定"):
            if withdraw_amount > 0:
                withdrawals = [0.0] * num_people
                withdrawals[member_idx] = withdraw_amount
                
                new_row = pd.DataFrame([{
                    "Date": datetime.now().strftime("%Y-%m-%d"),
                    "Type": "出金",
                    "Total_Amount": withdraw_amount,
                    "Breakdown": ",".join(map(str, withdrawals)),
                    "Note": f"{target_member}出金"
                }])
                
                final_df = pd.concat([hist_df, new_row], ignore_index=True)
                conn.update(worksheet=selected_project, data=final_df)
                
                # LINE通知（一斉通知）
                if "line" in st.secrets:
                    token = st.secrets["line"]["channel_access_token"]
                    msg = f"【出金通知】\n{selected_project}\n{target_member}が ${withdraw_amount:,.2f} を出金しました。"
                    for uid in user_ids:
                        send_line_message(token, uid, msg)
                
                st.success(f"{target_member} の出金を記録しました。")
                st.rerun()

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
