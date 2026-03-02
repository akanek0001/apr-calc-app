import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import requests
import json
import re

# --- 1. ページ設定 (最優先) ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- 2. ユーティリティ関数 ---
def to_f(val):
    """数値変換のエラーを徹底ガード"""
    if pd.isna(val): return 0.0
    try:
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    """リスト分割のエラーをガード"""
    if pd.isna(val): return ["0"] * n
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

def send_line_message(token, user_id, text):
    """LINE通知"""
    if not user_id or str(user_id) == "nan": return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": text}]}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
        return res.status_code
    except:
        return 500

# --- 3. メイン処理 ---
st.title("🏦 APR管理システム")

try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    
    # 【対策1】ヘッダー名に依存せず、すべての値を読み込む
    # worksheet="LineID" はあなたのスプレッドシートのシート名と一致させてください
    settings_df = conn.read(worksheet="LineID", ttl=0)
    
    if settings_df.empty:
        st.error("スプレッドシート 'LineID' が空です。")
        st.stop()

    # 【対策2】列番号（0, 1, 2...）でデータを取得する（見出しの名前が違っても大丈夫なように）
    # A列: ProjectName, B列: NumPeople, D列: BasePrincipals, E列: Rates, F列: Line_User_ID
    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    
    # 選択したプロジェクトの行を抽出
    p_info = settings_df[settings_df.iloc[:, 0] == selected_project].iloc[0]

    # LINE IDの取得（F列 = インデックス5）
    user_ids = []
    if settings_df.shape[1] >= 6:
        user_ids = settings_df.iloc[:, 5].dropna().unique().tolist()

    # 基本情報の解析（列番号で指定）
    num_people = int(to_f(p_info.iloc[1]))      # B列
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)] # D列
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]       # E列

    # --- 履歴シートの読み込み ---
    try:
        hist_df = conn.read(worksheet=selected_project, ttl=0)
    except:
        hist_df = pd.DataFrame()

    # 履歴計算（Type, Breakdownなどの列名がない場合を考慮）
    current_principals = []
    total_earned = [0.0] * num_people
    total_withdrawn = [0.0] * num_people

    if not hist_df.empty:
        # 列名（Type=B列, Breakdown=D列）を決め打ちせず位置で探すか、存在確認する
        for _, row in hist_df.iterrows():
            try:
                # TypeはB列(1)、BreakdownはD列(3)と仮定
                rtype = str(row.iloc[1])
                rbreakdown = str(row.iloc[3])
                vals = rbreakdown.split(",")
                for i in range(num_people):
                    if i < len(vals):
                        amount = to_f(vals[i])
                        if rtype == "収益": total_earned[i] += amount
                        elif rtype == "出金": total_withdrawn[i] += amount
            except: continue

    for i in range(num_people):
        current_principals.append(base_principals[i] + total_earned[i] - total_withdrawn[i])

    # --- 画面表示 ---
    tab1, tab2 = st.tabs(["📈 収益確定", "💸 出金処理"])

    with tab1:
        st.subheader(f"【{selected_project}】収益確定")
        total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.01)
        net_factor = 0.67
        today_yields = [round((p * (total_apr * net_factor * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(current_principals)]
        
        cols = st.columns(num_people)
        for i, col in enumerate(cols):
            col.metric(f"No.{i+1} 元本", f"${current_principals[i]:,.2f}")
            col.write(f"+${today_yields[i]:,.4f}")

        if st.button("保存してLINE一斉通知"):
            new_data = {
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "収益",
                "Total_Amount": sum(today_yields),
                "Breakdown": ",".join(map(str, today_yields)),
                "Note": f"APR:{total_apr}%"
            }
            # 書き込み
            updated_hist = pd.concat([hist_df, pd.DataFrame([new_data])], ignore_index=True)
            conn.update(worksheet=selected_project, data=updated_hist)
            
            # LINE通知
            if "line" in st.secrets:
                token = st.secrets["line"]["channel_access_token"]
                msg = f"【収益報告】\n{selected_project}\nAPR:{total_apr}%\n"
                for i in range(num_people):
                    msg += f"No.{i+1}: +${today_yields[i]:,.4f}\n(元本:${current_principals[i]+today_yields[i]:,.2f})\n"
                
                success_count = 0
                for uid in user_ids:
                    if send_line_message(token, uid, msg) == 200: success_count += 1
                st.success(f"保存完了！{success_count}名に通知しました。")
            st.rerun()

    with tab2:
        st.subheader("出金記録")
        target = st.selectbox("メンバー", [f"No.{i+1}" for i in range(num_people)])
        idx = int(target.split(".")[1]) - 1
        amt = st.number_input("金額 ($)", min_value=0.0, max_value=current_principals[idx])
        
        if st.button("出金を確定"):
            withdrawals = [0.0] * num_people
            withdrawals[idx] = amt
            new_data = {
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "出金",
                "Total_Amount": amt,
                "Breakdown": ",".join(map(str, withdrawals)),
                "Note": f"No.{idx+1}出金"
            }
            updated_hist = pd.concat([hist_df, pd.DataFrame([new_data])], ignore_index=True)
            conn.update(worksheet=selected_project, data=updated_hist)
            st.success("出金を記録しました。")
            st.rerun()

except Exception as e:
    st.error(f"システムエラー: {e}")
