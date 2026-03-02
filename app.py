import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import requests
import json
import re

# --- 1. ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- 2. ユーティリティ (エラーガード付き) ---
def to_f(val):
    if pd.isna(val): return 0.0
    try:
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "": return ["0"] * n
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

def send_line_message(token, user_id, text):
    if not user_id or str(user_id) == "nan": return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {"to": str(user_id), "messages": [{"type": "text", "text": text}]}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
        return res.status_code
    except: return 500

# --- 3. メインロジック ---
st.title("🏦 APR管理システム 最終安定版")

try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    
    # シート読み込み
    settings_df = conn.read(worksheet="Settings", ttl=0)
    line_id_df = conn.read(worksheet="LineID", ttl=0)
    
    if settings_df.empty:
        st.error("Settingsシートが空です。データを入力してください。")
        st.stop()

    # プロジェクト選択 (A列)
    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = settings_df[settings_df.iloc[:, 0] == selected_project].iloc[0]

    # 設定値の抽出
    num_people = int(to_f(p_info.iloc[1]))      # B列: 人数
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)] # D列: 初期元本
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]       # E列: 配分比率
    
    # 複利判定 (F列: TRUEなら複利)
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"]

    # LINE ID取得 (LineIDシートの全列からIDらしきものを探す)
    user_ids = []
    if not line_id_df.empty:
        # Line_User_ID列があれば優先、なければ一番右の列
        if "Line_User_ID" in line_id_df.columns:
            user_ids = line_id_df["Line_User_ID"].dropna().unique().tolist()
        else:
            user_ids = line_id_df.iloc[:, -1].dropna().unique().tolist()

    # 履歴読み込み
    try:
        hist_df = conn.read(worksheet=selected_project, ttl=0)
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # 収益・出金の累計計算
    total_earned = [0.0] * num_people
    total_withdrawn = [0.0] * num_people

    if not hist_df.empty:
        for _, row in hist_df.iterrows():
            try:
                # 列名ではなく位置で取得 (Type:B列=1, Breakdown:D列=3)
                rtype = str(row.iloc[1])
                rbreakdown = str(row.iloc[3])
                vals = [to_f(v) for v in rbreakdown.split(",")]
                for i in range(num_people):
                    if i < len(vals):
                        if rtype == "収益": total_earned[i] += vals[i]
                        elif rtype == "出金": total_withdrawn[i] += vals[i]
            except: continue

    # 元本計算 (複利モードの適用)
    calc_principals = []
    for i in range(num_people):
        if is_compound:
            # 複利：初期元本 + 累計収益 - 累計出金
            calc_principals.append(base_principals[i] + total_earned[i] - total_withdrawn[i])
        else:
            # 単利：初期元本固定
            calc_principals.append(base_principals[i])

    # --- 表示エリア ---
    st.sidebar.info(f"計算モード: {'✅ 複利' if is_compound else '💡 単利'}")
    tab1, tab2 = st.tabs(["📈 収益の確定", "💸 出金の記録"])

    with tab1:
        st.subheader(f"【{selected_project}】本日の収益")
        total_apr = st.number_input("本日の全体のAPR (%)", value=100.0, step=0.01)
        net_factor = 0.67
        
        # 収益計算
        today_yields = [round((p * (total_apr * net_factor * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(calc_principals)]
        
        cols = st.columns(num_people)
        for i, col in enumerate(cols):
            col.metric(f"No.{i+1} 元本", f"${calc_principals[i]:,.2f}")
            col.write(f"収益: +${today_yields[i]:,.4f}")

        if st.button("収益を保存して全員にLINE通知"):
            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "収益",
                "Total_Amount": sum(today_yields),
                "Breakdown": ",".join(map(str, today_yields)),
                "Note": f"APR:{total_apr}%"
            }])
            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            conn.update(worksheet=selected_project, data=updated_hist)
            
          [line]
channel_access_token = "EYZpry+KdabiZ1B/G8PlQkyLW5/BK/qjccWqFAMfYV7/H3NMzgMoJy5mX8HtgYypmYIx/eDNNet4qDKa3EkNaYVLReb9pRwExD+kEyT2EyzVhwU+UppXQrHpRNQHK4duaIfQ56zewqW32knrDNYnaQdB04t89/1O/w1cDnyilFU="
user_id = "U352695b567963ba0e6c5be7fe0aade88"

# メール送信設定
[gmail]
user = "akanek@gmail.com"
password = "qdlwdfnbgvtcsxvc"  # スペースを詰めたもの
 
            if "line" in st.secrets:
                token = st.secrets["line"]["channel_access_token"]
                
                # --- 報告書の作成 ---
                now_str = datetime.now().strftime("%Y/%m/%d %H:%M")
                mode_str = "【複利運用】" if is_compound else "【単利運用】"
                
                msg = f"🏦 【運用収益報告書】\n"
                msg += f"プロジェクト: {selected_project}\n"
                msg += f"報告日時: {now_str}\n\n"
                msg += f"━━━━━━━━━━━━━━\n"
                msg += f"📊 本日の運用結果\n"
                msg += f"━━━━━━━━━━━━━━\n"
                msg += f"本日のAPR: {total_apr}%\n\n"
                msg += f"💰 各メンバー収益明細\n"
                
                for i in range(num_people):
                    new_p = calc_principals[i] + today_yields[i]
                    msg += f"・No.{i+1}: +${today_yields[i]:,.4f}\n"
                    msg += f"  (現在元本: ${new_p:,.2f})\n"
                
                msg += f"\n━━━━━━━━━━━━━━\n"
                msg += f"💡 運用状況メモ\n"
                msg += f"現在のモード: {mode_str}\n"
                if is_compound:
                    msg += f"※収益は次回の元本に組み入れられます。\n"
                msg += f"━━━━━━━━━━━━━━"
                
                # 送信実行
                success_count = 0
                for uid in user_ids:
                    if send_line_message(token, uid, msg) == 200:
                        success_count += 1
                st.success(f"報告書を送信しました（送信成功: {success_count}名）")



    with tab2:
        st.subheader("出金・精算の記録")
        target_no = st.selectbox("メンバー", [f"No.{i+1}" for i in range(num_people)])
        idx = int(target_no.split(".")[1]) - 1
        st.warning(f"出金可能額: ${calc_principals[idx]:,.2f}")
        amt = st.number_input("出金額 ($)", min_value=0.0, max_value=calc_principals[idx], step=1.0)
        
        if st.button("出金を確定"):
            if amt > 0:
                withdrawals = [0.0] * num_people
                withdrawals[idx] = amt
                new_row = pd.DataFrame([{
                    "Date": datetime.now().strftime("%Y-%m-%d"),
                    "Type": "出金",
                    "Total_Amount": amt,
                    "Breakdown": ",".join(map(str, withdrawals)),
                    "Note": f"Member {idx+1} Withdrawal"
                }])
                updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
                conn.update(worksheet=selected_project, data=updated_hist)
                st.success("出金を記録しました。")
                st.rerun()

except Exception as e:
    st.error(f"システムエラーが発生しました: {e}")
