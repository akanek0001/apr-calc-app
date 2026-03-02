import streamlit as st
import pandas as pd
import requests
import json
import re
from datetime import datetime, timedelta

# --- 1. ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- 2. ユーティリティ ---
def to_f(val):
    if pd.isna(val) or str(val).strip() == "": return 0.0
    try:
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == "nan": return ["0"] * n
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

def send_line_multimedia(token, user_id, text, image_url=None):
    if not user_id or str(user_id) == "nan": return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    messages = [{"type": "text", "text": text}]
    if image_url:
        messages.append({"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url})
    payload = {"to": str(user_id), "messages": messages}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
        return res.status_code
    except: return 500

# --- 3. メインロジック ---
st.title("🏦 APR資産運用管理システム")

try:
    # GoogleシートのURL取得（secretsから）
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    
    # Settingsシートの読み込み（gid=0 または 特定のgid）
    settings_df = pd.read_csv(f"{base_url}/export?format=csv&gid=0") 
    # LineIDシートの読み込み（gidを指定してください。例: gid=12345678）
    # ※gidが不明な場合は、スプレッドシートのタブを切り替えた時のURL末尾の数字を確認してください
    line_id_url = f"{base_url}/export?format=csv&gid={st.secrets['gsheets'].get('lineid_gid', '0')}"
    line_id_df = pd.read_csv(line_id_url)

    if settings_df.empty:
        st.error("Settingsシートが読み込めません。")
        st.stop()

    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = settings_df[settings_df.iloc[:, 0] == selected_project].iloc[0]

    # 設定値の抽出
    num_people = int(to_f(p_info.iloc[1]))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"]

    # メンバー名の取得（G列/Index 6）
    member_names = split_val(p_info.iloc[6], num_people)

    # 履歴シートの読み込み（プロジェクト名と同じシート名を想定）
    # 注意: CSV直接読み込みでは「シート名」指定が難しいため、
    # 履歴保存を本格的に行う場合は、元のURL形式を維持する必要があります。
    # ここでは表示用に読み込みを試みます。
    try:
        hist_url = f"{base_url}/export?format=csv&gid={st.secrets['gsheets'].get(selected_project+'_gid', '0')}"
        hist_df = pd.read_csv(hist_url)
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # --- 評価額の計算ロジック ---
    total_earned = [0.0] * num_people
    total_withdrawn = [0.0] * num_people
    total_deposited = [0.0] * num_people
    
    if not hist_df.empty:
        for _, row in hist_df.iterrows():
            try:
                rtype = str(row["Type"])
                rbreakdown = str(row["Breakdown"])
                vals = [to_f(v) for v in rbreakdown.split(",")]
                for i in range(num_people):
                    if i < len(vals):
                        if rtype == "収益": total_earned[i] += vals[i]
                        elif rtype == "出金": total_withdrawn[i] += vals[i]
                        elif rtype == "入金": total_deposited[i] += vals[i]
            except: continue

    calc_principals = [(base_principals[i] + total_earned[i] + total_deposited[i] - total_withdrawn[i]) if is_compound else (base_principals[i] + total_deposited[i] - total_withdrawn[i]) for i in range(num_people)]

    tab1, tab2 = st.tabs(["📈 収益確定・報告", "💸 入出金・精算"])

    with tab1:
        st.subheader("📊 本日の運用報告作成")
        total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
        uploaded_file = st.file_uploader("エビデンス画像をアップロード", type=['png', 'jpg', 'jpeg'])
        
        # 収益計算（表示用）
        today_yields = [round((p * (total_apr * 0.77 * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(calc_principals)]
        
        res_display = pd.DataFrame({
            "メンバー": member_names,
            "現在元本": [f"${p:,.2f}" for p in calc_principals],
            "本日収益": [f"${y:,.4f}" for y in today_yields]
        })
        st.table(res_display)

        if st.button("収益を確定してLINE送信"):
            # ※注意: pandasのCSV読み込みではスプレッドシートへの「書き込み」ができません。
            # 書き込みには Google Sheets API への認証か、以前使用していたGAS経由の送信が必要です。
            st.info("データ保存にはGoogle Sheets APIの設定が必要です。現在は表示とLINE送信のみ実行します。")
            
            if "line" in st.secrets:
                # LINE送信ロジック（そのまま維持）
                token = st.secrets["line"]["channel_access_token"]
                # LineIDシートからU...で始まるIDを抽出
                user_ids = [str(x).strip() for x in line_id_df.values.flatten() if str(x).startswith('U')]
                
                msg = f"🏦 【収益報告】\nプロジェクト: {selected_project}\nAPR: {total_apr}%\n"
                for i in range(num_people):
                    msg += f"・{member_names[i]}: +${today_yields[i]:,.4f}\n"
                
                success = 0
                for uid in set(user_ids):
                    if send_line_multimedia(token, uid, msg) == 200: success += 1
                st.success(f"{success}名に送信完了")

    with tab2:
        st.subheader("💸 入出金記録（手動管理用）")
        st.write("現在、このアプリからは表示のみ可能です。記録の追加はスプレッドシート本体で行ってください。")

except Exception as e:
    st.error(f"システムエラー: {e}")
