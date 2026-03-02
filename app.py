import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import requests
import json
import re

# --- 1. ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- 2. ユーティリティ ---
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

def send_line_multimedia(token, user_id, text, image_url=None):
    """テキストと画像をLINEで送信"""
    if not user_id or str(user_id) == "nan": return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    
    # メッセージの組み立て
    messages = [{"type": "text", "text": text}]
    
    # 画像がある場合は追加（LINEの仕様上、公開URLが必要）
    if image_url:
        messages.append({
            "type": "image",
            "originalContentUrl": image_url,
            "previewImageUrl": image_url
        })
        
    payload = {"to": str(user_id), "messages": messages}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
        return res.status_code
    except: return 500

# --- 3. メインロジック ---
st.title("🏦 APR管理システム（画像送付対応版）")

try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings", ttl=0)
    line_id_df = conn.read(worksheet="LineID", ttl=0)
    
    if settings_df.empty:
        st.error("Settingsシートが空です。")
        st.stop()

    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = settings_df[settings_df.iloc[:, 0] == selected_project].iloc[0]

    num_people = int(to_f(p_info.iloc[1]))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"]

    user_ids = []
    if not line_id_df.empty:
        user_ids = line_id_df.iloc[:, -1].dropna().unique().tolist()

    try:
        hist_df = conn.read(worksheet=selected_project, ttl=0)
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    total_earned = [0.0] * num_people
    total_withdrawn = [0.0] * num_people
    if not hist_df.empty:
        for _, row in hist_df.iterrows():
            try:
                rtype, rbreakdown = str(row.iloc[1]), str(row.iloc[3])
                vals = [to_f(v) for v in rbreakdown.split(",")]
                for i in range(num_people):
                    if i < len(vals):
                        if rtype == "収益": total_earned[i] += vals[i]
                        elif rtype == "出金": total_withdrawn[i] += vals[i]
            except: continue

    calc_principals = [ (base_principals[i] + total_earned[i] - total_withdrawn[i]) if is_compound else base_principals[i] for i in range(num_people) ]

    tab1, tab2 = st.tabs(["📈 収益確定・画像送付", "💸 出金記録"])

    with tab1:
        st.subheader("本日の収益確定と証拠画像のアップロード")
        total_apr = st.number_input("本日のAPR (%)", value=100.0)
        
        # --- 画像アップロード欄 ---
        uploaded_file = st.file_uploader("運用画面のスクショをアップロードしてください", type=['png', 'jpg', 'jpeg'])
        if uploaded_file:
            st.image(uploaded_file, caption="送信予定のスクリーンショット", width=300)
        
        # 画像URLの入力欄（※LINE送信にはURLが必要なため、暫定的に手動URL入力またはImgBB等の連携が必要です）
        manual_img_url = st.text_input("画像の公開URL（お使いの画像ホスティング先URLがあれば入力してください）")

        today_yields = [round((p * (total_apr * 0.67 * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(calc_principals)]
        
        if st.button("収益保存 ＆ 画像付きLINE一斉通知"):
            # 保存処理
            new_row = pd.DataFrame([{"Date": datetime.now().strftime("%Y-%m-%d"), "Type": "収益", "Total_Amount": sum(today_yields), "Breakdown": ",".join(map(str, today_yields)), "Note": f"APR:{total_apr}%"}])
            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            conn.update(worksheet=selected_project, data=updated_hist)
            
            # LINE送信内容
            if "line" in st.secrets:
                token = st.secrets["line"]["channel_access_token"]
                now_str = datetime.now().strftime("%Y/%m/%d %H:%M")
                msg = f"🏦 【運用収益報告書】\nプロジェクト: {selected_project}\n日時: {now_str}\n"
                msg += f"━━━━━━━━━━━━━━\n本日のAPR: {total_apr}%\n"
                for i in range(num_people):
                    msg += f"・No.{i+1}: +${today_yields[i]:,.4f} (元本:${calc_principals[i]+today_yields[i]:,.2f})\n"
                msg += f"━━━━━━━━━━━━━━"

                success = 0
                for uid in user_ids:
                    # テキストと画像URLを送信
                    if send_line_multimedia(token, uid, msg, manual_img_url) == 200:
                        success += 1
                st.success(f"報告完了！ {success}名に送信しました。")
            st.rerun()

    with tab2:
        # 出金処理（以前のロジックと同じ）
        st.subheader("出金記録")
        # (中略: 前回のコードと同様)

except Exception as e:
    st.error(f"システムエラー: {e}")
