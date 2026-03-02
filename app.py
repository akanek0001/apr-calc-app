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
    if not user_id or str(user_id) == "nan": return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    messages = [{"type": "text", "text": text}]
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
st.title("🏦 APR管理システム（画像投稿完結型）")

try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings", ttl=0)
    line_id_df = conn.read(worksheet="LineID", ttl=0)
    
    if settings_df.empty:
        st.error("Settingsシートが空です。")
        st.stop()

    # プロジェクト選択
    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = settings_df[settings_df.iloc[:, 0] == selected_project].iloc[0]

    num_people = int(to_f(p_info.iloc[1]))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"]

    # LINE ID取得
    user_ids = []
    if not line_id_df.empty:
        user_ids = line_id_df.iloc[:, -1].dropna().unique().tolist()

    # 履歴取得
    try:
        hist_df = conn.read(worksheet=selected_project, ttl=0)
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # 累計計算
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

    calc_principals = [(base_principals[i] + total_earned[i] - total_withdrawn[i]) if is_compound else base_principals[i] for i in range(num_people)]

    tab1, tab2 = st.tabs(["📈 収益確定・報告", "💸 出金記録"])

    with tab1:
        st.subheader("本日の報告作成")
        total_apr = st.number_input("本日のAPR (%)", value=100.0)
        
        # --- 画像アップロード & URL変換 ---
        uploaded_file = st.file_uploader("運用画面のスクショをアップロード", type=['png', 'jpg', 'jpeg'])
        
        final_img_url = None
        if uploaded_file:
            st.image(uploaded_file, caption="プレビュー", width=300)
            if st.button("画像を確定してURL生成"):
                with st.spinner("LINE用に画像を変換中..."):
                    try:
                        api_key = st.secrets["imgbb"]["api_key"]
                        files = {"image": uploaded_file.getvalue()}
                        res = requests.post("https://api.imgbb.com/1/upload", params={"key": api_key}, files=files)
                        final_img_url = res.json()["data"]["url"]
                        st.session_state["img_url"] = final_img_url
                        st.success("画像の準備が整いました！")
                    except:
                        st.error("APIキーが無効、または未設定です。Secretsを確認してください。")

        # 送信ボタン
        if st.button("収益保存 ＆ 画像付きLINE一斉通知"):
            today_yields = [round((p * (total_apr * 0.67 * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(calc_principals)]
            
            # 1. 履歴保存
            new_row = pd.DataFrame([{"Date": datetime.now().strftime("%Y-%m-%d"), "Type": "収益", "Total_Amount": sum(today_yields), "Breakdown": ",".join(map(str, today_yields)), "Note": f"APR:{total_apr}%"}])
            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            conn.update(worksheet=selected_project, data=updated_hist)
            
            # 2. LINE送信
            if "line" in st.secrets:
                token = st.secrets["line"]["channel_access_token"]
                now_str = datetime.now().strftime("%Y/%m/%d %H:%M")
                msg = f"🏦 【運用収益報告書】\nプロジェクト: {selected_project}\n日時: {now_str}\n"
                msg += f"━━━━━━━━━━━━━━\n本日のAPR: {total_apr}%\n"
                for i in range(num_people):
                    msg += f"・No.{i+1}: +${today_yields[i]:,.4f} (元本:${calc_principals[i]+today_yields[i]:,.2f})\n"
                msg += f"━━━━━━━━━━━━━━"

                img_to_send = st.session_state.get("img_url")
                success = 0
                for uid in user_ids:
                    if send_line_multimedia(token, uid, msg, img_to_send) == 200:
                        success += 1
                st.success(f"報告完了！ {success}名に送信しました。")
                if "img_url" in st.session_state: del st.session_state["img_url"]
            st.rerun()

    with tab2:
        st.subheader("出金精算")
        # (前回同様の出金ロジック...)

except Exception as e:
    st.error(f"システムエラー: {e}")
