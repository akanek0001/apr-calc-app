import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime, timedelta  # timedeltaを追加
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
        messages.append({"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url})
    payload = {"to": str(user_id), "messages": messages}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
        return res.status_code
    except: return 500

# --- 3. メインロジック ---
st.title("🏦 APR資産運用管理システム")

try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings", ttl=60)
    line_id_df = conn.read(worksheet="LineID", ttl=60)
    
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
        all_cells = line_id_df.values.flatten().astype(str)
        user_ids = sorted(list(set([str(x).strip() for x in all_cells if str(x).startswith('U')])))

    try:
        hist_df = conn.read(worksheet=selected_project, ttl=60)
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

    calc_principals = [(base_principals[i] + total_earned[i] - total_withdrawn[i]) if is_compound else base_principals[i] for i in range(num_people)]

    tab1, tab2 = st.tabs(["📈 収益確定・報告", "💸 出金・精算"])

    with tab1:
        st.subheader("📊 本日の運用報告作成")
        total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
        uploaded_file = st.file_uploader("エビデンス画像をアップロード", type=['png', 'jpg', 'jpeg'])
        
        if uploaded_file:
            st.image(uploaded_file, caption="送信プレビュー", width=400)
            if st.button("画像を確定（URL生成）"):
                with st.spinner("変換中..."):
                    try:
                        res = requests.post("https://api.imgbb.com/1/upload", params={"key": st.secrets["imgbb"]["api_key"]}, files={"image": uploaded_file.getvalue()})
                        st.session_state["img_url"] = res.json()["data"]["url"]
                        st.success("画像準備完了")
                    except: st.error("ImgBBエラー")

        today_yields = [round((p * (total_apr * 0.67 * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(calc_principals)]

        if st.button("収益を確定してLINE送信"):
            # 保存
            new_row = pd.DataFrame([{"Date": datetime.now().strftime("%Y-%m-%d"), "Type": "収益", "Total_Amount": sum(today_yields), "Breakdown": ",".join(map(str, today_yields)), "Note": f"APR:{total_apr}%"}])
            conn.update(worksheet=selected_project, data=pd.concat([hist_df, new_row], ignore_index=True))
            
            # LINE送信
            if "line" in st.secrets:
                # --- 【重要】日本時間(JST)への修正 ---
                jst_now = datetime.utcnow() + timedelta(hours=9)
                now_str = jst_now.strftime("%Y/%m/%d %H:%M")
                # ------------------------------------
                
                msg = f"🏦 【資産運用収益報告書】\n━━━━━━━━━━━━━━\nプロジェクト: {selected_project}\n報告日時: {now_str}\n━━━━━━━━━━━━━━\n\n📈 本日の結果\nAPR: {total_apr}%\nモード: {'複利運用' if is_compound else '単利運用'}\n\n💰 収益明細\n"
                for i in range(num_people):
                    msg += f"・No.{i+1}: +${today_yields[i]:,.4f}\n  (元本: ${calc_principals[i]+today_yields[i]:,.2f})\n"
                msg += f"\n━━━━━━━━━━━━━━\n※画像エビデンスを添付いたします。\nご確認のほどお願い申し上げます。"

                img_url = st.session_state.get("img_url")
                success = 0
                for uid in user_ids:
                    if send_line_multimedia(st.secrets["line"]["channel_access_token"], uid, msg, img_url) == 200: success += 1
                st.success(f"{success}名に送信完了")
            st.rerun()

    with tab2:
        st.subheader("💸 出金・精算の記録")
        target_no = st.selectbox("メンバーを選択", [f"No.{i+1}" for i in range(num_people)])
        idx = int(target_no.split(".")[1]) - 1
        st.info(f"現在の出金可能額: **${calc_principals[idx]:,.2f}**")
        withdraw_amt = st.number_input("出金額 ($)", min_value=0.0, max_value=calc_principals[idx], step=10.0)
        memo = st.text_input("備考", value="出金精算")
        if st.button("出金データを保存"):
            if withdraw_amt > 0:
                w_list = [0.0] * num_people
                w_list[idx] = withdraw_amt
                new_row = pd.DataFrame([{"Date": datetime.now().strftime("%Y-%m-%d"), "Type": "出金", "Total_Amount": withdraw_amt, "Breakdown": ",".join(map(str, w_list)), "Note": memo}])
                conn.update(worksheet=selected_project, data=pd.concat([hist_df, new_row], ignore_index=True))
                st.success(f"記録完了")
                st.rerun()

except Exception as e:
    st.error(f"システムエラー: {e}")
