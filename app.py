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
    except Exception as e:
        return str(e)

# --- 3. メインロジック ---
st.title("🏦 APR管理システム（送信デバッグ版）")

try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    # API制限回避のため ttl=60 に設定
    settings_df = conn.read(worksheet="Settings", ttl=60)
    line_id_df = conn.read(worksheet="LineID", ttl=60)
    
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

    # LINE ID取得 (LineIDシートからUから始まるIDを探す)
    user_ids = []
    if not line_id_df.empty:
        # シート全体から "U" で始まる文字列を検索
        all_cells = line_id_df.values.flatten().astype(str)
        user_ids = [str(x).strip() for x in all_cells if str(x).startswith('U')]

    # 履歴取得
    try:
        hist_df = conn.read(worksheet=selected_project, ttl=60)
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # 累計計算ロジック (簡略化)
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
        
        uploaded_file = st.file_uploader("運用画面のスクショをアップロード", type=['png', 'jpg', 'jpeg'])
        
        if uploaded_file:
            st.image(uploaded_file, caption="プレビュー", width=300)
            if st.button("画像を確定してURL生成"):
                try:
                    api_key = st.secrets["imgbb"]["api_key"]
                    files = {"image": uploaded_file.getvalue()}
                    res = requests.post("https://api.imgbb.com/1/upload", params={"key": api_key}, files=files)
                    st.session_state["img_url"] = res.json()["data"]["url"]
                    st.success(f"画像準備完了: {st.session_state['img_url']}")
                except:
                    st.error("ImgBBの設定エラー")

        if st.button("収益保存 ＆ LINE送信実行"):
            today_yields = [round((p * (total_apr * 0.67 * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(calc_principals)]
            
            # 保存
            new_row = pd.DataFrame([{"Date": datetime.now().strftime("%Y-%m-%d"), "Type": "収益", "Total_Amount": sum(today_yields), "Breakdown": ",".join(map(str, today_yields)), "Note": f"APR:{total_apr}%"}])
            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            conn.update(worksheet=selected_project, data=updated_hist)
            
            # --- 送信デバッグログを表示 ---
            st.divider()
            st.write("🔍 **送信ログ:**")
            
            if "line" not in st.secrets:
                st.error("Secretsに [line] セクションが見つかりません。")
            else:
                token = st.secrets["line"]["channel_access_token"]
                msg = f"🏦 【運用報告】\nAPR: {total_apr}%\n" + "\n".join([f"No.{i+1}: +${v:,.4f}" for i,v in enumerate(today_yields)])
                img_url = st.session_state.get("img_url")

                if not user_ids:
                    st.warning("LineIDシートから 'U' で始まるIDが見つかりませんでした。")
                
                success_count = 0
                for uid in user_ids:
                    status = send_line_multimedia(token, uid, msg, img_url)
                    if status == 200:
                        st.write(f"✅ 送信成功: {uid}")
                        success_count += 1
                    else:
                        st.error(f"❌ 送信失敗: {uid} (エラーコード: {status})")
                
                st.success(f"最終結果: {success_count}名に送信完了")
            st.rerun()

except Exception as e:
    st.error(f"システムエラー: {e}")
