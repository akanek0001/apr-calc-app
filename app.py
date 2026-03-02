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
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == "nan": return ["-"] * n
    # カンマ、スペース、改行で分割
    items = [x.strip() for x in re.split(r'[,\s\n\r]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "-")
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
    
    # Settingsシートの読み込み（1行目を見出しとして認識）
    settings_df = pd.read_csv(f"{base_url}/export?format=csv&gid=0", header=0)
    settings_df.columns = settings_df.columns.str.strip() # 項目名の前後空白を削除

    if settings_df.empty:
        st.error("Settingsシートが空です。")
        st.stop()

    # プロジェクト選択（名前で検索）
    project_list = settings_df["Project_Name"].dropna().unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = settings_df[settings_df["Project_Name"] == selected_project].iloc[0]

    # --- 【重要】項目名（Name）でデータを抽出 ---
    num_people = int(to_f(p_info["Num_People"]))
    member_names = split_val(p_info["MemberNames"], num_people)
    base_principals = [to_f(p) for p in split_val(p_info["IndividualPrincipals"], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info["ProfitRates"], num_people)]
    is_compound = str(p_info["IsCompound"]).upper() in ["TRUE", "はい", "YES", "1"]

    # 履歴データの読み込み（履歴保存機能がある場合）
    try:
        hist_gid = st.secrets["gsheets"].get(f"{selected_project}_gid", "0")
        hist_df = pd.read_csv(f"{base_url}/export?format=csv&gid={hist_gid}")
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # 元本計算（複利/単利）
    total_earned = [0.0] * num_people
    if not hist_df.empty and "Breakdown" in hist_df.columns:
        for _, row in hist_df[hist_df["Type"] == "収益"].iterrows():
            vals = [to_f(v) for v in str(row["Breakdown"]).split(",")]
            for i in range(num_people):
                if i < len(vals): total_earned[i] += vals[i]

    calc_principals = [(base_principals[i] + total_earned[i]) if is_compound else base_principals[i] for i in range(num_people)]

    # --- 画面表示 ---
    st.subheader(f"📊 {selected_project} 運用報告作成")
    total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 収益計算（手数料 0.77 係数）
    today_yields = [round((p * (total_apr * 0.77 * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(calc_principals)]
    
    res_display = pd.DataFrame({
        "メンバー": member_names,
        "現在元本": [f"${p:,.2f}" for p in calc_principals],
        "分配比率": rate_list,
        "本日収益": [f"${y:,.4f}" for y in today_yields]
    })
    res_display.index = range(1, len(res_display) + 1)
    st.table(res_display)
    st.metric("総収益合計", f"${sum(today_yields):,.4f}")

    # LINE送信
    st.markdown("---")
    uploaded_file = st.file_uploader("エビデンス画像 (任意)", type=['png', 'jpg', 'jpeg'])
    
    if st.button("🚀 収益を確定して一斉送信", type="primary"):
        with st.spinner("送信中..."):
            msg = f"🏦 【{selected_project}】 収益報告\n"
            msg += f"📅 {datetime.now().strftime('%Y/%m/%d %H:%M')}\n"
            msg += f"📈 APR: {total_apr}%\n" + "-"*15 + "\n"
            for i in range(num_people):
                msg += f"・{member_names[i]}: +${today_yields[i]:,.4f}\n"
            msg += "-"*15 + "\n💰 合計: +${sum(today_yields):,.4f}"

            img_url = None
            if uploaded_file:
                try:
                    res = requests.post("https://api.imgbb.com/1/upload", params={"key": st.secrets["imgbb"]["api_key"]}, files={"image": uploaded_file.getvalue()})
                    img_url = res.json()["data"]["url"]
                except: st.warning("画像のアップロードに失敗しました。")

            # LineIDシートから送信先取得
            line_id_url = f"{base_url}/export?format=csv&gid={st.secrets['gsheets'].get('lineid_gid', '0')}"
            line_id_df = pd.read_csv(line_id_url)
            user_ids = [str(x).strip() for x in line_id_df.values.flatten() if str(x).startswith('U')]
            
            token = st.secrets["line"]["channel_access_token"]
            success = 0
            for uid in set(user_ids):
                if send_line_multimedia(token, uid, msg, img_url) == 200: success += 1
            st.success(f"{success}名に送信完了しました")

except Exception as e:
    st.error(f"システムエラー: {e}")
