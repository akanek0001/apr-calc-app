import streamlit as st
import pandas as pd
import requests
import re
from datetime import datetime

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
    if pd.isna(val) or str(val).strip() == "": return ["-"] * n
    items = [x.strip() for x in re.split(r'[,\s\n\r]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "-")
    return items[:n]

st.title("💰 APR管理システム (直通版)")

# --- 3. データ読み込み (401エラーを回避する直接取得方式) ---
# スプレッドシートが「全員に公開」されていれば、認証なしで100%読み込めます
SHEET_ID = "11RHXA2q7Bd_0Pa8Ao33qYdlCWSF4xW6AKDV_Fe-ZBhA"

try:
    # Settingsシート (最初のシート: gid=0) を直接CSVとして読み込む
    settings_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=0"
    df_settings = pd.read_csv(settings_url)
    df_settings.columns = [str(c).strip() for c in df_settings.columns]

    # LineIDシートを「シート名」指定でCSVとして読み込む
    line_id_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=LineID"
    df_line = pd.read_csv(line_id_url)
    df_line.columns = [str(c).strip() for c in df_line.columns]

    # プロジェクト選択
    p_col = df_settings.columns[0]
    project_list = df_settings[p_col].dropna().unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df_settings[df_settings[p_col] == selected_project].iloc[0]

    # データ抽出
    num_p = int(to_f(p_info.iloc[1]))
    member_names = split_val(p_info.iloc[2], num_p)
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_p)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_p)]
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"]

    # --- 4. 計算と表示 ---
    st.subheader(f"📊 {selected_project} 本日の収益確定")
    total_apr = st.number_input("本日の全体のAPR (%)", value=100.0, step=0.01)
    
    # 0.77係数で計算
    today_yields = [round((p * (total_apr * 0.77 * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(base_principals)]
    
    st.table(pd.DataFrame({
        "メンバー": member_names,
        "元本 ($)": [f"${p:,.2f}" for p in base_principals],
        "本日収益 ($)": [f"${y:,.4f}" for y in today_yields]
    }))
    
    st.info(f"運用モード: {'複利' if is_compound else '単利'}")

    # --- 5. 画像 & LINE送信 ---
    st.markdown("---")
    uploaded_file = st.file_uploader("🖼️ 画像をアップロード", type=['png', 'jpg', 'jpeg'])
    
    if st.button("🚀 LINE報告を一斉送信", type="primary"):
        with st.spinner("送信中..."):
            line_token = st.secrets["line"]["channel_access_token"]
            imgbb_key = st.secrets["imgbb"]["api_key"]
            
            # メッセージ本文
            msg = f"🏦 【{selected_project}】 収益報告\n📈 APR: {total_apr}%\n" + "-"*15 + "\n"
            for i in range(num_p):
                msg += f"・{member_names[i]}: +${today_yields[i]:,.4f}\n"
            msg += "-"*15 + f"\n💰 合計: +${sum(today_yields):,.4f}"

            # ImgBB
            img_url = None
            if uploaded_file:
                res_img = requests.post("https://api.imgbb.com/1/upload", params={"key": imgbb_key}, files={"image": uploaded_file.getvalue()})
                if res_img.status_code == 200: img_url = res_img.json()["data"]["url"]

            # 送信先取得
            target_row = df_line[df_line.iloc[:, 0] == selected_project]
            if not target_row.empty:
                raw_ids = str(target_row.iloc[0, 1])
                user_ids = list(set(re.findall(r'U[a-fA-F0-9]{32}', raw_ids)))
                
                for uid in user_ids:
                    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {line_token}"}
                    payload = {"to": uid, "messages": [{"type": "text", "text": msg}]}
                    if img_url:
                        payload["messages"].append({"type": "image", "originalContentUrl": img_url, "previewImageUrl": img_url})
                    requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json=payload)
                st.success("LINE送信完了しました。")

except Exception as e:
    st.error(f"接続エラー: {e}")
