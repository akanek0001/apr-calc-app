import streamlit as st
import pandas as pd
import requests
import re
from datetime import datetime

# --- 1. ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- 2. ユーティリティ関数 ---
def to_f(val):
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == "nan": return 0.0
    try:
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == "nan": return ["-"] * n
    items = [x.strip() for x in re.split(r'[,\s\n\r]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "-")
    return items[:n]

st.title("💰 APR資産運用管理システム")

try:
    # --- 3. スプレッドシート読み込み (2枚のシートを参照) ---
    raw_url = st.secrets["gsheets"]["public_gsheets_url"]
    base_url = raw_url.split('/edit')[0]
    
    # ① Settingsシート (GID: 465185900) - プロジェクト構成データ
    settings_gid = "465185900" 
    df_settings = pd.read_csv(f"{base_url}/export?format=csv&gid={settings_gid}", header=0)
    df_settings.columns = [str(c).strip() for c in df_settings.columns]

    # ② LineIDシート - 送信先データ
    line_id_url = f"{base_url}/gviz/tq?tqx=out:csv&sheet=LineID"
    df_line = pd.read_csv(line_id_url, header=0)
    df_line.columns = [str(c).strip() for c in df_line.columns]

    # プロジェクト選択
    project_list = df_settings["Project_Name"].dropna().unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df_settings[df_settings["Project_Name"] == selected_project].iloc[0]

    # --- 4. データの抽出 (複利フラグ含む) ---
    num_p = int(to_f(p_info["Num_People"]))
    member_names = split_val(p_info["MemberNames"], num_p)
    base_principals = [to_f(p) for p in split_val(p_info["IndividualPrincipals"], num_p)]
    rate_list = [to_f(p_info["ProfitRates"]) if num_p == 1 else to_f(r) for r in split_val(p_info["ProfitRates"], num_p)]
    
    # 複利(IsCompound)の判定
    is_compound = str(p_info["IsCompound"]).upper() in ["TRUE", "はい", "YES", "1"]

    # --- 5. 計算と表示 ---
    st.subheader(f"📊 {selected_project} 本日の収益計算")
    total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 収益計算 (手数料 0.77 係数)
    today_yields = [round((p * (total_apr * 0.77 * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(base_principals)]
    
    res_display = pd.DataFrame({
        "メンバー": member_names,
        "元本 ($)": [f"${p:,.2f}" for p in base_principals],
        "分配比率": rate_list,
        "本日収益 ($)": [f"${y:,.4f}" for y in today_yields]
    })
    res_display.index = range(1, len(res_display) + 1)
    st.table(res_display)
    
    col1, col2 = st.columns(2)
    col1.metric("総収益合計", f"${sum(today_yields):,.4f}")
    col2.info(f"複利運用設定: {'ON' if is_compound else 'OFF'}")

    # --- 6. 画像アップロード & LINE送信 (LineIDシート参照) ---
    st.markdown("---")
    uploaded_file = st.file_uploader("🖼️ エビデンス画像をアップロード", type=['png', 'jpg', 'jpeg'])
    
    if st.button("🚀 LINE報告を一斉送信", type="primary"):
        with st.spinner("送信中..."):
            line_token = st.secrets["line"]["channel_access_token"]
            imgbb_api_key = st.secrets["imgbb"]["api_key"]
            
            # メッセージ本文作成
            msg = f"🏦 【{selected_project}】 収益報告\n📈 APR: {total_apr}%\n"
            msg += f"🔄 複利運用: {'あり' if is_compound else 'なし'}\n" + "-"*15 + "\n"
            for i in range(num_p):
                msg += f"・{member_names[i]}: +${today_yields[i]:,.4f}\n"
            msg += "-"*15 + "\n💰 合計: +${sum(today_yields):,.4f}"

            # ImgBBへの画像アップロード
            img_url = None
            if uploaded_file:
                try:
                    res_img = requests.post("https://api.imgbb.com/1/upload", 
                                            params={"key": imgbb_api_key}, 
                                            files={"image": uploaded_file.getvalue()})
                    if res_img.status_code == 200:
                        img_url = res_img.json()["data"]["url"]
                except:
                    st.warning("画像のアップロードに失敗しました。")

            # LineIDシートから該当プロジェクトのIDを抽出
            if "Project_Name" in df_line.columns and "LineID" in df_line.columns:
                target_row = df_line[df_line["Project_Name"] == selected_project]
                if not target_row.empty:
                    raw_ids = str(target_row.iloc[0]["LineID"])
                    # Uから始まる33文字のIDをすべて抽出
                    user_ids = list(set(re.findall(r'U[a-fA-F0-9]{32}', raw_ids)))
                    
                    if user_ids:
                        success_count = 0
                        for uid in user_ids:
                            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {line_token}"}
                            messages = [{"type": "text", "text": msg}]
                            if img_url:
                                messages.append({"type": "image", "originalContentUrl": img_url, "previewImageUrl": img_url})
                            
                            payload = {"to": uid, "messages": messages}
                            res = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json=payload)
                            if res.status_code == 200: success_count += 1
                        st.success(f"{success_count} 名のメンバーに報告を送信しました。")
                    else:
                        st.error("LineIDシート内に有効なIDが見つかりませんでした。")
                else:
                    st.error(f"LineIDシートにプロジェクト名 '{selected_project}' が登録されていません。")
            else:
                st.error("LineIDシートの列名を確認してください（Project_Name, LineID）。")

except Exception as e:
    st.error(f"システムエラー: {e}")
