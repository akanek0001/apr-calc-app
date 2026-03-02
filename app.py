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
    if pd.isna(val) or str(val).strip() == "": return 0.0
    try:
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "": return ["0"] * n
    items = [x.strip() for x in re.split(r'[,\s\n\r]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

# --- 3. メインロジック ---
st.title("💰 APR管理システム 最終安定版")

try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    
    # シート読み込み
    settings_df = conn.read(worksheet="Settings", ttl=0)
    line_id_df = conn.read(worksheet="LineID", ttl=0)
    
    if settings_df.empty:
        st.error("Settingsシートが空です。")
        st.stop()

    # プロジェクト選択 (A列: Project_Name)
    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = settings_df[settings_df.iloc[:, 0] == selected_project].iloc[0]

    # 設定値の抽出
    num_people = int(to_f(p_info.iloc[1]))      # B列: 人数
    member_names = split_val(p_info.iloc[2], num_people) # C列: メンバー名
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)] # D列: 初期元本
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]       # E列: 配分比率
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"]  # F列: 複利判定

    # --- LINE IDの特定 (選択中プロジェクトに紐づくものだけ) ---
    user_ids = []
    if not line_id_df.empty:
        # LineIDシートのA列がProject_Name、B列がLineIDであると想定
        target_line_row = line_id_df[line_id_df.iloc[:, 0] == selected_project]
        if not target_line_row.empty:
            raw_ids = str(target_line_row.iloc[0, 1])
            user_ids = list(set(re.findall(r'U[a-fA-F0-9]{32}', raw_ids)))

    # 履歴読み込み
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
                rtype, rbreakdown = str(row["Type"]), str(row["Breakdown"])
                vals = [to_f(v) for v in rbreakdown.split(",")]
                for i in range(num_people):
                    if i < len(vals):
                        if rtype == "収益": total_earned[i] += vals[i]
                        elif rtype == "出金": total_withdrawn[i] += vals[i]
            except: continue

    # 元本計算
    calc_principals = [(base_principals[i] + total_earned[i] - total_withdrawn[i] if is_compound else base_principals[i]) for i in range(num_people)]

    # --- UI表示 ---
    st.sidebar.info(f"計算モード: {'✅ 複利' if is_compound else '💡 単利'}")
    tab1, tab2 = st.tabs(["📈 収益の確定", "💸 出金の記録"])

    with tab1:
        st.subheader(f"📊 {selected_project} 本日の収益確定")
        total_apr = st.number_input("本日の全体のAPR (%)", value=100.0, step=0.01)
        net_factor = 0.77  # 係数 0.77 を適用
        
        today_yields = [round((p * (total_apr * net_factor * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(calc_principals)]
        
        res_df = pd.DataFrame({
            "メンバー": member_names,
            "計算元本": [f"${p:,.2f}" for p in calc_principals],
            "配分比率": rate_list,
            "本日収益": [f"${y:,.4f}" for y in today_yields]
        })
        st.table(res_df)
        st.metric("総収益合計", f"${sum(today_yields):,.4f}")

        # 画像アップロード
        uploaded_file = st.file_uploader("🖼️ エビデンス画像(任意)", type=['png', 'jpg', 'jpeg'])

        if st.button("🚀 収益を保存してLINE通知", type="primary"):
            # 1. 保存処理
            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "収益",
                "Total_Amount": sum(today_yields),
                "Breakdown": ",".join(map(str, today_yields)),
                "Note": f"APR:{total_apr}%"
            }])
            conn.update(worksheet=selected_project, data=pd.concat([hist_df, new_row], ignore_index=True))
            
            # 2. 画像アップロード (ImgBB)
            img_url = None
            if uploaded_file and "imgbb" in st.secrets:
                res_img = requests.post("https://api.imgbb.com/1/upload", 
                                        params={"key": st.secrets["imgbb"]["api_key"]}, 
                                        files={"image": uploaded_file.getvalue()})
                if res_img.status_code == 200:
                    img_url = res_img.json()["data"]["url"]

            # 3. LINE送信
            if "line" in st.secrets and user_ids:
                token = st.secrets["line"]["channel_access_token"]
                msg = f"🏦 【{selected_project}】 収益報告\n📈 APR: {total_apr}%\n"
                msg += f"🔄 モード: {'複利' if is_compound else '単利'}\n" + "-"*15 + "\n"
                for i in range(num_people):
                    msg += f"・{member_names[i]}: +${today_yields[i]:,.4f}\n"
                msg += "-"*15 + f"\n💰 合計: +${sum(today_yields):,.4f}"

                success = 0
                for uid in user_ids:
                    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
                    msgs = [{"type": "text", "text": msg}]
                    if img_url:
                        msgs.append({"type": "image", "originalContentUrl": img_url, "previewImageUrl": img_url})
                    
                    res = requests.post("https://api.line.me/v2/bot/message/push", 
                                        headers=headers, json={"to": uid, "messages": msgs})
                    if res.status_code == 200: success += 1
                st.success(f"{success}名に送信完了")
            
            st.rerun()

    with tab2:
        st.subheader("💸 出金の記録")
        target_name = st.selectbox("メンバーを選択", member_names)
        idx = member_names.index(target_name)
        st.warning(f"{target_name} さんの出金可能額: ${calc_principals[idx]:,.2f}")
        amt = st.number_input("出金額 ($)", min_value=0.0, max_value=calc_principals[idx], step=1.0)
        
        if st.button("出金を確定する"):
            if amt > 0:
                withdrawals = [0.0] * num_people
                withdrawals[idx] = amt
                new_row = pd.DataFrame([{
                    "Date": datetime.now().strftime("%Y-%m-%d"),
                    "Type": "出金",
                    "Total_Amount": amt,
                    "Breakdown": ",".join(map(str, withdrawals)),
                    "Note": f"{target_name} Withdrawal"
                }])
                conn.update(worksheet=selected_project, data=pd.concat([hist_df, new_row], ignore_index=True))
                st.success("出金を記録しました。")
                st.rerun()

except Exception as e:
    st.error(f"システムエラー: {e}")
