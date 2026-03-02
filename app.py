import streamlit as st
import pandas as pd
import requests
import json
import re
from datetime import datetime

# --- 1. ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- 2. ユーティリティ ---
def to_f(val):
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == "nan": return 0.0
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

# --- 3. メインロジック ---
st.title("💰 APR資産運用管理システム")

try:
    # GoogleシートのURL取得（secretsから）
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    
    # Settingsシートの読み込み (header=0 で1行目の項目名を正しく認識)
    df = pd.read_csv(f"{base_url}/export?format=csv&gid=0", header=0)
    
    # 項目名の空白を削除してクリーンにする
    df.columns = [str(c).strip() for c in df.columns]

    # プロジェクト選択 (A列: Project_Name)
    project_list = df.iloc[:, 0].dropna().unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    
    # 選択された行のデータを抽出
    p_info = df[df.iloc[:, 0] == selected_project].iloc[0]

    # --- 【重要】ご提示の項目順に基づく物理インデックス指定 ---
    # 0:Project_Name, 1:Num_People, 2:TotalPrincipal, 3:IndividualPrincipals, 
    # 4:ProfitRates, 5:IsCompound, 6:MemberNames
    
    num_people = int(to_f(p_info.iloc[1])) # B列
    member_names = split_val(p_info.iloc[6], num_people) # G列
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)] # D列
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)] # E列
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"] # F列

    # --- 計算と表示 ---
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
    st.metric("総収益合計", f"${sum(today_yields):,.4f}")

    # --- 4. LINE送信セクション ---
    st.markdown("---")
    uploaded_file = st.file_uploader("エビデンス画像 (任意)", type=['png', 'jpg', 'jpeg'])
    
    if st.button("🚀 LINE報告を一斉送信", type="primary"):
        with st.spinner("送信中..."):
            msg = f"🏦 【{selected_project}】 収益報告\n"
            msg += f"📅 {datetime.now().strftime('%Y/%m/%d %H:%M')}\n"
            msg += f"📈 APR: {total_apr}%\n" + "-"*15 + "\n"
            for i in range(num_people):
                msg += f"・{member_names[i]}: +${today_yields[i]:,.4f}\n"
            msg += "-"*15 + "\n💰 合計: +${sum(today_yields):,.4f}"

            # LINE IDの取得 (H列: LineID)
            # LineIDがカンマ区切りで入っている場合を想定
            raw_line_ids = str(p_info.iloc[7]) # H列
            user_ids = [uid.strip() for uid in re.split(r'[,\s\n\r]+', raw_line_ids) if uid.strip().startswith('U')]

            if not user_ids:
                st.warning("送信対象のLINE ID（Uから始まる文字列）が見つかりませんでした。")
            else:
                token = st.secrets["line"]["channel_access_token"]
                # (簡易的な送信ループ。本来は個別に送るかBroadcast)
                success = 0
                for uid in set(user_ids):
                    # LINE APIへのリクエスト（send_line_multimedia的な処理）
                    url = "https://api.line.me/v2/bot/message/push"
                    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
                    payload = {"to": uid, "messages": [{"type": "text", "text": msg}]}
                    res = requests.post(url, headers=headers, json=payload)
                    if res.status_code == 200: success += 1
                
                st.success(f"{success}名に送信完了しました")

except Exception as e:
    st.error(f"システムエラー: {e}")
    st.info("スプレッドシートの列順が A:Project_Name, B:Num_People, C:TotalPrincipal, D:IndividualPrincipals... になっているか再確認してください。")
