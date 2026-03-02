import streamlit as st
import pandas as pd
import requests
import re
from datetime import datetime

# --- 1. ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- 2. ユーティリティ関数 ---
def to_f(val):
    if pd.isna(val) or str(val).strip() == "": return 0.0
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

st.title("🏦 APR資産運用管理システム")

try:
    # --- 3. スプレッドシート読み込み設定（修正点） ---
    # SecretsからURLを取得
    raw_url = st.secrets["gsheets"]["public_gsheets_url"]
    base_url = raw_url.split('/edit')[0]
    
    # URLからgid（シート番号）を正規表現で確実に抽出
    gid_match = re.search(r'gid=([0-9]+)', raw_url)
    target_gid = gid_match.group(1) if gid_match else "0"
    
    # 💡 URLのgidを直接指定してCSVを取得（これで履歴シートの誤認を物理的に防ぐ）
    settings_url = f"{base_url}/export?format=csv&gid={target_gid}"
    
    # データを読み込み、1行目を見出しとして認識
    df = pd.read_csv(settings_url)
    df.columns = [str(c).strip().replace('"', '') for c in df.columns]

    # プロジェクト選択（Project_Name列が存在するか確認）
    if "Project_Name" not in df.columns:
        st.error(f"指定されたシート(gid={target_gid})に 'Project_Name' 列が見つかりません。シートが正しいか確認してください。")
        st.stop()

    project_list = df["Project_Name"].dropna().unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df[df["Project_Name"] == selected_project].iloc[0]

    # --- 4. データの抽出 ---
    num_p = int(to_f(p_info["Num_People"]))
    names = split_val(p_info["MemberNames"], num_p)
    principals = [to_f(p) for p in split_val(p_info["IndividualPrincipals"], num_p)]
    rates = [to_f(p_info["ProfitRates"]) if num_p == 1 else to_f(r) for r in split_val(p_info["ProfitRates"], num_p)]

    # --- 5. 計算と表示 ---
    st.subheader(f"📊 {selected_project} 本日の計算")
    total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 計算式: 元本 * (APR * 0.77 / 100) / 365
    yields = [round((p * (total_apr * 0.77 * rates[i] / 100)) / 365, 4) for i, p in enumerate(principals)]
    
    res_display = pd.DataFrame({
        "メンバー": names,
        "元本 ($)": [f"${p:,.2f}" for p in principals],
        "分配比率": rates,
        "本日収益 ($)": [f"${y:,.4f}" for y in yields]
    })
    res_display.index = range(1, len(res_display) + 1)
    st.table(res_display)
    st.metric("総収益合計", f"${sum(yields):,.4f}")

    # --- 6. LINE送信 ---
    st.markdown("---")
    if st.button("🚀 LINE報告を一斉送信", type="primary"):
        token = st.secrets["line"]["channel_access_token"]
        # LineID列から送信先を取得
        u_ids = [uid.strip() for uid in re.split(r'[,\s]+', str(p_info["LineID"])) if uid.strip().startswith('U')]
        
        if u_ids:
            msg = f"🏦 【{selected_project}】 収益報告\n📈 APR: {total_apr}%\n" + "-"*15 + "\n"
            for i in range(num_p):
                msg += f"・{names[i]}: +${yields[i]:,.4f}\n"
            
            success = 0
            for uid in set(u_ids):
                res = requests.post("https://api.line.me/v2/bot/message/push", 
                                    headers={"Authorization": f"Bearer {token}"},
                                    json={"to": uid, "messages": [{"type": "text", "text": msg}]})
                if res.status_code == 200: success += 1
            st.success(f"{success}名に送信完了しました")
        else:
            st.warning("有効なLINE IDが見つかりません。")

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
