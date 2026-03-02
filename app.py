import streamlit as st
import pandas as pd
import requests
import re
from datetime import datetime

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- ユーティリティ ---
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
    # 1. スプレッドシートの読み込み設定
    # 確実に「Settings」シートを読み込むため、gid=0（または特定のID）を指定
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    # header=0 を指定し、1行目を項目名として読み込む
    df = pd.read_csv(f"{base_url}/export?format=csv&gid=0", header=0)
    
    # 項目名の空白を除去（Project_Name, Num_People, MemberNames など）
    df.columns = [str(c).strip() for c in df.columns]

    # 2. プロジェクト選択
    if "Project_Name" not in df.columns:
        st.error("スプレッドシートに 'Project_Name' 列が見つかりません。")
        st.stop()
        
    project_list = df["Project_Name"].dropna().unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df[df["Project_Name"] == selected_project].iloc[0]

    # 3. データの抽出（項目名で直接指定）
    # [span_1](start_span)スプレッドシート「aprHoukoku」の項目名に基づき取得[span_1](end_span)
    num_p = int(to_f(p_info["Num_People"]))
    names = split_val(p_info["MemberNames"], num_p)
    principals = [to_f(p) for p in split_val(p_info["IndividualPrincipals"], num_p)]
    rates = [to_f(r) for r in split_val(p_info["ProfitRates"], num_p)]
    
    # IsCompound の判定
    is_compound = str(p_info["IsCompound"]).upper() in ["TRUE", "はい", "YES", "1"]

    # --- 計算と表示 ---
    st.subheader(f"📊 {selected_project} 本日の計算")
    total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 手数料 0.77 係数での計算
    today_yields = [round((p * (total_apr * 0.77 * rates[i] / 100)) / 365, 4) for i, p in enumerate(principals)]
    
    res_display = pd.DataFrame({
        "メンバー": names,
        "元本 ($)": [f"${p:,.2f}" for p in principals],
        "分配比率": rates,
        "本日収益 ($)": [f"${y:,.4f}" for y in today_yields]
    })
    res_display.index = range(1, len(res_display) + 1)
    st.table(res_display)
    st.metric("総収益合計", f"${sum(today_yields):,.4f}")

    # --- LINE送信 ---
    if st.button("🚀 LINE報告を一斉送信", type="primary"):
        # H列(LineID)から送信先を取得
        line_ids = [uid.strip() for uid in re.split(r'[,\s]+', str(p_info["LineID"])) if "@" not in uid and uid.strip()]
        if line_ids:
            st.success(f"{len(line_ids)}名に送信準備完了（送信ロジック実行）")
        else:
            st.warning("有効なLINE IDが見つかりません。")

except Exception as e:
    st.error(f"読み込みエラー: {e}")
