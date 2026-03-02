import streamlit as st
import pandas as pd
import requests
import json
import re
from datetime import datetime

# --- 1. ページ基本設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- 2. 補助関数 ---
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

# --- 3. メインロジック ---
st.title("💰 APR資産運用管理システム")

try:
    # GoogleシートのURL取得
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    
    # 【最重要】header=0 で1行目を項目名として固定。これで見出しの読み込みミスを防ぎます。
    df = pd.read_csv(f"{base_url}/export?format=csv&gid=0", header=0)
    
    # 列名の前後空白を削除し、大文字小文字を無視して一致しやすくする
    df.columns = [str(c).strip() for c in df.columns]

    # プロジェクト名が A列（Project_Name）にあることを前提にリスト化
    # 日付列などを拾わないよう、列名を直接指定して抽出します
    if "Project_Name" in df.columns:
        project_list = df["Project_Name"].dropna().unique().tolist()
    else:
        # 万が一列名が認識できない場合は1列目(0番目)を使用
        project_list = df.iloc[:, 0].dropna().unique().tolist()

    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    
    # 選択したプロジェクトの行を特定
    p_info = df[df.iloc[:, 0] == selected_project].iloc[0]

    # --- あなたのシート項目名に完全準拠 ---
    num_p = int(to_f(p_info["Num_People"]))
    names = split_val(p_info["MemberNames"], num_p)
    principals = [to_f(p) for p in split_val(p_info["IndividualPrincipals"], num_p)]
    rates = [to_f(r) for r in split_val(p_info["ProfitRates"], num_p)]

    # --- 画面表示 ---
    st.subheader(f"📊 {selected_project} 本日の計算")
    apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 計算 (手数料 0.77)
    yields = [round((p * (apr * 0.77 * rates[i] / 100)) / 365, 4) for i, p in enumerate(principals)]
    
    res_df = pd.DataFrame({
        "メンバー": names,
        "元本 ($)": [f"${p:,.2f}" for p in principals],
        "分配比率": rates,
        "本日収益 ($)": [f"${y:,.4f}" for y in yields]
    })
    res_df.index = range(1, len(res_df) + 1)
    st.table(res_df)
    st.metric("総収益合計", f"${sum(yields):,.4f}")

    # LINE送信 (H列: LineID)
    if st.button("🚀 LINE報告送信", type="primary"):
        line_ids = [uid.strip() for uid in re.split(r'[,\s]+', str(p_info["LineID"])) if uid.strip().startswith('U')]
        if line_ids:
            token = st.secrets["line"]["channel_access_token"]
            msg = f"🏦 【{selected_project}】収益報告\n" + "-"*10 + f"\n合計: ${sum(yields):,.4f}"
            for uid in set(line_ids):
                requests.post("https://api.line.me/v2/bot/message/push", 
                              headers={"Authorization": f"Bearer {token}"},
                              json={"to": uid, "messages": [{"type": "text", "text": msg}]})
            st.success("送信完了")

except Exception as e:
    st.error(f"システムエラー: {e}")
