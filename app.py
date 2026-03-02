import streamlit as st
import pandas as pd
import requests
import re

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide")

st.title("🏦 APR資産運用管理システム (接続確認版)")

try:
    # 1. スプレッドシートのベースURL取得
    base_url = st.secrets["gsheets"]["public_gsheets_url"].split('/edit')[0]
    
    # 2. Settingsシートを明示的に読み込む
    # もし Settingsシートが一番左（gid=0）でない場合は、SecretsのURLの末尾のgidを確認してください
    settings_url = f"{base_url}/export?format=csv&gid=0"
    df = pd.read_csv(settings_url, header=0)
    df.columns = [str(c).strip() for c in df.columns]

    # --- 【確認用】現在読み込んでいるシートの項目名を表示 ---
    with st.expander("🛠️ 接続中のシート構成を確認（エラー時用）"):
        st.write("現在読み込んでいる列の名前:", df.columns.tolist())
        st.write("データの先頭1行:", df.iloc[0].to_dict())

    # 3. 正しい列（Project_Name）からリストを作る
    if "Project_Name" in df.columns:
        # 正しいシートを読んでいる場合
        project_list = df["Project_Name"].dropna().unique().tolist()
    else:
        # もし Project_Name が見つからない（別のシートを読んでいる）場合
        st.error("⚠️ Settingsシートが見つかりません。一番左のタブが『Settings』になっていますか？")
        st.info(f"現在の1列目の名前: {df.columns[0]}")
        st.stop()

    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df[df["Project_Name"] == selected_project].iloc[0]

    # --- 4. データの抽出（名前で指定） ---
    num_p = int(pd.to_numeric(p_info["Num_People"], errors='coerce') or 0)
    
    def split_data(val, n):
        items = [x.strip() for x in re.split(r'[,\s\n\r]+', str(val)) if x.strip()]
        while len(items) < n: items.append(items[-1] if items else "0")
        return items[:n]

    names = split_data(p_info["MemberNames"], num_p)
    # IndividualPrincipals という項目名で取得
    principals = [float(str(p).replace(',','')) for p in split_data(p_info["IndividualPrincipals"], num_p)]
    rates = [float(str(r).replace('%','')) for r in split_data(p_info["ProfitRates"], num_p)]

    # --- 表示 ---
    st.subheader(f"📊 {selected_project} 本日の収益計算")
    apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 計算 (手数料 0.77)
    yields = [round((p * (apr * 0.77 * rates[i] / 100)) / 365, 4) for i, p in enumerate(principals)]
    
    res_df = pd.DataFrame({
        "メンバー": names,
        "元本 ($)": [f"${p:,.2f}" for p in principals],
        "分配比率": rates,
        "本日収益 ($)": [f"${y:,.4f}" for y in yields]
    })
    st.table(res_df)

except Exception as e:
    st.error(f"読み込みエラー: {e}")
