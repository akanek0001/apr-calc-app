import streamlit as st
import pandas as pd
import requests
import re

# --- 1. ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide")
st.title("💰 APR資産運用管理システム")

try:
    # SecretsからURLを取得し、シート番号(gid)を特定
    raw_url = st.secrets["gsheets"]["public_gsheets_url"]
    base_url = raw_url.split('/edit')[0]
    
    # URL末尾の gid= 以降の数字を確実に取得
    if "gid=" in raw_url:
        gid = re.search(r'gid=([0-9]+)', raw_url).group(1)
    else:
        gid = "0"
    
    # 【最重要】指定されたgidのシートをピンポイントで読み込む
    csv_url = f"{base_url}/export?format=csv&gid={gid}"
    df = pd.read_csv(csv_url, header=0)
    
    # 列名の空白を除去
    df.columns = [str(c).strip() for c in df.columns]

    # プロジェクト選択 (Project_Name列を使用)
    if "Project_Name" not in df.columns:
        st.error(f"エラー: 指定されたシートに 'Project_Name' 列がありません。")
        st.write("現在の列名:", df.columns.tolist())
        st.stop()

    project_list = df["Project_Name"].dropna().unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df[df["Project_Name"] == selected_project].iloc[0]

    # データ抽出
    num_p = int(float(str(p_info["Num_People"]).replace(',','')) if pd.notna(p_info["Num_People"]) else 0)
    
    def split_val(val, n):
        items = [x.strip() for x in re.split(r'[,\s\n\r]+', str(val)) if x.strip()]
        while len(items) < n: items.append(items[-1] if items else "0")
        return items[:n]

    names = split_val(p_info["MemberNames"], num_p)
    principals = [float(str(p).replace(',','').replace('$','')) for p in split_val(p_info["IndividualPrincipals"], num_p)]
    rates = [float(str(r).replace('%','')) for r in split_val(p_info["ProfitRates"], num_p)]

    # 収益計算
    st.subheader(f"📊 {selected_project} 本日の収益計算")
    apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    yields = [round((p * (apr * 0.77 * rates[i] / 100)) / 365, 4) for i, p in enumerate(principals)]
    
    res_df = pd.DataFrame({
        "メンバー": names,
        "元本 ($)": [f"${p:,.2f}" for p in principals],
        "分配比率": rates,
        "本日収益 ($)": [f"${y:,.4f}" for y in yields]
    })
    st.table(res_df)
    st.metric("総収益合計", f"${sum(yields):,.4f}")

    # LINE送信
    if st.button("🚀 LINE報告を一斉送信", type="primary"):
        token = st.secrets["line"]["channel_access_token"]
        line_ids = [uid.strip() for uid in re.split(r'[,\s]+', str(p_info["LineID"])) if uid.strip().startswith('U')]
        
        if line_ids:
            msg = f"🏦 【{selected_project}】 収益報告\n📈 APR: {apr}%\n" + "-"*15 + "\n"
            for i in range(num_p): msg += f"・{names[i]}: +${yields[i]:,.4f}\n"
            
            success = 0
            for uid in set(line_ids):
                res = requests.post("https://api.line.me/v2/bot/message/push", 
                                    headers={"Authorization": f"Bearer {token}"},
                                    json={"to": uid, "messages": [{"type": "text", "text": msg}]})
                if res.status_code == 200: success += 1
            st.success(f"{success}名に送信完了")

except Exception as e:
    st.error(f"システムエラー: {e}")
