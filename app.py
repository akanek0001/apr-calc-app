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

st.title("💰 APR資産運用管理システム")

try:
    # --- 3. スプレッドシート読み込み（GID指定） ---
    raw_url = st.secrets["gsheets"]["public_gsheets_url"]
    base_url = raw_url.split('/edit')[0]
    
    # URLからgid（465185900）を自動抽出
    gid_match = re.search(r'gid=([0-9]+)', raw_url)
    target_gid = gid_match.group(1) if gid_match else "0"
    
    # CSVエクスポート
    settings_url = f"{base_url}/export?format=csv&gid={target_gid}"
    df = pd.read_csv(settings_url, header=0)
    df.columns = [str(c).strip().replace('"', '') for c in df.columns]

    # プロジェクト選択
    project_list = df["Project_Name"].dropna().unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = df[df["Project_Name"] == selected_project].iloc[0]

    # --- 4. データの抽出（複利フラグ含む） ---
    num_p = int(to_f(p_info["Num_People"]))
    names = split_val(p_info["MemberNames"], num_p)
    base_principals = [to_f(p) for p in split_val(p_info["IndividualPrincipals"], num_p)]
    rates = [to_f(p_info["ProfitRates"]) if num_p == 1 else to_f(r) for r in split_val(p_info["ProfitRates"], num_p)]
    
    # 複利(IsCompound)の判定
    is_compound = str(p_info["IsCompound"]).upper() in ["TRUE", "はい", "YES", "1"]

    # --- 5. 計算と表示 ---
    st.subheader(f"📊 {selected_project} 本日の計算")
    total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    
    # 収益計算 (手数料 0.77 係数)
    yields = [round((p * (total_apr * 0.77 * rates[i] / 100)) / 365, 4) for i, p in enumerate(base_principals)]
    
    # 複利の場合の表示用元本（もし必要なら計算後の数値を出すなど調整可能）
    display_principals = base_principals 
    
    res_display = pd.DataFrame({
        "メンバー": names,
        "元本 ($)": [f"${p:,.2f}" for p in display_principals],
        "分配比率": rates,
        "本日収益 ($)": [f"${y:,.4f}" for y in yields]
    })
    res_display.index = range(1, len(res_display) + 1)
    st.table(res_display)
    
    col1, col2 = st.columns(2)
    col1.metric("総収益合計", f"${sum(yields):,.4f}")
    col2.info(f"複利設定: {'ON' if is_compound else 'OFF'}")

    # --- 6. 画像アップロード & LINE送信 ---
    st.markdown("---")
    uploaded_file = st.file_uploader("🖼️ エビデンス画像をアップロード", type=['png', 'jpg', 'jpeg'])
    
    if st.button("🚀 LINE報告を一斉送信", type="primary"):
        with st.spinner("送信中..."):
            token = st.secrets["line"]["channel_access_token"]
            
            # メッセージ作成
            msg = f"🏦 【{selected_project}】 収益報告\n📈 APR: {total_apr}%\n"
            msg += f"🔄 複利運用: {'あり' if is_compound else 'なし'}\n" + "-"*15 + "\n"
            for i in range(num_p):
                msg += f"・{names[i]}: +${yields[i]:,.4f}\n"
            msg += "-"*15 + "\n💰 合計: +${sum(yields):,.4f}"

            # ImgBBへの画像アップロード
            img_url = None
            if uploaded_file:
                try:
                    res_img = requests.post("https://api.imgbb.com/1/upload", 
                                            params={"key": st.secrets["imgbb"]["api_key"]}, 
                                            files={"image": uploaded_file.getvalue()})
                    img_url = res_img.json()["data"]["url"]
                except:
                    st.warning("画像のアップロードに失敗しました。")

            # LINE送信処理
            u_ids = [uid.strip() for uid in re.split(r'[,\s]+', str(p_info["LineID"])) if uid.strip().startswith('U')]
            if u_ids:
                success = 0
                for uid in set(u_ids):
                    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
                    messages = [{"type": "text", "text": msg}]
                    if img_url:
                        messages.append({"type": "image", "originalContentUrl": img_url, "previewImageUrl": img_url})
                    
                    payload = {"to": uid, "messages": messages}
                    res = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json=payload)
                    if res.status_code == 200: success += 1
                st.success(f"{success}名に送信完了しました")
            else:
                st.error("有効なLINE IDが見つかりません。")

except Exception as e:
    st.error(f"システムエラー: {e}")
