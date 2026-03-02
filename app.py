import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import re

# --- 1. ページ基本設定 ---
st.set_page_config(page_title="APRプロ管理システム", layout="wide", page_icon="📈")

# --- 2. 汎用計算・通信ユーティリティ ---
def to_f(val):
    if pd.isna(val): return 0.0
    try:
        clean = str(val).replace(',','').replace('$','').replace('%','').strip()
        return float(clean) if clean else 0.0
    except: return 0.0

def split_val(val, n):
    if pd.isna(val) or str(val).strip() == "": return ["0"] * n
    items = [x.strip() for x in re.split(r'[,\s]+', str(val)) if x.strip()]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

def send_line_multimedia(token, user_id, text, image_url=None):
    if not user_id or str(user_id) == "nan": return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    messages = [{"type": "text", "text": text}]
    if image_url:
        messages.append({"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url})
    payload = {"to": str(user_id), "messages": messages}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
        return res.status_code
    except: return 500

# --- 3. メインシステム ---
st.title("🏦 APR資産運用管理システム Pro")

try:
    # --- データ接続 ---
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings", ttl=60)
    line_id_df = conn.read(worksheet="LineID", ttl=60)
    
    # --- サイドバー管理画面 ---
    st.sidebar.header("⚙️ システム管理")
    
    # 3. 接続ステータス表示
    st.sidebar.subheader("📡 接続ステータス")
    line_ok = "line" in st.secrets and st.secrets["line"].get("channel_access_token")
    imgbb_ok = "imgbb" in st.secrets and st.secrets["imgbb"].get("api_key")
    st.sidebar.write(f"{'🟢' if line_ok else '🔴'} LINE API")
    st.sidebar.write(f"{'🟢' if imgbb_ok else '🔴'} ImgBB API")

    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクト選択", project_list)
    
    # ダウンロード機能
    st.sidebar.markdown("---")
    raw_url = st.secrets["gsheets"]["public_gsheets_url"]
    export_url = raw_url.replace("/edit?usp=sharing", "/export?format=xlsx")
    st.sidebar.link_button("📥 Excelバックアップ", export_url)

    # データ展開
    p_info = settings_df[settings_df.iloc[:, 0] == selected_project].iloc[0]
    num_people = int(to_f(p_info.iloc[1]))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"]
    display_names = split_val(str(p_info.iloc[6]), num_people)

    # LINE宛先取得
    user_ids = []
    if not line_id_df.empty:
        all_cells = line_id_df.values.flatten().astype(str)
        user_ids = sorted(list(set([x.strip() for x in all_cells if x.startswith('U')])))

    # 履歴取得
    try:
        hist_df = conn.read(worksheet=selected_project, ttl=60)
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # 収益集計
    total_earned = [0.0] * num_people
    total_withdrawn = [0.0] * num_people
    total_deposited = [0.0] * num_people
    if not hist_df.empty:
        for _, row in hist_df.iterrows():
            try:
                rtype, rbreakdown = str(row.iloc[1]), str(row.iloc[3])
                vals = [to_f(v) for v in rbreakdown.split(",")]
                for i in range(num_people):
                    if i < len(vals):
                        if rtype == "収益": total_earned[i] += vals[i]
                        elif rtype == "出金": total_withdrawn[i] += vals[i]
                        elif rtype == "入金": total_deposited[i] += vals[i]
            except: continue

    calc_principals = [(base_principals[i] + total_earned[i] + total_deposited[i] - total_withdrawn[i]) if is_compound else (base_principals[i] + total_deposited[i] - total_withdrawn[i]) for i in range(num_people)]

    # --- メインタブ構成 ---
    tab1, tab2, tab3 = st.tabs(["📈 収益報告", "💸 入出金管理", "📊 分析ダッシュボード"])

    # --- タブ1: 収益報告 (プレビュー機能付き) ---
    with tab1:
        st.subheader("日次収益の確定")
        COEFF = 0.77
        total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
        uploaded_file = st.file_uploader("エビデンス画像アップロード", type=['png', 'jpg', 'jpeg'])
        
        # 収益計算
        today_yields = [round((p * (total_apr * COEFF * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(calc_principals)]
        
        # 2. プレビューの構築
        st.markdown("---")
        st.subheader("📝 送信プレビュー確認")
        jst_now = datetime.utcnow() + timedelta(hours=9)
        now_str = jst_now.strftime("%Y/%m/%d %H:%M")
        
        preview_text = f"🏦 【{selected_project} 収益報告】\n━━━━━━━━━━━━━━\n報告日時: {now_str}\nAPR: {total_apr}%\n━━━━━━━━━━━━━━\n\n💰 メンバー別収益\n"
        for i in range(num_people):
            preview_text += f"・{display_names[i]}: +${today_yields[i]:,.4f}\n  (評価額: ${calc_principals[i]+today_yields[i]:,.2f})\n"
        preview_text += f"\n━━━━━━━━━━━━━━\n※画像をご確認ください。"
        
        st.code(preview_text, language="markdown")
        
        if uploaded_file:
            st.image(uploaded_file, caption="添付予定のエビデンス", width=300)

        if st.button("🚀 この内容で確定・LINE送信", type="primary"):
            if not uploaded_file:
                st.warning("エビデンス画像をアップロードしてください。")
            else:
                with st.spinner("処理中..."):
                    # ImgBBアップロード
                    res = requests.post("https://api.imgbb.com/1/upload", params={"key": st.secrets["imgbb"]["api_key"]}, files={"image": uploaded_file.getvalue()})
                    img_url = res.json()["data"]["url"]
                    
                    # 履歴保存
                    new_row = pd.DataFrame([{"Date": datetime.now().strftime("%Y-%m-%d"), "Type": "収益", "Total_Amount": sum(today_yields), "Breakdown": ",".join(map(str, today_yields)), "Note": f"[{selected_project}] APR:{total_apr}%"}])
                    conn.update(worksheet=selected_project, data=pd.concat([hist_df, new_row], ignore_index=True))
                    
                    # LINE送信
                    success = sum(1 for uid in user_ids if send_line_multimedia(st.secrets["line"]["channel_access_token"], uid, preview_text, img_url) == 200)
                    st.success(f"完了！ {success}名に送信し、履歴を保存しました。")
                    st.rerun()

    # --- タブ2: 入出金管理 ---
    with tab2:
        st.subheader("個別メンテナンス")
        selected_name = st.selectbox("対象メンバーを選択", display_names)
        idx = display_names.index(selected_name)
        st.info(f"対象: {selected_name} / 現在の評価額: **${calc_principals[idx]:,.2f}**")
        
        c1, c2 = st.columns(2)
        with c1: trans_type = st.radio("記録種別", ["入金（預け入れ）", "出金（引き出し）"])
        with c2: amount = st.number_input("金額 ($)", min_value=0.0, step=10.0)
        
        user_memo = st.text_input("備考", placeholder="例: 月初追加分")
        if st.button("💾 データを保存"):
            if amount > 0:
                val_list = [0.0] * num_people
                val_list[idx] = amount
                type_label = "入金" if "入金" in trans_type else "出金"
                final_memo = f"[{selected_project} / {selected_name}] {user_memo if user_memo else type_label}"
                new_row = pd.DataFrame([{"Date": datetime.now().strftime("%Y-%m-%d"), "Type": type_label, "Total_Amount": amount, "Breakdown": ",".join(map(str, val_list)), "Note": final_memo}])
                conn.update(worksheet=selected_project, data=pd.concat([hist_df, new_row], ignore_index=True))
                st.success("入出金を記録しました。")
                st.rerun()

    # --- 1. タブ3: 分析ダッシュボード ---
    with tab3:
        st.subheader("📊 運用推移分析")
        if not hist_df.empty and "収益" in hist_df["Type"].values:
            # グラフデータの作成
            chart_df = hist_df[hist_df["Type"] == "収益"].copy()
            chart_df["Date"] = pd.to_datetime(chart_df["Date"])
            
            # メトリクス表示
            col_m1, col_m2 = st.columns(2)
            col_m1.metric("プロジェクト総資産", f"${sum(calc_principals):,.2f}")
            col_m2.metric("累計収益", f"${sum(total_earned):,.2f}")
            
            st.write("📈 日次収益（Total Amount）の推移")
            st.line_chart(data=chart_df, x="Date", y="Total_Amount")
            
            st.write("📋 最近の履歴（最新10件）")
            st.dataframe(hist_df.iloc[::-1].head(10), use_container_width=True)
        else:
            st.info("まだ十分な収益データがありません。報告を開始するとここにグラフが表示されます。")

except Exception as e:
    st.error(f"致命的なエラー: {e}")
