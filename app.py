import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import plotly.express as px  # ★グラフ強化用
from datetime import datetime, timedelta
import requests
import json
import re

# --- 1. ページ基本設定 ---
st.set_page_config(page_title="APR究極管理システム", layout="wide", page_icon="💎")

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
st.title("💎 APR資産運用管理システム Ultra")

try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    settings_df = conn.read(worksheet="Settings", ttl=60)
    line_id_df = conn.read(worksheet="LineID", ttl=60)
    
    # サイドバー管理
    st.sidebar.header("⚙️ システム管理")
    line_ok = "line" in st.secrets and st.secrets["line"].get("channel_access_token")
    imgbb_ok = "imgbb" in st.secrets and st.secrets["imgbb"].get("api_key")
    st.sidebar.write(f"{'🟢' if line_ok else '🔴'} LINE接続")
    st.sidebar.write(f"{'🟢' if imgbb_ok else '🔴'} ImgBB接続")

    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクト選択", project_list)
    
    # データ展開
    p_info = settings_df[settings_df.iloc[:, 0] == selected_project].iloc[0]
    num_people = int(to_f(p_info.iloc[1]))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"]
    display_names = split_val(str(p_info.iloc[6]), num_people)

    # 履歴取得
    try:
        hist_df = conn.read(worksheet=selected_project, ttl=60)
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # 収益/入出金 集計
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

    tab1, tab2, tab3 = st.tabs(["📈 収益報告", "💸 入出金管理", "🚀 詳細分析ダッシュボード"])

    # --- タブ1 & タブ2 は前回のプロフェッショナル版と同様（省略せず実装） ---
    with tab1:
        st.subheader("運用収益の確定")
        COEFF = 0.77
        total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
        uploaded_file = st.file_uploader("エビデンス画像", type=['png', 'jpg', 'jpeg'])
        today_yields = [round((p * (total_apr * COEFF * rate_list[i] / 100)) / 365, 4) for i, p in enumerate(calc_principals)]
        
        st.markdown("---")
        st.subheader("📝 送信プレビュー")
        jst_now = datetime.utcnow() + timedelta(hours=9)
        preview_text = f"🏦 【{selected_project} 収益報告】\n━━━━━━━━━━━━━━\n報告日時: {jst_now.strftime('%Y/%m/%d %H:%M')}\nAPR: {total_apr}%\n━━━━━━━━━━━━━━\n\n💰 収益明細\n"
        for i in range(num_people):
            preview_text += f"・{display_names[i]}: +${today_yields[i]:,.4f}\n  (評価額: ${calc_principals[i]+today_yields[i]:,.2f})\n"
        st.code(preview_text)

        if st.button("🚀 確定・LINE一斉送信", type="primary"):
            if uploaded_file:
                res = requests.post("https://api.imgbb.com/1/upload", params={"key": st.secrets["imgbb"]["api_key"]}, files={"image": uploaded_file.getvalue()})
                img_url = res.json()["data"]["url"]
                new_row = pd.DataFrame([{"Date": datetime.now().strftime("%Y-%m-%d"), "Type": "収益", "Total_Amount": sum(today_yields), "Breakdown": ",".join(map(str, today_yields)), "Note": f"[{selected_project}] APR:{total_apr}%"}])
                conn.update(worksheet=selected_project, data=pd.concat([hist_df, new_row], ignore_index=True))
                
                # LineIDシートから送信先取得
                all_cells = line_id_df.values.flatten().astype(str)
                uids = sorted(list(set([x.strip() for x in all_cells if x.startswith('U')])))
                for uid in uids: send_line_multimedia(st.secrets["line"]["channel_access_token"], uid, preview_text, img_url)
                st.success("完了！")
                st.rerun()

    with tab2:
        st.subheader("入出金記録")
        selected_name = st.selectbox("メンバーを選択", display_names)
        idx = display_names.index(selected_name)
        st.info(f"評価額: **${calc_principals[idx]:,.2f}**")
        amount = st.number_input("金額 ($)", min_value=0.0)
        user_memo = st.text_input("備考")
        if st.button("💾 保存"):
            type_label = "入金" if amount > 0 else "出金"
            val_list = [0.0] * num_people
            val_list[idx] = abs(amount)
            new_row = pd.DataFrame([{"Date": datetime.now().strftime("%Y-%m-%d"), "Type": type_label, "Total_Amount": abs(amount), "Breakdown": ",".join(map(str, val_list)), "Note": f"[{selected_project} / {selected_name}] {user_memo}"}])
            conn.update(worksheet=selected_project, data=pd.concat([hist_df, new_row], ignore_index=True))
            st.rerun()

    # --- 🚀 タブ3: 超詳細分析ダッシュボード ---
    with tab3:
        st.subheader("📊 エグゼクティブ・ダッシュボード")
        
        # 1. サマリーメトリクス
        m1, m2, m3 = st.columns(3)
        m1.metric("総運用資産 (AUM)", f"${sum(calc_principals):,.2f}")
        m2.metric("累計分配利益", f"${sum(total_earned):,.2f}", delta=f"{sum(today_yields):,.2f} (本日)")
        total_roi = (sum(total_earned) / sum(base_principals) * 100) if sum(base_principals) > 0 else 0
        m3.metric("プロジェクトROI", f"{total_roi:.2f}%")

        st.markdown("---")
        
        # 2. グラフセクション
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.write("🥧 **現在の資産構成比 (Share of Wallet)**")
            pie_data = pd.DataFrame({"Name": display_names, "Value": calc_principals})
            fig_pie = px.pie(pie_data, values='Value', names='Name', hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig_pie, use_container_width=True)
            

        with col_right:
            st.write("📊 **メンバー別・累計収益獲得数**")
            bar_data = pd.DataFrame({"Name": display_names, "Profit": total_earned})
            fig_bar = px.bar(bar_data, x='Name', y='Profit', color='Name', text_auto='.2s')
            st.plotly_chart(fig_bar, use_container_width=True)
            

        st.markdown("---")

        # 3. メンバー別・詳細データテーブル
        st.write("📑 **投資家別パフォーマンス詳細**")
        analysis_list = []
        for i in range(num_people):
            roi = (total_earned[i] / base_principals[i] * 100) if base_principals[i] > 0 else 0
            analysis_list.append({
                "名前": display_names[i],
                "初期元本": f"${base_principals[i]:,.2f}",
                "入金合計": f"${total_deposited[i]:,.2f}",
                "出金合計": f"${total_withdrawn[i]:,.2f}",
                "累計収益": f"${total_earned[i]:,.2f}",
                "現在の評価額": f"${calc_principals[i]:,.2f}",
                "ROI (%)": f"{roi:.2f}%"
            })
        st.table(pd.DataFrame(analysis_list))

except Exception as e:
    st.error(f"システムエラー: {e}")
