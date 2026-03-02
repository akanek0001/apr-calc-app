import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import requests
import json
import re

# --- 1. ページ基本設定 ---
st.set_page_config(page_title="APR究極管理システム", layout="wide", page_icon="💎")

# --- 2. 汎用ユーティリティ ---
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
    settings_df = conn.read(worksheet="Settings", ttl=10) # 追加反映を早くするためttl短縮
    line_id_df = conn.read(worksheet="LineID", ttl=60)
    
    # サイドバー管理
    st.sidebar.header("⚙️ システム管理")
    project_list = settings_df.iloc[:, 0].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクト選択", project_list)
    
    # データ展開
    p_idx = settings_df[settings_df.iloc[:, 0] == selected_project].index[0]
    p_info = settings_df.loc[p_idx]
    
    num_people = int(to_f(p_info.iloc[1]))
    base_principals = [to_f(p) for p in split_val(p_info.iloc[3], num_people)]
    rate_list = [to_f(r) for r in split_val(p_info.iloc[4], num_people)]
    is_compound = str(p_info.iloc[5]).upper() in ["TRUE", "はい", "YES", "1"]
    display_names = split_val(str(p_info.iloc[6]), num_people)

    # 履歴取得
    try:
        hist_df = conn.read(worksheet=selected_project, ttl=10)
    except:
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])

    # 集計計算
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

    # --- タブ構成 ---
    tab1, tab2, tab3, tab4 = st.tabs(["📈 収益報告", "💸 入出金管理", "🚀 詳細分析", "👥 ユーザー管理"])

    # --- タブ1: 収益報告 ---
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
                
                all_cells = line_id_df.values.flatten().astype(str)
                uids = sorted(list(set([x.strip() for x in all_cells if x.startswith('U')])))
                for uid in uids: send_line_multimedia(st.secrets["line"]["channel_access_token"], uid, preview_text, img_url)
                st.success("報告完了！")
                st.rerun()
            else:
                st.warning("画像が必要です。")

    # --- タブ2: 入出金 ---
    with tab2:
        st.subheader("入出金記録")
        selected_name = st.selectbox("メンバーを選択", display_names)
        idx = display_names.index(selected_name)
        st.info(f"評価額: **${calc_principals[idx]:,.2f}**")
        amount = st.number_input("金額 ($)", min_value=0.0)
        t_type = st.radio("種別", ["入金", "出金"])
        user_memo = st.text_input("備考")
        if st.button("💾 入出金を保存"):
            val_list = [0.0] * num_people
            val_list[idx] = amount
            new_row = pd.DataFrame([{"Date": datetime.now().strftime("%Y-%m-%d"), "Type": t_type, "Total_Amount": amount, "Breakdown": ",".join(map(str, val_list)), "Note": f"[{selected_project} / {selected_name}] {user_memo}"}])
            conn.update(worksheet=selected_project, data=pd.concat([hist_df, new_row], ignore_index=True))
            st.success("保存完了")
            st.rerun()

    # --- タブ3: 詳細分析 ---
    with tab3:
        st.subheader("📊 分析ダッシュボード")
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric("総資産 (AUM)", f"${sum(calc_principals):,.2f}")
        col_m2.metric("累計利益", f"${sum(total_earned):,.2f}")
        total_roi = (sum(total_earned) / sum(base_principals) * 100) if sum(base_principals) > 0 else 0
        col_m3.metric("プロジェクトROI", f"{total_roi:.2f}%")
        
        c_left, c_right = st.columns(2)
        with c_left:
            fig_pie = px.pie(values=calc_principals, names=display_names, title="資産構成比", hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)
        with c_right:
            fig_bar = px.bar(x=display_names, y=total_earned, title="メンバー別累計収益", labels={'x':'名前', 'y':'利益($)'})
            st.plotly_chart(fig_bar, use_container_width=True)

    # --- ★タブ4: ユーザー管理 (新規追加機能) ---
    with tab4:
        st.subheader("👤 運用ユーザーの管理・追加")
        
        # 現在のユーザー一覧表示
        st.write("📋 現在の登録メンバー")
        member_data = pd.DataFrame({
            "名前": display_names,
            "初期元本": base_principals,
            "配分比率": rate_list
        })
        st.table(member_data)
        
        st.markdown("---")
        st.subheader("➕ 新規メンバーを追加")
        with st.form("add_user_form"):
            new_name = st.text_input("新しいメンバーの名前", placeholder="例: 山田太郎")
            new_principal = st.number_input("初期元本 ($)", min_value=0.0, step=100.0)
            new_rate = st.number_input("配分比率 (1を基準とした重み)", value=1.0, step=0.1)
            
            submit_btn = st.form_submit_button("このユーザーをプロジェクトに追加")
            
            if submit_btn:
                if new_name:
                    # データの更新準備
                    updated_names = display_names + [new_name]
                    updated_principals = base_principals + [new_principal]
                    updated_rates = rate_list + [new_rate]
                    new_num_people = len(updated_names)
                    
                    # Settingsシートの情報を更新
                    settings_df.loc[p_idx, settings_df.columns[1]] = new_num_people # 人数
                    settings_df.loc[p_idx, settings_df.columns[3]] = ",".join(map(str, updated_principals)) # 元本
                    settings_df.loc[p_idx, settings_df.columns[4]] = ",".join(map(str, updated_rates)) # 比率
                    settings_df.loc[p_idx, settings_df.columns[6]] = ",".join(updated_names) # 名前
                    
                    # Google Sheetsへ書き込み
                    conn.update(worksheet="Settings", data=settings_df)
                    
                    st.success(f"✨ {new_name} 様をプロジェクトに追加しました！")
                    st.info("※反映に数秒かかる場合があります。自動的にリロードします。")
                    st.rerun()
                else:
                    st.error("名前を入力してください。")

except Exception as e:
    st.error(f"システムエラー: {e}")
