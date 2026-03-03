import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import requests, json
 
import gspread 
from google.oauth2.service_account import Credentials

# --- ページ設定 ---
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# --- Google Sheets 接続 ---
def gs_client():
    cred_info = st.secrets["connections"]["gsheets"]["credentials"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(cred_info, scopes=scopes)
    return gspread.authorize(creds)

def open_sheet():
    spreadsheet_url = st.secrets["connections"]["gsheets"]["spreadsheet"]
    return gs_client().open_by_url(spreadsheet_url)

def ws_to_df(ws):
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=header)

def df_to_ws(ws, df):
    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).fillna("").values.tolist())

def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\u3000", " ", regex=False)  # 全角スペース→半角
        .str.strip()
    )
    return df

# --- ユーティリティ ---
def to_f(val) -> float:
    try:
        s = str(val).replace(",", "").replace("$", "").replace("%", "").strip()
        return float(s) if s else 0.0
    except:
        return 0.0

def split_csv(val, n: int):
    items = [x.strip() for x in str(val).split(",") if x.strip() != ""]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

def only_line_ids(values):
    out = []
    for v in values:
        s = str(v).strip()
        if s.startswith("U"):
            out.append(s)
    seen, uniq = set(), []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq

def send_line(token, user_id, text, image_url=None):
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    messages = [{"type": "text", "text": text}]
    if image_url:
        messages.append({
            "type": "image",
            "originalContentUrl": image_url,
            "previewImageUrl": image_url
        })

    payload = {"to": str(user_id), "messages": messages}
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
    return r.status_code

def upload_imgbb(file_bytes: bytes) -> str | None:
    """ImgBBへアップロードしてURLを返す（失敗時None）"""
    try:
        res = requests.post(
            "https://api.imgbb.com/1/upload",
            params={"key": st.secrets["imgbb"]["api_key"]},
            files={"image": file_bytes},
            timeout=30
        )
        data = res.json()
        return data["data"]["url"]
    except:
        return None

# --- メイン ---
st.title("🏦 APR資産運用管理システム")

try:
    sh = open_sheet()

    # Settings
    settings_df = clean_cols(ws_to_df(sh.worksheet("Settings")))
    if settings_df.empty:
        st.error("Settingsシートが空です。")
        st.stop()

    required_cols = [
        "Project_Name", "Num_People", "TotalPrincipal",
        "IndividualPrincipals", "ProfitRates", "IsCompound",
        "MemberNames", "LineID"
    ]
    missing = [c for c in required_cols if c not in settings_df.columns]
    if missing:
        st.error(f"Settingsシートの列が不足: {missing}\n現在の列: {list(settings_df.columns)}")
        st.stop()

    project_list = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = settings_df[settings_df["Project_Name"].astype(str) == str(selected_project)].iloc[0]

    # 設定値
    num_people = int(to_f(p_info["Num_People"]))
    base_principals = [to_f(x) for x in split_csv(p_info["IndividualPrincipals"], num_people)]
    rate_list = [to_f(x) for x in split_csv(p_info["ProfitRates"], num_people)]
    is_compound = str(p_info["IsCompound"]).strip().upper() in ["TRUE", "YES", "1", "はい"]

    member_names = split_csv(p_info["MemberNames"], num_people)
    if len(member_names) < num_people:
        member_names = [f"No.{i+1}" for i in range(num_people)]

    # LineID（LineIDシート優先、無ければSettingsのLineID）
    user_ids = []
    try:
        line_id_df = clean_cols(ws_to_df(sh.worksheet("LineID")))
        if not line_id_df.empty:
            if "LineID" in line_id_df.columns:
                user_ids = only_line_ids(line_id_df["LineID"].dropna().tolist())
            else:
                user_ids = only_line_ids(line_id_df.iloc[:, -1].dropna().tolist())
    except:
        user_ids = []

    if not user_ids:
        user_ids = only_line_ids(split_csv(p_info["LineID"], 999))

    st.sidebar.info(f"計算モード: {'複利' if is_compound else '単利'}")
    st.sidebar.write(f"送信先ID数: {len(user_ids)}")

    # 履歴（プロジェクト名のシート）
    try:
        hist_ws = sh.worksheet(selected_project)
        hist_df = clean_cols(ws_to_df(hist_ws))
    except:
        hist_ws = sh.add_worksheet(title=selected_project, rows=1000, cols=20)
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])
        df_to_ws(hist_ws, hist_df)

    # 履歴集計
    total_earned = [0.0] * num_people
    total_withdrawn = [0.0] * num_people
    if not hist_df.empty and all(c in hist_df.columns for c in ["Type", "Breakdown"]):
        for _, row in hist_df.iterrows():
            rtype = str(row.get("Type", "")).strip()
            rbreakdown = str(row.get("Breakdown", "")).strip()
            vals = [to_f(v) for v in rbreakdown.split(",")] if rbreakdown else []
            for i in range(num_people):
                if i < len(vals):
                    if rtype == "収益":
                        total_earned[i] += vals[i]
                    elif rtype == "出金":
                        total_withdrawn[i] += vals[i]

    # 現在元本（複利なら収益・出金反映）
    calc_principals = []
    for i in range(num_people):
        if is_compound:
            calc_principals.append(base_principals[i] + total_earned[i] - total_withdrawn[i])
        else:
            calc_principals.append(base_principals[i])

    tab1, tab2 = st.tabs(["📈 収益確定・画像付きLINE送信", "💸 出金記録"])

    # --- 収益 ---
    with tab1:
        st.subheader(f"【{selected_project}】本日の収益")

        total_apr = st.number_input("本日の全体APR (%)", value=100.0, step=0.1)
        net_factor = 0.67

        uploaded_file = st.file_uploader("エビデンス画像をアップロード（任意）", type=["png", "jpg", "jpeg"])
        if uploaded_file:
            st.image(uploaded_file, caption="送信プレビュー", width=420)

        # 収益計算（rate_list反映、365日割）
        today_yields = []
        for i in range(num_people):
            p = calc_principals[i]
            r = rate_list[i]
            y = (p * (total_apr * net_factor * r / 100.0)) / 365.0
            today_yields.append(round(y, 4))

        # 表示
        cols = st.columns(num_people if num_people <= 6 else 6)
        for i in range(num_people):
            with cols[i % len(cols)]:
                name = member_names[i] if i < len(member_names) else f"No.{i+1}"
                st.metric(f"{name}", f"${calc_principals[i]:,.2f}", f"+${today_yields[i]:,.4f}")

        if st.button("収益を保存して（画像付きで）LINE送信"):
            # 画像アップロード（任意）
            image_url = None
            if uploaded_file:
                with st.spinner("ImgBBへ画像アップロード中..."):
                    image_url = upload_imgbb(uploaded_file.getvalue())
                if uploaded_file and not image_url:
                    st.error("画像アップロードに失敗しました（ImgBB）。画像なしで続行するなら画像を外して再実行してください。")
                    st.stop()

            # 履歴に追記
            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "収益",
                "Total_Amount": sum(today_yields),
                "Breakdown": ",".join(map(str, today_yields)),
                "Note": f"APR:{total_apr}%"
            }])

            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            df_to_ws(hist_ws, updated_hist)

            # メッセージ（JST）
            jst_now = datetime.utcnow() + timedelta(hours=9)
            now_str = jst_now.strftime("%Y/%m/%d %H:%M")
            mode_str = "複利運用" if is_compound else "単利運用"

            msg = "🏦 【資産運用収益報告書】\n"
            msg += f"プロジェクト: {selected_project}\n"
            msg += f"報告日時: {now_str}\n"
            msg += f"本日のAPR: {total_apr}%\n"
            msg += f"モード: {mode_str}\n\n"
            msg += "💰 収益明細\n"
            for i in range(num_people):
                name = member_names[i] if i < len(member_names) else f"No.{i+1}"
                new_p = calc_principals[i] + today_yields[i] if is_compound else calc_principals[i]
                msg += f"・{name}: +${today_yields[i]:,.4f}\n"
                if is_compound:
                    msg += f"  (次回元本: ${new_p:,.2f})\n"

            if image_url:
                msg += "\n📎 エビデンス画像を添付します。"

            # 送信
            token = st.secrets["line"]["channel_access_token"]
            success = 0
            fail = 0
            for uid in user_ids:
                code = send_line(token, uid, msg, image_url=image_url)
                if code == 200:
                    success += 1
                else:
                    fail += 1

            st.success(f"送信完了：成功 {success} / 失敗 {fail}")
            st.rerun()

    # --- 出金 ---
    with tab2:
        st.subheader("出金・精算の記録")
        labels = [member_names[i] if i < len(member_names) else f"No.{i+1}" for i in range(num_people)]
        target = st.selectbox("メンバー", labels)
        idx = labels.index(target)

        st.info(f"現在の出金可能額: ${calc_principals[idx]:,.2f}")
        amt = st.number_input("出金額 ($)", min_value=0.0, max_value=float(calc_principals[idx]), step=10.0)
        memo = st.text_input("備考", value="出金精算")

        if st.button("出金を保存"):
            if amt <= 0:
                st.warning("出金額が0です。")
                st.stop()

            withdrawals = [0.0] * num_people
            withdrawals[idx] = float(amt)

            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "出金",
                "Total_Amount": float(amt),
                "Breakdown": ",".join(map(str, withdrawals)),
                "Note": memo
            }])

            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            df_to_ws(hist_ws, updated_hist)

            st.success("出金を記録しました。")
            st.rerun()

except Exception as e:
    st.error(f"システムエラー: {e}")
