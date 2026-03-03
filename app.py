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
        .str.replace("\u3000", " ", regex=False)
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

def join_csv(values):
    return ",".join([str(v) for v in values])

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

# --- Settings 管理 ---
SETTINGS_COLS = [
    "Project_Name",
    "Num_People",
    "TotalPrincipal",
    "IndividualPrincipals",
    "ProfitRates",
    "IsCompound",
    "MemberNames",
    "LineID",
]

def ensure_settings_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    for c in SETTINGS_COLS:
        if c not in df.columns:
            df[c] = ""
    return df[SETTINGS_COLS]

def upsert_project(settings_df: pd.DataFrame, project_name: str, payload: dict) -> pd.DataFrame:
    settings_df = settings_df.copy()
    mask = settings_df["Project_Name"].astype(str) == str(project_name)
    row = {c: payload.get(c, "") for c in SETTINGS_COLS}
    row["Project_Name"] = project_name

    if mask.any():
        idx = settings_df[mask].index[0]
        for k, v in row.items():
            settings_df.at[idx, k] = v
    else:
        settings_df = pd.concat([settings_df, pd.DataFrame([row])], ignore_index=True)

    # 空行除去（Project_Name空は消す）
    settings_df["Project_Name"] = settings_df["Project_Name"].astype(str).str.strip()
    settings_df = settings_df[settings_df["Project_Name"] != ""].reset_index(drop=True)
    return settings_df

def delete_project(settings_df: pd.DataFrame, project_name: str) -> pd.DataFrame:
    settings_df = settings_df.copy()
    settings_df = settings_df[settings_df["Project_Name"].astype(str) != str(project_name)].reset_index(drop=True)
    return settings_df

# --- メイン ---
st.title("🏦 APR資産運用管理システム")

try:
    sh = open_sheet()

    # Settings
    settings_ws = sh.worksheet("Settings")
    settings_df = ensure_settings_schema(ws_to_df(settings_ws))
    if settings_df.empty:
        # 空でも管理タブで作れるようにする
        settings_df = pd.DataFrame(columns=SETTINGS_COLS)

    # プロジェクト選択
    project_list = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list) if project_list else None

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

    tab_manage, tab_profit, tab_withdraw, tab_deposit = st.tabs(
        ["⚙️ 管理（プロジェクト/メンバー）", "📈 収益確定・画像付きLINE送信", "💸 出金記録", "➕ 入金記録"]
    )

    # =========================
    #  管理タブ（増減できる）
    # =========================
    with tab_manage:
        st.subheader("⚙️ プロジェクトとメンバーの管理（Settingsを書き換えます）")

        # 新規 or 編集
        st.markdown("### プロジェクト追加 / 更新")
        new_name = st.text_input("Project_Name（新規追加や更新対象）", value=selected_project or "")
        num_people_in = st.number_input("Num_People", min_value=1, step=1, value=1 if not selected_project else int(to_f(settings_df[settings_df["Project_Name"]==selected_project].iloc[0]["Num_People"])))
        is_compound_in = st.selectbox("IsCompound", ["FALSE", "TRUE"], index=1 if selected_project and str(settings_df[settings_df["Project_Name"]==selected_project].iloc[0]["IsCompound"]).strip().upper() in ["TRUE","YES","1","はい"] else 0)

        st.caption("下の3つは人数分をカンマ区切りで入力（例: 1000,2000,1500）")

        # 既存値のプリフィル
        def get_field(pname, field, default=""):
            if not pname:
                return default
            m = settings_df["Project_Name"].astype(str) == str(pname)
            if not m.any():
                return default
            return str(settings_df[m].iloc[0].get(field, default))

        principals_text = st.text_input("IndividualPrincipals", value=get_field(new_name, "IndividualPrincipals", "1000"))
        rates_text = st.text_input("ProfitRates", value=get_field(new_name, "ProfitRates", "1"))
        names_text = st.text_input("MemberNames", value=get_field(new_name, "MemberNames", "No.1"))

        lineid_text = st.text_input("LineID（Settings側。未使用なら空でOK）", value=get_field(new_name, "LineID", ""))

        total_principal_text = st.text_input("TotalPrincipal（任意）", value=get_field(new_name, "TotalPrincipal", ""))

        colA, colB = st.columns(2)
        with colA:
            if st.button("✅ Settingsに保存（追加/更新）"):
                if not new_name.strip():
                    st.error("Project_Nameが空です。")
                    st.stop()

                payload = {
                    "Project_Name": new_name.strip(),
                    "Num_People": str(int(num_people_in)),
                    "TotalPrincipal": total_principal_text.strip(),
                    "IndividualPrincipals": principals_text.strip(),
                    "ProfitRates": rates_text.strip(),
                    "IsCompound": is_compound_in,
                    "MemberNames": names_text.strip(),
                    "LineID": lineid_text.strip(),
                }
                settings_df2 = upsert_project(settings_df, new_name.strip(), payload)
                df_to_ws(settings_ws, settings_df2)
                st.success("Settingsを保存しました。画面を更新します。")
                st.rerun()

        with colB:
            if selected_project:
                if st.button("🗑 プロジェクト削除（Settingsから削除）"):
                    settings_df2 = delete_project(settings_df, selected_project)
                    df_to_ws(settings_ws, settings_df2)
                    st.success("削除しました。")
                    st.rerun()

        st.markdown("---")
        st.markdown("### 現在のSettings（確認用）")
        st.dataframe(settings_df, use_container_width=True)

    # =========================
    #  収益/出金/入金は
    #  プロジェクト選択が必要
    # =========================
    if not selected_project:
        with tab_profit:
            st.warning("左のサイドバーでプロジェクトを選択してください。")
        with tab_withdraw:
            st.warning("左のサイドバーでプロジェクトを選択してください。")
        with tab_deposit:
            st.warning("左のサイドバーでプロジェクトを選択してください。")
        st.stop()

    # 選択プロジェクト設定
    p_info = settings_df[settings_df["Project_Name"].astype(str) == str(selected_project)].iloc[0]
    num_people = int(to_f(p_info["Num_People"]))
    base_principals = [to_f(x) for x in split_csv(p_info["IndividualPrincipals"], num_people)]
    rate_list = [to_f(x) for x in split_csv(p_info["ProfitRates"], num_people)]
    is_compound = str(p_info["IsCompound"]).strip().upper() in ["TRUE", "YES", "1", "はい"]
    member_names = split_csv(p_info["MemberNames"], num_people)
    if len(member_names) < num_people:
        member_names = [f"No.{i+1}" for i in range(num_people)]

    # LineIDがLineIDシートに無い場合のフォールバック
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
        hist_df = pd.DataFrame(columns=["Date","Type","Total_Amount","Breakdown","Note"])
        df_to_ws(hist_ws, hist_df)

    # 履歴集計（収益/出金/入金）
    total_earned = [0.0] * num_people
    total_withdrawn = [0.0] * num_people
    total_deposit = [0.0] * num_people

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
                    elif rtype == "入金":
                        total_deposit[i] += vals[i]

    # 現在元本
    calc_principals = []
    for i in range(num_people):
        base_plus_deposit = base_principals[i] + total_deposit[i]
        if is_compound:
            calc_principals.append(base_plus_deposit + total_earned[i] - total_withdrawn[i])
        else:
            calc_principals.append(base_plus_deposit)

    labels = [member_names[i] if i < len(member_names) else f"No.{i+1}" for i in range(num_people)]

    # =========================
    #  収益タブ（画像付きLINE）
    # =========================
    with tab_profit:
        st.subheader(f"【{selected_project}】本日の収益")

        total_apr = st.number_input("本日の全体APR (%)", value=100.0, step=0.1)
        net_factor = 0.67

        uploaded_file = st.file_uploader("エビデンス画像をアップロード（任意）", type=["png", "jpg", "jpeg"])
        if uploaded_file:
            st.image(uploaded_file, caption="送信プレビュー", width=420)

        today_yields = []
        for i in range(num_people):
            p = calc_principals[i]
            r = rate_list[i]
            y = (p * (total_apr * net_factor * r / 100.0)) / 365.0
            today_yields.append(round(y, 4))

        cols = st.columns(num_people if num_people <= 6 else 6)
        for i in range(num_people):
            with cols[i % len(cols)]:
                st.metric(labels[i], f"${calc_principals[i]:,.2f}", f"+${today_yields[i]:,.4f}")

        if st.button("収益を保存して（画像付きで）LINE送信"):
            image_url = None
            if uploaded_file:
                with st.spinner("ImgBBへ画像アップロード中..."):
                    image_url = upload_imgbb(uploaded_file.getvalue())
                if uploaded_file and not image_url:
                    st.error("画像アップロード失敗（ImgBB）。画像なしで続行するなら画像を外して再実行。")
                    st.stop()

            # 履歴追記
            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "収益",
                "Total_Amount": sum(today_yields),
                "Breakdown": join_csv(today_yields),
                "Note": f"APR:{total_apr}%"
            }])
            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            df_to_ws(hist_ws, updated_hist)

            # メッセージ
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
                msg += f"・{labels[i]}: +${today_yields[i]:,.4f}\n"
            if image_url:
                msg += "\n📎 エビデンス画像を添付します。"

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

    # =========================
    #  出金タブ（クラッシュ防止）
    # =========================
    with tab_withdraw:
        st.subheader("出金・精算の記録")

        target = st.selectbox("メンバー", labels, key="w_member")
        idx = labels.index(target)

        available = max(0.0, float(calc_principals[idx]))
        st.info(f"現在の出金可能額: ${available:,.2f}")

        if available <= 0:
            st.warning("出金可能額がありません。")
        else:
            amt = st.number_input("出金額 ($)", min_value=0.0, max_value=available, step=10.0, key="w_amt")
            memo = st.text_input("備考", value="出金精算", key="w_memo")

            if st.button("出金を保存", key="w_save"):
                if amt <= 0:
                    st.warning("出金額が0です。")
                    st.stop()

                withdrawals = [0.0] * num_people
                withdrawals[idx] = float(amt)

                new_row = pd.DataFrame([{
                    "Date": datetime.now().strftime("%Y-%m-%d"),
                    "Type": "出金",
                    "Total_Amount": float(amt),
                    "Breakdown": join_csv(withdrawals),
                    "Note": memo
                }])

                updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
                df_to_ws(hist_ws, updated_hist)
                st.success("出金を記録しました。")
                st.rerun()

    # =========================
    #  入金タブ（追加元本）
    # =========================
    with tab_deposit:
        st.subheader("➕ 入金（追加元本）の記録")

        target = st.selectbox("メンバー（入金先）", labels, key="d_member")
        idx = labels.index(target)

        deposit_amt = st.number_input("入金額 ($)", min_value=0.0, step=10.0, key="d_amt")
        memo = st.text_input("備考", value="追加入金", key="d_memo")

        if st.button("入金を保存", key="d_save"):
            if deposit_amt <= 0:
                st.warning("入金額が0です。")
                st.stop()

            deposits = [0.0] * num_people
            deposits[idx] = float(deposit_amt)

            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "入金",
                "Total_Amount": float(deposit_amt),
                "Breakdown": join_csv(deposits),
                "Note": memo
            }])

            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            df_to_ws(hist_ws, updated_hist)
            st.success("入金を記録しました。")
            st.rerun()

except Exception as e:
    st.error(f"システムエラー: {e}")
