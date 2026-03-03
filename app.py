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

def split_csv(val, n: int, default="0"):
    items = [x.strip() for x in str(val).split(",") if x.strip() != ""]
    if not items:
        items = [default]
    while len(items) < n:
        items.append(items[-1])
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
        messages.append({"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url})
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
    "TotalPrincipal",          # プロジェクト総額（この値で均等配分）
    "IndividualPrincipals",    # 参考用（入金などで増減してもOK）
    "ProfitRates",             # 使わない（残してOK）
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

    settings_df["Project_Name"] = settings_df["Project_Name"].astype(str).str.strip()
    settings_df = settings_df[settings_df["Project_Name"] != ""].reset_index(drop=True)
    return settings_df

def delete_project(settings_df: pd.DataFrame, project_name: str) -> pd.DataFrame:
    settings_df = settings_df.copy()
    settings_df = settings_df[settings_df["Project_Name"].astype(str) != str(project_name)].reset_index(drop=True)
    return settings_df

def resize_project_lists(p_info_row: pd.Series, new_n: int):
    """人数変更に合わせて MemberNames / IndividualPrincipals を伸縮（末尾値で埋め or 切り詰め）"""
    names = split_csv(p_info_row.get("MemberNames", ""), max(new_n, 1), default="No.1")
    principals = split_csv(p_info_row.get("IndividualPrincipals", ""), max(new_n, 1), default="0")

    # No.連番を自然にしたい場合（空/No.だけのとき補正）
    fixed_names = []
    for i in range(new_n):
        nm = names[i] if i < len(names) else ""
        nm = nm.strip()
        fixed_names.append(nm if nm else f"No.{i+1}")

    fixed_principals = [principals[i] if i < len(principals) else principals[-1] for i in range(new_n)]
    return fixed_names, fixed_principals

# --- メイン ---
st.title("🏦 APR資産運用管理システム")

try:
    sh = open_sheet()

    # Settings
    settings_ws = sh.worksheet("Settings")
    settings_df = ensure_settings_schema(ws_to_df(settings_ws))
    if settings_df.empty:
        settings_df = pd.DataFrame(columns=SETTINGS_COLS)

    project_list = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list) if project_list else None

    # LineID（LineIDシート優先）
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
        ["⚙️ 管理（人数±/メンバー）", "📈 収益確定・画像付きLINE送信", "💸 出金記録", "➕ 入金記録"]
    )

    # =========================
    # 管理タブ（人数をアプリで増減）
    # =========================
    with tab_manage:
        st.subheader("⚙️ プロジェクト管理（人数をアプリで増減）")

        if selected_project:
            p_row = settings_df[settings_df["Project_Name"].astype(str) == str(selected_project)].iloc[0]
            cur_n = int(to_f(p_row.get("Num_People", 1))) or 1
        else:
            p_row = None
            cur_n = 1

        col1, col2 = st.columns([2, 1])

        with col1:
            new_name = st.text_input("Project_Name（新規作成/編集）", value=selected_project or "")
            total_principal = st.number_input("TotalPrincipal（プロジェクト総額）", min_value=0.0, step=100.0,
                                              value=float(to_f(p_row.get("TotalPrincipal", 0)) if p_row is not None else 0.0))
            is_compound_in = st.selectbox("IsCompound", ["FALSE", "TRUE"],
                                          index=1 if p_row is not None and str(p_row.get("IsCompound", "")).strip().upper() in ["TRUE","YES","1","はい"] else 0)

        with col2:
            st.markdown("**人数 Num_People**")
            cA, cB, cC = st.columns([1,1,2])
            with cA:
                dec = st.button("−1")
            with cB:
                inc = st.button("+1")
            with cC:
                # 現在値表示
                new_n = st.number_input("現在人数", min_value=1, step=1, value=int(cur_n))

        # ボタンで±したい場合
        if selected_project:
            if inc:
                new_n = int(cur_n) + 1
            if dec:
                new_n = max(1, int(cur_n) - 1)

        # メンバーと個別元本の入力（人数に連動して伸縮）
        if selected_project:
            base_names, base_princs = resize_project_lists(p_row, int(new_n))
        else:
            base_names = [f"No.{i+1}" for i in range(int(new_n))]
            base_princs = ["0" for _ in range(int(new_n))]

        st.caption("メンバー名と個別元本（参考）を編集できます。人数変更すると自動で増減します。")
        names_text = st.text_input("MemberNames（カンマ区切り）", value=",".join(base_names))
        principals_text = st.text_input("IndividualPrincipals（カンマ区切り）", value=",".join(base_princs))

        lineid_text = st.text_input("LineID（Settings側。未使用なら空でOK）",
                                    value=str(p_row.get("LineID","")) if p_row is not None else "")

        colS, colD = st.columns(2)
        with colS:
            if st.button("✅ Settingsに保存（追加/更新）"):
                if not new_name.strip():
                    st.error("Project_Nameが空です。")
                    st.stop()

                payload = {
                    "Project_Name": new_name.strip(),
                    "Num_People": str(int(new_n)),
                    "TotalPrincipal": str(float(total_principal)),
                    "IndividualPrincipals": names_text and principals_text.strip(),
                    "ProfitRates": "",  # 使わないので空（残したければここに入れる）
                    "IsCompound": is_compound_in,
                    "MemberNames": names_text.strip(),
                    "LineID": lineid_text.strip(),
                }
                settings_df2 = upsert_project(settings_df, new_name.strip(), payload)
                df_to_ws(settings_ws, settings_df2)
                st.success("保存しました。画面を更新します。")
                st.rerun()

        with colD:
            if selected_project and st.button("🗑 プロジェクト削除"):
                settings_df2 = delete_project(settings_df, selected_project)
                df_to_ws(settings_ws, settings_df2)
                st.success("削除しました。")
                st.rerun()

        st.markdown("---")
        st.markdown("### 現在のSettings（確認用）")
        st.dataframe(settings_df, use_container_width=True)

    # =========================
    # 以降はプロジェクト選択が必要
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
    num_people = int(to_f(p_info["Num_People"])) or 1

    # プロジェクト総額（均等配分の基準）
    project_total_principal = float(to_f(p_info.get("TotalPrincipal", 0)))

    # メンバー名（人数分）
    member_names = split_csv(p_info.get("MemberNames", ""), num_people, default="No.1")
    member_names = [nm if nm else f"No.{i+1}" for i, nm in enumerate(member_names)]

    # 個別元本（参考）
    base_principals = [to_f(x) for x in split_csv(p_info.get("IndividualPrincipals", ""), num_people, default="0")]

    # 複利フラグ（入金・出金・収益の元本反映用に残す）
    is_compound = str(p_info.get("IsCompound","")).strip().upper() in ["TRUE", "YES", "1", "はい"]

    # LineIDがLineIDシートに無い場合のフォールバック
    if not user_ids:
        user_ids = only_line_ids(split_csv(p_info.get("LineID",""), 999, default=""))

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

    # 現在元本（参考表示用）
    calc_principals = []
    for i in range(num_people):
        base_plus_deposit = base_principals[i] + total_deposit[i]
        if is_compound:
            calc_principals.append(base_plus_deposit + total_earned[i] - total_withdrawn[i])
        else:
            calc_principals.append(base_plus_deposit)

    labels = member_names[:num_people]

    # =========================
    # 収益タブ（均等配分）
    # =========================
    with tab_profit:
        st.subheader(f"【{selected_project}】本日の収益（プロジェクト総額×APR×66%→均等配分）")

        total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
        net_factor = 0.66

        uploaded_file = st.file_uploader("エビデンス画像をアップロード（任意）", type=["png", "jpg", "jpeg"])
        if uploaded_file:
            st.image(uploaded_file, caption="送信プレビュー", width=420)

        # --- ここが要件のロジック ---
        # 1日分収益（プロジェクト総額ベース）
        project_daily_yield = (project_total_principal * (total_apr / 100.0) * net_factor) / 365.0
        per_person = round(project_daily_yield / max(1, num_people), 4)
        today_yields = [per_person] * num_people

        st.info(f"プロジェクト総額: ${project_total_principal:,.2f} / 1日収益原資(66%): ${project_daily_yield:,.4f} / 1人: ${per_person:,.4f}")

        cols = st.columns(num_people if num_people <= 6 else 6)
        for i in range(num_people):
            with cols[i % len(cols)]:
                st.metric(labels[i], f"参考元本: ${calc_principals[i]:,.2f}", f"+${today_yields[i]:,.4f}")

        if st.button("収益を保存して（画像付きで）LINE送信"):
            image_url = None
            if uploaded_file:
                with st.spinner("ImgBBへ画像アップロード中..."):
                    image_url = upload_imgbb(uploaded_file.getvalue())
                if uploaded_file and not image_url:
                    st.error("画像アップロード失敗（ImgBB）。画像なしで続行するなら画像を外して再実行。")
                    st.stop()

            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "収益",
                "Total_Amount": sum(today_yields),
                "Breakdown": join_csv(today_yields),
                "Note": f"APR:{total_apr}% net:{net_factor}"
            }])
            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            df_to_ws(hist_ws, updated_hist)

            jst_now = datetime.utcnow() + timedelta(hours=9)
            now_str = jst_now.strftime("%Y/%m/%d %H:%M")

            msg = "🏦 【資産運用収益報告書】\n"
            msg += f"プロジェクト: {selected_project}\n"
            msg += f"報告日時: {now_str}\n"
            msg += f"本日のAPR: {total_apr}%\n"
            msg += f"配分: プロジェクト総額×APR×66% を {num_people}人で均等\n"
            msg += f"総額: ${project_total_principal:,.2f}\n"
            msg += f"1日原資: ${project_daily_yield:,.4f}\n"
            msg += f"1人分: ${per_person:,.4f}\n\n"
            msg += "💰 明細\n"
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
    # 出金タブ（クラッシュ防止）
    # =========================
    with tab_withdraw:
        st.subheader("出金・精算の記録")

        target = st.selectbox("メンバー", labels, key="w_member")
        idx = labels.index(target)

        available = max(0.0, float(calc_principals[idx]))
        st.info(f"現在の出金可能額（参考）: ${available:,.2f}")

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
    # 入金タブ（追加入金）
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
