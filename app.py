import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import requests, json

import gspread
from google.oauth2.service_account import Credentials

# =========================
# Page
# =========================
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# =========================
# Google Sheets
# =========================
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

# =========================
# Utils
# =========================
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

# =========================
# LINE / ImgBB
# =========================
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

# =========================
# Admin gate (Secrets: [admin].password)
# =========================
def is_admin() -> bool:
    if st.session_state.get("is_admin") is True:
        return True

    admin_pw = st.secrets.get("admin", {}).get("password")
    if not admin_pw:
        return False

    with st.sidebar:
        st.markdown("### 🔐 管理者ログイン")
        pw = st.text_input("Admin Password", type="password", key="admin_pw_input")
        if st.button("ログイン", key="admin_login_btn"):
            if pw == admin_pw:
                st.session_state["is_admin"] = True
                st.success("管理者としてログインしました")
                st.rerun()
            else:
                st.error("パスワードが違います")
    return False

# =========================
# Admin notification (Secrets: [admin].notify_line_ids)
# =========================
def get_admin_notify_ids() -> list[str]:
    raw = ""
    try:
        raw = str(st.secrets.get("admin", {}).get("notify_line_ids", "")).strip()
    except:
        raw = ""
    if not raw:
        return []
    ids = [x.strip() for x in raw.split(",") if x.strip()]
    return only_line_ids(ids)

def notify_admin_cash_event(
    action_type: str,
    project: str,
    member: str,
    amount: float,
    memo: str,
    before_principal: float,
    after_principal: float,
    before_total: float,
    after_total: float
):
    admin_ids = get_admin_notify_ids()
    if not admin_ids:
        return

    jst_now = datetime.utcnow() + timedelta(hours=9)
    now_str = jst_now.strftime("%Y/%m/%d %H:%M")

    sign = "+" if action_type in ["入金", "収益"] else "-"

    msg = "🔔【入出金/収益 通知】\n"
    msg += f"種別: {action_type}\n"
    msg += f"プロジェクト: {project}\n"
    msg += f"対象: {member}\n"
    msg += f"金額: {sign}${amount:,.2f}\n"
    msg += f"個別元本（前）: ${before_principal:,.2f}\n"
    msg += f"個別元本（後）: ${after_principal:,.2f}\n"
    msg += f"総額（前）: ${before_total:,.2f}\n"
    msg += f"総額（後）: ${after_total:,.2f}\n"
    if memo:
        msg += f"備考: {memo}\n"
    msg += f"日時: {now_str}"

    token = st.secrets["line"]["channel_access_token"]
    for uid in admin_ids:
        send_line(token, uid, msg)

# =========================
# Settings schema
# =========================
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

    settings_df["Project_Name"] = settings_df["Project_Name"].astype(str).str.strip()
    settings_df = settings_df[settings_df["Project_Name"] != ""].reset_index(drop=True)
    return settings_df

def delete_project(settings_df: pd.DataFrame, project_name: str) -> pd.DataFrame:
    settings_df = settings_df.copy()
    settings_df = settings_df[settings_df["Project_Name"].astype(str) != str(project_name)].reset_index(drop=True)
    return settings_df

def resize_project_lists(p_info_row: pd.Series, new_n: int):
    names = split_csv(p_info_row.get("MemberNames", ""), max(new_n, 1), default="No.1")
    principals = split_csv(p_info_row.get("IndividualPrincipals", ""), max(new_n, 1), default="0")

    fixed_names = []
    for i in range(new_n):
        nm = names[i].strip() if i < len(names) else ""
        fixed_names.append(nm if nm else f"No.{i+1}")

    fixed_principals = [principals[i] if i < len(principals) else principals[-1] for i in range(new_n)]
    return fixed_names, fixed_principals

def _settings_row_index(settings_df: pd.DataFrame, project_name: str) -> int | None:
    mask = settings_df["Project_Name"].astype(str) == str(project_name)
    if not mask.any():
        return None
    return int(settings_df[mask].index[0])

def update_settings_total_principal(settings_ws, settings_df: pd.DataFrame, project_name: str, delta_amount: float):
    df = settings_df.copy()
    row_idx = _settings_row_index(df, project_name)
    if row_idx is None:
        return

    cur = to_f(df.at[row_idx, "TotalPrincipal"])
    new_val = float(cur) + float(delta_amount)
    df.at[row_idx, "TotalPrincipal"] = str(new_val)
    df_to_ws(settings_ws, df)

def update_settings_individual_principal_one(
    settings_ws,
    settings_df: pd.DataFrame,
    project_name: str,
    member_index: int,
    delta_amount: float
):
    df = settings_df.copy()
    row_idx = _settings_row_index(df, project_name)
    if row_idx is None:
        return

    p_info = df.loc[row_idx]
    n = int(to_f(p_info.get("Num_People", 1))) or 1
    principals = [to_f(x) for x in split_csv(p_info.get("IndividualPrincipals", ""), n, default="0")]

    if member_index < 0 or member_index >= n:
        return

    principals[member_index] = float(principals[member_index]) + float(delta_amount)
    df.at[row_idx, "IndividualPrincipals"] = join_csv(principals)
    df_to_ws(settings_ws, df)

def update_settings_individual_principals_batch(
    settings_ws,
    settings_df: pd.DataFrame,
    project_name: str,
    deltas: list[float]
):
    df = settings_df.copy()
    row_idx = _settings_row_index(df, project_name)
    if row_idx is None:
        return

    p_info = df.loc[row_idx]
    n = int(to_f(p_info.get("Num_People", 1))) or 1
    principals = [to_f(x) for x in split_csv(p_info.get("IndividualPrincipals", ""), n, default="0")]

    for i in range(min(n, len(deltas))):
        principals[i] = float(principals[i]) + float(deltas[i])

    df.at[row_idx, "IndividualPrincipals"] = join_csv(principals)
    df_to_ws(settings_ws, df)

# =========================
# Main
# =========================
st.title("🏦 APR資産運用管理システム")

try:
    sh = open_sheet()

    # Settings sheet
    settings_ws = sh.worksheet("Settings")
    settings_df = ensure_settings_schema(ws_to_df(settings_ws))
    if settings_df.empty:
        settings_df = pd.DataFrame(columns=SETTINGS_COLS)

    # Project selection
    project_list = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list) if project_list else None

    # LineID list (prefer LineID sheet)
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

    admin = is_admin()

    tab_manage, tab_profit, tab_cash = st.tabs(
        ["⚙️ 管理（人数±/メンバー）", "📈 収益確定・画像付きLINE送信", "💳 入金・出金（管理者へ通知）"]
    )

    # =========================
    # Manage tab (admin only)
    # =========================
    with tab_manage:
        st.subheader("⚙️ 管理（管理者のみ）")
        if not admin:
            st.warning("管理者ログインが必要です。")
        else:
            if selected_project:
                p_row = settings_df[settings_df["Project_Name"].astype(str) == str(selected_project)].iloc[0]
                cur_n = int(to_f(p_row.get("Num_People", 1))) or 1
            else:
                p_row = None
                cur_n = 1

            col1, col2 = st.columns([2, 1])
            with col1:
                new_name = st.text_input("Project_Name（新規作成/編集）", value=selected_project or "", key="m_project_name")
                total_principal = st.number_input(
                    "TotalPrincipal（プロジェクト総額）",
                    min_value=0.0,
                    step=100.0,
                    value=float(to_f(p_row.get("TotalPrincipal", 0)) if p_row is not None else 0.0),
                    key="m_total_principal"
                )
                is_compound_in = st.selectbox(
                    "IsCompound",
                    ["FALSE", "TRUE"],
                    index=1 if p_row is not None and str(p_row.get("IsCompound", "")).strip().upper() in ["TRUE", "YES", "1", "はい"] else 0,
                    key="m_is_compound"
                )

            with col2:
                st.markdown("**人数 Num_People**")
                cA, cB, cC = st.columns([1, 1, 2])
                with cA:
                    dec = st.button("−1", key="m_dec_people")
                with cB:
                    inc = st.button("+1", key="m_inc_people")
                with cC:
                    new_n = st.number_input("現在人数", min_value=1, step=1, value=int(cur_n), key="m_people_now")

            if selected_project:
                if inc:
                    new_n = int(cur_n) + 1
                if dec:
                    new_n = max(1, int(cur_n) - 1)

            if selected_project and p_row is not None:
                base_names, base_princs = resize_project_lists(p_row, int(new_n))
            else:
                base_names = [f"No.{i+1}" for i in range(int(new_n))]
                base_princs = ["0" for _ in range(int(new_n))]

            st.caption("人数変更で自動増減（末尾値で埋め／切り詰め）。")
            names_text = st.text_input("MemberNames（カンマ区切り）", value=",".join(base_names), key="m_member_names")
            principals_text = st.text_input("IndividualPrincipals（カンマ区切り：入出金・収益で自動更新）", value=",".join(base_princs), key="m_individual_principals")
            lineid_text = st.text_input("LineID（Settings側。LineIDシートがあれば未使用）", value=str(p_row.get("LineID", "")) if p_row is not None else "", key="m_lineid_fallback")

            colS, colD = st.columns(2)
            with colS:
                if st.button("✅ Settingsに保存（追加/更新）", key="m_save_settings"):
                    if not new_name.strip():
                        st.error("Project_Nameが空です。")
                        st.stop()

                    payload = {
                        "Project_Name": new_name.strip(),
                        "Num_People": str(int(new_n)),
                        "TotalPrincipal": str(float(total_principal)),
                        "IndividualPrincipals": principals_text.strip(),
                        "ProfitRates": "",
                        "IsCompound": is_compound_in,
                        "MemberNames": names_text.strip(),
                        "LineID": lineid_text.strip(),
                    }
                    settings_df2 = upsert_project(settings_df, new_name.strip(), payload)
                    df_to_ws(settings_ws, settings_df2)
                    st.success("保存しました。")
                    st.rerun()

            with colD:
                if selected_project and st.button("🗑 プロジェクト削除", key="m_delete_project"):
                    settings_df2 = delete_project(settings_df, selected_project)
                    df_to_ws(settings_ws, settings_df2)
                    st.success("削除しました。")
                    st.rerun()

            st.markdown("---")
            st.markdown("### 現在のSettings（管理者のみ確認可）")
            st.dataframe(settings_df, use_container_width=True)

    # =========================
    # Need project selected
    # =========================
    if not selected_project:
        with tab_profit:
            st.warning("左のサイドバーでプロジェクトを選択してください。")
        with tab_cash:
            st.warning("左のサイドバーでプロジェクトを選択してください。")
        st.stop()

    # Project config
    p_info = settings_df[settings_df["Project_Name"].astype(str) == str(selected_project)].iloc[0]
    num_people = int(to_f(p_info.get("Num_People", 1))) or 1
    project_total_principal = float(to_f(p_info.get("TotalPrincipal", 0)))
    is_compound = str(p_info.get("IsCompound", "")).strip().upper() in ["TRUE", "YES", "1", "はい"]

    member_names = split_csv(p_info.get("MemberNames", ""), num_people, default="No.1")
    member_names = [nm if nm else f"No.{i+1}" for i, nm in enumerate(member_names)]
    labels = member_names[:num_people]

    base_principals = [to_f(x) for x in split_csv(p_info.get("IndividualPrincipals", ""), num_people, default="0")]

    # fallback IDs from Settings if LineID sheet empty
    if not user_ids:
        user_ids = only_line_ids(split_csv(p_info.get("LineID", ""), 999, default=""))

    st.sidebar.info(f"計算モード: {'複利' if is_compound else '単利'}")
    st.sidebar.write(f"送信先ID数: {len(user_ids)}")

    # History sheet per project
    try:
        hist_ws = sh.worksheet(selected_project)
        hist_df = clean_cols(ws_to_df(hist_ws))
    except:
        hist_ws = sh.add_worksheet(title=selected_project, rows=1000, cols=20)
        hist_df = pd.DataFrame(columns=["Date", "Type", "Total_Amount", "Breakdown", "Note"])
        df_to_ws(hist_ws, hist_df)

    # Current principals are from Settings now (収益・入出金でSettingsが更新されるため)
    calc_principals = base_principals[:]  # 表示用

    # =========================
    # Profit tab (TotalPrincipal × APR × 0.66, equal split)
    # =========================
    with tab_profit:
        st.subheader(f"【{selected_project}】本日の収益（総額×APR×66%→均等配分 / 収益は総額・個別元本に加算）")

        total_apr = st.number_input("本日のAPR (%)", value=100.0, step=0.1, key="p_apr")
        net_factor = 0.66

        uploaded_file = st.file_uploader("エビデンス画像をアップロード（任意）", type=["png", "jpg", "jpeg"], key="p_file")
        if uploaded_file:
            st.image(uploaded_file, caption="送信プレビュー", width=420)

        before_total = project_total_principal

        project_daily_yield = (project_total_principal * (total_apr / 100.0) * net_factor) / 365.0
        per_person = round(project_daily_yield / max(1, num_people), 4)
        today_yields = [per_person] * num_people
        total_yield = float(sum(today_yields))
        after_total = before_total + total_yield

        st.info(
            f"総額: ${project_total_principal:,.2f} / "
            f"1日原資(66%): ${project_daily_yield:,.4f} / "
            f"1人: ${per_person:,.4f}"
        )

        cols = st.columns(num_people if num_people <= 6 else 6)
        for i in range(num_people):
            with cols[i % len(cols)]:
                st.metric(labels[i], f"個別元本: ${calc_principals[i]:,.2f}", f"+${today_yields[i]:,.4f}")

        if st.button("収益を保存して（画像付きで）LINE送信", key="p_send"):
            image_url = None
            if uploaded_file:
                with st.spinner("ImgBBへ画像アップロード中..."):
                    image_url = upload_imgbb(uploaded_file.getvalue())
                if uploaded_file and not image_url:
                    st.error("画像アップロード失敗（ImgBB）。画像なしで続行するなら画像を外して再実行。")
                    st.stop()

            # 履歴保存
            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": "収益",
                "Total_Amount": total_yield,
                "Breakdown": join_csv(today_yields),
                "Note": f"APR:{total_apr}% net:{net_factor}"
            }])
            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            df_to_ws(hist_ws, updated_hist)

            # --- ★収益を Settings に反映（総額＋個別元本を増やす） ---
            update_settings_total_principal(settings_ws, settings_df, selected_project, delta_amount=total_yield)
            update_settings_individual_principals_batch(settings_ws, settings_df, selected_project, deltas=today_yields)

            # 管理者通知（収益）
            # ※収益は全員対象なので member は "全員"
            if get_admin_notify_ids():
                notify_admin_cash_event(
                    action_type="収益",
                    project=selected_project,
                    member="全員",
                    amount=total_yield,
                    memo=f"APR:{total_apr}% net:{net_factor}",
                    before_principal=0.0,
                    after_principal=0.0,
                    before_total=before_total,
                    after_total=after_total
                )

            # LINE送信（参加者）
            jst_now = datetime.utcnow() + timedelta(hours=9)
            now_str = jst_now.strftime("%Y/%m/%d %H:%M")

            msg = "🏦 【資産運用収益報告書】\n"
            msg += f"プロジェクト: {selected_project}\n"
            msg += f"報告日時: {now_str}\n"
            msg += f"本日のAPR: {total_apr}%\n"
            msg += f"配分: 総額×APR×66% を {num_people}人で均等\n"
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
    # Cash tab (Deposit/Withdraw + update TotalPrincipal +/- + notify admin)
    # =========================
    with tab_cash:
        st.subheader("💳 入金・出金の記録（総額・個別元本を増減／管理者へLINE通知）")

        action = st.radio("種別", ["➕ 入金（追加元本）", "💸 出金（精算）"], horizontal=True, key="c_action")

        target = st.selectbox("メンバー", labels, key="c_member")
        idx = labels.index(target)

        # 出金可能は「個別元本」を下限0で
        available = max(0.0, float(calc_principals[idx]))
        st.caption(f"個別元本（現在）: ${calc_principals[idx]:,.2f} / 出金可能（下限0）: ${available:,.2f}")

        if action.startswith("💸"):
            if available <= 0:
                st.warning("出金可能額がありません。")
                st.stop()
            amt = st.number_input("出金額 ($)", min_value=0.0, max_value=available, step=10.0, key="c_amt_w")
            memo = st.text_input("備考", value="出金精算", key="c_memo_w")
            rtype = "出金"
            delta = -float(amt)
        else:
            amt = st.number_input("入金額 ($)", min_value=0.0, step=10.0, key="c_amt_d")
            memo = st.text_input("備考", value="追加入金", key="c_memo_d")
            rtype = "入金"
            delta = float(amt)

        if st.button("保存", key="c_save"):
            if amt <= 0:
                st.warning("金額が0です。")
                st.stop()

            # 履歴追記
            vec = [0.0] * num_people
            vec[idx] = float(amt)

            new_row = pd.DataFrame([{
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Type": rtype,
                "Total_Amount": float(amt),
                "Breakdown": join_csv(vec),
                "Note": memo
            }])

            updated_hist = pd.concat([hist_df, new_row], ignore_index=True)
            df_to_ws(hist_ws, updated_hist)

            # --- ★Settingsへ反映（総額も個別元本も増減） ---
            before_total = project_total_principal
            after_total = before_total + float(delta)

            before_p = float(calc_principals[idx])
            after_p = before_p + float(delta)

            # TotalPrincipal を増減
            update_settings_total_principal(settings_ws, settings_df, selected_project, delta_amount=float(delta))

            # IndividualPrincipals を対象者だけ増減
            update_settings_individual_principal_one(
                settings_ws=settings_ws,
                settings_df=settings_df,
                project_name=selected_project,
                member_index=idx,
                delta_amount=float(delta)
            )

            # 管理者通知
            notify_admin_cash_event(
                action_type=rtype,
                project=selected_project,
                member=labels[idx],
                amount=float(amt),
                memo=memo,
                before_principal=before_p,
                after_principal=after_p,
                before_total=before_total,
                after_total=after_total
            )

            st.success(f"{rtype}を記録しました（総額・個別元本を更新）。")
            st.rerun()

except Exception as e:
    st.error(f"システムエラー: {e}")
