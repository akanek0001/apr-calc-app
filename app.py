import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone
import requests, json

import gspread
from google.oauth2.service_account import Credentials

# =========================================================
# APR資産運用管理システム（全体コード：管理者ログインUIを常に表示） 
# =========================================================

st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

JST = timezone(timedelta(hours=9))

def now_jst() -> datetime:
    return datetime.now(JST)

def fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def fmt_time(dt: datetime) -> str:
    return dt.strftime("%H:%M:%S")

def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ---------- Sheets ----------
def gs_client():
    cred_info = st.secrets["connections"]["gsheets"]["credentials"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(cred_info, scopes=scopes)
    return gspread.authorize(creds)

def open_sheet():
    spreadsheet_url = st.secrets["connections"]["gsheets"]["spreadsheet"]
    return gs_client().open_by_url(spreadsheet_url)

def ws_to_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=header)

def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\u3000", " ", regex=False)
        .str.strip()
    )
    return df

def ensure_headers(ws, headers: list[str]):
    df = ws_to_df(ws)
    if df.empty:
        ws.clear()
        ws.update([headers])
        return
    df = clean_cols(df)
    current = list(df.columns)
    missing = [h for h in headers if h not in current]
    if missing:
        df2 = df.reindex(columns=current + missing, fill_value="")
        ws.clear()
        ws.update([df2.columns.tolist()] + df2.astype(str).fillna("").values.tolist())

def df_to_ws(ws, df: pd.DataFrame):
    df = clean_cols(df)
    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).fillna("").values.tolist())

def get_or_create_ws(sh, title: str, headers: list[str], rows=1000, cols=30):
    try:
        ws = sh.worksheet(title)
    except:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
    ensure_headers(ws, headers)
    return ws

# ---------- Utils ----------
def to_f(val) -> float:
    try:
        s = str(val).replace(",", "").replace("$", "").replace("%", "").strip()
        return float(s) if s else 0.0
    except:
        return 0.0

def split_csv(val, n: int) -> list[str]:
    items = [x.strip() for x in str(val).split(",") if str(x).strip() != ""]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

def uniq_keep_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def only_line_ids(values):
    out = []
    for v in values:
        s = str(v).strip()
        if s.startswith("U"):
            out.append(s)
    return uniq_keep_order(out)

# ---------- LINE ----------
def send_line(token: str, user_id: str, text: str, image_url: str | None = None) -> int:
    if not user_id or str(user_id).strip() == "":
        return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    messages = [{"type": "text", "text": text}]
    if image_url:
        messages.append({"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url})
    payload = {"to": str(user_id), "messages": messages}
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
        return r.status_code
    except:
        return 500

def upload_imgbb(file_bytes: bytes) -> str | None:
    try:
        res = requests.post(
            "https://api.imgbb.com/1/upload",
            params={"key": st.secrets["imgbb"]["api_key"]},
            files={"image": file_bytes},
            timeout=30,
        )
        data = res.json()
        return data["data"]["url"]
    except:
        return None

# ---------- Headers ----------
SETTINGS_HEADERS = [
    "Project_Name","Num_People","TotalPrincipal","IndividualPrincipals",
    "ProfitRates","IsCompound","MemberNames","LineID"
]
LINEID_HEADERS = ["Line_User_ID","Line_User","Date","Time","Type"]
MEMBERS_HEADERS = ["MemberName","SheetName","Line_User_ID","Line_User","LinkedAt","Status"]
LEDGER_HEADERS = ["Date","Time","Type","Amount","Balance_After","Note"]

# ---------- Admin ----------
def get_admin_pin() -> str | None:
    # 正式： [admin].pin
    try:
        return str(st.secrets["admin"]["pin"])
    except:
        return None

def is_admin() -> bool:
    return st.session_state.get("is_admin") is True

def admin_login_ui():
    st.markdown("### 🔐 管理者ログイン")

    pin_in_secrets = get_admin_pin()
    if not pin_in_secrets:
        st.error('Secretsに管理者PINがありません。下記をSecretsに追加してください：\n\n[admin]\npin = "1234"')
        # “入力欄”は出すが、検証できないのでログイン不可にする（空画面回避）
        st.text_input("管理者PIN（未設定のためログイン不可）", type="password", disabled=True)
        return False

    if is_admin():
        st.success("管理者ログイン中")
        if st.button("管理者ログアウト"):
            st.session_state["is_admin"] = False
            st.rerun()
        return True

    pin = st.text_input("管理者PIN", type="password")
    if st.button("管理者ログイン"):
        if str(pin) == pin_in_secrets:
            st.session_state["is_admin"] = True
            st.success("ログインしました")
            st.rerun()
        else:
            st.error("PINが違います")
    return False

# =========================================================
# Main
# =========================================================
st.title("🏦 APR資産運用管理システム")

try:
    sh = open_sheet()

    settings_ws = get_or_create_ws(sh, "Settings", SETTINGS_HEADERS)
    lineid_ws   = get_or_create_ws(sh, "LineID", LINEID_HEADERS)
    members_ws  = get_or_create_ws(sh, "Members", MEMBERS_HEADERS)

    settings_df = clean_cols(ws_to_df(settings_ws))
    lineid_df   = clean_cols(ws_to_df(lineid_ws))
    members_df  = clean_cols(ws_to_df(members_ws))

    if settings_df.empty:
        st.error("Settingsシートが空です。")
        st.stop()

    missing = [c for c in SETTINGS_HEADERS if c not in settings_df.columns]
    if missing:
        st.error(f"Settingsシートの列が不足: {missing}")
        st.stop()

    project_list = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    if not project_list:
        st.error("Settingsの Project_Name が空です。")
        st.stop()

    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = settings_df[settings_df["Project_Name"].astype(str) == str(selected_project)].iloc[0]

    num_people = int(to_f(p_info["Num_People"])) if str(p_info["Num_People"]).strip() != "" else 0
    is_compound = str(p_info["IsCompound"]).strip().upper() in ["TRUE", "YES", "1", "はい"]

    member_names = split_csv(p_info["MemberNames"], max(num_people, 1)) if num_people > 0 else []
    if num_people > 0 and len(member_names) < num_people:
        member_names = [f"No.{i+1}" for i in range(num_people)]

    base_principals = [to_f(x) for x in split_csv(p_info["IndividualPrincipals"], max(num_people, 1))] if num_people > 0 else []
    rate_list = [to_f(x) for x in split_csv(p_info["ProfitRates"], max(num_people, 1))] if num_people > 0 else []

    if members_df.empty:
        members_df = pd.DataFrame(columns=MEMBERS_HEADERS)

    linked_ids = only_line_ids(members_df.get("Line_User_ID", pd.Series(dtype=str)).dropna().tolist()) if not members_df.empty else []
    log_ids = only_line_ids(lineid_df.get("Line_User_ID", pd.Series(dtype=str)).dropna().tolist()) if not lineid_df.empty else []
    broadcast_ids = uniq_keep_order(linked_ids + log_ids)

    st.sidebar.info(f"計算モード: {'複利' if is_compound else '単利'}")
    st.sidebar.write(f"全員通知の送信先ID数: {len(broadcast_ids)}")

    tab_profit, tab_cash, tab_admin = st.tabs(
        ["📈 収益確定・全員LINE", "💳 入金・出金（個別通知）", "⚙️ 管理（管理者のみ）"]
    )

    # ---------- Profit ----------
    with tab_profit:
        st.subheader(f"【{selected_project}】本日の収益（全員通知）")
        if num_people <= 0:
            st.warning("Settingsの Num_People が未設定です。")
            st.stop()

        total_apr = st.number_input("本日の全体APR (%)", value=100.0, step=0.1)
        net_factor = 0.67

        uploaded_file = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])
        if uploaded_file:
            st.image(uploaded_file, caption="プレビュー", width=420)

        today_yields = []
        for i in range(num_people):
            p = base_principals[i] if i < len(base_principals) else 0.0
            r = rate_list[i] if i < len(rate_list) else 0.0
            y = (p * (total_apr * net_factor * r / 100.0)) / 365.0
            today_yields.append(round(y, 4))

        cols = st.columns(min(num_people, 6))
        for i in range(num_people):
            with cols[i % len(cols)]:
                nm = member_names[i] if i < len(member_names) else f"No.{i+1}"
                principal = base_principals[i] if i < len(base_principals) else 0.0
                st.metric(nm, f"${principal:,.2f}", f"+${today_yields[i]:,.4f}")

        st.caption("※本日の収益は全員に送信します（個人名はメッセージに含めません）。")

        if st.button("収益を全員にLINE送信（画像あり可）"):
            image_url = None
            if uploaded_file:
                with st.spinner("ImgBBへ画像アップロード中..."):
                    image_url = upload_imgbb(uploaded_file.getvalue())
                if not image_url:
                    st.error("画像アップロード失敗（ImgBB）。画像を外して再実行してください。")
                    st.stop()

            dt = now_jst()
            msg = "🏦 【本日の運用収益報告】\n"
            msg += f"プロジェクト: {selected_project}\n"
            msg += f"日時(JST): {dt.strftime('%Y/%m/%d %H:%M')}\n"
            msg += f"本日のAPR: {total_apr}%\n"
            msg += f"モード: {'複利' if is_compound else '単利'}\n\n"
            msg += f"本日の合計収益（概算）: ${sum(today_yields):,.4f}\n"
            if image_url:
                msg += "\n📎 エビデンス画像を添付します。"

            token = st.secrets["line"]["channel_access_token"]
            ok, ng = 0, 0
            for uid in broadcast_ids:
                code = send_line(token, uid, msg, image_url=image_url)
                if code == 200:
                    ok += 1
                else:
                    ng += 1
            st.success(f"送信完了：成功 {ok} / 失敗 {ng}")

    # ---------- Cash ----------
    with tab_cash:
        st.subheader("💳 入金・出金・収益反映（個別通知 + 個人台帳の残高自動更新）")
        if members_df.empty:
            st.warning("Membersシートが空です。先に⚙️管理でメンバー登録してください。")
            st.stop()

        mdf = members_df.copy()
        for c in MEMBERS_HEADERS:
            if c not in mdf.columns:
                mdf[c] = ""

        mdf_view = mdf[mdf["Line_User_ID"].astype(str).str.startswith("U")].copy()
        if mdf_view.empty:
            st.warning("紐付け済みメンバーがいません。⚙️管理で紐付けしてください。")
            st.stop()

        member_list = mdf_view["MemberName"].astype(str).tolist()
        selected_member = st.selectbox("メンバー（個人名）", member_list)

        row = mdf_view[mdf_view["MemberName"].astype(str) == str(selected_member)].iloc[0]
        sheet_name = str(row.get("SheetName", "")).strip()
        line_user_id = str(row.get("Line_User_ID", "")).strip()
        line_user_name = str(row.get("Line_User", "")).strip()

        if not sheet_name:
            st.error("Membersの SheetName が空です。")
            st.stop()

        ledger_ws = get_or_create_ws(sh, sheet_name, LEDGER_HEADERS)
        ledger_df = clean_cols(ws_to_df(ledger_ws))

        current_balance = 0.0
        if not ledger_df.empty and "Balance_After" in ledger_df.columns:
            current_balance = to_f(ledger_df.iloc[-1].get("Balance_After", 0.0))

        st.info(f"現在残高: ${current_balance:,.2f}（個人台帳: {sheet_name}）")

        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            tx_type = st.selectbox("種別", ["入金", "出金", "収益(APR反映)"])
        with c2:
            amount = st.number_input("金額 ($)", min_value=0.0, step=10.0)
        with c3:
            note = st.text_input("備考", value="")

        if st.button("保存して（個人に）LINE通知"):
            if amount <= 0:
                st.warning("金額が0です。")
                st.stop()

            dt = now_jst()
            amt = float(amount)

            if tx_type == "出金":
                new_balance = current_balance - amt
                sign = "➖"
                title = "💸 出金通知"
                store_type = "出金"
            else:
                new_balance = current_balance + amt
                sign = "➕"
                title = "💰 入金通知" if tx_type == "入金" else "📈 収益反映"
                store_type = "入金" if tx_type == "入金" else "収益"

            if ledger_df.empty:
                ledger_df = pd.DataFrame(columns=LEDGER_HEADERS)
            for h in LEDGER_HEADERS:
                if h not in ledger_df.columns:
                    ledger_df[h] = ""

            new_row = {
                "Date": fmt_date(dt),
                "Time": fmt_time(dt),
                "Type": store_type,
                "Amount": f"{amt:.2f}",
                "Balance_After": f"{new_balance:.2f}",
                "Note": note,
            }
            ledger_df = pd.concat([ledger_df, pd.DataFrame([new_row])], ignore_index=True)
            df_to_ws(ledger_ws, ledger_df)

            token = st.secrets["line"]["channel_access_token"]
            msg = f"{title}\n"
            if line_user_name:
                msg += f"LINE名: {line_user_name}\n"
            msg += f"メンバー: {selected_member}\n"
            msg += f"日時(JST): {dt.strftime('%Y/%m/%d %H:%M')}\n"
            msg += f"内容: {store_type} {sign}${amt:,.2f}\n"
            msg += f"残高: ${new_balance:,.2f}\n"
            if note:
                msg += f"備考: {note}\n"

            code = send_line(token, line_user_id, msg)
            if code == 200:
                st.success("保存しました。個人LINEへ通知しました。")
            else:
                st.warning(f"保存しましたが、LINE送信に失敗しました（HTTP {code}）。")

            st.rerun()

    # ---------- Admin ----------
    with tab_admin:
        st.subheader("⚙️ 管理（管理者のみ）")
        logged_in = admin_login_ui()

        # ログインできてない場合でも、画面を空にしない（ここで終了）
        if not logged_in:
            st.info("管理者ログイン後に、Members管理 / LINE紐付け / Settings確認が表示されます。")
            st.stop()

        # ここから管理者のみ
        settings_df = clean_cols(ws_to_df(settings_ws))
        lineid_df = clean_cols(ws_to_df(lineid_ws))
        members_df = clean_cols(ws_to_df(members_ws))
        if members_df.empty:
            members_df = pd.DataFrame(columns=MEMBERS_HEADERS)
        for c in MEMBERS_HEADERS:
            if c not in members_df.columns:
                members_df[c] = ""

        st.divider()
        with st.expander("🧾 Settings（確認用：管理者のみ）", expanded=False):
            st.dataframe(settings_df, use_container_width=True)

        st.divider()
        st.markdown("## 👤 Members管理")

        left, right = st.columns([1, 1])
        with left:
            st.markdown("### メンバー追加")
            new_member_name = st.text_input("MemberName（表示名）", value="", key="add_member_name")
            new_sheet_name = st.text_input("SheetName（個人台帳シート名）", value="", key="add_sheet_name")

            if st.button("➕ 追加（LINE未紐付け）", key="btn_add_member"):
                if not new_member_name.strip() or not new_sheet_name.strip():
                    st.error("MemberName と SheetName は必須です。")
                    st.stop()
                if (members_df["MemberName"].astype(str) == new_member_name.strip()).any():
                    st.error("同じMemberNameが既に存在します。")
                    st.stop()

                add = {
                    "MemberName": new_member_name.strip(),
                    "SheetName": new_sheet_name.strip(),
                    "Line_User_ID": "",
                    "Line_User": "",
                    "LinkedAt": "",
                    "Status": "unlinked",
                }
                members_df2 = pd.concat([members_df, pd.DataFrame([add])], ignore_index=True)
                df_to_ws(members_ws, members_df2)
                get_or_create_ws(sh, new_sheet_name.strip(), LEDGER_HEADERS)
                st.success("追加しました（個人台帳シートも作成）。")
                st.rerun()

        with right:
            st.markdown("### Members一覧")
            st.dataframe(members_df, use_container_width=True, height=280)

        st.markdown("---")
        st.markdown("### 🔗 LINE紐付け（LineIDログ → Members）")

        if lineid_df.empty:
            st.warning("LineIDシートが空です。")
            st.stop()

        if "Line_User_ID" not in lineid_df.columns or "Line_User" not in lineid_df.columns:
            st.error(f"LineIDシートの列名が不足。必要: Line_User_ID, Line_User / 現在: {list(lineid_df.columns)}")
            st.stop()

        existing_ids = set(only_line_ids(members_df.get("Line_User_ID", pd.Series(dtype=str)).dropna().tolist()))
        candidates = []
        for _, r in lineid_df.iterrows():
            uid = str(r.get("Line_User_ID", "")).strip()
            uname = str(r.get("Line_User", "")).strip()
            if uid.startswith("U") and uid not in existing_ids:
                candidates.append((uid, uname))

        # 重複除去
        seen = set()
        uniq_candidates = []
        for uid, uname in candidates:
            if uid not in seen:
                seen.add(uid)
                uniq_candidates.append((uid, uname))

        if not uniq_candidates:
            st.success("未紐付けのLINEユーザーはありません。")
            st.stop()

        opt_labels = [f"{uname} ({uid})" if uname else uid for uid, uname in uniq_candidates]
        sel = st.selectbox("未紐付けLINEユーザー", opt_labels, key="link_select_line")
        sel_idx = opt_labels.index(sel)
        sel_uid, sel_uname = uniq_candidates[sel_idx]

        member_options = members_df["MemberName"].astype(str).tolist()
        link_to = st.selectbox("紐付けるメンバー", member_options, key="link_select_member")

        if st.button("✅ 紐付け確定（Members更新）", key="btn_link_confirm"):
            idxs = members_df.index[members_df["MemberName"].astype(str) == str(link_to)].tolist()
            if not idxs:
                st.error("対象メンバーが見つかりません。")
                st.stop()
            idx = idxs[0]

            dt = now_jst()
            members_df.loc[idx, "Line_User_ID"] = sel_uid
            members_df.loc[idx, "Line_User"] = sel_uname
            members_df.loc[idx, "LinkedAt"] = fmt_dt(dt)
            members_df.loc[idx, "Status"] = "linked"
            df_to_ws(members_ws, members_df)
            st.success(f"{link_to} に {sel_uid} を紐付けました。")
            st.rerun()

except Exception as e:
    st.error(f"システムエラー: {e}")
