# app.py
# APR資産運用管理システム（Settings / Members / Ledger）
# - 429対策：全シートまとめ読み + cache(ttl) + 書き込み後cacheクリア + 429時は直近キャッシュで継続
# - APR：プロジェクトの総残高(Deposit-Withdraw) * APR_Rate / 365 をメンバー均等割り
# - LINE：APR/入出金を個別送信（Line_User_IDがある人のみ）
# - 画像添付：ImgBBにアップ→LINEへ画像メッセージ添付（任意）

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ------------------------
# Timezone
# ------------------------
JST = timezone(timedelta(hours=9), "JST")


def now_jst() -> datetime:
    return datetime.now(JST)


def jst_str(dt: datetime) -> str:
    return dt.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def is_truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def fnum(x: Any) -> float:
    try:
        return float(str(x).replace(",", "").replace("$", "").strip())
    except Exception:
        return 0.0


# ------------------------
# Headers
# ------------------------
DEFAULT_SETTINGS_HEADERS = [
    "Project_Name",
    "APR_Rate",          # 0.67 など（年利）
    "Currency",          # JPY/USDT...
    "Note",
    "UpdatedAt_JST",
]

DEFAULT_MEMBERS_HEADERS = [
    "Project_Name",
    "PersonName",
    "Line_User_ID",
    "LINE_DisplayName",
    "IsActive",          # TRUE/FALSE
    "CreatedAt_JST",
    "UpdatedAt_JST",
]

DEFAULT_LEDGER_HEADERS = [
    "Datetime_JST",
    "Project_Name",
    "PersonName",
    "Type",              # Deposit / Withdraw / APR / Other
    "Amount",
    "Currency",
    "Note",
    "ImageURL",          # ← 画像URL（任意）
    "Line_User_ID",
    "LINE_DisplayName",
    "Source",            # app / make / line / other
]


# ------------------------
# Admin Auth
# ------------------------
def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))


def admin_login_ui() -> None:
    pin_required = safe_str(st.secrets.get("admin", {}).get("pin", ""))
    if not pin_required:
        st.warning("Secrets の [admin].pin が未設定です。管理画面を保護できません。")
        st.session_state["admin_ok"] = False
        return

    if is_admin():
        c1, c2 = st.columns([1, 1])
        with c1:
            st.success("管理者ログイン中")
        with c2:
            if st.button("ログアウト", use_container_width=True):
                st.session_state["admin_ok"] = False
                st.toast("ログアウトしました")
        st.divider()
        return

    with st.form("admin_login_form", clear_on_submit=False):
        pin = st.text_input("管理者パス", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if pin == pin_required:
                st.session_state["admin_ok"] = True
                st.success("ログイン成功")
            else:
                st.session_state["admin_ok"] = False
                st.error("パスが違います")


# ------------------------
# Config
# ------------------------
@dataclass
class GSheetsConfig:
    spreadsheet_id: str
    settings_sheet: str = "Settings"
    members_sheet: str = "Members"
    ledger_sheet: str = "Ledger"


def _extract_spreadsheet_id(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if "/spreadsheets/d/" in s:
        try:
            return s.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception:
            return s
    return s


def load_cfg_from_secrets() -> Optional[GSheetsConfig]:
    con = st.secrets.get("connections", {}).get("gsheets", {})
    raw = safe_str(con.get("spreadsheet", "")).strip()
    sid = _extract_spreadsheet_id(raw)
    if not sid:
        return None
    return GSheetsConfig(spreadsheet_id=sid)


def load_line_token() -> str:
    return safe_str(st.secrets.get("app", {}).get("line_channel_access_token", "")).strip()


def load_imgbb_key() -> str:
    return safe_str(st.secrets.get("imgbb", {}).get("api_key", "")).strip()


# ------------------------
# LINE Push (text + optional image)
# ------------------------
def line_push_message(channel_access_token: str, to_user_id: str, text: str, image_url: Optional[str] = None) -> Tuple[bool, int]:
    if not channel_access_token:
        return (False, 0)
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {channel_access_token}",
        "Content-Type": "application/json",
    }

    messages = [{"type": "text", "text": text}]
    if image_url:
        messages.append({
            "type": "image",
            "originalContentUrl": image_url,
            "previewImageUrl": image_url,
        })

    payload = {"to": str(to_user_id), "messages": messages}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    return (200 <= r.status_code < 300, r.status_code)


# ------------------------
# ImgBB upload
# ------------------------
def upload_imgbb(api_key: str, file_bytes: bytes) -> Optional[str]:
    if not api_key:
        return None
    try:
        res = requests.post(
            "https://api.imgbb.com/1/upload",
            params={"key": api_key},
            files={"image": file_bytes},
            timeout=40
        )
        data = res.json()
        return data["data"]["url"]
    except Exception:
        return None


# ------------------------
# Google Sheets Client
# ------------------------
class GSheets:
    def __init__(self, cfg: GSheetsConfig):
        self.cfg = cfg

        con = st.secrets.get("connections", {}).get("gsheets", {})
        creds_info = con.get("credentials", None)
        if not creds_info:
            st.error("Secrets に [connections.gsheets.credentials] がありません。")
            st.stop()

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(dict(creds_info), scopes=scopes)
        self.gc = gspread.authorize(creds)

        if not self.cfg.spreadsheet_id:
            st.error("Spreadsheet ID が空です（secrets の connections.gsheets.spreadsheet を確認）。")
            st.stop()

        try:
            self.book = self.gc.open_by_key(self.cfg.spreadsheet_id)
        except APIError as e:
            st.error(f"Spreadsheet を開けません: {e}")
            st.stop()

        self._ensure_sheet(self.cfg.settings_sheet, DEFAULT_SETTINGS_HEADERS)
        self._ensure_sheet(self.cfg.members_sheet, DEFAULT_MEMBERS_HEADERS)
        self._ensure_sheet(self.cfg.ledger_sheet, DEFAULT_LEDGER_HEADERS)

    def _ensure_sheet(self, title: str, headers: List[str], rows: int = 2000, cols: int = 26) -> None:
        try:
            try:
                ws = self.book.worksheet(title)
            except Exception:
                ws = self.book.add_worksheet(title=title, rows=str(rows), cols=str(cols))

            values = ws.get_all_values()
            if not values:
                ws.append_row(headers, value_input_option="USER_ENTERED")
                return

            current = values[0]
            current = [str(h).strip() for h in current if str(h).strip() != ""]
            if not current:
                ws.update("1:1", [headers])
                return

            missing = [h for h in headers if h not in current]
            if missing:
                ws.update("1:1", [current + missing])

        except APIError as e:
            st.error(f"シート初期化に失敗: {title} / {e}")
            st.stop()

    def ws(self, name: str):
        return self.book.worksheet(name)

    def append_row(self, sheet_name: str, row: List[Any]) -> None:
        try:
            self.ws(sheet_name).append_row([safe_str(x) for x in row], value_input_option="USER_ENTERED")
        except APIError as e:
            st.error(f"書き込みエラー: {sheet_name} / {e}")
            st.stop()

    def upsert_setting_project(self, project_name: str, apr_rate: float, currency: str, note: str = "") -> None:
        ws = self.ws(self.cfg.settings_sheet)
        values = ws.get_all_values()
        if not values:
            ws.append_row(DEFAULT_SETTINGS_HEADERS, value_input_option="USER_ENTERED")
            values = [DEFAULT_SETTINGS_HEADERS]

        headers = values[0]
        col = {h: i for i, h in enumerate(headers)}

        def get_cell(row: List[str], key: str) -> str:
            i = col.get(key, -1)
            return row[i] if 0 <= i < len(row) else ""

        target_row = None
        for r_i in range(2, len(values) + 1):
            row = values[r_i - 1]
            if get_cell(row, "Project_Name") == project_name:
                target_row = r_i
                break

        ts = jst_str(now_jst())
        out = [""] * len(headers)
        out[col["Project_Name"]] = project_name
        out[col["APR_Rate"]] = str(apr_rate)
        out[col["Currency"]] = currency
        out[col["Note"]] = note
        out[col["UpdatedAt_JST"]] = ts

        if target_row is None:
            ws.append_row(out, value_input_option="USER_ENTERED")
        else:
            ws.update(f"A{target_row}:{chr(ord('A') + len(headers) - 1)}{target_row}", [out])

    def upsert_member(self, project_name: str, person_name: str, line_user_id: str, display_name: str, is_active: bool) -> None:
        ws = self.ws(self.cfg.members_sheet)
        values = ws.get_all_values()
        if not values:
            ws.append_row(DEFAULT_MEMBERS_HEADERS, value_input_option="USER_ENTERED")
            values = [DEFAULT_MEMBERS_HEADERS]

        headers = values[0]
        col = {h: i for i, h in enumerate(headers)}

        def get_cell(row: List[str], key: str) -> str:
            i = col.get(key, -1)
            return row[i] if 0 <= i < len(row) else ""

        target_row = None
        for r_i in range(2, len(values) + 1):
            row = values[r_i - 1]
            if get_cell(row, "Project_Name") == project_name and get_cell(row, "PersonName") == person_name:
                target_row = r_i
                break

        ts = jst_str(now_jst())
        if target_row is None:
            out = [""] * len(headers)
            out[col["Project_Name"]] = project_name
            out[col["PersonName"]] = person_name
            out[col["Line_User_ID"]] = line_user_id
            out[col["LINE_DisplayName"]] = display_name
            out[col["IsActive"]] = "TRUE" if is_active else "FALSE"
            out[col["CreatedAt_JST"]] = ts
            out[col["UpdatedAt_JST"]] = ts
            ws.append_row(out, value_input_option="USER_ENTERED")
        else:
            def update_cell(key: str, value: str):
                c = col[key] + 1
                ws.update_cell(target_row, c, value)

            update_cell("Line_User_ID", line_user_id)
            update_cell("LINE_DisplayName", display_name)
            update_cell("IsActive", "TRUE" if is_active else "FALSE")
            update_cell("UpdatedAt_JST", ts)


# ------------------------
# 429対策：3シートまとめ読み（TTLキャッシュ）
# - 429が起きたら前回の成功データで継続
# ------------------------
@st.cache_data(ttl=60, show_spinner=False)
def load_all_tables_cached(spreadsheet_id: str, creds_fingerprint: str) -> Dict[str, pd.DataFrame]:
    # ※ creds_fingerprint はキャッシュ分離用（中身は使わない）
    # ここでは「実際のgspread呼び出し」はしない（Streamlitのキャッシュ整合のため）
    return {}


def _fix_duplicate_headers(headers: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out = []
    for h in headers:
        h = str(h).strip()
        if h == "":
            h = "Unnamed"
        if h in seen:
            seen[h] += 1
            out.append(f"{h}__{seen[h]}")
        else:
            seen[h] = 0
            out.append(h)
    return out


def load_all_tables(gs: GSheets) -> Dict[str, pd.DataFrame]:
    """
    Settings/Members/Ledgerをまとめて読む。
    429時は直近session_stateに残っている last_good を返す。
    """
    key = f"{gs.cfg.spreadsheet_id}"
    last_good = st.session_state.get("last_good_tables", {})

    try:
        out: Dict[str, pd.DataFrame] = {}

        for name in [gs.cfg.settings_sheet, gs.cfg.members_sheet, gs.cfg.ledger_sheet]:
            ws = gs.ws(name)
            values = ws.get_all_values()  # ← 読み取りはここだけ（3回）
            if not values:
                out[name] = pd.DataFrame()
                continue
            headers = _fix_duplicate_headers(values[0])
            rows = values[1:]
            out[name] = pd.DataFrame(rows, columns=headers).fillna("")

        st.session_state["last_good_tables"] = out
        return out

    except APIError as e:
        # 429など
        if "429" in str(e):
            st.warning("Google Sheetsの読み取り上限(429)に達しました。しばらく待つか、キャッシュを使って表示します。")
            if last_good:
                return last_good
        st.error(f"読み取りエラー: {e}")
        st.stop()


def clear_tables_cache():
    # cache_data も session_state も両方クリア
    st.cache_data.clear()
    if "last_good_tables" in st.session_state:
        del st.session_state["last_good_tables"]


# ------------------------
# Business logic helpers
# ------------------------
def project_list_from_settings(df_settings: pd.DataFrame) -> List[str]:
    if df_settings.empty or "Project_Name" not in df_settings.columns:
        return []
    return sorted(list({str(x).strip() for x in df_settings["Project_Name"].tolist() if str(x).strip()}))


def project_setting(df_settings: pd.DataFrame, project: str) -> Tuple[float, str]:
    # デフォルト 0.67 / JPY
    apr = 0.67
    cur = "JPY"
    if df_settings.empty or "Project_Name" not in df_settings.columns:
        return apr, cur
    m = df_settings[df_settings["Project_Name"].astype(str) == str(project)]
    if m.empty:
        return apr, cur
    r = m.iloc[0].to_dict()
    try:
        apr = float(str(r.get("APR_Rate", "0.67")).strip() or "0.67")
    except Exception:
        apr = 0.67
    cur = str(r.get("Currency", "JPY")).strip() or "JPY"
    return apr, cur


def members_for_project(df_members: pd.DataFrame, project: str) -> pd.DataFrame:
    if df_members.empty:
        return pd.DataFrame()
    if "Project_Name" not in df_members.columns:
        return pd.DataFrame()
    m = df_members[df_members["Project_Name"].astype(str) == str(project)].copy()
    if "IsActive" in m.columns:
        m = m[m["IsActive"].astype(str).str.lower().isin(["true", "1", "yes", "y", "on"])]
    return m


def balance_for_project(df_ledger: pd.DataFrame, project: str) -> float:
    if df_ledger.empty:
        return 0.0
    need = {"Project_Name", "Type", "Amount"}
    if not need.issubset(set(df_ledger.columns)):
        return 0.0
    dfp = df_ledger[df_ledger["Project_Name"].astype(str) == str(project)].copy()
    if dfp.empty:
        return 0.0
    dfp["AmountNum"] = dfp["Amount"].apply(fnum)
    dep = dfp[dfp["Type"].astype(str) == "Deposit"]["AmountNum"].sum()
    wdr = dfp[dfp["Type"].astype(str) == "Withdraw"]["AmountNum"].sum()
    return float(dep - wdr)


# ------------------------
# UI
# ------------------------
def ui_sidebar_debug(cfg: GSheetsConfig):
    with st.sidebar:
        st.write("🔎 Debug")
        st.caption(f"spreadsheet: {cfg.spreadsheet_id}")
        con = st.secrets.get("connections", {}).get("gsheets", {})
        creds = con.get("credentials", {})
        st.caption(f"client_email: {safe_str(creds.get('client_email',''))}")
        st.caption(f"token_uri: {safe_str(creds.get('token_uri',''))}")
        st.caption("Sheets: Settings / Members / Ledger")
        if st.button("キャッシュクリア（429対策）"):
            clear_tables_cache()
            st.toast("キャッシュをクリアしました")


def ui_apr(gs: GSheets, tables: Dict[str, pd.DataFrame]):
    st.subheader("📈 APR報告（個別にLINE送信 + 台帳にAPR記録）")

    df_settings = tables.get(gs.cfg.settings_sheet, pd.DataFrame())
    df_members = tables.get(gs.cfg.members_sheet, pd.DataFrame())
    df_ledger = tables.get(gs.cfg.ledger_sheet, pd.DataFrame())

    projects = project_list_from_settings(df_settings)
    project = st.selectbox("Project", options=[""] + projects, key="apr_project")
    if not project:
        st.info("Project を選んでください")
        return

    apr_rate, currency = project_setting(df_settings, project)
    balance = balance_for_project(df_ledger, project)

    st.write(f"現在総額（Deposit-Withdraw）: **{balance:,.2f} {currency}**")
    st.write(f"APR_Rate（年利）: **{apr_rate:.2f}**（例：0.67 = 67%）")

    daily_profit_total = balance * float(apr_rate) / 365.0

    members = members_for_project(df_members, project)
    if members.empty or "PersonName" not in members.columns:
        st.warning("Members にこのプロジェクトのメンバーがいません。")
        return

    n = len(members)
    per = daily_profit_total / n if n > 0 else 0.0

    st.write(f"人数: **{n}** / 本日の収益合計: **{daily_profit_total:,.2f} {currency}** / 1人あたり: **{per:,.2f} {currency}**")

    # 画像添付（任意）
    uploaded = st.file_uploader("エビデンス画像（任意：ImgBB→LINE画像添付）", type=["png", "jpg", "jpeg"])
    if uploaded:
        st.image(uploaded, caption="プレビュー", width=420)

    line_token = load_line_token()
    imgbb_key = load_imgbb_key()

    send_line = st.checkbox("個別LINE送信（Line_User_IDがある人のみ）", value=bool(line_token))
    record_ledger = st.checkbox("LedgerにAPRとして記録する", value=True)

    note = st.text_input("メモ（Note）", value=f"APR daily ({apr_rate:.2f}/year)")

    if st.button("APRを実行（記録/送信）", type="primary"):
        dt = now_jst()

        image_url = None
        if uploaded:
            if not imgbb_key:
                st.error("ImgBBのapi_keyがSecretsにありません（[imgbb].api_key）。画像添付を外すか、Secretsに追加してください。")
                st.stop()
            with st.spinner("画像をアップロード中（ImgBB）..."):
                image_url = upload_imgbb(imgbb_key, uploaded.getvalue())
            if not image_url:
                st.error("画像アップロードに失敗しました。画像を外して再実行してください。")
                st.stop()

        ok_count = 0
        fail_count = 0

        for _, r in members.iterrows():
            person = safe_str(r.get("PersonName", ""))
            uid = safe_str(r.get("Line_User_ID", ""))
            disp = safe_str(r.get("LINE_DisplayName", ""))

            # Ledger記録
            if record_ledger:
                row = [
                    dt.strftime("%Y-%m-%d %H:%M:%S"),
                    project,
                    person,
                    "APR",
                    round(per, 2),
                    currency,
                    note,
                    image_url or "",
                    uid,
                    disp,
                    "app",
                ]
                gs.append_row(gs.cfg.ledger_sheet, row)

            # LINE送信
            if send_line and line_token and uid:
                msg = (
                    f"【APR報告】{project}\n"
                    f"本日の収益: {per:,.2f} {currency}\n"
                    f"年利: {apr_rate:.2f}\n"
                    f"日時(JST): {jst_str(dt)}\n"
                )
                ok, status = line_push_message(line_token, uid, msg, image_url=image_url)
                if ok:
                    ok_count += 1
                else:
                    fail_count += 1

        # 書き込み後はキャッシュを消す（次回表示が最新になる）
        clear_tables_cache()

        st.success(f"完了：LINE成功 {ok_count} / 失敗 {fail_count}（Ledger記録={record_ledger}）")
        st.rerun()


def ui_ledger(gs: GSheets, tables: Dict[str, pd.DataFrame]):
    st.subheader("📒 入金/出金（個別LINE通知 + 台帳記録）")

    df_settings = tables.get(gs.cfg.settings_sheet, pd.DataFrame())
    df_members = tables.get(gs.cfg.members_sheet, pd.DataFrame())
    df_ledger = tables.get(gs.cfg.ledger_sheet, pd.DataFrame())

    projects = project_list_from_settings(df_settings)
    project = st.selectbox("Project", options=[""] + projects, key="ledger_project")
    if not project:
        st.info("Project を選んでください")
        return

    apr_rate, currency_default = project_setting(df_settings, project)

    members = members_for_project(df_members, project)
    people = []
    if not members.empty and "PersonName" in members.columns:
        people = sorted([str(x).strip() for x in members["PersonName"].tolist() if str(x).strip()])

    tab1, tab2 = st.tabs(["記録する", "見る"])
    with tab1:
        with st.form("ledger_add", clear_on_submit=False):
            d = st.date_input("日付(JST)", value=now_jst().date())
            t = st.time_input("時刻(JST)", value=now_jst().time().replace(second=0, microsecond=0))
            dt = datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, tzinfo=JST)

            person = st.selectbox("PersonName", options=[""] + people)
            typ = st.selectbox("Type", options=["Deposit", "Withdraw"])
            amount = st.number_input("Amount", min_value=0.0, value=0.0, step=1000.0)
            currency = st.text_input("Currency", value=currency_default)
            note = st.text_input("Note", value="")
            source = st.selectbox("Source", options=["app", "make", "line", "other"])

            uploaded = st.file_uploader("画像（任意：ImgBB→LINE添付）", type=["png", "jpg", "jpeg"], key="ledger_image")
            if uploaded:
                st.image(uploaded, caption="プレビュー", width=420)

            ok = st.form_submit_button("追加（記録/送信）")

        if ok:
            if not person:
                st.error("PersonName を選んでください")
                return
            if amount <= 0:
                st.error("Amount は 0 より大きくしてください")
                return

            # member情報
            uid = ""
            disp = ""
            if not members.empty:
                m = members[members["PersonName"].astype(str) == str(person)]
                if not m.empty:
                    r = m.iloc[0].to_dict()
                    uid = safe_str(r.get("Line_User_ID", ""))
                    disp = safe_str(r.get("LINE_DisplayName", ""))

            # 画像アップロード
            image_url = ""
            if uploaded:
                imgbb_key = load_imgbb_key()
                if not imgbb_key:
                    st.error("ImgBBのapi_keyがSecretsにありません（[imgbb].api_key）。画像添付を外すか、Secretsに追加してください。")
                    return
                with st.spinner("画像をアップロード中（ImgBB）..."):
                    u = upload_imgbb(imgbb_key, uploaded.getvalue())
                if not u:
                    st.error("画像アップロードに失敗しました。画像を外して再実行してください。")
                    return
                image_url = u

            # Ledger記録
            row = [
                dt.strftime("%Y-%m-%d %H:%M:%S"),
                project,
                person,
                typ,
                float(amount),
                currency,
                note,
                image_url,
                uid,
                disp,
                source,
            ]
            gs.append_row(gs.cfg.ledger_sheet, row)

            # LINE通知（個人）
            line_token = load_line_token()
            if line_token and uid:
                msg = (
                    f"【{typ}通知】{project}\n"
                    f"{person} 様\n"
                    f"金額: {amount:,.2f} {currency}\n"
                    f"日時(JST): {jst_str(dt)}\n"
                )
                ok2, status = line_push_message(line_token, uid, msg, image_url=image_url or None)
                if not ok2:
                    st.warning(f"LINE送信に失敗しました（HTTP {status}）")

            clear_tables_cache()
            st.success("記録しました（必要ならLINE送信も実行）")
            st.rerun()

    with tab2:
        if df_ledger.empty:
            st.info("Ledger が空です")
            return
        if "Project_Name" in df_ledger.columns:
            show = df_ledger[df_ledger["Project_Name"].astype(str) == str(project)]
        else:
            show = df_ledger
        st.dataframe(show, use_container_width=True, hide_index=True)


def ui_admin(gs: GSheets, tables: Dict[str, pd.DataFrame]):
    st.subheader("⚙ 管理（管理者のみ）")
    admin_login_ui()
    if not is_admin():
        st.info("管理者パスを入力すると管理機能が表示されます")
        st.stop()

    st.success("管理者モード")

    st.write("### ヘッダー（コピペ用）")
    st.write("**Settings**")
    st.code("\t".join(DEFAULT_SETTINGS_HEADERS))
    st.write("**Members**")
    st.code("\t".join(DEFAULT_MEMBERS_HEADERS))
    st.write("**Ledger**")
    st.code("\t".join(DEFAULT_LEDGER_HEADERS))

    st.divider()

    # Settings upsert
    st.write("## Settings 更新")
    df_settings = tables.get(gs.cfg.settings_sheet, pd.DataFrame())
    st.dataframe(df_settings, use_container_width=True, hide_index=True)

    with st.form("settings_upsert", clear_on_submit=False):
        project = st.text_input("Project_Name", placeholder="例: プロジェクトA")
        apr_rate = st.number_input("APR_Rate（年利）", min_value=0.0, max_value=10.0, value=0.67, step=0.01)
        currency = st.text_input("Currency", value="JPY")
        note = st.text_input("Note", value="")
        ok = st.form_submit_button("保存")
    if ok:
        if not project.strip():
            st.error("Project_Name は必須です")
        else:
            gs.upsert_setting_project(project.strip(), float(apr_rate), currency.strip(), note.strip())
            clear_tables_cache()
            st.success("保存しました")
            st.rerun()

    st.divider()

    # Members upsert
    st.write("## Members 更新（Project + PersonName でUpsert）")
    df_members = tables.get(gs.cfg.members_sheet, pd.DataFrame())
    st.dataframe(df_members, use_container_width=True, hide_index=True)

    projects = project_list_from_settings(df_settings)
    prj = st.selectbox("Project（Members用）", options=[""] + projects, key="admin_members_project")
    with st.form("member_upsert", clear_on_submit=False):
        person = st.text_input("PersonName", placeholder="例: 祥子")
        line_uid = st.text_input("Line_User_ID", placeholder="Uxxxxxxxxxxxx")
        disp = st.text_input("LINE_DisplayName", placeholder="LINEの表示名")
        active = st.checkbox("IsActive", value=True)
        okm = st.form_submit_button("保存")
    if okm:
        if not prj:
            st.error("Project を選んでください")
        elif not person.strip():
            st.error("PersonName は必須です")
        else:
            gs.upsert_member(prj, person.strip(), line_uid.strip(), disp.strip(), bool(active))
            clear_tables_cache()
            st.success("保存しました")
            st.rerun()


# ------------------------
# Main
# ------------------------
def main():
    st.set_page_config(page_title="APR資産運用管理システム", layout="wide")
    st.title("🏦 APR資産運用管理システム")

    cfg = load_cfg_from_secrets()
    if cfg is None:
        st.error("Secrets の [connections.gsheets].spreadsheet が未設定です。")
        st.stop()

    ui_sidebar_debug(cfg)

    gs = GSheets(cfg)

    # 429対策：最初にまとめ読み（3回だけ）
    tables = load_all_tables(gs)

    tab_apr, tab_ledger, tab_members, tab_admin = st.tabs(
        ["📈 APR報告", "📒 入金/出金", "👤 Members（閲覧）", "⚙ 管理"]
    )

    with tab_apr:
        ui_apr(gs, tables)

    with tab_ledger:
        ui_ledger(gs, tables)

    with tab_members:
        df_members = tables.get(gs.cfg.members_sheet, pd.DataFrame())
        if df_members.empty:
            st.info("Members が空です")
        else:
            st.dataframe(df_members, use_container_width=True, hide_index=True)

    with tab_admin:
        ui_admin(gs, tables)


if __name__ == "__main__":
    main()
