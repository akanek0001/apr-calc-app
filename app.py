from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials


# =========================
# Timezone
# =========================
JST = timezone(timedelta(hours=9), "JST")


def now_jst() -> datetime:
    return datetime.now(JST)


def jst_str(dt: Optional[datetime] = None) -> str:
    dt = dt or now_jst()
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# =========================
# Helpers
# =========================
def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def is_truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on", "はい")


def to_f(v: Any) -> float:
    try:
        s = str(v).replace(",", "").replace("$", "").replace("¥", "").replace("%", "").strip()
        return float(s) if s else 0.0
    except Exception:
        return 0.0


def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\u3000", " ", regex=False)
        .str.strip()
    )
    return df


def uniq_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def extract_sheet_id(value: str) -> str:
    s = (value or "").strip()
    if "/spreadsheets/d/" in s:
        try:
            return s.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception:
            return s
    return s


# =========================
# LINE
# =========================
def send_line_push(token: str, user_id: str, text: str, image_url: Optional[str] = None) -> int:
    if not user_id or str(user_id).strip() == "" or str(user_id).lower() == "nan":
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
    except Exception:
        return 500


def upload_imgbb(file_bytes: bytes) -> Optional[str]:
    if "imgbb" not in st.secrets or "api_key" not in st.secrets["imgbb"]:
        return None
    try:
        res = requests.post(
            "https://api.imgbb.com/1/upload",
            params={"key": st.secrets["imgbb"]["api_key"]},
            files={"image": file_bytes},
            timeout=30,
        )
        data = res.json()
        return data["data"]["url"]
    except Exception:
        return None


# =========================
# Admin auth
# =========================
def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))


def admin_login_ui() -> None:
    pw = safe_str(st.secrets.get("admin", {}).get("pin", ""))  # 以前の仕様を踏襲（pinキーでもパス扱い）
    if not pw:
        st.warning("Secrets の [admin].pin が未設定です。管理機能を保護できません。")
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
        pin = st.text_input("管理者パスワード", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if pin == pw:
                st.session_state["admin_ok"] = True
                st.success("ログイン成功")
            else:
                st.session_state["admin_ok"] = False
                st.error("パスワードが違います")


# =========================
# Sheets config
# =========================
@dataclass
class GSheetsConfig:
    spreadsheet_id: str
    settings_sheet: str = "Settings"
    members_sheet: str = "Members"
    ledger_sheet: str = "Ledger"


DEFAULT_SETTINGS_HEADERS = ["Project_Name", "TotalPrincipal", "Currency", "IsCompound", "ReceiveFactor"]
DEFAULT_MEMBERS_HEADERS = [
    "PersonName", "Project_Name", "Line_User_ID", "LINE_DisplayName", "IsActive", "CreatedAt_JST", "UpdatedAt_JST"
]
DEFAULT_LEDGER_HEADERS = [
    "Datetime_JST", "Project_Name", "PersonName", "Type", "Amount", "Currency", "Note",
    "Line_User_ID", "LINE_DisplayName", "Source", "Evidence_URL"
]


# =========================
# Cached low-read wrappers (429対策)
# =========================
@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    con = st.secrets.get("connections", {}).get("gsheets", {})
    creds_info = con.get("credentials")
    if not creds_info:
        raise RuntimeError("Secrets に [connections.gsheets.credentials] がありません。")

    must = ["client_email", "private_key", "token_uri", "project_id", "type"]
    miss = [k for k in must if k not in creds_info or not str(creds_info.get(k, "")).strip()]
    if miss:
        raise RuntimeError(f"Service Account 情報に不足: {miss}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(dict(creds_info), scopes=scopes)
    return gspread.authorize(creds)


@st.cache_resource(show_spinner=False)
def open_book(spreadsheet_id: str):
    gc = get_gspread_client()
    return gc.open_by_key(spreadsheet_id)


@st.cache_data(ttl=10, show_spinner=False)
def cached_get_header_row(spreadsheet_id: str, sheet_name: str) -> List[str]:
    book = open_book(spreadsheet_id)
    ws = book.worksheet(sheet_name)
    # 1行目だけ読む（get_all_values禁止）
    row = ws.row_values(1)
    return [str(x).strip() for x in row]


@st.cache_data(ttl=10, show_spinner=False)
def cached_read_df(spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    book = open_book(spreadsheet_id)
    ws = book.worksheet(sheet_name)
    values = ws.get_all_values()  # ここは必要時だけ、かつttlで抑える
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)
    return clean_cols(df).fillna("")


def clear_cache_for_sheet():
    # データキャッシュだけクリア（書き込み直後に使う）
    cached_get_header_row.clear()
    cached_read_df.clear()


# =========================
# GSheets
# =========================
class GSheets:
    def __init__(self, cfg: GSheetsConfig):
        con = st.secrets.get("connections", {}).get("gsheets", {})
        spreadsheet = extract_sheet_id(safe_str(con.get("spreadsheet", "")))
        if not spreadsheet:
            st.error("Secrets の [connections.gsheets].spreadsheet が未設定です。")
            st.stop()

        self.cfg = cfg
        self.cfg.spreadsheet_id = spreadsheet

        try:
            self.book = open_book(self.cfg.spreadsheet_id)
        except Exception as e:
            st.error(f"Spreadsheet を開けません: {e}")
            st.stop()

        # シート存在とヘッダーは最小readで保証
        self._ensure_sheet(self.cfg.settings_sheet, DEFAULT_SETTINGS_HEADERS)
        self._ensure_sheet(self.cfg.members_sheet, DEFAULT_MEMBERS_HEADERS)
        self._ensure_sheet(self.cfg.ledger_sheet, DEFAULT_LEDGER_HEADERS)

    def _ensure_sheet(self, name: str, headers: List[str]) -> None:
        try:
            ws = self.book.worksheet(name)
        except Exception:
            ws = self.book.add_worksheet(title=name, rows=1000, cols=max(20, len(headers) + 5))
            ws.append_row(headers, value_input_option="USER_ENTERED")
            clear_cache_for_sheet()
            return

        # 1行目だけ読む
        row1 = ws.row_values(1)
        if not row1:
            ws.append_row(headers, value_input_option="USER_ENTERED")
            clear_cache_for_sheet()
            return

        current = [str(x).strip() for x in row1]
        missing = [h for h in headers if h not in current]
        if missing:
            ws.update("1:1", [current + missing])
            clear_cache_for_sheet()

    def headers(self, sheet_name: str) -> List[str]:
        return cached_get_header_row(self.cfg.spreadsheet_id, sheet_name)

    def read_df(self, sheet_name: str) -> pd.DataFrame:
        return cached_read_df(self.cfg.spreadsheet_id, sheet_name)

    def ws(self, name: str):
        return self.book.worksheet(name)

    def append_row_by_headers(self, sheet_name: str, row: Dict[str, Any]) -> None:
        ws = self.ws(sheet_name)
        headers = self.headers(sheet_name)
        out = [safe_str(row.get(h, "")) for h in headers]
        ws.append_row(out, value_input_option="USER_ENTERED")
        clear_cache_for_sheet()

    # ---- Settings ----
    def get_settings(self) -> pd.DataFrame:
        df = self.read_df(self.cfg.settings_sheet)
        for h in DEFAULT_SETTINGS_HEADERS:
            if h not in df.columns:
                df[h] = ""
        return df

    def update_project_total(self, project_name: str, new_total: float) -> None:
        ws = self.ws(self.cfg.settings_sheet)
        df = self.get_settings().fillna("")
        idxs = df.index[df["Project_Name"].astype(str) == str(project_name)].tolist()
        if not idxs:
            return
        idx = idxs[0]
        sheet_row = idx + 2
        headers = self.headers(self.cfg.settings_sheet)
        if "TotalPrincipal" not in headers:
            return
        col = headers.index("TotalPrincipal") + 1
        ws.update_cell(sheet_row, col, safe_str(round(new_total, 6)))
        clear_cache_for_sheet()

    def project_base_total(self, project_name: str) -> float:
        df = self.get_settings().fillna("")
        m = df[df["Project_Name"].astype(str) == str(project_name)]
        if m.empty:
            return 0.0
        return to_f(m.iloc[0].get("TotalPrincipal", 0))

    def project_meta(self, project_name: str) -> Dict[str, Any]:
        df = self.get_settings().fillna("")
        m = df[df["Project_Name"].astype(str) == str(project_name)]
        if m.empty:
            return {"Currency": "JPY", "IsCompound": "TRUE", "ReceiveFactor": "0.67"}
        return m.iloc[0].to_dict()

    # ---- Members ----
    def get_members(self) -> pd.DataFrame:
        df = self.read_df(self.cfg.members_sheet)
        for h in DEFAULT_MEMBERS_HEADERS:
            if h not in df.columns:
                df[h] = ""
        return df.fillna("")

    def members_for_project(self, project_name: str) -> pd.DataFrame:
        df = self.get_members().fillna("")
        df = df[df["Project_Name"].astype(str) == str(project_name)]

        def active_flag(x: Any) -> bool:
            if str(x).strip() == "":
                return True
            return is_truthy(x)

        df = df[df["IsActive"].apply(active_flag)]
        return df

    # ---- Ledger ----
    def get_ledger(self) -> pd.DataFrame:
        df = self.read_df(self.cfg.ledger_sheet)
        for h in DEFAULT_LEDGER_HEADERS:
            if h not in df.columns:
                df[h] = ""
        return df.fillna("")

    def project_current_total(self, project_name: str) -> float:
        base = self.project_base_total(project_name)
        led = self.get_ledger().fillna("")
        led = led[led["Project_Name"].astype(str) == str(project_name)]
        total = base
        for _, r in led.iterrows():
            typ = str(r.get("Type", "")).strip()
            amt = to_f(r.get("Amount", 0))
            if typ == "Deposit":
                total += amt
            elif typ == "Withdraw":
                total -= amt
            elif typ == "APR":
                total += amt
        return total


# =========================
# APR calc
# =========================
def calc_apr_daily_amount(total_principal: float, apr_percent: float, receive_factor: float) -> float:
    effective_apr = apr_percent * receive_factor
    daily_rate = (effective_apr / 100.0) / 365.0
    return total_principal * daily_rate


# =========================
# UI
# =========================
def ui_debug(gs: GSheets):
    st.sidebar.markdown("## 🔎 Debug")
    con = st.secrets.get("connections", {}).get("gsheets", {})
    st.sidebar.write("spreadsheet:", extract_sheet_id(safe_str(con.get("spreadsheet", ""))))
    creds = con.get("credentials", {})
    st.sidebar.write("client_email:", safe_str(creds.get("client_email", "")))
    st.sidebar.write("token_uri:", safe_str(creds.get("token_uri", "")))
    st.sidebar.write("Sheets:", gs.cfg.settings_sheet, gs.cfg.members_sheet, gs.cfg.ledger_sheet)
    if st.sidebar.button("キャッシュクリア"):
        clear_cache_for_sheet()
        st.rerun()


def ui_apr(gs: GSheets):
    st.subheader("📈 APR（本日の収益）→ 台帳記録 → 全員へ一斉LINE（受取率 0.67）")

    settings_df = gs.get_settings()
    if settings_df.empty:
        st.error("Settings が空です。")
        return

    projects = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    project = st.selectbox("プロジェクト", options=projects)

    meta = gs.project_meta(project)
    currency = str(meta.get("Currency", "JPY")).strip() or "JPY"

    receive_factor = to_f(meta.get("ReceiveFactor", 0.67))
    if receive_factor <= 0:
        receive_factor = 0.67

    current_total = gs.project_current_total(project)
    st.info(f"現在総額（Ledger反映）: {current_total:,.2f} {currency} / 受取率: {receive_factor:.2f}")

    members = gs.members_for_project(project)
    if members.empty:
        st.warning("Members にこのプロジェクトのメンバーがいません（Project_Name一致を確認）。")
        return

    token = safe_str(st.secrets.get("line", {}).get("channel_access_token", ""))
    uids = [str(x).strip() for x in members["Line_User_ID"].tolist() if str(x).strip().startswith("U")]
    uids = uniq_keep_order(uids)

    st.caption(f"人数: {len(members)} / 送信先ID: {len(uids)}")

    apr_percent = st.number_input("本日のAPR（年利, %）", value=20.0, step=0.1)
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])
    if uploaded:
        st.image(uploaded, width=360)

    total_daily = calc_apr_daily_amount(current_total, apr_percent, receive_factor)
    per_person = total_daily / max(1, len(members))
    st.metric("本日の合計収益", f"{total_daily:,.4f} {currency}")
    st.metric("1人あたり（均等）", f"{per_person:,.4f} {currency}")

    if st.button("✅ APRを台帳に記録して全員へ送信", use_container_width=True):
        evidence_url = ""
        if uploaded:
            with st.spinner("画像URL化（ImgBB）..."):
                u = upload_imgbb(uploaded.getvalue())
            if not u:
                st.error("ImgBB失敗。画像なしで再実行してください。")
                return
            evidence_url = u

        dt = jst_str()

        # 個人ごとにAPR行（均等）を台帳へ
        for _, m in members.iterrows():
            gs.append_row_by_headers(
                gs.cfg.ledger_sheet,
                {
                    "Datetime_JST": dt,
                    "Project_Name": project,
                    "PersonName": str(m.get("PersonName", "")).strip(),
                    "Type": "APR",
                    "Amount": round(per_person, 6),
                    "Currency": currency,
                    "Note": f"APR:{apr_percent}%, Receive:{receive_factor}",
                    "Line_User_ID": str(m.get("Line_User_ID", "")).strip(),
                    "LINE_DisplayName": str(m.get("LINE_DisplayName", "")).strip(),
                    "Source": "app",
                    "Evidence_URL": evidence_url,
                },
            )

        # TotalPrincipalも更新（APR分を総額に加算）
        new_total = current_total + total_daily
        gs.update_project_total(project, new_total)

        # 一斉LINE（個人名なし）
        if token and uids:
            msg = (
                "🏦【本日の運用収益報告】\n"
                f"プロジェクト: {project}\n"
                f"日時(JST): {dt}\n"
                f"APR(年利): {apr_percent}%\n"
                f"受取率: {receive_factor:.2f}\n"
                f"本日の合計収益: {total_daily:,.4f} {currency}\n"
                f"1人あたり(均等): {per_person:,.4f} {currency}\n"
            )
            if evidence_url:
                msg += "📎 エビデンス画像を添付します。"

            ok, ng = 0, 0
            for uid in uids:
                code = send_line_push(token, uid, msg, image_url=(evidence_url or None))
                if code == 200:
                    ok += 1
                else:
                    ng += 1
            st.success(f"送信：成功 {ok} / 失敗 {ng}")

        st.success(f"APR記録完了。更新後総額: {new_total:,.2f} {currency}")
        st.rerun()


def ui_cash(gs: GSheets):
    st.subheader("💸 入金 / 出金 → 台帳記録 → 個人へLINE通知")

    settings_df = gs.get_settings()
    if settings_df.empty:
        st.error("Settings が空です。")
        return

    projects = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    project = st.selectbox("プロジェクト", options=projects, key="cash_project")

    meta = gs.project_meta(project)
    currency_default = str(meta.get("Currency", "JPY")).strip() or "JPY"

    members = gs.members_for_project(project)
    if members.empty:
        st.warning("Members にこのプロジェクトのメンバーがいません（Project_Name一致を確認）。")
        return

    labels = members["PersonName"].astype(str).tolist()
    person = st.selectbox("メンバー（PersonName）", options=labels)

    mrow = members[members["PersonName"].astype(str) == str(person)].iloc[0]
    uid = str(mrow.get("Line_User_ID", "")).strip()
    disp = str(mrow.get("LINE_DisplayName", "")).strip()

    current_total = gs.project_current_total(project)
    st.info(f"現在総額（Ledger反映）: {current_total:,.2f} {currency_default}")

    dt = now_jst()
    with st.form("cash_form", clear_on_submit=False):
        d = st.date_input("日付（JST）", value=dt.date())
        t = st.time_input("時刻（JST）", value=dt.time().replace(second=0, microsecond=0))
        dt_fixed = datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, tzinfo=JST)

        typ = st.selectbox("種別", options=["Deposit", "Withdraw"])
        amount = st.number_input("金額", min_value=0.0, value=0.0, step=1000.0)
        currency = st.text_input("通貨", value=currency_default)
        note = st.text_input("メモ", value="")
        submit = st.form_submit_button("✅ 台帳に記録してLINE通知")

    if submit:
        if amount <= 0:
            st.error("金額が0です。")
            return

        dt_s = dt_fixed.strftime("%Y-%m-%d %H:%M:%S")

        gs.append_row_by_headers(
            gs.cfg.ledger_sheet,
            {
                "Datetime_JST": dt_s,
                "Project_Name": project,
                "PersonName": person,
                "Type": typ,
                "Amount": round(float(amount), 6),
                "Currency": currency.strip() or currency_default,
                "Note": note,
                "Line_User_ID": uid,
                "LINE_DisplayName": disp,
                "Source": "app",
                "Evidence_URL": "",
            },
        )

        # TotalPrincipal更新（入金/出金分を総額に反映）
        new_total = current_total + float(amount) if typ == "Deposit" else current_total - float(amount)
        gs.update_project_total(project, new_total)

        token = safe_str(st.secrets.get("line", {}).get("channel_access_token", ""))
        if token and uid.startswith("U"):
            msg = (
                f"🏦【{project} 入出金通知】\n"
                f"日時(JST): {dt_s}\n"
                f"種別: {'入金' if typ == 'Deposit' else '出金'}\n"
                f"金額: {float(amount):,.2f} {currency}\n"
                f"メモ: {note}\n"
                f"更新後の総額: {new_total:,.2f} {currency_default}\n"
            )
            code = send_line_push(token, uid, msg)
            if code == 200:
                st.success("記録＆LINE通知しました。")
            else:
                st.warning(f"記録は完了しましたが、LINE送信に失敗しました（HTTP {code}）。")
        else:
            st.success("記録しました（LINE送信は未設定/IDなし）。")

        st.rerun()


def ui_ledger(gs: GSheets):
    st.subheader("📒 Ledger（台帳）")
    df = gs.get_ledger()
    if df.empty:
        st.info("Ledger が空です。")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)


def main():
    st.set_page_config(page_title="APR資産運用管理システム", layout="wide", page_icon="🏦")
    st.title("🏦 APR資産運用管理システム")

    gs = GSheets(GSheetsConfig(spreadsheet_id=""))

    ui_debug(gs)

    tab1, tab2, tab3, tab4 = st.tabs(
        ["📈 APR報告（一斉）", "💸 入金/出金（個別）", "📒 台帳", "⚙️ 管理"]
    )

    with tab1:
        ui_apr(gs)
    with tab2:
        ui_cash(gs)
    with tab3:
        ui_ledger(gs)
    with tab4:
        st.subheader("⚙️ 管理（管理者のみ）")
        admin_login_ui()
        if not is_admin():
            st.info("管理者ログイン後に表示します。")
            st.stop()

        st.success("管理者モード")
        st.write("### ヘッダー（コピペ用）")
        st.write("Settings"); st.code("\t".join(DEFAULT_SETTINGS_HEADERS))
        st.write("Members"); st.code("\t".join(DEFAULT_MEMBERS_HEADERS))
        st.write("Ledger"); st.code("\t".join(DEFAULT_LEDGER_HEADERS))
        st.caption("※ 1行目に貼り付け（タブ区切り）")


if __name__ == "__main__":
    main()
