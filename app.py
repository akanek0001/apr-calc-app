# app.py
# APR資産運用管理システム（Settings / Members / Ledger）+ APR計算 + LINE個別送信
# Secretsは connections.gsheets 方式に対応
#
# 必須:
# - [connections.gsheets].spreadsheet （スプレッドシートID or URL）
# - [connections.gsheets.credentials] （サービスアカウント）
# - [admin].pin （管理画面保護）
#
# 任意:
# - [app].line_channel_access_token （LINE Push用）

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
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
    # URLならID抽出
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

    # シート名を変えたい場合はここ（固定で良いなら触らない）
    # 例: [gsheets] settings_sheet="Settings" ... みたいな上書きをしたい場合も可能だが、
    # 今回は混乱を避けるため固定にしている。

    return GSheetsConfig(spreadsheet_id=sid)


def load_line_token() -> str:
    return safe_str(st.secrets.get("app", {}).get("line_channel_access_token", "")).strip()


# ------------------------
# Google Sheets Client
# ------------------------
class GSheets:
    def __init__(self, cfg: GSheetsConfig):
        self.cfg = cfg

        # Credentials
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

        # Ensure base sheets
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

            # 1行目ヘッダーの重複を避けつつ、必要列が足りなければ追加
            current = values[0]
            # 空ヘッダーを削除
            current = [h.strip() for h in current if str(h).strip() != ""]
            if not current:
                ws.update("1:1", [headers])
                return

            # 既存ヘッダーに必要ヘッダーが無ければ末尾に追加
            missing = [h for h in headers if h not in current]
            if missing:
                new_headers = current + missing
                ws.update("1:1", [new_headers])

        except APIError as e:
            # 429 (quota) など
            st.error(f"シート初期化に失敗: {title} / {e}")
            st.stop()

    def ws(self, name: str):
        return self.book.worksheet(name)

    @st.cache_data(ttl=10, show_spinner=False)
    def read_df_cached(_cache_key: str, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
        # キャッシュ用（インスタンス参照を避ける）
        # 実体の取得は呼び出し側で行う
        return pd.DataFrame()

    def read_df(self, sheet_name: str) -> pd.DataFrame:
        try:
            ws = self.ws(sheet_name)
            values = ws.get_all_values()
        except APIError as e:
            st.error(f"読み取りエラー: {sheet_name} / {e}")
            st.stop()

        if not values:
            return pd.DataFrame()

        headers = values[0]
        rows = values[1:]
        # ヘッダー重複対策（pandasが死ぬ）
        seen = {}
        fixed = []
        for h in headers:
            h = str(h).strip()
            if h == "":
                h = "Unnamed"
            if h in seen:
                seen[h] += 1
                fixed.append(f"{h}__{seen[h]}")
            else:
                seen[h] = 0
                fixed.append(h)

        df = pd.DataFrame(rows, columns=fixed).fillna("")
        return df

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
            ws.update(f"A{target_row}:{chr(ord('A')+len(headers)-1)}{target_row}", [out])

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
            # update minimal
            def update_cell(key: str, value: str):
                c = col[key] + 1
                ws.update_cell(target_row, c, value)

            update_cell("Line_User_ID", line_user_id)
            update_cell("LINE_DisplayName", display_name)
            update_cell("IsActive", "TRUE" if is_active else "FALSE")
            update_cell("UpdatedAt_JST", ts)

    def members_for_project(self, project_name: str) -> pd.DataFrame:
        df = self.read_df(self.cfg.members_sheet)
        if df.empty:
            return df
        # 列名が壊れてるときは落ちないように
        if "Project_Name" not in df.columns:
            return pd.DataFrame()
        df = df[df["Project_Name"].astype(str) == str(project_name)]
        if "IsActive" in df.columns:
            df = df[df["IsActive"].astype(str).str.lower().isin(["true", "1", "yes", "y", "on"])]
        return df

    def get_project_list(self) -> List[str]:
        df = self.read_df(self.cfg.settings_sheet)
        if df.empty or "Project_Name" not in df.columns:
            return []
        return sorted(list({str(x).strip() for x in df["Project_Name"].tolist() if str(x).strip()}))

    def get_project_setting(self, project_name: str) -> Tuple[float, str]:
        # デフォルト 0.67 / JPY
        df = self.read_df(self.cfg.settings_sheet)
        apr = 0.67
        cur = "JPY"
        if df.empty or "Project_Name" not in df.columns:
            return apr, cur
        m = df[df["Project_Name"].astype(str) == str(project_name)]
        if m.empty:
            return apr, cur
        r = m.iloc[0].to_dict()
        try:
            apr = float(str(r.get("APR_Rate", "0.67")).strip() or "0.67")
        except Exception:
            apr = 0.67
        cur = str(r.get("Currency", "JPY")).strip() or "JPY"
        return apr, cur

    def calc_balance_for_project(self, project_name: str) -> float:
        df = self.read_df(self.cfg.ledger_sheet)
        if df.empty:
            return 0.0
        if "Project_Name" not in df.columns or "Type" not in df.columns or "Amount" not in df.columns:
            return 0.0

        dfp = df[df["Project_Name"].astype(str) == str(project_name)].copy()
        if dfp.empty:
            return 0.0

        def fnum(x: Any) -> float:
            try:
                return float(str(x).replace(",", "").strip())
            except Exception:
                return 0.0

        dfp["AmountNum"] = dfp["Amount"].apply(fnum)
        dep = dfp[dfp["Type"].astype(str) == "Deposit"]["AmountNum"].sum()
        wdr = dfp[dfp["Type"].astype(str) == "Withdraw"]["AmountNum"].sum()
        # APR行は残高に影響させる/させないは運用次第だが、ここでは「影響させない」(収益記録のみ)にする
        return float(dep - wdr)


# ------------------------
# LINE Push
# ------------------------
def line_push_message(channel_access_token: str, to_user_id: str, text: str) -> bool:
    if not channel_access_token:
        return False
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {channel_access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "to": to_user_id,
        "messages": [{"type": "text", "text": text}],
    }
    r = requests.post(url, headers=headers, json=payload, timeout=15)
    return 200 <= r.status_code < 300


# ------------------------
# UI
# ------------------------
def ui_debug(cfg: GSheetsConfig):
    with st.sidebar:
        st.write("🔎 Debug")
        st.caption(f"spreadsheet: {cfg.spreadsheet_id}")
        con = st.secrets.get("connections", {}).get("gsheets", {})
        creds = con.get("credentials", {})
        st.caption(f"client_email: {safe_str(creds.get('client_email',''))}")
        st.caption(f"token_uri: {safe_str(creds.get('token_uri',''))}")
        st.caption("Sheets: Settings Members Ledger")
        if st.button("キャッシュクリア"):
            st.cache_data.clear()
            st.toast("キャッシュをクリアしました")


def ui_settings(gs: GSheets):
    st.subheader("⚙️ Settings（プロジェクト設定）")
    df = gs.read_df(gs.cfg.settings_sheet)
    if df.empty:
        st.info("Settings が空です。まずプロジェクトを作成してください。")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.write("### プロジェクト追加/更新")
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
            st.success("保存しました")
            st.rerun()


def ui_members(gs: GSheets):
    st.subheader("👤 Members（個人名 ↔ LINE）")
    projects = gs.get_project_list()
    project = st.selectbox("Project", options=[""] + projects)
    if not project:
        st.info("Project を選んでください")
        return

    df = gs.members_for_project(project)
    if df.empty:
        st.warning("このプロジェクトのメンバーがいません")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.write("### メンバー追加/更新（Project + PersonName でUpsert）")
    with st.form("member_upsert", clear_on_submit=False):
        person = st.text_input("PersonName", placeholder="例: 田中太郎")
        line_uid = st.text_input("Line_User_ID", placeholder="例: Uxxxxxxxxxxxxxxxx")
        disp = st.text_input("LINE_DisplayName", placeholder="例: taro")
        active = st.checkbox("IsActive", value=True)
        ok = st.form_submit_button("保存")
    if ok:
        if not person.strip():
            st.error("PersonName は必須です")
        else:
            gs.upsert_member(project, person.strip(), line_uid.strip(), disp.strip(), bool(active))
            st.success("保存しました")
            st.rerun()


def ui_ledger(gs: GSheets):
    st.subheader("📒 入金/出金 台帳（Ledger）")

    projects = gs.get_project_list()
    project = st.selectbox("Project", options=[""] + projects, key="ledger_project")
    if not project:
        st.info("Project を選んでください")
        return

    tab1, tab2 = st.tabs(["記録する", "見る"])
    with tab1:
        members_df = gs.members_for_project(project)
        people = []
        if not members_df.empty and "PersonName" in members_df.columns:
            people = sorted([str(x).strip() for x in members_df["PersonName"].tolist() if str(x).strip()])

        with st.form("ledger_add", clear_on_submit=False):
            d = st.date_input("日付(JST)", value=now_jst().date())
            t = st.time_input("時刻(JST)", value=now_jst().time().replace(second=0, microsecond=0))
            dt = datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, tzinfo=JST)

            person = st.selectbox("PersonName", options=[""] + people)
            typ = st.selectbox("Type", options=["Deposit", "Withdraw", "Other"])
            amount = st.number_input("Amount", min_value=0.0, value=0.0, step=1000.0)
            currency = st.text_input("Currency", value=gs.get_project_setting(project)[1])
            note = st.text_input("Note", value="")
            source = st.selectbox("Source", options=["app", "make", "line", "other"])

            ok = st.form_submit_button("追加")
        if ok:
            if not person:
                st.error("PersonName を選んでください")
            else:
                # member情報からLINE ID/表示名を引く（無ければ空）
                line_uid = ""
                disp = ""
                if not members_df.empty:
                    m = members_df[members_df["PersonName"].astype(str) == str(person)]
                    if not m.empty:
                        r = m.iloc[0].to_dict()
                        line_uid = safe_str(r.get("Line_User_ID", ""))
                        disp = safe_str(r.get("LINE_DisplayName", ""))

                row = [
                    dt.strftime("%Y-%m-%d %H:%M:%S"),
                    project,
                    person,
                    typ,
                    amount,
                    currency,
                    note,
                    line_uid,
                    disp,
                    source,
                ]
                gs.append_row(gs.cfg.ledger_sheet, row)
                st.success("追加しました")
                st.rerun()

    with tab2:
        df = gs.read_df(gs.cfg.ledger_sheet)
        if df.empty:
            st.info("Ledger が空です")
            return
        if "Project_Name" in df.columns:
            df = df[df["Project_Name"].astype(str) == str(project)]
        st.dataframe(df, use_container_width=True, hide_index=True)


def ui_apr(gs: GSheets):
    st.subheader("📈 APR報告（一斉）")

    projects = gs.get_project_list()
    project = st.selectbox("Project", options=[""] + projects, key="apr_project")
    if not project:
        st.info("Project を選んでください")
        return

    apr_rate, currency = gs.get_project_setting(project)
    balance = gs.calc_balance_for_project(project)

    st.write(f"現在総額（Ledger反映）: **{balance:,.2f} {currency}** / 年利: **{apr_rate:.2f}**")

    # 日利換算（365）
    daily_profit_total = balance * float(apr_rate) / 365.0

    st.write(f"本日の収益（概算）: **{daily_profit_total:,.2f} {currency}**")

    members = gs.members_for_project(project)
    if members.empty or "PersonName" not in members.columns:
        st.warning("Members にこのプロジェクトのメンバーがいません（Project_Name一致を確認）。")
        return

    # メンバーへ均等割り（運用ルール次第：必要なら按分に変更）
    n = len(members)
    per = daily_profit_total / n if n > 0 else 0.0

    st.write(f"人数: {n} / 1人あたり: **{per:,.2f} {currency}**")

    # 実行
    line_token = load_line_token()
    send_line = st.checkbox("LINEに送る（tokenがある場合のみ）", value=bool(line_token))
    record_ledger = st.checkbox("LedgerにAPRとして記録する", value=True)
    note = st.text_input("メモ（Note）", value=f"APR daily ({apr_rate:.2f}/year)")

    if st.button("APRを実行（記録/送信）", type="primary"):
        dt = now_jst()
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
                    uid,
                    disp,
                    "app",
                ]
                gs.append_row(gs.cfg.ledger_sheet, row)

            # LINE送信
            if send_line and line_token and uid:
                msg = (
                    f"【APR報告】{project}\n"
                    f"{person} 様\n"
                    f"本日の収益: {per:,.2f} {currency}\n"
                    f"年利: {apr_rate:.2f}\n"
                    f"日時(JST): {jst_str(dt)}\n"
                )
                try:
                    if line_push_message(line_token, uid, msg):
                        ok_count += 1
                    else:
                        fail_count += 1
                except Exception:
                    fail_count += 1

        st.success(f"完了：LINE成功 {ok_count} / 失敗 {fail_count}（Ledger記録={record_ledger}）")
        st.rerun()


def ui_admin(gs: GSheets):
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
    ui_settings(gs)
    st.divider()
    ui_members(gs)


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

    ui_debug(cfg)

    # ここで spreadsheet_id を空で渡すのは禁止
    gs = GSheets(cfg)

    tab_apr, tab_ledger, tab_members, tab_admin = st.tabs(
        ["📈 APR報告（一斉）", "📒 入金/出金（個別）", "👤 Members（閲覧）", "⚙ 管理"]
    )

    with tab_apr:
        ui_apr(gs)

    with tab_ledger:
        ui_ledger(gs)

    with tab_members:
        # 閲覧のみ
        df = gs.read_df(gs.cfg.members_sheet)
        if df.empty:
            st.info("Members が空です")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)

    with tab_admin:
        ui_admin(gs)


if __name__ == "__main__":
    main()
