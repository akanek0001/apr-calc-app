# app.py
# Streamlit APR / 入出金管理 + 管理（⚙️）画面 + LINE個別通知（Push）
#
# ✅ Secretsは「connections.gsheets」方式（あなたの今の設定）に対応
# ✅ 取引（Deposit/Withdraw/APR/Other）を台帳へ記録
# ✅ 記録成功後に、該当メンバー（Line_User_ID）へLINE通知（Push）
# ✅ 管理者ログイン（[admin] pin）を復旧
#
# --------------------------------------------
# REQUIRED (Streamlit Cloud > Settings > Secrets)
# --------------------------------------------
# [admin]
# pin = "sugikiyo"   # ※PINでも「パス」でもOK（この値と一致すれば管理画面が開く）
#
# [connections.gsheets]
# spreadsheet = "1z6XuFavFlUMYcsXmASlTgqvDcvNKuhCoSZePb-PHyn0"  # IDまたはURL
#
# [connections.gsheets.credentials]
# type = "service_account"
# project_id = "xxxxx"
# private_key_id = "xxxxx"
# private_key = "-----BEGIN PRIVATE KEY-----\n....\n-----END PRIVATE KEY-----\n"
# client_email = "xxxxx@xxxxx.iam.gserviceaccount.com"
# client_id = "xxxxx"
# token_uri = "https://oauth2.googleapis.com/token"
#
# [gsheets]  # ←任意（シート名などだけ上書きしたいとき）
# members_sheet_name = "Members"
# ledger_sheet_name = "Ledger"
# use_personal_sheets = false
# personal_map_sheet_name = "PersonalMap"
#
# [line]  # ←LINE通知を使うなら必須
# channel_access_token = "（Messaging APIのChannel access token）"
#
# ※ service account を対象スプレッドシートに「編集者」で共有してください。
#
# --------------------------------------------
# requirements.txt（例）
# --------------------------------------------
# streamlit
# pandas
# gspread
# google-auth
# requests

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials


# =========================
# Time / Utilities
# =========================

JST = timezone(timedelta(hours=9), "JST")


def now_jst() -> datetime:
    return datetime.now(JST)


def to_jst_from_epoch_ms(epoch_ms: int) -> datetime:
    dt_utc = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return dt_utc.astimezone(JST)


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def is_truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


# =========================
# LINE Push (individual)
# =========================

def get_line_token() -> str:
    return safe_str(st.secrets.get("line", {}).get("channel_access_token", "")).strip()


def line_push_text(to_user_id: str, text: str) -> bool:
    """
    LINEのPushメッセージ送信（個人宛）
    成功: True / 失敗: False
    """
    token = get_line_token()
    if not token:
        st.warning("Secrets に line.channel_access_token が未設定のため、LINE通知をスキップしました。")
        return False
    if not to_user_id:
        st.warning("Line_User_ID が空のため、LINE通知をスキップしました。")
        return False

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "to": to_user_id,
        "messages": [{"type": "text", "text": text}],
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=15)
        if 200 <= r.status_code < 300:
            return True
        st.error(f"LINE送信失敗: status={r.status_code} body={r.text[:200]}")
        return False
    except Exception as e:
        st.error("LINE送信で例外が発生しました。")
        st.exception(e)
        return False


def format_notify_message(dt_jst_str: str, person: str, typ: str, amount: float, currency: str, note: str) -> str:
    note_part = f"\nメモ: {note}" if note else ""
    return (
        "【APRシステム通知】\n"
        f"日時: {dt_jst_str} (JST)\n"
        f"対象: {person}\n"
        f"種別: {typ}\n"
        f"金額: {amount} {currency}"
        f"{note_part}"
    )


# =========================
# Admin Auth (PIN/Pass)
# =========================

def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))


def admin_login_ui() -> None:
    """
    管理者PIN入力フォームを描画して、成功時に session_state.admin_ok を True にする。
    st.stop() は呼ばない（呼ぶとフォームが消える原因になる）。
    """
    pin_required = safe_str(st.secrets.get("admin", {}).get("pin", ""))
    if not pin_required:
        st.warning("Secrets に admin.pin が未設定です。管理画面を保護できません。")
        st.session_state["admin_ok"] = False
        return

    # すでにログイン済み
    if is_admin():
        col1, col2 = st.columns([1, 1])
        with col1:
            st.success("管理者ログイン中")
        with col2:
            if st.button("ログアウト", use_container_width=True):
                st.session_state["admin_ok"] = False
                st.toast("ログアウトしました")
        st.divider()
        return

    with st.form("admin_login_form", clear_on_submit=False):
        pin = st.text_input("管理者PIN/パス", type="password", help="Secrets の admin.pin と一致すると管理画面が開きます。")
        ok = st.form_submit_button("管理者ログイン")
        if ok:
            if pin == pin_required:
                st.session_state["admin_ok"] = True
                st.success("管理者ログインに成功しました。")
            else:
                st.session_state["admin_ok"] = False
                st.error("PIN/パスが違います。")


# =========================
# Google Sheets Config (connections.gsheets方式)
# =========================

@dataclass
class GSheetsConfig:
    registry_spreadsheet_id: str
    members_sheet_name: str
    ledger_sheet_name: str
    use_personal_sheets: bool = False
    personal_map_sheet_name: str = "PersonalMap"


def _extract_sheet_id(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if "/spreadsheets/d/" in s:
        # URL -> ID
        try:
            return s.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception:
            return s
    # すでにID
    return s


def load_gsheets_cfg() -> Optional[GSheetsConfig]:
    # connections.gsheets.spreadsheet を読む（あなたの現在の方式）
    con = st.secrets.get("connections", {}).get("gsheets", {})
    sid = _extract_sheet_id(safe_str(con.get("spreadsheet", "")))
    if not sid:
        return None

    # 任意：シート名だけは [gsheets] で上書きできるようにする
    g = st.secrets.get("gsheets", {})
    members = safe_str(g.get("members_sheet_name", "Members")).strip() or "Members"
    ledger = safe_str(g.get("ledger_sheet_name", "Ledger")).strip() or "Ledger"
    use_personal = is_truthy(g.get("use_personal_sheets", False))
    pmap = safe_str(g.get("personal_map_sheet_name", "PersonalMap")).strip() or "PersonalMap"

    return GSheetsConfig(
        registry_spreadsheet_id=sid,
        members_sheet_name=members,
        ledger_sheet_name=ledger,
        use_personal_sheets=use_personal,
        personal_map_sheet_name=pmap,
    )


# =========================
# Google Sheets Client
# =========================

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
        self.registry = self.gc.open_by_key(self.cfg.registry_spreadsheet_id)

    def ws(self, name: str):
        return self.registry.worksheet(name)

    # ---- read helpers ----
    def read_df(self, sheet_name: str, header_row: int = 1) -> pd.DataFrame:
        ws = self.ws(sheet_name)
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        header_idx = header_row - 1
        if len(values) <= header_idx:
            return pd.DataFrame()
        headers = values[header_idx]
        rows = values[header_idx + 1 :]
        df = pd.DataFrame(rows, columns=headers)
        return df

    def append_row(self, sheet_name: str, row: List[Any]) -> None:
        ws = self.ws(sheet_name)
        ws.append_row([safe_str(x) for x in row], value_input_option="USER_ENTERED")

    def upsert_member(self, person_name: str, line_user_id: str, line_display_name: str) -> None:
        """
        Members シート:
        - PersonName
        - Line_User_ID
        - LINE_DisplayName
        - CreatedAt_JST
        - UpdatedAt_JST
        """
        ws = self.ws(self.cfg.members_sheet_name)
        values = ws.get_all_values()
        if not values:
            header = ["PersonName", "Line_User_ID", "LINE_DisplayName", "CreatedAt_JST", "UpdatedAt_JST"]
            ws.append_row(header, value_input_option="USER_ENTERED")
            values = [header]

        headers = values[0]
        col = {h: i for i, h in enumerate(headers)}

        required = ["PersonName", "Line_User_ID", "LINE_DisplayName", "CreatedAt_JST", "UpdatedAt_JST"]
        missing = [h for h in required if h not in col]
        if missing:
            new_headers = headers + missing
            ws.update("1:1", [new_headers])
            headers = new_headers
            col = {h: i for i, h in enumerate(headers)}

        # find existing by Line_User_ID
        target_row = None
        for r_i in range(2, len(values) + 1):
            row = values[r_i - 1]
            uid = row[col["Line_User_ID"]] if col["Line_User_ID"] < len(row) else ""
            if uid == line_user_id:
                target_row = r_i
                break

        ts = now_jst().strftime("%Y-%m-%d %H:%M:%S")

        if target_row is None:
            out = [""] * len(headers)
            out[col["PersonName"]] = person_name
            out[col["Line_User_ID"]] = line_user_id
            out[col["LINE_DisplayName"]] = line_display_name
            out[col["CreatedAt_JST"]] = ts
            out[col["UpdatedAt_JST"]] = ts
            ws.append_row(out, value_input_option="USER_ENTERED")
        else:
            def set_cell(h: str, v: str):
                c = col[h] + 1
                ws.update_cell(target_row, c, v)

            set_cell("PersonName", person_name)
            set_cell("LINE_DisplayName", line_display_name)
            set_cell("UpdatedAt_JST", ts)

    def find_member_by_person(self, person_name: str) -> Optional[Dict[str, str]]:
        df = self.read_df(self.cfg.members_sheet_name)
        if df.empty:
            return None
        df = df.fillna("")
        if "PersonName" not in df.columns:
            return None
        m = df[df["PersonName"] == person_name]
        if m.empty:
            return None
        r = m.iloc[0].to_dict()
        return {k: safe_str(v) for k, v in r.items()}

    def find_member_by_line_user_id(self, line_user_id: str) -> Optional[Dict[str, str]]:
        df = self.read_df(self.cfg.members_sheet_name)
        if df.empty:
            return None
        df = df.fillna("")
        if "Line_User_ID" not in df.columns:
            return None
        m = df[df["Line_User_ID"] == line_user_id]
        if m.empty:
            return None
        r = m.iloc[0].to_dict()
        return {k: safe_str(v) for k, v in r.items()}

    # ---- personal sheets mapping (optional) ----
    def personal_sheet_id_for_person(self, person_name: str) -> Optional[str]:
        if not self.cfg.use_personal_sheets:
            return None
        df = self.read_df(self.cfg.personal_map_sheet_name)
        if df.empty:
            return None
        df = df.fillna("")
        if "PersonName" not in df.columns or "SpreadsheetId" not in df.columns:
            return None
        m = df[df["PersonName"] == person_name]
        if m.empty:
            return None
        return safe_str(m.iloc[0]["SpreadsheetId"])

    def append_to_personal_ledger(self, person_name: str, row: List[Any]) -> None:
        sid = self.personal_sheet_id_for_person(person_name)
        if not sid:
            return
        book = self.gc.open_by_key(sid)
        ws = book.worksheet(self.cfg.ledger_sheet_name)
        ws.append_row([safe_str(x) for x in row], value_input_option="USER_ENTERED")


# =========================
# Headers copy (for sheet creation)
# =========================

DEFAULT_MEMBERS_HEADERS = ["PersonName", "Line_User_ID", "LINE_DisplayName", "CreatedAt_JST", "UpdatedAt_JST"]
DEFAULT_LEDGER_HEADERS = [
    "Datetime_JST",
    "PersonName",
    "Type",             # Deposit / Withdraw / APR / Other
    "Amount",
    "Currency",         # JPY / USDT etc
    "Note",
    "Line_User_ID",
    "LINE_DisplayName",
    "Source",           # app / line / make / other
]


def ui_show_headers():
    st.write("### シートのヘッダー（コピペ用）")
    st.write("**Members**")
    st.code("\t".join(DEFAULT_MEMBERS_HEADERS))
    st.write("**Ledger**")
    st.code("\t".join(DEFAULT_LEDGER_HEADERS))
    st.caption("※ シート1行目に貼り付けてください（区切りはタブ）。")


# =========================
# UI: Member Management
# =========================

def ui_members(gs: GSheets):
    st.subheader("👤 メンバー管理（個人名 ↔ LINE）")

    colA, colB = st.columns([1, 1])
    with colA:
        st.write("#### 現在のメンバー一覧")
        df = gs.read_df(gs.cfg.members_sheet_name)
        if df.empty:
            st.info("Members シートが空です。")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)

    with colB:
        st.write("#### 手動で紐付け/更新（管理者）")
        with st.form("member_upsert", clear_on_submit=False):
            person = st.text_input("PersonName（個人名）", placeholder="例: 田中太郎")
            line_uid = st.text_input("Line_User_ID", placeholder="例: Uxxxxxxxxxxxxxxxx")
            disp = st.text_input("LINE_DisplayName", placeholder="例: taro")
            ok = st.form_submit_button("保存（Upsert）")

        if ok:
            if not person or not line_uid:
                st.error("PersonName と Line_User_ID は必須です。")
            else:
                gs.upsert_member(person_name=person, line_user_id=line_uid, line_display_name=disp)
                st.success("保存しました。")
                st.rerun()


# =========================
# UI: Ledger (Deposit/Withdraw/APR)
# =========================

def ui_ledger(gs: GSheets):
    st.subheader("💰 入金 / 出金 / APR 記録")

    tabs = st.tabs(["記録する", "台帳を見る"])
    with tabs[0]:
        df_members = gs.read_df(gs.cfg.members_sheet_name).fillna("")
        person_list: List[str] = []
        if not df_members.empty and "PersonName" in df_members.columns:
            person_list = sorted([p for p in df_members["PersonName"].tolist() if str(p).strip()])

        with st.form("ledger_form", clear_on_submit=False):
            dt = st.date_input("日付（JST）", value=now_jst().date())
            tm = st.time_input("時刻（JST）", value=now_jst().time().replace(second=0, microsecond=0))
            dt_jst = datetime(dt.year, dt.month, dt.day, tm.hour, tm.minute, tm.second, tzinfo=JST)

            person = st.selectbox("個人名（PersonName）", options=[""] + person_list)
            typ = st.selectbox("種別", options=["Deposit", "Withdraw", "APR", "Other"])
            amount = st.number_input("金額", min_value=0.0, value=0.0, step=1000.0)
            currency = st.text_input("通貨", value="JPY")
            note = st.text_input("メモ", value="")
            source = st.selectbox("Source", options=["app", "line", "make", "other"])

            notify = st.checkbox("LINE通知を送る（本人宛）", value=True)

            submit = st.form_submit_button("台帳に追加")

        if submit:
            if not person:
                st.error("個人名（PersonName）を選んでください。")
            else:
                m = gs.find_member_by_person(person)
                line_uid = safe_str(m.get("Line_User_ID")) if m else ""
                disp = safe_str(m.get("LINE_DisplayName")) if m else ""

                row = [
                    dt_jst.strftime("%Y-%m-%d %H:%M:%S"),
                    person,
                    typ,
                    float(amount),
                    currency,
                    note,
                    line_uid,
                    disp,
                    source,
                ]

                # Central ledger
                gs.append_row(gs.cfg.ledger_sheet_name, row)

                # Optional personal sheet write
                if gs.cfg.use_personal_sheets:
                    gs.append_to_personal_ledger(person_name=person, row=row)

                # --- LINE通知（本人宛）---
                if notify:
                    msg = format_notify_message(
                        dt_jst.strftime("%Y-%m-%d %H:%M:%S"),
                        person,
                        typ,
                        float(amount),
                        currency,
                        note,
                    )
                    sent = line_push_text(line_uid, msg)
                    if sent:
                        st.toast("LINE通知を送信しました")
                    else:
                        st.toast("LINE通知はスキップ/失敗（台帳は保存済み）")

                st.success("追加しました。")
                st.rerun()

    with tabs[1]:
        df = gs.read_df(gs.cfg.ledger_sheet_name).fillna("")
        if df.empty:
            st.info("Ledger シートが空です。")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)

            st.write("#### フィルタ（簡易）")
            col1, col2, col3 = st.columns(3)
            with col1:
                p = st.selectbox("PersonName", options=["(all)"] + sorted(df["PersonName"].unique().tolist()))
            with col2:
                t = st.selectbox("Type", options=["(all)"] + sorted(df["Type"].unique().tolist()))
            with col3:
                cur = st.selectbox("Currency", options=["(all)"] + sorted(df["Currency"].unique().tolist()))

            f = df.copy()
            if p != "(all)":
                f = f[f["PersonName"] == p]
            if t != "(all)":
                f = f[f["Type"] == t]
            if cur != "(all)":
                f = f[f["Currency"] == cur]

            st.dataframe(f, use_container_width=True, hide_index=True)


# =========================
# UI: Admin Panel (復旧)
# =========================

def ui_admin(gs: GSheets):
    st.subheader("⚙️ 管理（管理者のみ）")

    # ここが重要：フォームを表示してから、権限が無ければ停止
    admin_login_ui()

    if not is_admin():
        st.info("管理者PIN/パスを入力すると管理機能が表示されます。")
        st.stop()

    st.success("管理者機能が有効です。")
    st.divider()

    st.write("### 接続情報")
    st.write(f"- Registry Spreadsheet ID: `{gs.cfg.registry_spreadsheet_id}`")
    st.write(f"- Members sheet: `{gs.cfg.members_sheet_name}`")
    st.write(f"- Ledger sheet: `{gs.cfg.ledger_sheet_name}`")
    st.write(f"- Personal sheets: `{gs.cfg.use_personal_sheets}`")
    if gs.cfg.use_personal_sheets:
        st.write(f"- Personal map sheet: `{gs.cfg.personal_map_sheet_name}`")

    st.write("### LINE通知設定（確認）")
    tok = get_line_token()
    if tok:
        st.success("LINE Channel access token: 設定済み（表示は省略）")
    else:
        st.warning("LINE Channel access token: 未設定（通知はスキップされます）")

    ui_show_headers()
    st.divider()

    ui_members(gs)


# =========================
# Main App
# =========================

def main():
    st.set_page_config(page_title="APRシステム", layout="wide")
    st.title("APRシステム")

    cfg = load_gsheets_cfg()
    if cfg is None:
        st.error("Secrets の [connections.gsheets] spreadsheet が未設定です。Secrets を設定してください。")
        st.stop()

    gs = GSheets(cfg)

    tab_ledger, tab_members, tab_admin_ = st.tabs(["📒 台帳", "👤 メンバー", "⚙ 管理（管理者のみ）"])

    with tab_ledger:
        ui_ledger(gs)

    with tab_members:
        df = gs.read_df(gs.cfg.members_sheet_name).fillna("")
        if df.empty:
            st.info("Members シートが空です。")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption("※ メンバーの追加/更新は管理タブから行ってください。")

    with tab_admin_:
        ui_admin(gs)


if __name__ == "__main__":
    main()
