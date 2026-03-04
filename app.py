# app.py
# Streamlit APR / 入出金管理 + 管理者(⚙️)画面復旧テンプレート
# - 管理者PINログイン（Secretsの admin.pin）
# - メンバー（個人名 ↔ LINE User ID）管理
# - 入金/出金/APR記録（中央台帳シート）
# - 必要なら「個人別スプレッドシート」にも同時書き込み（任意）
#
# 重要:
# 1) Secrets を必ず設定してください（下の REQUIRED SECRETS を参照）
# 2) Google Sheets 構成は「中央台帳」方式が壊れにくいです（個人別はオプション扱いにしています）
#
# REQUIRED SECRETS（Streamlit Cloud > Settings > Secrets）例:
# [admin]
# pin = "1234"
#
# [gsheets]
# service_account_json = '''{ ... service account json ... }'''
# registry_spreadsheet_id = "中央台帳スプレッドシートID"
# members_sheet_name = "Members"
# ledger_sheet_name = "Ledger"
#
# # （任意）個人別スプレッドシートへも書く場合
# use_personal_sheets = true
# personal_map_sheet_name = "PersonalMap"   # 個人名→スプレッドシートID を持つシート（中央台帳内）
#
# ※ service account に対象スプレッドシートを共有（編集者）してください。

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# ---- Optional deps (gspread/google-auth) ----
# Streamlit Cloud で requirements.txt に入れてください:
# gspread
# google-auth
try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None


# =========================
# Utilities
# =========================

JST = timezone(timedelta(hours=9), "JST")


def now_jst() -> datetime:
    return datetime.now(JST)


def to_jst_from_epoch_ms(epoch_ms: int) -> datetime:
    """
    LINEのWebhook timestamp(ms) などを JST に変換。
    """
    dt_utc = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return dt_utc.astimezone(JST)


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def require_gspread():
    if gspread is None or Credentials is None:
        st.error("Google Sheets 連携に必要なライブラリ(gspread/google-auth)がインストールされていません。")
        st.stop()


def is_truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


# =========================
# Admin Auth
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
        pin = st.text_input("管理者PIN", type="password", help="Secrets の admin.pin と一致すると管理画面が開きます。")
        ok = st.form_submit_button("管理者ログイン")
        if ok:
            if pin == pin_required:
                st.session_state["admin_ok"] = True
                st.success("管理者ログインに成功しました。")
            else:
                st.session_state["admin_ok"] = False
                st.error("PINが違います。")


# =========================
# Google Sheets Client
# =========================

@dataclass
class GSheetsConfig:
    registry_spreadsheet_id: str
    members_sheet_name: str
    ledger_sheet_name: str
    use_personal_sheets: bool = False
    personal_map_sheet_name: str = "PersonalMap"


class GSheets:
    def __init__(self, cfg: GSheetsConfig):
        require_gspread()
        self.cfg = cfg

        raw = st.secrets.get("gsheets", {}).get("service_account_json", "")
        if not raw:
            st.error("Secrets に gsheets.service_account_json がありません。")
            st.stop()

        try:
            sa_info = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            # '''{...}''' の形でも入るので json.loads が失敗する場合はそのまま
            sa_info = json.loads(str(raw))

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
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
            # create header if empty
            header = ["PersonName", "Line_User_ID", "LINE_DisplayName", "CreatedAt_JST", "UpdatedAt_JST"]
            ws.append_row(header, value_input_option="USER_ENTERED")
            values = [header]

        headers = values[0]
        col = {h: i for i, h in enumerate(headers)}

        # ensure required columns exist
        required = ["PersonName", "Line_User_ID", "LINE_DisplayName", "CreatedAt_JST", "UpdatedAt_JST"]
        missing = [h for h in required if h not in col]
        if missing:
            # extend headers
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
            # append
            out = [""] * len(headers)
            out[col["PersonName"]] = person_name
            out[col["Line_User_ID"]] = line_user_id
            out[col["LINE_DisplayName"]] = line_display_name
            out[col["CreatedAt_JST"]] = ts
            out[col["UpdatedAt_JST"]] = ts
            ws.append_row(out, value_input_option="USER_ENTERED")
        else:
            # update row
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
        """
        個人別スプレッドシートへ Ledger と同じ形式で書き込みたい場合。
        その個人の spreadsheet_id を PersonalMap から引く。
        """
        sid = self.personal_sheet_id_for_person(person_name)
        if not sid:
            return
        book = self.gc.open_by_key(sid)
        ws = book.worksheet(self.cfg.ledger_sheet_name)
        ws.append_row([safe_str(x) for x in row], value_input_option="USER_ENTERED")


# =========================
# App Config
# =========================

def load_gsheets_cfg() -> Optional[GSheetsConfig]:
    g = st.secrets.get("gsheets", {})
    rid = safe_str(g.get("registry_spreadsheet_id"))
    members = safe_str(g.get("members_sheet_name", "Members"))
    ledger = safe_str(g.get("ledger_sheet_name", "Ledger"))
    if not rid:
        return None
    use_personal = is_truthy(g.get("use_personal_sheets", False))
    pmap = safe_str(g.get("personal_map_sheet_name", "PersonalMap")) or "PersonalMap"
    return GSheetsConfig(
        registry_spreadsheet_id=rid,
        members_sheet_name=members,
        ledger_sheet_name=ledger,
        use_personal_sheets=use_personal,
        personal_map_sheet_name=pmap,
    )


# =========================
# UI: Headers copy (for your sheet creation standard)
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
    "Source",           # app / line / make
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
        person_list = []
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
                    amount,
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
# UI: Admin Panel (Fix)
# =========================

def ui_admin(gs: GSheets):
    st.subheader("⚙️ 管理（管理者のみ）")

    # ここが重要：フォームを表示してから、権限が無ければ停止
    admin_login_ui()

    if not is_admin():
        st.info("管理者PINを入力すると管理機能が表示されます。")
        st.stop()

    st.success("管理者機能が有効です。")
    st.divider()

    st.write("### シート構造の確認")
    st.write(f"- Registry Spreadsheet ID: `{gs.cfg.registry_spreadsheet_id}`")
    st.write(f"- Members sheet: `{gs.cfg.members_sheet_name}`")
    st.write(f"- Ledger sheet: `{gs.cfg.ledger_sheet_name}`")
    st.write(f"- Personal sheets: `{gs.cfg.use_personal_sheets}`")
    if gs.cfg.use_personal_sheets:
        st.write(f"- Personal map sheet: `{gs.cfg.personal_map_sheet_name}`")

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
        st.error("Secrets の gsheets.registry_spreadsheet_id 等が未設定です。Secrets を設定してください。")
        st.stop()

    gs = GSheets(cfg)

    tab_ledger, tab_members, tab_admin_ = st.tabs(["📒 台帳", "👤 メンバー", "⚙ 管理（管理者のみ）"])

    with tab_ledger:
        ui_ledger(gs)

    with tab_members:
        # 一般表示（閲覧）＋（必要なら）管理者だけ編集したい場合はここも制御できます
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
