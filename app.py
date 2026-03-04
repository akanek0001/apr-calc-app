# app.py
# APRシステム（Streamlit + Google Sheets）
# - 管理者ログイン（Secrets: admin.password または admin.pin）
# - メンバー管理（個人名 ↔ LINE User ID / displayName）
# - 入金/出金/APR を中央台帳（Ledger）に記録
# - JST（日本時間）で統一
#
# ✅ 今回の実装は「connections.gsheets」方式に最適化
#   Secrets は以下いずれでもOK：
#
# --- 推奨（Streamlit Cloud の公式例に近い）---
# [connections.gsheets]
# spreadsheet = "スプレッドシートID または URL"
#
# [connections.gsheets.credentials]
# type = "service_account"
# project_id = "..."
# private_key_id = "..."
# private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
# client_email = "....iam.gserviceaccount.com"
# client_id = "..."
# token_uri = "https://oauth2.googleapis.com/token"
#
# --- 追加（アプリ側設定）---
# [admin]
# password = "your-pass"   # または pin = "xxxx"
#
# [gsheets]
# members_sheet_name = "Members"
# ledger_sheet_name = "Ledger"
# use_personal_sheets = false
# personal_map_sheet_name = "PersonalMap"
#
# 注意:
# - service account を対象スプレッドシートに「編集者」で共有してください
# - Sheets の 1行目はヘッダー行（Table contains headers = Yes 想定）

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

# 依存: requirements.txt に入れてください
# gspread
# google-auth
try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None


# =========================
# Time / Utils
# =========================

JST = timezone(timedelta(hours=9), "JST")


def now_jst() -> datetime:
    return datetime.now(JST)


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def is_truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def require_gspread() -> None:
    if gspread is None or Credentials is None:
        st.error("必要ライブラリがありません: gspread / google-auth を requirements.txt に追加してください。")
        st.stop()


def extract_sheet_id(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # URLなら /spreadsheets/d/<ID>/ を抜く
    if "/spreadsheets/d/" in s:
        try:
            return s.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception:
            return s
    # それ以外はIDとして扱う
    return s


# =========================
# Config
# =========================

@dataclass
class GSheetsConfig:
    registry_spreadsheet_id: str
    members_sheet_name: str = "Members"
    ledger_sheet_name: str = "Ledger"
    use_personal_sheets: bool = False
    personal_map_sheet_name: str = "PersonalMap"


def load_gsheets_cfg() -> Optional[GSheetsConfig]:
    # connections.gsheets の spreadsheet を最優先
    con = st.secrets.get("connections", {}).get("gsheets", {})
    spreadsheet = safe_str(con.get("spreadsheet", "")).strip()
    sid = extract_sheet_id(spreadsheet)

    # 旧方式 [gsheets] registry_spreadsheet_id がある場合もフォールバック
    if not sid:
        g = st.secrets.get("gsheets", {})
        sid = extract_sheet_id(safe_str(g.get("registry_spreadsheet_id", "")).strip())

    if not sid:
        return None

    # シート名などは [gsheets] を使う（無ければデフォルト）
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
# Admin Auth
# =========================

def admin_required_value() -> str:
    a = st.secrets.get("admin", {})
    # password 優先、無ければ pin
    pw = safe_str(a.get("password", "")).strip()
    if pw:
        return pw
    pin = safe_str(a.get("pin", "")).strip()
    return pin


def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))


def admin_login_ui() -> None:
    required = admin_required_value()
    if not required:
        st.warning("Secrets に admin.password (または admin.pin) が未設定です。管理画面は保護されません。")
        st.session_state["admin_ok"] = True  # 未設定なら便宜上通す（止めたいなら False に）
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
        val = st.text_input("管理者パスワード / PIN", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if val == required:
                st.session_state["admin_ok"] = True
                st.success("ログイン成功")
            else:
                st.session_state["admin_ok"] = False
                st.error("一致しません")


# =========================
# Sheet Headers (copy/paste)
# =========================

DEFAULT_MEMBERS_HEADERS = ["PersonName", "Line_User_ID", "LINE_DisplayName", "CreatedAt_JST", "UpdatedAt_JST"]

DEFAULT_LEDGER_HEADERS = [
    "Datetime_JST",
    "PersonName",
    "Type",            # Deposit / Withdraw / APR / Other
    "Amount",
    "Currency",        # JPY / USDT etc
    "Note",
    "Line_User_ID",
    "LINE_DisplayName",
    "Source",          # app / line / make / other
]


def ui_show_headers() -> None:
    st.write("### シートのヘッダー（コピペ用）")
    st.write("**Members**")
    st.code("\t".join(DEFAULT_MEMBERS_HEADERS))
    st.write("**Ledger**")
    st.code("\t".join(DEFAULT_LEDGER_HEADERS))
    st.caption("※ シートの1行目に貼り付け（区切りはタブ）")


# =========================
# Google Sheets Client
# =========================

class GSheets:
    def __init__(self, cfg: GSheetsConfig):
        require_gspread()
        self.cfg = cfg

        con = st.secrets.get("connections", {}).get("gsheets", {})
        creds_info = con.get("credentials", None)

        # 旧方式: [gsheets].service_account_json もフォールバック対応（文字列JSON）
        raw_json = st.secrets.get("gsheets", {}).get("service_account_json", "")

        if creds_info:
            sa_info = dict(creds_info)
        elif raw_json:
            # 文字列JSONを読む（壊れてるとJSONDecodeErrorになるので注意）
            import json
            try:
                sa_info = json.loads(raw_json) if isinstance(raw_json, str) else dict(raw_json)
            except Exception:
                st.error("gsheets.service_account_json のJSONが壊れています。connections.gsheets.credentials 方式を推奨します。")
                st.stop()
        else:
            st.error("Secrets に [connections.gsheets.credentials] がありません（または gsheets.service_account_json も未設定）。")
            st.stop()

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        try:
            creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        except Exception as e:
            st.error("Service account info が正しい形式ではありません（必須キー不足など）。")
            st.exception(e)
            st.stop()

        self.gc = gspread.authorize(creds)

        try:
            self.registry = self.gc.open_by_key(self.cfg.registry_spreadsheet_id)
        except Exception as e:
            st.error("スプレッドシートを開けません。ID/共有設定を確認してください。")
            st.exception(e)
            st.stop()

        # 必要シートが無ければ作る（壊れにくい）
        self.ensure_sheet(self.cfg.members_sheet_name, DEFAULT_MEMBERS_HEADERS)
        self.ensure_sheet(self.cfg.ledger_sheet_name, DEFAULT_LEDGER_HEADERS)
        if self.cfg.use_personal_sheets:
            self.ensure_sheet(self.cfg.personal_map_sheet_name, ["PersonName", "SpreadsheetId"])

    def ws(self, name: str):
        return self.registry.worksheet(name)

    def ensure_sheet(self, sheet_name: str, headers: List[str]) -> None:
        try:
            ws = self.registry.worksheet(sheet_name)
        except Exception:
            ws = self.registry.add_worksheet(title=sheet_name, rows=1000, cols=max(10, len(headers) + 5))
        # ヘッダー確認
        values = ws.get_all_values()
        if not values:
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return
        first = values[0]
        # 先頭行がヘッダーっぽくなければ、先頭行をヘッダーに合わせる（上書き）
        if first != headers:
            # 足りない列は追加（既存の列を壊さない）
            # 既存ヘッダーに無い required を追加
            exist = [safe_str(x) for x in first]
            colset = set(exist)
            merged = exist[:]
            for h in headers:
                if h not in colset:
                    merged.append(h)
            ws.update("1:1", [merged])

    def read_df(self, sheet_name: str, header_row: int = 1) -> pd.DataFrame:
        ws = self.ws(sheet_name)
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        hi = header_row - 1
        if len(values) <= hi:
            return pd.DataFrame()
        headers = values[hi]
        rows = values[hi + 1 :]
        df = pd.DataFrame(rows, columns=headers)
        return df

    def append_row(self, sheet_name: str, row: List[Any]) -> None:
        ws = self.ws(sheet_name)
        ws.append_row([safe_str(x) for x in row], value_input_option="USER_ENTERED")

    # ---- Members Upsert ----
    def upsert_member(self, person_name: str, line_user_id: str, line_display_name: str) -> None:
        ws = self.ws(self.cfg.members_sheet_name)
        values = ws.get_all_values()
        if not values:
            ws.append_row(DEFAULT_MEMBERS_HEADERS, value_input_option="USER_ENTERED")
            values = [DEFAULT_MEMBERS_HEADERS]

        headers = values[0]
        col = {h: i for i, h in enumerate(headers)}

        # 必須列がなければ追加
        for h in DEFAULT_MEMBERS_HEADERS:
            if h not in col:
                headers.append(h)
        ws.update("1:1", [headers])
        col = {h: i for i, h in enumerate(headers)}

        # Line_User_ID で検索
        target_row = None
        for r in range(2, len(values) + 1):
            row = values[r - 1]
            uid = row[col["Line_User_ID"]] if col["Line_User_ID"] < len(row) else ""
            if uid == line_user_id:
                target_row = r
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
                ws.update_cell(target_row, col[h] + 1, v)

            set_cell("PersonName", person_name)
            set_cell("LINE_DisplayName", line_display_name)
            set_cell("UpdatedAt_JST", ts)

    def find_member_by_person(self, person_name: str) -> Optional[Dict[str, str]]:
        df = self.read_df(self.cfg.members_sheet_name).fillna("")
        if df.empty or "PersonName" not in df.columns:
            return None
        m = df[df["PersonName"] == person_name]
        if m.empty:
            return None
        r = m.iloc[0].to_dict()
        return {k: safe_str(v) for k, v in r.items()}

    def find_member_by_line_user_id(self, line_user_id: str) -> Optional[Dict[str, str]]:
        df = self.read_df(self.cfg.members_sheet_name).fillna("")
        if df.empty or "Line_User_ID" not in df.columns:
            return None
        m = df[df["Line_User_ID"] == line_user_id]
        if m.empty:
            return None
        r = m.iloc[0].to_dict()
        return {k: safe_str(v) for k, v in r.items()}

    # ---- PersonalMap (optional) ----
    def personal_sheet_id_for_person(self, person_name: str) -> Optional[str]:
        if not self.cfg.use_personal_sheets:
            return None
        df = self.read_df(self.cfg.personal_map_sheet_name).fillna("")
        if df.empty or "PersonName" not in df.columns or "SpreadsheetId" not in df.columns:
            return None
        m = df[df["PersonName"] == person_name]
        if m.empty:
            return None
        return extract_sheet_id(safe_str(m.iloc[0]["SpreadsheetId"]))

    def append_to_personal_ledger(self, person_name: str, row: List[Any]) -> None:
        sid = self.personal_sheet_id_for_person(person_name)
        if not sid:
            return
        book = self.gc.open_by_key(sid)
        try:
            ws = book.worksheet(self.cfg.ledger_sheet_name)
        except Exception:
            ws = book.add_worksheet(title=self.cfg.ledger_sheet_name, rows=1000, cols=len(DEFAULT_LEDGER_HEADERS) + 5)
            ws.append_row(DEFAULT_LEDGER_HEADERS, value_input_option="USER_ENTERED")
        ws.append_row([safe_str(x) for x in row], value_input_option="USER_ENTERED")


# =========================
# UI: Members
# =========================

def ui_members(gs: GSheets) -> None:
    st.subheader("👤 メンバー管理（個人名 ↔ LINE）")

    colA, colB = st.columns([1, 1])

    with colA:
        st.write("#### 一覧")
        df = gs.read_df(gs.cfg.members_sheet_name).fillna("")
        if df.empty:
            st.info("Members シートが空です。")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)

    with colB:
        st.write("#### 追加 / 更新（Upsert）")
        st.caption("Line_User_ID が同じなら更新、無ければ新規追加します。")
        with st.form("member_upsert", clear_on_submit=False):
            person = st.text_input("PersonName（個人名）", placeholder="例: 田中太郎")
            line_uid = st.text_input("Line_User_ID", placeholder="例: Uxxxxxxxxxxxxxxxx")
            disp = st.text_input("LINE_DisplayName", placeholder="例: taro")
            ok = st.form_submit_button("保存")

        if ok:
            if not person.strip() or not line_uid.strip():
                st.error("PersonName と Line_User_ID は必須です。")
            else:
                gs.upsert_member(person.strip(), line_uid.strip(), disp.strip())
                st.success("保存しました。")
                st.rerun()


# =========================
# UI: Ledger
# =========================

def ui_ledger(gs: GSheets) -> None:
    st.subheader("💰 入金 / 出金 / APR 記録")

    tabs = st.tabs(["記録する", "台帳を見る"])

    # ---- 記録する ----
    with tabs[0]:
        df_members = gs.read_df(gs.cfg.members_sheet_name).fillna("")
        person_list: List[str] = []
        if not df_members.empty and "PersonName" in df_members.columns:
            person_list = sorted([p for p in df_members["PersonName"].tolist() if str(p).strip()])

        with st.form("ledger_form", clear_on_submit=False):
            d: date = st.date_input("日付（JST）", value=now_jst().date())
            t = st.time_input("時刻（JST）", value=now_jst().time().replace(second=0, microsecond=0))
            dt_jst = datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, tzinfo=JST)

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

                gs.append_row(gs.cfg.ledger_sheet_name, row)

                if gs.cfg.use_personal_sheets:
                    gs.append_to_personal_ledger(person_name=person, row=row)

                st.success("追加しました。")
                st.rerun()

    # ---- 台帳を見る ----
    with tabs[1]:
        df = gs.read_df(gs.cfg.ledger_sheet_name).fillna("")
        if df.empty:
            st.info("Ledger シートが空です。")
            return

        st.dataframe(df, use_container_width=True, hide_index=True)

        # 簡易フィルタ
        st.write("#### フィルタ")
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
# UI: Admin
# =========================

def ui_admin(gs: GSheets) -> None:
    st.subheader("⚙ 管理（管理者のみ）")

    admin_login_ui()

    if not is_admin():
        st.info("管理者パスワード / PIN を入力すると管理機能が表示されます。")
        st.stop()

    st.success("管理者機能が有効です。")
    st.divider()

    st.write("### 接続情報")
    st.write(f"- Spreadsheet ID: `{gs.cfg.registry_spreadsheet_id}`")
    st.write(f"- Members sheet: `{gs.cfg.members_sheet_name}`")
    st.write(f"- Ledger sheet: `{gs.cfg.ledger_sheet_name}`")
    st.write(f"- Personal sheets: `{gs.cfg.use_personal_sheets}`")
    if gs.cfg.use_personal_sheets:
        st.write(f"- Personal map sheet: `{gs.cfg.personal_map_sheet_name}`")

    ui_show_headers()
    st.divider()

    st.write("### メンバー管理")
    ui_members(gs)


# =========================
# Main
# =========================

def main() -> None:
    st.set_page_config(page_title="APRシステム", layout="wide")
    st.title("APRシステム")

    cfg = load_gsheets_cfg()
    if cfg is None:
        st.error("Secrets に Spreadsheet ID が見つかりません。connections.gsheets.spreadsheet を設定してください。")
        st.stop()

    gs = GSheets(cfg)

    tab_ledger, tab_members, tab_admin = st.tabs(["📒 台帳", "👤 メンバー", "⚙ 管理（管理者のみ）"])

    with tab_ledger:
        ui_ledger(gs)

    with tab_members:
        df = gs.read_df(gs.cfg.members_sheet_name).fillna("")
        if df.empty:
            st.info("Members シートが空です。")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption("※ 追加/更新は『⚙ 管理』タブから行ってください。")

    with tab_admin:
        ui_admin(gs)


if __name__ == "__main__":
    main()
