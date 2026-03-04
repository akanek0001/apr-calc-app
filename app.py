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


def s(x: Any) -> str:
    return "" if x is None else str(x)


# =========================
# LINE
# =========================
def send_line_text(token: str, user_id: str, text: str) -> int:
    if not token or not user_id:
        return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {"to": str(user_id), "messages": [{"type": "text", "text": text}]}
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
        return r.status_code
    except Exception:
        return 500


# =========================
# Admin Auth
# =========================
def admin_password() -> str:
    return s(st.secrets.get("admin", {}).get("password", "")).strip()


def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))


def admin_login_ui() -> None:
    pw_required = admin_password()
    if not pw_required:
        st.warning("Secrets の [admin].password が未設定です。管理画面の保護ができません。")
        st.session_state["admin_ok"] = False
        return

    if is_admin():
        c1, c2 = st.columns([1, 1])
        with c1:
            st.success("管理者ログイン中")
        with c2:
            if st.button("ログアウト", use_container_width=True):
                st.session_state["admin_ok"] = False
                st.rerun()
        return

    with st.form("admin_login_form", clear_on_submit=False):
        pw = st.text_input("管理者パスワード", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if pw == pw_required:
                st.session_state["admin_ok"] = True
                st.success("ログイン成功")
                st.rerun()
            else:
                st.session_state["admin_ok"] = False
                st.error("パスワードが違います")


# =========================
# Google Sheets (connections style secrets)
# =========================
@dataclass
class GCfg:
    spreadsheet_url: str
    credentials: Dict[str, Any]
    members_sheet: str = "Members"
    ledger_sheet: str = "Ledger"


MEMBERS_HEADERS = ["PersonName", "Line_User_ID", "LINE_DisplayName", "CreatedAt_JST", "UpdatedAt_JST"]
LEDGER_HEADERS = [
    "Datetime_JST",
    "PersonName",
    "Type",      # Deposit / Withdraw / APR / Other
    "Amount",
    "Currency",
    "Note",
    "Line_User_ID",
    "LINE_DisplayName",
    "Source",    # app
]


def load_cfg() -> GCfg:
    con = st.secrets.get("connections", {}).get("gsheets", {})
    spreadsheet_url = s(con.get("spreadsheet", "")).strip()

    cred = con.get("credentials", None)
    if not spreadsheet_url:
        st.error('Secrets に [connections.gsheets].spreadsheet がありません（URLを入れてください）。')
        st.stop()
    if not isinstance(cred, dict):
        st.error('Secrets に [connections.gsheets.credentials] がありません。')
        st.stop()

    # 必須フィールド確認（あなたのエラー対策）
    need = ["token_uri", "client_email", "private_key", "project_id"]
    missing = [k for k in need if not s(cred.get(k, "")).strip()]
    if missing:
        st.error(f"Service Account 情報が不足しています: {missing}")
        st.stop()

    return GCfg(
        spreadsheet_url=spreadsheet_url,
        credentials=cred,
        members_sheet=s(st.secrets.get("gsheets", {}).get("members_sheet_name", "Members")) or "Members",
        ledger_sheet=s(st.secrets.get("gsheets", {}).get("ledger_sheet_name", "Ledger")) or "Ledger",
    )


class GSheets:
    def __init__(self, cfg: GCfg):
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(cfg.credentials, scopes=scopes)
        self.gc = gspread.authorize(creds)
        self.book = self.gc.open_by_url(cfg.spreadsheet_url)
        self.cfg = cfg

    def ws(self, name: str):
        return self.book.worksheet(name)

    def ensure_sheet_with_header(self, name: str, headers: List[str]) -> None:
        try:
            ws = self.ws(name)
        except Exception:
            ws = self.book.add_worksheet(title=name, rows=1000, cols=max(10, len(headers) + 5))
        vals = ws.get_all_values()
        if not vals:
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return
        # 1行目ヘッダーが違う場合は上書き（安全側）
        if vals and [c.strip() for c in vals[0]] != headers:
            ws.update("1:1", [headers])

    def read_df(self, name: str) -> pd.DataFrame:
        ws = self.ws(name)
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        headers = values[0]
        rows = values[1:]
        return pd.DataFrame(rows, columns=headers)

    def append_row(self, name: str, row: List[Any]) -> None:
        ws = self.ws(name)
        ws.append_row([s(x) for x in row], value_input_option="USER_ENTERED")

    # --- Members upsert ---
    def upsert_member(self, person: str, line_uid: str, line_disp: str) -> None:
        self.ensure_sheet_with_header(self.cfg.members_sheet, MEMBERS_HEADERS)
        ws = self.ws(self.cfg.members_sheet)
        values = ws.get_all_values()
        headers = values[0]
        idx = {h: i for i, h in enumerate(headers)}

        target_row = None
        for r in range(2, len(values) + 1):
            row = values[r - 1]
            uid = row[idx["Line_User_ID"]] if idx["Line_User_ID"] < len(row) else ""
            if uid == line_uid and uid:
                target_row = r
                break

        ts = now_jst().strftime("%Y-%m-%d %H:%M:%S")
        if target_row is None:
            out = [""] * len(headers)
            out[idx["PersonName"]] = person
            out[idx["Line_User_ID"]] = line_uid
            out[idx["LINE_DisplayName"]] = line_disp
            out[idx["CreatedAt_JST"]] = ts
            out[idx["UpdatedAt_JST"]] = ts
            ws.append_row(out, value_input_option="USER_ENTERED")
        else:
            ws.update_cell(target_row, idx["PersonName"] + 1, person)
            ws.update_cell(target_row, idx["LINE_DisplayName"] + 1, line_disp)
            ws.update_cell(target_row, idx["UpdatedAt_JST"] + 1, ts)

    def member_by_person(self, person: str) -> Optional[Dict[str, str]]:
        df = self.read_df(self.cfg.members_sheet).fillna("")
        if df.empty:
            return None
        m = df[df["PersonName"] == person]
        if m.empty:
            return None
        return {k: s(v) for k, v in m.iloc[0].to_dict().items()}

    def all_line_ids(self) -> List[str]:
        df = self.read_df(self.cfg.members_sheet).fillna("")
        if df.empty or "Line_User_ID" not in df.columns:
            return []
        ids = [s(x).strip() for x in df["Line_User_ID"].tolist() if s(x).strip().startswith("U")]
        # uniq preserve order
        seen, out = set(), []
        for x in ids:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out


# =========================
# UI
# =========================
def ui_headers_help():
    st.write("### シートヘッダー（コピペ用）")
    st.write("**Members**")
    st.code("\t".join(MEMBERS_HEADERS))
    st.write("**Ledger**")
    st.code("\t".join(LEDGER_HEADERS))


def main():
    st.set_page_config(page_title="APRシステム", layout="wide", page_icon="🏦")
    st.title("🏦 APRシステム（安定版）")

    cfg = load_cfg()
    gs = GSheets(cfg)

    # ensure sheets exist
    gs.ensure_sheet_with_header(gs.cfg.members_sheet, MEMBERS_HEADERS)
    gs.ensure_sheet_with_header(gs.cfg.ledger_sheet, LEDGER_HEADERS)

    token = s(st.secrets.get("line", {}).get("channel_access_token", "")).strip()

    tab_ledger, tab_members, tab_admin = st.tabs(["💰 入出金/APR", "👤 メンバー", "⚙ 管理"])

    # -------------------
    # Ledger (Deposit/Withdraw/APR)
    # -------------------
    with tab_ledger:
        st.subheader("💰 入金 / 出金 / APR 記録（JSTで保存）")

        dfm = gs.read_df(gs.cfg.members_sheet).fillna("")
        persons = sorted([s(x).strip() for x in dfm.get("PersonName", pd.Series([])).tolist() if s(x).strip()])

        col1, col2 = st.columns([1, 1])
        with col1:
            st.write("#### 記録する")
            with st.form("ledger_form"):
                dt = st.date_input("日付（JST）", value=now_jst().date())
                tm = st.time_input("時刻（JST）", value=now_jst().time().replace(second=0, microsecond=0))
                dt_jst = datetime(dt.year, dt.month, dt.day, tm.hour, tm.minute, 0, tzinfo=JST)

                person = st.selectbox("個人名（PersonName）", options=[""] + persons)
                typ = st.selectbox("種別", options=["Deposit", "Withdraw", "APR", "Other"])
                amount = st.number_input("金額", min_value=0.0, value=0.0, step=1000.0)
                currency = st.text_input("通貨", value="JPY")
                note = st.text_input("メモ", value="")
                submit = st.form_submit_button("台帳に追加")

            if submit:
                if not person:
                    st.error("個人名を選んでください。")
                else:
                    m = gs.member_by_person(person) or {}
                    uid = s(m.get("Line_User_ID", "")).strip()
                    disp = s(m.get("LINE_DisplayName", "")).strip()

                    row = [
                        dt_jst.strftime("%Y-%m-%d %H:%M:%S"),
                        person,
                        typ,
                        float(amount),
                        currency,
                        note,
                        uid,
                        disp,
                        "app",
                    ]
                    gs.append_row(gs.cfg.ledger_sheet, row)

                    # 入金・出金は個人にLINE通知
                    if token and uid and typ in ("Deposit", "Withdraw"):
                        sign = "+" if typ == "Deposit" else "-"
                        msg = (
                            f"🏦 入出金通知\n"
                            f"日時: {dt_jst.strftime('%Y/%m/%d %H:%M')} (JST)\n"
                            f"種別: {typ}\n"
                            f"金額: {sign}{amount:,.0f} {currency}\n"
                        )
                        if note:
                            msg += f"メモ: {note}\n"
                        code = send_line_text(token, uid, msg)
                        if code == 200:
                            st.success("台帳に記録し、本人へLINE通知しました。")
                        else:
                            st.warning(f"台帳に記録しましたが、LINE送信に失敗しました（HTTP {code}）。")
                    else:
                        st.success("台帳に記録しました。")

                    st.rerun()

        with col2:
            st.write("#### APR報告（全員へ一斉送信・個人名なし）")
            st.caption("APR報告は全員に送る前提なので、メッセージに個人名は入れません。")
            apr = st.number_input("本日のAPR（%）", value=0.0, step=0.1)
            memo = st.text_input("メモ（任意）", value="")

            if st.button("APR報告を全員に送信", use_container_width=True):
                ids = gs.all_line_ids()
                if not token:
                    st.error("Secrets の [line].channel_access_token が未設定です。")
                elif not ids:
                    st.error("Members に Line_User_ID がありません。")
                else:
                    dt = now_jst().strftime("%Y/%m/%d %H:%M")
                    msg = f"🏦 本日のAPR報告\n日時: {dt} (JST)\nAPR: {apr}%\n"
                    if memo:
                        msg += f"メモ: {memo}\n"
                    ok = 0
                    ng = 0
                    for uid in ids:
                        code = send_line_text(token, uid, msg)
                        if code == 200:
                            ok += 1
                        else:
                            ng += 1
                    st.success(f"送信完了：成功 {ok} / 失敗 {ng}")

        st.divider()
        st.write("#### Ledger（最新）")
        dfl = gs.read_df(gs.cfg.ledger_sheet).fillna("")
        if dfl.empty:
            st.info("Ledger は空です。")
        else:
            st.dataframe(dfl.tail(200), use_container_width=True, hide_index=True)

    # -------------------
    # Members
    # -------------------
    with tab_members:
        st.subheader("👤 メンバー一覧（閲覧）")
        df = gs.read_df(gs.cfg.members_sheet).fillna("")
        if df.empty:
            st.info("Members は空です。")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption("※ メンバーの追加/編集は管理タブから行ってください。")

    # -------------------
    # Admin
    # -------------------
    with tab_admin:
        st.subheader("⚙ 管理（管理者のみ）")
        admin_login_ui()

        if not is_admin():
            st.info("管理者パスワードを入力すると管理機能が表示されます。")
            st.stop()

        st.success("管理者機能が有効です。")
        ui_headers_help()
        st.divider()

        st.write("### メンバー追加/更新（個人名 ↔ LINE ID）")
        with st.form("member_form"):
            person = st.text_input("PersonName（個人名）", placeholder="例: 祥子")
            uid = st.text_input("Line_User_ID", placeholder="例: Uxxxxxxxxxxxxxxxx")
            disp = st.text_input("LINE_DisplayName（任意）", placeholder="例: sacho")
            ok = st.form_submit_button("保存（Upsert）")

        if ok:
            if not person or not uid:
                st.error("PersonName と Line_User_ID は必須です。")
            else:
                gs.upsert_member(person, uid.strip(), disp.strip())
                st.success("保存しました。")
                st.rerun()

        st.divider()
        st.write("### 設定確認")
        st.write(f"- spreadsheet: `{cfg.spreadsheet_url}`")
        st.write(f"- members sheet: `{gs.cfg.members_sheet}`")
        st.write(f"- ledger sheet: `{gs.cfg.ledger_sheet}`")


if __name__ == "__main__":
    main()
