# app.py
# ✅ 完全版（中央台帳方式）
# - Google Sheets: Settings / Members / Ledger を使用
# - LINE ID 自動取得（MakeでMembersへUpsertしてもOK）+ アプリ側で手動紐付けも可
# - APR（受取率 67%）を日割り計算して「全員へ一斉LINE報告」+ Ledgerへ自動記録
# - 入金/出金 は「個人にLINE通知」+ Ledgerへ記録 + SettingsのTotalPrincipalを自動更新
# - 管理者ログイン（admin.pin）で管理タブ編集可
#
# =========================
# 必要シート（ヘッダー1行目）
# =========================
# Settings:
# Project_Name | TotalPrincipal | Currency | IsCompound | ReceiveFactor
#
# Members:
# PersonName | Project_Name | Line_User_ID | LINE_DisplayName | IsActive | CreatedAt_JST | UpdatedAt_JST
#
# Ledger:
# Datetime_JST | Project_Name | PersonName | Type | Amount | Currency | Note | Line_User_ID | LINE_DisplayName | Source | Evidence_URL
#
# =========================
# Secrets（Streamlit Cloud > Settings > Secrets）
# =========================
# [admin]
# pin = "your_password"
#
# [connections.gsheets]
# spreadsheet = "<SPREADSHEET_ID or URL>"
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
# [line]
# channel_access_token = "..."
#
# [imgbb]  # 任意（APRのエビデンス画像をURL化してLINE添付）
# api_key = "..."

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

# -----------------------------
# Timezone
# -----------------------------
JST = timezone(timedelta(hours=9), "JST")


def now_jst() -> datetime:
    return datetime.now(JST)


def jst_str(dt: Optional[datetime] = None) -> str:
    dt = dt or now_jst()
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# -----------------------------
# Helpers
# -----------------------------
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
        .str.replace("\u3000", " ", regex=False)  # 全角→半角
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


# -----------------------------
# LINE
# -----------------------------
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


# -----------------------------
# Admin auth
# -----------------------------
def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))


def admin_login_ui() -> None:
    pin_required = safe_str(st.secrets.get("admin", {}).get("pin", ""))
    if not pin_required:
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
            if pin == pin_required:
                st.session_state["admin_ok"] = True
                st.success("ログイン成功")
            else:
                st.session_state["admin_ok"] = False
                st.error("パスワードが違います")


# -----------------------------
# GSheets (connections.gsheets)
# -----------------------------
@dataclass
class GSheetsConfig:
    spreadsheet_id: str
    settings_sheet: str = "Settings"
    members_sheet: str = "Members"
    ledger_sheet: str = "Ledger"


DEFAULT_SETTINGS_HEADERS = ["Project_Name", "TotalPrincipal", "Currency", "IsCompound", "ReceiveFactor"]
DEFAULT_MEMBERS_HEADERS = [
    "PersonName",
    "Project_Name",
    "Line_User_ID",
    "LINE_DisplayName",
    "IsActive",
    "CreatedAt_JST",
    "UpdatedAt_JST",
]
DEFAULT_LEDGER_HEADERS = [
    "Datetime_JST",
    "Project_Name",
    "PersonName",
    "Type",
    "Amount",
    "Currency",
    "Note",
    "Line_User_ID",
    "LINE_DisplayName",
    "Source",
    "Evidence_URL",
]


def extract_sheet_id(value: str) -> str:
    s = (value or "").strip()
    if "/spreadsheets/d/" in s:
        try:
            return s.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception:
            return s
    return s


class GSheets:
    def __init__(self, cfg: GSheetsConfig):
        con = st.secrets.get("connections", {}).get("gsheets", {})
        spreadsheet = extract_sheet_id(safe_str(con.get("spreadsheet", "")))
        if not spreadsheet:
            st.error("Secrets の [connections.gsheets].spreadsheet が未設定です。")
            st.stop()

        creds_info = con.get("credentials")
        if not creds_info:
            st.error('Secrets に [connections.gsheets.credentials] がありません。')
            st.stop()

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(dict(creds_info), scopes=scopes)
        self.gc = gspread.authorize(creds)

        self.cfg = cfg
        self.cfg.spreadsheet_id = spreadsheet
        self.book = self.gc.open_by_key(self.cfg.spreadsheet_id)

        # ensure sheets exist
        self._ensure_sheet(self.cfg.settings_sheet, DEFAULT_SETTINGS_HEADERS)
        self._ensure_sheet(self.cfg.members_sheet, DEFAULT_MEMBERS_HEADERS)
        self._ensure_sheet(self.cfg.ledger_sheet, DEFAULT_LEDGER_HEADERS)

    def _ensure_sheet(self, name: str, headers: List[str]) -> None:
        try:
            ws = self.book.worksheet(name)
        except Exception:
            ws = self.book.add_worksheet(title=name, rows=1000, cols=max(20, len(headers) + 5))
        values = ws.get_all_values()
        if not values:
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return
        # if header row exists but missing some columns -> extend
        current = [str(x).strip() for x in values[0]]
        if current == [""] * len(current):
            ws.update("1:1", [headers])
            return
        missing = [h for h in headers if h not in current]
        if missing:
            new_headers = current + missing
            ws.update("1:1", [new_headers])

    def ws(self, name: str):
        return self.book.worksheet(name)

    def read_df(self, sheet_name: str) -> pd.DataFrame:
        ws = self.ws(sheet_name)
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        header = values[0]
        rows = values[1:]
        df = pd.DataFrame(rows, columns=header)
        return clean_cols(df).fillna("")

    def append_row(self, sheet_name: str, row: Dict[str, Any], header_order: List[str]) -> None:
        ws = self.ws(sheet_name)
        out = [safe_str(row.get(h, "")) for h in header_order]
        ws.append_row(out, value_input_option="USER_ENTERED")

    # ---- Settings helpers ----
    def get_settings(self) -> pd.DataFrame:
        df = self.read_df(self.cfg.settings_sheet)
        if df.empty:
            return df
        # ensure required cols
        for h in DEFAULT_SETTINGS_HEADERS:
            if h not in df.columns:
                df[h] = ""
        return df

    def upsert_project(self, project_name: str, total_principal: float, currency: str, is_compound: bool, receive_factor: float) -> None:
        ws = self.ws(self.cfg.settings_sheet)
        df = self.get_settings()
        df = df.fillna("")

        # normalize
        project_name = project_name.strip()
        if project_name == "":
            return

        now = jst_str()
        # find row by Project_Name (1-based in sheet, header is row 1)
        idx = None
        if not df.empty:
            matches = df.index[df["Project_Name"].astype(str) == project_name].tolist()
            if matches:
                idx = matches[0]  # 0-based in df

        row_dict = {
            "Project_Name": project_name,
            "TotalPrincipal": str(total_principal),
            "Currency": currency.strip() or "JPY",
            "IsCompound": "TRUE" if is_compound else "FALSE",
            "ReceiveFactor": str(receive_factor),
        }

        if idx is None:
            # append
            self.append_row(self.cfg.settings_sheet, row_dict, self._settings_headers())
        else:
            # update entire row cells (safe)
            sheet_row = idx + 2
            headers = self._settings_headers()
            for c_i, h in enumerate(headers, start=1):
                ws.update_cell(sheet_row, c_i, safe_str(row_dict.get(h, "")))

    def update_project_total(self, project_name: str, new_total: float) -> None:
        ws = self.ws(self.cfg.settings_sheet)
        df = self.get_settings()
        if df.empty:
            return
        matches = df.index[df["Project_Name"].astype(str) == str(project_name)].tolist()
        if not matches:
            return
        idx = matches[0]
        sheet_row = idx + 2

        # ensure column exists
        headers = self._settings_headers()
        if "TotalPrincipal" not in headers:
            return
        col = headers.index("TotalPrincipal") + 1
        ws.update_cell(sheet_row, col, safe_str(round(new_total, 6)))

    def _settings_headers(self) -> List[str]:
        ws = self.ws(self.cfg.settings_sheet)
        values = ws.get_all_values()
        return [str(x).strip() for x in (values[0] if values else DEFAULT_SETTINGS_HEADERS)]

    def _members_headers(self) -> List[str]:
        ws = self.ws(self.cfg.members_sheet)
        values = ws.get_all_values()
        return [str(x).strip() for x in (values[0] if values else DEFAULT_MEMBERS_HEADERS)]

    def _ledger_headers(self) -> List[str]:
        ws = self.ws(self.cfg.ledger_sheet)
        values = ws.get_all_values()
        return [str(x).strip() for x in (values[0] if values else DEFAULT_LEDGER_HEADERS)]

    # ---- Members helpers ----
    def get_members(self) -> pd.DataFrame:
        df = self.read_df(self.cfg.members_sheet)
        for h in DEFAULT_MEMBERS_HEADERS:
            if h not in df.columns:
                df[h] = ""
        return df.fillna("")

    def upsert_member(self, person_name: str, project_name: str, line_user_id: str, display_name: str, is_active: bool) -> None:
        ws = self.ws(self.cfg.members_sheet)
        df = self.get_members()

        person_name = person_name.strip()
        project_name = project_name.strip()
        line_user_id = line_user_id.strip()

        headers = self._members_headers()
        required = DEFAULT_MEMBERS_HEADERS
        missing = [h for h in required if h not in headers]
        if missing:
            new_headers = headers + missing
            ws.update("1:1", [new_headers])
            headers = new_headers

        ts = jst_str()

        # find by Line_User_ID if exists else by PersonName+Project
        target_row = None
        values = ws.get_all_values()
        col = {h: i for i, h in enumerate(headers)}

        for r_i in range(2, len(values) + 1):
            row = values[r_i - 1]
            uid = row[col["Line_User_ID"]] if col["Line_User_ID"] < len(row) else ""
            pn = row[col["PersonName"]] if col["PersonName"] < len(row) else ""
            pr = row[col["Project_Name"]] if col["Project_Name"] < len(row) else ""
            if line_user_id and uid == line_user_id:
                target_row = r_i
                break
            if (not line_user_id) and pn == person_name and pr == project_name:
                target_row = r_i
                break

        def set_cell(r: int, h: str, v: str):
            c = col[h] + 1
            ws.update_cell(r, c, v)

        if target_row is None:
            out = [""] * len(headers)
            out[col["PersonName"]] = person_name
            out[col["Project_Name"]] = project_name
            out[col["Line_User_ID"]] = line_user_id
            out[col["LINE_DisplayName"]] = display_name
            out[col["IsActive"]] = "TRUE" if is_active else "FALSE"
            out[col["CreatedAt_JST"]] = ts
            out[col["UpdatedAt_JST"]] = ts
            ws.append_row(out, value_input_option="USER_ENTERED")
        else:
            if person_name:
                set_cell(target_row, "PersonName", person_name)
            if project_name:
                set_cell(target_row, "Project_Name", project_name)
            if line_user_id:
                set_cell(target_row, "Line_User_ID", line_user_id)
            set_cell(target_row, "LINE_DisplayName", display_name)
            set_cell(target_row, "IsActive", "TRUE" if is_active else "FALSE")
            set_cell(target_row, "UpdatedAt_JST", ts)

    def members_for_project(self, project_name: str) -> pd.DataFrame:
        df = self.get_members()
        df = df.fillna("")
        df = df[df["Project_Name"].astype(str) == str(project_name)]
        # IsActive filter (default TRUE if blank)
        def active_flag(x: str) -> bool:
            if str(x).strip() == "":
                return True
            return is_truthy(x)

        df = df[df["IsActive"].apply(active_flag)]
        return df

    # ---- Ledger + balances ----
    def get_ledger(self) -> pd.DataFrame:
        df = self.read_df(self.cfg.ledger_sheet)
        for h in DEFAULT_LEDGER_HEADERS:
            if h not in df.columns:
                df[h] = ""
        return df.fillna("")

    def append_ledger(self, row: Dict[str, Any]) -> None:
        self.append_row(self.cfg.ledger_sheet, row, self._ledger_headers())

    def project_current_total(self, project_name: str) -> float:
        """Settings.TotalPrincipal を初期値として、Ledgerの Deposit/Withdraw/APR を反映した“現在総額”を返す"""
        settings = self.get_settings()
        settings = settings.fillna("")
        base = 0.0
        m = settings[settings["Project_Name"].astype(str) == str(project_name)]
        if not m.empty:
            base = to_f(m.iloc[0].get("TotalPrincipal", 0))

        led = self.get_ledger()
        led = led.fillna("")
        led = led[led["Project_Name"].astype(str) == str(project_name)]
        if led.empty:
            return base

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


# -----------------------------
# APR calc
# -----------------------------
def calc_apr_daily_amount(total_principal: float, apr_percent: float, receive_factor: float) -> float:
    # 例: apr=20% → effective=13.4% → daily = principal*(0.134)/365
    effective_apr = apr_percent * receive_factor
    daily_rate = (effective_apr / 100.0) / 365.0
    return total_principal * daily_rate


# -----------------------------
# UI
# -----------------------------
def ui_show_headers():
    st.write("### シートヘッダー（コピペ用）")
    st.write("**Settings**")
    st.code("\t".join(DEFAULT_SETTINGS_HEADERS))
    st.write("**Members**")
    st.code("\t".join(DEFAULT_MEMBERS_HEADERS))
    st.write("**Ledger**")
    st.code("\t".join(DEFAULT_LEDGER_HEADERS))


def ui_admin(gs: GSheets):
    st.subheader("⚙️ 管理（管理者のみ）")
    admin_login_ui()
    if not is_admin():
        st.info("管理者パスワードを入力すると編集ができます。")
        st.stop()

    st.success("管理者モード")
    st.divider()
    ui_show_headers()

    # --- Project settings ---
    st.write("## 📌 Projects（Settings）")
    settings_df = gs.get_settings()
    st.dataframe(settings_df, use_container_width=True, hide_index=True)

    with st.form("project_upsert"):
        st.write("### プロジェクト追加/更新")
        project_name = st.text_input("Project_Name", placeholder="例: プロジェクトA")
        total_principal = st.number_input("TotalPrincipal", min_value=0.0, value=0.0, step=1000.0)
        currency = st.text_input("Currency", value="JPY")
        is_compound = st.checkbox("IsCompound（複利）", value=True)
        receive_factor = st.number_input("ReceiveFactor（受取率）", min_value=0.0, max_value=1.0, value=0.67, step=0.01)
        ok = st.form_submit_button("保存")
    if ok:
        if not project_name.strip():
            st.error("Project_Name は必須です。")
        else:
            gs.upsert_project(project_name, float(total_principal), currency, bool(is_compound), float(receive_factor))
            st.success("保存しました")
            st.rerun()

    st.divider()

    # --- Member management ---
    st.write("## 👤 Members 管理（個人名↔LINE紐付け）")
    members_df = gs.get_members()
    st.dataframe(members_df, use_container_width=True, hide_index=True)

    proj_list = settings_df["Project_Name"].dropna().astype(str).unique().tolist() if not settings_df.empty else []
    with st.form("member_upsert"):
        person = st.text_input("PersonName", placeholder="例: 祥子")
        project = st.selectbox("Project_Name", options=[""] + proj_list)
        line_uid = st.text_input("Line_User_ID", placeholder="Uxxxxxxxxxxxxxxxx")
        disp = st.text_input("LINE_DisplayName", placeholder="LINE表示名（任意）")
        active = st.checkbox("IsActive", value=True)
        ok2 = st.form_submit_button("保存（Upsert）")
    if ok2:
        if not person.strip() or not project.strip():
            st.error("PersonName と Project_Name は必須です。")
        else:
            gs.upsert_member(person, project, line_uid, disp, active)
            st.success("保存しました")
            st.rerun()


def ui_apr(gs: GSheets):
    st.subheader("📈 APR（本日の収益）計算 → 台帳記録 → 全員へ一斉LINE")

    settings_df = gs.get_settings()
    if settings_df.empty:
        st.error("Settings が空です。管理タブでプロジェクトを作成してください。")
        st.stop()

    projects = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    project = st.selectbox("プロジェクト", options=projects)

    row = settings_df[settings_df["Project_Name"].astype(str) == str(project)].iloc[0]
    currency = str(row.get("Currency", "JPY")).strip() or "JPY"
    is_compound = is_truthy(row.get("IsCompound", "TRUE"))
    receive_factor = to_f(row.get("ReceiveFactor", 0.67))
    if receive_factor <= 0:
        receive_factor = 0.67

    # 現在総額（ledger反映）
    current_total = gs.project_current_total(project)

    st.info(
        f"現在総額（自動計算）: {current_total:,.2f} {currency}\n"
        f"複利: {'ON' if is_compound else 'OFF'} / 受取率: {receive_factor:.2f}"
    )

    members = gs.members_for_project(project)
    if members.empty:
        st.warning("このプロジェクトのメンバーがいません（Membersで Project_Name を設定してください）。")
        st.stop()

    # 送信先（Line_User_ID）
    token = safe_str(st.secrets.get("line", {}).get("channel_access_token", ""))
    if not token:
        st.warning("Secrets の [line].channel_access_token が未設定です（LINE送信なしで計算/記録は可能）。")

    member_uids = [str(x).strip() for x in members["Line_User_ID"].tolist() if str(x).strip().startswith("U")]
    member_uids = uniq_keep_order(member_uids)

    st.caption(f"参加人数: {len(members)} / LINE送信先ID数: {len(member_uids)}")

    apr_percent = st.number_input("本日のAPR（年利, %）", value=20.0, step=0.1)

    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])
    if uploaded:
        st.image(uploaded, width=380)

    # 計算
    total_daily = calc_apr_daily_amount(current_total, apr_percent, receive_factor)
    per_person = total_daily / max(1, len(members))

    st.write("### 計算結果")
    st.metric("本日のプロジェクト合計収益（推定）", f"{total_daily:,.4f} {currency}")
    st.metric("1人あたり（均等配分）", f"{per_person:,.4f} {currency}")

    if st.button("✅ APRを台帳に記録して、全員へ一斉LINE送信", use_container_width=True):
        evidence_url = ""
        if uploaded:
            with st.spinner("画像をURL化中（ImgBB）..."):
                url = upload_imgbb(uploaded.getvalue())
            if not url:
                st.error("ImgBBアップロードに失敗しました。画像なしで続行するなら画像を外して再実行してください。")
                st.stop()
            evidence_url = url

        # 台帳に「人数分」記録（personごとにAPR行を入れる）
        dt = jst_str()
        for _, m in members.iterrows():
            person = str(m.get("PersonName", "")).strip()
            uid = str(m.get("Line_User_ID", "")).strip()
            disp = str(m.get("LINE_DisplayName", "")).strip()

            gs.append_ledger(
                {
                    "Datetime_JST": dt,
                    "Project_Name": project,
                    "PersonName": person,
                    "Type": "APR",
                    "Amount": round(per_person, 6),
                    "Currency": currency,
                    "Note": f"APR:{apr_percent}%, Receive:{receive_factor}",
                    "Line_User_ID": uid,
                    "LINE_DisplayName": disp,
                    "Source": "app",
                    "Evidence_URL": evidence_url,
                }
            )

        # Settings.TotalPrincipal を自動更新（APR分を増やす）
        # ※ “現在総額”は ledgerから算出してるので本来は更新不要だが、ユーザー要望で更新も行う
        new_total = current_total + total_daily
        gs.update_project_total(project, new_total)

        # 一斉LINE（個人名は入れない）
        if token and member_uids:
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
            for uid in member_uids:
                code = send_line_push(token, uid, msg, image_url=(evidence_url or None))
                if code == 200:
                    ok += 1
                else:
                    ng += 1
            st.success(f"送信完了：成功 {ok} / 失敗 {ng}")

        st.success("APR記録・TotalPrincipal更新が完了しました。")
        st.rerun()


def ui_cash(gs: GSheets):
    st.subheader("💸 入金 / 出金（個人へLINE通知）")

    settings_df = gs.get_settings()
    if settings_df.empty:
        st.error("Settings が空です。")
        st.stop()

    projects = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    project = st.selectbox("プロジェクト", options=projects, key="cash_project")

    row = settings_df[settings_df["Project_Name"].astype(str) == str(project)].iloc[0]
    currency_default = str(row.get("Currency", "JPY")).strip() or "JPY"

    members = gs.members_for_project(project)
    if members.empty:
        st.warning("このプロジェクトのメンバーがいません。")
        st.stop()

    labels = members["PersonName"].astype(str).tolist()
    person = st.selectbox("メンバー（PersonName）", options=labels)

    mrow = members[members["PersonName"].astype(str) == str(person)].iloc[0]
    uid = str(mrow.get("Line_User_ID", "")).strip()
    disp = str(mrow.get("LINE_DisplayName", "")).strip()

    # 現在総額
    current_total = gs.project_current_total(project)
    st.info(f"現在総額（自動計算）: {current_total:,.2f} {currency_default}")

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
            st.stop()

        dt_s = dt_fixed.strftime("%Y-%m-%d %H:%M:%S")

        # Ledger記録
        gs.append_ledger(
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
            }
        )

        # Settings.TotalPrincipal を自動更新（入出金反映）
        new_total = current_total + float(amount) if typ == "Deposit" else current_total - float(amount)
        gs.update_project_total(project, new_total)

        # 個人へLINE通知（入金/出金）
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


def ui_ledger_view(gs: GSheets):
    st.subheader("📒 Ledger（台帳）")
    df = gs.get_ledger()
    if df.empty:
        st.info("Ledger が空です。")
        return

    df = df.fillna("")
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.write("### フィルタ")
    c1, c2, c3 = st.columns(3)
    with c1:
        proj = st.selectbox("Project_Name", options=["(all)"] + sorted(df["Project_Name"].unique().tolist()))
    with c2:
        typ = st.selectbox("Type", options=["(all)"] + sorted(df["Type"].unique().tolist()))
    with c3:
        person = st.selectbox("PersonName", options=["(all)"] + sorted(df["PersonName"].unique().tolist()))

    f = df.copy()
    if proj != "(all)":
        f = f[f["Project_Name"] == proj]
    if typ != "(all)":
        f = f[f["Type"] == typ]
    if person != "(all)":
        f = f[f["PersonName"] == person]

    st.dataframe(f, use_container_width=True, hide_index=True)


# -----------------------------
# Main
# -----------------------------
def main():
    st.set_page_config(page_title="APR資産運用管理システム", layout="wide", page_icon="🏦")
    st.title("🏦 APR資産運用管理システム（完全版）")

    gs = GSheets(GSheetsConfig(spreadsheet_id=""))

    tab1, tab2, tab3, tab4 = st.tabs(
        ["📈 APR報告（一斉送信）", "💸 入金/出金（個別通知）", "📒 台帳", "⚙️ 管理（管理者）"]
    )

    with tab1:
        ui_apr(gs)

    with tab2:
        ui_cash(gs)

    with tab3:
        ui_ledger_view(gs)

    with tab4:
        ui_admin(gs)


if __name__ == "__main__":
    main()
