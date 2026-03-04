# app.py
# APR資産運用管理システム（完全版）
# - Google Sheets: connections.gsheets 方式（Streamlit Secrets）
# - シート: Settings / Members / Ledger
# - 管理者のみ: メンバー管理（追加・IsActive切替）/ Settings閲覧
# - メンバー追加仕様: Line_User_ID が既に存在する場合は「更新しない（スキップ）」
# - 入金/出金: 個人にLINE通知（PersonNameで送る）
# - APR確定: 全員にLINE通知（個人名は入れない、No.ごとの金額のみ）
# - 画像添付: ImgBBにアップ→LINEに画像添付（任意）
# - 429対策: 読み取りキャッシュ + 書き込み後はキャッシュクリア、不要な rerun を抑制
#
# 必要な Secrets（Streamlit Cloud > Settings > Secrets）
#
# [admin]
# password = "あなたの管理者パス"
#
# [connections.gsheets]
# spreadsheet = "スプレッドシートIDまたはURL"
#
# [connections.gsheets.credentials]
# type = "service_account"
# project_id = "..."
# private_key_id = "..."
# private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
# client_email = "xxxx@xxxx.iam.gserviceaccount.com"
# client_id = "..."
# token_uri = "https://oauth2.googleapis.com/token"
#
# [line]
# channel_access_token = "LINE Channel Access Token"
#
# [imgbb]  # 任意（画像を使うなら）
# api_key = "ImgBB API KEY"


from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials


# =========================
# Timezone / Format
# =========================
JST = timezone(timedelta(hours=9), "JST")


def now_jst() -> datetime:
    return datetime.now(JST)


def fmt_dt_jst(dt: datetime) -> str:
    return dt.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")


def money(x: float) -> str:
    # 通貨表記は $ 固定
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "$0.00"


# =========================
# Parsing helpers
# =========================
def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def to_f(x: Any) -> float:
    try:
        s = str(x).replace(",", "").replace("$", "").replace("%", "").strip()
        return float(s) if s else 0.0
    except Exception:
        return 0.0


def split_csv(val: Any, n: int) -> List[str]:
    s = safe_str(val).strip()
    if not s:
        return ["0"] * n
    items = [x.strip() for x in s.split(",") if x.strip() != ""]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]


def split_names(val: Any, n: int) -> List[str]:
    s = safe_str(val).strip()
    if not s:
        return [f"No.{i+1}" for i in range(n)]
    items = [x.strip() for x in re.split(r"[,\n]+", s) if x.strip()]
    while len(items) < n:
        items.append(f"No.{len(items)+1}")
    return items[:n]


def truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = safe_str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "はい", "on")


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\u3000", " ", regex=False)  # 全角スペース→半角
        .str.strip()
    )
    return df


# =========================
# LINE / ImgBB
# =========================
def send_line(token: str, user_id: str, text: str, image_url: Optional[str] = None) -> int:
    if not user_id or str(user_id).strip() == "":
        return 400

    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    messages: List[Dict[str, Any]] = [{"type": "text", "text": text}]
    if image_url:
        messages.append(
            {
                "type": "image",
                "originalContentUrl": image_url,
                "previewImageUrl": image_url,
            }
        )

    payload = {"to": str(user_id), "messages": messages}
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
        return int(r.status_code)
    except Exception:
        return 500


def upload_imgbb(file_bytes: bytes) -> Optional[str]:
    key = st.secrets.get("imgbb", {}).get("api_key", "")
    if not key:
        return None
    try:
        res = requests.post(
            "https://api.imgbb.com/1/upload",
            params={"key": key},
            files={"image": file_bytes},
            timeout=30,
        )
        data = res.json()
        return data["data"]["url"]
    except Exception:
        return None


# =========================
# Admin Auth (password)
# =========================
def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))


def admin_gate_ui() -> None:
    pw_required = safe_str(st.secrets.get("admin", {}).get("password", ""))
    if not pw_required:
        st.warning("Secrets に admin.password が未設定です（管理機能を保護できません）。")
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
        return

    with st.form("admin_login_form", clear_on_submit=False):
        pw = st.text_input("管理者パスワード", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if pw == pw_required:
                st.session_state["admin_ok"] = True
                st.success("管理者ログインに成功しました。")
            else:
                st.session_state["admin_ok"] = False
                st.error("パスワードが違います。")


# =========================
# Google Sheets Client
# =========================
@dataclass
class GSheetsConfig:
    spreadsheet_id: str
    settings_sheet: str = "Settings"
    members_sheet: str = "Members"
    ledger_sheet: str = "Ledger"


DEFAULT_SETTINGS_HEADERS = [
    "Project_Name",
    "Num_People",
    "TotalPrincipal",
    "IndividualPrincipals",
    "ProfitRates",
    "IsCompound",
    "MemberNames",
]
# ※ LineIDは Settings に入れてもOKですが、本システムでは Members を送信先の基準にします。


DEFAULT_MEMBERS_HEADERS = [
    "PersonName",
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
    "Type",            # Deposit / Withdraw / APR
    "Amount",          # 当該イベント金額
    "Balance_After",   # 反映後の元本（個人）
    "APR_Percent",     # APRの時だけ入る
    "Net_Factor",      # 0.67 固定など
    "Note",
    "Line_User_ID",
    "LINE_DisplayName",
    "Image_URL",
]


def _extract_sheet_id_or_url(s: str) -> str:
    s = s.strip()
    if "/spreadsheets/d/" in s:
        try:
            return s.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception:
            return s
    return s


def load_cfg() -> Optional[GSheetsConfig]:
    con = st.secrets.get("connections", {}).get("gsheets", {})
    sid = safe_str(con.get("spreadsheet", "")).strip()
    if not sid:
        return None
    sid = _extract_sheet_id_or_url(sid)
    return GSheetsConfig(spreadsheet_id=sid)


class GSheets:
    def __init__(self, cfg: GSheetsConfig):
        self.cfg = cfg

        con = st.secrets.get("connections", {}).get("gsheets", {})
        creds_info = con.get("credentials", None)
        if not creds_info:
            st.error('Secrets に [connections.gsheets.credentials] がありません。')
            st.stop()

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(dict(creds_info), scopes=scopes)
        self.gc = gspread.authorize(creds)
        self.book = self.gc.open_by_key(self.cfg.spreadsheet_id)

        # 起動時に最低限のシートだけ保証（429対策のため、読み取りは必要最小限）
        self._ensure_sheet(self.cfg.settings_sheet, DEFAULT_SETTINGS_HEADERS)
        self._ensure_sheet(self.cfg.members_sheet, DEFAULT_MEMBERS_HEADERS)
        self._ensure_sheet(self.cfg.ledger_sheet, DEFAULT_LEDGER_HEADERS)

    def _ensure_sheet(self, name: str, headers: List[str]) -> None:
        try:
            ws = self.book.worksheet(name)
        except Exception:
            ws = self.book.add_worksheet(title=name, rows=2000, cols=max(20, len(headers) + 5))

        # 1行目を確認して、空ならヘッダー作成（get_all_valuesを避けて1行目だけ取得）
        row1 = ws.row_values(1)
        if not row1:
            ws.update("1:1", [headers], value_input_option="USER_ENTERED")
            return

        # ヘッダー不足があれば追加
        existing = [c.strip() for c in row1 if str(c).strip() != ""]
        if not existing:
            ws.update("1:1", [headers], value_input_option="USER_ENTERED")
            return

        missing = [h for h in headers if h not in existing]
        if missing:
            new_headers = existing + missing
            ws.update("1:1", [new_headers], value_input_option="USER_ENTERED")

    def ws(self, name: str):
        return self.book.worksheet(name)

    # 429対策：読取はキャッシュ（ttl長め）
    @st.cache_data(ttl=180)
    def read_df(_self, sheet_name: str) -> pd.DataFrame:
        ws = _self.ws(sheet_name)
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        header = values[0]
        rows = values[1:]
        df = pd.DataFrame(rows, columns=header)
        df = normalize_cols(df).fillna("")
        return df

    def clear_cache(self) -> None:
        try:
            st.cache_data.clear()
        except Exception:
            pass

    def append_row(self, sheet_name: str, row: List[Any]) -> None:
        ws = self.ws(sheet_name)
        ws.append_row([safe_str(x) for x in row], value_input_option="USER_ENTERED")

    # ===== Members: 仕様変更 =====
    # Line_User_IDが既に存在する場合は「更新しない（スキップ）」
    def add_member_skip_if_exists(self, person_name: str, line_user_id: str, line_display_name: str) -> str:
        ws = self.ws(self.cfg.members_sheet)

        values = ws.get_all_values()
        if not values:
            ws.append_row(DEFAULT_MEMBERS_HEADERS, value_input_option="USER_ENTERED")
            values = [DEFAULT_MEMBERS_HEADERS]

        headers = values[0]
        col = {h: i for i, h in enumerate(headers)}

        # 必須列を保証
        required = DEFAULT_MEMBERS_HEADERS
        missing = [h for h in required if h not in col]
        if missing:
            ws.update("1:1", [headers + missing], value_input_option="USER_ENTERED")
            headers = headers + missing
            col = {h: i for i, h in enumerate(headers)}

        # 既存チェック（Line_User_ID一致ならスキップ）
        for r_i in range(2, len(values) + 1):
            row = values[r_i - 1]
            uid = row[col["Line_User_ID"]] if col["Line_User_ID"] < len(row) else ""
            if uid == line_user_id:
                return "skipped"

        ts = fmt_dt_jst(now_jst())
        out = [""] * len(headers)
        out[col["PersonName"]] = person_name
        out[col["Line_User_ID"]] = line_user_id
        out[col["LINE_DisplayName"]] = line_display_name
        out[col["IsActive"]] = "TRUE"
        out[col["CreatedAt_JST"]] = ts
        out[col["UpdatedAt_JST"]] = ts
        ws.append_row(out, value_input_option="USER_ENTERED")
        return "inserted"

    def get_member_by_person(self, person_name: str) -> Optional[Dict[str, str]]:
        df = self.read_df(self.cfg.members_sheet)
        if df.empty:
            return None
        if "PersonName" not in df.columns:
            return None
        m = df[df["PersonName"].astype(str) == str(person_name)]
        if m.empty:
            return None
        r = m.iloc[0].to_dict()
        return {k: safe_str(v) for k, v in r.items()}

    def active_line_ids(self) -> List[str]:
        df = self.read_df(self.cfg.members_sheet)
        if df.empty:
            return []
        for c in DEFAULT_MEMBERS_HEADERS:
            if c not in df.columns:
                return []
        df = df.fillna("")
        def to_bool(x):
            s = str(x).strip().upper()
            return s in ["TRUE", "1", "YES", "Y", "はい", "ON"]
        active = df[df["IsActive"].apply(to_bool)]
        ids = []
        for uid in active["Line_User_ID"].tolist():
            s = str(uid).strip()
            if s.startswith("U"):
                ids.append(s)
        # uniq preserve order
        seen, out = set(), []
        for x in ids:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    # ===== Settings =====
    def read_settings(self) -> pd.DataFrame:
        df = self.read_df(self.cfg.settings_sheet)
        return df

    def update_project_principals(
        self,
        project_name: str,
        num_people: int,
        principals: List[float],
    ) -> None:
        """
        Settings の TotalPrincipal / IndividualPrincipals を更新（入金/出金/APRで自動更新用）
        """
        ws = self.ws(self.cfg.settings_sheet)
        values = ws.get_all_values()
        if not values:
            ws.append_row(DEFAULT_SETTINGS_HEADERS, value_input_option="USER_ENTERED")
            values = [DEFAULT_SETTINGS_HEADERS]

        headers = normalize_cols(pd.DataFrame(columns=values[0])).columns.tolist()
        col = {h: i for i, h in enumerate(headers)}

        need = ["Project_Name", "Num_People", "TotalPrincipal", "IndividualPrincipals"]
        missing = [h for h in need if h not in col]
        if missing:
            ws.update("1:1", [headers + missing], value_input_option="USER_ENTERED")
            headers = headers + missing
            col = {h: i for i, h in enumerate(headers)}

        # 行探索（Project_Name一致）
        target_row = None
        for r in range(2, len(values) + 1):
            row = values[r - 1]
            pn = row[col["Project_Name"]] if col["Project_Name"] < len(row) else ""
            if str(pn) == str(project_name):
                target_row = r
                break
        if target_row is None:
            # 新規行追加
            out = [""] * len(headers)
            out[col["Project_Name"]] = str(project_name)
            out[col["Num_People"]] = str(num_people)
            out[col["TotalPrincipal"]] = str(sum(principals))
            out[col["IndividualPrincipals"]] = ",".join([str(round(x, 6)) for x in principals])
            ws.append_row(out, value_input_option="USER_ENTERED")
            return

        # まとめ更新（最小API）
        updates = []
        updates.append((target_row, col["Num_People"] + 1, str(num_people)))
        updates.append((target_row, col["TotalPrincipal"] + 1, str(sum(principals))))
        updates.append((target_row, col["IndividualPrincipals"] + 1, ",".join([str(round(x, 6)) for x in principals])))

        for r, c, v in updates:
            ws.update_cell(r, c, v)


# =========================
# UI helpers
# =========================
def require_line_token() -> str:
    token = safe_str(st.secrets.get("line", {}).get("channel_access_token", "")).strip()
    if not token:
        st.error("Secrets に [line].channel_access_token がありません。")
        st.stop()
    return token


def ui_members_admin(gs: GSheets) -> None:
    st.subheader("👤 メンバー管理（管理者のみ）")

    admin_gate_ui()
    if not is_admin():
        st.info("管理者ログインすると操作できます。")
        st.stop()

    # 一覧 + IsActiveトグル
    df = gs.read_df(gs.cfg.members_sheet).fillna("")
    if df.empty:
        st.info("Members シートが空です。")
        df = pd.DataFrame(columns=DEFAULT_MEMBERS_HEADERS)

    # IsActive列がなければ追加
    if "IsActive" not in df.columns:
        df["IsActive"] = "TRUE"

    def to_bool(x):
        s = str(x).strip().upper()
        return s in ["TRUE", "1", "YES", "Y", "はい", "ON"]

    view = df.copy()
    view["IsActive"] = view["IsActive"].apply(to_bool)

    st.caption("Is Active を切り替えたら「保存」を押してください。")
    edited = st.data_editor(
        view,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={"IsActive": st.column_config.CheckboxColumn("Is Active")},
        disabled=[c for c in view.columns if c != "IsActive"],
        key="members_isactive_editor",
    )

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Is Active を保存", type="primary"):
            out = df.copy()
            out["IsActive"] = edited["IsActive"].apply(lambda b: "TRUE" if bool(b) else "FALSE")
            ws = gs.ws(gs.cfg.members_sheet)
            ws.clear()
            ws.update([out.columns.tolist()] + out.astype(str).values.tolist(), value_input_option="USER_ENTERED")
            gs.clear_cache()
            st.success("保存しました。")

    with c2:
        if st.button("再読込"):
            gs.clear_cache()
            st.toast("再読込しました（キャッシュクリア）")

    st.divider()

    # 追加（仕様：Line_User_ID一致なら更新しない）
    st.write("### 新規メンバー追加（Line_User_IDが既に存在する場合は追加せずスキップ）")
    with st.form("add_member_form", clear_on_submit=True):
        person = st.text_input("PersonName（個人名）")
        line_uid = st.text_input("Line_User_ID（Uから始まる）")
        disp = st.text_input("LINE_DisplayName（任意）")
        ok = st.form_submit_button("追加")

    if ok:
        if not person.strip() or not line_uid.strip():
            st.error("PersonName と Line_User_ID は必須です。")
        else:
            res = gs.add_member_skip_if_exists(person.strip(), line_uid.strip(), disp.strip())
            gs.clear_cache()
            if res == "inserted":
                st.success("新規メンバーとして追加しました。")
            else:
                st.info("同じ Line_User_ID が既に存在するため、更新せずスキップしました。")


def ui_settings_readonly(gs: GSheets) -> None:
    st.subheader("⚙️ Settings（確認用 / 管理者のみ）")
    admin_gate_ui()
    if not is_admin():
        st.info("管理者ログインすると表示できます。")
        st.stop()
    df = gs.read_settings()
    if df.empty:
        st.info("Settings シートが空です。")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)


def get_project_row(settings_df: pd.DataFrame, project: str) -> pd.Series:
    sdf = settings_df.copy()
    sdf = normalize_cols(sdf).fillna("")
    if "Project_Name" not in sdf.columns:
        raise ValueError("Settings に Project_Name 列がありません。")
    m = sdf[sdf["Project_Name"].astype(str) == str(project)]
    if m.empty:
        raise ValueError(f"Settings にプロジェクト '{project}' が見つかりません。")
    return m.iloc[0]


def compute_today_yields(
    principals: List[float],
    rates: List[float],
    apr_percent: float,
    net_factor: float = 0.67,
) -> List[float]:
    # 365日割、rateは “比率（例: 1,1,1）” として扱い、APR×net×rateで配分
    out = []
    for p, r in zip(principals, rates):
        y = (p * (apr_percent * net_factor * (r / 100.0))) / 365.0
        out.append(round(float(y), 6))
    return out


def ui_apr(gs: GSheets) -> None:
    st.subheader("📈 APR（収益確定 → 全員へLINE送信）")

    # Settings読取（キャッシュされる）
    settings_df = gs.read_settings()
    if settings_df.empty:
        st.error("Settings シートが空です。")
        return
    settings_df = normalize_cols(settings_df).fillna("")

    if "Project_Name" not in settings_df.columns:
        st.error("Settings に Project_Name 列がありません。")
        return

    project_list = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    if not project_list:
        st.error("Settings に Project_Name がありません。")
        return

    project = st.selectbox("プロジェクト", project_list, key="apr_project")

    p = get_project_row(settings_df, project)

    # 必須列
    required = ["Num_People", "IndividualPrincipals", "ProfitRates", "IsCompound", "MemberNames"]
    missing = [c for c in required if c not in settings_df.columns]
    if missing:
        st.error(f"Settings の列が不足しています: {missing}")
        return

    num_people = int(to_f(p["Num_People"]))
    member_names = split_names(p["MemberNames"], num_people)
    principals = [to_f(x) for x in split_csv(p["IndividualPrincipals"], num_people)]

    # ProfitRates は「%」扱いで、個別配分に使う（例: 100,100,100 なら等分）
    # 等分したいなら ProfitRates を全員100にしておくのが確実
    rates = [to_f(x) for x in split_csv(p["ProfitRates"], num_people)]
    is_compound = truthy(p["IsCompound"])

    st.caption(f"計算モード: {'複利（収益を元本に反映）' if is_compound else '単利（元本固定）'}")
    net_factor = 0.67  # 要望により 67%

    apr_percent = st.number_input("本日のAPR（%）", value=100.0, step=0.1, key="apr_percent")
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"], key="apr_image")
    if uploaded:
        st.image(uploaded, caption="送信プレビュー", width=420)

    today_yields = compute_today_yields(principals, rates, apr_percent, net_factor=net_factor)

    # 表示（個人名はUIではOK、LINEはNo.のみ）
    st.write("### 本日の収益（確認）")
    rows = []
    for i in range(num_people):
        rows.append(
            {
                "No": f"No.{i+1}",
                "Member": member_names[i],
                "Principal": money(principals[i]),
                "Yield": money(today_yields[i]),
            }
        )
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    if st.button("APRを確定して全員へLINE送信", type="primary"):
        token = require_line_token()

        # 画像URL（任意）
        image_url = None
        if uploaded:
            with st.spinner("画像をアップロード中（ImgBB）..."):
                image_url = upload_imgbb(uploaded.getvalue())
            if uploaded and not image_url:
                st.error("画像アップロードに失敗しました（ImgBB）。画像なしで続行する場合は画像を外してください。")
                return

        # 元本更新（複利のみ増える）
        new_principals = principals[:]
        for i in range(num_people):
            if is_compound:
                new_principals[i] = float(new_principals[i]) + float(today_yields[i])

        # Settingsへ自動反映（TotalPrincipal/IndividualPrincipals）
        gs.update_project_principals(project, num_people, new_principals)

        # Ledgerに記録（各人行）
        ts = fmt_dt_jst(now_jst())
        for i in range(num_people):
            # Members紐付け（通知先集計にも使うため、保存）
            m = gs.get_member_by_person(member_names[i])  # 名前がPersonNameと一致している運用が理想
            uid = safe_str(m.get("Line_User_ID")) if m else ""
            disp = safe_str(m.get("LINE_DisplayName")) if m else ""
            gs.append_row(
                gs.cfg.ledger_sheet,
                [
                    ts,
                    project,
                    member_names[i],
                    "APR",
                    today_yields[i],
                    new_principals[i] if is_compound else principals[i],
                    apr_percent,
                    net_factor,
                    f"APR確定",
                    uid,
                    disp,
                    image_url or "",
                ],
            )

        # 全員へ送信（個人名は入れない）
        active_ids = gs.active_line_ids()
        if not active_ids:
            st.warning("送信先（MembersでIsActive=TRUEかつLine_User_ID）がありません。")
        else:
            msg = "🏦 【APR 収益報告】\n"
            msg += f"プロジェクト: {project}\n"
            msg += f"報告日時: {fmt_dt_jst(now_jst())}\n"
            msg += f"本日のAPR: {apr_percent}%\n"
            msg += f"ネット係数: {int(net_factor*100)}%\n"
            msg += "\n💰 本日の配分（No.別）\n"
            for i in range(num_people):
                msg += f"・No.{i+1}: {money(today_yields[i])}\n"
            msg += "\n※個人名は記載していません。"
            if image_url:
                msg += "\n📎 エビデンス画像を添付します。"

            ok_cnt, ng_cnt = 0, 0
            for uid in active_ids:
                code = send_line(token, uid, msg, image_url=image_url)
                if code == 200:
                    ok_cnt += 1
                else:
                    ng_cnt += 1

            st.success(f"送信完了：成功 {ok_cnt} / 失敗 {ng_cnt}")

        # 書き込み後はキャッシュクリア（429 & 表示反映）
        gs.clear_cache()
        st.toast("確定しました（キャッシュクリア済み）。")


def ui_cashflow(gs: GSheets) -> None:
    st.subheader("💸 入金 / 出金（個人へLINE通知）")

    settings_df = gs.read_settings()
    if settings_df.empty:
        st.error("Settings シートが空です。")
        return
    settings_df = normalize_cols(settings_df).fillna("")
    if "Project_Name" not in settings_df.columns:
        st.error("Settings に Project_Name 列がありません。")
        return

    project_list = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    project = st.selectbox("プロジェクト", project_list, key="cf_project")

    p = get_project_row(settings_df, project)
    required = ["Num_People", "IndividualPrincipals", "MemberNames"]
    missing = [c for c in required if c not in settings_df.columns]
    if missing:
        st.error(f"Settings の列が不足しています: {missing}")
        return

    num_people = int(to_f(p["Num_People"]))
    member_names = split_names(p["MemberNames"], num_people)
    principals = [to_f(x) for x in split_csv(p["IndividualPrincipals"], num_people)]

    # 送信先
    token = require_line_token()

    # 入金/出金は同タブ
    typ = st.radio("種別", ["Deposit（入金）", "Withdraw（出金）"], horizontal=True)
    person = st.selectbox("個人（MemberNames）", member_names, key="cf_person")
    idx = member_names.index(person)

    # 現在残高
    st.info(f"現在元本: {money(principals[idx])}")

    amt = st.number_input("金額（$）", min_value=0.0, step=100.0, key="cf_amt")
    note = st.text_input("メモ（任意）", value="", key="cf_note")

    if st.button("確定（記録＋個人へLINE通知）", type="primary"):
        if amt <= 0:
            st.warning("金額が0です。")
            return

        is_withdraw = typ.startswith("Withdraw")
        delta = -float(amt) if is_withdraw else float(amt)

        new_principals = principals[:]
        new_principals[idx] = float(new_principals[idx]) + delta

        # 負になっても記録はできるが、UI崩壊防止のため警告
        if new_principals[idx] < 0:
            st.warning("結果の元本がマイナスになります（運用上OKなら続行）。")

        # Settingsへ自動反映
        gs.update_project_principals(project, num_people, new_principals)

        # MembersからLINE紐付け
        m = gs.get_member_by_person(person)
        line_uid = safe_str(m.get("Line_User_ID")) if m else ""
        disp = safe_str(m.get("LINE_DisplayName")) if m else ""

        # Ledgerへ記録
        ts = fmt_dt_jst(now_jst())
        event_type = "Withdraw" if is_withdraw else "Deposit"
        gs.append_row(
            gs.cfg.ledger_sheet,
            [
                ts,
                project,
                person,
                event_type,
                float(amt),
                new_principals[idx],
                "",
                "",
                note,
                line_uid,
                disp,
                "",
            ],
        )

        # 個人へLINE通知（Line_User_IDが無い場合は通知できない）
        if line_uid and line_uid.startswith("U"):
            msg = "🏦 【入出金通知】\n"
            msg += f"プロジェクト: {project}\n"
            msg += f"日時: {ts}\n"
            msg += f"種別: {'出金' if is_withdraw else '入金'}\n"
            msg += f"金額: {money(amt)}\n"
            msg += f"反映後元本: {money(new_principals[idx])}\n"
            if note.strip():
                msg += f"\nメモ: {note.strip()}"
            code = send_line(token, line_uid, msg)
            if code == 200:
                st.success("記録し、本人へLINE通知しました。")
            else:
                st.warning(f"記録しましたが、LINE送信に失敗しました（HTTP {code}）。")
        else:
            st.success("記録しました（この個人はLine_User_ID未登録のためLINE通知なし）。")

        gs.clear_cache()
        st.toast("反映しました（キャッシュクリア済み）。")


def ui_ledger_view(gs: GSheets) -> None:
    st.subheader("📒 台帳（Ledger）閲覧")

    df = gs.read_df(gs.cfg.ledger_sheet).fillna("")
    if df.empty:
        st.info("Ledger が空です。")
        return

    st.dataframe(df, use_container_width=True, hide_index=True)

    # 簡易フィルタ
    st.write("### フィルタ")
    cols = st.columns(4)
    with cols[0]:
        proj = st.selectbox("Project", ["(all)"] + sorted(df.get("Project_Name", pd.Series([""])).unique().tolist()))
    with cols[1]:
        person = st.selectbox("Person", ["(all)"] + sorted(df.get("PersonName", pd.Series([""])).unique().tolist()))
    with cols[2]:
        typ = st.selectbox("Type", ["(all)"] + sorted(df.get("Type", pd.Series([""])).unique().tolist()))
    with cols[3]:
        if st.button("再読込（キャッシュクリア）"):
            gs.clear_cache()
            st.toast("再読込しました。")

    f = df.copy()
    if proj != "(all)" and "Project_Name" in f.columns:
        f = f[f["Project_Name"] == proj]
    if person != "(all)" and "PersonName" in f.columns:
        f = f[f["PersonName"] == person]
    if typ != "(all)" and "Type" in f.columns:
        f = f[f["Type"] == typ]

    st.dataframe(f, use_container_width=True, hide_index=True)


# =========================
# Main
# =========================
def main():
    st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")
    st.title("🏦 APR資産運用管理システム")

    cfg = load_cfg()
    if cfg is None:
        st.error("Secrets の [connections.gsheets].spreadsheet が未設定です。")
        st.stop()

    # 429が出ている間は、起動直後のreadを増やさないため、ここでの読取はしない
    try:
        gs = GSheets(cfg)
    except Exception as e:
        st.error(f"Spreadsheet を開けません。共有設定（編集者）とIDを確認してください。: {e}")
        st.stop()

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "📈 APR（全員へ送信）",
            "💸 入金/出金（個人へ送信）",
            "📒 台帳（Ledger）",
            "⚙️ 管理（管理者のみ）",
        ]
    )

    with tab1:
        ui_apr(gs)

    with tab2:
        ui_cashflow(gs)

    with tab3:
        ui_ledger_view(gs)

    with tab4:
        # 管理者のみ：Members操作 / Settings確認
        admin_gate_ui()
        if not is_admin():
            st.info("管理者ログインすると操作できます。")
        else:
            st.success("管理者機能が有効です。")

        st.divider()
        if is_admin():
            ui_members_admin(gs)
            st.divider()
            ui_settings_readonly(gs)


if __name__ == "__main__":
    main()
