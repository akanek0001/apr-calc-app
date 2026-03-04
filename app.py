# app.py
# APR 管理システム（フル版 / 安定運用向け）
# - Google Sheets（gspread）: Settings / Members / Ledger を中央台帳として運用
# - 管理者PINログイン（Secrets: [admin].pin）
# - プロジェクト別 NetFactor（0.67 / 0.60 等）設定
# - APR計算 → 全員へLINE一斉送信（個人名は入れない）
# - 入金/出金 → 本人へLINE通知（PersonName ↔ Line_User_ID で紐付け）
# - 画像エビデンス（任意）: ImgBB → LINE添付
# - 429(Quota exceeded) 対策: st.cache_data TTL + まとめ読み + 最小読み込み
#
# ===== 必須 Secrets（Streamlit Cloud > Settings > Secrets）=====
# [admin]
# pin = "your-admin-pin"
#
# [connections.gsheets]
# spreadsheet = "https://docs.google.com/spreadsheets/d/<SPREADSHEET_ID>/edit"
# # or: spreadsheet = "<SPREADSHEET_ID>"
#
# [connections.gsheets.credentials]
# type = "service_account"
# project_id = "..."
# private_key_id = "..."
# private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
# client_email = "xxxxx@xxxxx.iam.gserviceaccount.com"
# client_id = "..."
# token_uri = "https://oauth2.googleapis.com/token"
#
# [line]
# channel_access_token = "..."
#
# [imgbb]
# api_key = "..."   # 画像を送る場合のみ
#
# ===============================================================

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import requests

import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# Timezone
# -----------------------------
JST = timezone(timedelta(hours=9), "JST")


def now_jst() -> datetime:
    return datetime.now(JST)


# -----------------------------
# Small utils
# -----------------------------
def s(x: Any) -> str:
    return "" if x is None else str(x)


def to_f(x: Any) -> float:
    try:
        v = str(x).replace(",", "").replace("$", "").replace("%", "").strip()
        return float(v) if v else 0.0
    except Exception:
        return 0.0


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    cols = []
    seen = {}
    for c in df.columns.astype(str).tolist():
        c = c.replace("\u3000", " ").strip()
        if c == "":
            c = "Unnamed"
        # duplicate header -> make unique
        if c in seen:
            seen[c] += 1
            c2 = f"{c}.{seen[c]}"
        else:
            seen[c] = 0
            c2 = c
        cols.append(c2)
    df.columns = cols
    return df


def extract_sheet_id(v: str) -> str:
    v = v.strip()
    if "/spreadsheets/d/" in v:
        try:
            return v.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception:
            return v
    return v


def only_line_user_ids(values: List[Any]) -> List[str]:
    out: List[str] = []
    for v in values:
        t = s(v).strip()
        if t.startswith("U") and len(t) >= 10:
            out.append(t)
    # unique keep order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


# -----------------------------
# LINE / ImgBB
# -----------------------------
def send_line_message(token: str, user_id: str, text: str, image_url: Optional[str] = None) -> int:
    if not user_id:
        return 400

    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    messages: List[Dict[str, Any]] = [{"type": "text", "text": text}]
    if image_url:
        messages.append({"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url})

    payload = {"to": user_id, "messages": messages}
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
        return r.status_code
    except Exception:
        return 500


def upload_imgbb(api_key: str, file_bytes: bytes) -> Optional[str]:
    try:
        res = requests.post(
            "https://api.imgbb.com/1/upload",
            params={"key": api_key},
            files={"image": file_bytes},
            timeout=30,
        )
        data = res.json()
        return data["data"]["url"]
    except Exception:
        return None


# -----------------------------
# Admin auth (PIN)
# -----------------------------
def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))


def admin_login_block() -> None:
    pin_required = s(st.secrets.get("admin", {}).get("pin", "")).strip()
    if not pin_required:
        st.warning("Secrets の [admin].pin が未設定です（管理者保護できません）。")
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
        pin = st.text_input("管理者PIN", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if pin == pin_required:
                st.session_state["admin_ok"] = True
                st.success("管理者ログインに成功しました。")
            else:
                st.session_state["admin_ok"] = False
                st.error("PINが違います。")


# -----------------------------
# GSheets config / client
# -----------------------------
@dataclass
class AppSheets:
    settings: str = "Settings"
    members: str = "Members"
    ledger: str = "Ledger"
    lineid: str = "LineID"  # fallback


DEFAULT_SETTINGS_HEADERS = [
    "Project_Name",     # プロジェクト名
    "NetFactor",        # 0.67 or 0.60 など
    "SplitMode",        # equal / proportional（デフォルト equal）
    "Currency",         # 例: USD (表示は $ 固定)
    "Active",           # TRUE/FALSE（プロジェクトのON/OFF）
]

DEFAULT_MEMBERS_HEADERS = [
    "Project_Name",
    "PersonName",
    "Line_User_ID",
    "LINE_DisplayName",
    "Active",           # TRUE/FALSE
    "CreatedAt_JST",
]

DEFAULT_LEDGER_HEADERS = [
    "Datetime_JST",
    "Project_Name",
    "PersonName",
    "Type",             # Deposit / Withdraw / APR
    "Amount",           # 正の数（Withdraw も正で保存）
    "Signed_Amount",    # Deposit:+ / Withdraw:- / APR:+
    "Balance_After",
    "Note",
    "Evidence_Image_URL",
]


@st.cache_resource(show_spinner=False)
def gs_client_from_secrets() -> gspread.Client:
    con = st.secrets.get("connections", {}).get("gsheets", {})
    creds_info = con.get("credentials", None)
    if not creds_info:
        raise RuntimeError("Secrets に [connections.gsheets.credentials] がありません。")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(dict(creds_info), scopes=scopes)
    return gspread.authorize(creds)


def open_spreadsheet() -> gspread.Spreadsheet:
    con = st.secrets.get("connections", {}).get("gsheets", {})
    sid = s(con.get("spreadsheet", "")).strip()
    if not sid:
        raise RuntimeError("Secrets の [connections.gsheets].spreadsheet が未設定です。")
    sid = extract_sheet_id(sid)
    gc = gs_client_from_secrets()
    return gc.open_by_key(sid)


def ensure_sheet(sp: gspread.Spreadsheet, title: str, headers: List[str]) -> None:
    try:
        ws = sp.worksheet(title)
        vals = ws.get_all_values()
        if not vals:
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return

        # ヘッダー重複や不足がある場合は“初回行だけ”矯正
        current = vals[0]
        current = [c.strip() for c in current]
        # duplicate fix
        seen = {}
        fixed = []
        changed = False
        for c in current:
            if c in seen:
                seen[c] += 1
                fixed.append(f"{c}.{seen[c]}")
                changed = True
            else:
                seen[c] = 0
                fixed.append(c)

        # add missing required columns (append to end)
        for h in headers:
            if h not in fixed:
                fixed.append(h)
                changed = True

        if changed:
            ws.update("1:1", [fixed])

    except gspread.WorksheetNotFound:
        ws = sp.add_worksheet(title=title, rows=2000, cols=max(10, len(headers) + 5))
        ws.append_row(headers, value_input_option="USER_ENTERED")


def _gs_read_all_values(ws: gspread.Worksheet) -> List[List[str]]:
    # 429対策（軽いバックオフ）
    for i in range(3):
        try:
            return ws.get_all_values()
        except gspread.exceptions.APIError as e:
            msg = s(e)
            if "429" in msg or "Quota exceeded" in msg:
                time.sleep(1.5 * (i + 1))
                continue
            raise
    # last try
    return ws.get_all_values()


@st.cache_data(ttl=20, show_spinner=False)
def read_sheet_df_cached(spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    gc = gs_client_from_secrets()
    sp = gc.open_by_key(spreadsheet_id)
    ws = sp.worksheet(sheet_name)
    values = _gs_read_all_values(ws)
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)
    df = clean_columns(df)
    return df


def cache_bust():
    # 書き込み後にキャッシュをクリアして最新を反映
    st.cache_data.clear()


# -----------------------------
# Domain logic: balances / APR
# -----------------------------
def members_for_project(df_members: pd.DataFrame, project: str, active_only: bool = True) -> pd.DataFrame:
    if df_members.empty:
        return df_members

    df = df_members.copy()
    for col in ["Project_Name", "PersonName", "Line_User_ID", "Active"]:
        if col not in df.columns:
            # absent column -> add
            df[col] = ""

    df = df[df["Project_Name"].astype(str) == str(project)]
    if active_only:
        df = df[df["Active"].astype(str).str.strip().str.upper().isin(["TRUE", "YES", "1", "はい", "ON"])]
    df = df.fillna("")
    return df


def compute_person_balance_from_ledger(df_ledger: pd.DataFrame, project: str, person: str) -> float:
    if df_ledger.empty:
        return 0.0

    df = df_ledger.copy().fillna("")
    required = ["Project_Name", "PersonName", "Signed_Amount"]
    for c in required:
        if c not in df.columns:
            return 0.0

    df = df[(df["Project_Name"].astype(str) == str(project)) & (df["PersonName"].astype(str) == str(person))]
    if df.empty:
        return 0.0
    return float(df["Signed_Amount"].apply(to_f).sum())


def compute_project_total_balance(df_ledger: pd.DataFrame, project: str, active_people: List[str]) -> float:
    total = 0.0
    for p in active_people:
        total += compute_person_balance_from_ledger(df_ledger, project, p)
    return total


def calc_daily_apr_distribution(
    project_total_balance: float,
    apr_percent: float,
    net_factor: float,
    people: List[str],
    split_mode: str,
    df_ledger: pd.DataFrame,
    project: str,
) -> Dict[str, float]:
    """
    1日分のAPR分配（USD表示のため $ 前提）。
    - equal: プロジェクト合計から日次利益を算出 → 人数で等分
    - proportional: 各人の残高比率で按分
    """
    if not people:
        return {}

    daily_profit_total = project_total_balance * (apr_percent / 100.0) * net_factor / 365.0
    daily_profit_total = round(daily_profit_total, 4)

    mode = (split_mode or "equal").strip().lower()
    if mode not in ("equal", "proportional"):
        mode = "equal"

    if mode == "equal":
        each = round(daily_profit_total / len(people), 4)
        return {p: each for p in people}

    # proportional
    balances = {p: max(0.0, compute_person_balance_from_ledger(df_ledger, project, p)) for p in people}
    denom = sum(balances.values())
    if denom <= 0:
        each = round(daily_profit_total / len(people), 4)
        return {p: each for p in people}

    out: Dict[str, float] = {}
    # rounding drift is OK for reporting; ledger stores per person
    for p in people:
        out[p] = round(daily_profit_total * (balances[p] / denom), 4)
    return out


# -----------------------------
# Writes: append ledger row with balance update
# -----------------------------
def append_ledger_row(
    spreadsheet_id: str,
    row: Dict[str, Any],
    settings_sheet: str,
    members_sheet: str,
    ledger_sheet: str,
) -> None:
    """
    429対策のため、書き込みは必要最小限で:
    - append_row で追加
    """
    gc = gs_client_from_secrets()
    sp = gc.open_by_key(spreadsheet_id)
    ws = sp.worksheet(ledger_sheet)

    # Ensure header has required columns (one-time)
    ensure_sheet(sp, ledger_sheet, DEFAULT_LEDGER_HEADERS)

    # Get header (single call)
    header = _gs_read_all_values(ws)[0]
    header = [h.strip() for h in header]
    # make sure row aligns with header order
    out = []
    for h in header:
        out.append(s(row.get(h, "")))
    ws.append_row(out, value_input_option="USER_ENTERED")


# -----------------------------
# Members: upsert policy
# - IMPORTANT: 「LINE User ID が一致したら更新しない」に変更（＝スキップ）
# -----------------------------
def add_member_if_new_line_user_id(
    spreadsheet_id: str,
    project: str,
    person_name: str,
    line_user_id: str,
    line_display_name: str,
    active: bool,
    members_sheet: str,
) -> Tuple[bool, str]:
    """
    Returns (created, message)
    Policy:
    - 既存に同一 Line_User_ID があれば「更新しない」= 何もしない（スキップ）
    - 既存に同一 (Project_Name + PersonName) があって Line_User_ID が空の場合は埋める…も禁止（更新しない）
      → とにかく “一致したら更新しない” を厳格に守る
    """
    gc = gs_client_from_secrets()
    sp = gc.open_by_key(spreadsheet_id)
    ensure_sheet(sp, members_sheet, DEFAULT_MEMBERS_HEADERS)
    ws = sp.worksheet(members_sheet)

    values = _gs_read_all_values(ws)
    if not values:
        ws.append_row(DEFAULT_MEMBERS_HEADERS, value_input_option="USER_ENTERED")
        values = [DEFAULT_MEMBERS_HEADERS]

    header = [h.strip() for h in values[0]]
    col = {h: i for i, h in enumerate(header)}
    # ensure required columns exist
    for h in DEFAULT_MEMBERS_HEADERS:
        if h not in col:
            header.append(h)
    if header != values[0]:
        ws.update("1:1", [header])
        col = {h: i for i, h in enumerate(header)}
        values = _gs_read_all_values(ws)

    # If Line_User_ID already exists anywhere -> skip
    for r in values[1:]:
        uid = r[col["Line_User_ID"]] if col["Line_User_ID"] < len(r) else ""
        if uid == line_user_id and uid:
            return (False, "同じ LINE User ID が既に存在するため、更新せずにスキップしました。")

    ts = now_jst().strftime("%Y-%m-%d %H:%M:%S")
    out_row = [""] * len(header)
    out_row[col["Project_Name"]] = project
    out_row[col["PersonName"]] = person_name
    out_row[col["Line_User_ID"]] = line_user_id
    out_row[col["LINE_DisplayName"]] = line_display_name
    out_row[col["Active"]] = "TRUE" if active else "FALSE"
    out_row[col["CreatedAt_JST"]] = ts
    ws.append_row(out_row, value_input_option="USER_ENTERED")
    return (True, "追加しました。")


# -----------------------------
# UI blocks
# -----------------------------
def ui_debug_quota_hint(e: Exception) -> None:
    msg = s(e)
    if "429" in msg or "Quota exceeded" in msg:
        st.error(
            "読み取り回数制限(429)に当たりました。\n"
            "このアプリはキャッシュで軽減していますが、短時間に連続操作すると発生します。\n"
            "対策: 連打しない / タブ切替を減らす / Ledger件数を増やしすぎない（必要なら月別に分ける）"
        )


def ui_project_selector(df_settings: pd.DataFrame) -> Tuple[str, float, str, str, bool]:
    """
    Returns:
      project_name, net_factor, split_mode, currency, project_active
    """
    df = df_settings.copy().fillna("")
    if "Project_Name" not in df.columns:
        st.error("Settings に Project_Name 列がありません。")
        st.stop()

    # Active列がなくても動く
    if "Active" not in df.columns:
        df["Active"] = "TRUE"

    projects = df[df["Project_Name"].astype(str).str.strip() != ""]
    if projects.empty:
        st.error("Settings にプロジェクトがありません。")
        st.stop()

    # ONのみを基本リストに（なければ全部）
    on_df = projects[projects["Active"].astype(str).str.strip().str.upper().isin(["TRUE", "YES", "1", "はい", "ON"])]
    use_df = on_df if not on_df.empty else projects

    project_list = use_df["Project_Name"].astype(str).tolist()
    project = st.sidebar.selectbox("プロジェクトを選択", project_list)

    row = projects[projects["Project_Name"].astype(str) == str(project)].iloc[0]

    net_factor = to_f(row.get("NetFactor", 0.67))
    if net_factor <= 0:
        net_factor = 0.67

    split_mode = s(row.get("SplitMode", "equal")).strip() or "equal"
    currency = s(row.get("Currency", "USD")).strip() or "USD"
    active = s(row.get("Active", "TRUE")).strip().upper() in ["TRUE", "YES", "1", "はい", "ON"]

    return project, net_factor, split_mode, currency, active


def ui_apr(
    spreadsheet_id: str,
    sheets: AppSheets,
    df_settings: pd.DataFrame,
    df_members: pd.DataFrame,
    df_ledger: pd.DataFrame,
    project: str,
    net_factor: float,
    split_mode: str,
) -> None:
    st.subheader("📈 APR（収益確定・画像付きLINE一斉送信）")
    st.caption("※ 本日の収益報告は全員に送るため、個人名はメッセージ本文に入れません。")

    members_active = members_for_project(df_members, project, active_only=True)
    people = members_active["PersonName"].astype(str).tolist() if not members_active.empty else []
    line_ids = members_active["Line_User_ID"].astype(str).tolist() if not members_active.empty else []
    line_ids = [x for x in line_ids if x.strip().startswith("U")]

    if not people:
        st.warning("このプロジェクトの Active メンバーがいません（Members を確認してください）。")
        return

    apr_percent = st.number_input("本日のAPR (%)", value=100.0, step=0.1)
    st.write(f"NetFactor: **{net_factor:.2f}**（Settingsでプロジェクト別に設定） / SplitMode: **{split_mode}**")

    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])
    if uploaded:
        st.image(uploaded, caption="送信プレビュー", width=420)

    # balances
    project_total_balance = compute_project_total_balance(df_ledger, project, people)

    # distribution
    dist = calc_daily_apr_distribution(
        project_total_balance=project_total_balance,
        apr_percent=apr_percent,
        net_factor=net_factor,
        people=people,
        split_mode=split_mode,
        df_ledger=df_ledger,
        project=project,
    )

    st.write("### 本日の分配（プレビュー）")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("プロジェクト合計残高", f"${project_total_balance:,.2f}")
    with col2:
        total_profit = sum(dist.values())
        st.metric("本日の合計利益（合計）", f"${total_profit:,.4f}")
    with col3:
        st.metric("人数", f"{len(people)}")

    preview_df = pd.DataFrame({"PersonName": list(dist.keys()), "APR_Profit": list(dist.values())})
    st.dataframe(preview_df, use_container_width=True, hide_index=True)

    if st.button("APRを確定（台帳保存）→ 全員にLINE送信", type="primary"):
        token = s(st.secrets.get("line", {}).get("channel_access_token", "")).strip()
        if not token:
            st.error("Secrets の [line].channel_access_token が未設定です。")
            return

        # evidence upload (optional)
        image_url = None
        if uploaded:
            api_key = s(st.secrets.get("imgbb", {}).get("api_key", "")).strip()
            if not api_key:
                st.error("画像を送るには Secrets の [imgbb].api_key が必要です。")
                return
            with st.spinner("ImgBBへ画像アップロード中..."):
                image_url = upload_imgbb(api_key, uploaded.getvalue())
            if uploaded and not image_url:
                st.error("ImgBBアップロードに失敗しました。画像なしで送る場合は画像を外して再実行してください。")
                return

        # write ledger rows (APR is per person)
        ts = now_jst().strftime("%Y-%m-%d %H:%M:%S")
        for person_name, profit in dist.items():
            bal_before = compute_person_balance_from_ledger(df_ledger, project, person_name)
            signed = float(profit)
            bal_after = bal_before + signed
            row = {
                "Datetime_JST": ts,
                "Project_Name": project,
                "PersonName": person_name,
                "Type": "APR",
                "Amount": float(abs(profit)),
                "Signed_Amount": signed,
                "Balance_After": bal_after,
                "Note": f"APR:{apr_percent}% NetFactor:{net_factor}",
                "Evidence_Image_URL": image_url or "",
            }
            append_ledger_row(
                spreadsheet_id=spreadsheet_id,
                row=row,
                settings_sheet=sheets.settings,
                members_sheet=sheets.members,
                ledger_sheet=sheets.ledger,
            )

        # APR report message (NO personal names)
        now_str = now_jst().strftime("%Y/%m/%d %H:%M")
        msg = ""
        msg += "🏦 【資産運用収益報告】\n"
        msg += f"プロジェクト: {project}\n"
        msg += f"報告日時: {now_str}\n"
        msg += f"本日のAPR: {apr_percent}%\n"
        msg += f"NetFactor: {net_factor:.2f}\n"
        msg += f"人数: {len(people)}\n"
        msg += f"本日の合計利益: ${sum(dist.values()):,.4f}\n"
        if image_url:
            msg += "\n📎 エビデンス画像を添付します。"

        # send to all (project active members)
        success, fail = 0, 0
        for uid in line_ids:
            code = send_line_message(token, uid, msg, image_url=image_url)
            if code == 200:
                success += 1
            else:
                fail += 1

        cache_bust()
        st.success(f"APR確定 & LINE送信完了：成功 {success} / 失敗 {fail}")


def ui_ledger(
    spreadsheet_id: str,
    sheets: AppSheets,
    df_members: pd.DataFrame,
    df_ledger: pd.DataFrame,
    project: str,
) -> None:
    st.subheader("💸 入金 / 出金（本人へLINE通知）")

    members_active = members_for_project(df_members, project, active_only=True)
    if members_active.empty:
        st.warning("このプロジェクトに Active メンバーがいません。")
        return

    # person list
    person_list = members_active["PersonName"].astype(str).tolist()
    person = st.selectbox("メンバー（PersonName）", person_list)

    # current balance
    cur_balance = compute_person_balance_from_ledger(df_ledger, project, person)
    st.info(f"現在残高: **${cur_balance:,.2f}**")

    typ = st.selectbox("種別", ["Deposit", "Withdraw"], index=0)
    amt = st.number_input("金額（$）", min_value=0.0, value=0.0, step=100.0)
    note = st.text_input("メモ（任意）", value="")

    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"], key="ledger_evidence")
    if uploaded:
        st.image(uploaded, caption="送信プレビュー", width=420)

    if st.button("台帳に保存 → 本人へLINE通知", type="primary"):
        if amt <= 0:
            st.error("金額が0です。")
            return

        token = s(st.secrets.get("line", {}).get("channel_access_token", "")).strip()
        if not token:
            st.error("Secrets の [line].channel_access_token が未設定です。")
            return

        # resolve member line id
        rowm = members_active[members_active["PersonName"].astype(str) == str(person)].iloc[0]
        uid = s(rowm.get("Line_User_ID", "")).strip()
        disp = s(rowm.get("LINE_DisplayName", "")).strip()
        if not uid.startswith("U"):
            st.error("このメンバーは Line_User_ID が未設定です（Members を確認してください）。")
            return

        # evidence upload (optional)
        image_url = None
        if uploaded:
            api_key = s(st.secrets.get("imgbb", {}).get("api_key", "")).strip()
            if not api_key:
                st.error("画像を送るには Secrets の [imgbb].api_key が必要です。")
                return
            with st.spinner("ImgBBへ画像アップロード中..."):
                image_url = upload_imgbb(api_key, uploaded.getvalue())
            if uploaded and not image_url:
                st.error("ImgBBアップロードに失敗しました。画像なしで送る場合は画像を外して再実行してください。")
                return

        # signed amount & balance after
        signed = float(amt) if typ == "Deposit" else -float(amt)
        bal_after = cur_balance + signed
        ts = now_jst().strftime("%Y-%m-%d %H:%M:%S")

        # write ledger
        row = {
            "Datetime_JST": ts,
            "Project_Name": project,
            "PersonName": person,
            "Type": typ,
            "Amount": float(amt),
            "Signed_Amount": signed,
            "Balance_After": bal_after,
            "Note": note,
            "Evidence_Image_URL": image_url or "",
        }
        append_ledger_row(
            spreadsheet_id=spreadsheet_id,
            row=row,
            settings_sheet=sheets.settings,
            members_sheet=sheets.members,
            ledger_sheet=sheets.ledger,
        )

        # send LINE to person
        now_str = now_jst().strftime("%Y/%m/%d %H:%M")
        action_jp = "入金" if typ == "Deposit" else "出金"
        msg = ""
        msg += f"🏦 【{action_jp}通知】\n"
        msg += f"プロジェクト: {project}\n"
        msg += f"日時: {now_str}\n"
        msg += f"金額: ${amt:,.2f}\n"
        msg += f"反映後残高: ${bal_after:,.2f}\n"
        if note:
            msg += f"メモ: {note}\n"
        if image_url:
            msg += "\n📎 エビデンス画像を添付します。"

        code = send_line_message(token, uid, msg, image_url=image_url)
        cache_bust()
        if code == 200:
            st.success("保存 & LINE通知が完了しました。")
        else:
            st.warning(f"保存は完了しましたが、LINE送信が失敗しました（HTTP {code}）。")

    st.divider()
    st.write("### 台帳（最新200件）")
    if df_ledger.empty:
        st.info("Ledger が空です。")
        return

    show = df_ledger.copy().fillna("")
    # ensure columns exist
    for c in DEFAULT_LEDGER_HEADERS:
        if c not in show.columns:
            show[c] = ""

    show = show[show["Project_Name"].astype(str) == str(project)]
    # latest 200
    if len(show) > 200:
        show = show.tail(200)

    st.dataframe(show, use_container_width=True, hide_index=True)


def ui_members_view(df_members: pd.DataFrame, project: str) -> None:
    st.subheader("👤 メンバー（閲覧）")
    if df_members.empty:
        st.info("Members が空です。")
        return
    df = df_members.copy().fillna("")
    for col in ["Project_Name", "PersonName", "Line_User_ID", "LINE_DisplayName", "Active", "CreatedAt_JST"]:
        if col not in df.columns:
            df[col] = ""
    df = df[df["Project_Name"].astype(str) == str(project)]
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption("※ 追加/更新は管理者タブから行います。")


def ui_admin(
    spreadsheet_id: str,
    sheets: AppSheets,
    df_settings: pd.DataFrame,
    df_members: pd.DataFrame,
) -> None:
    st.subheader("⚙ 管理（管理者のみ）")
    admin_login_block()
    if not is_admin():
        st.info("管理者PINを入力すると管理機能が表示されます。")
        return

    st.success("管理者機能が有効です。")

    st.divider()
    st.write("## ✅ ヘッダー（コピペ用）")
    st.write("**Settings**")
    st.code("\t".join(DEFAULT_SETTINGS_HEADERS))
    st.write("**Members**")
    st.code("\t".join(DEFAULT_MEMBERS_HEADERS))
    st.write("**Ledger**")
    st.code("\t".join(DEFAULT_LEDGER_HEADERS))
    st.caption("※ 1行目に貼り付け（区切りはタブ）")

    st.divider()
    st.write("## 🧩 プロジェクト設定（注意: 編集はシートで行う運用が安定）")
    st.dataframe(df_settings.fillna(""), use_container_width=True, hide_index=True)
    st.caption("Settings の NetFactor（0.67/0.60）と SplitMode（equal/proportional）をプロジェクト毎に設定します。")

    st.divider()
    st.write("## 👤 メンバー追加（管理者のみ）")
    # LS button state
    if "members_filter" not in st.session_state:
        st.session_state["members_filter"] = "active"

    b1, b2, _ = st.columns([1, 1, 2])
    with b1:
        if st.button("LS Active", use_container_width=True):
            st.session_state["members_filter"] = "active"
    with b2:
        if st.button("LS All", use_container_width=True):
            st.session_state["members_filter"] = "all"

    filt = st.session_state["members_filter"]
    st.caption(f"表示: {filt}")

    with st.form("admin_add_member", clear_on_submit=False):
        # project options from Settings
        projects = []
        if not df_settings.empty and "Project_Name" in df_settings.columns:
            projects = df_settings["Project_Name"].dropna().astype(str).tolist()
        project = st.selectbox("Project_Name", options=projects if projects else [""])
        person = st.text_input("PersonName（個人名）")
        line_uid = st.text_input("Line_User_ID（Uから始まる）")
        line_disp = st.text_input("LINE_DisplayName（任意）")
        active = st.checkbox("Active", value=True)
        ok = st.form_submit_button("追加（※同一Line_User_IDがあれば更新せずスキップ）")

    if ok:
        if not project or not person or not line_uid:
            st.error("Project_Name / PersonName / Line_User_ID は必須です。")
        else:
            created, msg = add_member_if_new_line_user_id(
                spreadsheet_id=spreadsheet_id,
                project=project,
                person_name=person,
                line_user_id=line_uid.strip(),
                line_display_name=line_disp.strip(),
                active=active,
                members_sheet=sheets.members,
            )
            cache_bust()
            if created:
                st.success(msg)
            else:
                st.warning(msg)

    st.divider()
    st.write("## 👤 メンバー一覧")
    if df_members.empty:
        st.info("Members が空です。")
        return

    df = df_members.copy().fillna("")
    for col in DEFAULT_MEMBERS_HEADERS:
        if col not in df.columns:
            df[col] = ""

    if filt == "active":
        df = df[df["Active"].astype(str).str.strip().str.upper().isin(["TRUE", "YES", "1", "はい", "ON"])]

    st.dataframe(df, use_container_width=True, hide_index=True)


# -----------------------------
# Main
# -----------------------------
def main():
    st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")
    st.title("🏦 APR管理システム")

    # Spreadsheet open & ensure sheets
    try:
        sp = open_spreadsheet()
        spreadsheet_id = sp.id
    except Exception as e:
        st.error(f"Spreadsheet を開けません。共有設定（編集者）とIDを確認してください。: {e}")
        ui_debug_quota_hint(e)
        return

    sheets = AppSheets()

    try:
        # Ensure base sheets exist (minimal calls)
        ensure_sheet(sp, sheets.settings, DEFAULT_SETTINGS_HEADERS)
        ensure_sheet(sp, sheets.members, DEFAULT_MEMBERS_HEADERS)
        ensure_sheet(sp, sheets.ledger, DEFAULT_LEDGER_HEADERS)
    except Exception as e:
        st.error(f"シート初期化に失敗: {e}")
        ui_debug_quota_hint(e)
        return

    # Read with cache
    try:
        df_settings = read_sheet_df_cached(spreadsheet_id, sheets.settings)
        df_members = read_sheet_df_cached(spreadsheet_id, sheets.members)
        df_ledger = read_sheet_df_cached(spreadsheet_id, sheets.ledger)
    except Exception as e:
        st.error(f"読み取りエラー: {e}")
        ui_debug_quota_hint(e)
        return

    df_settings = clean_columns(df_settings).fillna("")
    df_members = clean_columns(df_members).fillna("")
    df_ledger = clean_columns(df_ledger).fillna("")

    # Project select
    project, net_factor, split_mode, currency, proj_active = ui_project_selector(df_settings)
    st.sidebar.write(f"選択プロジェクト: **{project}**")
    st.sidebar.write(f"NetFactor: **{net_factor:.2f}** / SplitMode: **{split_mode}**")
    st.sidebar.write(f"通貨: **$**（表示固定）")

    if not proj_active:
        st.warning("このプロジェクトは Settings で Active=FALSE です。")

    tab1, tab2, tab3, tab4 = st.tabs(["📈 APR", "💸 入出金", "👤 メンバー", "⚙ 管理"])

    with tab1:
        ui_apr(
            spreadsheet_id=spreadsheet_id,
            sheets=sheets,
            df_settings=df_settings,
            df_members=df_members,
            df_ledger=df_ledger,
            project=project,
            net_factor=net_factor,
            split_mode=split_mode,
        )

    with tab2:
        ui_ledger(
            spreadsheet_id=spreadsheet_id,
            sheets=sheets,
            df_members=df_members,
            df_ledger=df_ledger,
            project=project,
        )

    with tab3:
        ui_members_view(df_members=df_members, project=project)

    with tab4:
        ui_admin(
            spreadsheet_id=spreadsheet_id,
            sheets=sheets,
            df_settings=df_settings,
            df_members=df_members,
        )


if __name__ == "__main__":
    main()
