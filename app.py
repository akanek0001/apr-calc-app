#デザインお気に入りエラーあり

# app.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st
import requests

import gspread
from google.oauth2.service_account import Credentials


# =========================
# Basic
# =========================
st.set_page_config(page_title="APR資産運用管理システム", layout="wide", page_icon="🏦")

JST = timezone(timedelta(hours=9), "JST")


def now_jst() -> datetime:
    return datetime.now(JST)


def fmt_money(x: float) -> str:
    return f"${x:,.2f}"


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def to_f(val: Any) -> float:
    try:
        s = str(val).replace(",", "").replace("$", "").replace("%", "").strip()
        return float(s) if s else 0.0
    except Exception:
        return 0.0


def split_csv(val: Any, n: int, default: str = "0") -> List[str]:
    items = [x.strip() for x in str(val).split(",") if x.strip() != ""]
    if not items:
        items = [default]
    while len(items) < n:
        items.append(items[-1])
    return items[:n]


def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\u3000", " ", regex=False)
        .str.strip()
    )
    return df


def is_truthy(v: Any) -> bool:
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on", "はい")


# =========================
# Secrets (connections.gsheets)
# =========================
def get_spreadsheet_id_from_secrets() -> str:
    con = st.secrets.get("connections", {}).get("gsheets", {})
    sid = str(con.get("spreadsheet", "")).strip()
    if not sid:
        return ""
    if "/spreadsheets/d/" in sid:
        try:
            sid = sid.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception:
            pass
    return sid


def get_service_account_info() -> Dict[str, Any]:
    con = st.secrets.get("connections", {}).get("gsheets", {})
    creds = con.get("credentials")
    if not creds:
        raise KeyError("Secrets に [connections.gsheets.credentials] がありません。")
    return dict(creds)


# =========================
# Google Sheets Client
# =========================
@dataclass
class GSheetsConfig:
    spreadsheet_id: str
    settings_sheet: str = "Settings"
    members_sheet: str = "Members"
    ledger_sheet: str = "Ledger"
    lineid_sheet: str = "LineID"  # Makeで溜めるIDシート（任意）


DEFAULT_SETTINGS_HEADERS = [
    "Project_Name",
    "Num_People",
    "TotalPrincipal",
    "IndividualPrincipals",
    "ProfitRates",
    "IsCompound",
    "MemberNames",
    "LineID",
    "NetFactor",   # ★ プロジェクト単位（0.67 or 0.60）
]

DEFAULT_MEMBERS_HEADERS = [
    "PersonName",
    "Line_User_ID",
    "LINE_DisplayName",
    "Active",          # TRUE/FALSE
    "CreatedAt_JST",
    "UpdatedAt_JST",
]

DEFAULT_LEDGER_HEADERS = [
    "Datetime_JST",
    "Project_Name",
    "PersonName",
    "Type",            # Deposit / Withdraw / APR
    "Amount",
    "Total_After",     # その人の元本（複利モード時の参考値）
    "Note",
    "Line_User_ID",
    "LINE_DisplayName",
    "Source",          # app / make / line
]


class GSheets:
    def __init__(self, cfg: GSheetsConfig):
        self.cfg = cfg

        sa = get_service_account_info()
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(sa, scopes=scopes)
        self.gc = gspread.authorize(creds)
        self.book = self.gc.open_by_key(cfg.spreadsheet_id)

        self._ensure_sheet(cfg.settings_sheet, DEFAULT_SETTINGS_HEADERS)
        self._ensure_sheet(cfg.members_sheet, DEFAULT_MEMBERS_HEADERS)
        self._ensure_sheet(cfg.ledger_sheet, DEFAULT_LEDGER_HEADERS)

    def _ensure_sheet(self, name: str, headers: List[str]) -> None:
        try:
            ws = self.book.worksheet(name)
        except Exception:
            ws = self.book.add_worksheet(title=name, rows=2000, cols=max(10, len(headers) + 5))
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return

        values = ws.get_all_values()
        if not values:
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return

        h = values[0]
        if len(h) != len(set(h)):
            st.error(f"ヘッダー名が重複しています: {name} / {h}")
            st.stop()

    def ws(self, name: str):
        return self.book.worksheet(name)

    def read_df(self, name: str) -> pd.DataFrame:
        ws = self.ws(name)
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        header = values[0]
        rows = values[1:]
        df = pd.DataFrame(rows, columns=header)
        return clean_cols(df)

    def append_row(self, name: str, row: List[Any]) -> None:
        ws = self.ws(name)
        ws.append_row([safe_str(x) for x in row], value_input_option="USER_ENTERED")


# =========================
# Cache (429対策)
# =========================
@st.cache_data(ttl=20, show_spinner=False)
def cached_read_df(spreadsheet_id: str, sheet_name: str, sa_fingerprint: str) -> pd.DataFrame:
    cfg = GSheetsConfig(spreadsheet_id=spreadsheet_id)
    gs = GSheets(cfg)
    return gs.read_df(sheet_name)


def sa_fingerprint_from_secrets() -> str:
    sa = get_service_account_info()
    return safe_str(sa.get("client_email", "")) + "|" + safe_str(sa.get("private_key_id", ""))


# =========================
# LINE / ImgBB
# =========================
def send_line(token: str, user_id: str, text: str, image_url: Optional[str] = None) -> int:
    if not user_id:
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
    try:
        api_key = st.secrets["imgbb"]["api_key"]
    except Exception:
        return None
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


# =========================
# Admin Auth
# =========================
def admin_login_ui() -> None:
    required = safe_str(st.secrets.get("admin", {}).get("pin", ""))
    if not required:
        st.warning("Secrets の [admin].pin が未設定です。管理機能を保護できません。")
        st.session_state["admin_ok"] = False
        return

    if st.session_state.get("admin_ok", False):
        c1, c2 = st.columns([1, 1])
        with c1:
            st.success("管理者ログイン中")
        with c2:
            if st.button("ログアウト", use_container_width=True):
                st.session_state["admin_ok"] = False
                st.rerun()
        return

    with st.form("admin_login_form"):
        pin = st.text_input("管理者パスワード", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            st.session_state["admin_ok"] = (pin == required)
            if st.session_state["admin_ok"]:
                st.success("ログイン成功")
                st.rerun()
            else:
                st.error("パスワードが違います")


def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))


# =========================
# Business logic
# =========================
def pick_project(settings_df: pd.DataFrame) -> str:
    projects = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    if not projects:
        return ""
    return st.sidebar.selectbox("プロジェクト", projects)


def settings_row(settings_df: pd.DataFrame, project_name: str) -> pd.Series:
    df = settings_df[settings_df["Project_Name"].astype(str) == str(project_name)]
    return df.iloc[0]


def compute_member_principals(
    ledger_df: pd.DataFrame,
    project: str,
    names: List[str],
    base_principals: List[float],
    is_compound: bool,
) -> List[float]:
    n = len(names)
    p = base_principals[:]
    if ledger_df.empty:
        return p

    df = ledger_df.copy().fillna("")
    df = df[df["Project_Name"].astype(str) == str(project)]
    df["Amount_f"] = df["Amount"].apply(to_f)

    for i in range(n):
        name = names[i]
        sub = df[df["PersonName"].astype(str) == str(name)]
        if sub.empty:
            continue
        dep = sub[sub["Type"] == "Deposit"]["Amount_f"].sum()
        wdr = sub[sub["Type"] == "Withdraw"]["Amount_f"].sum()
        apr = sub[sub["Type"] == "APR"]["Amount_f"].sum()
        p[i] = p[i] + dep - wdr
        if is_compound:
            p[i] = p[i] + apr
    return p


def only_line_ids(values: List[Any]) -> List[str]:
    out = []
    for v in values:
        s = str(v).strip()
        if s.startswith("U"):
            out.append(s)
    seen, uniq = set(), []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def get_broadcast_ids(gs: GSheets, settings_lineid_fallback: str) -> List[str]:
    try:
        df = gs.read_df(gs.cfg.lineid_sheet)
        if not df.empty:
            if "Line_User_ID" in df.columns:
                return only_line_ids(df["Line_User_ID"].dropna().tolist())
            if "LineID" in df.columns:
                return only_line_ids(df["LineID"].dropna().tolist())
            return only_line_ids(df.iloc[:, -1].dropna().tolist())
    except Exception:
        pass

    ids = split_csv(settings_lineid_fallback, 999, default="")
    return only_line_ids(ids)


# =========================
# Pages
# =========================
def page_apr(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame, ledger_df: pd.DataFrame):
    st.header("📈 APR（収益確定 → 全員へLINE報告）")

    project = pick_project(settings_df)
    if not project:
        st.info("Settings に Project_Name を追加してください。")
        return

    p = settings_row(settings_df, project)
    num_people = int(to_f(p["Num_People"]))
    is_compound = is_truthy(p["IsCompound"])

    member_names = split_csv(p.get("MemberNames", ""), num_people, default="")
    if not any(member_names):
        member_names = [f"No.{i+1}" for i in range(num_people)]
    else:
        member_names = [x if x else f"No.{i+1}" for i, x in enumerate(member_names)]

    base_principals = [to_f(x) for x in split_csv(p.get("IndividualPrincipals", "0"), num_people, default="0")]
    profit_rates = [to_f(x) for x in split_csv(p.get("ProfitRates", "100"), num_people, default="100")]

    # ★ プロジェクト単位 NetFactor（無ければ 0.67）
    net_factor = to_f(p.get("NetFactor", 0.67))
    if net_factor <= 0:
        net_factor = 0.67

    principals = compute_member_principals(
        ledger_df=ledger_df,
        project=project,
        names=member_names,
        base_principals=base_principals,
        is_compound=is_compound,
    )

    st.sidebar.info(f"複利: {'YES' if is_compound else 'NO'} / 人数: {num_people}")
    st.sidebar.info(f"NetFactor（プロジェクト）: {net_factor}")

    apr_percent = st.number_input("本日のAPR（%）", value=100.0, step=0.1)
    evidence = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])

    today_yields = []
    for i in range(num_people):
        p0 = principals[i]
        r = profit_rates[i] / 100.0
        y = (p0 * (apr_percent / 100.0) * net_factor * r) / 365.0
        today_yields.append(round(float(y), 6))

    st.subheader("本日の計算結果")
    cols = st.columns(3)
    with cols[0]:
        st.metric("総収益", fmt_money(sum(today_yields)))
    with cols[1]:
        st.metric("NetFactor", f"{net_factor:.2f}")
    with cols[2]:
        st.metric("報告日時（JST）", now_jst().strftime("%Y-%m-%d %H:%M"))

    show = pd.DataFrame({
        "PersonName": member_names,
        "Principal": principals,
        "ProfitRate(%)": [round(x, 4) for x in profit_rates],
        "TodayYield": today_yields,
    })
    st.dataframe(show, use_container_width=True, hide_index=True)

    if st.button("収益を確定して、全員へLINE送信", type="primary"):
        image_url = None
        if evidence is not None:
            with st.spinner("画像アップロード中（ImgBB）..."):
                image_url = upload_imgbb(evidence.getvalue())
            if not image_url:
                st.error("ImgBBアップロードに失敗しました。画像なしで送る場合は画像を外して再実行してください。")
                st.stop()

        dtj = now_jst().strftime("%Y-%m-%d %H:%M:%S")

        # APRは個人別に台帳へ記録（複利反映のため）
        for i in range(num_people):
            name = member_names[i]
            amt = today_yields[i]
            total_after = principals[i] + amt if is_compound else principals[i]

            m = None
            if not members_df.empty:
                mm = members_df[members_df["PersonName"].astype(str) == str(name)]
                if not mm.empty:
                    m = mm.iloc[0].to_dict()

            line_uid = safe_str(m.get("Line_User_ID")) if m else ""
            disp = safe_str(m.get("LINE_DisplayName")) if m else ""

            gs.append_row(gs.cfg.ledger_sheet, [
                dtj, project, name, "APR", amt, total_after,
                f"APR:{apr_percent}%, NetFactor:{net_factor}", line_uid, disp, "app"
            ])

        broadcast_ids = get_broadcast_ids(gs, safe_str(p.get("LineID", "")))
        token = safe_str(st.secrets.get("line", {}).get("channel_access_token", ""))
        if not token:
            st.error("Secrets の [line].channel_access_token が未設定です。")
            st.stop()

        msg = "🏦 【資産運用 収益報告】\n"
        msg += f"プロジェクト: {project}\n"
        msg += f"報告日時(JST): {dtj}\n"
        msg += f"本日のAPR: {apr_percent}%\n"
        msg += f"NetFactor: {net_factor}\n"
        msg += f"総収益: {fmt_money(sum(today_yields))}\n"
        if image_url:
            msg += "\n📎 エビデンス画像を添付します。"

        ok = 0
        ng = 0
        for uid in broadcast_ids:
            code = send_line(token, uid, msg, image_url=image_url)
            if code == 200:
                ok += 1
            else:
                ng += 1

        st.success(f"送信完了：成功 {ok} / 失敗 {ng}")
        st.cache_data.clear()
        st.rerun()


def page_cashflow(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame, ledger_df: pd.DataFrame):
    st.header("💸 入金 / 出金（本人へ個別LINE通知）")

    project = pick_project(settings_df)
    if not project:
        st.info("Settings に Project_Name を追加してください。")
        return

    p = settings_row(settings_df, project)
    num_people = int(to_f(p["Num_People"]))
    is_compound = is_truthy(p["IsCompound"])

    member_names = split_csv(p.get("MemberNames", ""), num_people, default="")
    if not any(member_names):
        member_names = [f"No.{i+1}" for i in range(num_people)]
    else:
        member_names = [x if x else f"No.{i+1}" for i, x in enumerate(member_names)]

    base_principals = [to_f(x) for x in split_csv(p.get("IndividualPrincipals", "0"), num_people, default="0")]

    principals = compute_member_principals(
        ledger_df=ledger_df,
        project=project,
        names=member_names,
        base_principals=base_principals,
        is_compound=is_compound,
    )

    c1, c2 = st.columns([1, 1])
    with c1:
        target = st.selectbox("メンバー", member_names)
        idx = member_names.index(target)
        st.info(f"現在元本（参考）: {fmt_money(principals[idx])}")

    with c2:
        typ = st.selectbox("種別", ["Deposit", "Withdraw"])
        amt = st.number_input("金額", min_value=0.0, value=0.0, step=1000.0)
        note = st.text_input("メモ", value="")

    if st.button("記録して本人へ通知", type="primary"):
        if amt <= 0:
            st.warning("金額が 0 です。")
            st.stop()

        dtj = now_jst().strftime("%Y-%m-%d %H:%M:%S")
        after = principals[idx] + amt if typ == "Deposit" else principals[idx] - amt

        m = None
        if not members_df.empty:
            mm = members_df[members_df["PersonName"].astype(str) == str(target)]
            if not mm.empty:
                m = mm.iloc[0].to_dict()

        line_uid = safe_str(m.get("Line_User_ID")) if m else ""
        disp = safe_str(m.get("LINE_DisplayName")) if m else ""

        gs.append_row(gs.cfg.ledger_sheet, [
            dtj, project, target, typ, float(amt), float(after),
            note, line_uid, disp, "app"
        ])

        token = safe_str(st.secrets.get("line", {}).get("channel_access_token", ""))
        if token and line_uid:
            msg = "🏦 【入出金通知】\n"
            msg += f"プロジェクト: {project}\n"
            msg += f"日時(JST): {dtj}\n"
            msg += f"種別: {'入金' if typ=='Deposit' else '出金'}\n"
            msg += f"金額: {fmt_money(float(amt))}\n"
            msg += f"反映後元本（参考）: {fmt_money(float(after))}\n"
            if note:
                msg += f"メモ: {note}\n"
            code = send_line(token, line_uid, msg)
            if code == 200:
                st.success("記録＋本人へ通知しました。")
            else:
                st.success("記録しましたが、LINE送信に失敗しました（ID/Token確認）。")
        else:
            st.success("記録しました（Membersの紐付けが無いのでLINEは送っていません）。")

        st.cache_data.clear()
        st.rerun()

    st.subheader("台帳（このプロジェクト）")
    if ledger_df.empty:
        st.info("Ledger が空です。")
        return

    df = ledger_df.copy().fillna("")
    df = df[df["Project_Name"].astype(str) == str(project)]
    st.dataframe(df, use_container_width=True, hide_index=True)


def page_members(gs: GSheets, members_df: pd.DataFrame):
    st.header("👤 メンバー（管理者のみ編集）")

    admin_login_ui()
    if not is_admin():
        st.info("管理者ログインすると編集できます。")
        st.dataframe(members_df, use_container_width=True, hide_index=True)
        return

    if "members_ls_mode" not in st.session_state:
        st.session_state["members_ls_mode"] = "ACTIVE"

    b1, b2 = st.columns([1, 1])
    with b1:
        if st.button("LS Active", use_container_width=True):
            st.session_state["members_ls_mode"] = "ACTIVE"
    with b2:
        if st.button("LS All", use_container_width=True):
            st.session_state["members_ls_mode"] = "ALL"

    mode = st.session_state["members_ls_mode"]

    df = members_df.copy().fillna("")
    if not df.empty and "Active" in df.columns and mode == "ACTIVE":
        df["Active_norm"] = df["Active"].astype(str).str.strip().str.upper()
        df = df[df["Active_norm"].isin(["TRUE", "1", "YES", "はい"])]
        df = df.drop(columns=["Active_norm"], errors="ignore")

    st.dataframe(df, use_container_width=True, hide_index=True)

    st.subheader("追加（LINE User ID が既に存在する場合は“追加しない”）")
    with st.form("add_member_form"):
        person = st.text_input("PersonName（個人名）")
        line_uid = st.text_input("Line_User_ID（Uxxxx...）")
        disp = st.text_input("LINE_DisplayName（任意）")
        active = st.checkbox("Active", value=True)
        submit = st.form_submit_button("追加")

    if submit:
        if not person or not line_uid:
            st.error("PersonName と Line_User_ID は必須です。")
            st.stop()

        cur = members_df.copy().fillna("")
        if not cur.empty and "Line_User_ID" in cur.columns:
            exists = cur[cur["Line_User_ID"].astype(str) == str(line_uid)]
            if not exists.empty:
                st.warning("この Line_User_ID は既に登録済みです（仕様：一致したら更新しない）。")
                st.stop()

        dt = now_jst().strftime("%Y-%m-%d %H:%M:%S")
        gs.append_row(gs.cfg.members_sheet, [
            person,
            line_uid,
            disp,
            "TRUE" if active else "FALSE",
            dt,
            dt
        ])
        st.success("追加しました。")
        st.cache_data.clear()
        st.rerun()


def page_admin():
    st.header("⚙️ 管理")
    admin_login_ui()
    if not is_admin():
        st.stop()

    st.subheader("ヘッダー（コピペ用）")
    st.write("Settings")
    st.code("\t".join(DEFAULT_SETTINGS_HEADERS))
    st.write("Members")
    st.code("\t".join(DEFAULT_MEMBERS_HEADERS))
    st.write("Ledger")
    st.code("\t".join(DEFAULT_LEDGER_HEADERS))
    st.caption("※ 1行目に貼り付け（区切りはタブ）")


# =========================
# Main
# =========================
def main():
    st.title("🏦 APR資産運用管理システム")

    spreadsheet_id = get_spreadsheet_id_from_secrets()
    if not spreadsheet_id:
        st.error("Secrets の [connections.gsheets].spreadsheet が未設定です。")
        st.stop()

    page = st.sidebar.radio("メニュー", ["📈 APR", "💸 入出金", "👤 メンバー", "⚙️ 管理"], index=0)

    fp = sa_fingerprint_from_secrets()

    try:
        gs = GSheets(GSheetsConfig(spreadsheet_id=spreadsheet_id))
    except Exception as e:
        st.error(f"Spreadsheet を開けません。共有設定（編集者）とIDを確認してください。: {e}")
        st.stop()

    try:
        settings_df = cached_read_df(spreadsheet_id, "Settings", fp).fillna("")
        members_df = cached_read_df(spreadsheet_id, "Members", fp).fillna("")
        ledger_df = cached_read_df(spreadsheet_id, "Ledger", fp).fillna("")
    except Exception as e:
        st.error(f"読み取りエラー: {e}")
        st.stop()

    if settings_df.empty:
        st.error("Settings シートが空です。")
        st.stop()

    for col in ["Project_Name", "Num_People", "IndividualPrincipals", "ProfitRates", "IsCompound"]:
        if col not in settings_df.columns:
            st.error(f"Settings に列がありません: {col}")
            st.stop()

    if page == "📈 APR":
        page_apr(gs, settings_df, members_df, ledger_df)
    elif page == "💸 入出金":
        page_cashflow(gs, settings_df, members_df, ledger_df)
    elif page == "👤 メンバー":
        page_members(gs, members_df)
    else:
        page_admin()


if __name__ == "__main__":
    main()
