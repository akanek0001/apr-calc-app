# app.py
# PRO版: 管理者ごとに分離して同じAPR運用ができるマルチ管理者版
# - 管理者ログインは「管理者選択 + PIN」
# - 管理者ごとに別シートでデータを分離（Settings/Members/Ledger/LineUsers を admin_namespace で分岐）
# - LINE token も admin_namespace に応じて切替（line.tokens[namespace]）
# - ダッシュボード追加
#   ・総資産
#   ・本日APR
#   ・グループ別残高 
#   ・個人残高
#   ・LINE通知履歴
# - APR計算仕様
#   1) PERSONAL:
#      日次配当 = Principal × (APR% / 100) × Rank係数 ÷ 365
#      Master=67%, Elite=60%
#   2) GROUP（PERSONAL以外）:
#      グループ総額 × (APR% / 100) × Settings.Net_Factor ÷ 365
#      を、そのプロジェクトの有効メンバー人数で均等割
# - 一括編集:
#      元行番号ベースで保存し、確実にシートへ反映

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, List, Optional, Tuple

import json
import pandas as pd
import requests
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

JST = timezone(timedelta(hours=9), "JST")

STATUS_ON = "🟢運用中"
STATUS_OFF = "🔴停止"
RANK_LABEL = "👑Master=67% / 🥈Elite=60%"

BASE_SETTINGS = "Settings"
BASE_MEMBERS = "Members"
BASE_LEDGER = "Ledger"
BASE_LINEUSERS = "LineUsers"

PERSONAL_PROJECT = "PERSONAL"


# -----------------------------
# Utils
# -----------------------------
def now_jst() -> datetime:
    return datetime.now(JST)


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fmt_usd(x: float) -> str:
    return f"${x:,.2f}"


def to_f(v: Any) -> float:
    try:
        s = str(v).replace(",", "").replace("$", "").replace("%", "").strip()
        return float(s) if s else 0.0
    except Exception:
        return 0.0


def truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on", "はい", "t")


def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\u3000", " ", regex=False)
        .str.strip()
    )
    return df


def extract_sheet_id(value: str) -> str:
    sid = (value or "").strip()
    if "/spreadsheets/d/" in sid:
        try:
            sid = sid.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception:
            pass
    return sid


def rank_to_factor(rank: str) -> float:
    r = (rank or "").strip().lower()
    if r == "master":
        return 0.67
    if r == "elite":
        return 0.60
    return 0.67


def normalize_rank(rank: Any) -> str:
    r = str(rank).strip()
    if not r:
        return "Master"
    if r.lower() == "master":
        return "Master"
    if r.lower() == "elite":
        return "Elite"
    return "Master"


def bool_to_status(v: Any) -> str:
    return STATUS_ON if truthy(v) else STATUS_OFF


def status_to_bool(s: Any) -> bool:
    return str(s).strip() == STATUS_ON


def is_line_uid(v: Any) -> bool:
    s = str(v).strip()
    return s.startswith("U") and len(s) >= 10


def dedup_line_ids(df: pd.DataFrame) -> List[str]:
    if df.empty or "Line_User_ID" not in df.columns:
        return []

    ids: List[str] = []
    for v in df["Line_User_ID"].tolist():
        s = str(v).strip()
        if s.startswith("U"):
            ids.append(s)

    seen, out = set(), []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def insert_person_name(msg_common: str, person_name: str) -> str:
    name_line = f"{person_name} 様"
    lines = msg_common.splitlines()
    if name_line in lines:
        return msg_common

    if lines and lines[0].strip() == "【ご連絡】":
        return "\n".join([lines[0], name_line] + lines[1:])
    return "\n".join([name_line] + lines)


def sheet_name(base: str, ns: str) -> str:
    ns = str(ns or "").strip()
    if not ns or ns == "default":
        return base
    return f"{base}__{ns}"


def get_line_token(ns: str) -> str:
    ns = str(ns or "").strip()
    line = st.secrets.get("line", {}) or {}

    tokens = line.get("tokens", None)
    if tokens:
        tok = str(tokens.get(ns, "")).strip()
        if tok:
            return tok

    legacy = str(line.get("channel_access_token", "")).strip()
    if legacy:
        return legacy

    st.error("LINEトークンが未設定です。secretsの [line].tokens または channel_access_token を確認してください。")
    st.stop()


# -----------------------------
# LINE
# -----------------------------
def send_line_push(token: str, user_id: str, text: str, image_url: Optional[str] = None) -> int:
    if not user_id:
        return 400

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    messages = [{"type": "text", "text": text}]
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
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=25)
        return r.status_code
    except Exception:
        return 500


# -----------------------------
# ImgBB
# -----------------------------
def upload_imgbb(file_bytes: bytes) -> Optional[str]:
    try:
        key = st.secrets["imgbb"]["api_key"]
    except Exception:
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


# -----------------------------
# Sheets headers
# -----------------------------
SETTINGS_HEADERS = ["Project_Name", "Net_Factor", "IsCompound", "UpdatedAt_JST", "Active"]

MEMBERS_HEADERS = [
    "Project_Name",
    "PersonName",
    "Principal",
    "Line_User_ID",
    "LINE_DisplayName",
    "Rank",
    "IsActive",
    "CreatedAt_JST",
    "UpdatedAt_JST",
]

LEDGER_HEADERS = [
    "Datetime_JST",
    "Project_Name",
    "PersonName",
    "Type",
    "Amount",
    "Note",
    "Evidence_URL",
    "Line_User_ID",
    "LINE_DisplayName",
    "Source",
]

LINEUSERS_HEADERS = ["Date", "Time", "Type", "Line_User_ID", "Line_User"]


# -----------------------------
# Admin (multi)
# -----------------------------
@dataclass
class AdminUser:
    name: str
    pin: str
    namespace: str


def load_admin_users() -> List[AdminUser]:
    a = st.secrets.get("admin", {}) or {}

    users = a.get("users", None)
    if users:
        out: List[AdminUser] = []
        for u in users:
            name = str(u.get("name", "")).strip() or "Admin"
            pin = str(u.get("pin", "")).strip()
            ns = str(u.get("namespace", "")).strip() or name
            if not pin:
                continue
            out.append(AdminUser(name=name, pin=pin, namespace=ns))
        if out:
            return out

    pin = str(a.get("pin", "")).strip() or str(a.get("password", "")).strip()
    if pin:
        return [AdminUser(name="Admin", pin=pin, namespace="default")]

    return []


def require_admin_login_multi() -> None:
    admins = load_admin_users()
    if not admins:
        st.error("Secrets に [admin].users（推奨）または [admin].pin が未設定です。")
        st.stop()

    if st.session_state.get("admin_ok", False) and st.session_state.get("admin_namespace"):
        return

    st.markdown("## 🔐 管理者ログイン")

    names = [a.name for a in admins]
    default_name = st.session_state.get("login_admin_name", names[0])
    if default_name not in names:
        default_name = names[0]

    with st.form("admin_gate_multi", clear_on_submit=False):
        admin_name = st.selectbox("管理者を選択", names, index=names.index(default_name))
        pw = st.text_input("管理者PIN", type="password")
        ok = st.form_submit_button("ログイン")

        if ok:
            st.session_state["login_admin_name"] = admin_name
            picked = next((a for a in admins if a.name == admin_name), None)
            if not picked:
                st.error("管理者が見つかりません。")
                st.stop()

            if pw == picked.pin:
                st.session_state["admin_ok"] = True
                st.session_state["admin_name"] = picked.name
                st.session_state["admin_namespace"] = picked.namespace
                st.rerun()
            else:
                st.session_state["admin_ok"] = False
                st.session_state["admin_name"] = ""
                st.session_state["admin_namespace"] = ""
                st.error("PINが違います。")

    st.stop()


def current_admin_label() -> str:
    name = str(st.session_state.get("admin_name", "")).strip() or "Admin"
    ns = str(st.session_state.get("admin_namespace", "")).strip() or "default"
    return f"{name}（namespace: {ns}）"


# -----------------------------
# Google Sheets
# -----------------------------
@dataclass
class GSheetsConfig:
    spreadsheet_id: str
    settings_sheet: str
    members_sheet: str
    ledger_sheet: str
    lineusers_sheet: str


def build_gs_config(spreadsheet_id: str, ns: str) -> GSheetsConfig:
    return GSheetsConfig(
        spreadsheet_id=spreadsheet_id,
        settings_sheet=sheet_name(BASE_SETTINGS, ns),
        members_sheet=sheet_name(BASE_MEMBERS, ns),
        ledger_sheet=sheet_name(BASE_LEDGER, ns),
        lineusers_sheet=sheet_name(BASE_LINEUSERS, ns),
    )


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

        try:
            self.book = self.gc.open_by_key(self.cfg.spreadsheet_id)
        except Exception as e:
            st.error(f"Spreadsheet を開けません。共有設定（編集者）とIDを確認してください。: {e}")
            st.stop()

        self._ensure_sheet(self.cfg.settings_sheet, SETTINGS_HEADERS)
        self._ensure_sheet(self.cfg.members_sheet, MEMBERS_HEADERS)
        self._ensure_sheet(self.cfg.ledger_sheet, LEDGER_HEADERS)
        self._ensure_sheet(self.cfg.lineusers_sheet, LINEUSERS_HEADERS)

    def _ws(self, name: str):
        return self.book.worksheet(name)

    def _ensure_sheet(self, name: str, headers: List[str]) -> None:
        try:
            ws = self._ws(name)
        except Exception:
            ws = self.book.add_worksheet(title=name, rows=2000, cols=max(30, len(headers) + 10))
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return

        try:
            first = ws.row_values(1)
        except APIError:
            return

        if not first:
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return

        colset = [str(c).strip() for c in first if str(c).strip()]
        missing = [h for h in headers if h not in colset]
        if missing:
            ws.update("1:1", [colset + missing])

    @st.cache_data(ttl=120)
    def read_df(_self, sheet_name: str) -> pd.DataFrame:
        ws = _self._ws(sheet_name)
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        df = pd.DataFrame(values[1:], columns=values[0])
        return clean_cols(df)

    def write_df(self, sheet_name: str, df: pd.DataFrame) -> None:
        ws = self._ws(sheet_name)
        df = df.fillna("").astype(str)
        ws.clear()
        ws.update([df.columns.tolist()] + df.values.tolist(), value_input_option="USER_ENTERED")

    def append_row(self, sheet_name: str, row: List[Any]) -> None:
        ws = self._ws(sheet_name)
        ws.append_row([("" if x is None else x) for x in row], value_input_option="USER_ENTERED")

    def clear_cache(self) -> None:
        st.cache_data.clear()


# -----------------------------
# Load / domain
# -----------------------------
def load_settings(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.settings_sheet)
    if df.empty:
        return df

    need = ["Project_Name", "IsCompound"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        st.error(f"Settingsシート({gs.cfg.settings_sheet})の列が不足: {missing}")
        st.stop()

    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()

    if "Active" not in df.columns:
        df["Active"] = "TRUE"
    df["Active"] = df["Active"].apply(truthy)

    if "Net_Factor" in df.columns:
        df["Net_Factor"] = df["Net_Factor"].apply(lambda x: to_f(x) if str(x).strip() else 0.67)
    else:
        df["Net_Factor"] = 0.67

    df["IsCompound"] = df["IsCompound"].apply(truthy)
    return df


def load_members(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.members_sheet)
    if df.empty:
        return df

    need = ["Project_Name", "PersonName", "Principal", "Line_User_ID", "LINE_DisplayName", "IsActive"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        st.error(f"Membersシート({gs.cfg.members_sheet})の列が不足: {missing}")
        st.stop()

    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
    df["PersonName"] = df["PersonName"].astype(str).str.strip()
    df["Principal"] = df["Principal"].apply(to_f)
    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df["LINE_DisplayName"] = df["LINE_DisplayName"].astype(str).str.strip()
    df["IsActive"] = df["IsActive"].apply(truthy)

    if "Rank" not in df.columns:
        df["Rank"] = "Master"
    df["Rank"] = df["Rank"].apply(normalize_rank)

    for c in ["CreatedAt_JST", "UpdatedAt_JST"]:
        if c not in df.columns:
            df[c] = ""

    return df


def load_ledger(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.ledger_sheet)
    if df.empty:
        return df

    for c in LEDGER_HEADERS:
        if c not in df.columns:
            df[c] = ""

    df["Amount"] = df["Amount"].apply(to_f)
    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
    df["PersonName"] = df["PersonName"].astype(str).str.strip()
    df["Type"] = df["Type"].astype(str).str.strip()
    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df["LINE_DisplayName"] = df["LINE_DisplayName"].astype(str).str.strip()
    df["Source"] = df["Source"].astype(str).str.strip()
    df["Datetime_JST"] = df["Datetime_JST"].astype(str).str.strip()
    return df


def load_line_users(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.lineusers_sheet)
    if df.empty:
        return df

    if "Line_User_ID" not in df.columns and "LineID" in df.columns:
        df = df.rename(columns={"LineID": "Line_User_ID"})

    if "Line_User" not in df.columns and "LINE_DisplayName" in df.columns:
        df = df.rename(columns={"LINE_DisplayName": "Line_User"})

    need = ["Line_User_ID", "Line_User"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        st.error(f"{gs.cfg.lineusers_sheet} シートの列が不足: {missing}")
        return pd.DataFrame()

    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df["Line_User"] = df["Line_User"].astype(str).str.strip()
    return df


def active_projects(settings_df: pd.DataFrame) -> List[str]:
    if settings_df.empty:
        return []
    df = settings_df[settings_df["Active"] == True]
    return df["Project_Name"].dropna().astype(str).unique().tolist()


def project_members_all(members_df: pd.DataFrame, project: str) -> pd.DataFrame:
    df = members_df.copy()
    df = df[df["Project_Name"] == str(project)]
    return df.reset_index(drop=True)


def project_members_active(members_df: pd.DataFrame, project: str) -> pd.DataFrame:
    df = members_df.copy()
    df = df[(df["Project_Name"] == str(project)) & (df["IsActive"] == True)]
    return df.reset_index(drop=True)


def write_members(gs: GSheets, members_df: pd.DataFrame) -> None:
    out = members_df.copy()
    out["Principal"] = out["Principal"].apply(lambda x: f"{float(x):.6f}")
    out["IsActive"] = out["IsActive"].apply(lambda x: "TRUE" if truthy(x) else "FALSE")
    out["Rank"] = out["Rank"].apply(normalize_rank)
    gs.write_df(gs.cfg.members_sheet, out)


def validate_no_dup_lineid_within_project(members_df: pd.DataFrame, project: str) -> Optional[str]:
    df = members_df[members_df["Project_Name"] == str(project)].copy()
    if df.empty:
        return None

    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df = df[df["Line_User_ID"] != ""]
    dup = df[df.duplicated(subset=["Line_User_ID"], keep=False)]
    if dup.empty:
        return None

    ids = dup["Line_User_ID"].unique().tolist()
    return f"同一プロジェクト内で Line_User_ID が重複しています: {ids}"


# -----------------------------
# APR calculation
# -----------------------------
def calc_project_apr(mem: pd.DataFrame, apr_percent: float, project_net_factor: float, project_name: str) -> pd.DataFrame:
    mem = mem.copy()

    # PERSONAL は個別計算
    if str(project_name).strip().upper() == PERSONAL_PROJECT:
        mem["Factor"] = mem["Rank"].apply(rank_to_factor)
        mem["DailyAPR"] = mem.apply(
            lambda r: (float(r["Principal"]) * (apr_percent / 100.0) * float(r["Factor"])) / 365.0,
            axis=1,
        )
        mem["CalcMode"] = "PERSONAL"
        return mem

    # GROUP は総額均等割
    total_principal = float(mem["Principal"].sum())
    count = len(mem)
    factor = float(project_net_factor if project_net_factor > 0 else 0.67)

    total_group_reward = (total_principal * (apr_percent / 100.0) * factor) / 365.0
    each_reward = (total_group_reward / count) if count > 0 else 0.0

    mem["Factor"] = factor
    mem["DailyAPR"] = each_reward
    mem["CalcMode"] = "GROUP_EQUAL"
    return mem


# -----------------------------
# UI: Dashboard
# -----------------------------
def ui_dashboard(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> None:
    st.subheader("📊 管理画面ダッシュボード")
    st.caption("総資産 / 本日APR / グループ別残高 / 個人残高 / LINE通知履歴")

    ledger_df = load_ledger(gs)

    active_mem = members_df.copy()
    if not active_mem.empty:
        active_mem = active_mem[active_mem["IsActive"] == True].copy()

    total_assets = float(active_mem["Principal"].sum()) if not active_mem.empty else 0.0

    today_prefix = now_jst().strftime("%Y-%m-%d")
    today_apr = 0.0
    if not ledger_df.empty:
        today_rows = ledger_df[ledger_df["Datetime_JST"].astype(str).str.startswith(today_prefix)].copy()
        today_apr = float(today_rows[today_rows["Type"] == "APR"]["Amount"].sum())

    group_df = pd.DataFrame()
    personal_df = pd.DataFrame()

    if not active_mem.empty:
        group_df = active_mem[active_mem["Project_Name"].astype(str).str.upper() != PERSONAL_PROJECT].copy()
        personal_df = active_mem[active_mem["Project_Name"].astype(str).str.upper() == PERSONAL_PROJECT].copy()

    c1, c2 = st.columns(2)
    with c1:
        st.metric("総資産", fmt_usd(total_assets))
    with c2:
        st.metric("本日APR", fmt_usd(today_apr))

    st.divider()

    c3, c4 = st.columns(2)

    with c3:
        st.markdown("#### グループ別残高")
        if group_df.empty:
            st.info("グループデータがありません。")
        else:
            group_summary = (
                group_df.groupby("Project_Name", as_index=False)
                .agg(人数=("PersonName", "count"), 総残高=("Principal", "sum"))
                .sort_values("総残高", ascending=False)
            )
            group_summary["総残高"] = group_summary["総残高"].apply(fmt_usd)
            st.dataframe(group_summary, use_container_width=True, hide_index=True)

    with c4:
        st.markdown("#### 個人残高")
        if personal_df.empty:
            st.info("PERSONAL データがありません。")
        else:
            p = personal_df[["PersonName", "Principal", "Rank", "LINE_DisplayName"]].copy()
            p["Principal"] = p["Principal"].apply(fmt_usd)
            p = p.rename(columns={
                "PersonName": "氏名",
                "Principal": "残高",
                "Rank": "ランク",
                "LINE_DisplayName": "LINE名",
            })
            st.dataframe(p, use_container_width=True, hide_index=True)

    st.divider()

    st.markdown("#### LINE通知履歴")
    if ledger_df.empty:
        st.info("通知履歴がありません。")
    else:
        hist = ledger_df.copy().sort_values("Datetime_JST", ascending=False)
        hist = hist[["Datetime_JST", "Project_Name", "PersonName", "Type", "Amount", "LINE_DisplayName", "Source", "Note"]].copy()
        hist["Amount"] = hist["Amount"].apply(fmt_usd)
        hist = hist.rename(columns={
            "Datetime_JST": "日時",
            "Project_Name": "プロジェクト",
            "PersonName": "氏名",
            "Type": "種別",
            "Amount": "金額",
            "LINE_DisplayName": "LINE名",
            "Source": "送信元",
            "Note": "備考",
        })
        st.dataframe(hist.head(50), use_container_width=True, hide_index=True)


# -----------------------------
# UI: APR
# -----------------------------
def ui_apr(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> None:
    st.subheader("📈 APR 確定")
    st.caption(f"{RANK_LABEL} / PERSONAL=個別計算 / GROUP=総額均等割 / 管理者: {current_admin_label()}")

    projects = active_projects(settings_df)
    if not projects:
        st.warning("有効（Active=TRUE）のプロジェクトがありません。Settingsを確認してください。")
        return

    project = st.selectbox("プロジェクト", projects)
    row = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]
    is_compound = bool(row["IsCompound"])
    project_net_factor = float(row.get("Net_Factor", 0.67))

    apr = st.number_input("本日のAPR（%）", value=100.0, step=0.1)
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])

    mem = project_members_active(members_df, project)
    if mem.empty:
        st.warning("このプロジェクトに 🟢運用中 のメンバーがいません（Membersを確認）。")
        return

    mem = calc_project_apr(
        mem=mem,
        apr_percent=float(apr),
        project_net_factor=project_net_factor,
        project_name=str(project),
    )

    total_principal = float(mem["Principal"].sum())
    total_reward = float(mem["DailyAPR"].sum())
    n_total = len(mem)
    n_master = int((mem["Rank"] == "Master").sum()) if "Rank" in mem.columns else 0
    n_elite = int((mem["Rank"] == "Elite").sum()) if "Rank" in mem.columns else 0

    if str(project).strip().upper() == PERSONAL_PROJECT:
        mode_label = "個別計算"
    else:
        mode_label = f"グループ総額均等割（Net_Factor={project_net_factor:.2f}）"

    st.write(f"- 総元本: {fmt_usd(total_principal)}")
    st.write(f"- 人数: {n_total}（Master {n_master} / Elite {n_elite}）")
    st.write(f"- 本日総配当（合計）: {fmt_usd(total_reward)}")
    st.write(f"- APR計算モード: {mode_label}")
    st.write(f"- 元本反映モード: {'複利（元本に加算）' if is_compound else '単利（元本は固定）'}")

    with st.expander("個人別の本日配当（確認）", expanded=False):
        show = mem[["PersonName", "Rank", "Principal", "DailyAPR", "Line_User_ID", "LINE_DisplayName"]].copy()
        show["Principal"] = show["Principal"].apply(lambda x: fmt_usd(float(x)))
        show["DailyAPR"] = show["DailyAPR"].apply(lambda x: fmt_usd(float(x)))
        st.dataframe(show, use_container_width=True, hide_index=True)

    if st.button("APRを確定して全員にLINE送信"):
        evidence_url = None
        if uploaded:
            with st.spinner("画像アップロード中..."):
                evidence_url = upload_imgbb(uploaded.getvalue())
            if not evidence_url:
                st.error("画像アップロードに失敗しました（ImgBB）。画像を外して再実行してください。")
                return

        ts = fmt_dt(now_jst())

        for _, r in mem.iterrows():
            note = (
                f"APR:{apr}%, "
                f"Mode:{r.get('CalcMode','')}, "
                f"Rank:{r['Rank']}, "
                f"Factor:{r['Factor']}"
            )
            gs.append_row(gs.cfg.ledger_sheet, [
                ts,
                project,
                r["PersonName"],
                "APR",
                float(r["DailyAPR"]),
                note,
                evidence_url or "",
                r["Line_User_ID"],
                r["LINE_DisplayName"],
                "app",
            ])

        if is_compound:
            mem_map = {str(r["PersonName"]).strip(): float(r["DailyAPR"]) for _, r in mem.iterrows()}
            for i in range(len(members_df)):
                if members_df.loc[i, "Project_Name"] == str(project) and truthy(members_df.loc[i, "IsActive"]):
                    pn = str(members_df.loc[i, "PersonName"]).strip()
                    addv = float(mem_map.get(pn, 0.0))
                    if addv != 0.0:
                        members_df.loc[i, "Principal"] = float(members_df.loc[i, "Principal"]) + addv
                        members_df.loc[i, "UpdatedAt_JST"] = ts
            write_members(gs, members_df)

        ns = str(st.session_state.get("admin_namespace", "")).strip() or "default"
        token = get_line_token(ns)
        targets = dedup_line_ids(mem)

        msg = "🏦【APR収益報告】\n"
        msg += f"プロジェクト: {project}\n"
        msg += f"報告日時: {now_jst().strftime('%Y/%m/%d %H:%M')}\n\n"
        msg += f"APR: {apr}%\n"
        if str(project).strip().upper() == PERSONAL_PROJECT:
            msg += "方式: PERSONAL 個別計算\n"
            msg += f"{RANK_LABEL}\n"
        else:
            msg += "方式: グループ総額均等割\n"
            msg += f"Net_Factor: {project_net_factor:.2f}\n"
        msg += f"人数: {n_total}\n"
        msg += f"本日総配当: {fmt_usd(total_reward)}\n"
        msg += f"元本反映: {'複利' if is_compound else '単利'}\n"
        if evidence_url:
            msg += "\n📎 エビデンス画像を添付します。"

        success, fail = 0, 0
        for uid in targets:
            code = send_line_push(token, uid, msg, evidence_url)
            if code == 200:
                success += 1
            else:
                fail += 1

        gs.clear_cache()
        st.success(f"送信完了（成功:{success} / 失敗:{fail}）")
        st.rerun()


# -----------------------------
# UI: Deposit/Withdraw
# -----------------------------
def ui_cash(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> None:
    st.subheader("💸 入金 / 出金（個別LINE通知）")
    st.caption(f"{RANK_LABEL} / 管理者: {current_admin_label()}")

    projects = active_projects(settings_df)
    if not projects:
        st.warning("有効（Active=TRUE）のプロジェクトがありません。Settingsを確認してください。")
        return

    project = st.selectbox("プロジェクト", projects, key="cash_project")

    mem = project_members_active(members_df, project)
    if mem.empty:
        st.warning("このプロジェクトに 🟢運用中 のメンバーがいません（Membersを確認）。")
        return

    person = st.selectbox("メンバー（🟢運用中のみ）", mem["PersonName"].tolist())
    row = mem[mem["PersonName"] == person].iloc[0]
    current = float(row["Principal"])
    st.info(f"現在残高: {fmt_usd(current)} / Rank: {normalize_rank(row.get('Rank', 'Master'))} / {STATUS_ON}")

    typ = st.selectbox("種別", ["Deposit", "Withdraw"])
    amt = st.number_input("金額", min_value=0.0, value=0.0, step=100.0)
    note = st.text_input("メモ（任意）", value="")
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"], key="cash_img")

    if st.button("確定して保存＆個別にLINE通知"):
        if amt <= 0:
            st.warning("金額が0です。")
            return
        if typ == "Withdraw" and float(amt) > current:
            st.error("出金額が現在残高を超えています。")
            return

        evidence_url = None
        if uploaded:
            with st.spinner("画像アップロード中..."):
                evidence_url = upload_imgbb(uploaded.getvalue())
            if not evidence_url:
                st.error("画像アップロードに失敗しました（ImgBB）。画像を外して再実行してください。")
                return

        new_balance = current + float(amt) if typ == "Deposit" else current - float(amt)
        ts = fmt_dt(now_jst())

        for i in range(len(members_df)):
            if members_df.loc[i, "Project_Name"] == str(project) and str(members_df.loc[i, "PersonName"]).strip() == str(person).strip():
                members_df.loc[i, "Principal"] = float(new_balance)
                members_df.loc[i, "UpdatedAt_JST"] = ts

        gs.append_row(gs.cfg.ledger_sheet, [
            ts, project, person, typ, float(amt), note, evidence_url or "",
            row["Line_User_ID"], row["LINE_DisplayName"], "app"
        ])

        write_members(gs, members_df)

        ns = str(st.session_state.get("admin_namespace", "")).strip() or "default"
        token = get_line_token(ns)

        msg = "💸【入出金通知】\n"
        msg += f"プロジェクト: {project}\n"
        msg += f"日時: {now_jst().strftime('%Y/%m/%d %H:%M')}\n"
        msg += f"種別: {typ}\n"
        msg += f"金額: {fmt_usd(float(amt))}\n"
        msg += f"更新後残高: {fmt_usd(float(new_balance))}\n"
        if note:
            msg += f"\nメモ: {note}"
        if evidence_url:
            msg += "\n\n📎 エビデンス画像を添付します。"

        code = send_line_push(token, str(row["Line_User_ID"]).strip(), msg, evidence_url)

        gs.clear_cache()
        if code == 200:
            st.success("保存＆送信完了")
        else:
            st.warning(f"保存は完了。LINE送信が失敗（HTTP {code}）")
        st.rerun()


# -----------------------------
# UI: Admin
# -----------------------------
def ui_admin(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("⚙️ 管理")
    st.caption(f"{RANK_LABEL} / 管理者: {current_admin_label()}")

    projects = active_projects(settings_df)
    if not projects:
        st.warning("有効（Active=TRUE）のプロジェクトがありません。Settingsを確認してください。")
        return members_df

    project = st.selectbox("対象プロジェクト", projects, key="admin_project")

    line_users_df = load_line_users(gs)
    line_users: List[Tuple[str, str, str]] = []
    if not line_users_df.empty:
        tmp = line_users_df.copy()
        tmp = tmp[tmp["Line_User_ID"].astype(str).str.startswith("U")]
        tmp = tmp.drop_duplicates(subset=["Line_User_ID"], keep="last")
        for _, r in tmp.iterrows():
            uid = str(r["Line_User_ID"]).strip()
            name = str(r.get("Line_User", "")).strip()
            label = f"{name} ({uid})" if name else uid
            line_users.append((label, uid, name))

    st.divider()

    view_all = members_df[members_df["Project_Name"] == str(project)].copy()
    view_all["_row_id"] = view_all.index

    if view_all.empty:
        st.info("このプロジェクトにメンバーがいません。下のフォームから追加してください。")
    else:
        q = st.text_input("検索（PersonName / LINE名 / Line_User_ID）", value="")
        view = view_all.copy()
        if q.strip():
            qq = q.strip().lower()
            view = view[
                view["PersonName"].astype(str).str.lower().str.contains(qq, na=False)
                | view["LINE_DisplayName"].astype(str).str.lower().str.contains(qq, na=False)
                | view["Line_User_ID"].astype(str).str.lower().str.contains(qq, na=False)
            ]

        show = view.copy()
        show["Principal"] = show["Principal"].apply(lambda x: fmt_usd(float(x)))
        show["Rank"] = show["Rank"].apply(normalize_rank)
        show["状態"] = show["IsActive"].apply(bool_to_status)
        show = show.drop(columns=["IsActive", "_row_id"], errors="ignore")

        st.markdown("#### 現在のメンバー一覧")
        st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()

    st.markdown("#### 📨 メンバーから選択して個別にLINE送信（個人名 自動挿入）")

    if view_all.empty:
        st.info("メンバーがいないため送信できません。")
    else:
        target_mode = st.radio("対象", ["🟢運用中のみ", "全メンバー（停止含む）"], horizontal=True)
        cand = view_all.copy() if target_mode.startswith("全") else view_all[view_all["IsActive"] == True].copy()
        cand = cand.reset_index(drop=True)

        def _label(r: pd.Series) -> str:
            name = str(r.get("PersonName", "")).strip()
            disp = str(r.get("LINE_DisplayName", "")).strip()
            uid = str(r.get("Line_User_ID", "")).strip()
            stt = bool_to_status(r.get("IsActive", True))
            if disp:
                return f"{stt} {name} / {disp}"
            return f"{stt} {name} / {uid}"

        options = [_label(cand.loc[i]) for i in range(len(cand))]
        selected = st.multiselect("送信先（複数可）", options=options)

        default_msg = "【ご連絡】\n"
        default_msg += f"プロジェクト: {project}\n"
        default_msg += f"日時: {now_jst().strftime('%Y/%m/%d %H:%M')}\n\n"

        msg_common = st.text_area(
            "メッセージ本文（共通）※送信時に「〇〇 様」を自動挿入します",
            value=st.session_state.get("direct_line_msg", default_msg),
            height=180
        )
        st.session_state["direct_line_msg"] = msg_common

        img = st.file_uploader("添付画像（任意・ImgBB）", type=["png", "jpg", "jpeg"], key="direct_line_img")

        c1, c2 = st.columns([1, 1])
        with c1:
            do_send = st.button("選択メンバーへ送信", use_container_width=True)
        with c2:
            clear_msg = st.button("本文を初期化", use_container_width=True)

        if clear_msg:
            st.session_state["direct_line_msg"] = default_msg
            st.rerun()

        if do_send:
            if not selected:
                st.warning("送信先を選択してください。")
            elif not msg_common.strip():
                st.warning("メッセージが空です。")
            else:
                evidence_url = None
                if img:
                    with st.spinner("画像アップロード中..."):
                        evidence_url = upload_imgbb(img.getvalue())
                    if not evidence_url:
                        st.error("画像アップロードに失敗しました（ImgBB）。画像を外して再実行してください。")
                        return members_df

                ns = str(st.session_state.get("admin_namespace", "")).strip() or "default"
                token = get_line_token(ns)

                label_to_row = {_label(cand.loc[i]): cand.loc[i] for i in range(len(cand))}

                success, fail = 0, 0
                failed_list = []

                for lab in selected:
                    r = label_to_row.get(lab)
                    if r is None:
                        fail += 1
                        failed_list.append(lab)
                        continue

                    uid = str(r.get("Line_User_ID", "")).strip()
                    person_name = str(r.get("PersonName", "")).strip()

                    if not is_line_uid(uid):
                        fail += 1
                        failed_list.append(f"{lab}（Line_User_ID不正）")
                        continue

                    personalized = insert_person_name(msg_common, person_name)
                    code = send_line_push(token, uid, personalized, evidence_url)

                    if code == 200:
                        success += 1
                    else:
                        fail += 1
                        failed_list.append(f"{lab}（HTTP {code}）")

                if fail == 0:
                    st.success(f"送信完了（成功:{success} / 失敗:{fail}）")
                else:
                    st.warning(f"送信結果（成功:{success} / 失敗:{fail}）")
                    with st.expander("失敗詳細", expanded=False):
                        st.write("\n".join(failed_list))

    st.divider()

    if not view_all.empty:
        st.markdown("#### ワンタップで 🟢運用中 / 🔴停止 を切替")
        names = view_all["PersonName"].astype(str).tolist()
        pick = st.selectbox("対象メンバー", names, key="toggle_member")
        cur_row = view_all[view_all["PersonName"] == pick].iloc[0]
        cur_status = bool_to_status(cur_row["IsActive"])

        c1, c2 = st.columns([2, 1])
        with c1:
            st.write(f"現在: **{cur_status}**")
        with c2:
            if st.button("切替", use_container_width=True):
                ts = fmt_dt(now_jst())
                row_id = int(cur_row["_row_id"])
                members_df.loc[row_id, "IsActive"] = (not truthy(members_df.loc[row_id, "IsActive"]))
                members_df.loc[row_id, "UpdatedAt_JST"] = ts

                msg2 = validate_no_dup_lineid_within_project(members_df, project)
                if msg2:
                    st.error(msg2)

                write_members(gs, members_df)
                gs.clear_cache()
                st.success("更新しました。")
                st.rerun()

    st.divider()

    if not view_all.empty:
        st.markdown("#### 一括編集（保存ボタンで確定）")

        edit_src = view_all.copy()
        edit_src["状態"] = edit_src["IsActive"].apply(bool_to_status)

        edit_show = edit_src[
            ["PersonName", "Principal", "Rank", "状態", "Line_User_ID", "LINE_DisplayName"]
        ].copy()

        row_ids = edit_src["_row_id"].tolist()

        edited = st.data_editor(
            edit_show,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            column_config={
                "Principal": st.column_config.NumberColumn("Principal", min_value=0.0, step=100.0),
                "Rank": st.column_config.SelectboxColumn("Rank", options=["Master", "Elite"]),
                "状態": st.column_config.SelectboxColumn("状態", options=[STATUS_ON, STATUS_OFF]),
            },
            key=f"members_editor_{project}",
        )

        c1, c2 = st.columns([1, 1])
        with c1:
            save = st.button("編集内容を保存", use_container_width=True, key=f"save_members_{project}")
        with c2:
            cancel = st.button("編集を破棄（再読み込み）", use_container_width=True, key=f"cancel_members_{project}")

        if cancel:
            gs.clear_cache()
            st.rerun()

        if save:
            ts = fmt_dt(now_jst())
            edited = edited.copy()
            edited["_row_id"] = row_ids

            for _, r in edited.iterrows():
                row_id = int(r["_row_id"])
                members_df.loc[row_id, "Principal"] = float(to_f(r["Principal"]))
                members_df.loc[row_id, "Rank"] = normalize_rank(r["Rank"])
                members_df.loc[row_id, "IsActive"] = status_to_bool(r["状態"])
                members_df.loc[row_id, "Line_User_ID"] = str(r["Line_User_ID"]).strip()
                members_df.loc[row_id, "LINE_DisplayName"] = str(r["LINE_DisplayName"]).strip()
                members_df.loc[row_id, "UpdatedAt_JST"] = ts

            msg3 = validate_no_dup_lineid_within_project(members_df, project)
            if msg3:
                st.error(msg3)
                return members_df

            write_members(gs, members_df)
            gs.clear_cache()
            st.success("保存しました。")
            st.rerun()

    st.divider()

    st.markdown("#### 追加（同一プロジェクト内で Line_User_ID が一致したら『追加しない／更新もしない』）")

    add_mode = st.selectbox(
        "追加先",
        ["個人(PERSONAL)", "プロジェクト"],
        key="member_add_mode",
    )

    all_projects = active_projects(settings_df)

    if add_mode == "個人(PERSONAL)":
        selected_project = PERSONAL_PROJECT
        st.info("登録先: PERSONAL（個人運用）")
    else:
        project_candidates = [p for p in all_projects if str(p).strip().upper() != PERSONAL_PROJECT]
        if not project_candidates:
            st.warning("PERSONAL以外のプロジェクトがありません。Settingsを確認してください。")
            return members_df

        selected_project = st.selectbox(
            "登録するプロジェクト",
            project_candidates,
            key="member_add_target_project",
        )

    if line_users:
        labels = ["（選択しない）"] + [x[0] for x in line_users]
        picked = st.selectbox("登録済みLINEユーザーから選択（Make.comで追記された台帳）", labels, index=0)
        if picked != "（選択しない）":
            idx = labels.index(picked) - 1
            _, uid, name = line_users[idx]
            st.session_state["prefill_line_uid"] = uid
            st.session_state["prefill_line_name"] = name

    pre_uid = st.session_state.get("prefill_line_uid", "")
    pre_name = st.session_state.get("prefill_line_name", "")

    with st.form("member_add", clear_on_submit=False):
        person = st.text_input("PersonName（個人名）")
        principal = st.number_input("Principal（残高）", min_value=0.0, value=0.0, step=100.0)
        line_uid = st.text_input("Line_User_ID（Uから始まる）", value=pre_uid)
        line_disp = st.text_input("LINE_DisplayName（任意）", value=pre_name)
        rank = st.selectbox("Rank（取り分）", ["Master", "Elite"], index=0, help="Master=67% / Elite=60%")
        status = st.selectbox("ステータス", [STATUS_ON, STATUS_OFF], index=0)
        submit = st.form_submit_button("保存（追加）")

    if submit:
        if not person or not line_uid:
            st.error("PersonName と Line_User_ID は必須です。")
            return members_df

        if not is_line_uid(line_uid):
            st.warning("Line_User_ID の形式が不正の可能性があります（通常Uから始まる）。続行は可能です。")

        exists = members_df[
            (members_df["Project_Name"] == str(selected_project)) &
            (members_df["Line_User_ID"].astype(str).str.strip() == str(line_uid).strip())
        ]
        if not exists.empty:
            st.warning("このプロジェクト内に同じ Line_User_ID が既に存在します。追加・更新は行いません。")
            return members_df

        ts = fmt_dt(now_jst())
        new_row = {
            "Project_Name": str(selected_project).strip(),
            "PersonName": str(person).strip(),
            "Principal": float(principal),
            "Line_User_ID": str(line_uid).strip(),
            "LINE_DisplayName": str(line_disp).strip(),
            "Rank": normalize_rank(rank),
            "IsActive": status_to_bool(status),
            "CreatedAt_JST": ts,
            "UpdatedAt_JST": ts,
        }
        members_df = pd.concat([members_df, pd.DataFrame([new_row])], ignore_index=True)

        msg4 = validate_no_dup_lineid_within_project(members_df, selected_project)
        if msg4:
            st.error(msg4)
            return members_df

        write_members(gs, members_df)
        gs.clear_cache()

        st.session_state["page"] = "⚙️ 管理"
        st.success(f"追加しました。登録先: {selected_project}")
        st.rerun()

    return members_df


# -----------------------------
# UI: Help
# -----------------------------
def ui_help(gs: GSheets) -> None:
    st.subheader("❓ ヘルプ / 使い方")
    st.caption(f"{RANK_LABEL} / PERSONAL=個別計算 / GROUP=総額均等割 / 管理者: {current_admin_label()}")

    st.markdown(
        """
このアプリは、プロジェクトごとの残高管理・APR確定・入金/出金の履歴管理（Ledger）と、LINE通知を行います。  
左メニュー（サイドバー）の **📊ダッシュボード / 📈APR / 💸入金/出金 / ⚙️管理 / ❓ヘルプ** で画面を切り替えます。
"""
    )

    with st.expander("APR計算ロジック", expanded=False):
        st.markdown(
            """
### PERSONAL
日次配当 = `Principal × (APR% / 100) × Rank係数 ÷ 365`

### GROUP（PERSONAL以外）
グループ日次総配当 = `グループ総元本 × (APR% / 100) × Net_Factor ÷ 365`  
1人あたり日次配当 = `グループ日次総配当 ÷ グループ人数`
"""
        )

    with st.expander("シート構成", expanded=False):
        st.code("\t".join(SETTINGS_HEADERS))
        st.code("\t".join(MEMBERS_HEADERS))
        st.code("\t".join(LEDGER_HEADERS))
        st.code("\t".join(LINEUSERS_HEADERS))


# -----------------------------
# Main
# -----------------------------
def main():
    st.set_page_config(page_title="APR資産運用管理", layout="wide", page_icon="🏦")
    st.title("🏦 APR資産運用管理システム")

    require_admin_login_multi()

    st.markdown(
        """
        <style>
          section[data-testid="stSidebar"] div[role="radiogroup"] > label {
            margin: 10px 0 !important;
            padding: 6px 8px !important;
          }
          section[data-testid="stSidebar"] div[role="radiogroup"] > label p {
            font-size: 16px !important;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.caption(f"👤 {current_admin_label()}")
        if st.button("🔓 ログアウト", use_container_width=True):
            st.session_state["admin_ok"] = False
            st.session_state["admin_name"] = ""
            st.session_state["admin_namespace"] = ""
            st.rerun()

    if "page" not in st.session_state:
        st.session_state["page"] = "📊 ダッシュボード"

    con = st.secrets.get("connections", {}).get("gsheets", {})
    sid_raw = str(con.get("spreadsheet", "")).strip()
    sid = extract_sheet_id(sid_raw)
    if not sid:
        st.error("Secrets の [connections.gsheets].spreadsheet が未設定です（URLまたはID）。")
        st.stop()

    ns = str(st.session_state.get("admin_namespace", "")).strip() or "default"
    gs = GSheets(build_gs_config(spreadsheet_id=sid, ns=ns))

    try:
        settings_df = load_settings(gs)
        members_df = load_members(gs)
    except APIError as e:
        st.error(f"読み取りエラー: {e}")
        st.stop()

    menu = ["📊 ダッシュボード", "📈 APR", "💸 入金/出金", "⚙️ 管理", "❓ ヘルプ"]
    page = st.sidebar.radio(
        "メニュー",
        options=menu,
        index=menu.index(st.session_state["page"]) if st.session_state["page"] in menu else 0,
    )
    st.session_state["page"] = page

    if page == "📊 ダッシュボード":
        ui_dashboard(gs, settings_df, members_df)
    elif page == "📈 APR":
        ui_apr(gs, settings_df, members_df)
    elif page == "💸 入金/出金":
        ui_cash(gs, settings_df, members_df)
    elif page == "⚙️ 管理":
        ui_admin(gs, settings_df, members_df)
    else:
        ui_help(gs)


if __name__ == "__main__":
    main()
