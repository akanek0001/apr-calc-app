# app.py
# PRO版: 管理者ごとに分離して同じAPR運用ができるマルチ管理者版
# - 管理者ログインは「管理者選択 + PIN」
# - 管理者ごとに別シートでデータを分離（Settings/Members/Ledger/LineUsers を admin_namespace で分岐）
# - LINE token も admin_namespace に応じて切替（line.tokens[namespace]）
# - UI/機能は PRO版と同等（APR / 入出金 / 管理 / ヘルプ + サイドバー行間 + 個別LINE送信）
# - APR計算:
#     Master = 67%
#     Elite  = 60%
#     日次配当 = Principal × (APR% / 100) × Rank係数 ÷ 365
# - LineUsers シートは Line_User_ID / LineID のどちらでも読めるように対応

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

    ids = []
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
    """
    送信時に個人名「〇〇 様」を自動挿入する。
    - 1行目が「【ご連絡】」ならその次に挿入
    - それ以外なら先頭に挿入
    - すでに「〇〇 様」が含まれていれば二重挿入しない
    """
    name_line = f"{person_name} 様"
    lines = msg_common.splitlines()
    if name_line in lines:
        return msg_common

    if lines and lines[0].strip() == "【ご連絡】":
        return "\n".join([lines[0], name_line] + lines[1:])
    return "\n".join([name_line] + lines)


def sheet_name(base: str, ns: str) -> str:
    """
    管理者namespaceに応じて、シート名を分岐。
    - ns が "default" / 空: base そのまま
    - それ以外: base__<ns> 形式
    """
    ns = str(ns or "").strip()
    if not ns or ns == "default":
        return base
    return f"{base}__{ns}"


def get_line_token(ns: str) -> str:
    """
    LINE token を namespace ごとに切替する。
    secrets 推奨:
      [line]
      tokens = { A="...", B="...", C="...", D="..." }

    互換:
      [line]
      channel_access_token = "..."
    """
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
    "Type",          # APR / Deposit / Withdraw
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
    namespace: str  # A / B / C / D など


def load_admin_users() -> List[AdminUser]:
    """
    secrets例（推奨）:
    [admin]
    users = [
      { name="管理者A", pin="1111", namespace="A" },
      { name="管理者B", pin="2222", namespace="B" }
    ]

    旧形式互換:
    [admin]
    pin = "1234"
    """
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


def load_line_users(gs: GSheets) -> pd.DataFrame:
    """
    Make 側の列名差異を吸収:
    - 正式: Line_User_ID
    - 互換 : LineID
    """
    df = gs.read_df(gs.cfg.lineusers_sheet)
    if df.empty:
        return df

    # 列名互換
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
# UI: APR
# -----------------------------
def ui_apr(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> None:
    st.subheader("📈 APR 確定（Master=67% / Elite=60%）")
    st.caption(f"{RANK_LABEL} / 管理者: {current_admin_label()}")

    projects = active_projects(settings_df)
    if not projects:
        st.warning("有効（Active=TRUE）のプロジェクトがありません。Settingsを確認してください。")
        return

    project = st.selectbox("プロジェクト", projects)

    row = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]
    is_compound = bool(row["IsCompound"])

    apr = st.number_input("本日のAPR（%）", value=100.0, step=0.1)
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])

    mem = project_members_active(members_df, project)
    if mem.empty:
        st.warning("このプロジェクトに 🟢運用中 のメンバーがいません（Membersを確認）。")
        return

    mem = mem.copy()
    mem["Factor"] = mem["Rank"].apply(rank_to_factor)

    # APR計算
    mem["DailyAPR"] = mem.apply(
        lambda r: (float(r["Principal"]) * (apr / 100.0) * float(r["Factor"])) / 365.0,
        axis=1
    )

    total_principal = float(mem["Principal"].sum())
    total_reward = float(mem["DailyAPR"].sum())
    n_total = len(mem)
    n_master = int((mem["Rank"] == "Master").sum())
    n_elite = int((mem["Rank"] == "Elite").sum())

    st.write(f"- 総元本: {fmt_usd(total_principal)}")
    st.write(f"- 人数: {n_total}（Master {n_master} / Elite {n_elite}）")
    st.write(f"- 本日総配当（合計）: {fmt_usd(total_reward)}")
    st.write(f"- モード: {'複利（元本に加算）' if is_compound else '単利（元本は固定）'}")

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
            note = f"APR:{apr}%, Rank:{r['Rank']}({r['Factor']})"
            gs.append_row(gs.cfg.ledger_sheet, [
                ts, project, r["PersonName"], "APR", float(r["DailyAPR"]),
                note,
                evidence_url or "",
                r["Line_User_ID"], r["LINE_DisplayName"], "app",
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
        msg += f"{RANK_LABEL}\n"
        msg += f"人数: {n_total}（Master {n_master} / Elite {n_elite}）\n"
        msg += f"本日総配当: {fmt_usd(total_reward)}\n"
        msg += f"モード: {'複利' if is_compound else '単利'}\n"
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
    st.info(f"現在残高: {fmt_usd(current)} / Rank: {normalize_rank(row.get('Rank','Master'))} / {STATUS_ON}")

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

    # --- 一覧 ---
    view_all = project_members_all(members_df, project)
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
        show = show.drop(columns=["IsActive"])

        st.markdown("#### 現在のメンバー一覧")
        st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()

    # --- 個別LINE送信 ---
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

    # --- ワンタップ停止/再開 ---
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
                for i in range(len(members_df)):
                    if members_df.loc[i, "Project_Name"] == str(project) and str(members_df.loc[i, "PersonName"]).strip() == str(pick).strip():
                        members_df.loc[i, "IsActive"] = (not truthy(members_df.loc[i, "IsActive"]))
                        members_df.loc[i, "UpdatedAt_JST"] = ts

                msg2 = validate_no_dup_lineid_within_project(members_df, project)
                if msg2:
                    st.error(msg2)

                write_members(gs, members_df)
                gs.clear_cache()
                st.success("更新しました。")
                st.rerun()

    st.divider()

    # --- 一括編集 ---
    if not view_all.empty:
        st.markdown("#### 一括編集（保存ボタンで確定）")

        edit = view_all.copy()
        edit["状態"] = edit["IsActive"].apply(bool_to_status)

        edit_show = edit[
            ["PersonName", "Principal", "Rank", "状態", "Line_User_ID", "LINE_DisplayName"]
        ].copy()

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
            key="members_editor",
        )

        c1, c2 = st.columns([1, 1])
        with c1:
            save = st.button("編集内容を保存", use_container_width=True)
        with c2:
            cancel = st.button("編集を破棄（再読み込み）", use_container_width=True)

        if cancel:
            gs.clear_cache()
            st.rerun()

        if save:
            ts = fmt_dt(now_jst())
            upd_map = {str(r["PersonName"]).strip(): r for _, r in edited.iterrows()}

            for i in range(len(members_df)):
                if members_df.loc[i, "Project_Name"] != str(project):
                    continue
                pn = str(members_df.loc[i, "PersonName"]).strip()
                if pn not in upd_map:
                    continue
                r = upd_map[pn]
                members_df.loc[i, "Principal"] = float(to_f(r["Principal"]))
                members_df.loc[i, "Rank"] = normalize_rank(r["Rank"])
                members_df.loc[i, "IsActive"] = status_to_bool(r["状態"])
                members_df.loc[i, "Line_User_ID"] = str(r["Line_User_ID"]).strip()
                members_df.loc[i, "LINE_DisplayName"] = str(r["LINE_DisplayName"]).strip()
                members_df.loc[i, "UpdatedAt_JST"] = ts

            msg3 = validate_no_dup_lineid_within_project(members_df, project)
            if msg3:
                st.error(msg3)
                st.stop()

            write_members(gs, members_df)
            gs.clear_cache()
            st.success("保存しました。")
            st.rerun()

    st.divider()

    # --- 追加 ---
    st.markdown("#### 追加（同一プロジェクト内で Line_User_ID が一致したら『追加しない／更新もしない』）")

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
            (members_df["Project_Name"] == str(project)) &
            (members_df["Line_User_ID"].astype(str).str.strip() == str(line_uid).strip())
        ]
        if not exists.empty:
            st.warning("このプロジェクト内に同じ Line_User_ID が既に存在します。追加・更新は行いません。")
            return members_df

        ts = fmt_dt(now_jst())
        new_row = {
            "Project_Name": str(project).strip(),
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

        msg4 = validate_no_dup_lineid_within_project(members_df, project)
        if msg4:
            st.error(msg4)
            return members_df

        write_members(gs, members_df)
        gs.clear_cache()

        st.session_state["page"] = "⚙️ 管理"
        st.success("追加しました。")
        st.rerun()

    return members_df


# -----------------------------
# UI: Help
# -----------------------------
def ui_help(gs: GSheets) -> None:
    st.subheader("❓ ヘルプ / 使い方")
    st.caption(f"{RANK_LABEL} / 管理者: {current_admin_label()}")

    st.markdown(
        f"""
このアプリは、プロジェクトごとの残高管理・APR確定・入金/出金の履歴管理（Ledger）と、LINE通知を行います。  
左メニュー（サイドバー）の **📈APR / 💸入金/出金 / ⚙️管理 / ❓ヘルプ** で画面を切り替えます。

- **{STATUS_ON}**：APR計算対象 / LINE送信対象  
- **{STATUS_OFF}**：対象外（運用から外す）

⚙️管理の「個別LINE送信」は、送信時に **個人名（〇〇 様）** を自動挿入します。
"""
    )

    with st.expander("0) Make.com（LINE登録フローのゴール）", expanded=False):
        st.markdown(
            f"""
ゴール（完成形）  
`LINE Watch Events → HTTP(プロフィール取得) → Google Sheets(Add a Rowで追記)`

※重複は現在シート側で管理している前提でも運用可能です。  
このアプリは、その結果として作られる **{gs.cfg.lineusers_sheet}** シートを読み込み、⚙️管理の「追加」で自動入力に使います。
"""
        )

    with st.expander("1) 事前準備（最初だけ）", expanded=False):
        st.markdown(
            """
- **Google Sheets（編集者共有）**  
  サービスアカウント（`client_email`）を、対象スプレッドシートに **編集者** で共有してください。
- **Secrets（Streamlit Cloud）**  
  - `[connections.gsheets].spreadsheet`：スプレッドシートURLまたはID  
  - `[connections.gsheets.credentials]`：サービスアカウントJSONの各キー  
  - `[line].tokens`：LINE Messaging API（管理者namespaceごとに分ける）
  - `[imgbb].api_key`：ImgBB（画像添付するなら）
  - `[admin].users`：管理者ごとのPIN（推奨）
"""
        )

    with st.expander("2) シート構成（列名は変更しない）", expanded=False):
        st.markdown("### Settings（プロジェクト設定）")
        st.code("\t".join(SETTINGS_HEADERS))
        st.markdown("### Members（メンバー台帳）")
        st.code("\t".join(MEMBERS_HEADERS))
        st.markdown("### Ledger（履歴）")
        st.code("\t".join(LEDGER_HEADERS))
        st.markdown("### LineUsers（Make.comで作るLINEユーザー登録台帳）")
        st.code("\t".join(LINEUSERS_HEADERS))
        st.info(
            f"※このマルチ管理者版では、管理者namespaceごとにシートが分かれます。\n"
            f"今の管理者の実シート名:\n"
            f"- {gs.cfg.settings_sheet}\n- {gs.cfg.members_sheet}\n- {gs.cfg.ledger_sheet}\n- {gs.cfg.lineusers_sheet}"
        )

    with st.expander("3) ⚙️管理の機能", expanded=False):
        st.markdown(
            f"""
- 検索でメンバーを絞り込み
- ワンタップで {STATUS_ON}/{STATUS_OFF} 切替
- 一括編集（Rank/残高/状態/LINE名など）→ 保存で反映
- メンバー選択で **個別LINE送信**（画像添付も可 / 個人名自動挿入）
- 追加はLineUsers台帳から選択すると Line_User_ID / LINE_DisplayName を自動入力

**重要（重複防止）**  
同一プロジェクト内で `Line_User_ID` が既に存在する場合、  
**追加もしない／更新もしない** 仕様です。
"""
        )

    with st.expander("4) APR計算ロジック", expanded=False):
        st.markdown(
            """
- Master = **67%**
- Elite = **60%**
- 日次配当 = `Principal × (APR% / 100) × Rank係数 ÷ 365`

例:
- Principal = 100,000
- 本日のAPR = 100%
- Rank = Master

`100,000 × 1.0 × 0.67 ÷ 365 ≒ 183.56`
"""
        )


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
        st.session_state["page"] = "📈 APR"

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

    menu = ["📈 APR", "💸 入金/出金", "⚙️ 管理", "❓ ヘルプ"]
    page = st.sidebar.radio(
        "メニュー",
        options=menu,
        index=menu.index(st.session_state["page"]) if st.session_state["page"] in menu else 0,
    )
    st.session_state["page"] = page

    if page == "📈 APR":
        ui_apr(gs, settings_df, members_df)
    elif page == "💸 入金/出金":
        ui_cash(gs, settings_df, members_df)
    elif page == "⚙️ 管理":
        ui_admin(gs, settings_df, members_df)
    else:
        ui_help(gs)


if __name__ == "__main__":
    main()
