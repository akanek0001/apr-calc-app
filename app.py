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


def now_jst() -> datetime:
    return datetime.now(JST)


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


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


def normalize_compound_timing(v: Any) -> str:
    s = str(v).strip().lower()
    if s in ("daily", "monthly", "none"):
        return s
    return "none"


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

    st.error("LINEトークンが未設定です。")
    st.stop()


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


SETTINGS_HEADERS = ["Project_Name", "Net_Factor", "IsCompound", "Compound_Timing", "UpdatedAt_JST", "Active"]

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
        st.error("Secrets に [admin].users または [admin].pin が未設定です。")
        st.stop()

    if st.session_state.get("admin_ok", False) and st.session_state.get("admin_namespace"):
        return

    st.markdown("## 🔐 管理者ログイン")

    names = [a.name for a in admins]

    with st.form("admin_gate_multi", clear_on_submit=False):
        admin_name = st.selectbox("管理者を選択", names)
        pw = st.text_input("管理者PIN", type="password")
        ok = st.form_submit_button("ログイン")

        if ok:
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
                st.error("PINが違います。")

    st.stop()


def current_admin_label() -> str:
    name = str(st.session_state.get("admin_name", "")).strip() or "Admin"
    ns = str(st.session_state.get("admin_namespace", "")).strip() or "default"
    return f"{name}（namespace: {ns}）"


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
            st.error(f"Spreadsheet を開けません。: {e}")
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
            ws = self.book.add_worksheet(title=name, rows=3000, cols=max(30, len(headers) + 10))
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


def load_settings(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.settings_sheet)
    if df.empty:
        return df

    if "Project_Name" not in df.columns:
        st.error(f"Settingsシート({gs.cfg.settings_sheet})に Project_Name 列がありません。")
        st.stop()

    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()

    if "Net_Factor" not in df.columns:
        df["Net_Factor"] = 0.67
    df["Net_Factor"] = df["Net_Factor"].apply(lambda x: to_f(x) if str(x).strip() else 0.67)

    if "IsCompound" not in df.columns:
        df["IsCompound"] = "FALSE"
    df["IsCompound"] = df["IsCompound"].apply(truthy)

    if "Compound_Timing" not in df.columns:
        df["Compound_Timing"] = "none"
    df["Compound_Timing"] = df["Compound_Timing"].apply(normalize_compound_timing)

    if "Active" not in df.columns:
        df["Active"] = "TRUE"
    df["Active"] = df["Active"].apply(truthy)

    if "UpdatedAt_JST" not in df.columns:
        df["UpdatedAt_JST"] = ""

    return df


def load_members(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.members_sheet)
    if df.empty:
        return df

    for c in MEMBERS_HEADERS:
        if c not in df.columns:
            df[c] = ""

    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
    df["PersonName"] = df["PersonName"].astype(str).str.strip()
    df["Principal"] = df["Principal"].apply(to_f)
    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df["LINE_DisplayName"] = df["LINE_DisplayName"].astype(str).str.strip()
    df["Rank"] = df["Rank"].apply(normalize_rank)
    df["IsActive"] = df["IsActive"].apply(truthy)
    return df


def load_ledger(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.ledger_sheet)
    if df.empty:
        return df
    for c in LEDGER_HEADERS:
        if c not in df.columns:
            df[c] = ""
    df["Amount"] = df["Amount"].apply(to_f)
    return df


def load_line_users(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.lineusers_sheet)
    if df.empty:
        return df

    if "Line_User_ID" not in df.columns and "LineID" in df.columns:
        df = df.rename(columns={"LineID": "Line_User_ID"})
    if "Line_User" not in df.columns and "LINE_DisplayName" in df.columns:
        df = df.rename(columns={"LINE_DisplayName": "Line_User"})

    if "Line_User_ID" not in df.columns:
        df["Line_User_ID"] = ""
    if "Line_User" not in df.columns:
        df["Line_User"] = ""

    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df["Line_User"] = df["Line_User"].astype(str).str.strip()
    return df


def active_projects(settings_df: pd.DataFrame) -> List[str]:
    if settings_df.empty:
        return []
    df = settings_df[settings_df["Active"] == True]
    return df["Project_Name"].dropna().astype(str).unique().tolist()


def project_members_active(members_df: pd.DataFrame, project: str) -> pd.DataFrame:
    if members_df.empty:
        return members_df.copy()
    return members_df[
        (members_df["Project_Name"] == str(project)) &
        (members_df["IsActive"] == True)
    ].copy().reset_index(drop=True)


def write_members(gs: GSheets, members_df: pd.DataFrame) -> None:
    out = members_df.copy()
    out["Principal"] = out["Principal"].apply(lambda x: f"{float(x):.6f}")
    out["IsActive"] = out["IsActive"].apply(lambda x: "TRUE" if truthy(x) else "FALSE")
    out["Rank"] = out["Rank"].apply(normalize_rank)
    gs.write_df(gs.cfg.members_sheet, out)


def validate_no_dup_lineid_within_project(members_df: pd.DataFrame, project: str) -> Optional[str]:
    if members_df.empty:
        return None
    df = members_df[members_df["Project_Name"] == str(project)].copy()
    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df = df[df["Line_User_ID"] != ""]
    dup = df[df.duplicated(subset=["Line_User_ID"], keep=False)]
    if dup.empty:
        return None
    ids = dup["Line_User_ID"].unique().tolist()
    return f"同一プロジェクト内で Line_User_ID が重複しています: {ids}"


def calc_project_apr(mem: pd.DataFrame, apr_percent: float, project_net_factor: float, project_name: str) -> pd.DataFrame:
    mem = mem.copy()

    if str(project_name).strip().upper() == PERSONAL_PROJECT:
        mem["Factor"] = mem["Rank"].apply(rank_to_factor)
        mem["DailyAPR"] = mem.apply(
            lambda r: (float(r["Principal"]) * (apr_percent / 100.0) * float(r["Factor"])) / 365.0,
            axis=1,
        )
        mem["CalcMode"] = "PERSONAL"
        return mem

    total_principal = float(mem["Principal"].sum())
    count = len(mem)
    factor = float(project_net_factor if project_net_factor > 0 else 0.67)
    total_group_reward = (total_principal * (apr_percent / 100.0) * factor) / 365.0
    each_reward = (total_group_reward / count) if count > 0 else 0.0

    mem["Factor"] = factor
    mem["DailyAPR"] = each_reward
    mem["CalcMode"] = "GROUP_EQUAL"
    return mem


def apply_monthly_compound(gs: GSheets, members_df: pd.DataFrame, project: str) -> Tuple[int, float]:
    ledger_df = load_ledger(gs)
    if ledger_df.empty:
        return 0, 0.0

    target = ledger_df[
        (ledger_df["Project_Name"].astype(str).str.strip() == str(project).strip()) &
        (ledger_df["Type"].astype(str).str.strip() == "APR") &
        (~ledger_df["Note"].astype(str).str.contains("COMPOUNDED", na=False))
    ].copy()

    if target.empty:
        return 0, 0.0

    sums = target.groupby("PersonName", as_index=False)["Amount"].sum()
    if sums.empty:
        return 0, 0.0

    ts = fmt_dt(now_jst())
    updated_count = 0
    total_added = 0.0

    for _, row in sums.iterrows():
        person = str(row["PersonName"]).strip()
        addv = float(row["Amount"])
        if addv == 0:
            continue

        mask = (
            members_df["Project_Name"].astype(str).str.strip() == str(project).strip()
        ) & (
            members_df["PersonName"].astype(str).str.strip() == person
        )
        idxs = members_df[mask].index.tolist()
        if not idxs:
            continue

        idx = idxs[0]
        members_df.loc[idx, "Principal"] = float(members_df.loc[idx, "Principal"]) + addv
        members_df.loc[idx, "UpdatedAt_JST"] = ts
        updated_count += 1
        total_added += addv

    if updated_count > 0:
        write_members(gs, members_df)

        ws = gs._ws(gs.cfg.ledger_sheet)
        values = ws.get_all_values()
        if values and len(values) >= 2:
            headers = values[0]
            note_idx = headers.index("Note") + 1 if "Note" in headers else None
            if note_idx:
                for row_no in range(2, len(values) + 1):
                    row = values[row_no - 1]
                    if len(row) < len(headers):
                        row = row + [""] * (len(headers) - len(row))

                    r_project = str(row[headers.index("Project_Name")]).strip()
                    r_type = str(row[headers.index("Type")]).strip()
                    r_note = str(row[headers.index("Note")]).strip()

                    if r_project == str(project).strip() and r_type == "APR" and "COMPOUNDED" not in r_note:
                        new_note = (r_note + " | " if r_note else "") + f"COMPOUNDED:{ts}"
                        ws.update_cell(row_no, note_idx, new_note)

        gs.clear_cache()

    return updated_count, total_added


def ui_dashboard(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> None:
    st.subheader("📊 管理画面ダッシュボード")

    ledger_df = load_ledger(gs)

    active_mem = members_df.copy()
    if not active_mem.empty:
        active_mem = active_mem[active_mem["IsActive"] == True].copy()

    total_assets = float(active_mem["Principal"].sum()) if not active_mem.empty else 0.0

    today_prefix = fmt_date(now_jst())
    today_apr = 0.0
    if not ledger_df.empty and "Datetime_JST" in ledger_df.columns:
        today_rows = ledger_df[ledger_df["Datetime_JST"].astype(str).str.startswith(today_prefix)].copy()
        today_apr = float(today_rows[today_rows["Type"] == "APR"]["Amount"].sum())

    c1, c2 = st.columns(2)
    with c1:
        st.metric("総資産", fmt_usd(total_assets))
    with c2:
        st.metric("本日APR", fmt_usd(today_apr))


def ui_apr(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> None:
    st.subheader("📈 APR 確定")
    st.caption(f"{RANK_LABEL} / PERSONAL=個別計算 / GROUP=総額均等割 / 管理者: {current_admin_label()}")

    projects = active_projects(settings_df)
    if not projects:
        st.warning("有効（Active=TRUE）のプロジェクトがありません。")
        return

    project = st.selectbox("プロジェクト", projects)
    row = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]
    project_net_factor = float(row.get("Net_Factor", 0.67))
    compound_timing = normalize_compound_timing(row.get("Compound_Timing", "none"))

    apr = st.number_input("本日のAPR（%）", value=100.0, step=0.1)
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])

    mem = project_members_active(members_df, project)
    if mem.empty:
        st.warning("このプロジェクトに 🟢運用中 のメンバーがいません。")
        return

    mem = calc_project_apr(mem, float(apr), project_net_factor, project)

    total_principal = float(mem["Principal"].sum())
    total_reward = float(mem["DailyAPR"].sum())
    n_total = len(mem)

    st.write(f"- 総元本: {fmt_usd(total_principal)}")
    st.write(f"- 人数: {n_total}")
    st.write(f"- 本日総配当: {fmt_usd(total_reward)}")
    st.write(f"- Compound_Timing: {compound_timing}")

    with st.expander("個人別の本日配当（確認）", expanded=False):
        show = mem[["PersonName", "Rank", "Principal", "DailyAPR", "Line_User_ID", "LINE_DisplayName"]].copy()
        show["Principal"] = show["Principal"].apply(fmt_usd)
        show["DailyAPR"] = show["DailyAPR"].apply(fmt_usd)
        st.dataframe(show, use_container_width=True, hide_index=True)

    if st.button("APRを確定して全員にLINE送信"):
        evidence_url = None
        if uploaded:
            evidence_url = upload_imgbb(uploaded.getvalue())
            if uploaded and not evidence_url:
                st.error("画像アップロードに失敗しました。")
                return

        ts = fmt_dt(now_jst())

        for _, r in mem.iterrows():
            note = f"APR:{apr}%, Mode:{r['CalcMode']}, Rank:{r['Rank']}, Factor:{r['Factor']}, CompoundTiming:{compound_timing}"
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

        if compound_timing == "daily":
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

        msg = "🏦【APR収益報告】\n"
        msg += f"プロジェクト: {project}\n"
        msg += f"報告日時: {now_jst().strftime('%Y/%m/%d %H:%M')}\n"
        msg += f"APR: {apr}%\n"
        msg += f"人数: {n_total}\n"
        msg += f"本日総配当: {fmt_usd(total_reward)}\n"
        msg += f"Compound_Timing: {compound_timing}\n"

        success, fail = 0, 0
        for uid in dedup_line_ids(mem):
            code = send_line_push(token, uid, msg, evidence_url)
            if code == 200:
                success += 1
            else:
                fail += 1

        gs.clear_cache()
        st.success(f"送信完了（成功:{success} / 失敗:{fail}）")
        st.rerun()

    if compound_timing == "monthly":
        st.divider()
        st.markdown("#### 月次複利反映")
        if st.button("未反映APRを元本へ反映"):
            count, total_added = apply_monthly_compound(gs, members_df, project)
            if count == 0:
                st.info("未反映のAPRはありません。")
            else:
                st.success(f"{count}名に反映しました。合計反映額: {fmt_usd(total_added)}")
            st.rerun()


def ui_help() -> None:
    st.subheader("❓ ヘルプ")
    st.markdown(
        """
- `daily` → APR確定時に元本へ即時加算
- `monthly` → Ledgerに記録のみ、ボタンで月次反映
- `none` → 単利、元本は固定
"""
    )


def main():
    st.set_page_config(page_title="APR資産運用管理", layout="wide", page_icon="🏦")
    st.title("🏦 APR資産運用管理システム")

    require_admin_login_multi()

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
        st.error("Secrets の [connections.gsheets].spreadsheet が未設定です。")
        st.stop()

    ns = str(st.session_state.get("admin_namespace", "")).strip() or "default"
    gs = GSheets(build_gs_config(spreadsheet_id=sid, ns=ns))

    settings_df = load_settings(gs)
    members_df = load_members(gs)

    menu = ["📊 ダッシュボード", "📈 APR", "❓ ヘルプ"]
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
    else:
        ui_help()


if __name__ == "__main__":
    main()
