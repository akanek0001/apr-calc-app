# app.py  (ページ状態保持 + ヘルプページ内蔵 + Master/Elite(67/60)対応 + LINE登録台帳(LineUsers)連携)
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, List, Optional

import json
import pandas as pd
import requests
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

JST = timezone(timedelta(hours=9), "JST")

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
    except:
        return 0.0

def truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on", "はい")

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
        except:
            pass
    return sid

def safe_str(x: Any) -> str:
    return "" if x is None else str(x)

def rank_to_factor(rank: str) -> float:
    r = (rank or "").strip().lower()
    if r == "master":
        return 0.67
    if r == "elite":
        return 0.60
    # 未設定時は Master 扱い（運用が止まらないように）
    return 0.67

def normalize_rank(rank: Any) -> str:
    r = str(rank).strip()
    if not r:
        return "Master"
    if r.lower() == "master":
        return "Master"
    if r.lower() == "elite":
        return "Elite"
    # それ以外の値が入っていても Master に寄せる
    return "Master"

# -----------------------------
# LINE
# -----------------------------
def send_line_push(token: str, user_id: str, text: str, image_url: Optional[str] = None) -> int:
    if not user_id:
        return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    messages = [{"type": "text", "text": text}]
    if image_url:
        messages.append({"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url})

    payload = {"to": str(user_id), "messages": messages}
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=25)
        return r.status_code
    except:
        return 500

# -----------------------------
# ImgBB
# -----------------------------
def upload_imgbb(file_bytes: bytes) -> Optional[str]:
    try:
        key = st.secrets["imgbb"]["api_key"]
    except:
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
    except:
        return None

# -----------------------------
# Sheets headers (※列名は変えない。足りない列だけ追加)
# -----------------------------
SETTINGS_HEADERS = ["Project_Name", "Net_Factor", "IsCompound", "UpdatedAt_JST", "Active"]
MEMBERS_HEADERS = [
    "Project_Name",
    "PersonName",
    "Principal",
    "Line_User_ID",
    "LINE_DisplayName",
    "Rank",          # Master / Elite（追加）
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
    "Source",        # app
]
# ★ Make.com の登録台帳用（LINE Watch Events → HTTP → Sheets に対応）
LINEUSERS_SHEET = "LineUsers"
LINEUSERS_HEADERS = ["Date", "Time", "Type", "Line_User_ID", "Line_User"]

@dataclass
class GSheetsConfig:
    spreadsheet_id: str
    settings_sheet: str = "Settings"
    members_sheet: str = "Members"
    ledger_sheet: str = "Ledger"
    lineusers_sheet: str = LINEUSERS_SHEET

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

        try:
            self.book = self.gc.open_by_key(self.cfg.spreadsheet_id)
        except Exception as e:
            st.error(f"Spreadsheet を開けません。共有設定（編集者）とIDを確認してください。: {e}")
            st.stop()

        # シートとヘッダー（足りない列だけ追加）
        self._ensure_sheet(self.cfg.settings_sheet, SETTINGS_HEADERS)
        self._ensure_sheet(self.cfg.members_sheet, MEMBERS_HEADERS)
        self._ensure_sheet(self.cfg.ledger_sheet, LEDGER_HEADERS)
        # ★ LINE登録台帳
        self._ensure_sheet(self.cfg.lineusers_sheet, LINEUSERS_HEADERS)

    def _ws(self, name: str):
        return self.book.worksheet(name)

    def _ensure_sheet(self, name: str, headers: List[str]) -> None:
        try:
            ws = self._ws(name)
        except Exception:
            ws = self.book.add_worksheet(title=name, rows=1000, cols=max(20, len(headers) + 5))
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
# Admin Auth（pin優先、なければpassword）
# -----------------------------
def admin_secret() -> str:
    a = st.secrets.get("admin", {})
    pin = str(a.get("pin", "")).strip()
    pw  = str(a.get("password", "")).strip()
    return pin or pw

def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))

def admin_login_ui() -> None:
    required = admin_secret()
    if not required:
        st.warning("Secrets に [admin].pin（または password）が未設定です。")
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

    with st.form("admin_login", clear_on_submit=False):
        pw = st.text_input("管理者PIN", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if pw == required:
                st.session_state["admin_ok"] = True
                st.rerun()
            else:
                st.session_state["admin_ok"] = False
                st.error("PINが違います。")

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
        st.error(f"Settingsシートの列が不足: {missing}")
        st.stop()

    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()

    # Active（任意）: 無ければ全部TRUE扱い
    if "Active" not in df.columns:
        df["Active"] = "TRUE"
    df["Active"] = df["Active"].apply(truthy)

    # Net_Factorは残しておく（他で使っていても列名は変えない）
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
        st.error(f"Membersシートの列が不足: {missing}")
        st.stop()

    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
    df["PersonName"] = df["PersonName"].astype(str).str.strip()
    df["Principal"] = df["Principal"].apply(to_f)
    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df["LINE_DisplayName"] = df["LINE_DisplayName"].astype(str).str.strip()
    df["IsActive"] = df["IsActive"].apply(truthy)

    # Rank（無い/空ならMaster）
    if "Rank" not in df.columns:
        df["Rank"] = "Master"
    df["Rank"] = df["Rank"].apply(normalize_rank)

    # タイムスタンプ列（無ければ空で保持）
    for c in ["CreatedAt_JST", "UpdatedAt_JST"]:
        if c not in df.columns:
            df[c] = ""

    return df

def load_line_users(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.lineusers_sheet)
    if df.empty:
        return df

    need = ["Line_User_ID", "Line_User"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        # Make.com 側の列名が違うケースはここで止める
        st.error(f"{gs.cfg.lineusers_sheet} シートの列が不足: {missing}（Make.comのAdd a Rowの列名を合わせてください）")
        return pd.DataFrame()

    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df["Line_User"] = df["Line_User"].astype(str).str.strip()
    return df

def project_members(members_df: pd.DataFrame, project: str) -> pd.DataFrame:
    df = members_df.copy()
    df = df[(df["Project_Name"] == str(project)) & (df["IsActive"] == True)]
    return df.reset_index(drop=True)

def dedup_line_ids(df: pd.DataFrame) -> List[str]:
    ids = []
    if "Line_User_ID" not in df.columns:
        return []
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

def active_projects(settings_df: pd.DataFrame) -> List[str]:
    if settings_df.empty:
        return []
    df = settings_df[settings_df["Active"] == True]
    return df["Project_Name"].dropna().astype(str).unique().tolist()

# -----------------------------
# UI: APR
# -----------------------------
def ui_apr(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("📈 APR 確定（Master=67% / Elite=60%）")

    projects = active_projects(settings_df)
    if not projects:
        st.warning("有効（Active=TRUE）のプロジェクトがありません。Settingsを確認してください。")
        return members_df

    project = st.selectbox("プロジェクト", projects)

    row = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]
    is_compound = bool(row["IsCompound"])

    apr = st.number_input("本日のAPR（%）", value=100.0, step=0.1)
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])

    mem = project_members(members_df, project)
    if mem.empty:
        st.warning("このプロジェクトに有効メンバーがいません（Membersを確認）。")
        return members_df

    mem = mem.copy()
    mem["Factor"] = mem["Rank"].apply(rank_to_factor)
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

    if not is_admin():
        st.info("APR確定は管理者のみ実行できます（⚙️管理でログイン）。")
        return members_df

    if st.button("APRを確定して全員にLINE送信"):
        evidence_url = None
        if uploaded:
            with st.spinner("画像アップロード中..."):
                evidence_url = upload_imgbb(uploaded.getvalue())
            if not evidence_url:
                st.error("画像アップロードに失敗しました（ImgBB）。画像を外して再実行してください。")
                return members_df

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
            for i in range(len(members_df)):
                if members_df.loc[i, "Project_Name"] == str(project) and bool(members_df.loc[i, "IsActive"]) is True:
                    pn = str(members_df.loc[i, "PersonName"]).strip()
                    mrow = mem[mem["PersonName"] == pn]
                    if not mrow.empty:
                        addv = float(mrow.iloc[0]["DailyAPR"])
                        members_df.loc[i, "Principal"] = float(members_df.loc[i, "Principal"]) + addv
                        members_df.loc[i, "UpdatedAt_JST"] = ts

            out = members_df.copy()
            out["Principal"] = out["Principal"].apply(lambda x: f"{float(x):.6f}")
            out["IsActive"] = out["IsActive"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
            out["Rank"] = out["Rank"].apply(normalize_rank)
            gs.write_df(gs.cfg.members_sheet, out)

        token = st.secrets["line"]["channel_access_token"]
        targets = dedup_line_ids(mem)

        msg = "🏦【APR収益報告】\n"
        msg += f"プロジェクト: {project}\n"
        msg += f"報告日時: {now_jst().strftime('%Y/%m/%d %H:%M')}\n\n"
        msg += f"APR: {apr}%\n"
        msg += f"Master: 67% / Elite: 60%\n"
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

    return members_df

# -----------------------------
# UI: Deposit/Withdraw
# -----------------------------
def ui_cash(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("💸 入金 / 出金（個別LINE通知）")

    projects = active_projects(settings_df)
    if not projects:
        st.warning("有効（Active=TRUE）のプロジェクトがありません。Settingsを確認してください。")
        return members_df

    project = st.selectbox("プロジェクト", projects, key="cash_project")

    mem = project_members(members_df, project)
    if mem.empty:
        st.warning("このプロジェクトに有効メンバーがいません（Membersを確認）。")
        return members_df

    person = st.selectbox("メンバー", mem["PersonName"].tolist())
    row = mem[mem["PersonName"] == person].iloc[0]
    current = float(row["Principal"])
    st.info(f"現在残高: {fmt_usd(current)} / Rank: {normalize_rank(row.get('Rank','Master'))}")

    typ = st.selectbox("種別", ["Deposit", "Withdraw"])
    amt = st.number_input("金額", min_value=0.0, value=0.0, step=100.0)
    note = st.text_input("メモ（任意）", value="")
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"], key="cash_img")

    if not is_admin():
        st.info("入金/出金の記録は管理者のみ実行できます（⚙️管理でログイン）。")
        return members_df

    if st.button("確定して保存＆個別にLINE通知"):
        if amt <= 0:
            st.warning("金額が0です。")
            return members_df

        evidence_url = None
        if uploaded:
            with st.spinner("画像アップロード中..."):
                evidence_url = upload_imgbb(uploaded.getvalue())
            if not evidence_url:
                st.error("画像アップロードに失敗しました（ImgBB）。画像を外して再実行してください。")
                return members_df

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

        out = members_df.copy()
        out["Principal"] = out["Principal"].apply(lambda x: f"{float(x):.6f}")
        out["IsActive"] = out["IsActive"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        out["Rank"] = out["Rank"].apply(normalize_rank)
        gs.write_df(gs.cfg.members_sheet, out)

        token = st.secrets["line"]["channel_access_token"]
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

    return members_df

# -----------------------------
# UI: Admin
# -----------------------------
def ui_admin(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("⚙️ 管理（管理者のみ）")
    admin_login_ui()

    if not is_admin():
        st.info("ログインすると、メンバー追加・編集が表示されます。")
        return members_df

    projects = active_projects(settings_df)
    if not projects:
        st.warning("有効（Active=TRUE）のプロジェクトがありません。Settingsを確認してください。")
        return members_df

    # ★ LINE登録台帳の読み込み（Make.comの成果物）
    line_users_df = load_line_users(gs)
    line_users = []
    if not line_users_df.empty:
        tmp = line_users_df.copy()
        tmp = tmp[(tmp["Line_User_ID"].astype(str).str.startswith("U"))]
        tmp = tmp.drop_duplicates(subset=["Line_User_ID"], keep="last")
        for _, r in tmp.iterrows():
            uid = str(r["Line_User_ID"]).strip()
            name = str(r.get("Line_User", "")).strip()
            label = f"{name} ({uid})" if name else uid
            line_users.append((label, uid, name))

    project = st.selectbox("対象プロジェクト", projects, key="admin_project")

    with st.expander("現在のメンバー一覧", expanded=True):
        view = members_df[members_df["Project_Name"] == str(project)].copy()
        if view.empty:
            st.info("まだメンバーがいません。下のフォームから追加してください。")
        else:
            show = view.copy()
            show["Principal"] = show["Principal"].apply(lambda x: fmt_usd(float(x)))
            show["Rank"] = show["Rank"].apply(normalize_rank)
            st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()

    st.write("#### 追加（同一プロジェクト内で Line_User_ID が一致したら『追加しない／更新もしない』）")

    # ★ 登録済みLINEユーザーから自動入力
    if line_users:
        labels = ["（選択しない）"] + [x[0] for x in line_users]
        picked = st.selectbox("登録済みLINEユーザーから選択（Make.comで追加された台帳）", labels, index=0)
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
        is_active = st.selectbox("IsActive", ["TRUE", "FALSE"], index=0)
        submit = st.form_submit_button("保存（追加）")

    if submit:
        if not person or not line_uid:
            st.error("PersonName と Line_User_ID は必須です。")
            return members_df

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
            "IsActive": True if is_active == "TRUE" else False,
            "CreatedAt_JST": ts,
            "UpdatedAt_JST": ts,
        }
        members_df = pd.concat([members_df, pd.DataFrame([new_row])], ignore_index=True)

        out = members_df.copy()
        out["Principal"] = out["Principal"].apply(lambda x: f"{float(x):.6f}")
        out["IsActive"] = out["IsActive"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        out["Rank"] = out["Rank"].apply(normalize_rank)

        gs.write_df(gs.cfg.members_sheet, out)
        gs.clear_cache()

        st.session_state["page"] = "⚙️ 管理"
        st.success("追加しました。")
        st.rerun()

    return members_df

# -----------------------------
# UI: Help
# -----------------------------
def ui_help() -> None:
    st.subheader("❓ ヘルプ / 使い方")

    st.markdown(
        """
このアプリは、プロジェクトごとの残高管理・APR確定・入金/出金の履歴管理（Ledger）と、LINE通知を行います。  
左メニュー（サイドバー）の **📈APR / 💸入金/出金 / ⚙️管理 / ❓ヘルプ** で画面を切り替えます。
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
  - `[line].channel_access_token`：LINE Messaging API  
  - `[imgbb].api_key`：ImgBB（画像添付するなら）
  - `[admin].pin`（またはpassword）：管理者ログイン用
"""
        )

    with st.expander("2) シート構成（列名は変更しない）", expanded=False):
        st.markdown("### Settings（プロジェクト設定）")
        st.code("\t".join(SETTINGS_HEADERS))
        st.markdown(
            """
- `Project_Name`：プロジェクト名  
- `IsCompound`：TRUEで複利（APRがPrincipalに加算）  
- `Active`：TRUEのプロジェクトだけ一覧に出ます  
"""
        )
        st.markdown("### Members（メンバー台帳）")
        st.code("\t".join(MEMBERS_HEADERS))
        st.markdown(
            """
- `Rank`：**Master / Elite**  
  - Master = **67%**
  - Elite = **60%**
"""
        )
        st.markdown("### Ledger（履歴）")
        st.code("\t".join(LEDGER_HEADERS))
        st.markdown("APR / Deposit / Withdraw の履歴が時刻（JST）付きで残ります。")

        st.markdown("### LineUsers（Make.comで作るLINEユーザー登録台帳）")
        st.code("\t".join(LINEUSERS_HEADERS))
        st.markdown(
            """
Make.com で以下フローで追記する想定です：  
`LINE Watch Events → HTTP(プロフィール取得) → Sheets(Search Rows) → Filter(0件のみ) → Sheets(Add a Row)`  

この台帳の内容を、⚙️管理 > メンバー追加で選択でき、Line_User_ID / LINE_DisplayName を自動入力できます。
"""
        )

    with st.expander("3) 📈 APRの使い方（全員に一斉LINE）", expanded=False):
        st.markdown(
            """
1. 左メニューで **📈 APR**  
2. プロジェクト選択  
3. 本日のAPR(%)を入力  
4. 必要ならエビデンス画像を選択  
5. 管理者ログイン済みなら **APRを確定して全員にLINE送信** が押せます

**計算式（個人ごと）**  
`日次配当 = Principal × APR% × Factor ÷ 365`  
- Master: Factor=0.67  
- Elite: Factor=0.60

※送信するLINE文面は「全員同一」で、個人名を含めません。
"""
        )

    with st.expander("4) 💸 入金/出金の使い方（個別LINE）", expanded=False):
        st.markdown(
            """
1. 左メニューで **💸 入金/出金**  
2. プロジェクト → メンバー選択  
3. Deposit / Withdraw を選択して金額入力  
4. （任意）エビデンス画像  
5. 管理者ログイン済みなら **確定して保存＆個別にLINE通知** が押せます

- Deposit：残高に加算  
- Withdraw：残高から減算  
- Ledgerに履歴が残り、本人へ個別LINEが飛びます
"""
        )

    with st.expander("5) ⚙️ 管理の使い方（メンバー追加）", expanded=False):
        st.markdown(
            """
1. 左メニューで **⚙️ 管理**  
2. 管理者PINでログイン  
3. プロジェクトを選択  
4. メンバーを追加

**重要（重複防止）**  
同一プロジェクト内で `Line_User_ID` が既に存在する場合、  
**追加もしない／更新もしない** 仕様です。

**LINE登録台帳（LineUsers）連携**  
Make.com で友だち追加時に LineUsers に追記しておくと、  
⚙️管理の画面で登録済みユーザーを選べます（自動入力）。
"""
        )

    with st.expander("6) よくあるエラー", expanded=False):
        st.markdown(
            """
- **Spreadsheet を開けない**  
  - サービスアカウントの `client_email` をスプレッドシートへ編集者共有できているか  
  - `spreadsheet` が正しいID/URLか
- **429 Quota exceeded**  
  - 再読み込み連打を避ける  
  - 書き込み後は自動でキャッシュクリアする設計（内部で実行）
- **LINEが届かない**  
  - `Line_User_ID` が正しいか（Uから始まる）  
  - `channel_access_token` が正しいか  
- **LineUsersが空 / 読めない**  
  - Make.com の Add a Row の列名が `Line_User_ID`, `Line_User` になっているか
"""
        )

# -----------------------------
# Main (sidebar radio)
# -----------------------------
def main():
    st.set_page_config(page_title="APR資産運用管理", layout="wide", page_icon="🏦")
    st.title("🏦 APR資産運用管理システム")

    if "page" not in st.session_state:
        st.session_state["page"] = "📈 APR"

    con = st.secrets.get("connections", {}).get("gsheets", {})
    sid_raw = str(con.get("spreadsheet", "")).strip()
    sid = extract_sheet_id(sid_raw)
    if not sid:
        st.error('Secrets の [connections.gsheets].spreadsheet が未設定です（URLまたはID）。')
        st.stop()

    gs = GSheets(GSheetsConfig(spreadsheet_id=sid))

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
        ui_help()

if __name__ == "__main__":
    main()
