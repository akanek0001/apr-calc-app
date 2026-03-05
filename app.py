# app.py  (Rank: Master(67%) / Elite(60%) 対応・ページ状態保持・画像添付LINE・個別入出金通知)
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

# =========================================================
# Utils
# =========================================================
def now_jst() -> datetime:
    return datetime.now(JST)

def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def fmt_usd(x: float) -> str:
    try:
        return f"${float(x):,.2f}"
    except:
        return "$0.00"

def to_f(v: Any) -> float:
    try:
        s = str(v).replace(",", "").replace("$", "").replace("％", "").replace("%", "").strip()
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

# =========================================================
# Rank (Master / Elite)
# =========================================================
RANK_MASTER = "Master"
RANK_ELITE = "Elite"

def rank_factor(rank: str) -> float:
    r = (rank or "").strip()
    if r == RANK_ELITE:
        return 0.60
    # default
    return 0.67

def normalize_rank(rank: str) -> str:
    r = (rank or "").strip()
    if r.lower() in ("elite", "エリート", "60", "0.60"):
        return RANK_ELITE
    if r.lower() in ("master", "マスター", "67", "0.67"):
        return RANK_MASTER
    return RANK_MASTER

# =========================================================
# LINE
# =========================================================
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

# =========================================================
# ImgBB
# =========================================================
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

# =========================================================
# Sheets schema (※列名は変更しない・追加のみ)
# =========================================================
SETTINGS_HEADERS = ["Project_Name", "Net_Factor", "IsCompound", "UpdatedAt_JST", "Active"]  # Active は任意（無くてもOK）
MEMBERS_HEADERS = [
    "Project_Name",
    "PersonName",
    "Principal",
    "Line_User_ID",
    "LINE_DisplayName",
    "Rank",            # ★追加（Master / Elite）
    "IsActive",
    "CreatedAt_JST",
    "UpdatedAt_JST",
]
LEDGER_HEADERS = [
    "Datetime_JST",
    "Project_Name",
    "PersonName",
    "Type",            # APR / Deposit / Withdraw
    "Amount",
    "Note",
    "Evidence_URL",
    "Line_User_ID",
    "LINE_DisplayName",
    "Source",          # app
]

@dataclass
class GSheetsConfig:
    spreadsheet_id: str
    settings_sheet: str = "Settings"
    members_sheet: str = "Members"
    ledger_sheet: str = "Ledger"

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

        # 必要シート & 必要列を確保（既存列は変更しない／不足列のみ右に追加）
        self._ensure_sheet(self.cfg.settings_sheet, SETTINGS_HEADERS)
        self._ensure_sheet(self.cfg.members_sheet, MEMBERS_HEADERS)
        self._ensure_sheet(self.cfg.ledger_sheet, LEDGER_HEADERS)

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

# =========================================================
# Admin Auth (pin優先 / なければpassword)
# =========================================================
def admin_secret() -> str:
    admin = st.secrets.get("admin", {})
    pin = str(admin.get("pin", "")).strip()
    if pin:
        return pin
    pw = str(admin.get("password", "")).strip()
    return pw

def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))

def admin_login_ui() -> None:
    required = admin_secret()
    if not required:
        st.warning("Secrets に [admin].pin または [admin].password が未設定です。")
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
        pw = st.text_input("管理者パスワード / PIN", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if pw == required:
                st.session_state["admin_ok"] = True
                st.rerun()
            else:
                st.session_state["admin_ok"] = False
                st.error("パスワード/PINが違います。")

# =========================================================
# Domain load
# =========================================================
def load_settings(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.settings_sheet)
    if df.empty:
        return df

    # 必須
    need = ["Project_Name", "Net_Factor", "IsCompound"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        st.error(f"Settingsシートの列が不足: {missing}")
        st.stop()

    df = df.copy()
    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
    df["Net_Factor"] = df["Net_Factor"].apply(lambda x: to_f(x) if str(x).strip() else 0.67)
    df["IsCompound"] = df["IsCompound"].apply(truthy)

    # Active がある場合だけ反映（無い場合は全てActive扱い）
    if "Active" in df.columns:
        df["Active"] = df["Active"].apply(truthy)
    else:
        df["Active"] = True

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

    df = df.copy()
    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
    df["PersonName"] = df["PersonName"].astype(str).str.strip()
    df["Principal"] = df["Principal"].apply(to_f)
    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df["LINE_DisplayName"] = df["LINE_DisplayName"].astype(str).str.strip()
    df["IsActive"] = df["IsActive"].apply(truthy)

    # Rank が無い/空なら Master
    if "Rank" not in df.columns:
        df["Rank"] = RANK_MASTER
    df["Rank"] = df["Rank"].apply(lambda x: normalize_rank(str(x)))

    # CreatedAt/UpdatedAt が無い場合も壊れないように空で持つ
    if "CreatedAt_JST" not in df.columns:
        df["CreatedAt_JST"] = ""
    if "UpdatedAt_JST" not in df.columns:
        df["UpdatedAt_JST"] = ""

    return df

def project_members(members_df: pd.DataFrame, project: str) -> pd.DataFrame:
    df = members_df.copy()
    df = df[(df["Project_Name"] == str(project)) & (df["IsActive"] == True)]
    return df.reset_index(drop=True)

def dedup_line_ids(df: pd.DataFrame) -> List[str]:
    ids: List[str] = []
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

def write_members_back(gs: GSheets, members_df: pd.DataFrame) -> None:
    out = members_df.copy()

    # 既存列名は変えない（文字列化だけ）
    out["Principal"] = out["Principal"].apply(lambda x: f"{float(x):.6f}")
    out["IsActive"] = out["IsActive"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
    out["Rank"] = out["Rank"].apply(lambda x: normalize_rank(str(x)))

    gs.write_df(gs.cfg.members_sheet, out)

# =========================================================
# UI: APR
# =========================================================
def ui_apr(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("📈 APR 確定（Rank選択：Master 67% / Elite 60%）")

    if settings_df.empty:
        st.warning("Settingsシートが空です。先に Settings を入力してください。")
        return members_df

    active_projects = settings_df[settings_df["Active"] == True]["Project_Name"].dropna().astype(str).unique().tolist()
    if not active_projects:
        st.warning("Active=TRUE のプロジェクトがありません（Settings.Active）。")
        return members_df

    project = st.selectbox("プロジェクト", active_projects)

    row = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]
    is_compound = bool(row["IsCompound"])
    # Net_Factor は表示のみ（Rankが優先のため、ここは参考値として残す）
    net_factor_setting = float(row["Net_Factor"]) if "Net_Factor" in row else 0.67

    apr = st.number_input("本日のAPR（%）", value=100.0, step=0.1)
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])

    mem = project_members(members_df, project)
    if mem.empty:
        st.warning("このプロジェクトに有効メンバーがいません（Membersを確認）。")
        return members_df

    # 個人別に日次APRを計算（Master/Eliteのみ）
    mem = mem.copy()
    mem["Rank"] = mem["Rank"].apply(lambda x: normalize_rank(str(x)))
    mem["Factor"] = mem["Rank"].apply(rank_factor)
    mem["DailyReward"] = mem.apply(
        lambda r: (float(r["Principal"]) * (apr / 100.0) * float(r["Factor"])) / 365.0,
        axis=1
    )

    total_principal = float(mem["Principal"].sum())
    total_reward = float(mem["DailyReward"].sum())

    cnt_master = int((mem["Rank"] == RANK_MASTER).sum())
    cnt_elite = int((mem["Rank"] == RANK_ELITE).sum())

    st.write(f"- 総元本: {fmt_usd(total_principal)}")
    st.write(f"- 本日のAPR: {apr}%")
    st.write(f"- Rank: Master(67%) / Elite(60%)")
    st.write(f"- 人数: Master={cnt_master}, Elite={cnt_elite}, 合計={len(mem)}")
    st.write(f"- 本日総配当（合計）: {fmt_usd(total_reward)}")
    st.write(f"- モード: {'複利（残高に加算）' if is_compound else '単利（残高は固定）'}")
    st.caption(f"※ Settings.Net_Factor={net_factor_setting} は参考値。実際の配分は Rank に従います。")

    if not is_admin():
        st.info("APR確定は管理者のみ実行できます（管理画面でログイン）。")
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

        # 複利なら Principal に加算（同一Project&Activeのみ）
        if is_compound:
            # members_df 側へ反映（全体テーブルを更新）
            for i in range(len(members_df)):
                if (
                    str(members_df.loc[i, "Project_Name"]).strip() == str(project)
                    and bool(members_df.loc[i, "IsActive"]) is True
                ):
                    # 해당行のrankで加算額を決める（memから引く）
                    person_i = str(members_df.loc[i, "PersonName"]).strip()
                    mrow = mem[mem["PersonName"] == person_i]
                    if not mrow.empty:
                        add = float(mrow.iloc[0]["DailyReward"])
                        members_df.loc[i, "Principal"] = float(members_df.loc[i, "Principal"]) + add

        # Ledgerへ個人別に記録（Amountは個別DailyReward）
        for _, r in mem.iterrows():
            note = f"APR:{apr}%, Rank:{r['Rank']}, factor:{float(r['Factor']):.2f}"
            gs.append_row(gs.cfg.ledger_sheet, [
                ts, project, r["PersonName"], "APR", float(r["DailyReward"]),
                note,
                evidence_url or "",
                r["Line_User_ID"], r["LINE_DisplayName"], "app",
            ])

        # Members書戻し（複利の場合のみ残高が変化。単利でもRankの正規化等のため書き戻してOK）
        write_members_back(gs, members_df)

        # LINE送信（全員へ同一メッセージ：個人名は入れない）
        token = st.secrets["line"]["channel_access_token"]
        targets = dedup_line_ids(mem)

        msg = "🏦【APR収益報告】\n"
        msg += f"プロジェクト: {project}\n"
        msg += f"報告日時: {now_jst().strftime('%Y/%m/%d %H:%M')}\n\n"
        msg += f"APR: {apr}%\n"
        msg += f"Master: 67%\n"
        msg += f"Elite : 60%\n\n"
        msg += f"人数: Master={cnt_master}, Elite={cnt_elite}, 合計={len(mem)}\n"
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

# =========================================================
# UI: Deposit/Withdraw (個別通知)
# =========================================================
def ui_cash(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("💸 入金 / 出金（個別LINE通知）")

    if settings_df.empty:
        st.warning("Settingsシートが空です。先に Settings を入力してください。")
        return members_df

    active_projects = settings_df[settings_df["Active"] == True]["Project_Name"].dropna().astype(str).unique().tolist()
    if not active_projects:
        st.warning("Active=TRUE のプロジェクトがありません（Settings.Active）。")
        return members_df

    project = st.selectbox("プロジェクト", active_projects, key="cash_project")

    mem = project_members(members_df, project)
    if mem.empty:
        st.warning("このプロジェクトに有効メンバーがいません（Membersを確認）。")
        return members_df

    person = st.selectbox("メンバー", mem["PersonName"].tolist())
    row = mem[mem["PersonName"] == person].iloc[0]
    current = float(row["Principal"])
    st.info(f"現在残高: {fmt_usd(current)}")

    typ = st.selectbox("種別", ["Deposit", "Withdraw"])
    amt = st.number_input("金額", min_value=0.0, value=0.0, step=100.0)
    note = st.text_input("メモ（任意）", value="")
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"], key="cash_img")

    if not is_admin():
        st.info("入金/出金の記録は管理者のみ実行できます（管理画面でログイン）。")
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

        # 残高更新
        for i in range(len(members_df)):
            if (
                str(members_df.loc[i, "Project_Name"]).strip() == str(project)
                and str(members_df.loc[i, "PersonName"]).strip() == str(person)
            ):
                members_df.loc[i, "Principal"] = float(new_balance)
                members_df.loc[i, "UpdatedAt_JST"] = fmt_dt(now_jst())
                break

        ts = fmt_dt(now_jst())
        gs.append_row(gs.cfg.ledger_sheet, [
            ts, project, person, typ, float(amt), note, evidence_url or "",
            row["Line_User_ID"], row["LINE_DisplayName"], "app"
        ])

        write_members_back(gs, members_df)

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

# =========================================================
# UI: Admin (メンバー管理は管理者のみ)
#   - 追加更新: 「同一プロジェクト内で Line_User_ID が一致したら更新しない」
#     → つまり既存があれば「追加もしない / 更新もしない」でブロック
# =========================================================
def ui_admin(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("⚙️ 管理（管理者のみ）")
    admin_login_ui()

    if not is_admin():
        st.info("ログインすると、メンバー追加・編集が表示されます。")
        return members_df

    if settings_df.empty:
        st.warning("Settingsシートが空です。先にSettingsを入力してください。")
        return members_df

    active_projects = settings_df[settings_df["Active"] == True]["Project_Name"].dropna().astype(str).unique().tolist()
    if not active_projects:
        st.warning("Active=TRUE のプロジェクトがありません（Settings.Active）。")
        return members_df

    project = st.selectbox("対象プロジェクト", active_projects, key="admin_project")

    with st.expander("現在のメンバー一覧", expanded=True):
        view = members_df[members_df["Project_Name"] == str(project)].copy()
        if view.empty:
            st.info("まだメンバーがいません。下のフォームから追加してください。")
        else:
            show = view.copy()
            show["Principal"] = show["Principal"].apply(lambda x: fmt_usd(float(x)))
            show["Rank"] = show["Rank"].apply(lambda x: normalize_rank(str(x)))
            st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()
    st.write("#### メンバー追加（同一プロジェクト内で Line_User_ID が一致する場合は登録しない）")

    with st.form("member_add", clear_on_submit=False):
        person = st.text_input("PersonName（個人名）")
        principal = st.number_input("Principal（残高）", min_value=0.0, value=0.0, step=100.0)
        line_uid = st.text_input("Line_User_ID（Uから始まる）")
        line_disp = st.text_input("LINE_DisplayName（任意）", value="")
        rank = st.selectbox("Rank（Master=67% / Elite=60%）", [RANK_MASTER, RANK_ELITE], index=0)
        is_active = st.selectbox("IsActive", ["TRUE", "FALSE"], index=0)
        submit = st.form_submit_button("保存（Add）")

    if submit:
        if not person or not line_uid:
            st.error("PersonName と Line_User_ID は必須です。")
            return members_df

        # ★一致したら「更新しない / 追加もしない」
        exists = False
        for i in range(len(members_df)):
            if (
                str(members_df.loc[i, "Project_Name"]).strip() == str(project)
                and str(members_df.loc[i, "Line_User_ID"]).strip() == str(line_uid).strip()
            ):
                exists = True
                break

        if exists:
            st.warning("このプロジェクト内で同じ Line_User_ID が既に登録済みです。更新もしません。")
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

        write_members_back(gs, members_df)
        gs.clear_cache()

        # 管理画面に留まる
        st.session_state["page"] = "⚙️ 管理"

        st.success("追加しました。")
        st.rerun()

    return members_df

# =========================================================
# Main
# =========================================================
def main():
    st.set_page_config(page_title="APR資産運用管理", layout="wide", page_icon="🏦")
    st.title("🏦 APR資産運用管理システム")

    # page state
    if "page" not in st.session_state:
        st.session_state["page"] = "📈 APR"

    # Spreadsheet ID (connections.gsheets.spreadsheet)
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

    # Sidebar navigation (keeps state across rerun)
    page = st.sidebar.radio(
        "メニュー",
        options=["📈 APR", "💸 入金/出金", "⚙️ 管理"],
        index=["📈 APR", "💸 入金/出金", "⚙️ 管理"].index(st.session_state["page"]),
    )
    st.session_state["page"] = page

    if page == "📈 APR":
        members_df = ui_apr(gs, settings_df, members_df)
    elif page == "💸 入金/出金":
        members_df = ui_cash(gs, settings_df, members_df)
    else:
        members_df = ui_admin(gs, settings_df, members_df)

if __name__ == "__main__":
    main()
