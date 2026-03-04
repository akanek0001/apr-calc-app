# app.py
# APR資産運用管理（$表記固定 / デバッグ無し / メンバー管理=管理者のみ）
# - Settings: Project_Name, Net_Factor(例0.67), IsCompound(TRUE/FALSE), UpdatedAt_JST(任意)
# - Members : Project_Name, PersonName, Principal, Line_User_ID, LINE_DisplayName, IsActive(TRUE/FALSE), CreatedAt_JST, UpdatedAt_JST
# - Ledger  : Datetime_JST, Project_Name, PersonName, Type(Deposit/Withdraw/APR), Amount, Note, Evidence_URL, Line_User_ID, LINE_DisplayName, Source
#
# Secrets（Streamlit Cloud > Settings > Secrets）必須:
# [admin]
# password = "あなたの管理者パス"
#
# [line]
# channel_access_token = "LINEチャネルアクセストークン"
#
# [imgbb]
# api_key = "ImgBB API Key"
#
# [connections.gsheets]
# spreadsheet = "https://docs.google.com/spreadsheets/d/<ID>/edit"  # URLでもIDでもOK
#
# [connections.gsheets.credentials]
# type="service_account"
# project_id="..."
# private_key_id="..."
# private_key="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
# client_email="...@....iam.gserviceaccount.com"
# client_id="..."
# token_uri="https://oauth2.googleapis.com/token"
#
# 重要: 上記 service account を「スプレッドシート本体」に編集者共有してください。

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

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
        s = str(v).replace(",", "").replace("$", "").strip()
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
# Sheets Config / Client
# -----------------------------
SETTINGS_HEADERS = ["Project_Name", "Net_Factor", "IsCompound", "UpdatedAt_JST"]
MEMBERS_HEADERS = [
    "Project_Name",
    "PersonName",
    "Principal",
    "Line_User_ID",
    "LINE_DisplayName",
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

        # 初期化（429対策：最低限の呼び出し）
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
            # quota等で読めない時は作成済み前提で抜ける
            return

        if not first:
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return

        # ヘッダー重複/不足の事故を避ける（不足だけ追加）
        colset = [c.strip() for c in first if str(c).strip()]
        missing = [h for h in headers if h not in colset]
        if missing:
            new_headers = colset + missing
            ws.update("1:1", [new_headers])

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
# Admin Auth
# -----------------------------
def admin_password() -> str:
    return str(st.secrets.get("admin", {}).get("password", "")).strip()


def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))


def admin_login_ui() -> None:
    pw_required = admin_password()
    if not pw_required:
        st.warning("Secrets に [admin].password が未設定です。")
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
        pw = st.text_input("管理者パスワード", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if pw == pw_required:
                st.session_state["admin_ok"] = True
                st.rerun()
            else:
                st.session_state["admin_ok"] = False
                st.error("パスワードが違います。")


# -----------------------------
# Domain: Settings / Members
# -----------------------------
def load_settings(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.settings_sheet)
    if df.empty:
        # 空ならヘッダだけの想定。ユーザー側で入力してもらう。
        return df

    # 必須列チェック
    need = ["Project_Name", "Net_Factor", "IsCompound"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        st.error(f"Settingsシートの列が不足しています: {missing}")
        st.stop()

    # normalize
    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
    # Net_Factor が空なら 0.67
    df["Net_Factor"] = df["Net_Factor"].apply(lambda x: to_f(x) if str(x).strip() else 0.67)
    df["IsCompound"] = df["IsCompound"].apply(truthy)
    return df


def load_members(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.members_sheet)
    if df.empty:
        return df

    need = ["Project_Name", "PersonName", "Principal", "Line_User_ID", "LINE_DisplayName", "IsActive"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        st.error(f"Membersシートの列が不足しています: {missing}")
        st.stop()

    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
    df["PersonName"] = df["PersonName"].astype(str).str.strip()
    df["Principal"] = df["Principal"].apply(to_f)
    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df["LINE_DisplayName"] = df["LINE_DisplayName"].astype(str).str.strip()
    df["IsActive"] = df["IsActive"].apply(truthy)
    return df


def project_members(members_df: pd.DataFrame, project: str) -> pd.DataFrame:
    df = members_df.copy()
    df = df[(df["Project_Name"] == str(project)) & (df["IsActive"] == True)]
    df = df.reset_index(drop=True)
    return df


def dedup_line_ids(df: pd.DataFrame) -> List[str]:
    ids = []
    for v in df.get("Line_User_ID", []):
        s = str(v).strip()
        if s.startswith("U"):
            ids.append(s)
    seen, out = set(), []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# -----------------------------
# UI: APR
# -----------------------------
def ui_apr(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> Tuple[pd.DataFrame, bool]:
    st.subheader("📈 APR 確定（67%・均等配分）")

    if not is_admin():
        st.info("APR確定は管理者のみ実行できます。管理タブでログインしてください。")
        return members_df, False

    if settings_df.empty:
        st.warning("Settingsシートが空です。先に Settings を入力してください。")
        return members_df, False

    projects = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    project = st.selectbox("プロジェクト", projects)

    row = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]
    net_factor = float(row["Net_Factor"]) if row["Net_Factor"] else 0.67
    is_compound = bool(row["IsCompound"])

    apr = st.number_input("本日のAPR（%）", value=100.0, step=0.1)
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])

    mem = project_members(members_df, project)
    if mem.empty:
        st.warning("このプロジェクトに有効メンバーがいません（Membersを確認）。")
        return members_df, False

    total_principal = float(mem["Principal"].sum())
    n = int(len(mem))
    # 日次の配当総額（均等配分）
    total_reward = (total_principal * (apr / 100.0) * 0.67) / 365.0
    per_member = total_reward / n if n > 0 else 0.0

    st.write(f"- 総元本: {fmt_usd(total_principal)}")
    st.write(f"- 取り分: 67%（固定）")
    st.write(f"- 本日総配当: {fmt_usd(total_reward)}")
    st.write(f"- 1人あたり: {fmt_usd(per_member)}（{n}人で均等）")
    st.write(f"- モード: {'複利（元本に加算）' if is_compound else '単利（元本は固定）'}")

    if st.button("APRを確定して全員にLINE送信"):
        # 画像アップロード
        evidence_url = None
        if uploaded:
            with st.spinner("画像アップロード中..."):
                evidence_url = upload_imgbb(uploaded.getvalue())
            if not evidence_url:
                st.error("画像アップロードに失敗しました（ImgBB）。画像を外して再実行するか、ImgBB設定を確認してください。")
                st.stop()

        # Members更新（複利のときだけ）
        if is_compound:
            for i in range(len(members_df)):
                if members_df.loc[i, "Project_Name"] == str(project) and bool(members_df.loc[i, "IsActive"]) is True:
                    members_df.loc[i, "Principal"] = float(members_df.loc[i, "Principal"]) + float(per_member)

        # Ledger追記（個人ごとに記録）
        ts = fmt_dt(now_jst())
        for _, r in mem.iterrows():
            gs.append_row(gs.cfg.ledger_sheet, [
                ts,
                project,
                r["PersonName"],
                "APR",
                float(per_member),
                f"APR:{apr}%, net:0.67, equal:{n}",
                evidence_url or "",
                r["Line_User_ID"],
                r["LINE_DisplayName"],
                "app",
            ])

        # Members保存（複利時だけ更新したが、単利でもPrincipalをfloat→str戻すために保存してOK）
        out_members = members_df.copy()
        # 書き込みは文字列で
        out_members["Principal"] = out_members["Principal"].apply(lambda x: f"{float(x):.6f}")
        out_members["IsActive"] = out_members["IsActive"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        gs.write_df(gs.cfg.members_sheet, out_members)

        # LINE送信（全員に同じ文面、個人名は入れない）
        token = st.secrets["line"]["channel_access_token"]
        targets = dedup_line_ids(mem)

        msg = "🏦【APR収益報告】\n"
        msg += f"プロジェクト: {project}\n"
        msg += f"報告日時: {now_jst().strftime('%Y/%m/%d %H:%M')}\n\n"
        msg += f"APR: {apr}%\n"
        msg += f"取り分: 67%\n"
        msg += f"人数: {n}\n"
        msg += f"1人あたり: {fmt_usd(per_member)}\n"
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
        return members_df, True

    return members_df, False


# -----------------------------
# UI: Deposit/Withdraw (個別通知)
# -----------------------------
def ui_cash(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> Tuple[pd.DataFrame, bool]:
    st.subheader("💸 入金 / 出金（個別LINE通知）")

    if not is_admin():
        st.info("入金/出金の記録は管理者のみ実行できます。管理タブでログインしてください。")
        return members_df, False

    if settings_df.empty:
        st.warning("Settingsシートが空です。先に Settings を入力してください。")
        return members_df, False

    projects = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    project = st.selectbox("プロジェクト", projects, key="cash_project")

    mem = project_members(members_df, project)
    if mem.empty:
        st.warning("このプロジェクトに有効メンバーがいません（Membersを確認）。")
        return members_df, False

    person = st.selectbox("メンバー", mem["PersonName"].tolist())
    row = mem[mem["PersonName"] == person].iloc[0]

    current = float(row["Principal"])
    st.info(f"現在残高: {fmt_usd(current)}")

    typ = st.selectbox("種別", ["Deposit", "Withdraw"])
    amt = st.number_input("金額", min_value=0.0, value=0.0, step=100.0)
    note = st.text_input("メモ（任意）", value="")
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"], key="cash_img")

    if st.button("確定して保存＆個別にLINE通知"):
        if amt <= 0:
            st.warning("金額が0です。")
            return members_df, False

        # 画像アップロード
        evidence_url = None
        if uploaded:
            with st.spinner("画像アップロード中..."):
                evidence_url = upload_imgbb(uploaded.getvalue())
            if not evidence_url:
                st.error("画像アップロードに失敗しました（ImgBB）。画像を外して再実行するか、ImgBB設定を確認してください。")
                st.stop()

        new_balance = current + float(amt) if typ == "Deposit" else current - float(amt)

        # Members更新（該当者のみ）
        for i in range(len(members_df)):
            if members_df.loc[i, "Project_Name"] == str(project) and members_df.loc[i, "PersonName"] == str(person):
                members_df.loc[i, "Principal"] = float(new_balance)

        # Ledger追記
        ts = fmt_dt(now_jst())
        gs.append_row(gs.cfg.ledger_sheet, [
            ts,
            project,
            person,
            typ,
            float(amt),
            note,
            evidence_url or "",
            row["Line_User_ID"],
            row["LINE_DisplayName"],
            "app",
        ])

        # Members保存
        out_members = members_df.copy()
        out_members["Principal"] = out_members["Principal"].apply(lambda x: f"{float(x):.6f}")
        out_members["IsActive"] = out_members["IsActive"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        gs.write_df(gs.cfg.members_sheet, out_members)

        # 個別LINE通知
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
        return members_df, True

    return members_df, False


# -----------------------------
# UI: Admin (Members only)
# -----------------------------
def ui_admin(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> Tuple[pd.DataFrame, bool]:
    st.subheader("⚙️ 管理（管理者のみ）")

    admin_login_ui()
    if not is_admin():
        st.info("管理者ログインすると、メンバー追加・編集が表示されます。")
        st.stop()

    st.success("管理者機能が有効です。")

    st.divider()
    st.write("### メンバー管理（追加/編集）")

    if settings_df.empty:
        st.warning("Settingsシートが空です。先にSettingsを入力してください。")
        return members_df, False

    projects = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    project = st.selectbox("対象プロジェクト", projects, key="admin_project")

    with st.expander("現在のメンバー一覧", expanded=True):
        view = members_df[members_df["Project_Name"] == str(project)].copy()
        if view.empty:
            st.info("まだメンバーがいません。下のフォームから追加してください。")
        else:
            show = view.copy()
            show["Principal"] = show["Principal"].apply(lambda x: fmt_usd(float(x)))
            st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()
    st.write("#### 追加 / 更新（Line_User_ID で上書き）")

    with st.form("member_upsert", clear_on_submit=False):
        person = st.text_input("PersonName（個人名）")
        principal = st.number_input("Principal（残高）", min_value=0.0, value=0.0, step=100.0)
        line_uid = st.text_input("Line_User_ID（Uから始まる）")
        line_disp = st.text_input("LINE_DisplayName（任意）", value="")
        is_active = st.selectbox("IsActive", ["TRUE", "FALSE"], index=0)
        submit = st.form_submit_button("保存（Upsert）")

    if submit:
        if not person or not line_uid:
            st.error("PersonName と Line_User_ID は必須です。")
            return members_df, False

        ts = fmt_dt(now_jst())

        # Upsert: 同一プロジェクト内で Line_User_ID が一致する行を更新。なければ追加。
        updated = False
        for i in range(len(members_df)):
            if members_df.loc[i, "Project_Name"] == str(project) and str(members_df.loc[i, "Line_User_ID"]).strip() == str(line_uid).strip():
                members_df.loc[i, "PersonName"] = str(person).strip()
                members_df.loc[i, "Principal"] = float(principal)
                members_df.loc[i, "LINE_DisplayName"] = str(line_disp).strip()
                members_df.loc[i, "IsActive"] = True if is_active == "TRUE" else False
                members_df.loc[i, "UpdatedAt_JST"] = ts
                updated = True
                break

        if not updated:
            new_row = {
                "Project_Name": str(project).strip(),
                "PersonName": str(person).strip(),
                "Principal": float(principal),
                "Line_User_ID": str(line_uid).strip(),
                "LINE_DisplayName": str(line_disp).strip(),
                "IsActive": True if is_active == "TRUE" else False,
                "CreatedAt_JST": ts,
                "UpdatedAt_JST": ts,
            }
            members_df = pd.concat([members_df, pd.DataFrame([new_row])], ignore_index=True)

        out = members_df.copy()
        out["Principal"] = out["Principal"].apply(lambda x: f"{float(x):.6f}")
        out["IsActive"] = out["IsActive"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        gs.write_df(gs.cfg.members_sheet, out)
        gs.clear_cache()
        st.success("保存しました。")
        st.rerun()

    return members_df, False


# -----------------------------
# Main
# -----------------------------
def main():
    st.set_page_config(page_title="APR資産運用管理", layout="wide", page_icon="🏦")
    st.title("🏦 APR資産運用管理システム")

    # spreadsheet id
    con = st.secrets.get("connections", {}).get("gsheets", {})
    sid_raw = str(con.get("spreadsheet", "")).strip()
    sid = extract_sheet_id(sid_raw)
    if not sid:
        st.error('Secrets の [connections.gsheets].spreadsheet が未設定です（URLまたはID）。')
        st.stop()

    gs = GSheets(GSheetsConfig(spreadsheet_id=sid))

    # 読み取り（キャッシュあり）
    try:
        settings_df = load_settings(gs)
        members_df = load_members(gs)
    except APIError as e:
        # 429 など
        st.error(f"読み取りエラー: {e}")
        st.stop()

    tab_apr, tab_cash, tab_admin = st.tabs(["📈 APR", "💸 入金/出金", "⚙️ 管理"])

    with tab_apr:
        members_df, _ = ui_apr(gs, settings_df, members_df)

    with tab_cash:
        members_df, _ = ui_cash(gs, settings_df, members_df)

    with tab_admin:
        members_df, _ = ui_admin(gs, settings_df, members_df)


if __name__ == "__main__":
    main()
