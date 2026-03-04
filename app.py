# app.py
# APR資産運用 管理システム（完全版 / 429対策 / 画像添付 / LINE通知）
#
# ✅ できること
# - Google Sheets（中央台帳）で「Settings / Members / Ledger」を運用
# - 入金/出金：個人にLINE通知（画像エビデンス添付可）
# - APR確定：プロジェクト参加者全員に一斉LINE通知（個人名は入れない／画像添付可）
# - APR/入金/出金はすべて「元本（Principal）」を増減（ご要望通り）
# - 429（Read quota exceeded）に強い：起動時に重い読み取りを避け、キャッシュと軽量取得
#
# ✅ Secrets（Streamlit Cloud > Settings > Secrets）例（この形で入れてください）
# [admin]
# password = "sugikiyo"
#
# [connections.gsheets]
# spreadsheet = "https://docs.google.com/spreadsheets/d/<ID>/edit"   # URLでもIDでもOK
#
# [connections.gsheets.credentials]
# type = "service_account"
# project_id = "xxx"
# private_key_id = "xxx"
# private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
# client_email = "xxx@xxx.iam.gserviceaccount.com"
# client_id = "xxx"
# token_uri = "https://oauth2.googleapis.com/token"
#
# [line]
# channel_access_token = "xxxxx"
# # 任意：管理者にも通知したい場合
# # admin_user_id = "Uxxxxxxxxxxxxxxxxxxxxxxxx"
#
# [imgbb]
# api_key = "xxxxx"
#
# ✅ シート構成（中央台帳スプレッドシート内）
# - Settings
# - Members
# - Ledger
#
# ✅ ヘッダー（1行目にコピペ）
# Settings:
# Project_Name,Net_Factor,Currency,IsCompound,UpdatedAt_JST
#
# Members:
# Project_Name,PersonName,Principal,Line_User_ID,LINE_DisplayName,IsActive,CreatedAt_JST,UpdatedAt_JST
#
# Ledger:
# Datetime_JST,Project_Name,PersonName,Type,Amount,Currency,Note,Evidence_URL,Line_User_ID,LINE_DisplayName,Source
#
# ------------------------------------------------------------

from __future__ import annotations

import time
import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import requests

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# =========================
# Timezone
# =========================
JST = timezone(timedelta(hours=9), "JST")

def now_jst() -> datetime:
    return datetime.now(JST)

def fmt_jst(dt: datetime) -> str:
    return dt.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")

# =========================
# Sheet Headers
# =========================
SETTINGS_HEADERS = ["Project_Name", "Net_Factor", "Currency", "IsCompound", "UpdatedAt_JST"]
MEMBERS_HEADERS  = ["Project_Name", "PersonName", "Principal", "Line_User_ID", "LINE_DisplayName", "IsActive", "CreatedAt_JST", "UpdatedAt_JST"]
LEDGER_HEADERS   = ["Datetime_JST", "Project_Name", "PersonName", "Type", "Amount", "Currency", "Note", "Evidence_URL", "Line_User_ID", "LINE_DisplayName", "Source"]

# =========================
# Helpers
# =========================
def safe_str(x: Any) -> str:
    return "" if x is None else str(x)

def to_f(x: Any) -> float:
    try:
        s = str(x).replace(",", "").replace("$", "").replace("%", "").strip()
        return float(s) if s else 0.0
    except:
        return 0.0

def truthy(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in ("1", "true", "yes", "y", "on", "はい")

def extract_sheet_id(url_or_id: str) -> str:
    s = str(url_or_id).strip()
    if "/spreadsheets/d/" in s:
        try:
            return s.split("/spreadsheets/d/")[1].split("/")[0]
        except:
            return s
    return s

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

# =========================
# LINE / ImgBB
# =========================
def send_line(token: str, user_id: str, text: str, image_url: Optional[str] = None) -> int:
    if not user_id or str(user_id).strip() == "":
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
    except:
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
            timeout=30
        )
        data = res.json()
        return data["data"]["url"]
    except:
        return None

# =========================
# Google Sheets Client (429対策)
# =========================
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
            st.error(f"Spreadsheet を開けません: {e}")
            st.stop()

        # 起動時の初期化は「軽量」で。429ならスキップして起動継続。
        self._ensure_sheet(self.cfg.settings_sheet, SETTINGS_HEADERS)
        self._ensure_sheet(self.cfg.members_sheet, MEMBERS_HEADERS)
        self._ensure_sheet(self.cfg.ledger_sheet,  LEDGER_HEADERS)

    def ws(self, name: str):
        return self.book.worksheet(name)

    def _ensure_sheet(self, title: str, headers: List[str], rows: int = 2000, cols: int = 26) -> None:
        """
        429対策：
        - get_all_values() は起動時に使わない
        - row_values(1) のみでヘッダー確認
        - 429は警告してスキップ（落とさない）
        """
        for attempt in range(2):
            try:
                try:
                    ws = self.book.worksheet(title)
                except Exception:
                    ws = self.book.add_worksheet(title=title, rows=str(rows), cols=str(cols))

                try:
                    first_row = ws.row_values(1)  # 軽量
                except APIError as e:
                    if "429" in str(e):
                        st.warning(f"Sheets初期化を一時スキップ（429）: {title}")
                        return
                    raise

                if not first_row:
                    ws.append_row(headers, value_input_option="USER_ENTERED")
                    return

                current = [str(h).strip() for h in first_row if str(h).strip() != ""]
                if not current:
                    ws.update("1:1", [headers])
                    return

                missing = [h for h in headers if h not in current]
                if missing:
                    ws.update("1:1", [current + missing])
                return

            except APIError as e:
                if "429" in str(e):
                    time.sleep(0.6)
                    continue
                st.error(f"シート初期化に失敗: {title} / {e}")
                st.stop()

        st.warning(f"Sheets初期化をスキップ（429継続）: {title}")

    def _read_values_retry(self, ws, max_retry: int = 3) -> List[List[str]]:
        for i in range(max_retry):
            try:
                return ws.get_all_values()
            except APIError as e:
                if "429" in str(e):
                    time.sleep(0.8 + i * 0.6)
                    continue
                raise
        raise APIError("APIError: [429]: Quota exceeded (read)")

    @st.cache_data(ttl=30, show_spinner=False)
    def read_df_cached(_self, sheet_name: str) -> pd.DataFrame:
        """
        キャッシュで読み取り回数を減らす（429対策）
        """
        ws = _self.ws(sheet_name)
        values = _self._read_values_retry(ws)
        if not values:
            return pd.DataFrame()
        header = values[0]
        rows = values[1:]
        df = pd.DataFrame(rows, columns=header)
        # 列名の前後スペース除去（重複/崩れ対策）
        df.columns = (
            df.columns.astype(str)
            .str.replace("\u3000", " ", regex=False)
            .str.strip()
        )
        return df

    def clear_cache(self):
        st.cache_data.clear()

    def write_df(self, sheet_name: str, df: pd.DataFrame) -> None:
        ws = self.ws(sheet_name)
        # 全消し→更新は書き込みになる（読取り quota には影響小）
        ws.clear()
        df2 = df.copy()
        df2 = df2.fillna("")
        ws.update([df2.columns.tolist()] + df2.astype(str).values.tolist())

    def append_row(self, sheet_name: str, row: List[Any]) -> None:
        ws = self.ws(sheet_name)
        ws.append_row([safe_str(x) for x in row], value_input_option="USER_ENTERED")

# =========================
# Admin Auth
# =========================
def admin_ok() -> bool:
    return bool(st.session_state.get("admin_ok", False))

def admin_login_ui():
    pwd = safe_str(st.secrets.get("admin", {}).get("password", ""))
    if not pwd:
        st.warning("Secrets の [admin].password が未設定です（管理保護できません）。")
        return

    if admin_ok():
        c1, c2 = st.columns([1, 1])
        with c1:
            st.success("管理者ログイン中")
        with c2:
            if st.button("ログアウト", use_container_width=True):
                st.session_state["admin_ok"] = False
                st.rerun()
        st.divider()
        return

    with st.form("admin_login_form"):
        pin = st.text_input("管理者パスワード", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if pin == pwd:
                st.session_state["admin_ok"] = True
                st.success("ログインしました")
                st.rerun()
            else:
                st.error("パスワードが違います")

# =========================
# Domain Logic
# =========================
def normalize_settings(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=SETTINGS_HEADERS)
    for c in SETTINGS_HEADERS:
        if c not in df.columns:
            df[c] = ""
    return df[SETTINGS_HEADERS].copy()

def normalize_members(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=MEMBERS_HEADERS)
    for c in MEMBERS_HEADERS:
        if c not in df.columns:
            df[c] = ""
    return df[MEMBERS_HEADERS].copy()

def normalize_ledger(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=LEDGER_HEADERS)
    for c in LEDGER_HEADERS:
        if c not in df.columns:
            df[c] = ""
    return df[LEDGER_HEADERS].copy()

def projects_from_settings(settings_df: pd.DataFrame) -> List[str]:
    if settings_df.empty:
        return []
    return settings_df["Project_Name"].dropna().astype(str).unique().tolist()

def members_for_project(members_df: pd.DataFrame, project: str) -> pd.DataFrame:
    df = members_df.copy()
    df["Project_Name"] = df["Project_Name"].astype(str)
    df = df[df["Project_Name"] == str(project)]
    # activeのみ
    if "IsActive" in df.columns:
        df["IsActive"] = df["IsActive"].astype(str).str.strip()
        df = df[df["IsActive"].str.lower().isin(["1", "true", "yes", "y", "on", "はい", ""])]
    return df

def update_member_principal(members_df: pd.DataFrame, project: str, person: str, new_principal: float) -> pd.DataFrame:
    df = members_df.copy()
    mask = (df["Project_Name"].astype(str) == str(project)) & (df["PersonName"].astype(str) == str(person))
    if mask.any():
        df.loc[mask, "Principal"] = str(new_principal)
        df.loc[mask, "UpdatedAt_JST"] = fmt_jst(now_jst())
    return df

def recompute_project_total_principal(members_df: pd.DataFrame, project: str) -> float:
    df = members_for_project(members_df, project)
    if df.empty:
        return 0.0
    return float(sum(to_f(x) for x in df["Principal"].tolist()))

def upsert_project(settings_df: pd.DataFrame, project: str, net_factor: float, currency: str, is_compound: bool) -> pd.DataFrame:
    df = settings_df.copy()
    project = str(project).strip()
    now = fmt_jst(now_jst())
    mask = df["Project_Name"].astype(str) == project
    row = {
        "Project_Name": project,
        "Net_Factor": str(net_factor),
        "Currency": currency,
        "IsCompound": "TRUE" if is_compound else "FALSE",
        "UpdatedAt_JST": now,
    }
    if mask.any():
        for k, v in row.items():
            df.loc[mask, k] = v
    else:
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    return df

def add_member(members_df: pd.DataFrame, project: str, person: str, principal: float, line_id: str, disp: str, active: bool=True) -> pd.DataFrame:
    df = members_df.copy()
    now = fmt_jst(now_jst())
    row = {
        "Project_Name": str(project).strip(),
        "PersonName": str(person).strip(),
        "Principal": str(principal),
        "Line_User_ID": str(line_id).strip(),
        "LINE_DisplayName": str(disp).strip(),
        "IsActive": "TRUE" if active else "FALSE",
        "CreatedAt_JST": now,
        "UpdatedAt_JST": now,
    }
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    return df

def line_ids_for_project(members_df: pd.DataFrame, project: str) -> List[str]:
    df = members_for_project(members_df, project)
    if df.empty:
        return []
    return only_line_ids(df["Line_User_ID"].tolist())

# =========================
# UI
# =========================
def ui_headers_copy():
    st.write("### ヘッダー（コピペ用）")
    st.write("**Settings**")
    st.code(",".join(SETTINGS_HEADERS))
    st.write("**Members**")
    st.code(",".join(MEMBERS_HEADERS))
    st.write("**Ledger**")
    st.code(",".join(LEDGER_HEADERS))

def ui_debug(gs: GSheets):
    with st.sidebar:
        st.markdown("### 🔎 Debug")
        con = st.secrets.get("connections", {}).get("gsheets", {})
        sid = extract_sheet_id(con.get("spreadsheet", ""))
        st.write(f"spreadsheet: {sid}")
        cred = con.get("credentials", {})
        st.write(f"client_email: {safe_str(cred.get('client_email'))}")
        st.write(f"token_uri: {safe_str(cred.get('token_uri'))}")
        try:
            sh_names = [ws.title for ws in gs.book.worksheets()]
            st.write("Sheets:", ", ".join(sh_names[:10]))
        except Exception:
            pass

def ui_apr(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame):
    st.subheader("📈 APR 確定（画像添付＋全員へ一斉LINE）")

    projects = projects_from_settings(settings_df)
    if not projects:
        st.info("Settings にプロジェクトがありません。管理タブで作成してください。")
        return

    project = st.selectbox("プロジェクト", projects)
    srow = settings_df[settings_df["Project_Name"].astype(str) == str(project)].iloc[0]
    net_factor = to_f(srow["Net_Factor"]) if str(srow["Net_Factor"]).strip() != "" else 0.67
    currency = str(srow["Currency"]).strip() or "JPY"
    is_compound = truthy(srow["IsCompound"])

    dfm = members_for_project(members_df, project)
    if dfm.empty:
        st.warning("このプロジェクトにメンバーがいません（Members）。")
        return

    # 入力
    apr_percent = st.number_input("本日のAPR（%）", value=100.0, step=0.1)
    st.caption(f"受取率（Net_Factor）: {net_factor}（例: 0.67 = 67%） / 通貨: {currency} / 複利: {'ON' if is_compound else 'OFF'}")

    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png","jpg","jpeg"])

    # 計算：総元本 × APR × net_factor / 365 を “人数で均等割”
    total_principal = recompute_project_total_principal(members_df, project)
    n = len(dfm)
    if n <= 0:
        return

    total_reward = (total_principal * (apr_percent/100.0) * net_factor) / 365.0
    per_reward = total_reward / n if n else 0.0

    st.write("#### 計算結果（確認）")
    st.write(f"- 総元本: {total_principal:,.2f} {currency}")
    st.write(f"- 本日の総受取: {total_reward:,.4f} {currency}")
    st.write(f"- 1人あたり（均等）: {per_reward:,.4f} {currency}")

    # 表（見やすさ用）
    preview = dfm[["PersonName","Principal","Line_User_ID","LINE_DisplayName"]].copy()
    preview["Principal"] = preview["Principal"].apply(lambda x: f"{to_f(x):,.2f}")
    st.dataframe(preview, use_container_width=True, hide_index=True)

    if st.button("APRを確定して、台帳更新＋全員へLINE送信"):
        # 画像アップロード（任意）
        evidence_url = None
        if uploaded is not None:
            with st.spinner("画像をアップロード中（ImgBB）..."):
                evidence_url = upload_imgbb(uploaded.getvalue())
            if not evidence_url:
                st.error("画像アップロードに失敗しました。画像なしで実行する場合は画像を外して再実行してください。")
                return

        # 台帳に「各人」分を記録＆元本増加（APRも増額扱い）
        token = safe_str(st.secrets.get("line", {}).get("channel_access_token", ""))
        dt = fmt_jst(now_jst())

        updated_members = members_df.copy()

        for _, r in dfm.iterrows():
            person = safe_str(r["PersonName"])
            line_uid = safe_str(r.get("Line_User_ID",""))
            disp = safe_str(r.get("LINE_DisplayName",""))
            cur_p = to_f(r.get("Principal","0"))

            # APRは元本に加算（ご要望通り）
            new_p = cur_p + float(per_reward)
            updated_members = update_member_principal(updated_members, project, person, new_p)

            gs.append_row(gs.cfg.ledger_sheet, [
                dt, project, person, "APR", round(float(per_reward), 6), currency,
                f"APR:{apr_percent}%, Net:{net_factor}", evidence_url or "",
                line_uid, disp, "app"
            ])

        # Members 保存
        gs.write_df(gs.cfg.members_sheet, normalize_members(updated_members))

        # 一斉LINE（個人名は入れない）
        if token:
            ids = line_ids_for_project(updated_members, project)
            msg = "🏦【APR 収益報告】\n"
            msg += f"プロジェクト: {project}\n"
            msg += f"日時: {dt}\n"
            msg += f"本日のAPR: {apr_percent}%\n"
            msg += f"受取率: {net_factor*100:.0f}%\n"
            msg += f"総元本: {total_principal:,.2f} {currency}\n"
            msg += f"本日の総受取: {total_reward:,.4f} {currency}\n"
            msg += f"均等配分（人数 {n}）: {per_reward:,.4f} {currency}\n"
            msg += "\n※個別明細は台帳に記録されています。"

            ok, ng = 0, 0
            for uid in ids:
                code = send_line(token, uid, msg, image_url=evidence_url)
                if code == 200:
                    ok += 1
                else:
                    ng += 1
            st.success(f"APR確定：台帳更新完了 / LINE送信 成功 {ok}・失敗 {ng}")
        else:
            st.success("APR確定：台帳更新完了（LINEトークン未設定のため通知なし）")

        gs.clear_cache()
        st.rerun()

def ui_cashflow(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame):
    st.subheader("💸 入金 / 出金（個人へLINE通知＋画像添付）")

    projects = projects_from_settings(settings_df)
    if not projects:
        st.info("Settings にプロジェクトがありません。管理タブで作成してください。")
        return

    project = st.selectbox("プロジェクト", projects, key="cash_project")
    srow = settings_df[settings_df["Project_Name"].astype(str) == str(project)].iloc[0]
    currency = str(srow["Currency"]).strip() or "JPY"

    dfm = members_for_project(members_df, project)
    if dfm.empty:
        st.warning("このプロジェクトにメンバーがいません（Members）。")
        return

    names = dfm["PersonName"].dropna().astype(str).tolist()
    person = st.selectbox("メンバー", names)

    typ = st.selectbox("種別", ["Deposit（入金）", "Withdraw（出金）"])
    amount = st.number_input("金額", min_value=0.0, value=0.0, step=1000.0)
    note = st.text_input("メモ", value="")
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png","jpg","jpeg"], key="cash_img")

    rowm = dfm[dfm["PersonName"].astype(str) == str(person)].iloc[0]
    cur_p = to_f(rowm.get("Principal","0"))
    line_uid = safe_str(rowm.get("Line_User_ID",""))
    disp = safe_str(rowm.get("LINE_DisplayName",""))

    st.info(f"現在元本: {cur_p:,.2f} {currency}")

    if st.button("記録してLINE通知"):
        if amount <= 0:
            st.warning("金額が0です。")
            return

        evidence_url = None
        if uploaded is not None:
            with st.spinner("画像をアップロード中（ImgBB）..."):
                evidence_url = upload_imgbb(uploaded.getvalue())
            if not evidence_url:
                st.error("画像アップロードに失敗しました。画像なしで実行する場合は画像を外して再実行してください。")
                return

        dt = fmt_jst(now_jst())
        token = safe_str(st.secrets.get("line", {}).get("channel_access_token", ""))

        if typ.startswith("Deposit"):
            new_p = cur_p + float(amount)
            ledger_type = "Deposit"
        else:
            new_p = cur_p - float(amount)
            ledger_type = "Withdraw"

        # マイナス防止（必要なら外せます）
        if new_p < 0:
            st.error(f"出金後の元本がマイナスになります: {new_p:,.2f} {currency}\n金額を見直してください。")
            return

        updated_members = update_member_principal(members_df, project, person, new_p)
        gs.write_df(gs.cfg.members_sheet, normalize_members(updated_members))

        gs.append_row(gs.cfg.ledger_sheet, [
            dt, project, person, ledger_type, round(float(amount), 6), currency,
            note, evidence_url or "", line_uid, disp, "app"
        ])

        # 個人へLINE通知
        if token and line_uid:
            msg = "🏦【入出金通知】\n"
            msg += f"プロジェクト: {project}\n"
            msg += f"日時: {dt}\n"
            msg += f"種別: {ledger_type}\n"
            msg += f"金額: {amount:,.2f} {currency}\n"
            msg += f"反映後元本: {new_p:,.2f} {currency}\n"
            if note:
                msg += f"メモ: {note}\n"
            send_line(token, line_uid, msg, image_url=evidence_url)

        # 任意：管理者にも通知
        admin_uid = safe_str(st.secrets.get("line", {}).get("admin_user_id", ""))
        if token and admin_uid:
            msg2 = "🛠【管理者通知】\n"
            msg2 += f"{person} / {project}\n{ledger_type}: {amount:,.2f} {currency}\nAfter: {new_p:,.2f} {currency}\n{dt}"
            send_line(token, admin_uid, msg2, image_url=evidence_url)

        st.success("記録しました（台帳・元本更新・LINE通知）")
        gs.clear_cache()
        st.rerun()

def ui_ledger_view(gs: GSheets):
    st.subheader("📒 台帳（Ledger）")
    try:
        df = normalize_ledger(gs.read_df_cached(gs.cfg.ledger_sheet))
    except Exception as e:
        st.error(f"読み取りエラー: {gs.cfg.ledger_sheet} / {e}")
        return

    if df.empty:
        st.info("Ledger が空です。")
        return

    st.dataframe(df, use_container_width=True, hide_index=True)

def ui_members_view(gs: GSheets):
    st.subheader("👤 メンバー（Members）")
    try:
        df = normalize_members(gs.read_df_cached(gs.cfg.members_sheet))
    except Exception as e:
        st.error(f"読み取りエラー: {gs.cfg.members_sheet} / {e}")
        return

    if df.empty:
        st.info("Members が空です。")
        return

    st.dataframe(df, use_container_width=True, hide_index=True)

def ui_admin(gs: GSheets):
    st.subheader("⚙ 管理（管理者のみ）")
    admin_login_ui()
    if not admin_ok():
        st.info("管理者パスワードを入力してください。")
        return

    st.success("管理者機能が有効です。")
    ui_headers_copy()
    st.divider()

    # 現在データ
    settings_df = normalize_settings(gs.read_df_cached(gs.cfg.settings_sheet))
    members_df  = normalize_members(gs.read_df_cached(gs.cfg.members_sheet))

    # プロジェクト管理
    st.write("## 🧩 プロジェクト管理（Settings）")
    with st.form("project_form"):
        pname = st.text_input("Project_Name（新規/更新）")
        net = st.number_input("Net_Factor（例: 0.67 = 67%）", value=0.67, step=0.01)
        cur = st.text_input("Currency", value="JPY")
        comp = st.checkbox("IsCompound（複利）", value=True)
        ok = st.form_submit_button("保存（Upsert）")

    if ok:
        if not pname.strip():
            st.error("Project_Name を入力してください。")
        else:
            settings_df = upsert_project(settings_df, pname.strip(), float(net), cur.strip() or "JPY", bool(comp))
            gs.write_df(gs.cfg.settings_sheet, normalize_settings(settings_df))
            gs.clear_cache()
            st.success("保存しました。")
            st.rerun()

    st.write("### Settings 現在値")
    st.dataframe(settings_df, use_container_width=True, hide_index=True)

    st.divider()

    # メンバー管理
    st.write("## 👤 メンバー管理（Members）")
    projects = projects_from_settings(settings_df)
    if projects:
        p = st.selectbox("対象プロジェクト", projects, key="admin_project")
        with st.form("member_add_form"):
            person = st.text_input("PersonName")
            principal = st.number_input("Principal（元本）", min_value=0.0, value=0.0, step=1000.0)
            line_id = st.text_input("Line_User_ID（任意）", placeholder="Uxxxxxxxxxxxxxxxx")
            disp = st.text_input("LINE_DisplayName（任意）", placeholder="表示名")
            active = st.checkbox("IsActive", value=True)
            ok2 = st.form_submit_button("メンバー追加")

        if ok2:
            if not person.strip():
                st.error("PersonName は必須です。")
            else:
                members_df = add_member(members_df, p, person.strip(), float(principal), line_id, disp, active=active)
                gs.write_df(gs.cfg.members_sheet, normalize_members(members_df))
                gs.clear_cache()
                st.success("メンバーを追加しました。")
                st.rerun()

    st.write("### Members 現在値")
    st.dataframe(members_df, use_container_width=True, hide_index=True)

# =========================
# Main
# =========================
def main():
    st.set_page_config(page_title="APR資産運用管理システム", layout="wide", page_icon="🏦")
    st.title("🏦 APR資産運用管理システム（完全版）")

    # connections.gsheets から spreadsheet を読む
    con = st.secrets.get("connections", {}).get("gsheets", {})
    sid_raw = safe_str(con.get("spreadsheet", ""))
    if not sid_raw:
        st.error('Secrets に [connections.gsheets].spreadsheet がありません。')
        st.stop()

    spreadsheet_id = extract_sheet_id(sid_raw)

    gs = GSheets(GSheetsConfig(spreadsheet_id=spreadsheet_id))
    ui_debug(gs)

    # データ読み込み（キャッシュ）
    try:
        settings_df = normalize_settings(gs.read_df_cached(gs.cfg.settings_sheet))
        members_df  = normalize_members(gs.read_df_cached(gs.cfg.members_sheet))
    except APIError as e:
        if "429" in str(e):
            st.warning("Google Sheets の読み取り制限(429)に達しています。少し待ってから再読込してください。")
            if st.button("再読込（キャッシュクリア）"):
                gs.clear_cache()
                st.rerun()
            st.stop()
        st.error(f"読み取りエラー: {e}")
        st.stop()
    except Exception as e:
        st.error(f"読み取りエラー: {e}")
        st.stop()

    tabs = st.tabs(["📈 APR確定", "💸 入金/出金", "📒 台帳", "👤 メンバー", "⚙ 管理"])

    with tabs[0]:
        ui_apr(gs, settings_df, members_df)

    with tabs[1]:
        ui_cashflow(gs, settings_df, members_df)

    with tabs[2]:
        ui_ledger_view(gs)

    with tabs[3]:
        ui_members_view(gs)

    with tabs[4]:
        ui_admin(gs)

if __name__ == "__main__":
    main()
