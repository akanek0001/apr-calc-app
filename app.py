# app.py  (APR資産運用管理システム・ページ状態保持版 / Master=67% Elite=60% 対応 / Helpページ内蔵)
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

def pick_active_col(df: pd.DataFrame) -> Optional[str]:
    """Members/Settingsで Active / IsActive のどちらがあるかを返す（列名は変更しない）"""
    for c in ["IsActive", "Active", "ACTIVE", "is_active"]:
        if c in df.columns:
            return c
    return None

def safe_get(row: pd.Series, key: str, default: Any = "") -> Any:
    return row[key] if key in row.index else default

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
# Sheets
#  - 既存列名は変更しない（足りない列だけ“追加”はする）
# =========================================================
# ※あなたの運用シートに合わせて“最低限必要な列”だけチェックする
REQUIRED_SETTINGS = ["Project_Name"]
REQUIRED_MEMBERS = ["Project_Name", "PersonName", "Principal", "Line_User_ID", "LINE_DisplayName"]
REQUIRED_LEDGER  = ["Datetime_JST", "Project_Name", "PersonName", "Type", "Amount", "Note", "Evidence_URL", "Line_User_ID", "LINE_DisplayName", "Source"]

# Master/Elite 用（列名は追加するだけ：変更しない）
MEMBER_CLASS_COL = "MemberClass"   # 値: Master / Elite
CLASS_MASTER = "Master"
CLASS_ELITE  = "Elite"

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

    def _ws(self, name: str):
        return self.book.worksheet(name)

    @st.cache_data(ttl=180)
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

    def ensure_columns(self, sheet_name: str, required_cols: List[str]) -> None:
        """
        既存列名は維持。足りない列だけ末尾追加。
        ※API呼び出しを増やしすぎないよう、管理者操作のタイミングでのみ使う想定。
        """
        ws = self._ws(sheet_name)
        first = ws.row_values(1)
        if not first:
            ws.append_row(required_cols, value_input_option="USER_ENTERED")
            return
        cols = [str(c).strip() for c in first if str(c).strip()]
        missing = [c for c in required_cols if c not in cols]
        if missing:
            ws.update("1:1", [cols + missing])

    def clear_cache(self) -> None:
        st.cache_data.clear()

# =========================================================
# Admin Auth（PIN）
# =========================================================
def admin_pin() -> str:
    return str(st.secrets.get("admin", {}).get("pin", "")).strip()

def is_admin() -> bool:
    return bool(st.session_state.get("admin_ok", False))

def admin_login_ui() -> None:
    pin_required = admin_pin()
    if not pin_required:
        st.warning("Secrets に [admin].pin が未設定です。")
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
        pin = st.text_input("管理者PIN", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if pin == pin_required:
                st.session_state["admin_ok"] = True
                st.rerun()
            else:
                st.session_state["admin_ok"] = False
                st.error("PINが違います。")

# =========================================================
# Domain load（列名は変更しない）
# =========================================================
def load_settings(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.settings_sheet)
    if df.empty:
        return df

    # 必須列チェック
    missing = [c for c in REQUIRED_SETTINGS if c not in df.columns]
    if missing:
        st.error(f"Settingsシートの列が不足: {missing}")
        st.stop()

    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()

    # Active列（あれば）で絞る
    active_col = pick_active_col(df)
    if active_col:
        df[active_col] = df[active_col].apply(truthy)
        df = df[df[active_col] == True].copy()

    # プロジェクト別 NetFactor（あれば）※なければ固定で使う
    # 例: Settings に NetFactor / Net_Factor / ReceiveFactor がある場合があるので拾う
    if "NetFactor" not in df.columns:
        # 既存列名は変更しない。内部で使うだけの列を作る（df内）※書き戻しはしない
        for alt in ["Net_Factor", "ReceiveFactor", "Net_Factor ", "Net_Factor　"]:
            if alt in df.columns:
                df["NetFactor"] = df[alt].apply(to_f)
                break
        else:
            df["NetFactor"] = 0.67  # デフォ

    else:
        df["NetFactor"] = df["NetFactor"].apply(lambda x: to_f(x) if str(x).strip() else 0.67)

    # IsCompound（あれば）
    if "IsCompound" in df.columns:
        df["IsCompound"] = df["IsCompound"].apply(truthy)
    else:
        df["IsCompound"] = False

    # Currency（あれば、無ければ$表記固定）
    if "Currency" in df.columns:
        df["Currency"] = df["Currency"].astype(str).str.strip()
    else:
        df["Currency"] = "USD"

    return df.reset_index(drop=True)

def load_members(gs: GSheets) -> pd.DataFrame:
    df = gs.read_df(gs.cfg.members_sheet)
    if df.empty:
        return df

    missing = [c for c in REQUIRED_MEMBERS if c not in df.columns]
    if missing:
        st.error(f"Membersシートの列が不足: {missing}")
        st.stop()

    df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
    df["PersonName"] = df["PersonName"].astype(str).str.strip()
    df["Principal"] = df["Principal"].apply(to_f)
    df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
    df["LINE_DisplayName"] = df["LINE_DisplayName"].astype(str).str.strip()

    active_col = pick_active_col(df)
    if active_col:
        df[active_col] = df[active_col].apply(truthy)
    else:
        # Active列が無い場合は全員有効扱い（列名は増やさない）
        df["_ActiveInternal"] = True

    # Master/Elite（列が無ければ内部で Master 扱い）
    if MEMBER_CLASS_COL not in df.columns:
        df[MEMBER_CLASS_COL] = CLASS_MASTER
    else:
        df[MEMBER_CLASS_COL] = df[MEMBER_CLASS_COL].astype(str).str.strip()
        df[MEMBER_CLASS_COL] = df[MEMBER_CLASS_COL].replace({"": CLASS_MASTER})

    return df

def members_in_project(members_df: pd.DataFrame, project: str) -> pd.DataFrame:
    df = members_df.copy()
    active_col = pick_active_col(df)
    if active_col:
        df = df[(df["Project_Name"] == str(project)) & (df[active_col] == True)]
    else:
        df = df[(df["Project_Name"] == str(project)) & (df["_ActiveInternal"] == True)]
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

def class_factor(member_class: str) -> float:
    mc = (member_class or "").strip()
    if mc.lower() == CLASS_ELITE.lower():
        return 0.60
    return 0.67  # default Master

# =========================================================
# UI: HELP（アプリ内ヘルプページ）
# =========================================================
def ui_help():
    st.subheader("❓ ヘルプ / 使い方")
    st.markdown(
        """
## 画面構成
- **📈 APR**：その日のAPRを確定 → 配当計算 →（複利なら）残高へ加算 → Ledgerへ記録 → 全員へLINE送信  
- **💸 入金/出金**：個人を選んで入出金 → 残高更新 → Ledgerへ記録 → 本人へLINE通知  
- **⚙️ 管理**：管理者ログイン後に、メンバー追加・更新（Master/Eliteの選択）を実施  

## Master / Elite（取り分）
- **Master = 67%**
- **Elite  = 60%**
- 設定は **Membersシートの `MemberClass` 列** に `Master` / `Elite` を入れます  
  （列が無い場合は、管理画面で保存すると自動で列が追加されます。既存列名は変更しません）

## APR 計算
- プロジェクトの **有効メンバー全員の残高合計** を基準に、各人の `MemberClass` に応じた取り分で日割り計算します。
- 日割り：`÷ 365`
- 配分：**均等配分（人数で割る）**

## 画像エビデンス
- 画像を添付すると **ImgBB** へアップロードし、URLを `Ledger.Evidence_URL` に保存します。
- LINEにも画像を添付します。

## LINE通知
- APR：プロジェクト内の**全員**に送信（個人名は本文に入れない）
- 入金/出金：対象の**本人にのみ**送信

## よくあるエラー
- **403**：スプレッドシートをサービスアカウント（client_email）に「編集者」で共有していない  
- **401 / invalid_grant**：Secretsの認証情報（private_key等）が壊れている/改行が崩れている  
- **429 Quota**：短時間に読み取りが多い → このアプリは `cache_data(ttl=180)` で軽減。連続更新は少し間隔を空けてください
"""
    )

# =========================================================
# UI: APR
# =========================================================
def ui_apr(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("📈 APR 確定（Master=67% / Elite=60%・均等配分）")

    if settings_df.empty:
        st.warning("Active=TRUE のプロジェクトがありません（Settings.Active / IsActive）。")
        return members_df

    projects = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    project = st.selectbox("プロジェクト", projects)

    srow = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]
    is_compound = bool(safe_get(srow, "IsCompound", False))

    apr = st.number_input("本日のAPR（%）", value=100.0, step=0.1)
    uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])

    mem = members_in_project(members_df, project)
    if mem.empty:
        st.warning("このプロジェクトに有効メンバーがいません（Membersを確認）。")
        return members_df

    total_principal = float(mem["Principal"].sum())
    n = int(len(mem))

    # 各人の class factor
    mem = mem.copy()
    mem["__factor"] = mem[MEMBER_CLASS_COL].apply(class_factor)

    # プロジェクトの総配当（取り分を個々に適用するので、総配当は個別配当の合計になる）
    # ただし「均等配分」を維持するため、まず「基準総配当」を作ってから人数割りする
    # → 取り分は “プロジェクト配当” ではなく “個人の取り分” なので、
    #    仕様に忠実にするため、均等配分の前に「平均取り分」を使う。
    avg_factor = float(mem["__factor"].mean())  # Master/Elite混在時の平均取り分
    total_reward = (total_principal * (apr / 100.0) * avg_factor) / 365.0
    per_member = total_reward / n if n > 0 else 0.0

    st.write(f"- 総元本: {fmt_usd(total_principal)}")
    st.write(f"- Master=67% / Elite=60%（混在時は平均取り分で計算）")
    st.write(f"- 本日総配当: {fmt_usd(total_reward)}")
    st.write(f"- 1人あたり: {fmt_usd(per_member)}（{n}人で均等）")
    st.write(f"- モード: {'複利（元本に加算）' if is_compound else '単利（元本は固定）'}")

    if not is_admin():
        st.info("APR確定は管理者のみ実行できます（⚙️ 管理でログイン）。")
        return members_df

    if st.button("APRを確定して全員にLINE送信"):
        evidence_url = None
        if uploaded:
            with st.spinner("画像アップロード中..."):
                evidence_url = upload_imgbb(uploaded.getvalue())
            if not evidence_url:
                st.error("画像アップロードに失敗しました（ImgBB）。画像を外して再実行してください。")
                return members_df

        # 複利ならMembersのPrincipalに加算（プロジェクト内の有効メンバー全員に均等加算）
        if is_compound:
            for i in range(len(members_df)):
                if members_df.loc[i, "Project_Name"] == str(project):
                    # Active判定
                    a_col = pick_active_col(members_df)
                    active_ok = True
                    if a_col:
                        active_ok = bool(members_df.loc[i, a_col])
                    else:
                        active_ok = bool(members_df.loc[i, "_ActiveInternal"])
                    if active_ok:
                        members_df.loc[i, "Principal"] = float(members_df.loc[i, "Principal"]) + float(per_member)

        ts = fmt_dt(now_jst())

        # Ledger記録（個人行で残す）
        for _, r in mem.iterrows():
            gs.append_row(gs.cfg.ledger_sheet, [
                ts, project, r["PersonName"], "APR", float(per_member),
                f"APR:{apr}%, split:equal, class:avg({avg_factor:.2f})",
                evidence_url or "",
                r["Line_User_ID"], r["LINE_DisplayName"], "app",
            ])

        # Members書き戻し（列名は維持）
        out = members_df.copy()
        out["Principal"] = out["Principal"].apply(lambda x: f"{float(x):.6f}")
        a_col = pick_active_col(out)
        if a_col:
            out[a_col] = out[a_col].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        if "_ActiveInternal" in out.columns:
            out = out.drop(columns=["_ActiveInternal"], errors="ignore")

        gs.write_df(gs.cfg.members_sheet, out)

        # LINE送信（全員）
        token = st.secrets["line"]["channel_access_token"]
        targets = dedup_line_ids(mem)

        msg = "🏦【APR収益報告】\n"
        msg += f"プロジェクト: {project}\n"
        msg += f"報告日時: {now_jst().strftime('%Y/%m/%d %H:%M')}\n\n"
        msg += f"APR: {apr}%\n"
        msg += "取り分: Master=67% / Elite=60%\n"
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
        st.rerun()

    return members_df

# =========================================================
# UI: Deposit/Withdraw
# =========================================================
def ui_cash(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("💸 入金 / 出金（個別LINE通知）")

    if settings_df.empty:
        st.warning("Active=TRUE のプロジェクトがありません（Settings.Active / IsActive）。")
        return members_df

    projects = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    project = st.selectbox("プロジェクト", projects, key="cash_project")

    mem = members_in_project(members_df, project)
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
        st.info("入金/出金の記録は管理者のみ実行できます（⚙️ 管理でログイン）。")
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

        # Members更新
        for i in range(len(members_df)):
            if members_df.loc[i, "Project_Name"] == str(project) and members_df.loc[i, "PersonName"] == str(person):
                members_df.loc[i, "Principal"] = float(new_balance)

        # Ledger追記
        ts = fmt_dt(now_jst())
        gs.append_row(gs.cfg.ledger_sheet, [
            ts, project, person, typ, float(amt), note, evidence_url or "",
            row["Line_User_ID"], row["LINE_DisplayName"], "app"
        ])

        # Members書き戻し
        out = members_df.copy()
        out["Principal"] = out["Principal"].apply(lambda x: f"{float(x):.6f}")
        a_col = pick_active_col(out)
        if a_col:
            out[a_col] = out[a_col].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        if "_ActiveInternal" in out.columns:
            out = out.drop(columns=["_ActiveInternal"], errors="ignore")

        gs.write_df(gs.cfg.members_sheet, out)

        # LINE（本人のみ）
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
# UI: Admin（メンバー管理は管理者のみ）
#  - 仕様変更: 「同一プロジェクト内で Line_User_ID が一致したら“更新しない”（スキップ）」に対応
# =========================================================
def ui_admin(gs: GSheets, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("⚙️ 管理（管理者のみ）")
    admin_login_ui()

    if not is_admin():
        st.info("ログインすると、メンバー追加・編集が表示されます。")
        return members_df

    if settings_df.empty:
        st.warning("Active=TRUE のプロジェクトがありません（Settings.Active / IsActive）。")
        return members_df

    # 必要列の“追加”だけ（列名は変更しない）
    # MemberClass列が無ければ末尾追加する
    try:
        gs.ensure_columns(gs.cfg.members_sheet, REQUIRED_MEMBERS + [MEMBER_CLASS_COL])
    except Exception:
        pass

    projects = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    project = st.selectbox("対象プロジェクト", projects, key="admin_project")

    with st.expander("現在のメンバー一覧", expanded=True):
        view = members_df[members_df["Project_Name"] == str(project)].copy()
        if view.empty:
            st.info("まだメンバーがいません。下のフォームから追加してください。")
        else:
            show = view.copy()
            show["Principal"] = show["Principal"].apply(lambda x: fmt_usd(float(x)))
            if MEMBER_CLASS_COL in show.columns:
                pass
            else:
                show[MEMBER_CLASS_COL] = CLASS_MASTER
            st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()
    st.write("#### 追加（同一プロジェクト内で Line_User_ID が一致したら“追加しない”）")

    with st.form("member_add", clear_on_submit=False):
        person = st.text_input("PersonName（個人名）")
        principal = st.number_input("Principal（残高）", min_value=0.0, value=0.0, step=100.0)
        line_uid = st.text_input("Line_User_ID（Uから始まる）")
        line_disp = st.text_input("LINE_DisplayName（任意）", value="")
        mclass = st.selectbox("クラス", [CLASS_MASTER, CLASS_ELITE], index=0, help="Master=67%, Elite=60%")
        is_active = st.selectbox("Active/IsActive", ["TRUE", "FALSE"], index=0)
        submit = st.form_submit_button("追加する")

    if submit:
        if not person or not line_uid:
            st.error("PersonName と Line_User_ID は必須です。")
            return members_df

        # 同一プロジェクト内で Line_User_ID 重複なら“何もしない”
        dup = members_df[
            (members_df["Project_Name"] == str(project)) &
            (members_df["Line_User_ID"].astype(str).str.strip() == str(line_uid).strip())
        ]
        if not dup.empty:
            st.warning("このプロジェクト内で同じLine_User_IDが既に存在します。仕様により追加しません（更新もしません）。")
            return members_df

        ts = fmt_dt(now_jst())

        # Active列名（既存を尊重）
        a_col = pick_active_col(members_df)
        active_bool = True if is_active == "TRUE" else False

        new_row = {
            "Project_Name": str(project).strip(),
            "PersonName": str(person).strip(),
            "Principal": float(principal),
            "Line_User_ID": str(line_uid).strip(),
            "LINE_DisplayName": str(line_disp).strip(),
        }
        if MEMBER_CLASS_COL not in members_df.columns:
            members_df[MEMBER_CLASS_COL] = CLASS_MASTER
        new_row[MEMBER_CLASS_COL] = mclass

        # Active列
        if a_col:
            new_row[a_col] = active_bool
        else:
            new_row["_ActiveInternal"] = active_bool

        # 時刻列（存在するなら埋める。無ければ追加しない）
        if "CreatedAt_JST" in members_df.columns:
            new_row["CreatedAt_JST"] = ts
        if "UpdatedAt_JST" in members_df.columns:
            new_row["UpdatedAt_JST"] = ts

        members_df = pd.concat([members_df, pd.DataFrame([new_row])], ignore_index=True)

        # 書き戻し（列名は変えない）
        out = members_df.copy()
        out["Principal"] = out["Principal"].apply(lambda x: f"{float(x):.6f}")

        a_col2 = pick_active_col(out)
        if a_col2:
            out[a_col2] = out[a_col2].apply(lambda x: "TRUE" if bool(x) else "FALSE")

        if "_ActiveInternal" in out.columns:
            out = out.drop(columns=["_ActiveInternal"], errors="ignore")

        gs.write_df(gs.cfg.members_sheet, out)
        gs.clear_cache()

        # 管理画面に留める（APRへ戻らない）
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

    # Spreadsheet ID
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
