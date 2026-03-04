# =========================
# App Config (connections.gsheets 方式)
# =========================

from dataclasses import dataclass
from typing import Optional
import streamlit as st

@dataclass
class GSheetsConfig:
    registry_spreadsheet_id: str
    members_sheet_name: str
    ledger_sheet_name: str
    use_personal_sheets: bool = False
    personal_map_sheet_name: str = "PersonalMap"


def load_gsheets_cfg() -> Optional[GSheetsConfig]:
    # connections.gsheets.spreadsheet を読む
    con = st.secrets.get("connections", {}).get("gsheets", {})
    sid = str(con.get("spreadsheet", "")).strip()
    if not sid:
        return None

    # URLで入っていたらIDだけ抜く（安全）
    # 例: https://docs.google.com/spreadsheets/d/<ID>/edit?gid=... → <ID>
    if "/spreadsheets/d/" in sid:
        try:
            sid = sid.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception:
            pass

    g = st.secrets.get("gsheets", {})  # 任意：シート名だけは [gsheets] で上書きできるようにする
    members = str(g.get("members_sheet_name", "Members")).strip() or "Members"
    ledger = str(g.get("ledger_sheet_name", "Ledger")).strip() or "Ledger"
    use_personal = bool(g.get("use_personal_sheets", False))
    pmap = str(g.get("personal_map_sheet_name", "PersonalMap")).strip() or "PersonalMap"

    return GSheetsConfig(
        registry_spreadsheet_id=sid,
        members_sheet_name=members,
        ledger_sheet_name=ledger,
        use_personal_sheets=use_personal,
        personal_map_sheet_name=pmap,
    )


# =========================
# Google Sheets Client (connections.gsheets.credentials 方式)
# =========================

import gspread
from google.oauth2.service_account import Credentials

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

        self.registry = self.gc.open_by_key(self.cfg.registry_spreadsheet_id)

    def ws(self, name: str):
        return self.registry.worksheet(name)

    # 以下、あなたの元コードの read_df / append_row / upsert_member... をそのまま残す




st.title("接続テスト")

cfg = load_gsheets_cfg()
st.write("cfg:", cfg)

if cfg is None:
    st.error("Secretsのconnections.gsheets.spreadsheetが読めていません")
    st.stop()

gs = GSheets(cfg)
st.success("Google Sheets 接続OK")

# Membersシートを読めるか試す
ws = gs.ws("Members")
st.write("Members sheet title:", ws.title)
st.write("Rows:", ws.row_count, "Cols:", ws.col_count)
