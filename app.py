from __future__ import annotations

# =========================================================
# IMPORT
# =========================================================
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Any, Dict, List, Optional, Set, Tuple
import json
import re

import pandas as pd
import requests
import streamlit as st
from PIL import Image, ImageEnhance, ImageFilter, ImageOps, ImageDraw

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# =========================================================
# CONFIG
# =========================================================
class AppConfig:
    APP_TITLE = "APR資産運用管理システム"
    APP_ICON = "🏦"
    PAGE_LAYOUT = "wide"
    JST = timezone(timedelta(hours=9), "JST")

    STATUS = {"ON": "🟢運用中", "OFF": "🔴停止"}
    RANK = {"MASTER": "Master", "ELITE": "Elite"}
    FACTOR = {"MASTER": 0.67, "ELITE": 0.60}
    RANK_LABEL = "👑Master=67% / 🥈Elite=60%"

    PROJECT = {"PERSONAL": "PERSONAL"}
    COMPOUND = {"DAILY": "daily", "MONTHLY": "monthly", "NONE": "none"}
    COMPOUND_LABEL = {"daily": "日次複利", "monthly": "月次複利", "none": "単利"}

    TYPE = {"APR": "APR", "LINE": "LINE", "DEPOSIT": "Deposit", "WITHDRAW": "Withdraw"}
    SOURCE = {"APP": "app"}

    SHEET = {
        "SETTINGS": "Settings",
        "MEMBERS": "Members",
        "LEDGER": "Ledger",
        "LINEUSERS": "LineUsers",
        "APR_SUMMARY": "APR_Summary",
        "SMARTVAULT_HISTORY": "SmartVault_History",
    }

    HEADERS = {
        "SETTINGS": [
            "Project_Name",
            "Net_Factor",
            "IsCompound",
            "Compound_Timing",
            "Crop_Left_Ratio_PC",
            "Crop_Top_Ratio_PC",
            "Crop_Right_Ratio_PC",
            "Crop_Bottom_Ratio_PC",
            "Crop_Left_Ratio_Mobile",
            "Crop_Top_Ratio_Mobile",
            "Crop_Right_Ratio_Mobile",
            "Crop_Bottom_Ratio_Mobile",
            "UpdatedAt_JST",
            "Active",
        ],
        "MEMBERS": [
            "Project_Name",
            "PersonName",
            "Principal",
            "Line_User_ID",
            "LINE_DisplayName",
            "Rank",
            "IsActive",
            "CreatedAt_JST",
            
            "UpdatedAt_JST",
        ],
        "LEDGER": [
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
        ],
        "LINEUSERS": ["Date", "Time", "Type", "Line_User_ID", "Line_User"],
        "APR_SUMMARY": ["Date_JST", "PersonName", "Total_APR", "APR_Count", "Asset_Ratio", "LINE_DisplayName"],
        "SMARTVAULT_HISTORY": [
            "Datetime_JST",
            "Project_Name",
            "Liquidity",
            "Yesterday_Profit",
            "APR",
            "Source_Mode",
            "OCR_Liquidity",
            "OCR_Yesterday_Profit",
            "OCR_APR",
            "Evidence_URL",
            "Admin_Name",
            "Admin_Namespace",
            "Note",
        ],
    }

    PAGE = {
        "DASHBOARD": "📊 ダッシュボード",
        "APR": "📈 APR",
        "CASH": "💸 入金/出金",
        "ADMIN": "⚙️ 管理",
        "HELP": "❓ ヘルプ",
    }

    SESSION_KEYS = {
        "SETTINGS": "settings_df",
        "MEMBERS": "members_df",
        "LEDGER": "ledger_df",
        "LINEUSERS": "line_users_df",
        "APR_SUMMARY": "apr_summary_df",
    }

    APR_LINE_NOTE_KEYWORD = "APR:"

    OCR_DEFAULTS_PC = {
        "Crop_Left_Ratio_PC": 0.70,
        "Crop_Top_Ratio_PC": 0.20,
        "Crop_Right_Ratio_PC": 0.90,
        "Crop_Bottom_Ratio_PC": 0.285,
    }

    OCR_DEFAULTS_MOBILE = {
        "Crop_Left_Ratio_Mobile": 0.68,
        "Crop_Top_Ratio_Mobile": 0.23,
        "Crop_Right_Ratio_Mobile": 0.92,
        "Crop_Bottom_Ratio_Mobile": 0.355,
    }

    SMARTVAULT_BOXES_MOBILE = {
        "TOTAL_LIQUIDITY": {"left": 0.05, "top": 0.25, "right": 0.40, "bottom": 0.34},
        "YESTERDAY_PROFIT": {"left": 0.41, "top": 0.25, "right": 0.69, "bottom": 0.34},
        "APR": {"left": 0.70, "top": 0.25, "right": 0.93, "bottom": 0.34},
    }


# =========================================================
# UTILS
# =========================================================
class U:
    @staticmethod
    def now_jst() -> datetime:
        return datetime.now(AppConfig.JST)

    @staticmethod
    def fmt_dt(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def fmt_date(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def fmt_usd(x: float) -> str:
        return f"${x:,.2f}"

    @staticmethod
    def to_f(v: Any) -> float:
        try:
            s = str(v).replace(",", "").replace("$", "").replace("%", "").strip()
            return float(s) if s else 0.0
        except Exception:
            return 0.0

    @staticmethod
    def to_num_series(s: pd.Series, default: float = 0.0) -> pd.Series:
        out = pd.to_numeric(
            s.astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("$", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.strip(),
            errors="coerce",
        )
        return out.fillna(default)

    @staticmethod
    def truthy(v: Any) -> bool:
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("1", "true", "yes", "y", "on", "はい", "t")

    @staticmethod
    def truthy_series(s: pd.Series) -> pd.Series:
        return s.astype(str).str.strip().str.lower().isin(["1", "true", "yes", "y", "on", "はい", "t"])

    @staticmethod
    def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out.columns = out.columns.astype(str).str.replace("\u3000", " ", regex=False).str.strip()
        return out

    @staticmethod
    def extract_sheet_id(value: str) -> str:
        sid = (value or "").strip()
        if "/spreadsheets/d/" in sid:
            try:
                sid = sid.split("/spreadsheets/d/")[1].split("/")[0]
            except Exception:
                pass
        return sid

    @staticmethod
    def normalize_rank(rank: Any) -> str:
        return AppConfig.RANK["ELITE"] if str(rank).strip().lower() == "elite" else AppConfig.RANK["MASTER"]

    @staticmethod
    def rank_factor(rank: Any) -> float:
        return AppConfig.FACTOR["ELITE"] if str(rank).strip().lower() == "elite" else AppConfig.FACTOR["MASTER"]

    @staticmethod
    def bool_to_status(v: Any) -> str:
        return AppConfig.STATUS["ON"] if U.truthy(v) else AppConfig.STATUS["OFF"]

    @staticmethod
    def status_to_bool(v: Any) -> bool:
        return str(v).strip() == AppConfig.STATUS["ON"]

    @staticmethod
    def normalize_compound(v: Any) -> str:
        s = str(v).strip().lower()
        return s if s in AppConfig.COMPOUND.values() else AppConfig.COMPOUND["NONE"]

    @staticmethod
    def compound_label(v: Any) -> str:
        return AppConfig.COMPOUND_LABEL[U.normalize_compound(v)]

    @staticmethod
    def is_line_uid(v: Any) -> bool:
        s = str(v).strip()
        return s.startswith("U") and len(s) >= 10

    @staticmethod
    def sheet_name(base: str, ns: str) -> str:
        ns = str(ns or "").strip()
        return base if not ns or ns == "default" else f"{base}__{ns}"

    @staticmethod
    def insert_person_name(msg_common: str, person_name: str) -> str:
        name_line = f"{person_name} 様"
        lines = msg_common.splitlines()
        if name_line in lines:
            return msg_common
        if lines and lines[0].strip() == "【ご連絡】":
            return "\n".join([lines[0], name_line] + lines[1:])
        return "\n".join([name_line] + lines)

    @staticmethod
    def apr_val(x: str) -> float:
        s = str(x).replace("%", "").replace(",", "").strip()
        if not s:
            return 0.0
        try:
            return float(s)
        except Exception:
            return 0.0

    @staticmethod
    def to_ratio(v: Any, default: float) -> float:
        try:
            x = float(str(v).strip())
            if 0.0 <= x <= 1.0:
                return x
            return default
        except Exception:
            return default

    @staticmethod
    def crop_image_by_ratio(
        file_bytes: bytes,
        left_ratio: float,
        top_ratio: float,
        right_ratio: float,
        bottom_ratio: float,
    ) -> bytes:
        try:
            img = Image.open(BytesIO(file_bytes)).convert("RGB")
            w, h = img.size

            left = max(0, min(int(w * left_ratio), w - 1))
            top = max(0, min(int(h * top_ratio), h - 1))
            right = max(left + 1, min(int(w * right_ratio), w))
            bottom = max(top + 1, min(int(h * bottom_ratio), h))

            cropped = img.crop((left, top, right, bottom))
            buf = BytesIO()
            cropped.save(buf, format="PNG")
            return buf.getvalue()
        except Exception:
            return file_bytes

    @staticmethod
    def is_mobile_tall_image(file_bytes: bytes) -> bool:
        try:
            img = Image.open(BytesIO(file_bytes))
            w, h = img.size
            return h / max(w, 1) > 1.45
        except Exception:
            return False

    @staticmethod
    def preprocess_ocr_image(file_bytes: bytes) -> List[bytes]:
        outputs: List[bytes] = []

        try:
            base = Image.open(BytesIO(file_bytes)).convert("L")
            variants: List[Image.Image] = []

            img1 = ImageOps.autocontrast(base)
            img1 = ImageEnhance.Contrast(img1).enhance(3.0)
            img1 = ImageEnhance.Sharpness(img1).enhance(2.5)
            img1 = img1.resize((base.width * 4, base.height * 4))
            variants.append(img1)

            img2 = ImageOps.autocontrast(base)
            img2 = ImageEnhance.Contrast(img2).enhance(3.5)
            img2 = img2.resize((base.width * 5, base.height * 5))
            img2 = img2.point(lambda x: 255 if x > 165 else 0)
            variants.append(img2)

            img3 = ImageOps.autocontrast(base)
            img3 = ImageEnhance.Contrast(img3).enhance(3.2)
            img3 = img3.resize((base.width * 5, base.height * 5))
            img3 = img3.point(lambda x: 255 if x > 145 else 0)
            variants.append(img3)

            img4 = ImageOps.autocontrast(base)
            img4 = img4.filter(ImageFilter.MedianFilter(size=3))
            img4 = ImageEnhance.Contrast(img4).enhance(2.8)
            img4 = ImageEnhance.Sharpness(img4).enhance(3.2)
            img4 = img4.resize((base.width * 4, base.height * 4))
            variants.append(img4)

            for img in variants:
                buf = BytesIO()
                img.save(buf, format="PNG")
                outputs.append(buf.getvalue())

        except Exception:
            return [file_bytes]

        return outputs if outputs else [file_bytes]

    @staticmethod
    def extract_percent_candidates(text: str) -> List[float]:
        if not text:
            return []

        norm = str(text)

        replace_map = {
            "％": "%",
            "O": "0",
            "o": "0",
            "Q": "0",
            "D": "0",
            "I": "1",
            "l": "1",
            "|": "1",
            "S": "5",
            "s": "5",
            ",": ".",
        }
        for k, v in replace_map.items():
            norm = norm.replace(k, v)

        norm = re.sub(r"[ \t\u3000]+", " ", norm)

        patterns = [
            r"(?i)apr\s*[:：]?\s*(\d+(?:\.\d+)?)\s*%",
            r"(?i)apr\s*[:：]?\s*(\d+(?:\.\d+)?)",
            r"(?i)apy\s*[:：]?\s*(\d+(?:\.\d+)?)\s*%",
            r"(?i)rate\s*[:：]?\s*(\d+(?:\.\d+)?)\s*%",
            r"(\d+(?:\.\d+)?)\s*%",
            r"(\d{1,3}\.\d{1,4})",
        ]

        vals: List[float] = []
        seen = set()

        for pat in patterns:
            for v in re.findall(pat, norm):
                try:
                    f = float(v)
                    if 0 <= f <= 300:
                        key = round(f, 6)
                        if key not in seen:
                            seen.add(key)
                            vals.append(f)
                except Exception:
                    pass

        def score(x: float) -> tuple:
            if 1 <= x <= 80:
                return (0, abs(x - 40))
            if 80 < x <= 150:
                return (1, abs(x - 100))
            return (2, x)

        return sorted(vals, key=score)

    @staticmethod
    def extract_usd_candidates(text: str) -> List[float]:
        if not text:
            return []

        norm = str(text)

        replace_map = {
            "＄": "$",
            "，": ",",
            "。": ".",
            "O": "0",
            "o": "0",
            "Q": "0",
            "D": "0",
            "I": "1",
            "l": "1",
            "|": "1",
            "S": "5",
            "s": "5",
        }
        for k, v in replace_map.items():
            norm = norm.replace(k, v)

        norm = re.sub(r"[ \t\u3000]+", " ", norm)

        patterns = [
            r"\$?\s*(\d{1,3}(?:,\d{3})+(?:\.\d+)?)",
            r"\$?\s*(\d+\.\d+)",
        ]

        vals: List[float] = []
        seen = set()

        for pat in patterns:
            for v in re.findall(pat, norm):
                try:
                    f = float(str(v).replace(",", ""))
                    if 0 <= f <= 1000000000:
                        key = round(f, 6)
                        if key not in seen:
                            seen.add(key)
                            vals.append(f)
                except Exception:
                    pass

        return vals

    @staticmethod
    def pick_total_liquidity(vals: List[float]) -> Optional[float]:
        if not vals:
            return None
        positives = [float(v) for v in vals if float(v) > 0]
        if not positives:
            return None
        return max(positives)

    @staticmethod
    def pick_yesterday_profit(vals: List[float]) -> Optional[float]:
        if not vals:
            return None

        candidates = [float(v) for v in vals if float(v) >= 0]
        if not candidates:
            return None

        small_first = [v for v in candidates if v <= 1000000]
        if small_first:
            return sorted(small_first)[0] if len(small_first) == 1 else min(small_first, key=lambda x: len(str(int(x))))
        return min(candidates)

    @staticmethod
    def draw_ocr_boxes(file_bytes: bytes, boxes: Dict[str, Dict[str, float]]) -> bytes:
        try:
            img = Image.open(BytesIO(file_bytes)).convert("RGB")
            draw = ImageDraw.Draw(img)
            w, h = img.size

            for label, box in boxes.items():
                left = int(w * box["left"])
                top = int(h * box["top"])
                right = int(w * box["right"])
                bottom = int(h * box["bottom"])

                draw.rectangle((left, top, right, bottom), outline="red", width=4)
                draw.text((left, max(0, top - 20)), label, fill="red")

            buf = BytesIO()
            img.save(buf, format="PNG")
            return buf.getvalue()
        except Exception:
            return file_bytes

    @staticmethod
    def detect_source_mode(
        final_liquidity: float,
        final_profit: float,
        final_apr: float,
        ocr_liquidity: Optional[float],
        ocr_profit: Optional[float],
        ocr_apr: Optional[float],
    ) -> str:
        has_ocr = any(v is not None for v in [ocr_liquidity, ocr_profit, ocr_apr])
        if not has_ocr:
            return "manual"

        def same(a: Optional[float], b: float) -> bool:
            if a is None:
                return False
            return abs(float(a) - float(b)) < 1e-9

        if same(ocr_liquidity, final_liquidity) and same(ocr_profit, final_profit) and same(ocr_apr, final_apr):
            return "ocr"
        return "ocr+manual"


# =========================================================
# AUTH
# =========================================================
@dataclass
class AdminUser:
    name: str
    pin: str
    namespace: str


class AdminAuth:
    @staticmethod
    def load_users() -> List[AdminUser]:
        admin = st.secrets.get("admin", {}) or {}
        users = admin.get("users")
        if users:
            out: List[AdminUser] = []
            for u in users:
                name = str(u.get("name", "")).strip() or "Admin"
                pin = str(u.get("pin", "")).strip()
                ns = str(u.get("namespace", "")).strip() or name
                if pin:
                    out.append(AdminUser(name=name, pin=pin, namespace=ns))
            if out:
                return out

        pin = str(admin.get("pin", "")).strip() or str(admin.get("password", "")).strip()
        return [AdminUser(name="Admin", pin=pin, namespace="default")] if pin else []

    @staticmethod
    def require_login() -> None:
        admins = AdminAuth.load_users()
        if not admins:
            st.error("Secrets に [admin].users または [admin].pin が未設定です。")
            st.stop()

        if st.session_state.get("admin_ok") and st.session_state.get("admin_namespace"):
            return

        names = [a.name for a in admins]
        default_name = st.session_state.get("login_admin_name", names[0])
        if default_name not in names:
            default_name = names[0]

        st.markdown("## 🔐 管理者ログイン")
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
                st.session_state["admin_ok"] = False
                st.session_state["admin_name"] = ""
                st.session_state["admin_namespace"] = ""
                st.error("PINが違います。")
        st.stop()

    @staticmethod
    def current_label() -> str:
        name = str(st.session_state.get("admin_name", "")).strip() or "Admin"
        ns = str(st.session_state.get("admin_namespace", "")).strip() or "default"
        return f"{name}（namespace: {ns}）"

    @staticmethod
    def current_name() -> str:
        return str(st.session_state.get("admin_name", "")).strip() or "Admin"

    @staticmethod
    def current_namespace() -> str:
        return str(st.session_state.get("admin_namespace", "")).strip() or "default"


# =========================================================
# EXTERNAL SERVICE
# =========================================================
class ExternalService:
    @staticmethod
    def get_line_token(ns: str) -> str:
        line = st.secrets.get("line", {}) or {}
        tokens = line.get("tokens")
        if tokens:
            tok = str(tokens.get(ns, "")).strip()
            if tok:
                return tok
        legacy = str(line.get("channel_access_token", "")).strip()
        if legacy:
            return legacy
        st.error("LINEトークンが未設定です。")
        st.stop()

    @staticmethod
    def send_line_push(token: str, user_id: str, text: str, image_url: Optional[str] = None) -> int:
        if not user_id:
            return 400

        url = "https://api.line.me/v2/bot/message/push"
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
        messages = [{"type": "text", "text": text}]
        if image_url:
            messages.append({"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url})

        try:
            r = requests.post(url, headers=headers, data=json.dumps({"to": str(user_id), "messages": messages}), timeout=25)
            return r.status_code
        except Exception:
            return 500

    @staticmethod
    def upload_imgbb(file_bytes: bytes) -> Optional[str]:
        try:
            key = st.secrets["imgbb"]["api_key"]
        except Exception:
            return None

        try:
            res = requests.post("https://api.imgbb.com/1/upload", params={"key": key}, files={"image": file_bytes}, timeout=30)
            return res.json()["data"]["url"]
        except Exception:
            return None

    @staticmethod
    def ocr_space_extract_text_with_crop(
        file_bytes: bytes,
        crop_left_ratio: float,
        crop_top_ratio: float,
        crop_right_ratio: float,
        crop_bottom_ratio: float,
    ) -> str:
        try:
            api_key = st.secrets["ocrspace"]["api_key"]
        except Exception:
            return ""

        texts: List[str] = []

        try:
            cropped_bytes = U.crop_image_by_ratio(
                file_bytes=file_bytes,
                left_ratio=crop_left_ratio,
                top_ratio=crop_top_ratio,
                right_ratio=crop_right_ratio,
                bottom_ratio=crop_bottom_ratio,
            )

            processed_list = U.preprocess_ocr_image(cropped_bytes)
            targets = [("cropped.png", cropped_bytes)] + [(f"processed_{i}.png", b) for i, b in enumerate(processed_list, start=1)]

            for target_name, target_bytes in targets:
                for engine in (2, 1):
                    try:
                        res = requests.post(
                            "https://api.ocr.space/parse/image",
                            files={"filename": (target_name, target_bytes)},
                            data={
                                "apikey": api_key,
                                "language": "eng",
                                "isOverlayRequired": False,
                                "OCREngine": engine,
                                "scale": True,
                                "detectOrientation": True,
                                "isTable": False,
                            },
                            timeout=60,
                        )
                        data = res.json()
                        for p in data.get("ParsedResults", []):
                            txt = str(p.get("ParsedText", "")).strip()
                            if txt:
                                texts.append(txt)
                    except Exception:
                        continue

            uniq, seen = [], set()
            for t in texts:
                key = t.strip()
                if key and key not in seen:
                    seen.add(key)
                    uniq.append(key)

            return "\n\n".join(uniq)

        except Exception:
            return ""


# =========================================================
# GSHEET SERVICE
# =========================================================
@dataclass
class SheetNames:
    SETTINGS: str
    MEMBERS: str
    LEDGER: str
    LINEUSERS: str
    APR_SUMMARY: str
    SMARTVAULT_HISTORY: str


class GSheetService:
    def __init__(self, spreadsheet_id: str, namespace: str):
        self.spreadsheet_id = spreadsheet_id
        self.namespace = namespace
        self.names = SheetNames(
            SETTINGS=U.sheet_name(AppConfig.SHEET["SETTINGS"], namespace),
            MEMBERS=U.sheet_name(AppConfig.SHEET["MEMBERS"], namespace),
            LEDGER=U.sheet_name(AppConfig.SHEET["LEDGER"], namespace),
            LINEUSERS=U.sheet_name(AppConfig.SHEET["LINEUSERS"], namespace),
            APR_SUMMARY=U.sheet_name(AppConfig.SHEET["APR_SUMMARY"], namespace),
            SMARTVAULT_HISTORY=U.sheet_name(AppConfig.SHEET["SMARTVAULT_HISTORY"], namespace),
        )

        con = st.secrets.get("connections", {}).get("gsheets", {})
        creds_info = con.get("credentials")
        if not creds_info:
            st.error("Secrets に [connections.gsheets.credentials] がありません。")
            st.stop()

        creds = Credentials.from_service_account_info(
            dict(creds_info),
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
        self.gc = gspread.authorize(creds)
        self.book = self.gc.open_by_key(self.spreadsheet_id)

        ensure_key = (
            f"_sheet_ensured_{self.names.SETTINGS}_{self.names.MEMBERS}_{self.names.LEDGER}_"
            f"{self.names.LINEUSERS}_{self.names.APR_SUMMARY}_{self.names.SMARTVAULT_HISTORY}"
        )
        if not st.session_state.get(ensure_key, False):
            for key in AppConfig.HEADERS:
                self.ensure_sheet(key)
            st.session_state[ensure_key] = True

    def actual_name(self, key: str) -> str:
        return getattr(self.names, key)

    def ws(self, key_or_name: str):
        name = self.actual_name(key_or_name) if hasattr(self.names, key_or_name) else key_or_name
        return self.book.worksheet(name)

    def spreadsheet_url(self) -> str:
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"

    def ensure_sheet(self, key: str) -> None:
        name = self.actual_name(key)
        headers = AppConfig.HEADERS[key]
        try:
            ws = self.ws(key)
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

    @st.cache_data(ttl=600)
    def load_df(_self, key: str) -> pd.DataFrame:
        try:
            values = _self.ws(key).get_all_values()
        except APIError as e:
            raise RuntimeError(f"Google Sheets 読み取りエラー: {_self.actual_name(key)} を取得できません。") from e
        except Exception as e:
            raise RuntimeError(f"{_self.actual_name(key)} の読み取り中にエラーが発生しました: {e}") from e

        if not values:
            return pd.DataFrame()

        return U.clean_cols(pd.DataFrame(values[1:], columns=values[0]))

    def write_df(self, key: str, df: pd.DataFrame) -> None:
        ws = self.ws(key)
        out = df.fillna("").astype(str)
        ws.clear()
        ws.update([out.columns.tolist()] + out.values.tolist(), value_input_option="USER_ENTERED")

    def append_row(self, key: str, row: List[Any]) -> None:
        try:
            self.ws(key).append_row([("" if x is None else x) for x in row], value_input_option="USER_ENTERED")
        except Exception as e:
            raise RuntimeError(f"{self.actual_name(key)} への追記に失敗しました: {e}")

    def overwrite_rows(self, key: str, rows: List[List[Any]]) -> None:
        ws = self.ws(key)
        ws.clear()
        ws.update(rows, value_input_option="USER_ENTERED")

    def clear_cache(self) -> None:
        st.cache_data.clear()


# =========================================================
# REPOSITORY
# =========================================================
class Repository:
    def __init__(self, gs: GSheetService):
        self.gs = gs

    def _ensure_setting_defaults(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for k, v in AppConfig.OCR_DEFAULTS_PC.items():
            if k not in out.columns:
                out[k] = v
            else:
                out[k] = out[k].replace("", v)
        for k, v in AppConfig.OCR_DEFAULTS_MOBILE.items():
            if k not in out.columns:
                out[k] = v
            else:
                out[k] = out[k].replace("", v)
        return out

    def load_settings(self) -> pd.DataFrame:
        try:
            df = self.gs.load_df("SETTINGS")
        except Exception as e:
            st.error(str(e))
            return pd.DataFrame(columns=AppConfig.HEADERS["SETTINGS"])

        if df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["SETTINGS"])

        for c in AppConfig.HEADERS["SETTINGS"]:
            if c not in df.columns:
                df[c] = ""

        df = df[AppConfig.HEADERS["SETTINGS"]].copy()
        df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
        df = df[df["Project_Name"] != ""].copy()
        df["Net_Factor"] = U.to_num_series(df["Net_Factor"], AppConfig.FACTOR["MASTER"])
        df.loc[df["Net_Factor"] <= 0, "Net_Factor"] = AppConfig.FACTOR["MASTER"]
        df["IsCompound"] = U.truthy_series(df["IsCompound"])
        df["Compound_Timing"] = df["Compound_Timing"].apply(U.normalize_compound)
        df["Active"] = df["Active"].apply(lambda x: U.truthy(x) if str(x).strip() else True)
        df["UpdatedAt_JST"] = df["UpdatedAt_JST"].astype(str).str.strip()

        for k, v in AppConfig.OCR_DEFAULTS_PC.items():
            df[k] = df[k].apply(lambda x, default=v: U.to_ratio(x, default))
        for k, v in AppConfig.OCR_DEFAULTS_MOBILE.items():
            df[k] = df[k].apply(lambda x, default=v: U.to_ratio(x, default))

        personal_df = df[df["Project_Name"].str.upper() == AppConfig.PROJECT["PERSONAL"]].tail(1).copy()
        other_df = df[df["Project_Name"].str.upper() != AppConfig.PROJECT["PERSONAL"]].drop_duplicates(subset=["Project_Name"], keep="last")
        out = pd.concat([personal_df, other_df], ignore_index=True)

        if AppConfig.PROJECT["PERSONAL"] not in out["Project_Name"].astype(str).tolist():
            out = pd.concat(
                [
                    pd.DataFrame(
                        [
                            {
                                "Project_Name": AppConfig.PROJECT["PERSONAL"],
                                "Net_Factor": AppConfig.FACTOR["MASTER"],
                                "IsCompound": True,
                                "Compound_Timing": AppConfig.COMPOUND["DAILY"],
                                "Crop_Left_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"],
                                "Crop_Top_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"],
                                "Crop_Right_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"],
                                "Crop_Bottom_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"],
                                "Crop_Left_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"],
                                "Crop_Top_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"],
                                "Crop_Right_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"],
                                "Crop_Bottom_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"],
                                "UpdatedAt_JST": U.fmt_dt(U.now_jst()),
                                "Active": True,
                            }
                        ]
                    ),
                    out,
                ],
                ignore_index=True,
            )

        return self._ensure_setting_defaults(out)

    def write_settings(self, df: pd.DataFrame) -> None:
        out = df.copy()
        for c in AppConfig.HEADERS["SETTINGS"]:
            if c not in out.columns:
                out[c] = ""
        out = out[AppConfig.HEADERS["SETTINGS"]].copy()
        out["Project_Name"] = out["Project_Name"].astype(str).str.strip()
        out = out[out["Project_Name"] != ""].copy()
        out["Net_Factor"] = U.to_num_series(out["Net_Factor"], AppConfig.FACTOR["MASTER"]).map(lambda x: f"{float(x):.2f}")
        out["IsCompound"] = out["IsCompound"].apply(lambda x: "TRUE" if U.truthy(x) else "FALSE")
        out["Compound_Timing"] = out["Compound_Timing"].apply(U.normalize_compound)

        for k, v in AppConfig.OCR_DEFAULTS_PC.items():
            out[k] = out[k].apply(lambda x, default=v: f"{U.to_ratio(x, default):.3f}")
        for k, v in AppConfig.OCR_DEFAULTS_MOBILE.items():
            out[k] = out[k].apply(lambda x, default=v: f"{U.to_ratio(x, default):.3f}")

        out["Active"] = out["Active"].apply(lambda x: "TRUE" if U.truthy(x) else "FALSE")
        out["UpdatedAt_JST"] = out["UpdatedAt_JST"].astype(str)
        self.gs.write_df("SETTINGS", out)

    def repair_settings(self, settings_df: pd.DataFrame) -> pd.DataFrame:
        repaired = settings_df.copy()
        before_count = len(repaired)

        if repaired.empty:
            repaired = pd.DataFrame(columns=AppConfig.HEADERS["SETTINGS"])

        for c in AppConfig.HEADERS["SETTINGS"]:
            if c not in repaired.columns:
                repaired[c] = ""

        repaired = self._ensure_setting_defaults(repaired)
        repaired["Project_Name"] = repaired["Project_Name"].astype(str).str.strip()
        repaired = repaired[repaired["Project_Name"] != ""].copy()

        personal_df = repaired[repaired["Project_Name"].str.upper() == AppConfig.PROJECT["PERSONAL"]].tail(1).copy()
        other_df = repaired[repaired["Project_Name"].str.upper() != AppConfig.PROJECT["PERSONAL"]].drop_duplicates(subset=["Project_Name"], keep="last")
        repaired = pd.concat([personal_df, other_df], ignore_index=True)

        repaired["Net_Factor"] = U.to_num_series(repaired["Net_Factor"], AppConfig.FACTOR["MASTER"])
        repaired.loc[repaired["Net_Factor"] <= 0, "Net_Factor"] = AppConfig.FACTOR["MASTER"]
        repaired["IsCompound"] = repaired["IsCompound"].apply(U.truthy)
        repaired["Compound_Timing"] = repaired["Compound_Timing"].apply(U.normalize_compound)
        repaired["Active"] = repaired["Active"].apply(lambda x: U.truthy(x) if str(x).strip() else True)
        repaired["UpdatedAt_JST"] = repaired["UpdatedAt_JST"].astype(str) if "UpdatedAt_JST" in repaired.columns else ""

        for k, v in AppConfig.OCR_DEFAULTS_PC.items():
            repaired[k] = repaired[k].apply(lambda x, default=v: U.to_ratio(x, default))
        for k, v in AppConfig.OCR_DEFAULTS_MOBILE.items():
            repaired[k] = repaired[k].apply(lambda x, default=v: U.to_ratio(x, default))

        if AppConfig.PROJECT["PERSONAL"] not in repaired["Project_Name"].astype(str).tolist():
            repaired = pd.concat(
                [
                    pd.DataFrame(
                        [
                            {
                                "Project_Name": AppConfig.PROJECT["PERSONAL"],
                                "Net_Factor": AppConfig.FACTOR["MASTER"],
                                "IsCompound": True,
                                "Compound_Timing": AppConfig.COMPOUND["DAILY"],
                                "Crop_Left_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"],
                                "Crop_Top_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"],
                                "Crop_Right_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"],
                                "Crop_Bottom_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"],
                                "Crop_Left_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"],
                                "Crop_Top_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"],
                                "Crop_Right_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"],
                                "Crop_Bottom_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"],
                                "UpdatedAt_JST": U.fmt_dt(U.now_jst()),
                                "Active": True,
                            }
                        ]
                    ),
                    repaired,
                ],
                ignore_index=True,
            )

        need_write = len(repaired) != before_count or settings_df.empty
        try:
            left = repaired[AppConfig.HEADERS["SETTINGS"]].astype(str).reset_index(drop=True)
            right = settings_df.reindex(columns=AppConfig.HEADERS["SETTINGS"]).astype(str).reset_index(drop=True)
            if not left.equals(right):
                need_write = True
        except Exception:
            need_write = True

        if need_write:
            self.write_settings(repaired)
            self.gs.clear_cache()
            repaired = self.load_settings()

        return repaired

    def load_members(self) -> pd.DataFrame:
        try:
            df = self.gs.load_df("MEMBERS")
        except Exception as e:
            st.error(str(e))
            return pd.DataFrame(columns=AppConfig.HEADERS["MEMBERS"])

        if df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["MEMBERS"])

        for c in AppConfig.HEADERS["MEMBERS"]:
            if c not in df.columns:
                df[c] = ""

        df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
        df["PersonName"] = df["PersonName"].astype(str).str.strip()
        df["Principal"] = U.to_num_series(df["Principal"])
        df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
        df["LINE_DisplayName"] = df["LINE_DisplayName"].astype(str).str.strip()
        df["Rank"] = df["Rank"].apply(U.normalize_rank)
        df["IsActive"] = df["IsActive"].apply(U.truthy)
        return df

    def write_members(self, members_df: pd.DataFrame) -> None:
        out = members_df.copy()
        out["Principal"] = U.to_num_series(out["Principal"]).map(lambda x: f"{float(x):.6f}")
        out["IsActive"] = out["IsActive"].apply(lambda x: "TRUE" if U.truthy(x) else "FALSE")
        out["Rank"] = out["Rank"].apply(U.normalize_rank)
        self.gs.write_df("MEMBERS", out)

    def load_ledger(self) -> pd.DataFrame:
        try:
            df = self.gs.load_df("LEDGER")
        except Exception as e:
            st.error(str(e))
            return pd.DataFrame(columns=AppConfig.HEADERS["LEDGER"])

        if df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["LEDGER"])

        for c in AppConfig.HEADERS["LEDGER"]:
            if c not in df.columns:
                df[c] = ""
        df["Amount"] = U.to_num_series(df["Amount"])
        return df

    def load_line_users(self) -> pd.DataFrame:
        try:
            df = self.gs.load_df("LINEUSERS")
        except Exception as e:
            st.error(str(e))
            return pd.DataFrame(columns=AppConfig.HEADERS["LINEUSERS"])

        if df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["LINEUSERS"])

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

    def write_apr_summary(self, summary_df: pd.DataFrame) -> None:
        if summary_df.empty:
            return
        out = summary_df.copy()
        out["Date_JST"] = out["Date_JST"].astype(str)
        out["PersonName"] = out["PersonName"].astype(str)
        out["Total_APR"] = U.to_num_series(out["Total_APR"]).map(lambda x: f"{float(x):.6f}")
        out["APR_Count"] = U.to_num_series(out["APR_Count"]).astype(int).astype(str)
        out["Asset_Ratio"] = out["Asset_Ratio"].astype(str)
        out["LINE_DisplayName"] = out["LINE_DisplayName"].astype(str)
        self.gs.write_df("APR_SUMMARY", out)

    def append_ledger(
        self,
        dt_jst: str,
        project: str,
        person_name: str,
        typ: str,
        amount: float,
        note: str,
        evidence_url: str = "",
        line_user_id: str = "",
        line_display_name: str = "",
        source: str = AppConfig.SOURCE["APP"],
    ) -> None:
        if not str(project).strip():
            raise ValueError("project が空です")
        if not str(person_name).strip():
            raise ValueError("person_name が空です")
        if not str(typ).strip():
            raise ValueError("typ が空です")
        self.gs.append_row(
            "LEDGER",
            [dt_jst, project, person_name, typ, float(amount), note, evidence_url or "", line_user_id or "", line_display_name or "", source],
        )

    def append_smartvault_history(
        self,
        dt_jst: str,
        project: str,
        liquidity: float,
        yesterday_profit: float,
        apr: float,
        source_mode: str,
        ocr_liquidity: Optional[float],
        ocr_yesterday_profit: Optional[float],
        ocr_apr: Optional[float],
        evidence_url: str,
        admin_name: str,
        admin_namespace: str,
        note: str = "",
    ) -> None:
        self.gs.append_row(
            "SMARTVAULT_HISTORY",
            [
                dt_jst,
                project,
                float(liquidity),
                float(yesterday_profit),
                float(apr),
                str(source_mode),
                "" if ocr_liquidity is None else float(ocr_liquidity),
                "" if ocr_yesterday_profit is None else float(ocr_yesterday_profit),
                "" if ocr_apr is None else float(ocr_apr),
                evidence_url or "",
                admin_name or "",
                admin_namespace or "",
                note or "",
            ],
        )

    def active_projects(self, settings_df: pd.DataFrame) -> List[str]:
        if settings_df.empty:
            return []
        return settings_df.loc[settings_df["Active"] == True, "Project_Name"].dropna().astype(str).unique().tolist()

    def project_members_active(self, members_df: pd.DataFrame, project: str) -> pd.DataFrame:
        if members_df.empty:
            return members_df.copy()
        return members_df[(members_df["Project_Name"] == str(project)) & (members_df["IsActive"] == True)].copy().reset_index(drop=True)

    def validate_no_dup_lineid(self, members_df: pd.DataFrame, project: str) -> Optional[str]:
        if members_df.empty:
            return None
        df = members_df[members_df["Project_Name"] == str(project)].copy()
        df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
        df = df[df["Line_User_ID"] != ""]
        dup = df[df.duplicated(subset=["Line_User_ID"], keep=False)]
        return None if dup.empty else f"同一プロジェクト内で Line_User_ID が重複しています: {dup['Line_User_ID'].unique().tolist()}"

    def existing_apr_keys_for_date(self, date_jst: str) -> Set[Tuple[str, str]]:
        ledger_df = self.load_ledger()
        if ledger_df.empty:
            return set()
        df = ledger_df[
            (ledger_df["Type"].astype(str).str.strip() == AppConfig.TYPE["APR"])
            & (ledger_df["Datetime_JST"].astype(str).str.startswith(date_jst))
        ].copy()
        if df.empty:
            return set()
        return set(zip(df["Project_Name"].astype(str).str.strip(), df["PersonName"].astype(str).str.strip()))

    def reset_today_apr_records(self, date_jst: str, project: str) -> Tuple[int, int]:
        ws = self.gs.ws("LEDGER")
        values = ws.get_all_values()
        if not values:
            return 0, 0

        headers = values[0]
        if len(values) == 1:
            return 0, 0

        need_cols = ["Datetime_JST", "Project_Name", "Type", "Note"]
        if any(c not in headers for c in need_cols):
            return 0, 0

        idx_dt = headers.index("Datetime_JST")
        idx_project = headers.index("Project_Name")
        idx_type = headers.index("Type")
        idx_note = headers.index("Note")
        kept_rows, deleted_apr, deleted_line = [headers], 0, 0

        for row in values[1:]:
            row = row + [""] * (len(headers) - len(row))
            dt_v = str(row[idx_dt]).strip()
            project_v = str(row[idx_project]).strip()
            type_v = str(row[idx_type]).strip()
            note_v = str(row[idx_note]).strip()

            is_today = dt_v.startswith(date_jst)
            is_project = project_v == str(project).strip()
            delete_apr = is_today and is_project and type_v == AppConfig.TYPE["APR"]
            delete_line = is_today and is_project and type_v == AppConfig.TYPE["LINE"] and AppConfig.APR_LINE_NOTE_KEYWORD in note_v

            if delete_apr:
                deleted_apr += 1
                continue
            if delete_line:
                deleted_line += 1
                continue
            kept_rows.append(row[: len(headers)])

        if deleted_apr > 0 or deleted_line > 0:
            self.gs.overwrite_rows("LEDGER", kept_rows)
            self.gs.clear_cache()

        return deleted_apr, deleted_line


# =========================================================
# FINANCE ENGINE
# =========================================================
class FinanceEngine:
    def calc_project_apr(self, mem: pd.DataFrame, apr_percent: float, project_net_factor: float, project_name: str) -> pd.DataFrame:
        out = mem.copy()
        if str(project_name).strip().upper() == AppConfig.PROJECT["PERSONAL"]:
            out["Factor"] = out["Rank"].map(U.rank_factor)
            out["DailyAPR"] = (out["Principal"] * (apr_percent / 100.0) * out["Factor"]) / 365.0
            out["CalcMode"] = "PERSONAL"
            return out

        total_principal = float(out["Principal"].sum())
        count = len(out)
        factor = float(project_net_factor if project_net_factor > 0 else AppConfig.FACTOR["MASTER"])
        total_group_reward = (total_principal * (apr_percent / 100.0) * factor) / 365.0
        out["Factor"] = factor
        out["DailyAPR"] = (total_group_reward / count) if count > 0 else 0.0
        out["CalcMode"] = "GROUP_EQUAL"
        return out

    def build_apr_summary(self, ledger_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
        if ledger_df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["APR_SUMMARY"])

        apr_df = ledger_df[ledger_df["Type"].astype(str).str.strip() == AppConfig.TYPE["APR"]].copy()
        if apr_df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["APR_SUMMARY"])

        apr_df["PersonName"] = apr_df["PersonName"].astype(str).str.strip()
        apr_df["LINE_DisplayName"] = apr_df["LINE_DisplayName"].astype(str).str.strip()
        apr_df["Amount"] = U.to_num_series(apr_df["Amount"])

        active_mem = members_df[members_df["IsActive"] == True].copy() if not members_df.empty and "IsActive" in members_df.columns else members_df.copy()
        total_assets = float(active_mem["Principal"].sum()) if not active_mem.empty else 0.0

        summary = apr_df.groupby("PersonName", as_index=False).agg(Total_APR=("Amount", "sum"), APR_Count=("Amount", "count"))
        disp_map = apr_df.sort_values("Datetime_JST", ascending=False).drop_duplicates(subset=["PersonName"])[["PersonName", "LINE_DisplayName"]].copy()
        summary = summary.merge(disp_map, on="PersonName", how="left")
        summary["Date_JST"] = U.fmt_date(U.now_jst())
        summary["Asset_Ratio"] = summary["Total_APR"].map(lambda x: f"{(float(x) / total_assets) * 100:.2f}%" if total_assets > 0 else "0.00%")
        return summary[["Date_JST", "PersonName", "Total_APR", "APR_Count", "Asset_Ratio", "LINE_DisplayName"]].copy()

    def apply_monthly_compound(self, repo: Repository, members_df: pd.DataFrame, project: str) -> Tuple[int, float]:
        ledger_df = repo.load_ledger()
        if ledger_df.empty:
            return 0, 0.0

        target = ledger_df[
            (ledger_df["Project_Name"].astype(str).str.strip() == str(project).strip())
            & (ledger_df["Type"].astype(str).str.strip() == AppConfig.TYPE["APR"])
            & (~ledger_df["Note"].astype(str).str.contains("COMPOUNDED", na=False))
        ].copy()
        if target.empty:
            return 0, 0.0

        sums = target.groupby("PersonName", as_index=False)["Amount"].sum()
        if sums.empty:
            return 0, 0.0

        ts = U.fmt_dt(U.now_jst())
        updated_count, total_added = 0, 0.0
        add_map = dict(zip(sums["PersonName"].astype(str).str.strip(), U.to_num_series(sums["Amount"])))
        mask = (members_df["Project_Name"].astype(str).str.strip() == str(project).strip()) & (
            members_df["PersonName"].astype(str).str.strip().isin(add_map.keys())
        )

        if mask.any():
            for idx in members_df[mask].index.tolist():
                person = str(members_df.loc[idx, "PersonName"]).strip()
                addv = float(add_map.get(person, 0.0))
                if addv == 0:
                    continue
                members_df.loc[idx, "Principal"] = float(members_df.loc[idx, "Principal"]) + addv
                members_df.loc[idx, "UpdatedAt_JST"] = ts
                updated_count += 1
                total_added += addv

        if updated_count > 0:
            repo.write_members(members_df)
            ws = repo.gs.ws("LEDGER")
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
                        if r_project == str(project).strip() and r_type == AppConfig.TYPE["APR"] and "COMPOUNDED" not in r_note:
                            ws.update_cell(row_no, note_idx, (r_note + " | " if r_note else "") + f"COMPOUNDED:{ts}")
            repo.gs.clear_cache()

        return updated_count, total_added


# =========================================================
# DATA STORE
# =========================================================
class DataStore:
    def __init__(self, repo: Repository, engine: FinanceEngine):
        self.repo = repo
        self.engine = engine

    def clear(self) -> None:
        for key in AppConfig.SESSION_KEYS.values():
            if key in st.session_state:
                del st.session_state[key]

    def load(self, force: bool = False) -> Dict[str, pd.DataFrame]:
        if force or AppConfig.SESSION_KEYS["SETTINGS"] not in st.session_state:
            st.session_state[AppConfig.SESSION_KEYS["SETTINGS"]] = self.repo.repair_settings(self.repo.load_settings())
        if force or AppConfig.SESSION_KEYS["MEMBERS"] not in st.session_state:
            st.session_state[AppConfig.SESSION_KEYS["MEMBERS"]] = self.repo.load_members()
        if force or AppConfig.SESSION_KEYS["LEDGER"] not in st.session_state:
            st.session_state[AppConfig.SESSION_KEYS["LEDGER"]] = self.repo.load_ledger()
        if force or AppConfig.SESSION_KEYS["LINEUSERS"] not in st.session_state:
            st.session_state[AppConfig.SESSION_KEYS["LINEUSERS"]] = self.repo.load_line_users()

        settings_df = st.session_state[AppConfig.SESSION_KEYS["SETTINGS"]]
        members_df = st.session_state[AppConfig.SESSION_KEYS["MEMBERS"]]
        ledger_df = st.session_state[AppConfig.SESSION_KEYS["LEDGER"]]
        line_users_df = st.session_state[AppConfig.SESSION_KEYS["LINEUSERS"]]
        apr_summary_df = self.engine.build_apr_summary(ledger_df, members_df)
        st.session_state[AppConfig.SESSION_KEYS["APR_SUMMARY"]] = apr_summary_df

        return {
            "settings_df": settings_df,
            "members_df": members_df,
            "ledger_df": ledger_df,
            "line_users_df": line_users_df,
            "apr_summary_df": apr_summary_df,
        }

    def refresh(self) -> Dict[str, pd.DataFrame]:
        self.repo.gs.clear_cache()
        self.clear()
        return self.load(force=True)

    def persist_and_refresh(self) -> Dict[str, pd.DataFrame]:
        data = self.refresh()
        self.repo.write_apr_summary(data["apr_summary_df"])
        return self.refresh()


# =========================================================
# UI
# =========================================================
class AppUI:
    def __init__(self, repo: Repository, engine: FinanceEngine, store: DataStore):
        self.repo = repo
        self.engine = engine
        self.store = store

    def _ocr_crop_text(self, file_bytes: bytes, box: Dict[str, float]) -> str:
        return ExternalService.ocr_space_extract_text_with_crop(
            file_bytes=file_bytes,
            crop_left_ratio=box["left"],
            crop_top_ratio=box["top"],
            crop_right_ratio=box["right"],
            crop_bottom_ratio=box["bottom"],
        )

    def _ocr_smartvault_mobile_metrics(self, file_bytes: bytes) -> Dict[str, Any]:
        boxes = AppConfig.SMARTVAULT_BOXES_MOBILE

        total_text = self._ocr_crop_text(file_bytes, boxes["TOTAL_LIQUIDITY"])
        profit_text = self._ocr_crop_text(file_bytes, boxes["YESTERDAY_PROFIT"])
        apr_text = self._ocr_crop_text(file_bytes, boxes["APR"])

        total_vals = U.extract_usd_candidates(total_text)
        profit_vals = U.extract_usd_candidates(profit_text)
        apr_vals = U.extract_percent_candidates(apr_text)

        total_liquidity = U.pick_total_liquidity(total_vals)
        yesterday_profit = U.pick_yesterday_profit(profit_vals)
        apr_value = apr_vals[0] if apr_vals else None

        boxed_preview = U.draw_ocr_boxes(file_bytes, boxes)

        return {
            "boxes": boxes,
            "total_text": total_text,
            "profit_text": profit_text,
            "apr_text": apr_text,
            "total_vals": total_vals,
            "profit_vals": profit_vals,
            "apr_vals": apr_vals,
            "total_liquidity": total_liquidity,
            "yesterday_profit": yesterday_profit,
            "apr_value": apr_value,
            "boxed_preview": boxed_preview,
        }

    def render_dashboard(self, members_df: pd.DataFrame, ledger_df: pd.DataFrame, apr_summary_df: pd.DataFrame) -> None:
        st.subheader("📊 管理画面ダッシュボード")
        st.caption("総資産 / 本日APR / グループ別残高 / 個人残高 / 個人別累計APR / LINE通知履歴")

        active_mem = members_df[members_df["IsActive"] == True].copy() if not members_df.empty else members_df.copy()
        total_assets = float(active_mem["Principal"].sum()) if not active_mem.empty else 0.0

        today_prefix, today_apr = U.fmt_date(U.now_jst()), 0.0
        if not ledger_df.empty and "Datetime_JST" in ledger_df.columns:
            today_rows = ledger_df[ledger_df["Datetime_JST"].astype(str).str.startswith(today_prefix)].copy()
            today_apr = float(today_rows[today_rows["Type"].astype(str).str.strip() == AppConfig.TYPE["APR"]]["Amount"].sum())

        c1, c2 = st.columns(2)
        c1.metric("総資産", U.fmt_usd(total_assets))
        c2.metric("本日APR", U.fmt_usd(today_apr))

        st.divider()
        c3, c4 = st.columns(2)

        with c3:
            st.markdown("#### グループ別残高")
            group_df = active_mem[active_mem["Project_Name"].astype(str).str.upper() != AppConfig.PROJECT["PERSONAL"]].copy() if not active_mem.empty else pd.DataFrame()
            if group_df.empty:
                st.info("グループデータがありません。")
            else:
                group_summary = group_df.groupby("Project_Name", as_index=False).agg(人数=("PersonName", "count"), 総残高=("Principal", "sum")).sort_values("総残高", ascending=False)
                group_summary["総残高"] = group_summary["総残高"].apply(U.fmt_usd)
                st.dataframe(group_summary, use_container_width=True, hide_index=True)

        with c4:
            st.markdown("#### 個人残高")
            personal_df = active_mem[active_mem["Project_Name"].astype(str).str.upper() == AppConfig.PROJECT["PERSONAL"]].copy() if not active_mem.empty else pd.DataFrame()
            if personal_df.empty:
                st.info("PERSONAL データがありません。")
            else:
                p = personal_df[["PersonName", "Principal", "LINE_DisplayName"]].copy()
                p["資産割合"] = p["Principal"].map(lambda x: f"{(float(x) / total_assets) * 100:.2f}%" if total_assets > 0 else "0.00%")
                p["Principal_num"] = p["Principal"].astype(float)
                p["Principal"] = p["Principal"].apply(U.fmt_usd)
                p = p.sort_values("Principal_num", ascending=False)[["PersonName", "Principal", "資産割合", "LINE_DisplayName"]]
                st.dataframe(p, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### 個人別 累計APR")
        if apr_summary_df.empty:
            st.info("APR履歴がありません。")
        else:
            view = apr_summary_df.copy()
            view["Total_APR_num"] = U.to_num_series(view["Total_APR"])
            view["Total_APR"] = view["Total_APR_num"].apply(U.fmt_usd)
            view = view.sort_values("Total_APR_num", ascending=False)[["PersonName", "Total_APR", "APR_Count", "Asset_Ratio", "LINE_DisplayName"]]
            view = view.rename(columns={"Total_APR": "累計APR", "APR_Count": "件数", "Asset_Ratio": "総資産比"})
            st.dataframe(view, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### LINE通知履歴")
        c_hist1, c_hist2 = st.columns([1, 1])
        with c_hist1:
            if st.button("LINE送信履歴をリセット表示", use_container_width=True):
                st.session_state["hide_line_history"] = True
                st.rerun()
        with c_hist2:
            if st.button("LINE送信履歴を再表示", use_container_width=True):
                st.session_state["hide_line_history"] = False
                st.rerun()

        if st.session_state.get("hide_line_history", False):
            st.info("LINE通知履歴はリセット表示中です。シートの記録は削除していません。")
        else:
            if ledger_df.empty:
                st.info("通知履歴がありません。")
            else:
                line_hist = ledger_df[ledger_df["Type"].astype(str).str.strip() == AppConfig.TYPE["LINE"]].copy()
                if line_hist.empty:
                    st.info("LINE通知履歴はまだありません。")
                else:
                    cols = [c for c in ["Datetime_JST", "Project_Name", "PersonName", "Type", "Line_User_ID", "LINE_DisplayName", "Note", "Source"] if c in line_hist.columns]
                    st.dataframe(line_hist.sort_values("Datetime_JST", ascending=False)[cols].head(100), use_container_width=True, hide_index=True)

    def render_apr(self, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> None:
        st.subheader("📈 APR 確定")
        st.caption(f"{AppConfig.RANK_LABEL} / PERSONAL=個別計算 / GROUP=総額均等割 / 管理者: {AdminAuth.current_label()}")

        projects = self.repo.active_projects(settings_df)
        if not projects:
            st.warning("有効（Active=TRUE）のプロジェクトがありません。")
            return

        project = st.selectbox("基準プロジェクト", projects)
        send_scope = st.radio("送信対象", ["選択中プロジェクトのみ", "全有効プロジェクト"], horizontal=True)

        st.markdown("#### 流動性 / 昨日の収益 / APR（別取得・手動設定可）")
        c1, c2, c3 = st.columns(3)
        with c1:
            total_liquidity_raw = st.text_input(
                "流動性（手動設定可）",
                value=st.session_state.get("sv_total_liquidity", ""),
                key="sv_total_liquidity",
                placeholder="$78,354.35",
            )
        with c2:
            yesterday_profit_raw = st.text_input(
                "昨日の収益（手動設定可）",
                value=st.session_state.get("sv_yesterday_profit", ""),
                key="sv_yesterday_profit",
                placeholder="$90.87",
            )
        with c3:
            apr_raw = st.text_input(
                "APR（%・手動設定可）",
                value=st.session_state.get("sv_apr", ""),
                key="sv_apr",
                placeholder="42.33",
            )

        total_liquidity = U.to_f(total_liquidity_raw)
        yesterday_profit = U.to_f(yesterday_profit_raw)
        apr = U.apr_val(apr_raw)

        ocr_liquidity = st.session_state.get("ocr_total_liquidity")
        ocr_yesterday_profit = st.session_state.get("ocr_yesterday_profit")
        ocr_apr = st.session_state.get("ocr_apr")

        st.info(
            f"流動性 = {U.fmt_usd(total_liquidity)} / "
            f"昨日の収益 = {U.fmt_usd(yesterday_profit)} / "
            f"最終APR = {apr:.4f}%"
        )

        uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"], key="apr_img")

        if uploaded is not None and st.button("OCRで別取得"):
            file_bytes = uploaded.getvalue()

            crop_left_ratio = AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"]
            crop_top_ratio = AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"]
            crop_right_ratio = AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"]
            crop_bottom_ratio = AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"]

            try:
                srow = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]

                if U.is_mobile_tall_image(file_bytes):
                    crop_left_ratio = U.to_ratio(
                        srow.get("Crop_Left_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"]),
                        AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"],
                    )
                    crop_top_ratio = U.to_ratio(
                        srow.get("Crop_Top_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"]),
                        AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"],
                    )
                    crop_right_ratio = U.to_ratio(
                        srow.get("Crop_Right_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"]),
                        AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"],
                    )
                    crop_bottom_ratio = U.to_ratio(
                        srow.get("Crop_Bottom_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"]),
                        AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"],
                    )
                else:
                    crop_left_ratio = U.to_ratio(
                        srow.get("Crop_Left_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"]),
                        AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"],
                    )
                    crop_top_ratio = U.to_ratio(
                        srow.get("Crop_Top_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"]),
                        AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"],
                    )
                    crop_right_ratio = U.to_ratio(
                        srow.get("Crop_Right_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"]),
                        AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"],
                    )
                    crop_bottom_ratio = U.to_ratio(
                        srow.get("Crop_Bottom_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"]),
                        AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"],
                    )
            except Exception:
                pass

            raw_text = ExternalService.ocr_space_extract_text_with_crop(
                file_bytes=file_bytes,
                crop_left_ratio=crop_left_ratio,
                crop_top_ratio=crop_top_ratio,
                crop_right_ratio=crop_right_ratio,
                crop_bottom_ratio=crop_bottom_ratio,
            )

            if raw_text:
                with st.expander("OCR生テキスト（通常範囲）", expanded=False):
                    st.text(raw_text)

            st.info(
                f"OCR切り抜き範囲: left={crop_left_ratio:.3f}, top={crop_top_ratio:.3f}, right={crop_right_ratio:.3f}, bottom={crop_bottom_ratio:.3f}"
            )

            if U.is_mobile_tall_image(file_bytes):
                smart = self._ocr_smartvault_mobile_metrics(file_bytes)

                st.markdown("#### SmartVaultモバイル専用OCR結果")
                st.image(smart["boxed_preview"], caption="赤枠 = OCR対象範囲", use_container_width=True)

                c_a, c_b, c_c = st.columns(3)
                with c_a:
                    if smart["total_liquidity"] is not None:
                        st.success(f"流動性: {U.fmt_usd(float(smart['total_liquidity']))}")
                    else:
                        st.warning("流動性: 未検出")

                with c_b:
                    if smart["yesterday_profit"] is not None:
                        st.success(f"昨日の収益: {U.fmt_usd(float(smart['yesterday_profit']))}")
                    else:
                        st.warning("昨日の収益: 未検出")

                with c_c:
                    if smart["apr_value"] is not None:
                        st.success(f"APR: {float(smart['apr_value']):.2f}%")
                    else:
                        st.warning("APR: 未検出")

                st.caption(
                    f"流動性範囲 left={smart['boxes']['TOTAL_LIQUIDITY']['left']:.2f}, top={smart['boxes']['TOTAL_LIQUIDITY']['top']:.2f}, right={smart['boxes']['TOTAL_LIQUIDITY']['right']:.2f}, bottom={smart['boxes']['TOTAL_LIQUIDITY']['bottom']:.2f}"
                )
                st.caption(
                    f"昨日の収益範囲 left={smart['boxes']['YESTERDAY_PROFIT']['left']:.2f}, top={smart['boxes']['YESTERDAY_PROFIT']['top']:.2f}, right={smart['boxes']['YESTERDAY_PROFIT']['right']:.2f}, bottom={smart['boxes']['YESTERDAY_PROFIT']['bottom']:.2f}"
                )
                st.caption(
                    f"APR範囲 left={smart['boxes']['APR']['left']:.2f}, top={smart['boxes']['APR']['top']:.2f}, right={smart['boxes']['APR']['right']:.2f}, bottom={smart['boxes']['APR']['bottom']:.2f}"
                )

                with st.expander("OCR生テキスト（流動性）", expanded=False):
                    st.text(smart["total_text"] or "")
                with st.expander("OCR生テキスト（昨日の収益）", expanded=False):
                    st.text(smart["profit_text"] or "")
                with st.expander("OCR生テキスト（APR）", expanded=False):
                    st.text(smart["apr_text"] or "")

                if smart["total_liquidity"] is not None:
                    st.session_state["sv_total_liquidity"] = f"{float(smart['total_liquidity']):,.2f}"
                    st.session_state["ocr_total_liquidity"] = float(smart["total_liquidity"])
                if smart["yesterday_profit"] is not None:
                    st.session_state["sv_yesterday_profit"] = f"{float(smart['yesterday_profit']):,.2f}"
                    st.session_state["ocr_yesterday_profit"] = float(smart["yesterday_profit"])
                if smart["apr_value"] is not None:
                    st.session_state["sv_apr"] = f"{float(smart['apr_value']):.4f}"
                    st.session_state["ocr_apr"] = float(smart["apr_value"])

                st.rerun()
            else:
                apr_candidates = U.extract_percent_candidates(raw_text)
                if apr_candidates:
                    best = apr_candidates[0]
                    st.success(f"通常OCRからAPR候補を検出: {best}%")
                    st.session_state["sv_apr"] = f"{float(best):.4f}"
                    st.session_state["ocr_apr"] = float(best)
                    st.rerun()
                else:
                    st.warning("APR候補は見つかりませんでした。")

        target_projects = projects if send_scope == "全有効プロジェクト" else [project]
        today_key = U.fmt_date(U.now_jst())
        existing_apr_keys = self.repo.existing_apr_keys_for_date(today_key)

        preview_rows: List[dict] = []
        total_members, total_principal, total_reward, skipped_members = 0, 0.0, 0.0, 0

        for p in target_projects:
            row = settings_df[settings_df["Project_Name"] == str(p)].iloc[0]
            project_net_factor = float(row.get("Net_Factor", AppConfig.FACTOR["MASTER"]))
            compound_timing = U.normalize_compound(row.get("Compound_Timing", AppConfig.COMPOUND["NONE"]))
            mem = self.repo.project_members_active(members_df, p)
            if mem.empty:
                continue

            mem_calc = self.engine.calc_project_apr(mem, float(apr), project_net_factor, p)
            for _, r in mem_calc.iterrows():
                person = str(r["PersonName"]).strip()
                is_done = (str(p).strip(), person) in existing_apr_keys
                if is_done:
                    skipped_members += 1
                else:
                    total_members += 1
                    total_principal += float(r["Principal"])
                    total_reward += float(r["DailyAPR"])

                preview_rows.append(
                    {
                        "Project_Name": p,
                        "PersonName": person,
                        "Rank": str(r["Rank"]).strip(),
                        "Compound_Timing": U.compound_label(compound_timing),
                        "Principal": U.fmt_usd(float(r["Principal"])),
                        "DailyAPR": U.fmt_usd(float(r["DailyAPR"])),
                        "Line_User_ID": str(r["Line_User_ID"]).strip(),
                        "LINE_DisplayName": str(r["LINE_DisplayName"]).strip(),
                        "流動性": U.fmt_usd(float(total_liquidity)),
                        "昨日の収益": U.fmt_usd(float(yesterday_profit)),
                        "APR": f"{apr:.4f}%",
                        "本日APR状態": "本日記録済み" if is_done else "未記録",
                    }
                )

        if total_members == 0 and skipped_members == 0:
            st.warning("送信対象に 🟢運用中 のメンバーがいません。")
            return

        st.markdown(f"送信対象プロジェクト数: {len(target_projects)} / 本日未記録の対象人数: {total_members} / 本日記録済み人数: {skipped_members}")

        apr_percent_display = (total_reward / total_principal * 100.0) if total_principal > 0 else 0.0

        csum1, csum2 = st.columns([1.2, 2.8])
        with csum1:
            if send_scope == "選択中プロジェクトのみ":
                if st.button("本日のAPR記録をリセット", key="reset_today_apr_top", use_container_width=True):
                    try:
                        deleted_apr, deleted_line = self.repo.reset_today_apr_records(today_key, project)
                        self.store.persist_and_refresh()
                        if deleted_apr == 0 and deleted_line == 0:
                            st.info("削除対象はありません。")
                        else:
                            st.success(f"本日分をリセットしました。APR削除:{deleted_apr}件 / LINE削除:{deleted_line}件")
                        st.rerun()
                    except Exception as e:
                        st.error(f"APRリセットでエラー: {e}")
                        st.stop()

        with csum2:
            st.markdown(
                f"""
**本日対象サマリー**  
流動性: **{U.fmt_usd(total_liquidity)}**　/　昨日の収益: **{U.fmt_usd(yesterday_profit)}**　/　最終APR: **{apr:.4f}%**  
総投資額: **{U.fmt_usd(total_principal)}**　/　APR合計: **{U.fmt_usd(total_reward)}**　/　実効APR: **{apr_percent_display:.4f}%**
"""
            )

        with st.expander("個人別の本日配当（確認）", expanded=False):
            st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)

        if st.button("APRを確定して対象全員にLINE送信"):
            try:
                if apr <= 0:
                    st.warning("APRが0以下です。")
                    return

                evidence_url = None
                if uploaded:
                    evidence_url = ExternalService.upload_imgbb(uploaded.getvalue())
                    if not evidence_url:
                        st.error("画像アップロードに失敗しました。")
                        return

                source_mode = U.detect_source_mode(
                    final_liquidity=float(total_liquidity),
                    final_profit=float(yesterday_profit),
                    final_apr=float(apr),
                    ocr_liquidity=st.session_state.get("ocr_total_liquidity"),
                    ocr_profit=st.session_state.get("ocr_yesterday_profit"),
                    ocr_apr=st.session_state.get("ocr_apr"),
                )

                ts = U.fmt_dt(U.now_jst())
                apr_ledger_count, line_log_count, success, fail, skip_count = 0, 0, 0, 0, 0
                existing_apr_keys = self.repo.existing_apr_keys_for_date(today_key)
                token = ExternalService.get_line_token(AdminAuth.current_namespace())
                daily_add_map: Dict[Tuple[str, str], float] = {}

                self.repo.append_smartvault_history(
                    dt_jst=ts,
                    project=project,
                    liquidity=float(total_liquidity),
                    yesterday_profit=float(yesterday_profit),
                    apr=float(apr),
                    source_mode=source_mode,
                    ocr_liquidity=st.session_state.get("ocr_total_liquidity"),
                    ocr_yesterday_profit=st.session_state.get("ocr_yesterday_profit"),
                    ocr_apr=st.session_state.get("ocr_apr"),
                    evidence_url=evidence_url or "",
                    admin_name=AdminAuth.current_name(),
                    admin_namespace=AdminAuth.current_namespace(),
                    note="APR確定時に保存",
                )

                for p in target_projects:
                    row = settings_df[settings_df["Project_Name"] == str(p)].iloc[0]
                    project_net_factor = float(row.get("Net_Factor", AppConfig.FACTOR["MASTER"]))
                    compound_timing = U.normalize_compound(row.get("Compound_Timing", AppConfig.COMPOUND["NONE"]))
                    mem = self.repo.project_members_active(members_df, p)
                    if mem.empty:
                        continue

                    mem_calc = self.engine.calc_project_apr(mem, float(apr), project_net_factor, p)
                    for _, r in mem_calc.iterrows():
                        person = str(r["PersonName"]).strip()
                        uid = str(r["Line_User_ID"]).strip()
                        disp = str(r["LINE_DisplayName"]).strip()
                        daily_apr = float(r["DailyAPR"])
                        current_principal = float(r["Principal"])
                        apr_key = (str(p).strip(), person)

                        if apr_key in existing_apr_keys:
                            skip_count += 1
                            continue

                        note = (
                            f"APR:{apr}%, "
                            f"Liquidity:{total_liquidity}, "
                            f"YesterdayProfit:{yesterday_profit}, "
                            f"SourceMode:{source_mode}, "
                            f"Mode:{r['CalcMode']}, Rank:{r['Rank']}, Factor:{r['Factor']}, CompoundTiming:{compound_timing}"
                        )
                        self.repo.append_ledger(ts, p, person, AppConfig.TYPE["APR"], daily_apr, note, evidence_url or "", uid, disp)
                        existing_apr_keys.add(apr_key)
                        apr_ledger_count += 1

                        if compound_timing == AppConfig.COMPOUND["DAILY"]:
                            daily_add_map[(str(p).strip(), person)] = daily_add_map.get((str(p).strip(), person), 0.0) + daily_apr
                            person_after_amount = current_principal + daily_apr
                        else:
                            person_after_amount = current_principal

                        personalized_msg = (
                            "🏦【APR収益報告】\n"
                            f"{person} 様\n"
                            f"報告日時: {U.now_jst().strftime('%Y/%m/%d %H:%M')}\n"
                            f"流動性: {U.fmt_usd(total_liquidity)}\n"
                            f"昨日の収益: {U.fmt_usd(yesterday_profit)}\n"
                            f"APR: {apr:.4f}%\n"
                            f"本日配当: {U.fmt_usd(daily_apr)}\n"
                            f"現在運用額: {U.fmt_usd(current_principal)}\n"
                            f"複利タイプ: {U.compound_label(compound_timing)}\n"
                        )

                        if compound_timing == AppConfig.COMPOUND["DAILY"]:
                            personalized_msg += f"複利反映後運用額: {U.fmt_usd(person_after_amount)}\n"

                        if not uid:
                            code, line_note = 0, "LINE未送信: Line_User_IDなし"
                        else:
                            code = ExternalService.send_line_push(token, uid, personalized_msg, evidence_url)
                            line_note = (
                                f"HTTP:{code}, "
                                f"Liquidity:{total_liquidity}, "
                                f"YesterdayProfit:{yesterday_profit}, "
                                f"APR:{apr}%, SourceMode:{source_mode}, CompoundTiming:{compound_timing}"
                            )

                        self.repo.append_ledger(ts, p, person, AppConfig.TYPE["LINE"], 0, line_note, evidence_url or "", uid, disp)
                        line_log_count += 1

                        if code == 200:
                            success += 1
                        else:
                            fail += 1

                if daily_add_map:
                    for i in range(len(members_df)):
                        p = str(members_df.loc[i, "Project_Name"]).strip()
                        pn = str(members_df.loc[i, "PersonName"]).strip()
                        addv = float(daily_add_map.get((p, pn), 0.0))
                        if addv != 0.0 and U.truthy(members_df.loc[i, "IsActive"]):
                            members_df.loc[i, "Principal"] = float(members_df.loc[i, "Principal"]) + addv
                            members_df.loc[i, "UpdatedAt_JST"] = ts
                    self.repo.write_members(members_df)

                self.store.persist_and_refresh()
                st.success(
                    f"APR記録:{apr_ledger_count}件 / LINE履歴記録:{line_log_count}件 / "
                    f"送信成功:{success} / 送信失敗:{fail} / 重複スキップ:{skip_count}件"
                )
                st.rerun()

            except Exception as e:
                st.error(f"APR確定処理でエラー: {e}")
                st.stop()

        if send_scope == "選択中プロジェクトのみ":
            row = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]
            compound_timing = U.normalize_compound(row.get("Compound_Timing", AppConfig.COMPOUND["NONE"]))
            if compound_timing == AppConfig.COMPOUND["MONTHLY"]:
                st.divider()
                st.markdown("#### 月次複利反映")
                if st.button("未反映APRを元本へ反映"):
                    try:
                        count, total_added = self.engine.apply_monthly_compound(self.repo, members_df, project)
                        self.store.persist_and_refresh()
                        if count == 0:
                            st.info("未反映のAPRはありません。")
                        else:
                            st.success(f"{count}名に反映しました。合計反映額: {U.fmt_usd(total_added)}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"月次複利反映でエラー: {e}")
                        st.stop()

    def render_cash(self, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> None:
        st.subheader("💸 入金 / 出金（個別LINE通知）")
        projects = self.repo.active_projects(settings_df)
        if not projects:
            st.warning("有効なプロジェクトがありません。")
            return

        project = st.selectbox("プロジェクト", projects, key="cash_project")
        mem = self.repo.project_members_active(members_df, project)
        if mem.empty:
            st.warning("このプロジェクトに 🟢運用中 のメンバーがいません。")
            return

        person = st.selectbox("メンバー", mem["PersonName"].tolist())
        row = mem[mem["PersonName"] == person].iloc[0]
        current = float(row["Principal"])

        typ = st.selectbox("種別", [AppConfig.TYPE["DEPOSIT"], AppConfig.TYPE["WITHDRAW"]])
        amt = st.number_input("金額", min_value=0.0, value=0.0, step=100.0)
        note = st.text_input("メモ（任意）", value="")
        uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"], key="cash_img")

        if st.button("確定して保存＆個別にLINE通知"):
            try:
                if amt <= 0:
                    st.warning("金額が0です。")
                    return
                if typ == AppConfig.TYPE["WITHDRAW"] and float(amt) > current:
                    st.error("出金額が現在残高を超えています。")
                    return

                evidence_url = ExternalService.upload_imgbb(uploaded.getvalue()) if uploaded else None
                if uploaded and not evidence_url:
                    st.error("画像アップロードに失敗しました。")
                    return

                new_balance = current + float(amt) if typ == AppConfig.TYPE["DEPOSIT"] else current - float(amt)
                ts = U.fmt_dt(U.now_jst())

                for i in range(len(members_df)):
                    if members_df.loc[i, "Project_Name"] == str(project) and str(members_df.loc[i, "PersonName"]).strip() == str(person).strip():
                        members_df.loc[i, "Principal"] = float(new_balance)
                        members_df.loc[i, "UpdatedAt_JST"] = ts

                self.repo.append_ledger(
                    ts,
                    project,
                    person,
                    typ,
                    float(amt),
                    note,
                    evidence_url or "",
                    str(row["Line_User_ID"]).strip(),
                    str(row["LINE_DisplayName"]).strip(),
                )
                self.repo.write_members(members_df)

                token = ExternalService.get_line_token(AdminAuth.current_namespace())
                uid = str(row["Line_User_ID"]).strip()
                msg = (
                    "💸【入出金通知】\n"
                    f"{person} 様\n"
                    f"日時: {U.now_jst().strftime('%Y/%m/%d %H:%M')}\n"
                    f"種別: {typ}\n"
                    f"金額: {U.fmt_usd(float(amt))}\n"
                    f"更新後残高: {U.fmt_usd(float(new_balance))}\n"
                )

                if uid:
                    code = ExternalService.send_line_push(token, uid, msg, evidence_url)
                    line_note = f"HTTP:{code}, Type:{typ}, Amount:{float(amt)}, NewBalance:{float(new_balance)}"
                else:
                    code, line_note = 0, "LINE未送信: Line_User_IDなし"

                self.repo.append_ledger(ts, project, person, AppConfig.TYPE["LINE"], 0, line_note, evidence_url or "", uid, str(row["LINE_DisplayName"]).strip())
                self.store.persist_and_refresh()

                if code == 200:
                    st.success("入出金保存＆LINE送信記録完了")
                else:
                    st.warning(f"入出金保存完了 / LINE送信または送信記録あり（HTTP {code}）")
                st.rerun()
            except Exception as e:
                st.error(f"入出金処理でエラー: {e}")
                st.stop()

    def render_admin(self, settings_df: pd.DataFrame, members_df: pd.DataFrame, line_users_df: pd.DataFrame) -> None:
        st.subheader("⚙️ 管理")

        projects = self.repo.active_projects(settings_df)
        if not projects:
            st.warning("有効なプロジェクトがありません。")
            return

        project = st.selectbox("対象プロジェクト", projects, key="admin_project")

        line_users: List[Tuple[str, str, str]] = []
        if not line_users_df.empty:
            tmp = line_users_df[line_users_df["Line_User_ID"].astype(str).str.startswith("U")].drop_duplicates(subset=["Line_User_ID"], keep="last")
            for _, r in tmp.iterrows():
                uid = str(r["Line_User_ID"]).strip()
                name = str(r.get("Line_User", "")).strip()
                line_users.append((f"{name} ({uid})" if name else uid, uid, name))

        view_all = members_df[members_df["Project_Name"] == str(project)].copy()
        view_all["_row_id"] = view_all.index

        if not view_all.empty:
            st.markdown("#### 現在のメンバー一覧")
            show = view_all.copy()
            show["Principal"] = show["Principal"].apply(U.fmt_usd)
            show["状態"] = show["IsActive"].apply(U.bool_to_status)
            st.dataframe(show.drop(columns=["_row_id"], errors="ignore"), use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### 📨 メンバーから選択して個別にLINE送信（個人名 自動挿入）")
        if view_all.empty:
            st.info("メンバーがいないため送信できません。")
        else:
            target_mode = st.radio("対象", ["🟢運用中のみ", "全メンバー（停止含む）"], horizontal=True)
            cand = view_all.copy() if target_mode.startswith("全") else view_all[view_all["IsActive"] == True].copy().reset_index(drop=True)

            def label_row(r: pd.Series) -> str:
                name = str(r.get("PersonName", "")).strip()
                disp = str(r.get("LINE_DisplayName", "")).strip()
                uid = str(r.get("Line_User_ID", "")).strip()
                stt = U.bool_to_status(r.get("IsActive", True))
                return f"{stt} {name} / {disp}" if disp else f"{stt} {name} / {uid}"

            options = [label_row(cand.loc[i]) for i in range(len(cand))]
            selected = st.multiselect("送信先（複数可）", options=options)

            default_msg = f"【ご連絡】\nプロジェクト: {project}\n日時: {U.now_jst().strftime('%Y/%m/%d %H:%M')}\n\n"
            msg_common = st.text_area(
                "メッセージ本文（共通）※送信時に「〇〇 様」を自動挿入します",
                value=st.session_state.get("direct_line_msg", default_msg),
                height=180,
            )
            st.session_state["direct_line_msg"] = msg_common
            img = st.file_uploader("添付画像（任意・ImgBB）", type=["png", "jpg", "jpeg"], key="direct_line_img")

            c1, c2 = st.columns([1, 1])
            do_send = c1.button("選択メンバーへ送信", use_container_width=True)
            clear_msg = c2.button("本文を初期化", use_container_width=True)

            if clear_msg:
                st.session_state["direct_line_msg"] = default_msg
                st.rerun()

            if do_send:
                if not selected:
                    st.warning("送信先を選択してください。")
                elif not msg_common.strip():
                    st.warning("メッセージが空です。")
                else:
                    evidence_url = ExternalService.upload_imgbb(img.getvalue()) if img else None
                    if img and not evidence_url:
                        st.error("画像アップロードに失敗しました。")
                        return

                    token = ExternalService.get_line_token(AdminAuth.current_namespace())
                    label_to_row = {label_row(cand.loc[i]): cand.loc[i] for i in range(len(cand))}
                    success, fail, failed_list, ts, line_log_count = 0, 0, [], U.fmt_dt(U.now_jst()), 0

                    for lab in selected:
                        r = label_to_row.get(lab)
                        if r is None:
                            fail += 1
                            failed_list.append(lab)
                            continue

                        uid = str(r.get("Line_User_ID", "")).strip()
                        person_name = str(r.get("PersonName", "")).strip()
                        disp = str(r.get("LINE_DisplayName", "")).strip()
                        personalized = U.insert_person_name(msg_common, person_name)

                        if not U.is_line_uid(uid):
                            fail += 1
                            failed_list.append(f"{lab}（Line_User_ID不正）")
                            self.repo.append_ledger(ts, project, person_name, AppConfig.TYPE["LINE"], 0, "LINE未送信: Line_User_ID不正", evidence_url or "", uid, disp)
                            line_log_count += 1
                            continue

                        code = ExternalService.send_line_push(token, uid, personalized, evidence_url)
                        self.repo.append_ledger(ts, project, person_name, AppConfig.TYPE["LINE"], 0, f"HTTP:{code}, DirectMessage", evidence_url or "", uid, disp)
                        line_log_count += 1

                        if code == 200:
                            success += 1
                        else:
                            fail += 1
                            failed_list.append(f"{lab}（HTTP {code}）")

                    self.store.persist_and_refresh()
                    if fail == 0:
                        st.success(f"送信完了（成功:{success} / 失敗:{fail} / Ledger記録:{line_log_count}）")
                    else:
                        st.warning(f"送信結果（成功:{success} / 失敗:{fail} / Ledger記録:{line_log_count}）")
                        with st.expander("失敗詳細", expanded=False):
                            st.write("\n".join(failed_list))

        st.divider()
        if not view_all.empty:
            st.markdown("#### 状態切替")

            status_options = []
            for _, r in view_all.iterrows():
                person_name = str(r["PersonName"]).strip()
                status_label = U.bool_to_status(r["IsActive"])
                status_options.append(f"{person_name} ｜ {status_label}")

            selected_label = st.selectbox("対象メンバー", status_options, key=f"status_target_{project}")
            selected_name = str(selected_label).split("｜")[0].strip()

            cur_row = view_all[view_all["PersonName"].astype(str).str.strip() == selected_name].iloc[0]
            current_status = U.bool_to_status(cur_row["IsActive"])
            next_status = AppConfig.STATUS["OFF"] if U.truthy(cur_row["IsActive"]) else AppConfig.STATUS["ON"]
            button_label = f"{current_status} → {next_status}"

            if st.button(button_label, use_container_width=True, key=f"toggle_status_{project}"):
                row_id = int(cur_row["_row_id"])
                ts = U.fmt_dt(U.now_jst())

                members_df.loc[row_id, "IsActive"] = not U.truthy(members_df.loc[row_id, "IsActive"])
                members_df.loc[row_id, "UpdatedAt_JST"] = ts

                msg = self.repo.validate_no_dup_lineid(members_df, project)
                if msg:
                    st.error(msg)
                    return

                self.repo.write_members(members_df)
                self.store.persist_and_refresh()
                st.success(f"{selected_name} を {next_status} に更新しました。")
                st.rerun()

        st.divider()
        if not view_all.empty:
            st.markdown("#### 一括編集（保存ボタンで確定）")
            edit_src = view_all.copy()
            edit_src["状態"] = edit_src["IsActive"].apply(U.bool_to_status)
            edit_show = edit_src[["PersonName", "Principal", "Rank", "状態", "Line_User_ID", "LINE_DisplayName"]].copy()
            row_ids = edit_src["_row_id"].tolist()

            edited = st.data_editor(
                edit_show,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_config={
                    "Principal": st.column_config.NumberColumn("Principal", min_value=0.0, step=100.0),
                    "Rank": st.column_config.SelectboxColumn("Rank", options=[AppConfig.RANK["MASTER"], AppConfig.RANK["ELITE"]]),
                    "状態": st.column_config.SelectboxColumn("状態", options=[AppConfig.STATUS["ON"], AppConfig.STATUS["OFF"]]),
                },
                key=f"members_editor_{project}",
            )

            c1, c2 = st.columns([1, 1])
            save = c1.button("編集内容を保存", use_container_width=True, key=f"save_members_{project}")
            cancel = c2.button("編集を破棄（再読み込み）", use_container_width=True, key=f"cancel_members_{project}")

            if cancel:
                self.store.refresh()
                st.rerun()

            if save:
                ts = U.fmt_dt(U.now_jst())
                edited = edited.copy()
                edited["_row_id"] = row_ids

                for _, r in edited.iterrows():
                    row_id = int(r["_row_id"])
                    members_df.loc[row_id, "Principal"] = float(U.to_f(r["Principal"]))
                    members_df.loc[row_id, "Rank"] = U.normalize_rank(r["Rank"])
                    members_df.loc[row_id, "IsActive"] = U.status_to_bool(r["状態"])
                    members_df.loc[row_id, "Line_User_ID"] = str(r["Line_User_ID"]).strip()
                    members_df.loc[row_id, "LINE_DisplayName"] = str(r["LINE_DisplayName"]).strip()
                    members_df.loc[row_id, "UpdatedAt_JST"] = ts

                msg = self.repo.validate_no_dup_lineid(members_df, project)
                if msg:
                    st.error(msg)
                    return

                self.repo.write_members(members_df)
                self.store.persist_and_refresh()
                st.success("保存しました。")
                st.rerun()

        st.divider()
        st.markdown("#### 追加（同一プロジェクト内で Line_User_ID が一致したら追加しない）")

        add_mode = st.selectbox("追加先", ["個人(PERSONAL)", "プロジェクト"], key="member_add_mode")
        all_projects = self.repo.active_projects(settings_df)
        if add_mode == "個人(PERSONAL)":
            selected_project = AppConfig.PROJECT["PERSONAL"]
            st.info("登録先: PERSONAL")
        else:
            project_candidates = [p for p in all_projects if str(p).strip().upper() != AppConfig.PROJECT["PERSONAL"]]
            if not project_candidates:
                st.warning("PERSONAL以外のプロジェクトがありません。")
                return
            selected_project = st.selectbox("登録するプロジェクト", project_candidates, key="member_add_target_project")

        if line_users:
            labels = ["（選択しない）"] + [x[0] for x in line_users]
            picked = st.selectbox("登録済みLINEユーザーから選択", labels, index=0)
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
            rank = st.selectbox("Rank", [AppConfig.RANK["MASTER"], AppConfig.RANK["ELITE"]], index=0)
            status = st.selectbox("ステータス", [AppConfig.STATUS["ON"], AppConfig.STATUS["OFF"]], index=0)
            submit = st.form_submit_button("保存（追加）")

        if submit:
            if not person or not line_uid:
                st.error("PersonName と Line_User_ID は必須です。")
                return

            exists = members_df[
                (members_df["Project_Name"] == str(selected_project))
                & (members_df["Line_User_ID"].astype(str).str.strip() == str(line_uid).strip())
            ]
            if not exists.empty:
                st.warning("このプロジェクト内に同じ Line_User_ID が既に存在します。")
                return

            ts = U.fmt_dt(U.now_jst())
            new_row = {
                "Project_Name": str(selected_project).strip(),
                "PersonName": str(person).strip(),
                "Principal": float(principal),
                "Line_User_ID": str(line_uid).strip(),
                "LINE_DisplayName": str(line_disp).strip(),
                "Rank": U.normalize_rank(rank),
                "IsActive": U.status_to_bool(status),
                "CreatedAt_JST": ts,
                "UpdatedAt_JST": ts,
            }
            members_df = pd.concat([members_df, pd.DataFrame([new_row])], ignore_index=True)

            msg = self.repo.validate_no_dup_lineid(members_df, selected_project)
            if msg:
                st.error(msg)
                return

            self.repo.write_members(members_df)
            self.store.persist_and_refresh()
            st.success(f"追加しました。登録先: {selected_project}")
            st.rerun()

    def render_help(self, gs: GSheetService, settings_df: pd.DataFrame) -> None:
        st.subheader("❓ ヘルプ / 使い方")
        st.caption(f"{AppConfig.RANK_LABEL} / 管理者: {AdminAuth.current_label()}")

        st.markdown(
            """
このアプリは、APR運用の記録、入出金、メンバー管理、LINE通知をまとめて扱う管理システムです。
左メニューの **📊 ダッシュボード / 📈 APR / 💸 入金/出金 / ⚙️ 管理 / ❓ ヘルプ** で画面を切り替えます。
"""
        )

        with st.expander("1. 現在の接続情報", expanded=False):
            st.code(
                f"""参照シート
Settings           = {gs.names.SETTINGS}
Members            = {gs.names.MEMBERS}
Ledger             = {gs.names.LEDGER}
LineUsers          = {gs.names.LINEUSERS}
APR_Summary        = {gs.names.APR_SUMMARY}
SmartVault_History = {gs.names.SMARTVAULT_HISTORY}

Spreadsheet ID
{gs.spreadsheet_id}

Spreadsheet URL
{gs.spreadsheet_url()}
"""
            )

        with st.expander("2. シート構成", expanded=False):
            st.markdown("### Settings")
            st.code("\t".join(AppConfig.HEADERS["SETTINGS"]))
            st.markdown("### Members")
            st.code("\t".join(AppConfig.HEADERS["MEMBERS"]))
            st.markdown("### Ledger")
            st.code("\t".join(AppConfig.HEADERS["LEDGER"]))
            st.markdown("### LineUsers")
            st.code("\t".join(AppConfig.HEADERS["LINEUSERS"]))
            st.markdown("### APR Summary")
            st.code("\t".join(AppConfig.HEADERS["APR_SUMMARY"]))
            st.markdown("### SmartVault_History")
            st.code("\t".join(AppConfig.HEADERS["SMARTVAULT_HISTORY"]))

        with st.expander("3. Compound_Timing の意味", expanded=False):
            st.markdown(
                """
- `daily`
  APR確定時に元本へ即時加算します。次回以降は増えた元本で計算します。

- `monthly`
  APR確定時は Ledger に記録のみ行います。元本への反映は APR画面の「未反映APRを元本へ反映」でまとめて行います。

- `none`
  単利です。APRは Ledger に記録しますが、元本には加算しません。
"""
            )

        with st.expander("4. APR計算ロジック", expanded=False):
            st.markdown(
                """
### 入力項目
APR画面では以下を管理します。

- 流動性
- 昨日の収益
- APR

いずれも手動入力できます。画像を入れた場合は OCRで別取得 もできます。

### OCR
Smart Vaultモバイル画面では固定ボックスで
- 総流動性
- 昨日の収益
- APR
を別々にOCRしています。

### SmartVault履歴
APR確定時に `SmartVault_History` シートへ
- 最終採用値
- OCR取得値
- Source_Mode（manual / ocr / ocr+manual）
を保存します。

### PERSONAL
個人ごとの元本で計算します。

`DailyAPR = Principal × (最終APR% / 100) × Rank係数 ÷ 365`

- Master = 0.67
- Elite = 0.60

### GROUP（PERSONAL以外）
グループ総額を基準に計算し、人数で均等割します。

`グループ総配当 = グループ総元本 × (最終APR% / 100) × Net_Factor ÷ 365`

`1人あたり配当 = グループ総配当 ÷ 人数`

### 重複防止
同日・同一プロジェクト・同一人物の APR は Ledger を見て1回だけ記録します。
本日のAPRをやり直したい場合は、APR画面の「本日のAPR記録をリセット」を使います。
"""
            )

        with st.expander("5. Make連携", expanded=False):
            st.markdown(
                """
### 目的
LINEユーザー情報を `LineUsers` シートへ自動登録し、管理画面の追加候補として使います。

### 推奨フロー
`LINE Watch Events → HTTP(プロフィール取得) → Google Sheets Search Rows → Filter(0件のみ) → Google Sheets Add a Row`
"""
            )
            st.code("\t".join(AppConfig.HEADERS["LINEUSERS"]))

        with st.expander("6. Settings自動修復", expanded=False):
            st.markdown(
                """
Settings シートの不足列補完、PERSONAL行の不足補完、OCR初期座標の補完を行います。
シート構造が崩れたときはこちらを実行してください。
"""
            )
            if st.button("Settingsを自動修復", key="help_fix_settings", use_container_width=True):
                try:
                    self.repo.repair_settings(self.repo.load_settings())
                    self.store.persist_and_refresh()
                    st.success(f"{self.repo.gs.names.SETTINGS} を修復しました。")
                    st.rerun()
                except Exception as e:
                    st.error(f"Settings修復でエラー: {e}")

        with st.expander("7. OCR設定（座標設定 + 赤枠プレビュー）", expanded=False):
            projects = self.repo.active_projects(settings_df)
            if not projects:
                st.warning("有効なプロジェクトがありません。")
                return

            ocr_project = st.selectbox("OCR設定対象プロジェクト", projects, key="help_ocr_project")
            row_setting = settings_df[settings_df["Project_Name"] == ocr_project].iloc[0]

            st.markdown("#### 現在値")
            current_vals = pd.DataFrame(
                [
                    {
                        "Crop_Left_Ratio_PC": row_setting.get("Crop_Left_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"]),
                        "Crop_Top_Ratio_PC": row_setting.get("Crop_Top_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"]),
                        "Crop_Right_Ratio_PC": row_setting.get("Crop_Right_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"]),
                        "Crop_Bottom_Ratio_PC": row_setting.get("Crop_Bottom_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"]),
                        "Crop_Left_Ratio_Mobile": row_setting.get("Crop_Left_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"]),
                        "Crop_Top_Ratio_Mobile": row_setting.get("Crop_Top_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"]),
                        "Crop_Right_Ratio_Mobile": row_setting.get("Crop_Right_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"]),
                        "Crop_Bottom_Ratio_Mobile": row_setting.get("Crop_Bottom_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"]),
                    }
                ]
            )
            st.dataframe(current_vals, use_container_width=True, hide_index=True)

            st.markdown("#### SmartVaultモバイル専用 固定赤枠座標")
            st.code(
                f"""TOTAL_LIQUIDITY
left={AppConfig.SMARTVAULT_BOXES_MOBILE['TOTAL_LIQUIDITY']['left']:.2f}
top={AppConfig.SMARTVAULT_BOXES_MOBILE['TOTAL_LIQUIDITY']['top']:.2f}
right={AppConfig.SMARTVAULT_BOXES_MOBILE['TOTAL_LIQUIDITY']['right']:.2f}
bottom={AppConfig.SMARTVAULT_BOXES_MOBILE['TOTAL_LIQUIDITY']['bottom']:.2f}

YESTERDAY_PROFIT
left={AppConfig.SMARTVAULT_BOXES_MOBILE['YESTERDAY_PROFIT']['left']:.2f}
top={AppConfig.SMARTVAULT_BOXES_MOBILE['YESTERDAY_PROFIT']['top']:.2f}
right={AppConfig.SMARTVAULT_BOXES_MOBILE['YESTERDAY_PROFIT']['right']:.2f}
bottom={AppConfig.SMARTVAULT_BOXES_MOBILE['YESTERDAY_PROFIT']['bottom']:.2f}

APR
left={AppConfig.SMARTVAULT_BOXES_MOBILE['APR']['left']:.2f}
top={AppConfig.SMARTVAULT_BOXES_MOBILE['APR']['top']:.2f}
right={AppConfig.SMARTVAULT_BOXES_MOBILE['APR']['right']:.2f}
bottom={AppConfig.SMARTVAULT_BOXES_MOBILE['APR']['bottom']:.2f}
"""
            )

            st.markdown("#### 座標入力")

            st.markdown("##### PC")
            c1, c2, c3, c4 = st.columns(4)
            pc_left = c1.number_input(
                "Crop_Left_Ratio_PC",
                min_value=0.0,
                max_value=1.0,
                value=float(row_setting.get("Crop_Left_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"])),
                step=0.01,
                key=f"help_pc_left_{ocr_project}",
            )
            pc_top = c2.number_input(
                "Crop_Top_Ratio_PC",
                min_value=0.0,
                max_value=1.0,
                value=float(row_setting.get("Crop_Top_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"])),
                step=0.01,
                key=f"help_pc_top_{ocr_project}",
            )
            pc_right = c3.number_input(
                "Crop_Right_Ratio_PC",
                min_value=0.0,
                max_value=1.0,
                value=float(row_setting.get("Crop_Right_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"])),
                step=0.01,
                key=f"help_pc_right_{ocr_project}",
            )
            pc_bottom = c4.number_input(
                "Crop_Bottom_Ratio_PC",
                min_value=0.0,
                max_value=1.0,
                value=float(row_setting.get("Crop_Bottom_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"])),
                step=0.01,
                key=f"help_pc_bottom_{ocr_project}",
            )

            st.markdown("##### Mobile")
            c5, c6, c7, c8 = st.columns(4)
            mobile_left = c5.number_input(
                "Crop_Left_Ratio_Mobile",
                min_value=0.0,
                max_value=1.0,
                value=float(row_setting.get("Crop_Left_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"])),
                step=0.01,
                key=f"help_mobile_left_{ocr_project}",
            )
            mobile_top = c6.number_input(
                "Crop_Top_Ratio_Mobile",
                min_value=0.0,
                max_value=1.0,
                value=float(row_setting.get("Crop_Top_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"])),
                step=0.01,
                key=f"help_mobile_top_{ocr_project}",
            )
            mobile_right = c7.number_input(
                "Crop_Right_Ratio_Mobile",
                min_value=0.0,
                max_value=1.0,
                value=float(row_setting.get("Crop_Right_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"])),
                step=0.01,
                key=f"help_mobile_right_{ocr_project}",
            )
            mobile_bottom = c8.number_input(
                "Crop_Bottom_Ratio_Mobile",
                min_value=0.0,
                max_value=1.0,
                value=float(row_setting.get("Crop_Bottom_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"])),
                step=0.01,
                key=f"help_mobile_bottom_{ocr_project}",
            )

            st.markdown("#### OCR確認用画像アップロード")
            preview = st.file_uploader(
                "画像をアップロードすると赤枠プレビューします",
                type=["png", "jpg", "jpeg"],
                key="help_ocr_preview",
            )

            if preview is not None:
                try:
                    file_bytes = preview.getvalue()

                    custom_mobile_box = {
                        "CUSTOM_MOBILE": {
                            "left": float(mobile_left),
                            "top": float(mobile_top),
                            "right": float(mobile_right),
                            "bottom": float(mobile_bottom),
                        }
                    }
                    custom_pc_box = {
                        "CUSTOM_PC": {
                            "left": float(pc_left),
                            "top": float(pc_top),
                            "right": float(pc_right),
                            "bottom": float(pc_bottom),
                        }
                    }

                    smartvault_boxes = AppConfig.SMARTVAULT_BOXES_MOBILE

                    st.markdown("##### 元画像")
                    st.image(file_bytes, caption="元画像", use_container_width=True)

                    st.markdown("##### SmartVault固定赤枠プレビュー")
                    smart_boxed = U.draw_ocr_boxes(file_bytes, smartvault_boxes)
                    st.image(smart_boxed, caption="SmartVault固定赤枠", use_container_width=True)

                    st.markdown("##### あなたのMobile座標 赤枠プレビュー")
                    mobile_boxed = U.draw_ocr_boxes(file_bytes, custom_mobile_box)
                    st.image(mobile_boxed, caption="現在のMobile設定", use_container_width=True)

                    st.markdown("##### あなたのPC座標 赤枠プレビュー")
                    pc_boxed = U.draw_ocr_boxes(file_bytes, custom_pc_box)
                    st.image(pc_boxed, caption="現在のPC設定", use_container_width=True)

                except Exception as e:
                    st.error(f"赤枠プレビュー表示でエラー: {e}")

            if st.button("OCR座標を保存", key=f"help_save_ocr_{ocr_project}", use_container_width=True):
                try:
                    idx = settings_df[settings_df["Project_Name"] == ocr_project].index[0]
                    settings_df.loc[idx, "Crop_Left_Ratio_PC"] = U.to_ratio(pc_left, AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"])
                    settings_df.loc[idx, "Crop_Top_Ratio_PC"] = U.to_ratio(pc_top, AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"])
                    settings_df.loc[idx, "Crop_Right_Ratio_PC"] = U.to_ratio(pc_right, AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"])
                    settings_df.loc[idx, "Crop_Bottom_Ratio_PC"] = U.to_ratio(pc_bottom, AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"])

                    settings_df.loc[idx, "Crop_Left_Ratio_Mobile"] = U.to_ratio(mobile_left, AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"])
                    settings_df.loc[idx, "Crop_Top_Ratio_Mobile"] = U.to_ratio(mobile_top, AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"])
                    settings_df.loc[idx, "Crop_Right_Ratio_Mobile"] = U.to_ratio(mobile_right, AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"])
                    settings_df.loc[idx, "Crop_Bottom_Ratio_Mobile"] = U.to_ratio(mobile_bottom, AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"])
                    settings_df.loc[idx, "UpdatedAt_JST"] = U.fmt_dt(U.now_jst())

                    self.repo.write_settings(settings_df)
                    self.store.persist_and_refresh()
                    st.success("OCR設定を保存しました。")
                    st.rerun()
                except Exception as e:
                    st.error(f"OCR設定保存でエラー: {e}")


# =========================================================
# APP CONTROLLER
# =========================================================
class AppController:
    def __init__(self):
        self.gs: Optional[GSheetService] = None
        self.repo: Optional[Repository] = None
        self.engine: Optional[FinanceEngine] = None
        self.store: Optional[DataStore] = None
        self.ui: Optional[AppUI] = None

    def setup_page(self) -> None:
        st.set_page_config(page_title=AppConfig.APP_TITLE, layout=AppConfig.PAGE_LAYOUT, page_icon=AppConfig.APP_ICON)
        st.title(f"{AppConfig.APP_ICON} {AppConfig.APP_TITLE}")

    def setup_auth(self) -> None:
        AdminAuth.require_login()
        st.markdown(
            """
            <style>
              section[data-testid="stSidebar"] div[role="radiogroup"] > label { margin: 10px 0 !important; padding: 6px 8px !important; }
              section[data-testid="stSidebar"] div[role="radiogroup"] > label p { font-size: 16px !important; }
            </style>
            """,
            unsafe_allow_html=True,
        )
        with st.sidebar:
            st.caption(f"👤 {AdminAuth.current_label()}")
            if st.button("🔓 ログアウト", use_container_width=True):
                st.session_state["admin_ok"] = False
                st.session_state["admin_name"] = ""
                st.session_state["admin_namespace"] = ""
                for key in AppConfig.SESSION_KEYS.values():
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()

    def setup_state(self) -> None:
        if "page" not in st.session_state:
            st.session_state["page"] = AppConfig.PAGE["DASHBOARD"]
        if "hide_line_history" not in st.session_state:
            st.session_state["hide_line_history"] = False

    def setup_services(self) -> None:
        con = st.secrets.get("connections", {}).get("gsheets", {})
        sid = U.extract_sheet_id(str(con.get("spreadsheet", "")).strip())
        if not sid:
            st.error("Secrets の [connections.gsheets].spreadsheet が未設定です。")
            st.stop()

        try:
            self.gs = GSheetService(spreadsheet_id=sid, namespace=AdminAuth.current_namespace())
        except Exception as e:
            msg = str(e)
            if "Quota exceeded" in msg or "429" in msg:
                st.error("Google Sheets API の読み取り上限に達しています。1〜2分待ってから再読み込みしてください。")
            else:
                st.error(f"Spreadsheet を開けません。: {e}")
            st.stop()

        self.repo = Repository(self.gs)
        self.engine = FinanceEngine()
        self.store = DataStore(self.repo, self.engine)
        self.ui = AppUI(self.repo, self.engine, self.store)

    def run(self) -> None:
        self.setup_page()
        self.setup_auth()
        self.setup_state()
        self.setup_services()

        data = self.store.load(force=False)
        menu = [
            AppConfig.PAGE["DASHBOARD"],
            AppConfig.PAGE["APR"],
            AppConfig.PAGE["CASH"],
            AppConfig.PAGE["ADMIN"],
            AppConfig.PAGE["HELP"],
        ]
        page = st.sidebar.radio("メニュー", options=menu, index=menu.index(st.session_state["page"]) if st.session_state["page"] in menu else 0)
        st.session_state["page"] = page

        if page == AppConfig.PAGE["DASHBOARD"]:
            self.repo.write_apr_summary(data["apr_summary_df"])
            self.ui.render_dashboard(data["members_df"], data["ledger_df"], data["apr_summary_df"])
        elif page == AppConfig.PAGE["APR"]:
            self.ui.render_apr(data["settings_df"], data["members_df"])
        elif page == AppConfig.PAGE["CASH"]:
            self.ui.render_cash(data["settings_df"], data["members_df"])
        elif page == AppConfig.PAGE["ADMIN"]:
            self.ui.render_admin(data["settings_df"], data["members_df"], data["line_users_df"])
        else:
            self.ui.render_help(self.gs, data["settings_df"])


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    AppController().run()


if __name__ == "__main__":
    main()
