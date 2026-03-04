import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone
import requests, json, re

import gspread
from google.oauth2.service_account import Credentials

# =========================================================
# APR資産運用管理システム（全体コード）
# - Google Sheets: gspread + Service Account
# - LINE: Push message（個別通知 / 全員通知）
# - ImgBB: エビデンス画像アップロード（任意）
# - Sheets:
#   - Settings（プロジェクト設定）
#   - LineID（Makeが自動取得して追記するログ）
#   - Members（個人名⇔個人台帳⇔LINE ID 紐付け台帳）※管理者が紐付け
#   - 各個人台帳シート（Members.SheetName に対応）
# =========================================================

# ---------- ページ設定 ----------
st.set_page_config(page_title="APR管理システム", layout="wide", page_icon="🏦")

# ---------- JST ----------
JST = timezone(timedelta(hours=9))

def now_jst() -> datetime:
    return datetime.now(JST)

def fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def fmt_time(dt: datetime) -> str:
    return dt.strftime("%H:%M:%S")

def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ---------- Sheets 接続 ----------
def gs_client():
    # secrets.toml / Streamlit Cloud Secrets に以下が必要
    # [connections.gsheets]
    # spreadsheet = "https://docs.google.com/spreadsheets/d/....../edit"
    # [connections.gsheets.credentials]
    # type="service_account" ... （Googleのサービスアカウントjsonの中身）
    cred_info = st.secrets["connections"]["gsheets"]["credentials"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(cred_info, scopes=scopes)
    return gspread.authorize(creds)

def open_sheet():
    spreadsheet_url = st.secrets["connections"]["gsheets"]["spreadsheet"]
    return gs_client().open_by_url(spreadsheet_url)

def ws_to_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=header)

def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\u3000", " ", regex=False)  # 全角スペース→半角
        .str.strip()
    )
    return df

def ensure_headers(ws, headers: list[str]):
    """シートが空ならヘッダーだけ入れる。ヘッダーがあるなら足りない列を右に追加。"""
    df = ws_to_df(ws)
    if df.empty:
        ws.clear()
        ws.update([headers])
        return

    df = clean_cols(df)
    current = list(df.columns)
    missing = [h for h in headers if h not in current]
    if missing:
        # 既存データを壊さず右に追加
        new_cols = current + missing
        df2 = df.reindex(columns=new_cols, fill_value="")
        ws.clear()
        ws.update([df2.columns.tolist()] + df2.astype(str).fillna("").values.tolist())

def df_to_ws(ws, df: pd.DataFrame):
    df = clean_cols(df)
    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).fillna("").values.tolist())

def get_or_create_ws(sh, title: str, headers: list[str], rows=1000, cols=30):
    try:
        ws = sh.worksheet(title)
    except:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
    ensure_headers(ws, headers)
    return ws

# ---------- 数値ユーティリティ ----------
def to_f(val) -> float:
    try:
        s = str(val).replace(",", "").replace("$", "").replace("%", "").strip()
        return float(s) if s else 0.0
    except:
        return 0.0

def split_csv(val, n: int) -> list[str]:
    items = [x.strip() for x in str(val).split(",") if str(x).strip() != ""]
    while len(items) < n:
        items.append(items[-1] if items else "0")
    return items[:n]

def uniq_keep_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def only_line_ids(values):
    out = []
    for v in values:
        s = str(v).strip()
        if s.startswith("U"):
            out.append(s)
    return uniq_keep_order(out)

# ---------- LINE送信 ----------
def send_line(token: str, user_id: str, text: str, image_url: str | None = None) -> int:
    if not user_id or str(user_id).strip() == "":
        return 400
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    messages = [{"type": "text", "text": text}]
    if image_url:
        messages.append({
            "type": "image",
            "originalContentUrl": image_url,
            "previewImageUrl": image_url
        })

    payload = {"to": str(user_id), "messages": messages}
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
        return r.status_code
    except:
        return 500

def upload_imgbb(file_bytes: bytes) -> str | None:
    """ImgBBへアップロードしてURLを返す（失敗時None）"""
    try:
        res = requests.post(
            "https://api.imgbb.com/1/upload",
            params={"key": st.secrets["imgbb"]["api_key"]},
            files={"image": file_bytes},
            timeout=30
        )
        data = res.json()
        return data["data"]["url"]
    except:
        return None

# ---------- 設定 ----------
SETTINGS_HEADERS = [
    "Project_Name",
    "Num_People",
    "TotalPrincipal",
    "IndividualPrincipals",
    "ProfitRates",
    "IsCompound",
    "MemberNames",
    "LineID"
]

LINEID_HEADERS = [
    "Line_User_ID",
    "Line_User",
    "Date",
    "Time",
    "Type"
]

MEMBERS_HEADERS = [
    "MemberName",
    "SheetName",
    "Line_User_ID",
    "Line_User",
    "LinkedAt",
    "Status"
]

LEDGER_HEADERS = [
    "Date",
    "Time",
    "Type",          # 入金 / 出金 / 収益
    "Amount",        # 変動額（+/- ではなく純額：入金は正、出金は正でOK）
    "Balance_After", # 取引後残高（自動計算）
    "Note"
]

# ---------- 管理者ガード ----------
def is_admin() -> bool:
    # secretsに admin.pin がある場合のみ管理を有効化
    # [admin]
    # pin = "1234"
    if "admin" not in st.secrets or "pin" not in st.secrets["admin"]:
        return False
    if st.session_state.get("is_admin") is True:
        return True
    return False

def admin_login_ui():
    if "admin" not in st.secrets or "pin" not in st.secrets["admin"]:
        st.info("管理者PINが未設定のため、管理機能は無効です（secretsに [admin].pin を設定してください）。")
        return

    if st.session_state.get("is_admin") is True:
        st.success("管理者ログイン中")
        if st.button("管理者ログアウト"):
            st.session_state["is_admin"] = False
            st.rerun()
        return

    pin = st.text_input("管理者PIN", type="password")
    if st.button("管理者ログイン"):
        if str(pin) == str(st.secrets["admin"]["pin"]):
            st.session_state["is_admin"] = True
            st.success("ログインしました")
            st.rerun()
        else:
            st.error("PINが違います")

# =========================================================
# メイン
# =========================================================
st.title("🏦 APR資産運用管理システム")

try:
    sh = open_sheet()

    # 必須シートのヘッダー整備
    settings_ws = get_or_create_ws(sh, "Settings", SETTINGS_HEADERS)
    lineid_ws   = get_or_create_ws(sh, "LineID", LINEID_HEADERS)   # Makeのログ（既にあるなら合わせる）
    members_ws  = get_or_create_ws(sh, "Members", MEMBERS_HEADERS) # 新規作成した紐付け台帳

    settings_df = clean_cols(ws_to_df(settings_ws))
    lineid_df   = clean_cols(ws_to_df(lineid_ws))
    members_df  = clean_cols(ws_to_df(members_ws))

    # Settings検証
    if settings_df.empty:
        st.error("Settingsシートが空です。Project_Name などを入力してください。")
        st.stop()

    missing = [c for c in SETTINGS_HEADERS if c not in settings_df.columns]
    if missing:
        st.error(f"Settingsシートの列が不足: {missing}\n現在の列: {list(settings_df.columns)}")
        st.stop()

    # プロジェクト選択
    project_list = settings_df["Project_Name"].dropna().astype(str).unique().tolist()
    if not project_list:
        st.error("Settingsの Project_Name が空です。")
        st.stop()

    selected_project = st.sidebar.selectbox("プロジェクトを選択", project_list)
    p_info = settings_df[settings_df["Project_Name"].astype(str) == str(selected_project)].iloc[0]

    # 設定値
    num_people = int(to_f(p_info["Num_People"])) if str(p_info["Num_People"]).strip() != "" else 0
    is_compound = str(p_info["IsCompound"]).strip().upper() in ["TRUE", "YES", "1", "はい"]

    # メンバー名（プロジェクトの表示用）
    member_names = split_csv(p_info["MemberNames"], max(num_people, 1)) if num_people > 0 else []
    if num_people > 0 and len(member_names) < num_people:
        member_names = [f"No.{i+1}" for i in range(num_people)]

    # 個別元本 & 配分比率
    base_principals = [to_f(x) for x in split_csv(p_info["IndividualPrincipals"], max(num_people, 1))] if num_people > 0 else []
    rate_list = [to_f(x) for x in split_csv(p_info["ProfitRates"], max(num_people, 1))] if num_people > 0 else []

    # Members整備（空の場合でもUIが動くように）
    if members_df.empty:
        members_df = pd.DataFrame(columns=MEMBERS_HEADERS)

    # LINE送信先（全員通知用）
    # 優先: Members の linked 行（Line_User_IDが埋まっている）
    linked_ids = []
    if "Line_User_ID" in members_df.columns and not members_df.empty:
        linked_ids = only_line_ids(members_df["Line_User_ID"].dropna().tolist())

    # 補助: LineIDログからも拾う（重複はuniq）
    log_ids = []
    if not lineid_df.empty and "Line_User_ID" in lineid_df.columns:
        log_ids = only_line_ids(lineid_df["Line_User_ID"].dropna().tolist())

    broadcast_ids = uniq_keep_order(linked_ids + log_ids)

    st.sidebar.info(f"計算モード: {'複利' if is_compound else '単利'}")
    st.sidebar.write(f"全員通知の送信先ID数: {len(broadcast_ids)}")

    # タブ構成（入出金は同じタブ）
    tab_profit, tab_cash, tab_admin = st.tabs(["📈 収益確定・全員LINE", "💳 入金・出金（個別通知）", "⚙️ 管理（管理者のみ）"])

    # =========================================================
    # 1) 収益確定・全員LINE
    # =========================================================
    with tab_profit:
        st.subheader(f"【{selected_project}】本日の収益（全員通知）")

        if num_people <= 0:
            st.warning("Settingsの Num_People が未設定です。")
            st.stop()

        total_apr = st.number_input("本日の全体APR (%)", value=100.0, step=0.1)
        net_factor = 0.67

        uploaded_file = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"])
        if uploaded_file:
            st.image(uploaded_file, caption="プレビュー", width=420)

        # 収益の算出（プロジェクト設定の IndividualPrincipals / ProfitRates に従う）
        # 複利の場合、プロジェクト履歴を見て元本を増減したいなら「プロジェクト履歴シート」を別途運用が必要。
        # ここでは「当日の配分計算と全員通知」を確実に動かすため、基礎元本ベースで計算します。
        today_yields = []
        for i in range(num_people):
            p = base_principals[i] if i < len(base_principals) else 0.0
            r = rate_list[i] if i < len(rate_list) else 0.0
            y = (p * (total_apr * net_factor * r / 100.0)) / 365.0
            today_yields.append(round(y, 4))

        # 表示
        cols = st.columns(min(num_people, 6))
        for i in range(num_people):
            with cols[i % len(cols)]:
                nm = member_names[i] if i < len(member_names) else f"No.{i+1}"
                principal = base_principals[i] if i < len(base_principals) else 0.0
                st.metric(nm, f"${principal:,.2f}", f"+${today_yields[i]:,.4f}")

        st.divider()
        st.caption("※本日の収益の報告は全員に送信します（個人名はメッセージに含めません）。")

        if st.button("収益を全員にLINE送信（画像あり可）"):
            # 画像アップロード（任意）
            image_url = None
            if uploaded_file:
                with st.spinner("ImgBBへ画像アップロード中..."):
                    image_url = upload_imgbb(uploaded_file.getvalue())
                if not image_url:
                    st.error("画像アップロードに失敗しました（ImgBB）。画像なしで送るなら画像を外して再実行してください。")
                    st.stop()

            # メッセージ（個人名は入れない）
            dt = now_jst()
            msg = "🏦 【本日の運用収益報告】\n"
            msg += f"プロジェクト: {selected_project}\n"
            msg += f"日時(JST): {dt.strftime('%Y/%m/%d %H:%M')}\n"
            msg += f"本日のAPR: {total_apr}%\n"
            msg += f"モード: {'複利' if is_compound else '単利'}\n"
            msg += "\n"
            msg += f"本日の合計収益（概算）: ${sum(today_yields):,.4f}\n"
            if image_url:
                msg += "\n📎 エビデンス画像を添付します。"

            # 送信
            token = st.secrets["line"]["channel_access_token"]
            ok, ng = 0, 0
            for uid in broadcast_ids:
                code = send_line(token, uid, msg, image_url=image_url)
                if code == 200:
                    ok += 1
                else:
                    ng += 1
            st.success(f"送信完了：成功 {ok} / 失敗 {ng}")

    # =========================================================
    # 2) 入金・出金（個別通知） + 個人台帳に記録（残高を自動更新）
    # =========================================================
    with tab_cash:
        st.subheader("💳 入金・出金の記録（個別通知 + 個人台帳の残高自動更新）")

        if members_df.empty:
            st.warning("Membersシートが空です。先に⚙️管理でメンバーを登録してください。")
            st.stop()

        # 紐付け済みメンバーだけに絞る（誤送信防止）
        # 未紐付けでも記録だけしたい場合は、下の linked_only を False にしてください。
        linked_only = True

        mdf = members_df.copy()
        for c in MEMBERS_HEADERS:
            if c not in mdf.columns:
                mdf[c] = ""

        if linked_only:
            mdf_view = mdf[(mdf["Line_User_ID"].astype(str).str.startswith("U"))].copy()
            if mdf_view.empty:
                st.warning("紐付け済みメンバーがいません（Membersの Line_User_ID が空です）。⚙️管理で紐付けしてください。")
                st.stop()
        else:
            mdf_view = mdf.copy()

        # プルダウン
        member_list = mdf_view["MemberName"].astype(str).tolist()
        selected_member = st.selectbox("メンバー（個人名）", member_list)

        row = mdf_view[mdf_view["MemberName"].astype(str) == str(selected_member)].iloc[0]
        sheet_name = str(row.get("SheetName", "")).strip()
        line_user_id = str(row.get("Line_User_ID", "")).strip()
        line_user_name = str(row.get("Line_User", "")).strip()

        if not sheet_name:
            st.error("Membersの SheetName が空です。個人台帳シート名を設定してください。")
            st.stop()

        # 個人台帳シートを用意
        ledger_ws = get_or_create_ws(sh, sheet_name, LEDGER_HEADERS)
        ledger_df = clean_cols(ws_to_df(ledger_ws))

        # 直近残高
        current_balance = 0.0
        if not ledger_df.empty and "Balance_After" in ledger_df.columns:
            # 最後の行の残高
            last = ledger_df.iloc[-1]
            current_balance = to_f(last.get("Balance_After", 0.0))

        st.info(f"現在残高: ${current_balance:,.2f}（個人台帳: {sheet_name}）")

        colA, colB = st.columns(2)
        with colA:
            tx_type = st.selectbox("種別", ["入金", "出金"])
        with colB:
            amount = st.number_input("金額 ($)", min_value=0.0, step=10.0)

        note = st.text_input("備考", value="")

        # 収益(=APRで入った額) も「入金として残高に加算したい」要件に対応するなら、
        # ここでType=収益も記録可能にします（残高は入金方向に増える）。
        extra = st.checkbox("APR/収益として記録（残高に加算）", value=False)
        if extra:
            tx_type = "収益"

        if st.button("保存して（個人に）LINE通知"):
            if amount <= 0:
                st.warning("金額が0です。")
                st.stop()

            dt = now_jst()
            amt = float(amount)

            # 残高更新ルール
            if tx_type == "出金":
                new_balance = current_balance - amt
            else:
                # 入金 / 収益 は残高を増やす
                new_balance = current_balance + amt

            # 台帳に追記
            new_row = {
                "Date": fmt_date(dt),
                "Time": fmt_time(dt),
                "Type": tx_type,
                "Amount": f"{amt:.2f}",
                "Balance_After": f"{new_balance:.2f}",
                "Note": note
            }

            if ledger_df.empty:
                ledger_df = pd.DataFrame(columns=LEDGER_HEADERS)

            # 余計な列が混ざっていても壊れないように必要列だけ確保
            for h in LEDGER_HEADERS:
                if h not in ledger_df.columns:
                    ledger_df[h] = ""

            ledger_df = pd.concat([ledger_df, pd.DataFrame([new_row])], ignore_index=True)
            df_to_ws(ledger_ws, ledger_df)

            # 個人通知（紐付け済みのみ）
            if line_user_id.startswith("U"):
                token = st.secrets["line"]["channel_access_token"]
                sign = "➕" if tx_type in ["入金", "収益"] else "➖"
                title = "💰 入金通知" if tx_type == "入金" else ("📈 収益反映" if tx_type == "収益" else "💸 出金通知")

                msg = f"{title}\n"
                if line_user_name:
                    msg += f"LINE名: {line_user_name}\n"
                msg += f"メンバー: {selected_member}\n"
                msg += f"日時(JST): {dt.strftime('%Y/%m/%d %H:%M')}\n"
                msg += f"内容: {tx_type} {sign}${amt:,.2f}\n"
                msg += f"残高: ${new_balance:,.2f}\n"
                if note:
                    msg += f"備考: {note}\n"

                code = send_line(token, line_user_id, msg)
                if code == 200:
                    st.success("保存しました。個人LINEへ通知しました。")
                else:
                    st.warning(f"保存しましたが、LINE送信に失敗しました（HTTP {code}）。")
            else:
                st.warning("保存しましたが、このメンバーはLINE未紐付けのため通知しませんでした。")

            st.rerun()

    # =========================================================
    # 3) 管理（管理者のみ）
    # - Members 管理（人数±/メンバー追加）
    # - LineIDログ（未紐付け）→ Membersへ紐付け
    # =========================================================
    with tab_admin:
        st.subheader("⚙️ 管理（管理者のみ）")
        admin_login_ui()

        if not is_admin():
            st.stop()

        st.divider()

        # --- Members 管理 ---
        st.markdown("### 👤 メンバー管理（Membersシート）")

        mdf = members_df.copy()
        for c in MEMBERS_HEADERS:
            if c not in mdf.columns:
                mdf[c] = ""

        with st.expander("Membersを表示（編集は下のフォーム推奨）", expanded=False):
            st.dataframe(mdf, use_container_width=True)

        st.markdown("#### メンバー追加")
        c1, c2 = st.columns(2)
        with c1:
            new_member_name = st.text_input("MemberName（表示名）", value="")
        with c2:
            new_sheet_name = st.text_input("SheetName（個人台帳シート名）", value="")

        if st.button("メンバーを追加（LINE未紐付け）"):
            if not new_member_name.strip() or not new_sheet_name.strip():
                st.error("MemberName と SheetName は必須です。")
                st.stop()

            # 既存チェック（MemberName重複は避ける）
            if (mdf["MemberName"].astype(str) == new_member_name.strip()).any():
                st.error("同じMemberNameが既に存在します。")
                st.stop()

            dt = now_jst()
            add = {
                "MemberName": new_member_name.strip(),
                "SheetName": new_sheet_name.strip(),
                "Line_User_ID": "",
                "Line_User": "",
                "LinkedAt": "",
                "Status": "unlinked"
            }
            mdf2 = pd.concat([mdf, pd.DataFrame([add])], ignore_index=True)
            df_to_ws(members_ws, mdf2)
            st.success("Membersに追加しました。")
            st.rerun()

        st.divider()

        # --- LINE紐付け（LineIDログ → Members） ---
        st.markdown("### 🔗 LINE紐付け（LineIDログ → Members）")
        st.caption("Makeが記録したLineIDログから、未紐付けのLINEユーザーを選び、Membersのメンバーに紐付けます。")

        # 最新の読み直し
        lineid_df = clean_cols(ws_to_df(lineid_ws))
        members_df = clean_cols(ws_to_df(members_ws))

        if lineid_df.empty:
            st.warning("LineIDシートが空です（Makeの自動取得がまだ記録されていません）。")
            st.stop()

        # 列名ゆらぎ対策
        for col in ["Line_User_ID", "Line_User"]:
            if col not in lineid_df.columns:
                st.error(f"LineIDシートに列 {col} がありません。ヘッダーを確認してください。")
                st.stop()

        # 未紐付け候補抽出：LineIDに存在するがMembersに未登録のもの
        existing_ids = set(only_line_ids(members_df.get("Line_User_ID", pd.Series(dtype=str)).dropna().tolist())) if not members_df.empty else set()
        candidates = []
        for _, r in lineid_df.iterrows():
            uid = str(r.get("Line_User_ID", "")).strip()
            uname = str(r.get("Line_User", "")).strip()
            if uid.startswith("U") and uid not in existing_ids:
                candidates.append((uid, uname))

        # 重複除去
        seen = set()
        uniq_candidates = []
        for uid, uname in candidates:
            if uid not in seen:
                seen.add(uid)
                uniq_candidates.append((uid, uname))

        if not uniq_candidates:
            st.success("未紐付けのLINEユーザーはありません。")
            st.stop()

        # 選択UI
        option_labels = [f"{uname} ({uid})" if uname else uid for uid, uname in uniq_candidates]
        sel = st.selectbox("未紐付けLINEユーザー", option_labels)
        sel_idx = option_labels.index(sel)
        sel_uid, sel_uname = uniq_candidates[sel_idx]

        # 紐付け先メンバー（unlinkedも含め全部）
        if members_df.empty:
            st.error("Membersが空です。先にメンバーを追加してください。")
            st.stop()

        member_options = members_df["MemberName"].astype(str).tolist()
        link_to = st.selectbox("紐付けるメンバー", member_options)

        if st.button("紐付け確定（Membersを更新）"):
            mdf = members_df.copy()
            # 対象行
            idxs = mdf.index[mdf["MemberName"].astype(str) == str(link_to)].tolist()
            if not idxs:
                st.error("対象メンバーが見つかりません。")
                st.stop()
            idx = idxs[0]

            dt = now_jst()
            mdf.loc[idx, "Line_User_ID"] = sel_uid
            mdf.loc[idx, "Line_User"] = sel_uname
            mdf.loc[idx, "LinkedAt"] = fmt_dt(dt)
            mdf.loc[idx, "Status"] = "linked"

            df_to_ws(members_ws, mdf)
            st.success(f"{link_to} に {sel_uid} を紐付けました。")
            st.rerun()

except Exception as e:
    st.error(f"システムエラー: {e}")
