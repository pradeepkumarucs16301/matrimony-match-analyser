"""
app.py — Databricks App (Gradio UI)
=====================================
Thillai Matrimony Match Analyser

Production-ready Databricks App version.
     - Uses the app service principal with OAuth
     - Uses the SQL warehouse configured as a Databricks App resource
     - Loads the complete Gold dataset once into shared memory
     - Logs the final total profile count explicitly
"""

import gradio as gr
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import threading
import requests
import json
import logging
import time
from datetime import datetime
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
import base64
from auth import init_sql_helpers, handle_signup, handle_otp_verify, handle_login, log_audit_event

# ── Logging Setup ─────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "[%(asctime)s] %(levelname)s — %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("matrimony_app")

# ── Connection ────────────────────────────────────────────────────────────
# ── Databricks connection configuration ───────────────────────────────────
# The SQL warehouse must be added to the Databricks App as a resource with
# resource key: sql-warehouse
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()

if not DATABRICKS_WAREHOUSE_ID:
    raise RuntimeError(
        "DATABRICKS_WAREHOUSE_ID is not set. "
        "Add a SQL warehouse resource to the Databricks App and map it in app.yaml."
    )

_cfg = Config()
DATABRICKS_HOST = _cfg.host

# ── Workspace client for downloading UC Volume images ─────────────────────
_w = WorkspaceClient()

def get_connection():
    """Create an OAuth-authenticated Databricks SQL connection.

    Databricks Apps inject the app service-principal credentials into the
    runtime. Config() picks them up through Databricks unified authentication.
    """
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        credentials_provider=lambda: _cfg.authenticate,
    )

# ── Tables ────────────────────────────────────────────────────────────────
GOLD_TABLE   = "matrimony.default.gold_profiles"
COMPAT_TABLE = "matrimony.default.star_compatibility"

log.info("=" * 60)
log.info("  Match Analyser — Starting")
log.info("=" * 60)
log.info(f"HOST         : {DATABRICKS_HOST}")
log.info(f"WAREHOUSE_ID : {DATABRICKS_WAREHOUSE_ID}")
log.info(f"GOLD_TABLE   : {GOLD_TABLE}")
log.info(f"COMPAT_TABLE : {COMPAT_TABLE}")
log.info("AUTH         : Databricks App service principal / OAuth ✅")

# ── Image cache + helper ─────────────────────────────────────────────────
_image_cache = {}

def get_profile_image_html(image_volume_path):
    """Download image from a UC Volume and return a base64-encoded <img> tag."""
    if not image_volume_path:
        return ""
    cached = _image_cache.get(image_volume_path)
    if cached is not None:
        return cached
    try:
        resp = _w.files.download(file_path=image_volume_path)
        image_bytes = resp.contents.read()
        b64 = base64.b64encode(image_bytes).decode("utf-8")
        html = (
            f'<img src="data:image/jpeg;base64,{b64}" '
            f'style="max-width:250px; max-height:350px; border-radius:8px; '
            f'box-shadow:0 2px 8px rgba(0,0,0,0.15);" />'
        )
        _image_cache[image_volume_path] = html
        log.info(f"[IMAGE] ✅ Downloaded {image_volume_path} ({len(image_bytes):,} bytes)")
        return html
    except Exception as e:
        log.warning(f"[IMAGE] ❌ Failed to download {image_volume_path}: {e}")
        _image_cache[image_volume_path] = ""
        return ""

# ── Tamil labels ──────────────────────────────────────────────────────────
STAR_TAMIL = {
    "ASWINI":"அஸ்வினி","BHARANI":"பரணி","KRITHIGAI":"கிருத்திகை",
    "ROHINI":"ரோகிணி","MIRUGASEERUSHAM":"மிருகசீரிடம்",
    "THIRUVADHIRAI":"திருவாதிரை","PUNARPOOSAM":"புனர்பூசம்",
    "POOSAM":"பூசம்","AYILYAM":"ஆயில்யம்","MAGAM":"மகம்",
    "POORAM":"பூரம்","UTHIRAM":"உத்திரம்","HASTHAM":"ஹஸ்தம்",
    "CHITHIRAI":"சித்திரை","SWATHI":"சுவாதி","VISAKAM":"விசாகம்",
    "ANUSHAM":"அனுஷம்","KETTAI":"கேட்டை","MOOLAM":"மூலம்",
    "POORADAM":"பூராடம்","UTHIRADAM":"உத்திராடம்",
    "THIRUVONAM":"திருவோணம்","AVITTAM":"அவிட்டம்",
    "SADHAYAM":"சதயம்","POORATTATHI":"பூரட்டாதி",
    "UTTHIRATTATHI":"உத்திரட்டாதி","REVATHI":"ரேவதி",
    "CHITRAI":"சித்திரை","BARANI":"பரணி",
    "MIRUGASIRISHAM":"மிருகசீரிடம்",
}
RASI_TAMIL = {
    "MESHAM":"மேஷம்","RISHABAM":"ரிஷபம்","MITHUNAM":"மிதுனம்",
    "KATAKAM":"கடகம்","SIMMAM":"சிம்மம்","KANNI":"கன்னி",
    "THULAM":"துலாம்","VRICHIKA":"விருச்சிகம்","DHANUSU":"தனுசு",
    "MAGARAM":"மகரம்","KUMBHAM":"கும்பம்","MEENAM":"மீனம்",
}


# ╔══════════════════════════════════════════════════════╗
# ║     DATABRICKS STATEMENT EXECUTION API               ║
# ║     Faster than SQL Connector — no cold start        ║
# ╚══════════════════════════════════════════════════════╝

def run_query(query_sql, max_attempts=3):
    """Execute SQL and return a Pandas DataFrame with retry handling."""
    short_sql = " ".join(query_sql.strip().split())[:120]
    last_error = None

    for attempt in range(1, max_attempts + 1):
        t0 = time.time()
        conn = None
        cursor = None
        try:
            log.info(f"[SQL] Attempt {attempt}/{max_attempts} | {short_sql}...")
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(query_sql)

            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            df = pd.DataFrame(rows, columns=columns)

            elapsed = round(time.time() - t0, 1)
            log.info(
                f"[SQL] SUCCEEDED | rows={len(df)} | cols={len(df.columns)} | time={elapsed}s"
            )
            return df

        except Exception as e:
            last_error = e
            elapsed = round(time.time() - t0, 1)
            log.error(
                f"[SQL] FAILED | attempt={attempt}/{max_attempts} | "
                f"time={elapsed}s | error={e}"
            )
            if attempt < max_attempts:
                time.sleep(min(2 * attempt, 5))

        finally:
            try:
                if cursor:
                    cursor.close()
            except Exception:
                pass
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    raise RuntimeError(f"SQL query failed after {max_attempts} attempts: {last_error}")

def execute_dml(query_sql, params=None, max_attempts=3):
    """Execute INSERT/UPDATE/DELETE SQL. Returns True on success."""
    short_sql = " ".join(query_sql.strip().split())[:120]
    for attempt in range(1, max_attempts + 1):
        conn = None
        cursor = None
        try:
            log.info(f"[DML] Attempt {attempt}/{max_attempts} | {short_sql}...")
            conn = get_connection()
            cursor = conn.cursor()
            if params:
                cursor.execute(query_sql, params)
            else:
                cursor.execute(query_sql)
            conn.commit()
            log.info(f"[DML] SUCCEEDED")
            return True
        except Exception as e:
            log.error(f"[DML] FAILED | attempt={attempt} | error={e}")
            if attempt < max_attempts:
                time.sleep(min(2 * attempt, 5))
        finally:
            try:
                if cursor: cursor.close()
            except Exception: pass
            try:
                if conn: conn.close()
            except Exception: pass
    return False

# Initialize auth module with SQL helpers
init_sql_helpers(run_query, execute_dml)




# ╔══════════════════════════════════════════════════════╗
# ║         GLOBAL IN-MEMORY CACHE                       ║
# ╚══════════════════════════════════════════════════════╝

_cache = {
    "gold"  : pd.DataFrame(),
    "compat": pd.DataFrame(),
    "status": "⏳ Loading data in background...",
    "loaded": False,
    "lock"  : threading.Lock(),
}


def _load_data_background():
    """Load Gold profiles + compatibility data once and share them across users."""
    global _cache
    log.info("[LOADER] ============================================================")
    log.info("[LOADER] BACKGROUND DATA LOAD STARTED")
    log.info("[LOADER] ============================================================")
    t_start = time.time()

    try:
        # Step 1: Load the complete Gold table. SELECT * is intentional here:
        # it avoids breaking the app when the Gold schema gains a new column.
        _cache["status"] = "⏳ Step 1/2 — Loading all profiles from Gold..."
        log.info("[LOADER] Step 1/2 — Loading ALL profiles from Gold table")
        df_gold = run_query(f"SELECT * FROM {GOLD_TABLE}")
        df_gold.columns = [str(c).lower() for c in df_gold.columns]

        if "reg_no" not in df_gold.columns:
            raise RuntimeError("Gold table does not contain required column: reg_no")

        if "gender" not in df_gold.columns:
            df_gold["gender"] = df_gold["reg_no"].astype(str).str.upper().str[0].map({
                "F": "Female",
                "M": "Male",
            }).fillna("Unknown")

        if "birth_year" in df_gold.columns:
            df_gold["birth_year"] = pd.to_numeric(df_gold["birth_year"], errors="coerce")

        # Normalize commonly filtered string columns so .str operations are safe.
        for column in ["reg_no", "name", "sub_caste", "star", "rasi"]:
            if column in df_gold.columns:
                df_gold[column] = df_gold[column].fillna("").astype(str).str.strip()

        total_profiles = len(df_gold)
        female_count = int((df_gold["gender"] == "Female").sum())
        male_count = int((df_gold["gender"] == "Male").sum())
        unknown_count = total_profiles - female_count - male_count

        log.info("[LOADER] ------------------------------------------------------------")
        log.info(f"[LOADER] TOTAL PROFILES LOADED : {total_profiles}")
        log.info(f"[LOADER] Female profiles       : {female_count}")
        log.info(f"[LOADER] Male profiles         : {male_count}")
        log.info(f"[LOADER] Unknown gender       : {unknown_count}")
        log.info(f"[LOADER] Gold columns         : {len(df_gold.columns)}")
        log.info("[LOADER] ------------------------------------------------------------")

        if total_profiles == 0:
            raise RuntimeError(
                f"Gold table {GOLD_TABLE} returned 0 profiles. "
                "Check the Gold transformation before starting the App."
            )

        # Step 2: Compatibility table
        _cache["status"] = (
            f"⏳ Step 2/2 — Loading compatibility table | {total_profiles:,} profiles loaded..."
        )
        log.info("[LOADER] Step 2/2 — Loading compatibility table")
        df_compat = run_query(f"SELECT * FROM {COMPAT_TABLE}")
        df_compat.columns = [str(c).lower() for c in df_compat.columns]
        log.info(f"[LOADER] Compatibility rows loaded: {len(df_compat)}")

        total_time = round(time.time() - t_start, 1)
        with _cache["lock"]:
            _cache["gold"] = df_gold
            _cache["compat"] = df_compat
            _cache["loaded"] = True
            _cache["status"] = (
                f"✅ **{total_profiles:,} profiles loaded** | "
                f"Female: {female_count:,} | Male: {male_count:,} | "
                f"Ready for all users 🎉"
            )

        log.info("[LOADER] ============================================================")
        log.info(f"[LOADER] ✅ TOTAL PROFILES READY FOR APP: {total_profiles}")
        log.info(f"[LOADER]    Female: {female_count} | Male: {male_count}")
        log.info(f"[LOADER]    Compatibility rows: {len(df_compat)}")
        log.info(f"[LOADER]    Load time: {total_time}s")
        log.info("[LOADER] ✅ ALL DATA LOADED — APP READY")
        log.info("[LOADER] ============================================================")

    except Exception as e:
        with _cache["lock"]:
            _cache["loaded"] = False
            _cache["status"] = f"❌ Load failed: {e}"
        log.error(f"[LOADER] ❌ BACKGROUND LOAD FAILED: {e}", exc_info=True)


# Start background load
_thread = threading.Thread(target=_load_data_background, daemon=True)
_thread.start()
log.info("[STARTUP] ✅ App started — data loading in background thread")


# ╔══════════════════════════════════════════════════════╗
# ║              FILTER + DISPLAY LOGIC                  ║
# ╚══════════════════════════════════════════════════════╝

def get_data_status():
    return _cache["status"]


def apply_filters(year_from, year_to, gender, sub_caste, stars, rasis, exclude_ids):
    log.info(f"[FILTER] Request — year={year_from}-{year_to} | gender={gender} | caste={sub_caste} | stars={stars} | rasis={rasis}")

    if not _cache["loaded"]:
        log.warning("[FILTER] Cache not loaded yet — returning empty")
        return pd.DataFrame(), "⏳ Data still loading — please wait..."

    t0 = time.time()
    df = _cache["gold"].copy()
    log.info(f"[FILTER] Starting with {len(df)} total profiles")

    # Gender filter — show opposite gender profiles
    # If user is Male → show Female profiles (he's looking for a bride)
    # If user is Female → show Male profiles (she's looking for a groom)
    if gender == "Male (Looking for Bride)":
        df = df[df["gender"] == "Female"]
        log.info(f"[FILTER] Male user → showing Female profiles: {len(df)}")
    elif gender == "Female (Looking for Groom)":
        df = df[df["gender"] == "Male"]
        log.info(f"[FILTER] Female user → showing Male profiles: {len(df)}")

    df = df[df["birth_year"].between(int(year_from), int(year_to))]
    log.info(f"[FILTER] After year filter: {len(df)} profiles")

    if sub_caste and sub_caste != "All":
        df = df[df["sub_caste"].str.upper().str.strip() == sub_caste.upper().strip()]
        log.info(f"[FILTER] After caste filter: {len(df)} profiles")
    if stars:
        df = df[df["star"].isin(stars)]
        log.info(f"[FILTER] After star filter: {len(df)} profiles")
    if rasis:
        df = df[df["rasi"].isin(rasis)]
        log.info(f"[FILTER] After rasi filter: {len(df)} profiles")
    if exclude_ids:
        ids = [x.strip().upper() for x in exclude_ids.split(",") if x.strip()]
        if ids:
            df = df[~df["reg_no"].isin(ids)]
            log.info(f"[FILTER] After exclude filter: {len(df)} profiles")

    elapsed = round(time.time() - t0, 3)
    log.info(f"[FILTER] ✅ Result: {len(df)} profiles in {elapsed}s")
    return df.reset_index(drop=True), f"✅ **{len(df)} profiles** matched."


def format_table(df):
    cols = ["reg_no","name","dob","birth_year","star","rasi",
            "sub_caste","qualification","job"]
    out  = df[[c for c in cols if c in df.columns]].copy()
    out["star_tamil"] = out["star"].map(STAR_TAMIL).fillna("")
    out["rasi_tamil"] = out["rasi"].map(RASI_TAMIL).fillna("")
    out.index = range(1, len(out)+1)
    return out


def run_filter(year_from, year_to, gender, sub_caste, stars, rasis, exclude_ids):
    df, summary = apply_filters(
        year_from, year_to, gender, sub_caste, stars, rasis, exclude_ids
    )
    return summary, format_table(df)


# ╔══════════════════════════════════════════════════════╗
# ║           COMPATIBILITY ENGINE                       ║
# ╚══════════════════════════════════════════════════════╝

def get_compatibility_info(user_gender, user_star):
    log.info(f"[COMPAT] Request — gender={user_gender} | star={user_star}")
    if not _cache["loaded"]:
        log.warning("[COMPAT] Cache not loaded yet")
        return "⏳ Data still loading...", pd.DataFrame(), pd.DataFrame()
    if not user_star:
        return "Please select your star.", pd.DataFrame(), pd.DataFrame()

    user_star = user_star.upper()
    df_compat = _cache["compat"]
    df_gold   = _cache["gold"]

    # Gender-aware compatibility:
    # Male user   → his star is "boy_star" → find matching "girl_star" profiles
    # Female user → her star is "girl_star" → find matching "boy_star" profiles
    is_male = "Male" in user_gender

    if is_male:
        # User is male → find girl stars that match his boy star
        utthamam_matches = df_compat[
            (df_compat["boy_star"] == user_star) &
            (df_compat["match_level"] == "UTTHAMAM")
        ]["girl_star"].tolist()
        madhyamam_matches = df_compat[
            (df_compat["boy_star"] == user_star) &
            (df_compat["match_level"] == "MADHYAMAM")
        ]["girl_star"].tolist()
        target_gender = "Female"
        label = f"🧑 {user_star} (Boy's star)"
    else:
        # User is female → find boy stars that match her girl star
        utthamam_matches = df_compat[
            (df_compat["girl_star"] == user_star) &
            (df_compat["match_level"] == "UTTHAMAM")
        ]["boy_star"].tolist()
        madhyamam_matches = df_compat[
            (df_compat["girl_star"] == user_star) &
            (df_compat["match_level"] == "MADHYAMAM")
        ]["boy_star"].tolist()
        target_gender = "Male"
        label = f"👩 {user_star} (Girl's star)"

    ut_tamil = [f"{s} ({STAR_TAMIL.get(s,s)})" for s in utthamam_matches]
    md_tamil = [f"{s} ({STAR_TAMIL.get(s,s)})" for s in madhyamam_matches]

    summary = f"""## ⭐ Compatibility for {label} ({STAR_TAMIL.get(user_star,'')})
**Showing {target_gender} profiles that match your star**

### 🥇 Utthamam (Best Match) — {len(utthamam_matches)} stars
{', '.join(ut_tamil) or 'None found'}

### 🥈 Madhyamam (Good Match) — {len(madhyamam_matches)} stars
{', '.join(md_tamil) or 'None found'}
"""
    cols       = ["reg_no","name","dob","birth_year","star","rasi",
                  "sub_caste","qualification","gender"]
    cols_avail = [c for c in cols if c in df_gold.columns]

    # Filter by opposite gender
    df_target = df_gold[df_gold["gender"] == target_gender]
    df_best = df_target[df_target["star"].isin(utthamam_matches)][cols_avail].reset_index(drop=True)
    df_good = df_target[df_target["star"].isin(madhyamam_matches)][cols_avail].reset_index(drop=True)
    df_best.index = range(1, len(df_best)+1)
    df_good.index = range(1, len(df_good)+1)
    log.info(f"[COMPAT] ✅ Utthamam={len(df_best)} | Madhyamam={len(df_good)} {target_gender} profiles")

    return summary, df_best, df_good


# ╔══════════════════════════════════════════════════════╗
# ║              CHARTS                                  ║
# ╚══════════════════════════════════════════════════════╝

def make_charts(year_from, year_to, gender, sub_caste, stars, rasis, exclude_ids):
    df, _ = apply_filters(year_from, year_to, gender, sub_caste, stars, rasis, exclude_ids)
    if df.empty:
        empty = go.Figure()
        empty.add_annotation(
            text="No data — adjust filters or wait for load",
            xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(size=14)
        )
        return empty, empty, empty, empty

    star_counts = df["star"].replace("", pd.NA).dropna().value_counts().reset_index()
    star_counts.columns = ["Star", "Count"]
    fig_star = px.bar(
        star_counts, x="Star", y="Count",
        title=f"⭐ Star Distribution ({len(df)} profiles)",
        text="Count"
    )
    fig_star.update_traces(textposition="outside")
    fig_star.update_layout(showlegend=False, xaxis_tickangle=-30)

    rasi_counts = df["rasi"].replace("", pd.NA).dropna().value_counts().reset_index()
    rasi_counts.columns = ["Rasi", "Count"]
    fig_rasi = px.pie(
        rasi_counts, names="Rasi", values="Count",
        title="🪐 Rasi Distribution"
    )
    fig_rasi.update_traces(textposition="inside", textinfo="label+percent")

    year_counts = df["birth_year"].dropna().value_counts().sort_index().reset_index()
    year_counts.columns = ["Year", "Count"]
    fig_year = px.bar(
        year_counts, x="Year", y="Count",
        title="📅 Birth Year Distribution", text="Count"
    )
    fig_year.update_traces(textposition="outside")
    fig_year.update_layout(showlegend=False)

    gender_counts = df["gender"].value_counts().reset_index()
    gender_counts.columns = ["Gender", "Count"]
    fig_gender = px.pie(
        gender_counts, names="Gender", values="Count",
        title="👤 Gender Distribution"
    )
    fig_gender.update_traces(textposition="inside", textinfo="label+percent")

    return fig_star, fig_rasi, fig_year, fig_gender


# ╔══════════════════════════════════════════════════════╗
# ║           PROFILE DETAIL VIEW                        ║
# ╚══════════════════════════════════════════════════════╝

def get_profile_detail(reg_no):
    log.info(f"[PROFILE] Detail request for reg_no={reg_no}")
    if not reg_no:
        return "Enter a Reg No (e.g. F13073) and click View Profile.", ""
    if not _cache["loaded"]:
        log.warning("[PROFILE] Cache not loaded yet")
        return "⏳ Data still loading — please wait...", ""

    row = _cache["gold"][
        _cache["gold"]["reg_no"].str.upper() == reg_no.upper().strip()
    ]
    if row.empty:
        log.warning(f"[PROFILE] reg_no={reg_no} not found in cache")
        return f"❌ Profile {reg_no} not found.", ""
    log.info(f"[PROFILE] ✅ Found profile for {reg_no}")

    r         = row.iloc[0]

    # Download profile image from UC Volume
    image_path = r.get("image_volume_path", "") if "image_volume_path" in _cache["gold"].columns else ""
    if not image_path:
        # Fallback: construct path from reg_no and gender
        gender_folder = "female" if r.get("gender", "") == "Female" else "male"
        image_path = f"/Volumes/matrimony/default/profile_images/{gender_folder}/{reg_no.upper().strip()}.jpg"
    image_html = get_profile_image_html(image_path)

    star      = r.get("star","")
    rasi      = r.get("rasi","")
    best      = r.get("best_match_stars","")
    good      = r.get("good_match_stars","")
    best_disp = ", ".join([f"{s} ({STAR_TAMIL.get(s,s)})"
                           for s in str(best).split(",") if s.strip()]) if best else "—"
    good_disp = ", ".join([f"{s} ({STAR_TAMIL.get(s,s)})"
                           for s in str(good).split(",") if s.strip()]) if good else "—"

    return f"""
## 👤 {r.get('name','')}

### 📋 Basic Info
| Field | Details |
|-------|---------|
| **Reg No** | {r.get('reg_no','')} |
| **DOB** | {r.get('dob','')} |
| **Birth Year** | {r.get('birth_year','')} |
| **Birth Place** | {r.get('birth_place','')} |
| **Star** | {star} ({STAR_TAMIL.get(star,'')}) |
| **Rasi** | {rasi} ({RASI_TAMIL.get(rasi,'')}) |
| **Caste** | {r.get('caste','')} |
| **Sub Caste** | {r.get('sub_caste','')} |
| **Marital Status** | {r.get('marital_status','')} |
| **Diet** | {r.get('diet','')} |

### 🎓 Education & Career
| Field | Details |
|-------|---------|
| **Qualification** | {r.get('qualification','')} |
| **Job** | {r.get('job','')} |
| **Income / Height / Complexion** | {r.get('income_month_height_complexion','')} |
| **Own House / Nativity** | {r.get('own_house_nativity','')} |

### 📞 Contact
| Field | Details |
|-------|---------|
| **Contact Person** | {r.get('contact_person','')} |
| **Contact No** | {r.get('contact_no','')} |
| **Email** | {r.get('email_id','—')} |

---
### ⭐ Star Compatibility
| Level | Stars |
|-------|-------|
| 🥇 **Utthamam** | {best_disp} |
| 🥈 **Madhyamam** | {good_disp} |
""", image_html


def refresh_data():
    log.info("[REFRESH] Manual data refresh triggered")
    global _cache
    with _cache["lock"]:
        _cache["loaded"] = False
        _cache["gold"]   = pd.DataFrame()
        _cache["compat"] = pd.DataFrame()
        _cache["status"] = "⏳ Refreshing data..."
    t = threading.Thread(target=_load_data_background, daemon=True)
    t.start()
    log.info("[REFRESH] Background refresh thread started")
    return "🔄 Refresh started in background..."


# ╔══════════════════════════════════════════════════════╗
# ║              DROPDOWN OPTIONS                        ║
# ╚══════════════════════════════════════════════════════╝

all_castes = [
    "All","SENGUNTHAR / KAIKOLAR",
    "AGAMUDAYAR / ARCOT / THOLIL MUDALIAR",
    "MUDALIAR","VANNIYA KULA KSHATRIYAR",
    "NADAR","GOUNDER","CHETTIAR","PILLAI","THEVAR / MUKKULATHOR",
]
all_stars = [
    "ASWINI","BHARANI","KRITHIGAI","ROHINI","MIRUGASEERUSHAM",
    "THIRUVADHIRAI","PUNARPOOSAM","POOSAM","AYILYAM","MAGAM",
    "POORAM","UTHIRAM","HASTHAM","CHITHIRAI","SWATHI","VISAKAM",
    "ANUSHAM","KETTAI","MOOLAM","POORADAM","UTHIRADAM","THIRUVONAM",
    "AVITTAM","SADHAYAM","POORATTATHI","UTTHIRATTATHI","REVATHI",
]
all_rasis = [
    "MESHAM","RISHABAM","MITHUNAM","KATAKAM","SIMMAM","KANNI",
    "THULAM","VRICHIKA","DHANUSU","MAGARAM","KUMBHAM","MEENAM",
]




# ╔════════════════════════════════════════════════════════╗═
# ║                  GRADIO UI                           ║
# ╚═════════════════════════════════════════════════════════╝

with gr.Blocks(
    title="Match Analyser",
    theme=gr.themes.Soft(
        primary_hue="pink",
        secondary_hue="rose",
        neutral_hue="slate"
    ),
    css="""
        .gradio-container {
            background: linear-gradient(135deg, #ffeef8 0%, #fff0f5 100%);
        }
        h1, h2, h3 {
            color: #c2185b !important;
        }
        .primary {
            background: linear-gradient(90deg, #ec407a 0%, #f06292 100%) !important;
        }
    """
) as app:

    logged_in_user = gr.State(None)

    # ── LOGIN / SIGNUP SECTION ────────────────────────────────────────
    with gr.Group(visible=True) as login_section:
        gr.Markdown("""
        # Match Analyser
        ### Please login or sign up to access the app
        """)
        with gr.Tabs():
            with gr.Tab("Login"):
                login_email = gr.Textbox(label="Email", placeholder="your@email.com")
                login_password = gr.Textbox(label="Password", type="password")
                login_btn = gr.Button("Login", variant="primary")
                login_msg = gr.Markdown("")

            with gr.Tab("Sign Up"):
                signup_name = gr.Textbox(label="Full Name", placeholder="Your name")
                signup_email = gr.Textbox(label="Email", placeholder="your@email.com")
                signup_password = gr.Textbox(label="Password", type="password", placeholder="Min 6 characters")
                signup_btn = gr.Button("Sign Up & Send OTP", variant="primary")
                signup_msg = gr.Markdown("")

            with gr.Tab("Verify OTP"):
                otp_email = gr.Textbox(label="Email", placeholder="your@email.com")
                otp_code = gr.Textbox(label="6-Digit OTP Code", placeholder="e.g. 123456")
                otp_btn = gr.Button("Verify OTP", variant="primary")
                otp_msg = gr.Markdown("")

    # ── MAIN APP SECTION ───────────────────────────────────────────
    with gr.Group(visible=False) as main_section:
        gr.Markdown("""
        # Match Analyser
        """)
        with gr.Row():
            status_display = gr.Markdown(_cache["status"])
            logout_btn     = gr.Button("Logout", size="sm", scale=0, variant="stop")

        with gr.Accordion("Filters", open=True):
            with gr.Row():
                gender_sel = gr.Radio(
                    choices=["Male (Looking for Bride)", "Female (Looking for Groom)"],
                    value="Male (Looking for Bride)",
                    label="I am a...",
                    info="Select your gender — app shows opposite gender profiles"
                )
            with gr.Row():
                year_from = gr.Slider(1990, 2005, value=2001, step=1, label="Birth Year From")
                year_to   = gr.Slider(1990, 2005, value=2005, step=1, label="Birth Year To")
            with gr.Row():
                sub_caste  = gr.Dropdown(all_castes, value="SENGUNTHAR / KAIKOLAR", label="Sub Caste")
                stars_sel  = gr.Dropdown(all_stars, multiselect=True,
                                         value=["SWATHI","POORADAM","AVITTAM","ANUSHAM","UTHIRAM","HASTHAM"],
                                         label="Preferred Stars (of the match)")
                rasis_sel  = gr.Dropdown(all_rasis, multiselect=True,
                                         value=["MESHAM","SIMMAM","THULAM"],
                                         label="Preferred Rasi (of the match)")
            with gr.Row():
                exclude_ids = gr.Textbox(label="Exclude Profile IDs (comma separated)",
                                         placeholder="e.g. F12836, F12699",
                                         value="F12836, F12699, F12143, F11105, F09868")
                filter_btn = gr.Button("Apply Filters", variant="primary")

        with gr.Tabs():
            with gr.Tab("Profiles"):
                filter_summary = gr.Markdown("Click **Apply Filters** to see results.")
                profiles_table = gr.DataFrame(interactive=False, wrap=True)
                gr.Markdown("### View Full Profile")
                with gr.Row():
                    reg_input  = gr.Textbox(label="Reg No", placeholder="e.g. F13073")
                    view_btn   = gr.Button("View Profile", variant="secondary")
                with gr.Row():
                    with gr.Column(scale=1, min_width=280):
                        profile_image = gr.HTML("", label="Photo")
                    with gr.Column(scale=2):
                        profile_detail = gr.Markdown("")
                filter_btn.click(
                    run_filter,
                    inputs=[year_from, year_to, gender_sel, sub_caste, stars_sel, rasis_sel, exclude_ids],
                    outputs=[filter_summary, profiles_table]
                )
                view_btn.click(get_profile_detail, inputs=reg_input, outputs=[profile_detail, profile_image])

            with gr.Tab("Compatibility"):
                gr.Markdown("""
                ### Find your best matching profiles by Star
                Based on traditional Tamil **Thirumana Porutham**
                - **Male user** -> enter your star -> see matching Female profiles
                - **Female user** -> enter your star -> see matching Male profiles
                """)
                with gr.Row():
                    compat_gender = gr.Radio(
                        choices=["Male (Looking for Bride)", "Female (Looking for Groom)"],
                        value="Male (Looking for Bride)", label="I am a...")
                    user_star_sel = gr.Dropdown(all_stars, label="My Star", value="SWATHI")
                    compat_btn    = gr.Button("Find Matches", variant="primary")
                compat_summary = gr.Markdown("")
                with gr.Row():
                    with gr.Column():
                        gr.Markdown("### Utthamam (Best Match) Profiles")
                        best_table = gr.DataFrame(interactive=False, wrap=True)
                    with gr.Column():
                        gr.Markdown("### Madhyamam (Good Match) Profiles")
                        good_table = gr.DataFrame(interactive=False, wrap=True)
                compat_btn.click(
                    get_compatibility_info,
                    inputs=[compat_gender, user_star_sel],
                    outputs=[compat_summary, best_table, good_table]
                )

            with gr.Tab("Charts"):
                charts_btn = gr.Button("Generate Charts", variant="primary")
                with gr.Row():
                    chart_star = gr.Plot(label="Star Distribution")
                    chart_rasi = gr.Plot(label="Rasi Distribution")
                with gr.Row():
                    chart_year = gr.Plot(label="Birth Year")
                    chart_gender = gr.Plot(label="Gender Distribution")
                charts_btn.click(
                    make_charts,
                    inputs=[year_from, year_to, gender_sel, sub_caste, stars_sel, rasis_sel, exclude_ids],
                    outputs=[chart_star, chart_rasi, chart_year, chart_gender]
                )

            with gr.Tab("About"):
                gr.Markdown("""
                ## Match Analyser
                | Layer | Table | Description |
                |-------|-------|-------------|
                | Bronze | `matrimony.default.bronze_profiles` | Raw scraped |
                | Gold | `matrimony.default.gold_profiles` | Parsed + enriched |
                | Compat | `matrimony.default.star_compatibility` | Star matching |
                | Auth | `matrimony.default.app_users` | User accounts |
                | Audit | `matrimony.default.app_audit_logs` | Login/signup logs |
                *Built with Databricks + Delta Lake + Gradio*
                """)

    # ── EVENT HANDLERS ────────────────────────────────────────
    def do_login(email, password):
        msg, user_email = handle_login(email, password)
        if user_email:
            return msg, user_email, gr.update(visible=False), gr.update(visible=True)
        return msg, None, gr.update(visible=True), gr.update(visible=False)
    login_btn.click(do_login, inputs=[login_email, login_password],
                    outputs=[login_msg, logged_in_user, login_section, main_section])

    signup_btn.click(handle_signup, inputs=[signup_name, signup_email, signup_password],
                     outputs=[signup_msg])

    def do_otp_verify(email, otp_code):
        msg, success = handle_otp_verify(email, otp_code)
        return msg
    otp_btn.click(do_otp_verify, inputs=[otp_email, otp_code], outputs=[otp_msg])

    def do_logout(user_email):
        log_audit_event(user_email, "LOGOUT", "User logged out")
        return None, gr.update(visible=True), gr.update(visible=False)
    logout_btn.click(do_logout, inputs=[logged_in_user],
                     outputs=[logged_in_user, login_section, main_section])

app.launch(
    server_name="0.0.0.0",
    server_port=int(os.getenv("DATABRICKS_APP_PORT", "8000")),
)
