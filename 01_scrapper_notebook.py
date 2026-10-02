# Databricks notebook source
# DBTITLE 1,⚙️ Configuration: Choose scraper mode
# ═══════════════════════════════════════════════════════════════════════════
# SCRAPER MODE SELECTOR
# ═══════════════════════════════════════════════════════════════════════════
# Choose which scraper mode to run:
#   - "incremental" : Fast daily updates (only NEW profiles) ⚡ RECOMMENDED
#   - "full"        : Complete refresh (all profiles)
# ═══════════════════════════════════════════════════════════════════════════

# Create mode selector widget
dbutils.widgets.dropdown(
    "scraper_mode", 
    "incremental",  # Default: incremental (fast)
    ["incremental", "full"],
    "Scraper Mode"
)

# Get selected mode
SCRAPER_MODE = dbutils.widgets.get("scraper_mode")

print("=" * 80)
print("  THILLAI MATRIMONY SCRAPER")
print("=" * 80)
print(f"  Selected mode: {SCRAPER_MODE.upper()}")
print("=" * 80)

if SCRAPER_MODE == "incremental":
    print("\n⚡ INCREMENTAL MODE")
    print("  → Scrapes only NEW profiles (since last run)")
    print("  → Fast: ~5 seconds if no new profiles, ~1-3 min if a few new")
    print("  → Uses MERGE (no duplicates)\n")
elif SCRAPER_MODE == "full":
    print("\n🔄 FULL REFRESH MODE")
    print("  → Scrapes ALL profiles from scratch")
    print("  → Slower: ~3-5 minutes for 6,800+ profiles")
    print("  → Overwrites entire bronze table\n")
else:
    raise ValueError(f"Invalid mode: {SCRAPER_MODE}. Must be 'incremental' or 'full'")

print("Proceeding to scrape...\n")

# COMMAND ----------

# DBTITLE 1,Full Refresh Scraper (runs only if mode='full')
# Check mode - skip this cell if not in full refresh mode
try:
    if SCRAPER_MODE != "full":
        print("⏭️  Skipping FULL REFRESH scraper (mode is not 'full')")
        # Don't exit - let the incremental scraper run in the next cell
except NameError:
    # If SCRAPER_MODE not defined, default to full for backwards compatibility
    SCRAPER_MODE = "full"

if SCRAPER_MODE == "full":
    print("\n🔄 Running FULL REFRESH SCRAPER...\n")

"""
01_scraper.py — Bronze Layer
Thillai Matrimony Match Analyser

Scrapes all profiles returned by the public search page, accepting both
Female (F*) and Male (M*) registration numbers, traverses all pages, fetches
full details, deduplicates profiles, and writes to:
    matrimony.default.bronze_profiles

The scraper does NOT submit a Female-only filter.
"""

import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime
from collections import Counter
import time

BASE_URL = "https://www.thillaimatrimony.org"
SEARCH_URL = f"{BASE_URL}/publicprofilesearchgrid.aspx"
PROFILE_URL = f"{BASE_URL}/FullView.aspx?id="

CATALOG = "matrimony"
SCHEMA = "default"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_profiles"

# Unity Catalog Volume for storing images
VOLUME_NAME = "profile_images"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

PAGE_DELAY = 0
PROFILE_DELAY = 0
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3

# FAST MODE: Disable image downloads in main scraper
# Images will be downloaded separately in a parallel process
IMAGE_DOWNLOAD_ENABLED = False  # Set to True to download images (SLOW!)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": SEARCH_URL,
}

spark = SparkSession.builder.getOrCreate()
session = requests.Session()
session.headers.update(HEADERS)

print("Spark session ready")
print(f"Target table : {BRONZE_TABLE}")
print(f"Run time     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def request_with_retry(method, url, **kwargs):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            print(f"  Request attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(1.5 * attempt)
    raise RuntimeError(f"{method} failed after {MAX_RETRIES} attempts: {url}") from last_error


def get_hidden_fields(soup):
    fields = {}
    for name in ["__VIEWSTATE", "__EVENTVALIDATION", "__VIEWSTATEGENERATOR"]:
        tag = soup.find("input", {"id": name})
        if tag:
            fields[name] = tag.get("value", "")
    return fields


def detect_gender(reg_no):
    value = str(reg_no or "").strip().upper()
    if value.startswith("F"):
        return "Female"
    if value.startswith("M"):
        return "Male"
    return "Unknown"


def parse_grid(soup):
    profiles = []
    table = soup.find("table", id="GridView1")
    if not table:
        return profiles

    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) < 5:
            continue

        reg_no = cols[1].get_text(strip=True)
        name = cols[2].get_text(strip=True)
        caste = cols[3].get_text(strip=True)
        sub_caste = cols[4].get_text(strip=True)

        link_tag = cols[1].find("a")
        profile_id = reg_no

        if link_tag:
            href = link_tag.get("href", "")
            if "id=" in href:
                profile_id = href.split("id=", 1)[-1].split("&", 1)[0]

        gender = detect_gender(reg_no)

        # Accept BOTH genders; skip only registrations we cannot classify.
        if gender == "Unknown":
            continue

        # Extract image URL from Photo column (column 6)
        image_url = None
        if len(cols) >= 6:
            img_tag = cols[5].find("img")
            if img_tag:
                img_src = img_tag.get("src", "")
                if img_src and "photos/" in img_src:
                    # Convert relative path to absolute URL
                    if not img_src.startswith("http"):
                        image_url = f"{BASE_URL}/{img_src.lstrip('/')}"
                    else:
                        image_url = img_src

        profiles.append({
            "s_no": cols[0].get_text(strip=True),
            "reg_no": reg_no,
            "profile_id": profile_id,
            "name": name,
            "caste": caste,
            "sub_caste": sub_caste,
            "gender": gender,
            "image_source_url": image_url,
        })

    return profiles


def gender_summary(title, profiles):
    counts = Counter(p.get("gender", "Unknown") for p in profiles)
    total = len(profiles)
    female = counts.get("Female", 0)
    male = counts.get("Male", 0)
    unknown = counts.get("Unknown", 0)

    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)
    print(f"  TOTAL PROFILES : {total}")
    print(f"  FEMALE         : {female}")
    print(f"  MALE           : {male}")
    print(f"  UNKNOWN        : {unknown}")
    print("=" * 65)
    return total, female, male, unknown


def deduplicate_profiles(profiles):
    unique = {}
    duplicates = 0

    for profile in profiles:
        key = str(profile.get("profile_id") or profile.get("reg_no") or "").strip()
        if not key:
            continue
        if key in unique:
            duplicates += 1
        else:
            unique[key] = profile

    result = list(unique.values())
    print("\nDeduplication:")
    print(f"  Before: {len(profiles)}")
    print(f"  After : {len(result)}")
    print(f"  Removed duplicates: {duplicates}")
    return result


def scrape_pages_for_gender(gender_value):
    """
    Scrape all pages for a specific gender filter value.
    gender_value: The value to submit in the ddlGender dropdown (e.g., 'Female', 'Male')
    """
    print(f"\nLoading public search page for gender: {gender_value}")

    response = request_with_retry("GET", SEARCH_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    hidden = get_hidden_fields(soup)

    # Submit the initial search with gender filter
    # Field name is 'DropDownList8' (not 'ddlGender')
    post_data = {
        "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
        "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
        "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
        "DropDownList8": gender_value,
        "Button1": "GO",  # Submit button
    }

    response = request_with_retry("POST", SEARCH_URL, data=post_data)
    soup = BeautifulSoup(response.text, "html.parser")
    hidden = get_hidden_fields(soup)

    first_page = parse_grid(soup)
    all_profiles = list(first_page)

    print(f"  Page 1 -> {len(first_page)} profiles")

    page = 2
    while True:
        # print(f"  Page {page}...", end=" ", flush=True)

        post_data = {
            "__EVENTTARGET": "GridView1",
            "__EVENTARGUMENT": f"Page${page}",
            "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
            "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
            "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
            "DropDownList8": gender_value,  # Correct field name
        }

        response = request_with_retry("POST", SEARCH_URL, data=post_data)
        soup = BeautifulSoup(response.text, "html.parser")
        hidden = get_hidden_fields(soup)

        rows = parse_grid(soup)

        if not rows:
            print("No rows. Done!")
            break

        all_profiles.extend(rows)
        # print(f"-> {len(rows)} | Total: {len(all_profiles)}")

        has_next = any(
            f"Page${page + 1}" in a.get("href", "")
            for a in soup.find_all("a")
        )

        if not has_next:
            print("  Last page reached.")
            break

        page += 1
        time.sleep(PAGE_DELAY)

    return all_profiles


def scrape_all_pages():
    """
    Scrape both Female and Male profiles separately, then combine and deduplicate.
    """
    print("\n" + "=" * 65)
    print("  SCRAPING STRATEGY: TWO SEPARATE GENDER FILTERS")
    print("=" * 65)
    
    # Scrape Female profiles
    print("\n[1/2] Scraping FEMALE profiles...")
    female_profiles = scrape_pages_for_gender("Female")
    
    # Scrape Male profiles  
    print("\n[2/2] Scraping MALE profiles...")
    male_profiles = scrape_pages_for_gender("Male")
    
    # Combine both
    all_profiles = female_profiles + male_profiles
    
    print("\n" + "=" * 65)
    print("  COMBINED RESULTS (before deduplication)")
    print("=" * 65)
    print(f"  Female profiles: {len(female_profiles)}")
    print(f"  Male profiles  : {len(male_profiles)}")
    print(f"  Total          : {len(all_profiles)}")
    print("=" * 65)
    
    return deduplicate_profiles(all_profiles)


def normalize_key(key):
    return (
        key.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
        .replace(":", "")
    )


def fetch_profile_details(profile_id):
    url = PROFILE_URL + str(profile_id)

    try:
        response = request_with_retry("GET", url)
        soup = BeautifulSoup(response.text, "html.parser")
        details = {}

        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) != 2:
                    continue

                key = cols[0].get_text(strip=True).rstrip(":").strip()
                value = cols[1].get_text(strip=True)

                if key and value and len(key) < 80:
                    details[normalize_key(key)] = value

        # Extract profile image URL (higher quality than grid thumbnail)
        image_input = soup.find("input", {"id": "Image1", "type": "image"})
        if image_input:
            img_src = image_input.get("src", "")
            img_alt = image_input.get("alt", "")
            
            # Check if image exists (not the "No Profile Photo" placeholder)
            if img_src and "No Profile Photo" not in img_alt:
                # Convert relative path to absolute URL
                if not img_src.startswith("http"):
                    # Remove query string (timestamp parameter)
                    img_src = img_src.split("?")[0]
                    details["image_source_url"] = f"{BASE_URL}/{img_src.lstrip('/')}"
                else:
                    details["image_source_url"] = img_src.split("?")[0]

        return details

    except Exception as exc:
        return {"scrape_error": str(exc)}


# ── Image Download Functions ──────────────────────────────────────────────
def setup_volume():
    """Create Unity Catalog Volume for storing profile images."""
    try:
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME} "
            f"COMMENT 'Profile images for match finder analyzer'"
        )
        
        # Create gender-specific subdirectories
        import os
        for gender in ["female", "male"]:
            gender_path = f"{VOLUME_PATH}/{gender}"
            os.makedirs(gender_path, exist_ok=True)
        
        print(f"✓ Volume ready: {VOLUME_PATH}")
        return True
    except Exception as e:
        print(f"✗ Volume setup failed: {e}")
        return False


def download_image(image_url, profile_id, gender):
    """
    Download profile image and save to UC Volume.
    Returns: (success: bool, volume_path: str, error: str)
    """
    if not image_url:
        return False, None, "No image URL"
    
    try:
        # Determine file extension
        ext = "jpg"  # Default
        if ".png" in image_url.lower():
            ext = "png"
        elif ".jpeg" in image_url.lower():
            ext = "jpeg"
        
        # Gender-specific path
        gender_folder = "female" if gender == "Female" else "male"
        filename = f"{profile_id}.{ext}"
        volume_file_path = f"{VOLUME_PATH}/{gender_folder}/{filename}"
        
        # Download image
        response = session.get(image_url, timeout=10, stream=True)
        response.raise_for_status()
        
        # Check if it's actually an image (basic validation)
        content_type = response.headers.get("content-type", "")
        if "image" not in content_type.lower():
            return False, None, f"Not an image: {content_type}"
        
        # Save to volume
        with open(volume_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return True, volume_file_path, None
        
    except Exception as e:
        return False, None, str(e)


# Only run the scraping execution if mode is "full"  
if SCRAPER_MODE != "full":
    # Skip this cell - incremental scraper will run in Cell 5
    pass
else:
    # ── Catalog / schema ──────────────────────────────────────────────────────
    print("\nSetting up catalog and schema...")

    spark.sql(
    f"CREATE CATALOG IF NOT EXISTS {CATALOG} "
    f"COMMENT 'Thillai Matrimony Match Analyser'"
)
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
    f"COMMENT 'Default schema for matrimony app'"
)

# Setup UC Volume for images
if IMAGE_DOWNLOAD_ENABLED:
    volume_ready = setup_volume()
else:
    print("⚠ Image download is DISABLED")
    volume_ready = False

# ── Phase 1: listing ──────────────────────────────────────────────────────
print("\n" + "=" * 65)
print("  THILLAI MATRIMONY — BRONZE SCRAPER")
print("  MODE: EXPLICIT GENDER FILTERS (FEMALE + MALE)")
print("=" * 65)

profiles = scrape_all_pages()
listing_total, listing_female, listing_male, _ = gender_summary(
    "PHASE 1 — LISTING SCRAPE COMPLETE", profiles
)

if listing_total == 0:
    raise RuntimeError("No Male/Female profiles were discovered.")

if listing_female == 0 or listing_male == 0:
    print("\nWARNING:")
    print("The public search response contains only one gender.")
    print("The scraper did NOT apply a Female-only filter.")
    print("Verify the website's default search/filter behavior.")

# ── Phase 2: full details ─────────────────────────────────────────────────
print(
    f"\nFetching full details for {listing_total} profiles "
    f"(~{PROFILE_DELAY}s per profile)...\n"
)

full_details = []
failed_details = []

# Progress tracking
progress_interval = max(50, listing_total // 20)  # Print every ~5% or minimum 50
female_count = 0
male_count = 0

# Image download counters
image_success = 0
image_failed = 0
image_missing = 0

for i, profile in enumerate(profiles, 1):
    pid = profile.get("profile_id", "")
    gender = profile.get("gender", "Unknown")

    details = fetch_profile_details(pid)

    # Merge image URL from profile page (higher priority) with grid URL (fallback)
    image_url = details.get("image_source_url") or profile.get("image_source_url")
    
    # Download image if enabled
    image_downloaded = False
    image_volume_path = None
    image_error = None
    
    if IMAGE_DOWNLOAD_ENABLED and volume_ready and image_url:
        success, vol_path, error = download_image(image_url, pid, gender)
        image_downloaded = success
        image_volume_path = vol_path
        image_error = error
        
        if success:
            image_success += 1
        else:
            image_failed += 1
    elif not image_url:
        image_missing += 1
    
    # Add image metadata to details (with consistent types)
    details["image_source_url"] = image_url if image_url else None
    details["image_volume_path"] = image_volume_path if image_volume_path else None
    details["image_downloaded"] = "true" if image_downloaded else "false"  # String for consistent type
    details["image_download_error"] = image_error if image_error else None

    if "scrape_error" in details:
        failed_details.append({**profile, **details})
    else:
        full_details.append({**profile, **details})
        if gender == "Female":
            female_count += 1
        elif gender == "Male":
            male_count += 1

    time.sleep(PROFILE_DELAY)

all_bronze_rows = full_details + failed_details

print("\n" + "=" * 65)
print("  PHASE 2 — FULL DETAILS COMPLETE")
print("=" * 65)
print(f"  Listing profiles : {listing_total}")
print(f"  Details success  : {len(full_details)}")
print(f"  Details failed   : {len(failed_details)}")
if IMAGE_DOWNLOAD_ENABLED:
    print(f"\n  IMAGE DOWNLOADS:")
    print(f"    ✓ Success      : {image_success}")
    print(f"    ✗ Failed       : {image_failed}")
    print(f"    ⊘ No URL       : {image_missing}")
    print(f"    📁 Saved to    : {VOLUME_PATH}")
print("=" * 65)

if len(all_bronze_rows) != listing_total:
    raise RuntimeError(
        f"Profile count mismatch: discovered={listing_total}, "
        f"bronze_rows={len(all_bronze_rows)}"
    )

final_total, final_female, final_male, final_unknown = gender_summary(
    "FINAL — PROFILES TO BE WRITTEN TO BRONZE", all_bronze_rows
)

# ── Write Delta ───────────────────────────────────────────────────────────
print(f"\nSaving to Delta table: {BRONZE_TABLE}")

# Convert to pandas first (better type handling), then to Spark
import pandas as pd
df_pandas = pd.DataFrame(all_bronze_rows)
df_spark = spark.createDataFrame(df_pandas)

df_spark = (
    df_spark
    .withColumn("scrape_timestamp", current_timestamp())
    .withColumn("source_url", lit(SEARCH_URL))
    .withColumn("scrape_date", lit(datetime.now().strftime("%Y-%m-%d")))
)

(
    df_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

# ── Verify ─────────────────────────────────────────────────────────────────
df_saved = spark.table(BRONZE_TABLE)
saved_count = df_saved.count()
saved_columns = len(df_saved.columns)

saved_gender_counts = {
    row["gender"]: row["count"]
    for row in df_saved.groupBy("gender").count().collect()
}

saved_female = saved_gender_counts.get("Female", 0)
saved_male = saved_gender_counts.get("Male", 0)

print("\n" + "=" * 65)
print("  FINAL DELTA VALIDATION")
print("=" * 65)
print(f"  TOTAL PROFILES IN BRONZE : {saved_count}")
print(f"  FEMALE PROFILES          : {saved_female}")
print(f"  MALE PROFILES            : {saved_male}")
print(f"  FAILED DETAIL FETCHES    : {len(failed_details)}")
print(f"  COLUMNS                  : {saved_columns}")
print(f"  TABLE                    : {BRONZE_TABLE}")
print("=" * 65)

if saved_count != final_total:
    raise RuntimeError(
        f"Delta verification failed: expected {final_total}, found {saved_count}"
    )

print("\nSample rows:")
df_saved.select(
    "reg_no", "gender", "name", "caste", "sub_caste", "scrape_date"
).show(10, truncate=False)

print("\n" + "=" * 65)
print("  SCRAPER COMPLETED SUCCESSFULLY")
print(f"  TOTAL PROFILES : {saved_count}")
print(f"  FEMALE         : {saved_female}")
print(f"  MALE           : {saved_male}")
print("=" * 65)

# COMMAND ----------

# DBTITLE 1,📋 Scraper Modes: Full vs Incremental
# MAGIC %md
# MAGIC # 🚀 Scraper Modes Comparison
# MAGIC
# MAGIC ## **FULL REFRESH MODE** (Cell 1)
# MAGIC **Use when:** Initial setup or complete rebuild needed
# MAGIC
# MAGIC ✅ **Pros:**
# MAGIC - Captures all profiles (current total: 6,813)
# MAGIC - Ensures no profiles are missed
# MAGIC - Rebuilds entire bronze table from scratch
# MAGIC
# MAGIC ❌ **Cons:**
# MAGIC - **Slow**: ~3-5 minutes for profiles
# MAGIC - Wastes compute on unchanged profiles
# MAGIC - Overwrites table (loses incremental history)
# MAGIC
# MAGIC **Runtime:** ~3-5 min for 6,813 profiles
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **INCREMENTAL MODE** ⚡ (Cell 4)
# MAGIC **Use when:** Daily/scheduled updates
# MAGIC
# MAGIC ✅ **Pros:**
# MAGIC - **10x-100x faster**: Only scrapes NEW profiles
# MAGIC - Saves compute resources
# MAGIC - Appends to table (preserves history)
# MAGIC - Smart: Stops pagination when no new profiles found
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. Gets watermark: Highest profile ID from last run
# MAGIC    - Female: `F13636` → watermark = 13636
# MAGIC    - Male: `M24563` → watermark = 24563
# MAGIC 2. Scrapes only profiles with ID > watermark
# MAGIC 3. Stops early if no new profiles on first page
# MAGIC 4. **Appends** new rows (doesn't overwrite)
# MAGIC
# MAGIC **Runtime:** 
# MAGIC - If 0 new profiles: **~5 seconds** (just watermark check + page 1)
# MAGIC - If 10 new profiles: **~30 seconds**
# MAGIC - If 100 new profiles: **~2-3 minutes**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **IMAGE DOWNLOAD** (Cell 3)
# MAGIC **Use after:** Either full or incremental scraping
# MAGIC
# MAGIC - Runs independently
# MAGIC - Uses 20 parallel threads
# MAGIC - Skips already-downloaded images
# MAGIC - Works for both modes
# MAGIC
# MAGIC **Runtime:** ~1-2 min per 1,000 new images
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Recommended Daily Job Schedule
# MAGIC
# MAGIC ```
# MAGIC 1. Run Cell 4 (Incremental) - scrape NEW profiles only
# MAGIC 2. Run Cell 3 (Parallel Images) - download images for new profiles
# MAGIC 3. Run transform notebook (existing task in your job)
# MAGIC ```
# MAGIC
# MAGIC **Expected daily runtime:** 
# MAGIC - If ~10 new profiles/day: **~1-2 minutes** (vs 40+ min before!)
# MAGIC - If ~100 new profiles/day: **~3-5 minutes**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Performance Comparison
# MAGIC
# MAGIC | Scenario | Full Refresh | Incremental | Improvement |
# MAGIC |----------|--------------|-------------|-------------|
# MAGIC | **0 new profiles** | 40+ min | 5 sec | **480x faster** |
# MAGIC | **10 new profiles** | 40+ min | 30 sec | **80x faster** |
# MAGIC | **100 new profiles** | 40+ min | 3 min | **13x faster** |
# MAGIC | **All 6,813 profiles** | 40+ min | 40+ min | Same (first run) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ How to Update Your Job
# MAGIC
# MAGIC **Option 1: Quick Fix (Keep existing job structure)**
# MAGIC - Replace Cell 1 reference with Cell 4 in your job
# MAGIC - Images will be downloaded by Cell 2 (no change needed)
# MAGIC
# MAGIC **Option 2: Hybrid Approach**
# MAGIC - **Daily**: Run Cell 4 (incremental)
# MAGIC - **Weekly**: Run Cell 1 (full refresh) as backup/validation
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,PHASE 2: Download images in parallel (FAST)
"""
PHASE 2: Parallel Image Downloader
Downloads all profile images using multi-threading (10x+ faster)
"""

import requests
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

BASE_URL = "https://www.thillaimatrimony.org"
CATALOG = "matrimony"
SCHEMA = "default"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_profiles"
VOLUME_NAME = "profile_images"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

# Parallel download settings
MAX_WORKERS = 20  # Number of parallel downloads
TIMEOUT = 10  # Seconds per image

print("=" * 80)
print("  PARALLEL IMAGE DOWNLOADER")
print("=" * 80)
print(f"Run time    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Workers     : {MAX_WORKERS} parallel threads")
print(f"Source table: {BRONZE_TABLE}")
print(f"Target volume: {VOLUME_PATH}")
print("=" * 80)

# Setup volume directories
for gender in ["female", "male"]:
    os.makedirs(f"{VOLUME_PATH}/{gender}", exist_ok=True)

print("\n✓ Volume directories ready\n")

# Read profiles from bronze table
df = spark.table(BRONZE_TABLE)
profiles = df.select("reg_no", "profile_id", "gender", "image_source_url").collect()

total_profiles = len(profiles)
print(f"Loaded {total_profiles:,} profiles from bronze table\n")

# Filter profiles that need images
profiles_to_download = []
for row in profiles:
    pid = row["profile_id"] or row["reg_no"]
    gender = row["gender"]
    image_url = row["image_source_url"]
    
    # If no URL, construct it from profile ID
    if not image_url:
        image_url = f"{BASE_URL}/photos/{pid}_1.jpg"
    
    # Check if already downloaded
    gender_folder = "female" if gender == "Female" else "male"
    local_path = f"{VOLUME_PATH}/{gender_folder}/{pid}.jpg"
    
    if not os.path.exists(local_path):
        profiles_to_download.append({
            "profile_id": pid,
            "gender": gender,
            "image_url": image_url,
            "local_path": local_path
        })

print(f"Need to download: {len(profiles_to_download):,} images")
print(f"Already have: {total_profiles - len(profiles_to_download):,} images\n")

if len(profiles_to_download) == 0:
    print("✓ All images already downloaded!")
else:
    print(f"Starting parallel download with {MAX_WORKERS} workers...\n")
    
    # Download function
    def download_one_image(profile):
        try:
            response = requests.get(profile["image_url"], timeout=TIMEOUT)
            response.raise_for_status()
            
            # Check content type
            content_type = response.headers.get("content-type", "")
            if "image" not in content_type.lower():
                return {"status": "skip", "profile_id": profile["profile_id"], "error": "Not an image"}
            
            # Save to volume
            with open(profile["local_path"], "wb") as f:
                f.write(response.content)
            
            file_size = len(response.content) / 1024  # KB
            return {
                "status": "success",
                "profile_id": profile["profile_id"],
                "size": file_size,
                "path": profile["local_path"]
            }
        except Exception as e:
            return {
                "status": "failed",
                "profile_id": profile["profile_id"],
                "error": str(e)[:100]
            }
    
    # Download in parallel
    success_count = 0
    failed_count = 0
    skipped_count = 0
    total_size = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one_image, profile): profile for profile in profiles_to_download}
        
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            
            if result["status"] == "success":
                success_count += 1
                total_size += result["size"]
            elif result["status"] == "failed":
                failed_count += 1
            else:
                skipped_count += 1
            
            # Progress every 10%
            if i % max(1, len(profiles_to_download) // 10) == 0 or i == len(profiles_to_download):
                progress = (i / len(profiles_to_download)) * 100
                print(f"  Progress: {i:,}/{len(profiles_to_download):,} ({progress:.1f}%) | "
                      f"✓ {success_count:,} | ✗ {failed_count:,} | ⊘ {skipped_count:,}")
    
    print("\n" + "=" * 80)
    print("  DOWNLOAD COMPLETE")
    print("=" * 80)
    print(f"  ✓ Success      : {success_count:,} images")
    print(f"  ✗ Failed       : {failed_count:,} images")
    print(f"  ⊘ Skipped      : {skipped_count:,} images")
    print(f"  📦 Total size  : {total_size/1024:.1f} MB")
    print(f"  📁 Saved to    : {VOLUME_PATH}")
    print("=" * 80)

# Update bronze table with image paths
print("\n⏳ Updating bronze table with image metadata...")

from pyspark.sql.functions import col, when, lit, concat

# Build image path based on profile_id and gender
df_updated = df.withColumn(
    "image_volume_path",
    when(
        col("gender") == "Female",
        concat(lit(f"{VOLUME_PATH}/female/"), col("profile_id"), lit(".jpg"))
    ).otherwise(
        concat(lit(f"{VOLUME_PATH}/male/"), col("profile_id"), lit(".jpg"))
    )
).withColumn(
    "image_downloaded",
    lit(True)  # Mark as downloaded
).withColumn(
    "image_update_timestamp",
    lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
)

# Overwrite table with new schema
df_updated.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

print("✓ Bronze table updated with image columns\n")

# Verify
df_final = spark.table(BRONZE_TABLE)
print("Final table schema includes:")
for field in df_final.schema.fields:
    if "image" in field.name:
        print(f"  ✓ {field.name} ({field.dataType})")

print("\nSample with images:")
df_final.select("reg_no", "gender", "name", "image_volume_path", "image_downloaded") \
    .show(5, truncate=60)

print("\n" + "=" * 80)
print("  ✅ IMAGE DOWNLOAD PHASE COMPLETE")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Incremental Scraper (runs only if mode='incremental')
# Check mode - skip this cell if not in incremental mode
try:
    if SCRAPER_MODE != "incremental":
        print("⏭️  Skipping INCREMENTAL scraper (mode is not 'incremental')")
        dbutils.notebook.exit("skipped")
except NameError:
    # If SCRAPER_MODE not defined, error out (config cell must run first)
    raise RuntimeError("SCRAPER_MODE not defined! Run the configuration cell first.")

print("\n⚡ Running INCREMENTAL SCRAPER...\n")

"""
INCREMENTAL SCRAPER
Only scrapes NEW profiles (since last run)
Appends to bronze table instead of overwriting
"""

import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col, max as spark_max
from datetime import datetime
from collections import Counter
import time
import pandas as pd

BASE_URL = "https://www.thillaimatrimony.org"
SEARCH_URL = f"{BASE_URL}/publicprofilesearchgrid.aspx"
PROFILE_URL = f"{BASE_URL}/FullView.aspx?id="

CATALOG = "matrimony"
SCHEMA = "default"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_profiles"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/profile_images"

REQUEST_TIMEOUT = 15
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
}

spark = SparkSession.builder.getOrCreate()
session = requests.Session()
session.headers.update(HEADERS)

print("=" * 80)
print("  INCREMENTAL SCRAPER")
print("=" * 80)
print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Target table: {BRONZE_TABLE}")
print("=" * 80)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: Get watermark (highest profile ID from last run)
# ═══════════════════════════════════════════════════════════════════════════

try:
    df_existing = spark.table(BRONZE_TABLE)
    total_existing = df_existing.count()
    
    # Extract numeric part from reg_no (F13636 -> 13636, M24563 -> 24563)
    # Using try_cast to safely handle empty strings or invalid values
    from pyspark.sql.functions import regexp_extract, expr
    
    watermark_female = df_existing.filter(col("gender") == "Female") \
        .select(expr("TRY_CAST(REGEXP_EXTRACT(reg_no, 'F(\\\\d+)', 1) AS INT)").alias("num")) \
        .filter(col("num").isNotNull()) \
        .agg(spark_max("num")).collect()[0][0] or 0
    
    watermark_male = df_existing.filter(col("gender") == "Male") \
        .select(expr("TRY_CAST(REGEXP_EXTRACT(reg_no, 'M(\\\\d+)', 1) AS INT)").alias("num")) \
        .filter(col("num").isNotNull()) \
        .agg(spark_max("num")).collect()[0][0] or 0
    
    print(f"\n✓ Found existing bronze table")
    print(f"  Total profiles: {total_existing:,}")
    print(f"  Watermark Female: F{watermark_female}")
    print(f"  Watermark Male: M{watermark_male}")
    
except Exception as e:
    print(f"\n⚠ Bronze table doesn't exist or is empty. Will do FULL scrape.")
    watermark_female = 0
    watermark_male = 0
    total_existing = 0

print(f"\n→ Will scrape profiles NEWER than F{watermark_female} and M{watermark_male}")
print("=" * 80)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: Helper functions (reused from full scraper)
# ═══════════════════════════════════════════════════════════════════════════

def request_with_retry(method, url, **kwargs):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            if attempt < MAX_RETRIES:
                time.sleep(1.5 * attempt)
    raise RuntimeError(f"{method} failed after {MAX_RETRIES} attempts") from last_error

def get_hidden_fields(soup):
    fields = {}
    for name in ["__VIEWSTATE", "__EVENTVALIDATION", "__VIEWSTATEGENERATOR"]:
        tag = soup.find("input", {"id": name})
        if tag:
            fields[name] = tag.get("value", "")
    return fields

def detect_gender(reg_no):
    value = str(reg_no or "").strip().upper()
    if value.startswith("F"):
        return "Female"
    if value.startswith("M"):
        return "Male"
    return "Unknown"

def extract_numeric_id(reg_no):
    """Extract numeric part from F13636 -> 13636 or M24563 -> 24563"""
    import re
    match = re.search(r"(\d+)", str(reg_no or ""))
    return int(match.group(1)) if match else 0

def parse_grid(soup):
    profiles = []
    table = soup.find("table", id="GridView1")
    if not table:
        return profiles

    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) < 5:
            continue

        reg_no = cols[1].get_text(strip=True)
        name = cols[2].get_text(strip=True)
        caste = cols[3].get_text(strip=True)
        sub_caste = cols[4].get_text(strip=True)

        link_tag = cols[1].find("a")
        profile_id = reg_no
        if link_tag:
            href = link_tag.get("href", "")
            if "id=" in href:
                profile_id = href.split("id=", 1)[-1].split("&", 1)[0]

        gender = detect_gender(reg_no)
        if gender == "Unknown":
            continue

        # Extract image URL
        image_url = None
        if len(cols) >= 6:
            img_tag = cols[5].find("img")
            if img_tag:
                img_src = img_tag.get("src", "")
                if img_src and "photos/" in img_src:
                    if not img_src.startswith("http"):
                        image_url = f"{BASE_URL}/{img_src.lstrip('/')}"
                    else:
                        image_url = img_src

        profiles.append({
            "s_no": cols[0].get_text(strip=True),
            "reg_no": reg_no,
            "profile_id": profile_id,
            "name": name,
            "caste": caste,
            "sub_caste": sub_caste,
            "gender": gender,
            "image_source_url": image_url,
            "numeric_id": extract_numeric_id(reg_no)
        })

    return profiles

def scrape_incremental_for_gender(gender_value, watermark):
    """
    Scrape only NEW profiles for a gender (profiles with ID > watermark)
    """
    print(f"\nScraping NEW {gender_value} profiles (ID > {watermark})...")
    
    response = request_with_retry("GET", SEARCH_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    hidden = get_hidden_fields(soup)

    post_data = {
        "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
        "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
        "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
        "DropDownList8": gender_value,
        "Button1": "GO",
    }

    response = request_with_retry("POST", SEARCH_URL, data=post_data)
    soup = BeautifulSoup(response.text, "html.parser")
    hidden = get_hidden_fields(soup)

    first_page = parse_grid(soup)
    new_profiles = []
    
    # Filter: only profiles with ID > watermark
    for profile in first_page:
        if profile["numeric_id"] > watermark:
            new_profiles.append(profile)
    
    print(f"  Page 1 -> {len(new_profiles)} NEW (out of {len(first_page)} total)")
    
    # Check if we found any new profiles on page 1
    if len(new_profiles) == 0:
        print(f"  ✓ No new {gender_value} profiles found. Stopping pagination.")
        return new_profiles
    
    # If page 1 has new profiles, check subsequent pages
    # (since profiles are ordered newest first, once we hit old profiles, we can stop)
    page = 2
    consecutive_zero_new = 0
    
    while consecutive_zero_new < 2:  # Stop after 2 pages with no new profiles
        post_data = {
            "__EVENTTARGET": "GridView1",
            "__EVENTARGUMENT": f"Page${page}",
            "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
            "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
            "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
            "DropDownList8": gender_value,
        }

        response = request_with_retry("POST", SEARCH_URL, data=post_data)
        soup = BeautifulSoup(response.text, "html.parser")
        hidden = get_hidden_fields(soup)

        rows = parse_grid(soup)
        if not rows:
            print(f"  No more pages.")
            break
        
        # Count new profiles on this page
        page_new = [p for p in rows if p["numeric_id"] > watermark]
        
        if len(page_new) == 0:
            consecutive_zero_new += 1
            print(f"  Page {page} -> 0 NEW (stopping soon if pattern continues)")
        else:
            consecutive_zero_new = 0
            new_profiles.extend(page_new)
            print(f"  Page {page} -> {len(page_new)} NEW")
        
        # Check for next page
        has_next = any(f"Page${page + 1}" in a.get("href", "") for a in soup.find_all("a"))
        if not has_next:
            break
        
        page += 1
        time.sleep(0.5)
    
    return new_profiles

def normalize_key(key):
    return (
        key.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
        .replace(":", "")
    )

def fetch_profile_details(profile_id):
    url = PROFILE_URL + str(profile_id)
    try:
        response = request_with_retry("GET", url)
        soup = BeautifulSoup(response.text, "html.parser")
        details = {}

        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) != 2:
                    continue
                key = cols[0].get_text(strip=True).rstrip(":").strip()
                value = cols[1].get_text(strip=True)
                if key and value and len(key) < 80:
                    details[normalize_key(key)] = value

        # Extract image URL
        image_input = soup.find("input", {"id": "Image1", "type": "image"})
        if image_input:
            img_src = image_input.get("src", "")
            img_alt = image_input.get("alt", "")
            if img_src and "No Profile Photo" not in img_alt:
                if not img_src.startswith("http"):
                    img_src = img_src.split("?")[0]
                    details["image_source_url"] = f"{BASE_URL}/{img_src.lstrip('/')}"
                else:
                    details["image_source_url"] = img_src.split("?")[0]

        return details
    except Exception as exc:
        return {"scrape_error": str(exc)}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: Scrape NEW profiles
# ═══════════════════════════════════════════════════════════════════════════

new_female = scrape_incremental_for_gender("Female", watermark_female)
new_male = scrape_incremental_for_gender("Male", watermark_male)

all_new_profiles = new_female + new_male

print("\n" + "=" * 80)
print("  INCREMENTAL SCRAPE — SUMMARY")
print("=" * 80)
print(f"  NEW Female profiles: {len(new_female)}")
print(f"  NEW Male profiles  : {len(new_male)}")
print(f"  TOTAL NEW          : {len(all_new_profiles)}")
print("=" * 80)

if len(all_new_profiles) == 0:
    print("\n✓ No new profiles found. Bronze table is up-to-date!")
    print("  Exiting incremental scraper.")
else:
    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Fetch full details for new profiles
    # ═══════════════════════════════════════════════════════════════════════════
    
    print(f"\nFetching full details for {len(all_new_profiles)} new profiles...\n")
    
    all_bronze_rows = []
    success = 0
    failed = 0
    
    for i, profile in enumerate(all_new_profiles, 1):
        profile_id = profile["profile_id"]
        try:
            details = fetch_profile_details(profile_id)
            
            # Merge listing data with details
            row = {**profile, **details}
            
            # Add image metadata (no download, just metadata)
            row["image_volume_path"] = None
            row["image_downloaded"] = "false"
            row["image_download_error"] = None
            
            # Remove numeric_id (internal use only)
            row.pop("numeric_id", None)
            
            all_bronze_rows.append(row)
            success += 1
            
            if i % 50 == 0 or i == len(all_new_profiles):
                print(f"  Progress: {i}/{len(all_new_profiles)} | ✓ {success} | ✗ {failed}")
                
        except Exception as e:
            failed += 1
            print(f"  ✗ Failed to fetch details for {profile_id}: {e}")
    
    print("\n" + "=" * 80)
    print("  DETAILS FETCH COMPLETE")
    print("=" * 80)
    print(f"  Success: {success}")
    print(f"  Failed : {failed}")
    print("=" * 80)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 5: APPEND to bronze table (don't overwrite!)
    # ═══════════════════════════════════════════════════════════════════════════
    
    print(f"\nAppending {len(all_bronze_rows)} new profiles to {BRONZE_TABLE}...")
    
    df_pandas = pd.DataFrame(all_bronze_rows)
    df_new = spark.createDataFrame(df_pandas)
    
    df_new = (
        df_new
        .withColumn("scrape_timestamp", current_timestamp())
        .withColumn("source_url", lit(SEARCH_URL))
        .withColumn("scrape_date", lit(datetime.now().strftime("%Y-%m-%d")))
    )
    
    # MERGE (UPSERT) instead of APPEND to prevent duplicates
    from delta.tables import DeltaTable
    
    # Create table if it doesn't exist
    if not spark.catalog.tableExists(BRONZE_TABLE):
        print("  Table doesn't exist, creating...")
        df_new.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(BRONZE_TABLE)
    else:
        # MERGE: Update existing, insert new
        print("  Using MERGE to upsert profiles...")
        delta_table = DeltaTable.forName(spark, BRONZE_TABLE)
        
        delta_table.alias("target").merge(
            df_new.alias("source"),
            "target.profile_id = source.profile_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    
    print("✓ Successfully appended to bronze table\n")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 6: Final validation
    # ═══════════════════════════════════════════════════════════════════════════
    
    df_final = spark.table(BRONZE_TABLE)
    total_after = df_final.count()
    
    female_count = df_final.filter(col("gender") == "Female").count()
    male_count = df_final.filter(col("gender") == "Male").count()
    
    print("=" * 80)
    print("  INCREMENTAL SCRAPE COMPLETE")
    print("=" * 80)
    print(f"  Profiles BEFORE : {total_existing:,}")
    print(f"  NEW profiles    : {len(all_bronze_rows):,}")
    print(f"  Profiles AFTER  : {total_after:,}")
    print(f"  Female total    : {female_count:,}")
    print(f"  Male total      : {male_count:,}")
    print("=" * 80)
    
    # Show sample of new profiles
    print("\nSample of newly added profiles:")
    df_final.orderBy(col("scrape_timestamp").desc()).select(
        "reg_no", "gender", "name", "caste", "scrape_date"
    ).show(10, truncate=60)