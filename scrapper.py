"""
01_scraper.py — Bronze Layer
==============================
Thillai Matrimony Match Analyser
Databricks App — Step 1 of 4

What this does:
    - Scrapes all female profiles from thillaimatrimony.org
    - Applies Female gender filter server-side
    - Navigates all pages via ASP.NET postback
    - Fetches full details from each profile page
    - Scrapes BOTH male and female profiles
    - Saves raw data to Delta table: matrimony.default.bronze_profiles

How to run:
    - Attach to any Databricks cluster (DBR 13+)
    - Run as a Notebook OR as a Databricks Workflow Job
    - Schedule daily to keep profiles up to date

Delta table: matrimony.default.bronze_profiles
    - Full refresh on each run (overwrite mode)
    - All raw columns preserved as-is from website
"""

# ── Imports ───────────────────────────────────────────────────────────────
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime
import time

# ── Config ────────────────────────────────────────────────────────────────
BASE_URL      = "https://www.thillaimatrimony.org"
SEARCH_URL    = f"{BASE_URL}/publicprofilesearchgrid.aspx"
PROFILE_URL   = f"{BASE_URL}/FullView.aspx?id="

CATALOG       = "matrimony"          # Dedicated catalog for this project
SCHEMA        = "default"            # Schema/database name
BRONZE_TABLE  = f"{CATALOG}.{SCHEMA}.bronze_profiles"

PAGE_DELAY    = 0.8    # seconds between page requests
PROFILE_DELAY = 0.5    # seconds between profile requests

HEADERS = {
    "User-Agent"     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept"         : "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer"        : SEARCH_URL,
}

# ── Spark Session ─────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()
print(f"✅ Spark session ready")
print(f"   Target table : {BRONZE_TABLE}")
print(f"   Run time     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ╔══════════════════════════════════════════════════════╗
# ║              SCRAPER FUNCTIONS                       ║
# ╚══════════════════════════════════════════════════════╝

session = requests.Session()
session.headers.update(HEADERS)


def get_hidden_fields(soup):
    """Extract ASP.NET ViewState hidden fields for postback."""
    fields = {}
    for name in ["__VIEWSTATE", "__EVENTVALIDATION", "__VIEWSTATEGENERATOR"]:
        tag = soup.find("input", {"id": name})
        if tag:
            fields[name] = tag.get("value", "")
    return fields


def parse_grid(soup):
    """Parse profile listing rows from search results grid."""
    profiles = []
    table = soup.find("table", id="GridView1")
    if not table:
        return profiles
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) < 5:
            continue
        reg_no   = cols[1].get_text(strip=True)
        name     = cols[2].get_text(strip=True)
        caste    = cols[3].get_text(strip=True)
        subcaste = cols[4].get_text(strip=True)

        link_tag   = cols[1].find("a")
        profile_id = reg_no
        if link_tag and "id=" in link_tag.get("href", ""):
            profile_id = link_tag["href"].split("id=")[-1]

        # Determine gender from Reg No prefix
        prefix = reg_no.upper()[0] if reg_no else ""
        if prefix == "F":
            gender = "Female"
        elif prefix == "M":
            gender = "Male"
        else:
            continue   # skip unknown prefixes

        profiles.append({
            "s_no"      : cols[0].get_text(strip=True),
            "reg_no"    : reg_no,
            "profile_id": profile_id,
            "name"      : name,
            "caste"     : caste,
            "sub_caste" : subcaste,
            "gender"    : gender,
        })
    return profiles


def scrape_all_genders():
    """Load search page — scrape ALL profiles (male + female)."""
    print("\n🌐 Loading search page (all genders)...")
    resp   = session.get(SEARCH_URL)
    soup   = BeautifulSoup(resp.text, "html.parser")
    hidden = get_hidden_fields(soup)

    # No gender filter — scrape all profiles
    print(f"   ✅ {len(parse_grid(soup))} profiles on first page (all genders).")
    return soup, hidden


def scrape_all_pages():
    """Navigate all pages using ASP.NET postback."""
    soup, hidden = scrape_all_genders()
    all_profiles = parse_grid(soup)
    print(f"\n📄 Page 1 → {len(all_profiles)} profiles")

    page = 2
    while True:
        print(f"📄 Page {page}...", end=" ", flush=True)
        post_data = {
            "__EVENTTARGET"       : "GridView1",
            "__EVENTARGUMENT"     : f"Page${page}",
            "__VIEWSTATE"         : hidden.get("__VIEWSTATE", ""),
            "__EVENTVALIDATION"   : hidden.get("__EVENTVALIDATION", ""),
            "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
        }
        resp   = session.post(SEARCH_URL, data=post_data)
        soup   = BeautifulSoup(resp.text, "html.parser")
        hidden = get_hidden_fields(soup)
        rows   = parse_grid(soup)

        if not rows:
            print("No rows. Done!")
            break

        all_profiles.extend(rows)
        print(f"→ {len(rows)} | Total: {len(all_profiles)}")

        has_next = any(
            f"Page${page + 1}" in a.get("href", "")
            for a in soup.find_all("a")
        )
        if not has_next:
            print("   ✅ Last page reached!")
            break

        page += 1
        time.sleep(PAGE_DELAY)

    return all_profiles


def fetch_profile_details(profile_id):
    """Fetch full details from individual profile page."""
    url = PROFILE_URL + profile_id
    try:
        resp    = session.get(url, timeout=10)
        soup    = BeautifulSoup(resp.text, "html.parser")
        details = {}
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) == 2:
                    key = cols[0].get_text(strip=True).rstrip(":").strip()
                    val = cols[1].get_text(strip=True)
                    if key and val and len(key) < 80:
                        # Normalize key to snake_case for Delta column names
                        col_key = (key.lower()
                                   .replace(" ", "_")
                                   .replace("/", "_")
                                   .replace("-", "_")
                                   .replace(".", "")
                                   .replace("(", "")
                                   .replace(")", ""))
                        details[col_key] = val
        return details
    except Exception as e:
        return {"scrape_error": str(e)}


# ╔══════════════════════════════════════════════════════╗
# ║              SETUP CATALOG & SCHEMA                  ║
# ╚══════════════════════════════════════════════════════╝

print(f"\n🗄️  Setting up catalog and schema...")

# Create catalog if not exists (requires metastore admin)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'Thillai Matrimony Match Analyser'")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Default schema for matrimony app'")
print(f"   ✅ Catalog  : {CATALOG}")
print(f"   ✅ Schema   : {SCHEMA}")


# ╔══════════════════════════════════════════════════════╗
# ║                  RUN SCRAPER                         ║
# ╚══════════════════════════════════════════════════════╝

print("\n" + "=" * 55)
print("  Thillai Matrimony — Bronze Scraper (All Genders)")
print("=" * 55)

# Phase 1: Scrape listing from all pages
profiles = scrape_all_pages()
print(f"\n✅ Listing complete : {len(profiles)} profiles")

# Phase 2: Fetch full details for each profile
print(f"\n🔍 Fetching full details (~{PROFILE_DELAY}s per profile)...\n")
full_details = []
for i, profile in enumerate(profiles, 1):
    pid  = profile.get("profile_id", "")
    name = profile.get("name", "")[:35]
    print(f"  [{i:>4}/{len(profiles)}] {pid} — {name}")
    details = fetch_profile_details(pid)
    full_details.append({**profile, **details})
    time.sleep(PROFILE_DELAY)

print(f"\n✅ Full details fetched for all {len(full_details)} profiles")


# ╔══════════════════════════════════════════════════════╗
# ║              SAVE TO DELTA TABLE                     ║
# ╚══════════════════════════════════════════════════════╝

print(f"\n💾 Saving to Delta table: {BRONZE_TABLE}")

# Convert to Spark DataFrame
df_spark = spark.createDataFrame(full_details)

# Add audit columns
df_spark = (df_spark
    .withColumn("scrape_timestamp", current_timestamp())
    .withColumn("source_url", lit(SEARCH_URL))
    .withColumn("scrape_date", lit(datetime.now().strftime("%Y-%m-%d")))
)

# Write to Delta — full refresh (overwrite)
(df_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

# Verify
count = spark.table(BRONZE_TABLE).count()
cols  = len(spark.table(BRONZE_TABLE).columns)

print(f"\n{'='*55}")
print(f"  ✅ Bronze table saved!")
print(f"  📊 Rows    : {count}")
print(f"  📋 Columns : {cols}")
print(f"  🗄️  Table  : {BRONZE_TABLE}")
print(f"{'='*55}")

# Preview
print("\n📋 Sample rows:")
spark.table(BRONZE_TABLE).select(
    "reg_no", "name", "caste", "sub_caste", "scrape_date"
).show(5, truncate=False)