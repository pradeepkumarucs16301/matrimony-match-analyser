# Databricks notebook source
"""
02_transform.py — Gold Layer + Compatibility Engine
=====================================================
Thillai Matrimony Match Analyser
Databricks App — Step 2 of 4

What this does:
    1. Reads raw profiles from Bronze table (matrimony.default.bronze_profiles)
    2. Parses DOB → Birth Year, DOB, Birth Place
    3. Parses Gothram-Star-Rasi → Star, Rasi
    4. Extracts WhatsApp numbers from Contact field
    5. Builds Star ↔ Rasi compatibility lookup table
       (based on traditional Tamil Thirumana Porutham)
    6. Saves enriched data to Gold table: matrimony.default.gold_profiles

Delta tables:
    Input  : matrimony.default.bronze_profiles
    Output : matrimony.default.gold_profiles
           : matrimony.default.star_compatibility  (compatibility lookup)
"""

# ── Imports ───────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, lit, current_timestamp, upper, trim,
    regexp_extract, split, when, array, array_contains
)
from pyspark.sql.types import (
    StringType, IntegerType, ArrayType, StructType, StructField
)
import re

# ── Config ────────────────────────────────────────────────────────────────
CATALOG       = "matrimony"
SCHEMA        = "default"
BRONZE_TABLE  = f"{CATALOG}.{SCHEMA}.bronze_profiles"
GOLD_TABLE    = f"{CATALOG}.{SCHEMA}.gold_profiles"
COMPAT_TABLE  = f"{CATALOG}.{SCHEMA}.star_compatibility"

# ── Spark ─────────────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()
print(f"✅ Spark ready")
print(f"   Bronze : {BRONZE_TABLE}")
print(f"   Gold   : {GOLD_TABLE}")
print(f"   Compat : {COMPAT_TABLE}")


# ╔══════════════════════════════════════════════════════╗
# ║         STAR ↔ RASI COMPATIBILITY TABLE              ║
# ║   Based on traditional Tamil Thirumana Porutham      ║
# ║   Source: Tamil astrology star matching tables       ║
# ╚══════════════════════════════════════════════════════╝

# Structure: For each girl's star → best boy stars (Utthamam + Madhyamam)
# This is the "Girl to Boy" matching table used in Tamil matrimony
STAR_COMPATIBILITY = {
    "ASWINI": {
        "rasi"       : "MESHAM",
        "utthamam"   : ["BHARANI", "THIRUVADHIRAI", "POOSAM", "ANUSHAM",
                        "POORADAM", "THIRUVONAM", "SADHAYAM", "UTTHIRATTATHI"],
        "madhyamam"  : ["POORATTATHI", "AVITTAM", "UTHRADAM", "VISAKAM",
                        "POORAM", "PUNARPOOSAM", "MIRUGASEERUSHAM", "CHITHIRAI",
                        "ROHINI", "KRITHIGAI"],
    },
    "BHARANI": {
        "rasi"       : "MESHAM",
        "utthamam"   : ["ASWINI", "KRITHIGAI", "MIRUGASEERUSHAM", "PUNARPOOSAM",
                        "AYILYAM", "CHITHIRAI", "VISAKAM", "KETTAI",
                        "MOOLAM", "UTHRADAM", "REVATHI"],
        "madhyamam"  : ["SADHAYAM", "THIRUVONAM", "SWATHI", "THIRUVADHIRAI",
                        "MAGAM", "VISAKAM"],
    },
    "KRITHIGAI": {
        "rasi"       : "MESHAM",   # 1st padham
        "utthamam"   : ["ASWINI", "BHARANI", "THIRUVADHIRAI", "POOSAM",
                        "HASTHAM", "SWATHI", "ANUSHAM", "MOOLAM",
                        "SADHAYAM", "UTTHIRATTATHI"],
        "madhyamam"  : ["MIRUGASEERUSHAM", "MAGAM", "CHITHIRAI", "KETTAI",
                        "AVITTAM", "UTHRADAM"],
    },
    "ROHINI": {
        "rasi"       : "RISHABAM",
        "utthamam"   : ["MIRUGASEERUSHAM", "PUNARPOOSAM", "AYILYAM", "POORAM",
                        "UTHIRADAM", "CHITHIRAI", "VISAKAM", "AVITTAM",
                        "POORATTATHI"],
        "madhyamam"  : ["ASWINI", "BHARANI", "ANUSHAM", "KETTAI",
                        "POORADAM", "UTHRADAM", "CHITHIRAI", "VISAGAM"],
    },
    "MIRUGASEERUSHAM": {
        "rasi"       : "RISHABAM",
        "utthamam"   : ["ROHINI", "MIRUGASEERUSHAM", "PUNARPOOSAM", "AYILYAM",
                        "POORAM", "UTHIRAM", "VISAKAM", "KETTAI",
                        "POORATTATHI", "REVATHI"],
        "madhyamam"  : ["ASWINI", "BHARANI", "KRITHIGAI", "THIRUVADHIRAI",
                        "MOOLAM", "POORADAM", "UTHIRADAM", "AVITTAM"],
    },
    "THIRUVADHIRAI": {
        "rasi"       : "MITHUNAM",
        "utthamam"   : ["ROHINI", "THIRUVADHIRAI", "PUNARPOOSAM", "AYILYAM",
                        "HASTHAM", "VISAKAM", "KETTAI", "THIRUVONAM",
                        "SADHAYAM", "POORATTATHI"],
        "madhyamam"  : ["ASWINI", "BHARANI", "KRITHIGAI", "POOSAM",
                        "MAGAM", "SWATHI", "ANUSHAM"],
    },
    "PUNARPOOSAM": {
        "rasi"       : "MITHUNAM",
        "utthamam"   : ["ASWINI", "ROHINI", "MIRUGASEERUSHAM", "THIRUVADHIRAI",
                        "POOSAM", "MAGAM", "CHITHIRAI", "SWATHI",
                        "ANUSHAM", "MOOLAM", "SADHAYAM", "UTTHIRATTATHI"],
        "madhyamam"  : ["BHARANI", "THIRUVONAM", "AYILYAM", "POORAM"],
    },
    "POOSAM": {
        "rasi"       : "KATAKAM",
        "utthamam"   : ["THIRUVADHIRAI", "AYILYAM", "HASTHAM", "SWATHI",
                        "VISAKAM", "KETTAI", "THIRUVONAM", "REVATHI"],
        "madhyamam"  : ["ASWINI", "BHARANI", "ROHINI", "MIRUGASEERUSHAM",
                        "POORAM", "ANUSHAM"],
    },
    "AYILYAM": {
        "rasi"       : "KATAKAM",
        "utthamam"   : ["ASWINI", "UTHIRAM", "VISAKAM", "UTTHIRATTATHI",
                        "AVITTAM", "UTHIRADAM"],
        "madhyamam"  : ["ROHINI", "THIRUVADHIRAI", "POOSAM", "MAGAM",
                        "SWATHI", "ANUSHAM"],
    },
    "MAGAM": {
        "rasi"       : "SIMMAM",
        "utthamam"   : ["BHARANI", "KRITHIGAI", "ROHINI", "THIRUVADHIRAI",
                        "PUNARPOOSAM", "MAGAM", "POORAM", "HASTHAM",
                        "SWATHI", "ANUSHAM", "THIRUVONAM", "SADHAYAM"],
        "madhyamam"  : ["ASWINI", "MIRUGASEERUSHAM", "POOSAM", "CHITHIRAI",
                        "UTHIRAM", "MOOLAM"],
    },
    "POORAM": {
        "rasi"       : "SIMMAM",
        "utthamam"   : ["ASWINI", "KRITHIGAI", "MIRUGASEERUSHAM", "PUNARPOOSAM",
                        "MAGAM", "SADHAYAM", "KETTAI", "MOOLAM", "AVITTAM"],
        "madhyamam"  : ["BHARANI", "ROHINI", "THIRUVADHIRAI", "AYILYAM",
                        "POORADAM", "UTHRADAM", "THIRUVONAM"],
    },
    "UTHIRAM": {
        "rasi"       : "SIMMAM",
        "utthamam"   : ["ASWINI", "BHARANI", "ROHINI", "THIRUVADHIRAI",
                        "HASTHAM", "SWATHI", "ANUSHAM", "POORADAM",
                        "THIRUVONAM", "SADHAYAM", "UTTHIRATTATHI"],
        "madhyamam"  : ["MIRUGASEERUSHAM", "PUNARPOOSAM", "POOSAM",
                        "MAGAM", "POORAM", "CHITHIRAI"],
    },
    "HASTHAM": {
        "rasi"       : "KANNI",
        "utthamam"   : ["MIRUGASEERUSHAM", "PUNARPOOSAM", "CHITHIRAI",
                        "VISAKAM", "KETTAI", "POORADAM", "AVITTAM"],
        "madhyamam"  : ["ASWINI", "THIRUVADHIRAI", "POOSAM", "MAGAM",
                        "SWATHI", "ANUSHAM", "REVATHI"],
    },
    "CHITHIRAI": {
        "rasi"       : "KANNI",
        "utthamam"   : ["ROHINI", "THIRUVADHIRAI", "UTHIRAM", "HASTHAM",
                        "SWATHI", "ANUSHAM", "MOOLAM", "THIRUVONAM", "SADHAYAM"],
        "madhyamam"  : ["ASWINI", "BHARANI", "MIRUGASEERUSHAM", "PUNARPOOSAM",
                        "POORAM", "VISAKAM", "KETTAI"],
    },
    "SWATHI": {
        "rasi"       : "THULAM",
        "utthamam"   : ["BHARANI", "MIRUGASEERUSHAM", "PUNARPOOSAM",
                        "CHITHIRAI", "SWATHI", "VISAKAM", "KETTAI",
                        "POORADAM", "POORATTATHI", "REVATHI"],
        "madhyamam"  : ["ASWINI", "KRITHIGAI", "ROHINI", "THIRUVADHIRAI",
                        "AYILYAM", "MAGAM", "POORAM", "UTHRADAM"],
    },
    "VISAKAM": {
        "rasi"       : "THULAM",
        "utthamam"   : ["ASWINI", "BHARANI", "ROHINI", "MIRUGASEERUSHAM",
                        "THIRUVADHIRAI", "POOSAM", "MAGAM", "ANUSHAM",
                        "MOOLAM", "AVITTAM", "SADHAYAM", "UTTHIRATTATHI"],
        "madhyamam"  : ["KRITHIGAI", "POORAM", "UTHIRAM", "HASTHAM",
                        "CHITHIRAI", "SWATHI", "UTHRADAM"],
    },
    "ANUSHAM": {
        "rasi"       : "VRICHIKA",
        "utthamam"   : ["ROHINI", "THIRUVADHIRAI", "PUNARPOOSAM", "AYILYAM",
                        "KETTAI", "THIRUVONAM", "SADHAYAM", "POORATTATHI",
                        "REVATHI"],
        "madhyamam"  : ["MIRUGASEERUSHAM", "POOSAM", "HASTHAM", "CHITHIRAI",
                        "SWATHI", "VISAKAM", "UTTHIRATTATHI"],
    },
    "KETTAI": {
        "rasi"       : "VRICHIKA",
        "utthamam"   : ["KRITHIGAI", "UTHIRAM", "MOOLAM", "UTHIRADAM",
                        "AVITTAM", "POORATTATHI", "UTTHIRATTATHI",
                        "ROHINI", "MIRUGASEERUSHAM", "PUNARPOOSAM"],
        "madhyamam"  : ["POOSAM", "ASWINI", "AYILYAM", "CHITHIRAI",
                        "MAGAM", "SWATHI", "VISAKAM", "ANUSHAM", "REVATHI"],
    },
    "MOOLAM": {
        "rasi"       : "DHANUSU",
        "utthamam"   : ["BHARANI", "THIRUVADHIRAI", "POORAM", "HASTHAM",
                        "POORADAM", "THIRUVONAM", "SADHAYAM"],
        "madhyamam"  : ["ASWINI", "ROHINI", "PUNARPOOSAM", "AYILYAM",
                        "MAGAM", "SWATHI", "VISAKAM", "ANUSHAM",
                        "KETTAI", "UTHIRAM", "REVATHI"],
    },
    "POORADAM": {
        "rasi"       : "DHANUSU",
        "utthamam"   : ["UTHIRAM", "CHITHIRAI", "VISAKAM", "UTHIRADAM",
                        "AVITTAM", "POORATTATHI", "REVATHI"],
        "madhyamam"  : ["ASWINI", "BHARANI", "KRITHIGAI", "PUNARPOOSAM",
                        "AYILYAM", "MAGAM", "POORAM", "ANUSHAM",
                        "KETTAI", "MOOLAM", "THIRUVADHIRAI", "HASTHAM",
                        "SWATHI", "UTHRADAM"],
    },
    "UTHIRADAM": {
        "rasi"       : "DHANUSU",
        "utthamam"   : ["ASWINI", "BHARANI", "POOSAM", "MAGAM", "POORAM",
                        "HASTHAM", "SWATHI", "ANUSHAM", "MOOLAM",
                        "POORADAM", "THIRUVONAM", "SADHAYAM", "UTTHIRATTATHI"],
        "madhyamam"  : ["ROHINI", "AYILYAM", "KETTAI", "AVITTAM", "REVATHI"],
    },
    "THIRUVONAM": {
        "rasi"       : "MAGARAM",
        "utthamam"   : ["BHARANI", "KRITHIGAI", "MIRUGASEERUSHAM", "PUNARPOOSAM",
                        "AYILYAM", "POORAM", "CHITHIRAI", "VISAKAM",
                        "KETTAI", "UTHIRADAM", "AVITTAM", "POORATTATHI",
                        "REVATHI"],
        "madhyamam"  : ["UTHIRAM", "MOOLAM", "POORADAM", "ANUSHAM"],
    },
    "AVITTAM": {
        "rasi"       : "KUMBHAM",
        "utthamam"   : ["ASWINI", "KRITHIGAI", "ROHINI", "THIRUVADHIRAI",
                        "POOSAM", "MAGAM", "UTHIRADAM", "HASTHAM",
                        "SWATHI", "ANUSHAM", "MOOLAM", "UTHIRAM",
                        "THIRUVONAM", "SADHAYAM", "UTTHIRATTATHI"],
        "madhyamam"  : ["VISAKAM", "KETTAI", "POORADAM"],
    },
    "SADHAYAM": {
        "rasi"       : "KUMBHAM",
        "utthamam"   : ["ASWINI", "BHARANI", "ROHINI", "THIRUVADHIRAI",
                        "PUNARPOOSAM", "MAGAM", "POORAM", "HASTHAM",
                        "SWATHI", "ANUSHAM", "MOOLAM", "UTHIRAM",
                        "THIRUVONAM", "POORADAM", "UTTHIRATTATHI"],
        "madhyamam"  : ["KETTAI", "AVITTAM", "UTHRADAM", "VISAKAM"],
    },
    "POORATTATHI": {
        "rasi"       : "KUMBHAM",
        "utthamam"   : ["SWATHI", "ANUSHAM", "MOOLAM", "AVITTAM", "SADHAYAM"],
        "madhyamam"  : ["UTTHIRATTATHI", "THIRUVONAM", "POORADAM",
                        "KETTAI", "ANUSHAM", "HASTHAM", "AYILYAM",
                        "MIRUGASEERUSHAM", "THIRUVADHIRAI", "CHITHIRAI"],
    },
    "UTTHIRATTATHI": {
        "rasi"       : "MEENAM",
        "utthamam"   : ["ROHINI", "THIRUVADHIRAI", "PUNARPOOSAM", "HASTHAM",
                        "KETTAI", "THIRUVONAM", "SADHAYAM", "POORATTATHI",
                        "REVATHI"],
        "madhyamam"  : ["AVITTAM", "UTHIRADAM", "MOOLAM", "SWATHI",
                        "AYILYAM", "UTHIRAM"],
    },
    "REVATHI": {
        "rasi"       : "MEENAM",
        "utthamam"   : ["KRITHIGAI", "MIRUGASEERUSHAM", "PUNARPOOSAM",
                        "POOSAM", "POORAM", "UTHIRAM", "HASTHAM",
                        "SWATHI", "ANUSHAM", "UTHIRADAM"],
        "madhyamam"  : ["SADHAYAM", "THIRUVONAM", "VISAKAM", "HASTHAM",
                        "POOSAM", "POORADAM", "PUNARPOOSAM", "ROHINI",
                        "KRITHIGAI"],
    },
}

# ── Rasi → Compatible Rasi mapping (for Rasi Porutham) ───────────────────
RASI_COMPATIBILITY = {
    "MESHAM"   : {"best": ["SIMMAM", "THULAM", "DHANUSU"],
                  "good": ["RISHABAM", "MITHUNAM", "KUMBHAM"]},
    "RISHABAM" : {"best": ["KANNI", "MAGARAM", "MEENAM"],
                  "good": ["MESHAM", "KATAKAM", "VRICHIKA"]},
    "MITHUNAM" : {"best": ["THULAM", "KUMBHAM"],
                  "good": ["MESHAM", "SIMMAM", "DHANUSU"]},
    "KATAKAM"  : {"best": ["VRICHIKA", "MEENAM"],
                  "good": ["RISHABAM", "KANNI", "MAGARAM"]},
    "SIMMAM"   : {"best": ["MESHAM", "DHANUSU"],
                  "good": ["MITHUNAM", "THULAM", "KUMBHAM"]},
    "KANNI"    : {"best": ["RISHABAM", "MAGARAM"],
                  "good": ["KATAKAM", "VRICHIKA", "MEENAM"]},
    "THULAM"   : {"best": ["MITHUNAM", "KUMBHAM", "MESHAM"],
                  "good": ["SIMMAM", "DHANUSU", "RISHABAM"]},
    "VRICHIKA" : {"best": ["KATAKAM", "MEENAM"],
                  "good": ["KANNI", "MAGARAM", "RISHABAM"]},
    "DHANUSU"  : {"best": ["MESHAM", "SIMMAM"],
                  "good": ["THULAM", "MITHUNAM", "KUMBHAM"]},
    "MAGARAM"  : {"best": ["RISHABAM", "KANNI"],
                  "good": ["VRICHIKA", "MEENAM", "KATAKAM"]},
    "KUMBHAM"  : {"best": ["MITHUNAM", "THULAM"],
                  "good": ["MESHAM", "SIMMAM", "DHANUSU"]},
    "MEENAM"   : {"best": ["KATAKAM", "VRICHIKA"],
                  "good": ["MAGARAM", "RISHABAM", "KANNI"]},
}


# ╔══════════════════════════════════════════════════════╗
# ║              PARSING UDFs                            ║
# ╚══════════════════════════════════════════════════════╝

def _extract_year(val):
    if not val: return None
    match = re.search(r'\b(\d{4})\b', str(val))
    return int(match.group(1)) if match else None

def _extract_dob(val):
    if not val: return None
    match = re.match(r'(\d{2}/\d{2}/\d{4})', str(val).strip())
    return match.group(1) if match else None

def _extract_place(val):
    if not val: return None
    parts = str(val).split('-')
    return parts[-1].strip() if len(parts) >= 3 else None

def _extract_star(val):
    if not val: return None
    parts = str(val).split('-')
    return parts[1].strip().upper() if len(parts) >= 2 else None

def _extract_rasi(val):
    if not val: return None
    parts = str(val).split('-')
    return parts[2].strip().upper() if len(parts) >= 3 else None

def _extract_primary_wa(val):
    if not val: return None
    raw = str(val)
    wa  = re.findall(r'[\d\s]{10,}\s*\(?[Ww]hats[Aa]pp\)?', raw)
    for m in wa:
        clean = re.sub(r'\s+', '', m.split('(')[0])
        if len(clean) == 10 and clean[0] in '6789': return '+91' + clean
        if len(clean) == 12 and clean.startswith('91'): return '+' + clean
    digits = re.findall(r'[\d\s]{10,}', raw)
    for d in digits:
        clean = re.sub(r'\s+', '', d)
        if len(clean) == 10 and clean[0] in '6789': return '+91' + clean
        if len(clean) == 12 and clean.startswith('91'): return '+' + clean
    return None

def _get_compatible_stars(star):
    """Return comma-separated best match stars for a given girl's star."""
    if not star: return None
    info = STAR_COMPATIBILITY.get(star.upper())
    if not info: return None
    return ','.join(info.get('utthamam', []))

def _get_good_stars(star):
    """Return comma-separated madhyamam (good) match stars."""
    if not star: return None
    info = STAR_COMPATIBILITY.get(star.upper())
    if not info: return None
    return ','.join(info.get('madhyamam', []))

def _get_compatible_rasi(star):
    """Return the natural Rasi of this star."""
    if not star: return None
    info = STAR_COMPATIBILITY.get(star.upper())
    return info.get('rasi') if info else None

# Register UDFs
udf_year       = udf(_extract_year,        IntegerType())
udf_dob        = udf(_extract_dob,         StringType())
udf_place      = udf(_extract_place,       StringType())
udf_star       = udf(_extract_star,        StringType())
udf_rasi       = udf(_extract_rasi,        StringType())
udf_primary_wa = udf(_extract_primary_wa,  StringType())
udf_compat     = udf(_get_compatible_stars,StringType())
udf_good       = udf(_get_good_stars,      StringType())
udf_rasi_map   = udf(_get_compatible_rasi, StringType())

print("✅ UDFs registered!")


# ╔══════════════════════════════════════════════════════╗
# ║              READ BRONZE TABLE                       ║
# ╚══════════════════════════════════════════════════════╝

print(f"\n📖 Reading Bronze table: {BRONZE_TABLE}")
df_bronze = spark.table(BRONZE_TABLE)
print(f"   Rows    : {df_bronze.count()}")
print(f"   Columns : {len(df_bronze.columns)}")


# ╔══════════════════════════════════════════════════════╗
# ║              TRANSFORM — GOLD LAYER                  ║
# ╚══════════════════════════════════════════════════════╝

print("\n🔄 Transforming to Gold...")

# Find actual column names (snake_case from scraper)
dob_col     = next((c for c in df_bronze.columns if 'dob' in c.lower() or 'time' in c.lower()), None)
star_col    = next((c for c in df_bronze.columns if 'gothram' in c.lower() or 'star' in c.lower()), None)
contact_col = next((c for c in df_bronze.columns if 'contact' in c.lower() and 'no' in c.lower()), None)
subcaste_col= next((c for c in df_bronze.columns if 'sub' in c.lower() and 'caste' in c.lower()), None)

print(f"   DOB column     : {dob_col}")
print(f"   Star column    : {star_col}")
print(f"   Contact column : {contact_col}")
print(f"   SubCaste col   : {subcaste_col}")

df_gold = (df_bronze

    # ── Parse DOB fields ──────────────────────────────────────────────────
    .withColumn("birth_year",  udf_year(col(dob_col))   if dob_col  else lit(None).cast(IntegerType()))
    .withColumn("dob",         udf_dob(col(dob_col))    if dob_col  else lit(None))
    .withColumn("birth_place", udf_place(col(dob_col))  if dob_col  else lit(None))

    # ── Parse Star & Rasi ─────────────────────────────────────────────────
    .withColumn("star",        udf_star(col(star_col))  if star_col else lit(None))
    .withColumn("rasi",        udf_rasi(col(star_col))  if star_col else lit(None))

    # ── Parse WhatsApp numbers ────────────────────────────────────────────
    .withColumn("primary_whatsapp", udf_primary_wa(col(contact_col)) if contact_col else lit(None))

    # ── Normalize Sub Caste ───────────────────────────────────────────────
    .withColumn("sub_caste_normalized",
        trim(upper(col(subcaste_col))) if subcaste_col else lit(None))

    # ── Compatibility fields ──────────────────────────────────────────────
    # For each girl's star, what are the best boy stars to look for?
    .withColumn("best_match_stars", udf_compat(col("star")))
    .withColumn("good_match_stars", udf_good(col("star")))
    .withColumn("natural_rasi",     udf_rasi_map(col("star")))

    # ── Audit ─────────────────────────────────────────────────────────────
    .withColumn("gold_timestamp", current_timestamp())
    .withColumn("is_female", col("reg_no").startswith("F"))
    .withColumn("gender", when(col("reg_no").startswith("F"), "Female").otherwise("Male"))
)

# Keep both male and female profiles
# is_female = True  → Female (F prefix)
# is_female = False → Male   (M prefix)
print(f"   Male profiles   : {df_gold.filter(col('is_female') == False).count()}")
print(f"   Female profiles : {df_gold.filter(col('is_female') == True).count()}")

count = df_gold.count()
print(f"\n✅ Gold transformation done! Rows: {count}")

# ── Save Gold table ───────────────────────────────────────────────────────
print(f"\n💾 Saving to Gold table: {GOLD_TABLE}")

(df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

print(f"   ✅ Saved {count} rows to {GOLD_TABLE}")


# ╔══════════════════════════════════════════════════════╗
# ║         SAVE COMPATIBILITY LOOKUP TABLE              ║
# ╚══════════════════════════════════════════════════════╝

print(f"\n💾 Building compatibility lookup table: {COMPAT_TABLE}")

compat_rows = []
for star, info in STAR_COMPATIBILITY.items():
    rasi = info.get("rasi", "")
    for match_star in info.get("utthamam", []):
        compat_rows.append({
            "girl_star"   : star,
            "girl_rasi"   : rasi,
            "boy_star"    : match_star,
            "match_level" : "UTTHAMAM",
            "match_score" : 3,
        })
    for match_star in info.get("madhyamam", []):
        compat_rows.append({
            "girl_star"   : star,
            "girl_rasi"   : rasi,
            "boy_star"    : match_star,
            "match_level" : "MADHYAMAM",
            "match_score" : 2,
        })

df_compat = spark.createDataFrame(compat_rows)
(df_compat.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(COMPAT_TABLE)
)
print(f"   ✅ Saved {df_compat.count()} compatibility rows")


# ╔══════════════════════════════════════════════════════╗
# ║                    SUMMARY                           ║
# ╚══════════════════════════════════════════════════════╝

print(f"\n{'='*55}")
print(f"  ✅ Gold Layer Complete!")
print(f"  📊 Profiles  : {count}")
print(f"  🗄️  Gold     : {GOLD_TABLE}")
print(f"  🗄️  Compat   : {COMPAT_TABLE}")
print(f"{'='*55}")

# Preview
print("\n📋 Sample — Parsed profiles:")
spark.table(GOLD_TABLE).select(
    "reg_no", "name", "dob", "birth_year",
    "star", "rasi", "primary_whatsapp",
    "best_match_stars"
).show(5, truncate=True)

print("\n⭐ Sample — Compatibility for SWATHI:")
spark.table(COMPAT_TABLE).filter(
    col("girl_star") == "SWATHI"
).orderBy("match_score", ascending=False).show(15, truncate=False)