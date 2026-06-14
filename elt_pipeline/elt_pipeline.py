"""
=============================================================
  TUGAS BESAR BIG DATA - GENAP 2025/2026
  Topik  : Medical / Kesehatan
  Bagian : Pipeline ELT (Extract - Load - Transform)
=============================================================

Dataset:
  Sumber 1 -> No-Show Appointments (Brazil): no_show_appointments.csv
  Sumber 2 -> Disease Symptoms and Patient Profile: disease_symptoms.csv
"""

import os, time, logging, warnings, sqlite3
import numpy as np
import pandas as pd
from datetime import datetime

warnings.filterwarnings("ignore")

BASE_DIR  = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR   = os.path.join(BASE_DIR, "raw")
LAKE_DIR  = os.path.join(BASE_DIR, "datalake")
LAKE_NS   = os.path.join(LAKE_DIR, "raw_no_show")
LAKE_DS   = os.path.join(LAKE_DIR, "raw_disease")
WH_DIR    = os.path.join(BASE_DIR, "warehouse")
LOG_DIR   = os.path.join(BASE_DIR, "elt_pipeline")

for d in [RAW_DIR, LAKE_NS, LAKE_DS, WH_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "elt_log.txt"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)
ELT_LOG = []

def log_step(stage, detail):
    ELT_LOG.append({"stage": stage, "timestamp": datetime.now().isoformat(), **detail})
    log.info(f"[{stage}] {detail}")

# =============================================================
# BAGIAN 1 - EXTRACT (mentah, tanpa preprocessing)
# =============================================================

def extract_raw_no_show(filepath):
    log.info("=== EXTRACT (RAW): No-Show Appointments ===")
    t0 = time.time()
    df = pd.read_csv(filepath)
    log_step("EXTRACT_RAW", {"source": "No-Show Appointments", "rows": len(df),
        "columns": list(df.columns), "duration": round(time.time()-t0, 2)})
    return df

def extract_raw_disease(filepath):
    log.info("=== EXTRACT (RAW): Disease Symptoms ===")
    t0 = time.time()
    df = pd.read_csv(filepath)
    log_step("EXTRACT_RAW", {"source": "Disease Symptoms", "rows": len(df),
        "columns": list(df.columns), "duration": round(time.time()-t0, 2)})
    return df

# =============================================================
# BAGIAN 2 - LOAD (ke Data Lake & Staging Warehouse)
# =============================================================

def load_to_datalake(ns_raw, ds_raw):
    log.info("=== LOAD: Data Lake (staging area) ===")
    t0 = time.time()
    ns_path = os.path.join(LAKE_NS, "no_show_raw.csv")
    ds_path = os.path.join(LAKE_DS, "disease_raw.csv")
    ns_raw.to_csv(ns_path, index=False)
    ds_raw.to_csv(ds_path, index=False)
    log_step("LOAD_DATALAKE", {"no_show_path": ns_path, "disease_path": ds_path,
        "no_show_rows": len(ns_raw), "disease_rows": len(ds_raw),
        "duration": round(time.time()-t0, 2)})

def load_raw_to_warehouse(ns_raw, ds_raw):
    log.info("=== LOAD: Warehouse Staging Tables ===")
    t0 = time.time()
    db_path = os.path.join(WH_DIR, "warehouse_elt.db")
    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE IF EXISTS stg_no_show")
    conn.execute("DROP TABLE IF EXISTS stg_disease")
    conn.commit()
    ns_raw.to_sql("stg_no_show", conn, if_exists="replace", index=False)
    ds_raw.to_sql("stg_disease", conn, if_exists="replace", index=False)
    log_step("LOAD_STAGING", {"database": db_path,
        "stg_no_show": len(ns_raw), "stg_disease": len(ds_raw),
        "duration": round(time.time()-t0, 2)})
    return conn

# =============================================================
# BAGIAN 3 - TRANSFORM (in-database SQL)
# =============================================================

def transform_in_warehouse(conn):
    log.info("=== TRANSFORM (in-warehouse SQL) ===")

    # ── Step 1: Tabel Dimensi ──────────────────────────────────
    log.info("-- Step 1: Tabel Dimensi --")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS dim_source (
        source_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT NOT NULL UNIQUE
    );
    INSERT OR IGNORE INTO dim_source(source_name) VALUES('no_show');
    INSERT OR IGNORE INTO dim_source(source_name) VALUES('disease');

    CREATE TABLE IF NOT EXISTS dim_outcome (
        outcome_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        outcome_label TEXT NOT NULL UNIQUE
    );
    INSERT OR IGNORE INTO dim_outcome(outcome_label) VALUES('No-Show / Negative');
    INSERT OR IGNORE INTO dim_outcome(outcome_label) VALUES('Show / Positive');

    CREATE TABLE IF NOT EXISTS dim_age_group (
        age_group_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        age_group_name TEXT NOT NULL UNIQUE,
        age_min        INTEGER,
        age_max        INTEGER
    );
    INSERT OR IGNORE INTO dim_age_group(age_group_name,age_min,age_max)
        VALUES('0-12 (Anak)',0,12),('13-17 (Remaja)',13,17),
              ('18-35 (Dewasa Muda)',18,35),('36-59 (Dewasa)',36,59),
              ('60+ (Lansia)',60,120);
    """)
    conn.commit()
    log_step("TRANSFORM_DIM", {"status": "dim_source, dim_outcome, dim_age_group created"})

    # ── Step 2: Cleaning No-Show via SQL ──────────────────────
    log.info("-- Step 2: Cleaning No-Show (SQL) --")

    # Deteksi nama kolom aktual dari staging
    cols = [r[1] for r in conn.execute("PRAGMA table_info(stg_no_show)")]
    log.info(f"Kolom stg_no_show: {cols}")

    # Cari kolom no-show dan appointment id secara fleksibel
    noshow_col = next((c for c in cols if "no" in c.lower() and "show" in c.lower()), None)
    appt_col   = next((c for c in cols if "appointment" in c.lower() and "id" in c.lower()), cols[1] if len(cols)>1 else cols[0])
    sched_col  = next((c for c in cols if "scheduled" in c.lower()), None)
    apptday_col= next((c for c in cols if "appointmentday" in c.lower()), None)
    gender_col = next((c for c in cols if "gender" in c.lower()), None)
    age_col    = next((c for c in cols if c.lower()=="age"), None)

    conn.execute("DROP TABLE IF EXISTS clean_no_show")

    noshow_expr = f"""CASE WHEN LOWER(TRIM("{noshow_col}"))='yes' THEN 1 ELSE 0 END""" if noshow_col else "0"
    gender_expr = f"""CASE WHEN UPPER(TRIM("{gender_col}"))='F' THEN 1 WHEN UPPER(TRIM("{gender_col}"))='M' THEN 0 ELSE -1 END""" if gender_col else "-1"
    age_expr    = f'CAST("{age_col}" AS REAL)' if age_col else "NULL"
    sched_expr  = f'"{sched_col}"' if sched_col else "NULL"
    apptday_expr= f'"{apptday_col}"' if apptday_col else "NULL"

    conn.execute(f"""
    CREATE TABLE clean_no_show AS
    SELECT
        "{appt_col}"                                   AS appointment_id,
        {age_expr}                                     AS age,
        ROUND(({age_expr} - (SELECT MIN("{age_col}") FROM stg_no_show)) /
              (SELECT MAX("{age_col}") - MIN("{age_col}") + 0.0001 FROM stg_no_show), 4)
                                                       AS age_norm,
        {gender_expr}                                  AS gender_code,
        {noshow_expr}                                  AS no_show_flag,
        {sched_expr}                                   AS scheduled_day,
        {apptday_expr}                                 AS appointment_day,
        'no_show'                                      AS source
    FROM stg_no_show
    WHERE "{appt_col}" IS NOT NULL
      AND {age_expr} BETWEEN 0 AND 120
    GROUP BY "{appt_col}"
    HAVING "{appt_col}" = MIN("{appt_col}")
    """)
    conn.commit()
    cnt = conn.execute("SELECT COUNT(*) FROM clean_no_show").fetchone()[0]
    log_step("TRANSFORM_CLEAN_NO_SHOW", {"rows_after_clean": cnt})

    # ── Step 3: Cleaning Disease via SQL ──────────────────────
    log.info("-- Step 3: Cleaning Disease (SQL) --")
    cols_ds = [r[1] for r in conn.execute("PRAGMA table_info(stg_disease)")]
    log.info(f"Kolom stg_disease: {cols_ds}")

    disease_col  = next((c for c in cols_ds if "disease" in c.lower()), cols_ds[0])
    age_col_ds   = next((c for c in cols_ds if c.lower()=="age"), None)
    gender_col_ds= next((c for c in cols_ds if "gender" in c.lower()), None)
    outcome_col  = next((c for c in cols_ds if "outcome" in c.lower()), None)
    fever_col    = next((c for c in cols_ds if "fever" in c.lower()), None)
    cough_col    = next((c for c in cols_ds if "cough" in c.lower()), None)
    bp_col       = next((c for c in cols_ds if "blood" in c.lower() and "pressure" in c.lower()), None)
    chol_col     = next((c for c in cols_ds if "cholesterol" in c.lower()), None)

    conn.execute("DROP TABLE IF EXISTS clean_disease")

    age_ds_expr    = f'CAST("{age_col_ds}" AS REAL)' if age_col_ds else "NULL"
    gender_ds_expr = f"""CASE WHEN LOWER(TRIM("{gender_col_ds}"))='female' THEN 1
                              WHEN LOWER(TRIM("{gender_col_ds}"))='male' THEN 0 ELSE -1 END""" if gender_col_ds else "-1"
    outcome_expr   = f"""CASE WHEN LOWER(TRIM("{outcome_col}"))='positive' THEN 1 ELSE 0 END""" if outcome_col else "0"
    fever_expr     = f"""CASE WHEN LOWER(TRIM("{fever_col}"))='yes' THEN 1 ELSE 0 END""" if fever_col else "0"
    cough_expr     = f"""CASE WHEN LOWER(TRIM("{cough_col}"))='yes' THEN 1 ELSE 0 END""" if cough_col else "0"
    bp_expr        = f"""CASE WHEN LOWER(TRIM("{bp_col}"))='low' THEN 0
                              WHEN LOWER(TRIM("{bp_col}"))='high' THEN 2 ELSE 1 END""" if bp_col else "1"
    chol_expr      = f"""CASE WHEN LOWER(TRIM("{chol_col}"))='low' THEN 0
                              WHEN LOWER(TRIM("{chol_col}"))='high' THEN 2 ELSE 1 END""" if chol_col else "1"

    conn.execute(f"""
    CREATE TABLE clean_disease AS
    SELECT
        "{disease_col}"                                AS disease_name,
        {age_ds_expr}                                  AS age,
        ROUND(({age_ds_expr} - (SELECT MIN("{age_col_ds}") FROM stg_disease)) /
              (SELECT MAX("{age_col_ds}") - MIN("{age_col_ds}") + 0.0001 FROM stg_disease), 4)
                                                       AS age_norm,
        {gender_ds_expr}                               AS gender_code,
        {outcome_expr}                                 AS outcome_flag,
        {fever_expr}                                   AS has_fever,
        {cough_expr}                                   AS has_cough,
        {bp_expr}                                      AS bp_encoded,
        {chol_expr}                                    AS cholesterol_enc,
        'disease'                                      AS source
    FROM stg_disease
    WHERE "{disease_col}" IS NOT NULL
    GROUP BY "{disease_col}", {age_ds_expr}, {gender_ds_expr}
    """)
    conn.commit()
    cnt = conn.execute("SELECT COUNT(*) FROM clean_disease").fetchone()[0]
    log_step("TRANSFORM_CLEAN_DISEASE", {"rows_after_clean": cnt})

    # ── Step 4: Feature Engineering via SQL ───────────────────
    log.info("-- Step 4: Feature Engineering (SQL) --")
    conn.executescript("""
    DROP TABLE IF EXISTS enriched_no_show;
    CREATE TABLE enriched_no_show AS
    SELECT *,
        CASE
            WHEN scheduled_day IS NOT NULL AND appointment_day IS NOT NULL
            THEN ABS(JULIANDAY(appointment_day) - JULIANDAY(scheduled_day))
            ELSE NULL
        END AS wait_days,
        CAST(STRFTIME('%m', appointment_day) AS INTEGER)   AS appt_month,
        CAST(STRFTIME('%w', appointment_day) AS INTEGER)   AS appt_day_of_week,
        CASE WHEN CAST(STRFTIME('%w', appointment_day) AS INTEGER) IN (0,6) THEN 1 ELSE 0 END
                                                           AS is_weekend,
        ROUND(age / (SELECT MAX(age)+0.0001 FROM clean_no_show), 4) AS age_norm_elt
    FROM clean_no_show;

    DROP TABLE IF EXISTS enriched_disease;
    CREATE TABLE enriched_disease AS
    SELECT *,
        (has_fever + has_cough)                            AS symptom_count,
        ROUND(age / (SELECT MAX(age)+0.0001 FROM clean_disease), 4) AS age_norm_elt
    FROM clean_disease;
    """)
    conn.commit()
    log_step("TRANSFORM_ENRICHMENT", {"status": "enriched_no_show & enriched_disease created"})

    # ── Step 5: Fact Table ─────────────────────────────────────
    log.info("-- Step 5: Buat fact_medical_elt --")
    conn.executescript("""
    DROP TABLE IF EXISTS fact_medical_elt;
    CREATE TABLE fact_medical_elt AS
    SELECT
        ROW_NUMBER() OVER ()         AS fact_id,
        p.source_id,
        CASE WHEN no_show_flag=1 THEN 1 ELSE 2 END AS outcome_id,
        CASE
            WHEN age BETWEEN 0  AND 12  THEN 1
            WHEN age BETWEEN 13 AND 17  THEN 2
            WHEN age BETWEEN 18 AND 35  THEN 3
            WHEN age BETWEEN 36 AND 59  THEN 4
            ELSE 5
        END AS age_group_id,
        age, age_norm, gender_code,
        no_show_flag   AS outcome_flag,
        NULL           AS comorbidity_count,
        wait_days, appt_month, appt_day_of_week, is_weekend,
        NULL AS symptom_count,
        NULL AS has_fever,
        NULL AS has_cough,
        NULL AS bp_encoded,
        NULL AS cholesterol_enc,
        'N/A' AS disease_name
    FROM enriched_no_show e
    JOIN dim_source p ON p.source_name='no_show'

    UNION ALL

    SELECT
        ROW_NUMBER() OVER ()         AS fact_id,
        p.source_id,
        CASE WHEN outcome_flag=1 THEN 2 ELSE 1 END AS outcome_id,
        CASE
            WHEN age BETWEEN 0  AND 12  THEN 1
            WHEN age BETWEEN 13 AND 17  THEN 2
            WHEN age BETWEEN 18 AND 35  THEN 3
            WHEN age BETWEEN 36 AND 59  THEN 4
            ELSE 5
        END AS age_group_id,
        age, age_norm, gender_code,
        outcome_flag,
        NULL AS comorbidity_count,
        NULL AS wait_days,
        NULL AS appt_month,
        NULL AS appt_day_of_week,
        NULL AS is_weekend,
        symptom_count, has_fever, has_cough, bp_encoded, cholesterol_enc,
        disease_name
    FROM enriched_disease e
    JOIN dim_source p ON p.source_name='disease';
    """)
    conn.commit()
    cnt = conn.execute("SELECT COUNT(*) FROM fact_medical_elt").fetchone()[0]
    log_step("TRANSFORM_FACT", {"fact_medical_elt_rows": cnt})

    # ── Step 6: Validasi 6 Aturan via SQL ────────────────────
    log.info("-- Step 6: Validasi Kualitas (SQL) --")
    checks = {
        "V1 Total baris fact":       "SELECT COUNT(*) AS total FROM fact_medical_elt",
        "V2 Null age":               "SELECT COUNT(*) AS null_age FROM fact_medical_elt WHERE age IS NULL",
        "V3 Age di luar [0,120]":    "SELECT COUNT(*) AS out_range FROM fact_medical_elt WHERE age NOT BETWEEN 0 AND 120",
        "V4 Source tidak valid":     "SELECT COUNT(*) AS invalid FROM fact_medical_elt WHERE source_id NOT IN (SELECT source_id FROM dim_source)",
        "V5 Distribusi per source":  "SELECT s.source_name, COUNT(*) AS total FROM fact_medical_elt f JOIN dim_source s ON f.source_id=s.source_id GROUP BY s.source_name",
        "V6 Distribusi outcome":     "SELECT o.outcome_label, COUNT(*) AS total FROM fact_medical_elt f JOIN dim_outcome o ON f.outcome_id=o.outcome_id GROUP BY o.outcome_label",
    }
    for label, sql in checks.items():
        result = pd.read_sql_query(sql, conn)
        log.info(f"\n{label}:\n{result.to_string(index=False)}")
    log_step("TRANSFORM_VALIDATION", {"checks_run": len(checks)})

    # ── Step 7: 8 Query Analitik ──────────────────────────────
    log.info("=== 8 Query SQL Analitik ===")
    queries = {
        "Q1 Total data per sumber":
            "SELECT s.source_name, COUNT(*) AS total FROM fact_medical_elt f JOIN dim_source s ON f.source_id=s.source_id GROUP BY s.source_name",
        "Q2 Distribusi outcome":
            "SELECT o.outcome_label, COUNT(*) AS total FROM fact_medical_elt f JOIN dim_outcome o ON f.outcome_id=o.outcome_id GROUP BY o.outcome_label",
        "Q3 Rata-rata usia per outcome":
            "SELECT o.outcome_label, ROUND(AVG(f.age),2) AS avg_age FROM fact_medical_elt f JOIN dim_outcome o ON f.outcome_id=o.outcome_id WHERE f.age IS NOT NULL GROUP BY o.outcome_label",
        "Q4 Distribusi kelompok usia":
            "SELECT ag.age_group_name, COUNT(*) AS total FROM fact_medical_elt f JOIN dim_age_group ag ON f.age_group_id=ag.age_group_id GROUP BY ag.age_group_name ORDER BY ag.age_min",
        "Q5 No-show rate per bulan":
            "SELECT appt_month, ROUND(AVG(outcome_flag),4) AS no_show_rate FROM fact_medical_elt WHERE source_id=1 AND appt_month IS NOT NULL GROUP BY appt_month ORDER BY appt_month",
        "Q6 No-show rate hari kerja vs akhir pekan":
            "SELECT is_weekend, ROUND(AVG(outcome_flag),4) AS no_show_rate, COUNT(*) AS total FROM fact_medical_elt WHERE source_id=1 AND is_weekend IS NOT NULL GROUP BY is_weekend",
        "Q7 Distribusi penyakit (Disease dataset)":
            "SELECT disease_name, COUNT(*) AS total FROM fact_medical_elt WHERE source_id=2 GROUP BY disease_name ORDER BY total DESC LIMIT 10",
        "Q8 Rata-rata wait_days per outcome":
            "SELECT o.outcome_label, ROUND(AVG(f.wait_days),2) AS avg_wait FROM fact_medical_elt f JOIN dim_outcome o ON f.outcome_id=o.outcome_id WHERE f.wait_days IS NOT NULL GROUP BY o.outcome_label",
    }
    for label, sql in queries.items():
        result = pd.read_sql_query(sql, conn)
        log.info(f"\n{label}:\n{result.to_string(index=False)}\n")

    conn.commit()
    log.info("=== Semua transformasi ELT selesai. ===")

# =============================================================
# MAIN
# =============================================================

def run_elt(no_show_path, disease_path):
    log.info("=== MULAI PIPELINE ELT - Big Data Medical ===")
    ns_raw = extract_raw_no_show(no_show_path)
    ds_raw = extract_raw_disease(disease_path)
    load_to_datalake(ns_raw, ds_raw)
    conn = load_raw_to_warehouse(ns_raw, ds_raw)
    transform_in_warehouse(conn)
    conn.close()
    pd.DataFrame(ELT_LOG).to_csv(os.path.join(LOG_DIR, "elt_summary.csv"), index=False)
    log.info("=== PIPELINE ELT SELESAI ===")

if __name__ == "__main__":
    NO_SHOW_PATH = "../raw/no_show_appointments.csv"
    DISEASE_PATH = "../raw/disease_symptoms.csv"
    run_elt(NO_SHOW_PATH, DISEASE_PATH)
