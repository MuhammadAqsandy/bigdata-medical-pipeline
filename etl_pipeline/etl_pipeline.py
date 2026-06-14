"""
=============================================================
  TUGAS BESAR BIG DATA - GENAP 2025/2026
  Topik  : Medical / Kesehatan
  Bagian : Pipeline ETL (Extract - Transform - Load)
=============================================================

Dataset:
  Sumber 1 -> No-Show Appointments (Brazil)
              File: no_show_appointments.csv
  Sumber 2 -> Disease Symptoms and Patient Profile
              File: disease_symptoms.csv

Struktur folder:
  bigdata_final_project/
  ├── etl_pipeline/
  │   └── etl_pipeline.py
  ├── raw/
  ├── datalake/
  └── warehouse/
"""

import os, time, logging, warnings, sqlite3
import numpy as np
import pandas as pd
from datetime import datetime

warnings.filterwarnings("ignore")

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR  = os.path.join(BASE_DIR, "raw")
LAKE_DIR = os.path.join(BASE_DIR, "datalake")
WH_DIR   = os.path.join(BASE_DIR, "warehouse")
LOG_DIR  = os.path.join(BASE_DIR, "etl_pipeline")

for d in [RAW_DIR, LAKE_DIR, WH_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "etl_log.txt"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)
ETL_LOG = []

def log_step(stage, detail):
    ETL_LOG.append({"stage": stage, "timestamp": datetime.now().isoformat(), **detail})
    log.info(f"[{stage}] {detail}")

# =============================================================
# BAGIAN 1 - EXTRACT
# =============================================================

def extract_source1(filepath):
    log.info("=== EXTRACT SOURCE 1: No-Show Appointments ===")
    t0 = time.time()
    df = pd.read_csv(filepath)
    raw_path = os.path.join(RAW_DIR, "raw_no_show.csv")
    df.to_csv(raw_path, index=False)
    log_step("EXTRACT", {"source": "No-Show Appointments", "rows": len(df),
        "columns": len(df.columns), "col_names": list(df.columns),
        "file_size": f"{os.path.getsize(raw_path)/1024:.1f} KB",
        "duration_s": round(time.time()-t0, 2)})
    return df

def extract_source2(filepath):
    log.info("=== EXTRACT SOURCE 2: Disease Symptoms ===")
    t0 = time.time()
    df = pd.read_csv(filepath)
    raw_path = os.path.join(RAW_DIR, "raw_disease_symptoms.csv")
    df.to_csv(raw_path, index=False)
    log_step("EXTRACT", {"source": "Disease Symptoms", "rows": len(df),
        "columns": len(df.columns), "col_names": list(df.columns),
        "file_size": f"{os.path.getsize(raw_path)/1024:.1f} KB",
        "duration_s": round(time.time()-t0, 2)})
    return df

# =============================================================
# BAGIAN 2 - TRANSFORM
# =============================================================

def clean_no_show(df):
    log.info("-- Cleaning: No-Show Appointments --")
    before = len(df)
    df.columns = [c.strip().lower().replace("-","_").replace(" ","_") for c in df.columns]
    id_col = "appointmentid" if "appointmentid" in df.columns else df.columns[1]
    df = df.drop_duplicates(subset=[id_col])
    df = df.dropna(subset=[id_col])
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("Unknown")
    for col in df.select_dtypes(include="number").columns:
        df[col] = df[col].fillna(df[col].median())
    for col in ["scheduledday", "appointmentday"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    date_cols = [c for c in ["scheduledday","appointmentday"] if c in df.columns]
    if date_cols:
        df = df.dropna(subset=date_cols)
    if "age" in df.columns:
        df = df[df["age"] >= 0]
        Q1, Q3 = df["age"].quantile([0.25, 0.75])
        IQR = Q3 - Q1
        df = df[df["age"].between(Q1-1.5*IQR, Q3+1.5*IQR)]
    log_step("CLEAN_NO_SHOW", {"before": before, "after": len(df), "removed": before-len(df)})
    return df

def clean_disease(df):
    log.info("-- Cleaning: Disease Symptoms --")
    before = len(df)
    df.columns = [c.strip().lower().replace(" ","_") for c in df.columns]
    df = df.drop_duplicates()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("Unknown")
    for col in df.select_dtypes(include="number").columns:
        df[col] = df[col].fillna(df[col].median())
    if "age" in df.columns:
        df = df[df["age"] >= 0]
    log_step("CLEAN_DISEASE", {"before": before, "after": len(df), "removed": before-len(df)})
    return df

def standardize_no_show(df):
    log.info("-- Standardisasi: No-Show --")
    if "age" in df.columns:
        mn, mx = df["age"].min(), df["age"].max()
        df["age_norm"] = (df["age"] - mn) / (mx - mn + 1e-9)
    ns_col = [c for c in df.columns if "no" in c and "show" in c]
    if ns_col:
        df["no_show_flag"] = df[ns_col[0]].str.strip().str.lower().map(
            {"yes":1,"no":0}).fillna(0).astype(int)
    if "gender" in df.columns:
        df["gender_code"] = df["gender"].str.strip().str.upper().map(
            {"M":0,"F":1}).fillna(-1).astype(int)
    df["source"] = "no_show"
    log_step("STANDARDIZE_NO_SHOW", {"rows": len(df)})
    return df

def standardize_disease(df):
    log.info("-- Standardisasi: Disease --")
    if "age" in df.columns:
        mn, mx = df["age"].min(), df["age"].max()
        df["age_norm"] = (df["age"] - mn) / (mx - mn + 1e-9)
    outcome_col = [c for c in df.columns if "outcome" in c]
    if outcome_col:
        df["outcome_code"] = df[outcome_col[0]].str.strip().str.lower().map(
            {"positive":1,"negative":0}).fillna(0).astype(int)
    if "gender" in df.columns:
        df["gender_code"] = df["gender"].str.strip().str.lower().map(
            {"male":0,"female":1}).fillna(-1).astype(int)
    df["source"] = "disease"
    log_step("STANDARDIZE_DISEASE", {"rows": len(df)})
    return df

def enrich_and_merge(ns, ds):
    log.info("-- Enrichment & Feature Engineering --")

    # No-Show features
    if "scheduledday" in ns.columns and "appointmentday" in ns.columns:
        ns["wait_days"]        = (ns["appointmentday"]-ns["scheduledday"]).dt.total_seconds().div(86400).abs().round(1)
        ns["appt_day_of_week"] = ns["appointmentday"].dt.dayofweek
        ns["appt_month"]       = ns["appointmentday"].dt.month
        ns["appt_hour"]        = ns["scheduledday"].dt.hour
        ns["is_weekend_appt"]  = ns["appt_day_of_week"].isin([5,6]).astype(int)
    med_cols = [c for c in ns.columns if c in
                ["hipertension","diabetes","alcoholism","handcap","scholarship"]]
    ns["comorbidity_count"] = ns[med_cols].apply(
        pd.to_numeric, errors="coerce").fillna(0).sum(axis=1).astype(int)

    # Disease features
    bool_cols = [c for c in ds.columns if ds[c].dtype==object and
                 ds[c].str.lower().isin(["yes","no"]).all()]
    ds["symptom_count"] = ds[bool_cols].apply(lambda col: col.str.lower().eq("yes")).sum(axis=1)
    fever_col  = [c for c in ds.columns if "fever" in c]
    ds["has_fever"] = (ds[fever_col[0]].str.lower()=="yes").astype(int) if fever_col else 0
    cough_col  = [c for c in ds.columns if "cough" in c]
    ds["has_cough"] = (ds[cough_col[0]].str.lower()=="yes").astype(int) if cough_col else 0
    bp_col = [c for c in ds.columns if "blood" in c and "pressure" in c]
    if bp_col:
        ds["bp_encoded"] = ds[bp_col[0]].str.lower().map({"low":0,"normal":1,"high":2}).fillna(1)
    chol_col = [c for c in ds.columns if "cholesterol" in c]
    if chol_col:
        ds["cholesterol_encoded"] = ds[chol_col[0]].str.lower().map({"low":0,"normal":1,"high":2}).fillna(1)

    # Seragamkan kolom
    def make_std(src_df, source_name, id_offset=0):
        s = pd.DataFrame()
        s["record_id"]         = range(id_offset+1, id_offset+len(src_df)+1)
        s["source"]            = source_name
        s["age"]               = src_df.get("age", np.nan)
        s["age_norm"]          = src_df.get("age_norm", np.nan)
        s["gender_code"]       = src_df.get("gender_code", -1)
        s["outcome_flag"]      = src_df.get("no_show_flag" if source_name=="no_show" else "outcome_code", 0)
        s["comorbidity_count"] = src_df.get("comorbidity_count", np.nan)
        s["wait_days"]         = src_df.get("wait_days", np.nan)
        s["appt_month"]        = src_df.get("appt_month", np.nan)
        s["appt_day_of_week"]  = src_df.get("appt_day_of_week", np.nan)
        s["is_weekend"]        = src_df.get("is_weekend_appt", np.nan)
        s["symptom_count"]     = src_df.get("symptom_count", np.nan)
        s["has_fever"]         = src_df.get("has_fever", np.nan)
        s["has_cough"]         = src_df.get("has_cough", np.nan)
        s["bp_encoded"]        = src_df.get("bp_encoded", np.nan)
        s["cholesterol_enc"]   = src_df.get("cholesterol_encoded", np.nan)
        disease_col = [c for c in src_df.columns if "disease" in c]
        s["disease_name"]      = src_df[disease_col[0]].values if disease_col else "N/A"
        return s

    ns_std = make_std(ns, "no_show", 0)
    ds_std = make_std(ds, "disease", len(ns))
    merged = pd.concat([ns_std, ds_std], ignore_index=True)
    merged["record_id"] = range(1, len(merged)+1)

    log_step("ENRICHMENT", {"no_show_rows": len(ns_std), "disease_rows": len(ds_std),
        "merged_rows": len(merged), "total_cols": len(merged.columns)})
    return merged

def validate_data(df):
    log.info("-- Validasi Kualitas Data --")
    issues = []
    dup = df["record_id"].duplicated().sum()
    if dup > 0:
        issues.append(f"Uniqueness: {dup} duplikat record_id")
        df = df.drop_duplicates(subset=["record_id"])
    for col in ["record_id","source","age"]:
        n = df[col].isna().sum()
        if n > 0:
            issues.append(f"Null check: {n} null di '{col}'")
    df = df.dropna(subset=["record_id","source"])
    before = len(df)
    df = df[(df["age"].isna()) | (df["age"].between(0,120))]
    if before-len(df) > 0:
        issues.append(f"Range check: {before-len(df)} baris age di luar [0,120]")
    df["record_id"]    = pd.to_numeric(df["record_id"], errors="coerce")
    df["gender_code"]  = pd.to_numeric(df["gender_code"], errors="coerce").fillna(-1).astype(int)
    df["outcome_flag"] = pd.to_numeric(df["outcome_flag"], errors="coerce").fillna(0).astype(int)
    bad = df[~df["source"].isin({"no_show","disease"})].shape[0]
    if bad > 0:
        issues.append(f"Referential: {bad} source tidak valid")
        df = df[df["source"].isin({"no_show","disease"})]
    dist = df["source"].value_counts(normalize=True).round(4).to_dict()
    issues.append(f"Distribusi source: {dist}")
    for issue in issues:
        log.warning(f"  VALIDASI -> {issue}")
    log_step("VALIDATION", {"final_rows": len(df), "final_cols": len(df.columns), "issues": issues})
    return df

# =============================================================
# BAGIAN 3 - LOAD
# =============================================================

def load_to_warehouse(df):
    log.info("=== LOAD: Data Warehouse (SQLite - Star Schema) ===")
    t0 = time.time()
    db_path = os.path.join(WH_DIR, "warehouse.db")
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    cur.executescript("""
    DROP TABLE IF EXISTS fact_medical;
    DROP TABLE IF EXISTS dim_source;
    DROP TABLE IF EXISTS dim_outcome;
    DROP TABLE IF EXISTS dim_age_group;

    CREATE TABLE dim_source (
        source_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT NOT NULL UNIQUE,
        description TEXT
    );
    CREATE TABLE dim_outcome (
        outcome_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        outcome_label TEXT NOT NULL UNIQUE
    );
    CREATE TABLE dim_age_group (
        age_group_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        age_group_name TEXT NOT NULL UNIQUE,
        age_min        INTEGER,
        age_max        INTEGER
    );
    CREATE TABLE fact_medical (
        fact_id           INTEGER PRIMARY KEY AUTOINCREMENT,
        record_id         INTEGER,
        source_id         INTEGER REFERENCES dim_source(source_id),
        outcome_id        INTEGER REFERENCES dim_outcome(outcome_id),
        age_group_id      INTEGER REFERENCES dim_age_group(age_group_id),
        age               REAL,
        age_norm          REAL,
        gender_code       INTEGER,
        outcome_flag      INTEGER,
        comorbidity_count REAL,
        wait_days         REAL,
        appt_month        REAL,
        appt_day_of_week  REAL,
        is_weekend        REAL,
        symptom_count     REAL,
        has_fever         REAL,
        has_cough         REAL,
        bp_encoded        REAL,
        cholesterol_enc   REAL,
        disease_name      TEXT
    );
    """)

    for name, desc in [("no_show","No-Show Appointments Brazil"),("disease","Disease Symptoms & Patient Profile")]:
        cur.execute("INSERT OR IGNORE INTO dim_source(source_name,description) VALUES(?,?)", (name,desc))
    for label in ["No-Show / Negative","Show / Positive"]:
        cur.execute("INSERT OR IGNORE INTO dim_outcome(outcome_label) VALUES(?)", (label,))
    for name,mn,mx in [("0-12 (Anak)",0,12),("13-17 (Remaja)",13,17),
                        ("18-35 (Dewasa Muda)",18,35),("36-59 (Dewasa)",36,59),("60+ (Lansia)",60,120)]:
        cur.execute("INSERT OR IGNORE INTO dim_age_group(age_group_name,age_min,age_max) VALUES(?,?,?)",(name,mn,mx))
    conn.commit()

    src_map = {r[1]:r[0] for r in cur.execute("SELECT source_id,source_name FROM dim_source")}
    age_groups_db = list(cur.execute("SELECT age_group_id,age_min,age_max FROM dim_age_group ORDER BY age_min"))

    def get_age_group(age):
        if pd.isna(age): return 3
        for gid,mn,mx in age_groups_db:
            if mn <= age <= mx: return gid
        return 3

    rows = []
    for _, r in df.iterrows():
        rows.append((
            int(r["record_id"]), src_map.get(r["source"],1),
            2 if r["outcome_flag"]==1 else 1,
            get_age_group(r["age"]),
            float(r["age"]) if pd.notna(r["age"]) else None,
            float(r["age_norm"]) if pd.notna(r["age_norm"]) else None,
            int(r["gender_code"]),
            int(r["outcome_flag"]),
            float(r["comorbidity_count"]) if pd.notna(r["comorbidity_count"]) else None,
            float(r["wait_days"]) if pd.notna(r["wait_days"]) else None,
            float(r["appt_month"]) if pd.notna(r["appt_month"]) else None,
            float(r["appt_day_of_week"]) if pd.notna(r["appt_day_of_week"]) else None,
            float(r["is_weekend"]) if pd.notna(r["is_weekend"]) else None,
            float(r["symptom_count"]) if pd.notna(r["symptom_count"]) else None,
            float(r["has_fever"]) if pd.notna(r["has_fever"]) else None,
            float(r["has_cough"]) if pd.notna(r["has_cough"]) else None,
            float(r["bp_encoded"]) if pd.notna(r["bp_encoded"]) else None,
            float(r["cholesterol_enc"]) if pd.notna(r["cholesterol_enc"]) else None,
            str(r["disease_name"]),
        ))

    cur.executemany("""INSERT INTO fact_medical(record_id,source_id,outcome_id,age_group_id,
        age,age_norm,gender_code,outcome_flag,comorbidity_count,wait_days,appt_month,
        appt_day_of_week,is_weekend,symptom_count,has_fever,has_cough,bp_encoded,
        cholesterol_enc,disease_name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()

    log_step("LOAD", {"database": db_path, "rows_loaded": len(rows),
        "duration_s": round(time.time()-t0,2), "status": "SUCCESS"})

    log.info("=== 8 Query SQL Analitik ===")
    queries = {
        "Q1 Total data per sumber":
            "SELECT s.source_name, COUNT(*) as total FROM fact_medical f JOIN dim_source s ON f.source_id=s.source_id GROUP BY s.source_name",
        "Q2 Distribusi outcome":
            "SELECT o.outcome_label, COUNT(*) as total FROM fact_medical f JOIN dim_outcome o ON f.outcome_id=o.outcome_id GROUP BY o.outcome_label",
        "Q3 Rata-rata usia per outcome":
            "SELECT o.outcome_label, ROUND(AVG(f.age),2) as avg_age FROM fact_medical f JOIN dim_outcome o ON f.outcome_id=o.outcome_id WHERE f.age IS NOT NULL GROUP BY o.outcome_label",
        "Q4 Distribusi per kelompok usia":
            "SELECT ag.age_group_name, COUNT(*) as total FROM fact_medical f JOIN dim_age_group ag ON f.age_group_id=ag.age_group_id GROUP BY ag.age_group_name ORDER BY ag.age_min",
        "Q5 No-show rate per bulan":
            "SELECT appt_month, ROUND(AVG(outcome_flag),4) as no_show_rate FROM fact_medical WHERE source_id=1 AND appt_month IS NOT NULL GROUP BY appt_month ORDER BY appt_month",
        "Q6 Rata-rata comorbidity per kelompok usia":
            "SELECT ag.age_group_name, ROUND(AVG(f.comorbidity_count),2) as avg_comorbidity FROM fact_medical f JOIN dim_age_group ag ON f.age_group_id=ag.age_group_id WHERE f.comorbidity_count IS NOT NULL GROUP BY ag.age_group_name",
        "Q7 No-show rate hari kerja vs akhir pekan":
            "SELECT is_weekend, ROUND(AVG(outcome_flag),4) as no_show_rate, COUNT(*) as total FROM fact_medical WHERE source_id=1 AND is_weekend IS NOT NULL GROUP BY is_weekend",
        "Q8 Top 10 penyakit dari dataset Disease":
            "SELECT disease_name, COUNT(*) as total FROM fact_medical WHERE source_id=2 GROUP BY disease_name ORDER BY total DESC LIMIT 10",
    }
    for label, sql in queries.items():
        result = pd.read_sql_query(sql, conn)
        log.info(f"\n{label}:\n{result.to_string(index=False)}\n")

    conn.close()
    log.info("ETL selesai. Database: " + db_path)

# =============================================================
# MAIN
# =============================================================

def run_etl(no_show_path, disease_path):
    log.info("=== MULAI PIPELINE ETL - Big Data Medical ===")
    ns_raw   = extract_source1(no_show_path)
    ds_raw   = extract_source2(disease_path)
    ns_clean = clean_no_show(ns_raw)
    ds_clean = clean_disease(ds_raw)
    ns_std   = standardize_no_show(ns_clean)
    ds_std   = standardize_disease(ds_clean)
    merged   = enrich_and_merge(ns_std, ds_std)
    final    = validate_data(merged)
    final.to_csv(os.path.join(LAKE_DIR, "transformed_data.csv"), index=False)
    log.info("Data disimpan ke datalake.")
    load_to_warehouse(final)
    pd.DataFrame(ETL_LOG).to_csv(os.path.join(LOG_DIR, "etl_summary.csv"), index=False)
    log.info("=== PIPELINE ETL SELESAI ===")

if __name__ == "__main__":
    NO_SHOW_PATH = "../raw/no_show_appointments.csv"
    DISEASE_PATH = "../raw/disease_symptoms.csv"
    run_etl(NO_SHOW_PATH, DISEASE_PATH)
