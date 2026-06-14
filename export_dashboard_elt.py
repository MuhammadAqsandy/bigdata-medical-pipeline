"""
=============================================================
  TUGAS BESAR BIG DATA - GENAP 2025/2026
  Export Data Dashboard dari Pipeline ELT
  Source: warehouse_elt.db
=============================================================

Jalankan SETELAH elt_pipeline.py berhasil dijalankan.
Output: folder dashboard_data_elt/ berisi CSV dari warehouse ELT.

Cara pakai:
  cd bigdata_final_project
  python export_dashboard_elt.py
"""

import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(__file__)
WH_PATH  = os.path.join(BASE_DIR, "warehouse", "warehouse_elt.db")
OUT_DIR  = os.path.join(BASE_DIR, "dashboard_data_elt")
os.makedirs(OUT_DIR, exist_ok=True)

conn = sqlite3.connect(WH_PATH)
print(f"Terhubung ke: {WH_PATH}")

queries = {

    # KPI Summary
    "kpi_summary_elt": """
        SELECT
            COUNT(*)                                          AS total_pasien,
            ROUND(AVG(outcome_flag), 4)                      AS no_show_rate,
            ROUND(AVG(age), 1)                               AS avg_usia,
            ROUND(AVG(CASE WHEN wait_days IS NOT NULL
                          THEN wait_days END), 1)            AS avg_wait_days,
            SUM(CASE WHEN outcome_flag=1 THEN 1 ELSE 0 END)  AS total_no_show,
            SUM(CASE WHEN outcome_flag=0 THEN 1 ELSE 0 END)  AS total_show,
            'ELT' AS pipeline
        FROM fact_medical_elt
    """,

    # Q1 - Total per sumber
    "q1_total_per_sumber_elt": """
        SELECT s.source_name AS sumber, COUNT(*) AS total, 'ELT' AS pipeline
        FROM fact_medical_elt f
        JOIN dim_source s ON f.source_id = s.source_id
        GROUP BY s.source_name
    """,

    # Q2 - Distribusi outcome
    "q2_distribusi_outcome_elt": """
        SELECT o.outcome_label AS outcome, COUNT(*) AS total,
               ROUND(COUNT(*) * 100.0 /
                     (SELECT COUNT(*) FROM fact_medical_elt), 2) AS persen,
               'ELT' AS pipeline
        FROM fact_medical_elt f
        JOIN dim_outcome o ON f.outcome_id = o.outcome_id
        GROUP BY o.outcome_label
    """,

    # Q3 - Rata-rata usia per outcome
    "q3_usia_per_outcome_elt": """
        SELECT o.outcome_label AS outcome,
               ROUND(AVG(f.age), 2) AS rata_rata_usia,
               MIN(f.age) AS usia_min,
               MAX(f.age) AS usia_max,
               'ELT' AS pipeline
        FROM fact_medical_elt f
        JOIN dim_outcome o ON f.outcome_id = o.outcome_id
        WHERE f.age IS NOT NULL
        GROUP BY o.outcome_label
    """,

    # Q4 - Distribusi kelompok usia
    "q4_distribusi_usia_elt": """
        SELECT ag.age_group_name AS kelompok_usia,
               ag.age_min, ag.age_max,
               COUNT(*) AS total,
               ROUND(AVG(f.outcome_flag), 4) AS no_show_rate,
               'ELT' AS pipeline
        FROM fact_medical_elt f
        JOIN dim_age_group ag ON f.age_group_id = ag.age_group_id
        GROUP BY ag.age_group_name
        ORDER BY ag.age_min
    """,

    # Q5 - No-show rate per bulan
    "q5_noshow_per_bulan_elt": """
        SELECT appt_month AS bulan,
               COUNT(*) AS total,
               SUM(outcome_flag) AS jumlah_no_show,
               ROUND(AVG(outcome_flag) * 100, 2) AS no_show_rate_persen,
               'ELT' AS pipeline
        FROM fact_medical_elt
        WHERE source_id = 1 AND appt_month IS NOT NULL
        GROUP BY appt_month
        ORDER BY appt_month
    """,

    # Q6 - Comorbidity per kelompok usia
    "q6_comorbidity_per_usia_elt": """
        SELECT ag.age_group_name AS kelompok_usia,
               ag.age_min,
               ROUND(AVG(f.comorbidity_count), 3) AS rata_comorbidity,
               COUNT(*) AS total,
               'ELT' AS pipeline
        FROM fact_medical_elt f
        JOIN dim_age_group ag ON f.age_group_id = ag.age_group_id
        WHERE f.comorbidity_count IS NOT NULL
        GROUP BY ag.age_group_name
        ORDER BY ag.age_min
    """,

    # Q7 - No-show hari kerja vs akhir pekan
    "q7_noshow_weekend_elt": """
        SELECT
            CASE WHEN is_weekend = 1 THEN 'Akhir Pekan'
                 ELSE 'Hari Kerja' END AS jenis_hari,
            COUNT(*) AS total,
            ROUND(AVG(outcome_flag) * 100, 2) AS no_show_rate_persen,
            'ELT' AS pipeline
        FROM fact_medical_elt
        WHERE source_id = 1 AND is_weekend IS NOT NULL
        GROUP BY is_weekend
    """,

    # Q8 - Top penyakit
    "q8_top_penyakit_elt": """
        SELECT disease_name AS penyakit,
               COUNT(*) AS jumlah_kasus,
               'ELT' AS pipeline
        FROM fact_medical_elt
        WHERE source_id = 2 AND disease_name != 'N/A'
        GROUP BY disease_name
        ORDER BY jumlah_kasus DESC
        LIMIT 15
    """,

    # Extra - No-show per hari
    "extra_noshow_per_hari_elt": """
        SELECT
            CASE appt_day_of_week
                WHEN 0 THEN 'Minggu' WHEN 1 THEN 'Senin'
                WHEN 2 THEN 'Selasa' WHEN 3 THEN 'Rabu'
                WHEN 4 THEN 'Kamis'  WHEN 5 THEN 'Jumat'
                WHEN 6 THEN 'Sabtu'  ELSE 'Unknown'
            END AS hari,
            appt_day_of_week AS urutan,
            COUNT(*) AS total,
            ROUND(AVG(outcome_flag) * 100, 2) AS no_show_rate_persen,
            'ELT' AS pipeline
        FROM fact_medical_elt
        WHERE source_id = 1 AND appt_day_of_week IS NOT NULL
        GROUP BY appt_day_of_week
        ORDER BY appt_day_of_week
    """,

    # Extra - Wait days distribusi
    "extra_wait_days_elt": """
        SELECT
            CASE
                WHEN wait_days = 0        THEN '0 hari (sama hari)'
                WHEN wait_days <= 7       THEN '1-7 hari'
                WHEN wait_days <= 30      THEN '8-30 hari'
                WHEN wait_days <= 60      THEN '31-60 hari'
                ELSE '> 60 hari'
            END AS kategori_tunggu,
            COUNT(*) AS total,
            ROUND(AVG(outcome_flag) * 100, 2) AS no_show_rate_persen,
            'ELT' AS pipeline
        FROM fact_medical_elt
        WHERE source_id = 1 AND wait_days IS NOT NULL
        GROUP BY kategori_tunggu
        ORDER BY MIN(wait_days)
    """,

    # Extra - Wait days per outcome
    "extra_wait_outcome_elt": """
        SELECT o.outcome_label AS outcome,
               ROUND(AVG(f.wait_days), 2) AS avg_wait,
               'ELT' AS pipeline
        FROM fact_medical_elt f
        JOIN dim_outcome o ON f.outcome_id = o.outcome_id
        WHERE f.wait_days IS NOT NULL
        GROUP BY o.outcome_label
    """,
}

print("\nMengexport data ELT untuk dashboard...\n")
for name, sql in queries.items():
    try:
        df = pd.read_sql_query(sql, conn)
        path = os.path.join(OUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"  [OK] {name}.csv ({len(df)} baris)")
    except Exception as e:
        print(f"  [ERROR] {name}: {e}")

conn.close()
print(f"\nSemua file ELT tersimpan di: {OUT_DIR}")
print("\nLangkah selanjutnya:")
print("  1. Buka https://lookerstudio.google.com")
print("  2. Buat laporan baru, nama: 'Dashboard ELT - Medical'")
print("  3. Upload CSV dari folder dashboard_data_elt/")
print("  4. Buat chart yang sama seperti Dashboard ETL")
