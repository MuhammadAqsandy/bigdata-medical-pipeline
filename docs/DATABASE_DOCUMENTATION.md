# 📊 Dokumentasi Database & Data Warehouse
## Big Data Pipeline – Medical / Kesehatan Masyarakat

---

## 1. Skema Database (Star Schema)

```
                         ┌─────────────────────┐
                         │     dim_source      │
                         ├─────────────────────┤
                         │ PK source_id        │
                         │    source_name      │
                         │    description      │
                         └──────────┬──────────┘
                                    │
┌─────────────────────┐             │             ┌─────────────────────┐
│    dim_outcome      │             │             │   dim_age_group     │
├─────────────────────┤             │             ├─────────────────────┤
│ PK outcome_id       │             │             │ PK age_group_id     │
│    outcome_label    │             │             │    age_group_name   │
└──────────┬──────────┘             │             │    age_min          │
           │                        │             │    age_max          │
           │         ┌──────────────┴──────────┐  └──────────┬──────────┘
           └─────────┤      fact_medical        ├────────────┘
                     ├─────────────────────────┤
                     │ PK fact_id              │
                     │ FK source_id            │
                     │ FK outcome_id           │
                     │ FK age_group_id         │
                     │    record_id            │
                     │    age                  │
                     │    age_norm             │
                     │    gender_code          │
                     │    outcome_flag         │
                     │    comorbidity_count    │
                     │    wait_days            │
                     │    appt_month           │
                     │    appt_day_of_week     │
                     │    is_weekend           │
                     │    symptom_count        │
                     │    has_fever            │
                     │    has_cough            │
                     │    bp_encoded           │
                     │    cholesterol_enc      │
                     │    disease_name         │
                     └─────────────────────────┘
```

---

## 2. Struktur Tabel

### 2.1 fact_medical (Tabel Fakta)

| Kolom | Tipe Data | Keterangan |
|-------|-----------|-----------|
| fact_id | INTEGER (PK) | Primary key auto-increment |
| record_id | INTEGER | ID record unik |
| source_id | INTEGER (FK) | Foreign key ke dim_source |
| outcome_id | INTEGER (FK) | Foreign key ke dim_outcome |
| age_group_id | INTEGER (FK) | Foreign key ke dim_age_group |
| age | REAL | Usia pasien (tahun) |
| age_norm | REAL | Usia ternormalisasi (Min-Max, 0-1) |
| gender_code | INTEGER | 0=Laki-laki, 1=Perempuan, -1=Unknown |
| outcome_flag | INTEGER | 0=Show/Negatif, 1=No-Show/Positif |
| comorbidity_count | REAL | Jumlah penyakit penyerta |
| wait_days | REAL | Hari tunggu antara jadwal & janji |
| appt_month | REAL | Bulan janji dokter (1-12) |
| appt_day_of_week | REAL | Hari dalam seminggu (0=Minggu) |
| is_weekend | REAL | 1=Akhir pekan, 0=Hari kerja |
| symptom_count | REAL | Jumlah gejala klinis |
| has_fever | REAL | 1=Ada demam, 0=Tidak |
| has_cough | REAL | 1=Ada batuk, 0=Tidak |
| bp_encoded | REAL | Tekanan darah: 0=Low, 1=Normal, 2=High |
| cholesterol_enc | REAL | Kolesterol: 0=Low, 1=Normal, 2=High |
| disease_name | TEXT | Nama penyakit (dari Disease dataset) |

### 2.2 dim_source (Dimensi Sumber Data)

| Kolom | Tipe Data | Keterangan |
|-------|-----------|-----------|
| source_id | INTEGER (PK) | Primary key auto-increment |
| source_name | TEXT | Nama sumber: 'no_show' / 'disease' |
| description | TEXT | Deskripsi lengkap sumber data |

**Data:**
| source_id | source_name | description |
|-----------|-------------|-------------|
| 1 | no_show | No-Show Appointments Brazil |
| 2 | disease | Disease Symptoms & Patient Profile |

### 2.3 dim_outcome (Dimensi Outcome)

| Kolom | Tipe Data | Keterangan |
|-------|-----------|-----------|
| outcome_id | INTEGER (PK) | Primary key auto-increment |
| outcome_label | TEXT | Label outcome |

**Data:**
| outcome_id | outcome_label |
|------------|--------------|
| 1 | No-Show / Negative |
| 2 | Show / Positive |

### 2.4 dim_age_group (Dimensi Kelompok Usia)

| Kolom | Tipe Data | Keterangan |
|-------|-----------|-----------|
| age_group_id | INTEGER (PK) | Primary key auto-increment |
| age_group_name | TEXT | Nama kelompok usia |
| age_min | INTEGER | Batas bawah usia |
| age_max | INTEGER | Batas atas usia |

**Data:**
| age_group_id | age_group_name | age_min | age_max |
|-------------|----------------|---------|---------|
| 1 | 0-12 (Anak) | 0 | 12 |
| 2 | 13-17 (Remaja) | 13 | 17 |
| 3 | 18-35 (Dewasa Muda) | 18 | 35 |
| 4 | 36-59 (Dewasa) | 36 | 59 |
| 5 | 60+ (Lansia) | 60 | 120 |

---

## 3. Perbedaan ETL vs ELT Database

| Aspek | ETL (`warehouse.db`) | ELT (`warehouse_elt.db`) |
|-------|---------------------|--------------------------|
| Tabel fakta | `fact_medical` | `fact_medical_elt` |
| Staging tables | Tidak ada | `stg_no_show`, `stg_disease` |
| Tabel intermediate | Tidak ada | `clean_no_show`, `clean_disease`, `enriched_no_show`, `enriched_disease` |
| Proses transformasi | Python (Pandas) | SQL in-database |

---

## 4. Query SQL Analitik (8 Query)

### Q1 – Total Data per Sumber
```sql
SELECT s.source_name, COUNT(*) AS total
FROM fact_medical f
JOIN dim_source s ON f.source_id = s.source_id
GROUP BY s.source_name;
```
**Hasil:** No-Show: 110.476 baris, Disease: 350 baris

---

### Q2 – Distribusi Outcome
```sql
SELECT o.outcome_label, COUNT(*) AS total,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_medical), 2) AS persen
FROM fact_medical f
JOIN dim_outcome o ON f.outcome_id = o.outcome_id
GROUP BY o.outcome_label;
```
**Hasil:** Show/Positive: 79.8%, No-Show/Negative: 20.2%

---

### Q3 – Rata-rata Usia per Outcome
```sql
SELECT o.outcome_label,
       ROUND(AVG(f.age), 2) AS avg_age,
       MIN(f.age) AS age_min,
       MAX(f.age) AS age_max
FROM fact_medical f
JOIN dim_outcome o ON f.outcome_id = o.outcome_id
WHERE f.age IS NOT NULL
GROUP BY o.outcome_label;
```
**Hasil:** No-Show avg 37.8 th vs Show avg 36.9 th

---

### Q4 – Distribusi Kelompok Usia
```sql
SELECT ag.age_group_name, COUNT(*) AS total,
       ROUND(AVG(f.outcome_flag), 4) AS no_show_rate
FROM fact_medical f
JOIN dim_age_group ag ON f.age_group_id = ag.age_group_id
GROUP BY ag.age_group_name
ORDER BY ag.age_min;
```
**Hasil:** Dewasa (18-35) mendominasi dengan no-show rate tertinggi

---

### Q5 – No-Show Rate per Bulan
```sql
SELECT appt_month AS bulan,
       COUNT(*) AS total,
       ROUND(AVG(outcome_flag) * 100, 2) AS no_show_rate_persen
FROM fact_medical
WHERE source_id = 1 AND appt_month IS NOT NULL
GROUP BY appt_month
ORDER BY appt_month;
```
**Hasil:** Mei tertinggi (20.8%), Juni terendah (18.5%)

---

### Q6 – Rata-rata Comorbidity per Kelompok Usia
```sql
SELECT ag.age_group_name,
       ROUND(AVG(f.comorbidity_count), 2) AS avg_comorbidity
FROM fact_medical f
JOIN dim_age_group ag ON f.age_group_id = ag.age_group_id
WHERE f.comorbidity_count IS NOT NULL
GROUP BY ag.age_group_name
ORDER BY ag.age_min;
```
**Hasil:** Lansia (0.87) vs Anak (0.11) — naik signifikan seiring usia

---

### Q7 – No-Show Rate Hari Kerja vs Akhir Pekan
```sql
SELECT CASE WHEN is_weekend = 1 THEN 'Akhir Pekan'
            ELSE 'Hari Kerja' END AS jenis_hari,
       ROUND(AVG(outcome_flag) * 100, 2) AS no_show_rate,
       COUNT(*) AS total
FROM fact_medical
WHERE source_id = 1 AND is_weekend IS NOT NULL
GROUP BY is_weekend;
```
**Hasil:** Akhir pekan (23.1%) lebih tinggi dari hari kerja (20.2%)

---

### Q8 – Top 10 Penyakit dari Disease Dataset
```sql
SELECT disease_name, COUNT(*) AS total
FROM fact_medical
WHERE source_id = 2
GROUP BY disease_name
ORDER BY total DESC
LIMIT 10;
```
**Hasil:** Asthma (16), Osteoporosis (12), Stroke (11), Migraine (10)

---

## 5. Statistik Database

| Metrik | Nilai |
|--------|-------|
| Total baris (ETL) | 110.826 |
| Total baris (ELT) | 110.826 |
| Jumlah tabel ETL | 4 (1 fakta + 3 dimensi) |
| Jumlah tabel ELT | 8 (1 fakta + 3 dimensi + 4 staging/intermediate) |
| Jumlah fitur baru | 11 fitur |
| Aturan validasi | 6 aturan |
| Query analitik | 8 query |
