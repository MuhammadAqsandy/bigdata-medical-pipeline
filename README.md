# 🏥 Big Data Pipeline – Medical / Kesehatan Masyarakat

> **Tugas Besar UAS Big Data | Semester Genap 2025/2026**

Proyek ini mengimplementasikan pipeline Big Data end-to-end dengan dua pendekatan berbeda (ETL dan ELT) menggunakan dataset kesehatan publik dari Kaggle.

---

## 📋 Deskripsi Project

Sistem ini memproses data dari dua sumber kesehatan untuk mengidentifikasi pola no-show pasien pada janji dokter dan profil penyakit masyarakat, kemudian divisualisasikan melalui dashboard analitik interaktif.

**Key Findings:**
- Tingkat no-show pasien sekitar **20.2%**
- Pasien no-show menunggu rata-rata **15.4 hari** vs 8.7 hari untuk yang hadir
- No-show lebih tinggi di **akhir pekan (23.1%)** dibanding hari kerja (20.2%)
- Comorbidity meningkat seiring usia: lansia (0.87) vs anak (0.11)

---

## 📦 Dataset

| No | Nama Dataset | Sumber | Jumlah Baris | Link |
|----|-------------|--------|-------------|------|
| 1 | No-Show Medical Appointments | Kaggle / Brazil | 110.527 baris | [Link](https://www.kaggle.com/datasets/joniarroba/noshowappointments) |
| 2 | Disease Symptoms & Patient Profile | Kaggle | ~350 baris | [Link](https://www.kaggle.com/datasets/uom190346a/disease-symptoms-and-patient-profile-dataset) |

---

## 🏗️ Arsitektur Sistem

```
SUMBER DATA (2 CSV)
        │
        ├─── PIPELINE ETL ──────────────────────────────────────────┐
        │    Extract → Data Cleaning → Standardisasi →             │
        │    Enrichment (11 fitur) → Validasi (6 aturan)           │
        │                                                           ▼
        │                                              DATA WAREHOUSE (SQLite)
        │                                              ┌─────────────────────┐
        │                                              │   fact_medical      │
        └─── PIPELINE ELT ─────────────────────────►  │   dim_source        │
             Extract → Load Staging →                  │   dim_outcome       │
             Transform SQL → Validasi SQL              │   dim_age_group     │
                                                       └─────────────────────┘
                                                                    │
                                                                    ▼
                                                        DASHBOARD ANALITIK
                                                        (Looker Studio)
```

---

## 📁 Struktur Repository

```
bigdata_final_project/
├── 📓 ETL_Pipeline_Medical.ipynb     # Google Colab ETL Pipeline
├── 📓 ELT_Pipeline_Medical.ipynb     # Google Colab ELT Pipeline
├── 📄 etl_pipeline/
│   └── etl_pipeline.py               # Script ETL (Python)
├── 📄 elt_pipeline/
│   └── elt_pipeline.py               # Script ELT (Python)
├── 📄 export_dashboard_data.py       # Export CSV untuk Dashboard ETL
├── 📄 export_dashboard_elt.py        # Export CSV untuk Dashboard ELT
├── 📁 raw/                           # Dataset CSV (tidak diupload, lihat link)
├── 📁 datalake/                      # Data Lake (generated)
├── 📁 warehouse/
│   ├── warehouse.db                  # Database ETL (SQLite)
│   └── warehouse_elt.db              # Database ELT (SQLite)
├── 📁 dashboard/
│   ├── dashboard_screenshot.png      # Screenshot Dashboard ETL
│   └── dashboard_elt_screenshot.png  # Screenshot Dashboard ELT
├── 📄 architecture_diagram.pdf       # Diagram arsitektur sistem
├── 📄 architecture_diagram.png       # Diagram arsitektur sistem (PNG)
├── 📄 report.pdf                     # Laporan Paper format IEEE
└── 📄 README.md                      # File ini
```

---

## 🚀 Cara Menjalankan

### Opsi 1 – Google Colab (Direkomendasikan)

1. Upload dataset ke Google Drive:
   ```
   MyDrive/bigdata_final_project/raw/no_show_appointments.csv
   MyDrive/bigdata_final_project/raw/disease_symptoms.csv
   ```

2. Buka notebook di Google Colab:
   - ETL: `ETL_Pipeline_Medical.ipynb`
   - ELT: `ELT_Pipeline_Medical.ipynb`

3. Jalankan semua cell: **Runtime → Run all**

### Opsi 2 – Lokal (Python)

**Prerequisites:**
```bash
pip install pandas numpy scipy
```

**Jalankan ETL:**
```bash
cd bigdata_final_project/etl_pipeline
python etl_pipeline.py
```

**Jalankan ELT:**
```bash
cd bigdata_final_project/elt_pipeline
python elt_pipeline.py
```

**Export data dashboard:**
```bash
cd bigdata_final_project
python export_dashboard_data.py    # untuk ETL
python export_dashboard_elt.py     # untuk ELT
```

---

## 🗄️ Dokumentasi Database

### Star Schema

```
                    ┌──────────────┐
                    │  dim_source  │
                    │  source_id   │
                    │  source_name │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴────────────┐    ┌──────────────────┐
│  dim_outcome │    │   fact_medical    │    │  dim_age_group   │
│  outcome_id  ├────┤   record_id       ├────┤  age_group_id    │
│  outcome_    │    │   source_id       │    │  age_group_name  │
│  label       │    │   outcome_id      │    │  age_min         │
└──────────────┘    │   age_group_id    │    │  age_max         │
                    │   age, age_norm   │    └──────────────────┘
                    │   outcome_flag    │
                    │   wait_days       │
                    │   comorbidity_    │
                    │   count, ...      │
                    └───────────────────┘
```

### Struktur Tabel

| Tabel | Kolom Utama | Keterangan |
|-------|------------|------------|
| `fact_medical` | record_id, source_id, outcome_id, age_group_id, age, wait_days, outcome_flag, comorbidity_count | Tabel fakta utama |
| `dim_source` | source_id, source_name, description | Dimensi sumber data |
| `dim_outcome` | outcome_id, outcome_label | Dimensi hasil (no-show/show) |
| `dim_age_group` | age_group_id, age_group_name, age_min, age_max | Dimensi kelompok usia |

---

## 📊 Dashboard

| Pipeline | Link Dashboard | Screenshot |
|---------|---------------|-----------|
| ETL | [Dashboard ETL – Looker Studio](#) | `dashboard/dashboard_screenshot.png` |
| ELT | [Dashboard ELT – Looker Studio](#) | `dashboard/dashboard_elt_screenshot.png` |

> **Ganti `#` dengan link Looker Studio setelah di-publish**

---

## 🛠️ Tech Stack

| Komponen | Teknologi |
|---------|-----------|
| Language | Python 3.12 |
| Data Processing | Pandas, NumPy, SciPy |
| Database | SQLite |
| Notebook | Google Colab |
| Dashboard | Looker Studio (Google Data Studio) |
| Dataset | Kaggle Public Datasets |

---

## 👤 Anggota Kelompok

| Nama | NIM |
|------|-----|
| Muhammad Aqsandy J Iskandar | 1103220214 |

**Program Studi:** S1 Teknik Komputer – Fakultas Teknik Elektro  
**Universitas:** Universitas Telkom Bandung  
**Kelas:** TK-46-02
