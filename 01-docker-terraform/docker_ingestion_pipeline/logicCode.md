# NYC Taxi Ingestion Pipeline
(Interview & Portfolio Cheat Sheet)

Dokumen ini menjelaskan **bagaimana pipeline ingestion bekerja end-to-end**, alasan desainnya, dan bagaimana modul-modul berinteraksi.
Ditulis sebagai **contekan interview** + **narasi portfolio**, dan **100% sesuai dengan code repo ini** (Module 01).


## 1) Elevator pitch (20–30 detik)

Project ini adalah **CLI ingestion tool** untuk memuat file data **CSV/TSV/Parquet** ke **PostgreSQL** dengan alur yang aman dan repeatable:

**download (cache + atomic `.part`) → detect format (extension-based) → advisory lock per tabel (session-scoped, polling + timeout) → bootstrap final schema (hanya saat final belum ada) → recreate staging like final → load ke staging via `COPY FROM STDIN` (streaming: chunk untuk baca file, batch untuk flush) → validasi staging (rowcount hard fail + month spillover check) → apply strategy (`replace` = swap staging→final, `append` = insert staging→final) → drop staging → post-load `ANALYZE`.**

Arsitekturnya Ports & Adapters: core hanya bergantung pada `Protocol` ports, sementara implementasi Postgres ada di `db/`.

---
Project ini adalah **CLI ingestion tool** untuk memuat data **NYC Taxi (CSV/TSV/Parquet)** ke **PostgreSQL** dengan pendekatan yang aman dan repeatable: pipeline melakukan **download dengan cache + atomic write (`.part`)**, mendeteksi format file, lalu mengambil **Postgres advisory lock per tabel (scoped by schema)** untuk mencegah ingestion paralel yang konflik. Setelah itu pipeline menyiapkan **final table** (bootstrap schema saat belum ada), membuat **staging table yang mirror final**, memuat data ke staging lewat **COPY FROM STDIN** secara streaming (chunk untuk baca file, batch untuk flush), menjalankan **validasi** (rowcount hard fail + month spillover check), lalu mengeksekusi strategi **`replace` (atomic rename swap staging→final)** atau **`append` (insert staging→final lalu drop staging)**, dan ditutup dengan **SQL `ANALYZE`** agar query planner Postgres punya statistik yang akurat. Arsitekturnya memakai **Ports & Adapters (Protocol)** sehingga core orchestration tetap clean dan implementasi DB/IO bisa diganti atau dites dengan mudah.

---

## 2) Arsitektur (Ports & Adapters) + mapping folder

### 2.1 Ports (kontrak) — `ports/*` ✅
Core memakai kontrak ini (semuanya `Protocol`):

- **`ports/database.py` → `Database`**
  - `engine: Engine`
  - `table_exists(table) -> bool`
  - `get_table_columns(table) -> List[str]`
  - `get_table_column_types(table) -> Mapping[str, str]`
  - `raw_connection() -> Any` (psycopg2, untuk `COPY`)
  - `begin() -> ContextManager[Connection]`
  - `connect() -> ContextManager[Connection]`
  - `execute(stmt, params) -> Any`

- **`ports/loader.py` → `Loader`**
  - `load(file_path, table_name) -> None`

- **`ports/lock.py` → `LockManager`**
  - `acquire(lock_key) -> ContextManager[None]`

- **`ports/schema.py` → `SchemaManager`**
  - `ensure_final_schema(file_path, final_table_name) -> None`
  - `recreate_staging_like_final(final_table, staging_table) -> None`

- **`ports/swapper.py` → `Swapper`**
  - `swap_tables_atomically(final_table, staging_table) -> None`

- **`ports/validator.py` → `Validator`**
  - `infer_expected_month_from_table(table_name) -> Optional[str]`
  - `validate_staging(staging_table, expected_month) -> Dict[str, Any]`

> **Interview story (hafalan):** “Core itu orchestration murni, bergantung pada ports `Protocol`. Implementasi konkret Postgres/IO ada di adapters. Jadi core tetap stabil walau adapter diganti.”

### 2.2 Adapters (implementasi)
- **Postgres gateway**
  - `db/client.py` → `PostgresClient` (memenuhi `Database`)
- **Lock**
  - `db/lock.py` → `AdvisoryLock` (memenuhi `LockManager`)
- **Schema**
  - `db/schema.py` → `PostgresSchemaManager` (memenuhi `SchemaManager`)
- **Loaders**
  - `db/loader_csv.py` → `CsvCopyLoader` (memenuhi `Loader`)
  - `db/loader_tsv.py` → `TsvCopyLoader` (memenuhi `Loader`)
  - `db/loader_parquet.py` → `ParquetStreamLoader` (memenuhi `Loader`)
- **Validation**
  - `db/validator_repo.py` → `PostgresStagingValidator` (memenuhi `Validator`)
- **Swap**
  - `db/swapper.py` → `AtomicSwapper` (memenuhi `Swapper`)
- **Post-load optimize**
  - `db/optimize.py` → `PostLoadOptimizer` (dipakai langsung; menjalankan SQL `ANALYZE`)
- **Utilities**
  - `utils/downloader.py`, `utils/file_types.py`, `utils/datetime_fix.py`, `utils/identifiers.py`, `utils/progress.py`

### 2.3 Mapping folder (DTC vibe 기억용)
- `main.py` → CLI + dependency injection
- `config.py` → paths + logging + loader defaults
- `core/` → flow ingestion end-to-end
- `ports/` → kontrak
- `db/` → implementasi Postgres (schema/load/validate/swap/optimize)
- `utils/` → download/detect/datetime/progress/security

---

## 3) Data flow end-to-end (step-by-step) sesuai code

### 3.1 Setup runtime (logging, paths, env)
- `.env` dibaca dari folder `main.py` dengan `override=False` (env sistem/Docker menang).
- Saat CLI mulai:
  - `paths = build_paths()` → memastikan `data_dir` dan `logs/` ada.
  - `configure_logging(paths)` → console sink “tqdm-safe” + file sink `logs/app.log`.

---

## 3.2 CLI: `ingest` (monthly) — `main.py`
1. Normalize `taxi` dan `month` (month dipaksa 2 digit).
2. Build URL GitHub DTC release:
   - `file_format=parquet` → `.parquet`
   - `file_format=csv` → `.csv.gz`
3. Tentukan table final:
   - `{taxi}_tripdata_{YYYY}_{MM}`
4. Build DB client:
   - `PostgresClient.from_params(...)`
   - `search_path={schema},public`
   - memastikan schema ada (`CREATE SCHEMA IF NOT EXISTS`)
5. Pre-flight `if_exists` jika table sudah ada:
   - `skip` → stop (return)
   - `fail` → exit(2)
   - `replace` → log “Atomic Swap (Staging -> Final)”
   - `append` → log “Append rows (Staging -> INSERT -> Final)”
6. Load loader settings:
   - `LOADER_CHUNK_SIZE` default **50_000**
   - `LOADER_BATCH_SIZE` default **10_000**
7. Build pipeline (inject lock, schema, loaders, validator, swapper, optimizer)
8. Run:
   - `pipeline.run(url=url, table_name=table_name, keep_local=keep_local, if_exists=if_exists)`

### Narasi interview yang lebih kuat untuk `--if-exists`
Aku membagi tanggung jawab strategi `if_exists` menjadi dua layer supaya behavior-nya jelas dan predictable. **CLI menangani `skip` dan `fail` sebagai pre-flight guard** berbasis `table_exists` supaya fail-fast dan hemat resource (nggak download/load kalau memang tidak perlu). Sementara itu **core pipeline menangani `replace` dan `append`** karena dua strategi ini butuh orchestration staging + validasi + transaksi DB. Dengan pola ini, entrypoint manapun (`ingest` atau `ingest-url`) tetap punya behavior yang konsisten.

---

## 3.3 CLI: `ingest-url` (custom URL) — `main.py`
- Jika `table_name` kosong → infer dari filename URL (hapus ext `.csv/.tsv/.parquet` + optional `.gz`, `-` → `_`).
- Menerima `--if-exists` juga: `skip|fail|replace|append`.
- Pre-flight check sama:
  - `skip` → stop (return)
  - `fail` → exit(2)
  - `replace` / `append` → log dan lanjut eksekusi pipeline
- Eksekusi pipeline:
  - `pipeline.run(url=url, table_name=table_name, keep_local=keep_local, if_exists=if_exists)`

---

## 3.4 Core: `IngestionPipeline.run` — `core/ingestion_pipeline.py`
Urutan persis (sesuai implementasi terbaru):

1. `table_name = sanitize_ident(table_name)`
2. `staging_table = sanitize_ident(f"{table_name}__staging")`
3. Lock key include schema (hindari contention lintas schema):
   - `schema = getattr(db, "schema", "public")`
   - `lock_key = f"ingest:{schema}:{table_name}"`
4. Validasi `if_exists` (core hanya support `replace|append`; selain itu raise `ValueError`)
5. `file_path = download_file(url)`
6. `fmt = detect_file_format(file_path)`
7. `with lock.acquire(lock_key):`
8. Jika final belum ada:
   - `schema.ensure_final_schema(file_path=file_path, final_table=table_name)`
9. `schema.recreate_staging_like_final(final_table=table_name, staging_table=staging_table)`
10. `loader = _get_loader(fmt)` → `loader.load(file_path=file_path, table_name=staging_table)`
11. `expected_month = validator.infer_expected_month_from_table(table_name)`
    - jika tidak ditemukan pola `YYYY_MM`, pipeline log debug dan month spillover check **akan tidak dilakukan** (secara natural) oleh validator.
12. `validator.validate_staging(staging_table=staging_table, expected_month=expected_month)`
13. Apply strategy:
    - **replace** → `swapper.swap_tables_atomically(final_table=table_name, staging_table=staging_table)`
    - **append** → `INSERT INTO final SELECT * FROM staging` lalu **drop staging** (di `finally`)
14. `optimizer.analyze(table_name)` → **SQL `ANALYZE`**
15. Error handling:
    - best-effort `DROP TABLE IF EXISTS "staging_table"`
16. Finally:
    - hapus file jika `keep_local=False`

---

## 3.5 Append semantics (wajib disebut untuk portfolio)
**Append = staging load + validate + insert to final + drop staging + analyze**

Secara eksplisit, strategi `append` bekerja seperti ini:
1) Pipeline tetap membuat staging dan load data ke staging lewat COPY streaming  
2) Data staging tetap melewati validation gate (minimal rowcount hard fail + optional month spillover check)  
3) Jika valid, pipeline menjalankan:
   - `INSERT INTO final SELECT * FROM staging` (di dalam transaksi `db.begin()`)  
4) Setelah insert selesai (sukses/gagal), staging dihapus dengan:
   - `DROP TABLE IF EXISTS staging` (di blok `finally`)  
5) Terakhir, pipeline menjalankan `ANALYZE final` untuk update statistik query planner  

> **Kenapa ini enterprise-friendly?** Karena append pun tetap punya “quality gate” (validator) dan staging isolation, bukan langsung load ke final.

---

## 4) Deep dive per komponen

## A) Security: identifier sanitization + quoting — `utils/identifiers.py` ✅
- `sanitize_ident(name)` whitelist ketat:
  - regex: `^[A-Za-z_][A-Za-z0-9_]*$`
  - jika tidak match → raise `ValueError("Unsafe identifier")`
- `qident(name)`:
  - quote: `"identifier"` setelah sanitize

**Kenapa penting:** table/column name dari input (CLI atau URL infer) tidak bisa jadi vector SQL injection.

---

## B) Database gateway — `db/client.py` (`PostgresClient`) ✅
### B1) Engine + search_path
`from_params()`:
- driver: `postgresql+psycopg2`
- `pool_pre_ping=True`
- `connect_args["options"] = "-csearch_path={schema},public"`
- memastikan schema ada:
  - `CREATE SCHEMA IF NOT EXISTS "schema"`

### B2) Metadata helpers
- `table_exists()` → query `information_schema.tables` by schema+table
- `get_table_columns()` → `information_schema.columns` ordered by `ordinal_position`
- `get_table_column_types()` → `column_name, data_type`

### B3) Connection model
- `connect()` (SQLAlchemy Connection context)
- `begin()` (transaction context)
- `raw_connection()` (psycopg2 connection untuk COPY)

---

## C) Downloader — `utils/downloader.py` ✅
### C1) Cache
Jika file sudah ada di `paths.data_dir` dan size > 0 → skip download.

### C2) Atomic write `.part`
- download stream `requests.get(... stream=True, timeout=60)`
- write ke `local_path.part`
- success → `os.replace(.part, local_path)` (atomic)
- failure → hapus `.part`, log error, re-raise

### C3) Post-condition
Jika file final missing/empty → raise RuntimeError.

---

## D) File format detection — `utils/file_types.py` ✅
Deteksi **hanya** dari extension:
- `.parquet` → PARQUET
- `.csv` / `.csv.gz` → CSV
- `.tsv` / `.tsv.gz` → TSV
- lainnya → UNKNOWN (akan gagal di core karena tidak ada loader)

---

## E) Progress bar wrapper — `utils/progress.py` ✅
- `progress_enable()` membaca env `ENABLE_PROGRESS` (default True)
- `track(total, desc)`:
  - jika progress disabled atau `tqdm` tidak ada → yield `NoopProgress` (update no-op)
  - else → yield tqdm bar (`leave=False`, `dynamic_ncols=True`)

---

## F) Datetime normalization — `utils/datetime_fix.py` ✅
`fix_datetime_columns(df)`:
- pasangan kandidat:
  - (`tpep_pickup_datetime`, `tpep_dropoff_datetime`)
  - (`lpep_pickup_datetime`, `lpep_dropoff_datetime`)
  - (`pickup_datetime`, `dropoff_datetime`)
- hanya jika **keduanya ada** → convert keduanya via `pd.to_datetime(errors="coerce")`

---

## G) Lock — `db/lock.py` ✅ (session-scoped + polling)
- Acquire (non-blocking):
  - `SELECT pg_try_advisory_lock(hashtext(:k)::bigint)`
- Release:
  - `SELECT pg_advisory_unlock(hashtext(:k)::bigint)`
- Poll loop:
  - `poll_s=0.2`, `timeout_s=60.0` default
  - timeout → raise `TimeoutError`
- Session-scoped:
  - lock dipegang selama connection `db.connect()` hidup.

---

## H) Schema manager — `db/schema.py` ✅
### H1) ensure_final_schema
- detect format:
  - Parquet → `_bootstrap_final_schema_from_parquet_footer`
  - CSV/TSV → `_bootstrap_final_schema_from_csv_sample(fmt=fmt)`
- unsupported → raise

### H2) CSV/TSV bootstrap (sample-based)
- separator:
  - TSV `\t`, CSV `,`
- `pd.read_csv(... nrows=sample_rows, compression="infer", low_memory=False, sep=separator)`
- `fix_datetime_columns(df)`
- `df.head(0).to_sql(if_exists="replace", index=False)`

### H3) Parquet bootstrap (footer schema)
- `pf = pq.ParquetFile(file_path)` → `schema_arrow`
- mapping Arrow type → Postgres type:
  - timestamptz/timestamp, ints, floats, boolean, text, date, numeric(p,s), bytea, fallback text
- DDL:
  - `DROP TABLE IF EXISTS final`
  - `CREATE TABLE final (...)`

### H4) Staging recreate
- `DROP TABLE IF EXISTS staging`
- `CREATE TABLE staging (LIKE final INCLUDING ALL)`

---

## I) Loaders — `db/loader_parquet.py`, `db/loader_csv.py`, `db/loader_tsv.py` ✅

### I0) `LOADER_CHUNK_SIZE` vs `LOADER_BATCH_SIZE`
Default dari `config.py`:
- **LOADER_CHUNK_SIZE = 50_000**
- **LOADER_BATCH_SIZE = 10_000**

Pemakaian:
- `chunk_size` = ukuran baca file (Parquet iter_batches / CSV&TSV chunksize)
- `batch_size` = ukuran flush ke DB (slice internal sebelum COPY)

### I1) Pola umum loader (semua format)
1. Baca kolom target dari database:
   - `db.get_table_columns(table_name)`
2. Baca tipe kolom dari database:
   - `db.get_table_column_types(table_name)`
3. Susun `COPY ... FROM STDIN WITH (FORMAT csv, HEADER false, NULL '', DELIMITER ...)`
4. Stream data:
   - baca per chunk (`chunk_size`)
   - transform/coerce (datetime, numeric, int)
   - flush per batch (`batch_size`) via `cur.copy_expert(copy_sql, buf)`
5. Transaction:
   - sukses → `commit`
   - error → `rollback`

### I2) TSV specifics
`TsvCopyLoader`:
- input parsing: `pd.read_csv(sep="\t")`
- first chunk:
  - **duplicate columns header check** → hard fail
- output ke Postgres:
  - stream sebagai CSV comma (`to_csv(sep=",")`)
  - COPY delimiter **comma** (`DELIMITER ','`)

### I3) CSV loader specifics
`CsvCopyLoader`:
- delimiter configurable (`delimiter=","` default)
- COPY delimiter mengikuti `self.delimiter`

### I4) Parquet loader specifics
`ParquetStreamLoader`:
- baca Arrow batch (`iter_batches(batch_size=chunk_size)`)
- batch pertama memvalidasi kolom wajib ada
- stream ke Postgres sebagai CSV comma

---

## J) Validator — `db/validator_repo.py` ✅
- Hard fail:
  - staging rowcount = 0 → raise
- Detect datetime column (case-insensitive match dari kandidat):
  - `lpep_pickup_datetime`, `tpep_pickup_datetime`, `pickup_datetime`
- Expected month:
  - diambil dari nama final table via regex `YYYY_MM`
- Month spillover:
  - hitung rows inside/outside range bulan
  - threshold:
    - abs: `max_outside_month_abs=200`
    - ratio: `max_outside_month_ratio=0.005`
  - default: jika exceed → **warning** (tidak fail) karena `fail_on_outside_month=False`

---

## K) Swapper — `db/swapper.py` ✅
Atomic swap via rename dalam transaction:
1. drop backup
2. jika final exists (`to_regclass(schema.final)`):
   - rename final → backup
3. rename staging → final
4. drop backup

Backup tidak dipertahankan setelah sukses.

---

## L) Post-load optimization — `db/optimize.py` ✅
- Menjalankan **SQL command**:
  - `ANALYZE "schema"."table"`
- Fully qualified table dibangun via sanitize+quote.

---

## 5) Mermaid diagrams

### A) Call graph
```mermaid
flowchart TD
  CLI[main.py] --> CFG[config.load_loader_settings<br>chunk=50k batch=10k]
  CLI --> DB[PostgresClient.from_params<br>search_path=schema,public]
  CLI --> P[IngestionPipeline.run]

  P --> DL[download_file<br>cache + atomic .part]
  P --> FMT[detect_file_format<br>extension]
  P --> LOCK[AdvisoryLock.acquire<br>pg_try_advisory_lock hashtext]

  LOCK --> SCH{final exists?}
  SCH -->|no| BOOT[SchemaManager.ensure_final_schema]
  LOCK --> STG[recreate_staging_like_final]
  LOCK --> LOAD[Loader.load<br>COPY FROM STDIN]
  LOCK --> VAL[Validator.validate_staging]
  VAL --> STRAT{if_exists}

  STRAT -->|replace| SWP[Swapper.swap_tables_atomically]
  STRAT -->|append| INS[INSERT final SELECT staging<br>then DROP staging]

  SWP --> OPT[PostLoadOptimizer.analyze<br>SQL ANALYZE]
  INS --> OPT
````

### B) Sequence diagram (replace vs append)

```mermaid
sequenceDiagram
    participant CLI as CLI (main.py)
    participant P as Pipeline (core)
    participant DL as Downloader
    participant L as Advisory Lock
    participant S as Schema Manager
    participant LD as Loader
    participant V as Validator
    participant SW as Swapper
    participant O as Optimizer

    CLI->>P: run(url, table_name, keep_local, if_exists)
    P->>DL: download_file(url)
    DL-->>P: file_path (cached or downloaded via .part + os.replace)

    P->>L: acquire("ingest:{schema}:{table_name}") (poll + timeout)

    alt Final table missing
        P->>S: ensure_final_schema(file_path, final_table=table_name)
    end
    P->>S: recreate_staging_like_final(final=table_name, staging=table__staging)
    P->>LD: load(file_path, staging) via COPY FROM STDIN
    P->>V: infer_expected_month_from_table(table_name)
    P->>V: validate_staging(staging, expected_month)

    alt if_exists == "replace"
        P->>SW: swap_tables_atomically(final, staging)
    else if_exists == "append"
        P->>P: INSERT INTO final SELECT * FROM staging
        P->>P: DROP TABLE IF EXISTS staging (finally)
    end

    P->>O: ANALYZE final
    L-->>P: release lock

```

---

## 6) Trade-offs / limitations (yang benar-benar relevan dari code)

1. **Downloader cache hanya berdasarkan size**

   * file korup tapi non-empty bisa lolos cache (tidak ada checksum/ETag).

2. **Format detection hanya berdasarkan extension**

   * salah extension → format UNKNOWN → fail (tidak ada sniffing).

3. **Schema CSV/TSV dari sample**

   * dtypes bergantung sample rows; bisa mismatch dengan keseluruhan data.

4. **Datetime fix pair-only**

   * hanya mengonversi jika pickup+dropoff pasangan lengkap.

5. **Advisory lock hashtext**

   * collision secara teori mungkin (tidak ada mitigation di code).

6. **Append menggunakan `INSERT INTO final SELECT * FROM staging`**

   * tidak ada dedupe/upsert: jika ada PK/unique constraint, append bisa gagal IntegrityError (sesuai behavior DB).

7. **Backup table tidak dipertahankan**

   * swap sukses akan drop backup; tidak ada rollback cepat via backup.

---

## 7) Interview Q&A cheat sheet (berbasis code)

### Q1: “Ceritain alur ingestion kamu.”

**A:** Download (cache + atomic `.part`), detect format by extension, acquire advisory lock per table (scoped by schema+table), bootstrap schema jika final belum ada, create staging like final, load staging via COPY streaming (chunk/batch), validate staging (rowcount, month spillover), lalu apply strategy: `replace` = atomic swap staging→final, `append` = insert staging→final. Terakhir run SQL `ANALYZE`.

### Q2: “Kenapa ports & adapters?”

**A:** Core hanya tergantung `Protocol` ports (`Database/Loader/Validator/...`). Implementasi Postgres/IO di `db/*` dan `utils/*`. Ini membuat core lebih mudah dites dan adapter bisa diganti tanpa ubah flow.

### Q3: “Apa bedanya chunk vs batch, defaultnya berapa?”

**A:** `LOADER_CHUNK_SIZE` default 50k untuk baca file, `LOADER_BATCH_SIZE` default 10k untuk flush COPY ke DB.

### Q4: “Append kamu gimana?”

**A:** Append tetap lewat staging + validation. Kalau valid, pipeline lakukan `INSERT INTO final SELECT * FROM staging`, drop staging, dan `ANALYZE final`. Jadi append pun punya quality gate, bukan load langsung ke final.

### Q5: “Validasi apa yang membuat job fail?”

**A:** Staging rowcount = 0 hard fail. Month spillover by default warning (fail kalau `fail_on_outside_month=True`).

### Q6: “Kenapa jalankan ANALYZE?”

**A:** Setelah bulk load dan promote (swap/append), `ANALYZE` memperbarui statistik Postgres supaya query planner tidak salah estimasi.