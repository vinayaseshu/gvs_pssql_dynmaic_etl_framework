/* =============================================================================
   Demo / test for etl_framework.sql
   Run after etl_framework.sql:  psql -f etl_framework.sql -f etl_demo.sql
============================================================================= */

-- ----------------------------------------------------------------- sample tables
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;

DROP TABLE IF EXISTS stg.patient_raw, dw.dim_patient, dw.patient_archive, stg.visit_raw, dw.fact_visit;

-- Raw staging: everything as text, messy formatting
CREATE TABLE stg.patient_raw (
    mrn         text,
    first_nm    text,
    last_nm     text,
    birth_dt    text,      -- 'YYYYMMDD' as in HL7 PID-7
    gender_cd   text,
    zip         text,
    load_ts     timestamptz DEFAULT now()
);

CREATE TABLE dw.dim_patient (
    patient_key  bigint GENERATED ALWAYS AS IDENTITY,
    mrn          varchar(20) PRIMARY KEY,
    full_name    varchar(200),
    birth_date   date,
    gender       char(1),
    zip5         char(5),
    source_sys   varchar(20),
    created_dt   timestamptz,
    updated_dt   timestamptz
);

CREATE TABLE dw.patient_archive (LIKE dw.dim_patient);
ALTER TABLE dw.patient_archive DROP COLUMN patient_key;

CREATE TABLE stg.visit_raw (
    visit_id   int,
    mrn        text,
    visit_ts   timestamp,
    charge_amt text
);

CREATE TABLE dw.fact_visit (
    visit_id    int,
    mrn         varchar(20),
    visit_date  date,
    charge_amt  numeric(12,2)
);

INSERT INTO stg.patient_raw (mrn, first_nm, last_nm, birth_dt, gender_cd, zip) VALUES
 ('MRN001', ' john ', 'SMITH',  '19800115', 'm', '33602-1234'),
 ('MRN002', 'Mary',   'jones',  '19751103', 'F', '32801'),
 ('MRN003', 'raj',    'PATEL',  '19900520', NULL, '33647');

INSERT INTO stg.visit_raw VALUES
 (1, 'MRN001', '2026-10-01 09:00', '150.00'),
 (2, 'MRN002', '2026-10-02 10:30', '89.5'),
 (3, 'MRN001', '2026-10-03 14:15', '1200');

-- ----------------------------------------------------------------- job metadata
TRUNCATE etl.etl_job RESTART IDENTITY CASCADE;

INSERT INTO etl.etl_job (job_name, job_group, run_order, source_schema, source_table,
                         target_schema, target_table, load_type, source_filter,
                         incremental_column, delete_source_after_load)
VALUES
 ('PATIENT_UPSERT',  'PATIENT', 10, 'stg', 'patient_raw', 'dw', 'dim_patient',     'UPSERT', 's.mrn IS NOT NULL', NULL,       false),
 ('PATIENT_MERGE',   'PATIENT', 20, 'stg', 'patient_raw', 'dw', 'dim_patient',     'MERGE',  's.mrn IS NOT NULL', NULL,       false),
 ('PATIENT_ARCHIVE', 'ARCHIVE', 30, 'stg', 'patient_raw', 'dw', 'patient_archive', 'TRUNCATE_LOAD', NULL,         NULL,       false),
 ('VISIT_INCR',      'VISIT',   40, 'stg', 'visit_raw',   'dw', 'fact_visit',      'APPEND', NULL,                'visit_ts', false);

-- Patient mappings (same column rules for UPSERT, MERGE and ARCHIVE jobs)
INSERT INTO etl.etl_column_mapping (job_id, target_column, source_column, source_expression, default_value, is_key, is_updatable, compare_on_change, column_order)
SELECT j.job_id, v.*
FROM etl.etl_job j
CROSS JOIN (VALUES
 ('mrn',        'mrn',  NULL,                                                       NULL,          true,  true,  true,  1),
 ('full_name',  NULL,   'initcap(btrim(s.first_nm)) || '' '' || initcap(btrim(s.last_nm))', NULL, false, true,  true,  2),
 ('birth_date', NULL,   'to_date(s.birth_dt, ''YYYYMMDD'')',                         NULL,          false, true,  true,  3),
 ('gender',     NULL,   'upper(s.gender_cd)',                                        '''U''',       false, true,  true,  4),
 ('zip5',       NULL,   'left(s.zip, 5)',                                            NULL,          false, true,  true,  5),
 ('source_sys', NULL,   NULL,                                                       '''EPIC''',    false, true,  true,  6),
 ('created_dt', NULL,   'now()',                                                    NULL,          false, false,  true, 7),  -- insert-only
 ('updated_dt', NULL,   'now()',                                                    NULL,          false, true,  false,  8)
) AS v(target_column, source_column, source_expression, default_value, is_key, is_updatable, compare_on_change, column_order)
WHERE j.job_name IN ('PATIENT_UPSERT', 'PATIENT_MERGE', 'PATIENT_ARCHIVE');

INSERT INTO etl.etl_column_mapping (job_id, target_column, source_column, source_expression, column_order)
SELECT job_id, v.* FROM etl.etl_job,
(VALUES ('visit_id',   'visit_id',   NULL,                    1),
        ('mrn',        'mrn',        NULL,                    2),
        ('visit_date', NULL,         's.visit_ts::date',      3),
        ('charge_amt', 'charge_amt', NULL,                    4)) v
WHERE job_name = 'VISIT_INCR';

-- ----------------------------------------------------------------- tests
\echo '=== 1. Dry run: show generated SQL only ==='
CALL etl.sp_execute_mapping('PATIENT_UPSERT', true);
SELECT generated_sql FROM etl.etl_run_log ORDER BY run_id DESC LIMIT 1;

\echo '=== 2. UPSERT load ==='
CALL etl.sp_execute_mapping('PATIENT_UPSERT');
SELECT mrn, full_name, birth_date, gender, zip5, source_sys FROM dw.dim_patient ORDER BY mrn;

\echo '=== 3. Source changes, then MERGE (only changed/new rows touched) ==='
UPDATE stg.patient_raw SET zip = '33610' WHERE mrn = 'MRN002';
INSERT INTO stg.patient_raw (mrn, first_nm, last_nm, birth_dt, gender_cd, zip)
VALUES ('MRN004', 'ana', 'lopez', '20010909', 'F', '33549');
CALL etl.sp_execute_mapping('PATIENT_MERGE');   -- expect 2 rows (1 update + 1 insert)
SELECT mrn, full_name, zip5 FROM dw.dim_patient ORDER BY mrn;

\echo '=== 4. Incremental APPEND with watermark ==='
CALL etl.sp_execute_mapping('VISIT_INCR');      -- 3 rows
INSERT INTO stg.visit_raw VALUES (4, 'MRN004', '2026-10-05 08:00', '45.25');
CALL etl.sp_execute_mapping('VISIT_INCR');      -- only 1 new row
CALL etl.sp_execute_mapping('VISIT_INCR');      -- 0 rows
SELECT * FROM dw.fact_visit ORDER BY visit_id;
SELECT job_name, last_watermark FROM etl.etl_job WHERE job_name = 'VISIT_INCR';

\echo '=== 5. Move semantics: archive + delete from source ==='
UPDATE etl.etl_job SET delete_source_after_load = true WHERE job_name = 'PATIENT_ARCHIVE';
CALL etl.sp_execute_mapping('PATIENT_ARCHIVE');
SELECT (SELECT count(*) FROM dw.patient_archive) AS archived,
       (SELECT count(*) FROM stg.patient_raw)    AS left_in_source;

\echo '=== 6. Failure is logged, data rolled back ==='
INSERT INTO stg.visit_raw VALUES (5, 'MRN001', '2026-10-06 08:00', 'not-a-number');
CALL etl.sp_execute_mapping('VISIT_INCR');
SELECT count(*) AS fact_rows_still FROM dw.fact_visit;

\echo '=== 7. Batch runner ==='
CALL etl.sp_run_all('PATIENT');   -- MERGE should now report 0 rows (nothing changed)

\echo '=== Run log ==='
SELECT run_id, job_name, load_type, status, rows_affected, rows_deleted,
       watermark_from, watermark_to, left(error_message, 60) AS error
FROM etl.etl_run_log ORDER BY run_id;

/* =============================================================================
   JOIN SCENARIOS
   stg.encounter_raw (driving, alias e)
     LEFT  JOIN dw.dim_patient  p  ON p.mrn = e.mrn               -- keep unmatched patients
     INNER JOIN ref.department  d  ON d.dept_cd = e.dept_cd AND d.is_active
============================================================================= */
CREATE SCHEMA IF NOT EXISTS ref;
DROP TABLE IF EXISTS ref.department, stg.encounter_raw, dw.fact_encounter, dw.encounter_hist;

CREATE TABLE ref.department (dept_cd text PRIMARY KEY, dept_name text, is_active boolean);
INSERT INTO ref.department VALUES
 ('CARD', 'Cardiology', true), ('ER', 'Emergency', true), ('OLD', 'Retired Unit', false);

CREATE TABLE stg.encounter_raw (
    enc_id     int, mrn text, dept_cd text, enc_ts timestamp,
    charge_amt text, feed_seq int          -- feed_seq: higher = newer version of the same encounter
);
INSERT INTO stg.encounter_raw VALUES
 (101, 'MRN001', 'CARD', '2026-10-07 09:00', '100.00', 1),
 (101, 'MRN001', 'CARD', '2026-10-07 09:00', '120.00', 2),   -- corrected charge, must win
 (102, 'MRN999', 'ER',   '2026-10-08 22:10', '75.50',  1),   -- patient not in dim -> LEFT JOIN keeps it
 (103, 'MRN002', 'OLD',  '2026-10-09 11:00', '60.00',  1);   -- inactive dept -> INNER JOIN drops it

CREATE TABLE dw.fact_encounter (
    enc_id       int PRIMARY KEY,
    patient_key  bigint,
    patient_name varchar(200),
    dept_name    varchar(100),
    enc_date     date,
    charge_amt   numeric(12,2),
    load_dt      timestamptz
);
CREATE TABLE dw.encounter_hist (LIKE dw.fact_encounter);

-- ---- job headers
INSERT INTO etl.etl_job (job_name, job_group, run_order, source_schema, source_table, source_alias,
                         target_schema, target_table, load_type, source_filter,
                         dedupe_on_key, dedupe_order_by, delete_source_after_load)
VALUES
 ('ENCOUNTER_JOIN', 'ENCOUNTER', 50, 'stg', 'encounter_raw', 'e', 'dw', 'fact_encounter', 'UPSERT',
  'e.enc_ts >= DATE ''2026-01-01''', true, 'e.feed_seq DESC', false),
 ('ENCOUNTER_MOVE', 'ENCOUNTER', 60, 'stg', 'encounter_raw', 'e', 'dw', 'encounter_hist', 'APPEND',
  'e.enc_ts >= DATE ''2026-01-01''', false, NULL, true);

-- ---- joins (same for both jobs)
INSERT INTO etl.etl_job_join (job_id, join_order, join_type, join_schema, join_table, join_alias, join_condition)
SELECT j.job_id, v.*
FROM etl.etl_job j
CROSS JOIN (VALUES
 (1, 'LEFT',  'dw',  'dim_patient', 'p', 'p.mrn = e.mrn'),
 (2, 'INNER', 'ref', 'department',  'd', 'd.dept_cd = e.dept_cd AND d.is_active')
) v(join_order, join_type, join_schema, join_table, join_alias, join_condition)
WHERE j.job_name IN ('ENCOUNTER_JOIN', 'ENCOUNTER_MOVE');

-- ---- column mappings: source_alias picks which joined table a column comes from
INSERT INTO etl.etl_column_mapping (job_id, target_column, source_alias, source_column,
                                    source_expression, default_value, is_key, column_order)
SELECT j.job_id, v.*
FROM etl.etl_job j
CROSS JOIN (VALUES
 ('enc_id',       'e', 'enc_id',      NULL,                  NULL,          true,  1),
 ('patient_key',  'p', 'patient_key', NULL,                  NULL,          false, 2),
 ('patient_name', 'p', 'full_name',   NULL,                  '''UNKNOWN''', false, 3),
 ('dept_name',    'd', 'dept_name',   NULL,                  NULL,          false, 4),
 ('enc_date',     NULL, NULL,         'e.enc_ts::date',      NULL,          false, 5),
 ('charge_amt',   NULL, 'charge_amt', NULL,                  '0',           false, 6),
 ('load_dt',      NULL, NULL,         'now()',               NULL,          false, 7)
) v(target_column, source_alias, source_column, source_expression, default_value, is_key, column_order)
WHERE j.job_name IN ('ENCOUNTER_JOIN', 'ENCOUNTER_MOVE');

\echo '=== 8. Join job: dry run (generated SQL) ==='
CALL etl.sp_execute_mapping('ENCOUNTER_JOIN', true);
SELECT generated_sql FROM etl.etl_run_log ORDER BY run_id DESC LIMIT 1;

\echo '=== 9. Join job: UPSERT with de-dup (expect 2 rows: 101 @ 120.00, 102 UNKNOWN; 103 dropped) ==='
CALL etl.sp_execute_mapping('ENCOUNTER_JOIN');
SELECT enc_id, patient_key, patient_name, dept_name, enc_date, charge_amt FROM dw.fact_encounter ORDER BY enc_id;

\echo '=== 10. Re-run is idempotent ==='
CALL etl.sp_execute_mapping('ENCOUNTER_JOIN');
SELECT count(*) AS fact_rows FROM dw.fact_encounter;

\echo '=== 11. Move through joins: only rows the join selected leave the source (103 stays) ==='
CALL etl.sp_execute_mapping('ENCOUNTER_MOVE');
SELECT enc_id, feed_seq, dept_cd FROM stg.encounter_raw ORDER BY enc_id;
SELECT generated_sql FROM etl.etl_run_log ORDER BY run_id DESC LIMIT 1;

\echo '=== 12. Validation: bad alias and missing join column are caught and logged ==='
UPDATE etl.etl_column_mapping SET source_alias = 'x'
 WHERE target_column = 'dept_name' AND job_id = (SELECT job_id FROM etl.etl_job WHERE job_name = 'ENCOUNTER_JOIN');
CALL etl.sp_execute_mapping('ENCOUNTER_JOIN');
UPDATE etl.etl_column_mapping SET source_alias = 'd', source_column = 'dept_desc'
 WHERE target_column = 'dept_name' AND job_id = (SELECT job_id FROM etl.etl_job WHERE job_name = 'ENCOUNTER_JOIN');
CALL etl.sp_execute_mapping('ENCOUNTER_JOIN');
UPDATE etl.etl_column_mapping SET source_column = 'dept_name'
 WHERE target_column = 'dept_name' AND job_id = (SELECT job_id FROM etl.etl_job WHERE job_name = 'ENCOUNTER_JOIN');

\echo '=== 13. Mapping document view ==='
SELECT job_name, column_order AS ord, target_column, target_data_type, source_table,
       source_alias AS alias, source_column, transformation_rule, default_value, is_key
FROM etl.v_mapping_document
WHERE job_name = 'ENCOUNTER_JOIN'
ORDER BY column_order;
SELECT DISTINCT job_name, from_clause FROM etl.v_mapping_document WHERE job_name = 'ENCOUNTER_JOIN';
