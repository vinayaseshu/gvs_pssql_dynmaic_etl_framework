/* =============================================================================
   Metadata-driven ETL framework for PostgreSQL (13+; MERGE load type needs 15+)
   -----------------------------------------------------------------------------
   Objects
     etl.etl_job             one row per source -> target load (driving table + alias)
     etl.etl_job_join        joins from the driving table to other tables/views
     etl.etl_column_mapping  source -> target column mappings per job
     etl.etl_run_log         audit log of every execution (incl. generated SQL)
     etl.fn_build_from       renders a job's FROM ... JOIN ... clause
     etl.v_mapping_document  flattened source-to-target mapping document
     etl.sp_execute_mapping  builds and runs the dynamic SQL for ONE job
     etl.sp_run_all          runs every active job (optionally by group), in order

   Generated statement shape
     SELECT <mapped exprs, cast to target types>
     FROM   <source> <alias>
            [INNER|LEFT|RIGHT|FULL|CROSS] JOIN <table> <alias> ON <condition>  (0..n)
     WHERE  <source_filter> AND <watermark window>
     [de-duplicated per key with ROW_NUMBER() when dedupe_on_key = true]

   Load types
     APPEND         INSERT ... SELECT
     TRUNCATE_LOAD  TRUNCATE target, then INSERT ... SELECT
     UPSERT         INSERT ... ON CONFLICT (key cols) DO UPDATE
                    (target needs a PK / unique index on the key columns)
     MERGE          MERGE INTO ... (PG15+; no unique constraint required,
                    updates only rows whose values actually changed)

   Security note: source_expression, default_value, source_filter,
   join_condition and dedupe_order_by are
   all executed as raw SQL. Only trusted ETL admins should have write access to
   the etl.* metadata tables.
============================================================================= */

CREATE SCHEMA IF NOT EXISTS etl;

-- -----------------------------------------------------------------------------
-- 1. Job header: which source goes to which target, and how
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl.etl_job (
    job_id                   integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_name                 text        NOT NULL UNIQUE,
    job_group                text        NOT NULL DEFAULT 'DEFAULT',
    run_order                integer     NOT NULL DEFAULT 100,
    source_schema            text        NOT NULL,
    source_table             text        NOT NULL,
    source_alias             text        NOT NULL DEFAULT 's'
        CHECK (source_alias ~ '^[a-z_][a-z0-9_]*$'),
    target_schema            text        NOT NULL,
    target_table             text        NOT NULL,
    load_type                text        NOT NULL DEFAULT 'APPEND'
        CHECK (load_type IN ('APPEND', 'TRUNCATE_LOAD', 'UPSERT', 'MERGE')),
    source_filter            text,        -- optional WHERE condition (no "WHERE"); may use any alias
    incremental_column       text,        -- optional driving-table column used as watermark
    last_watermark           text,        -- maintained by the procedure
    delete_source_after_load boolean     NOT NULL DEFAULT false,  -- true = "move" semantics
    dedupe_on_key            boolean     NOT NULL DEFAULT false,  -- keep 1 row per key column(s)
    dedupe_order_by          text,        -- which row wins, e.g. 'e.feed_seq DESC' (any alias)
    is_active                boolean     NOT NULL DEFAULT true,
    created_at               timestamptz NOT NULL DEFAULT now(),
    updated_at               timestamptz NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- 2. Joins: tables/views joined to the driving table, applied in join_order.
--    join_condition is the ON clause body and may reference the driving alias
--    and any join alias defined earlier, e.g.  p.mrn = e.mrn AND p.is_current
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl.etl_job_join (
    job_join_id     integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id          integer NOT NULL REFERENCES etl.etl_job(job_id) ON DELETE CASCADE,
    join_order      integer NOT NULL DEFAULT 100,
    join_type       text    NOT NULL DEFAULT 'INNER'
        CHECK (join_type IN ('INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS')),
    join_schema     text    NOT NULL,
    join_table      text    NOT NULL,       -- table or view
    join_alias      text    NOT NULL CHECK (join_alias ~ '^[a-z_][a-z0-9_]*$'),
    join_condition  text,                   -- ON clause body (NULL only for CROSS)
    is_active       boolean NOT NULL DEFAULT true,
    CONSTRAINT uq_job_join_alias UNIQUE (job_id, join_alias),
    CONSTRAINT ck_join_condition CHECK (
        (join_type =  'CROSS' AND join_condition IS NULL) OR
        (join_type <> 'CROSS' AND join_condition IS NOT NULL AND btrim(join_condition) <> ''))
);

-- -----------------------------------------------------------------------------
-- 3. Column mapping: one row per target column
--    Value = COALESCE(CAST(<source_expression | alias.source_column> AS <target type>), default_value)
--    alias = source_alias if given, otherwise the job's driving-table alias
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl.etl_column_mapping (
    mapping_id        integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id            integer NOT NULL REFERENCES etl.etl_job(job_id) ON DELETE CASCADE,
    target_column     text    NOT NULL,
    source_alias      text CHECK (source_alias ~ '^[a-z_][a-z0-9_]*$'),
                                        -- which table source_column comes from (NULL = driving table)
    source_column     text,             -- straight column copy
    source_expression text,             -- SQL expression over any alias; overrides source_column
    default_value     text,             -- SQL literal/expression used when the value is NULL
    is_key            boolean NOT NULL DEFAULT false,  -- business key for UPSERT / MERGE
    is_updatable      boolean NOT NULL DEFAULT true,   -- false = insert-only column (e.g. created_dt)
    compare_on_change boolean NOT NULL DEFAULT true,   -- MERGE: false = ignore when detecting changes (e.g. updated_dt)
    column_order      integer NOT NULL DEFAULT 100,
    is_active         boolean NOT NULL DEFAULT true,
    CONSTRAINT uq_job_target_col UNIQUE (job_id, target_column),
    CONSTRAINT ck_has_source CHECK (source_column IS NOT NULL
                                 OR source_expression IS NOT NULL
                                 OR default_value IS NOT NULL)
);

-- Upgrade path for installations created before joins were added (no-op on new installs)
ALTER TABLE etl.etl_job
    ADD COLUMN IF NOT EXISTS source_alias    text    NOT NULL DEFAULT 's'
        CHECK (source_alias ~ '^[a-z_][a-z0-9_]*$'),
    ADD COLUMN IF NOT EXISTS dedupe_on_key   boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS dedupe_order_by text;
ALTER TABLE etl.etl_column_mapping
    ADD COLUMN IF NOT EXISTS source_alias text CHECK (source_alias ~ '^[a-z_][a-z0-9_]*$');

-- -----------------------------------------------------------------------------
-- 4. Run log
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl.etl_run_log (
    run_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id         integer,
    job_name       text,
    load_type      text,
    status         text NOT NULL,     -- RUNNING / SUCCESS / FAILED / DRY_RUN
    rows_affected  bigint,
    rows_deleted   bigint,
    watermark_from text,
    watermark_to   text,
    generated_sql  text,
    error_message  text,
    error_detail   text,
    started_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    ended_at       timestamptz
);

-- -----------------------------------------------------------------------------
-- 5. FROM clause builder: driving table + active joins in join_order
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION etl.fn_build_from(p_job_id integer)
RETURNS text
LANGUAGE sql STABLE
AS $fn$
    SELECT format('%I.%I %I', j.source_schema, j.source_table, j.source_alias)
        || COALESCE((
            SELECT string_agg(
                       format(E'\n%s JOIN %I.%I %I%s',
                              jj.join_type, jj.join_schema, jj.join_table, jj.join_alias,
                              CASE WHEN jj.join_type = 'CROSS' THEN ''
                                   ELSE E'\n    ON ' || jj.join_condition END),
                       '' ORDER BY jj.join_order, jj.job_join_id)
            FROM etl.etl_job_join jj
            WHERE jj.job_id = j.job_id AND jj.is_active), '')
    FROM etl.etl_job j
    WHERE j.job_id = p_job_id;
$fn$;

-- -----------------------------------------------------------------------------
-- 6. Mapping document: one row per target column, with resolved source table,
--    transformation rule and the job's full FROM/JOIN clause
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW etl.v_mapping_document AS
SELECT j.job_name,
       j.job_group,
       j.load_type,
       format('%I.%I', j.target_schema, j.target_table)            AS target_table,
       m.column_order,
       m.target_column,
       format_type(ta.atttypid, ta.atttypmod)                       AS target_data_type,
       CASE WHEN m.source_expression IS NULL AND m.source_column IS NOT NULL THEN
            CASE WHEN jj.job_join_id IS NOT NULL
                 THEN format('%I.%I', jj.join_schema, jj.join_table)
                 ELSE format('%I.%I', j.source_schema, j.source_table) END
       END                                                          AS source_table,
       CASE WHEN m.source_expression IS NULL AND m.source_column IS NOT NULL
            THEN COALESCE(m.source_alias, j.source_alias) END       AS source_alias,
       m.source_column,
       CASE WHEN m.source_expression IS NOT NULL THEN m.source_expression
            WHEN m.source_column     IS NOT NULL THEN 'Direct move'
            ELSE 'Default value only' END                           AS transformation_rule,
       m.default_value,
       m.is_key,
       m.is_updatable,
       m.compare_on_change,
       j.source_filter,
       etl.fn_build_from(j.job_id)                                  AS from_clause,
       j.is_active AND m.is_active                                  AS is_active
FROM etl.etl_job j
JOIN etl.etl_column_mapping m ON m.job_id = j.job_id
LEFT JOIN etl.etl_job_join jj
       ON jj.job_id = j.job_id AND jj.is_active
      AND jj.join_alias = COALESCE(m.source_alias, j.source_alias)
LEFT JOIN pg_attribute ta
       ON ta.attrelid = to_regclass(format('%I.%I', j.target_schema, j.target_table))
      AND ta.attname = m.target_column AND ta.attnum > 0 AND NOT ta.attisdropped;

-- -----------------------------------------------------------------------------
-- 7. Execute ONE mapping job
--    CALL etl.sp_execute_mapping('job_name');              -- run
--    CALL etl.sp_execute_mapping('job_name', true);        -- dry run: build + log SQL only
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE etl.sp_execute_mapping(
    p_job_name  text,
    p_dry_run   boolean DEFAULT false,
    INOUT p_status text   DEFAULT NULL,
    INOUT p_rows   bigint DEFAULT NULL
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    j              etl.etl_job%ROWTYPE;
    v_run_id       bigint;
    v_src          text;   -- quoted source table
    v_tgt          text;   -- quoted target table
    v_src_oid      regclass;
    v_tgt_oid      regclass;
    v_missing      text;
    v_ins_cols     text;   -- "col1", "col2"
    v_sel_exprs    text;   -- <expr> AS "col1", ...
    v_key_cols     text;
    v_upd_set      text;   -- col = EXCLUDED.col / col = src.col
    v_on_clause    text;   -- MERGE join condition
    v_change_check text;   -- MERGE: only update changed rows
    v_src_cols     text;   -- src."col1", ...
    v_key_count    int;
    v_key_exprs    text;   -- key expressions for ROW_NUMBER() partitioning
    v_from         text;   -- FROM ... JOIN ... clause
    v_select       text;   -- full SELECT feeding the load
    v_join_count   int;
    v_where        text := '';
    v_wm_type      text;
    v_wm_new       text;
    v_sql          text;
    v_del_sql      text;
    v_rows_del     bigint := 0;
    v_err_msg      text;
    v_err_detail   text;
BEGIN
    p_status := NULL;
    p_rows   := 0;

    -- ---------------------------------------------------------------- job lookup
    SELECT * INTO j FROM etl.etl_job WHERE job_name = p_job_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ETL job "%" not found', p_job_name;
    END IF;

    INSERT INTO etl.etl_run_log (job_id, job_name, load_type, status, watermark_from)
    VALUES (j.job_id, j.job_name, j.load_type,
            CASE WHEN p_dry_run THEN 'DRY_RUN' ELSE 'RUNNING' END, j.last_watermark)
    RETURNING run_id INTO v_run_id;

    -- Everything below runs in a sub-transaction: on error the data changes roll
    -- back but the run-log row above survives and is marked FAILED.
    BEGIN
        IF NOT j.is_active THEN
            RAISE EXCEPTION 'Job "%" is inactive', j.job_name;
        END IF;

        v_src     := format('%I.%I', j.source_schema, j.source_table);
        v_tgt     := format('%I.%I', j.target_schema, j.target_table);
        v_src_oid := to_regclass(v_src);
        v_tgt_oid := to_regclass(v_tgt);

        IF v_src_oid IS NULL THEN RAISE EXCEPTION 'Source table % does not exist', v_src; END IF;
        IF v_tgt_oid IS NULL THEN RAISE EXCEPTION 'Target table % does not exist', v_tgt; END IF;

        -- ------------------------------------------------------------ validation
        IF NOT EXISTS (SELECT 1 FROM etl.etl_column_mapping
                       WHERE job_id = j.job_id AND is_active) THEN
            RAISE EXCEPTION 'Job "%" has no active column mappings', j.job_name;
        END IF;

        SELECT string_agg(m.target_column, ', ') INTO v_missing
        FROM etl.etl_column_mapping m
        WHERE m.job_id = j.job_id AND m.is_active
          AND NOT EXISTS (SELECT 1 FROM pg_attribute a
                          WHERE a.attrelid = v_tgt_oid AND a.attname = m.target_column
                            AND a.attnum > 0 AND NOT a.attisdropped);
        IF v_missing IS NOT NULL THEN
            RAISE EXCEPTION 'Target column(s) not found in %: %', v_tgt, v_missing;
        END IF;

        -- joins: tables exist, aliases don't clash with the driving alias
        SELECT string_agg(format('%I.%I', jj.join_schema, jj.join_table), ', '), count(*)
        INTO v_missing, v_join_count
        FROM etl.etl_job_join jj
        WHERE jj.job_id = j.job_id AND jj.is_active
          AND to_regclass(format('%I.%I', jj.join_schema, jj.join_table)) IS NULL;
        IF v_missing IS NOT NULL THEN
            RAISE EXCEPTION 'Join table(s) not found: %', v_missing;
        END IF;

        IF EXISTS (SELECT 1 FROM etl.etl_job_join jj
                   WHERE jj.job_id = j.job_id AND jj.is_active
                     AND jj.join_alias = j.source_alias) THEN
            RAISE EXCEPTION 'Join alias "%" clashes with the driving table alias', j.source_alias;
        END IF;

        SELECT count(*) INTO v_join_count
        FROM etl.etl_job_join jj WHERE jj.job_id = j.job_id AND jj.is_active;

        -- mapped source columns exist in the table behind their alias
        WITH aliases AS (
            SELECT j.source_alias AS alias, v_src_oid AS oid
            UNION ALL
            SELECT jj.join_alias, to_regclass(format('%I.%I', jj.join_schema, jj.join_table))
            FROM etl.etl_job_join jj
            WHERE jj.job_id = j.job_id AND jj.is_active
        )
        SELECT string_agg(
                   format('%s.%s', COALESCE(m.source_alias, j.source_alias), m.source_column)
                   || CASE WHEN al.alias IS NULL THEN ' (unknown alias)' ELSE '' END, ', ')
        INTO v_missing
        FROM etl.etl_column_mapping m
        LEFT JOIN aliases al ON al.alias = COALESCE(m.source_alias, j.source_alias)
        WHERE m.job_id = j.job_id AND m.is_active
          AND m.source_expression IS NULL AND m.source_column IS NOT NULL
          AND (al.alias IS NULL
               OR NOT EXISTS (SELECT 1 FROM pg_attribute a
                              WHERE a.attrelid = al.oid AND a.attname = m.source_column
                                AND a.attnum > 0 AND NOT a.attisdropped));
        IF v_missing IS NOT NULL THEN
            RAISE EXCEPTION 'Source column(s) not found: %', v_missing;
        END IF;

        v_from := etl.fn_build_from(j.job_id);

        -- ------------------------------------------------- column lists / exprs
        -- Each value is cast to the target column's exact type (incl. typmod).
        SELECT
            string_agg(format('%I', m.target_column), ', ' ORDER BY m.column_order, m.mapping_id),
            string_agg(format('%s AS %I', y.val_expr, m.target_column),
                       E',\n       ' ORDER BY m.column_order, m.mapping_id),
            string_agg(format('src.%I', m.target_column), ', ' ORDER BY m.column_order, m.mapping_id),
            string_agg(format('%I', m.target_column), ', ' ORDER BY m.column_order, m.mapping_id)
                FILTER (WHERE m.is_key),
            count(*) FILTER (WHERE m.is_key),
            string_agg(y.val_expr, ', ' ORDER BY m.column_order, m.mapping_id)
                FILTER (WHERE m.is_key)
        INTO v_ins_cols, v_sel_exprs, v_src_cols, v_key_cols, v_key_count, v_key_exprs
        FROM etl.etl_column_mapping m
        JOIN pg_attribute a ON a.attrelid = v_tgt_oid AND a.attname = m.target_column
                           AND a.attnum > 0 AND NOT a.attisdropped
        CROSS JOIN LATERAL (
            SELECT CASE WHEN m.source_expression IS NOT NULL THEN m.source_expression
                        WHEN m.source_column     IS NOT NULL
                             THEN format('%I.%I', COALESCE(m.source_alias, j.source_alias), m.source_column)
                        ELSE 'NULL' END AS src_expr
        ) x
        CROSS JOIN LATERAL (
            SELECT CASE WHEN m.default_value IS NOT NULL
                        THEN format('COALESCE(CAST(%s AS %s), CAST(%s AS %s))',
                                    x.src_expr, format_type(a.atttypid, a.atttypmod),
                                    m.default_value, format_type(a.atttypid, a.atttypmod))
                        ELSE format('CAST(%s AS %s)', x.src_expr, format_type(a.atttypid, a.atttypmod))
                   END AS val_expr
        ) y
        WHERE m.job_id = j.job_id AND m.is_active;

        IF j.load_type IN ('UPSERT', 'MERGE') AND v_key_count = 0 THEN
            RAISE EXCEPTION 'Load type % requires at least one key column (is_key = true)', j.load_type;
        END IF;
        IF j.dedupe_on_key AND v_key_count = 0 THEN
            RAISE EXCEPTION 'dedupe_on_key requires at least one key column (is_key = true)';
        END IF;

        -- ------------------------------------------------------------ WHERE clause
        IF j.source_filter IS NOT NULL AND btrim(j.source_filter) <> '' THEN
            v_where := format('(%s)', j.source_filter);
        END IF;

        IF j.incremental_column IS NOT NULL THEN
            SELECT format_type(a.atttypid, a.atttypmod) INTO v_wm_type
            FROM pg_attribute a
            WHERE a.attrelid = v_src_oid AND a.attname = j.incremental_column
              AND a.attnum > 0 AND NOT a.attisdropped;
            IF v_wm_type IS NULL THEN
                RAISE EXCEPTION 'Incremental column % not found in %', j.incremental_column, v_src;
            END IF;

            -- Freeze the upper bound first so rows arriving mid-load go to the next run.
            EXECUTE format('SELECT max(%I.%I)::text FROM %s %s',
                           j.source_alias, j.incremental_column, v_from,
                           CASE WHEN v_where <> '' THEN 'WHERE ' || v_where ELSE '' END)
            INTO v_wm_new;

            IF v_wm_new IS NULL THEN
                v_wm_new := j.last_watermark;   -- nothing in source
            END IF;

            v_where := concat_ws(' AND ', NULLIF(v_where, ''),
                CASE WHEN j.last_watermark IS NOT NULL
                     THEN format('%I.%I > CAST(%L AS %s)', j.source_alias, j.incremental_column,
                                 j.last_watermark, v_wm_type) END,
                CASE WHEN v_wm_new IS NOT NULL
                     THEN format('%I.%I <= CAST(%L AS %s)', j.source_alias, j.incremental_column,
                                 v_wm_new, v_wm_type) END);
        END IF;

        IF v_where <> '' THEN
            v_where := E'\nWHERE ' || v_where;
        END IF;

        -- ------------------------------------------------------------ SELECT body
        IF j.dedupe_on_key THEN
            -- One row per key: joins can fan out, and UPSERT/MERGE reject a key
            -- appearing twice in one statement.
            v_select := format(
                E'SELECT %s\nFROM (\n  SELECT %s,\n       row_number() OVER (PARTITION BY %s ORDER BY %s) AS etl_rn\n  FROM %s%s\n) etl_dedup\nWHERE etl_dedup.etl_rn = 1',
                v_ins_cols, v_sel_exprs, v_key_exprs,
                COALESCE(NULLIF(btrim(j.dedupe_order_by), ''), '(SELECT NULL)'),
                v_from, v_where);
        ELSE
            v_select := format(E'SELECT %s\nFROM %s%s', v_sel_exprs, v_from,
                               -- WHERE true avoids INSERT ... SELECT ... ON CONFLICT parse ambiguity
                               CASE WHEN v_where = '' AND j.load_type = 'UPSERT'
                                    THEN E'\nWHERE true' ELSE v_where END);
        END IF;

        -- ------------------------------------------------------------ build SQL
        CASE j.load_type
        WHEN 'APPEND', 'TRUNCATE_LOAD' THEN
            v_sql := format(E'INSERT INTO %s (%s)\n%s', v_tgt, v_ins_cols, v_select);

        WHEN 'UPSERT' THEN
            SELECT string_agg(format('%I = EXCLUDED.%I', m.target_column, m.target_column),
                              ', ' ORDER BY m.column_order, m.mapping_id)
            INTO v_upd_set
            FROM etl.etl_column_mapping m
            WHERE m.job_id = j.job_id AND m.is_active AND NOT m.is_key AND m.is_updatable;

            v_sql := format(E'INSERT INTO %s (%s)\n%s\nON CONFLICT (%s) %s',
                            v_tgt, v_ins_cols, v_select, v_key_cols,
                            CASE WHEN v_upd_set IS NULL THEN 'DO NOTHING'
                                 ELSE 'DO UPDATE SET ' || v_upd_set END);

        WHEN 'MERGE' THEN
            IF current_setting('server_version_num')::int < 150000 THEN
                RAISE EXCEPTION 'MERGE load type requires PostgreSQL 15+';
            END IF;

            SELECT string_agg(format('tgt.%I = src.%I', target_column, target_column), ' AND ')
            INTO v_on_clause
            FROM etl.etl_column_mapping
            WHERE job_id = j.job_id AND is_active AND is_key;

            SELECT string_agg(format('%I = src.%I', target_column, target_column),
                              ', ' ORDER BY column_order, mapping_id),
                   string_agg(format('tgt.%I IS DISTINCT FROM src.%I', target_column, target_column),
                              ' OR ' ORDER BY column_order, mapping_id) FILTER (WHERE compare_on_change)
            INTO v_upd_set, v_change_check
            FROM etl.etl_column_mapping
            WHERE job_id = j.job_id AND is_active AND NOT is_key AND is_updatable;

            v_sql := format(E'MERGE INTO %s AS tgt\nUSING (\n%s\n) AS src\nON %s',
                            v_tgt, v_select, v_on_clause);
            IF v_upd_set IS NOT NULL THEN
                v_sql := v_sql || format(E'\nWHEN MATCHED %sTHEN UPDATE SET %s',
                                         CASE WHEN v_change_check IS NOT NULL
                                              THEN format('AND (%s) ', v_change_check) ELSE '' END,
                                         v_upd_set);
            END IF;
            v_sql := v_sql || format(E'\nWHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)',
                                     v_ins_cols, v_src_cols);
        END CASE;

        IF j.delete_source_after_load THEN
            IF v_join_count = 0 THEN
                v_del_sql := format('DELETE FROM %s %I%s', v_src, j.source_alias, v_where);
            ELSE
                -- Delete exactly the driving rows the joined query selected
                -- (e.g. rows dropped by an INNER JOIN stay in the source).
                v_del_sql := format(
                    E'DELETE FROM %s AS etl_del\nWHERE (etl_del.tableoid, etl_del.ctid) IN (\n  SELECT %I.tableoid, %I.ctid\n  FROM %s%s)',
                    v_src, j.source_alias, j.source_alias, v_from, v_where);
            END IF;
        END IF;

        -- ------------------------------------------------------------ dry run
        IF p_dry_run THEN
            UPDATE etl.etl_run_log
               SET generated_sql = concat_ws(E';\n\n',
                       CASE WHEN j.load_type = 'TRUNCATE_LOAD' THEN 'TRUNCATE TABLE ' || v_tgt END,
                       v_sql, v_del_sql) || ';',
                   watermark_to = v_wm_new,
                   ended_at = clock_timestamp()
             WHERE run_id = v_run_id;
            RAISE NOTICE E'[DRY RUN] %:\n%', j.job_name, v_sql;
            p_status := 'DRY_RUN';
            RETURN;
        END IF;

        -- ------------------------------------------------------------ execute
        IF j.load_type = 'TRUNCATE_LOAD' THEN
            EXECUTE 'TRUNCATE TABLE ' || v_tgt;
        END IF;

        EXECUTE v_sql;
        GET DIAGNOSTICS p_rows = ROW_COUNT;

        IF v_del_sql IS NOT NULL THEN
            EXECUTE v_del_sql;
            GET DIAGNOSTICS v_rows_del = ROW_COUNT;
        END IF;

        UPDATE etl.etl_job
           SET last_watermark = COALESCE(v_wm_new, last_watermark),
               updated_at     = now()
         WHERE job_id = j.job_id;

        UPDATE etl.etl_run_log
           SET status        = 'SUCCESS',
               rows_affected = p_rows,
               rows_deleted  = v_rows_del,
               watermark_to  = v_wm_new,
               generated_sql = v_sql || COALESCE(E';\n\n' || v_del_sql, '') || ';',
               ended_at      = clock_timestamp()
         WHERE run_id = v_run_id;

        p_status := 'SUCCESS';
        RAISE NOTICE '[%] % : % row(s) loaded into %', p_status, j.job_name, p_rows, v_tgt;

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT, v_err_detail = PG_EXCEPTION_DETAIL;
        UPDATE etl.etl_run_log
           SET status        = 'FAILED',
               rows_affected = 0,
               generated_sql = v_sql,
               error_message = v_err_msg,
               error_detail  = v_err_detail,
               ended_at      = clock_timestamp()
         WHERE run_id = v_run_id;
        p_status := 'FAILED';
        p_rows   := 0;
        RAISE WARNING '[FAILED] % : %', j.job_name, v_err_msg;
    END;
END;
$proc$;

-- -----------------------------------------------------------------------------
-- 8. Run all active jobs (optionally one group), in run_order, committing after
--    each job so one failure doesn't undo the others.
--    Must be invoked with a top-level CALL (not inside BEGIN ... COMMIT).
--    CALL etl.sp_run_all();                    -- every active job
--    CALL etl.sp_run_all('PATIENT', true);     -- one group, stop at first failure
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE etl.sp_run_all(
    p_job_group     text    DEFAULT NULL,
    p_stop_on_error boolean DEFAULT false,
    p_dry_run       boolean DEFAULT false
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    r        record;
    v_status text;
    v_rows   bigint;
    v_ok     int := 0;
    v_failed int := 0;
BEGIN
    FOR r IN
        SELECT job_name FROM etl.etl_job
        WHERE is_active AND (p_job_group IS NULL OR job_group = p_job_group)
        ORDER BY run_order, job_id
    LOOP
        CALL etl.sp_execute_mapping(r.job_name, p_dry_run, v_status, v_rows);
        COMMIT;

        IF v_status = 'FAILED' THEN
            v_failed := v_failed + 1;
            IF p_stop_on_error THEN
                RAISE EXCEPTION 'Stopping batch: job "%" failed (see etl.etl_run_log)', r.job_name;
            END IF;
        ELSE
            v_ok := v_ok + 1;
        END IF;
    END LOOP;

    RAISE NOTICE 'Batch complete: % succeeded, % failed', v_ok, v_failed;
END;
$proc$;
