-- Minimal repro from duckdb-iceberg-bug-rewrite-empty (MoR delete all rows, rewrite_all).
-- Usage: make mock && ./build/reldebug/duckdb -unsigned -f scripts/repro_rewrite_empty_copy.sql

LOAD iceberg;
LOAD parquet;
LOAD avro;
LOAD httpfs;

ATTACH '' AS my_datalake (
    TYPE ICEBERG,
    URI 'http://127.0.0.1:19134',
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none',
    STAGE_CREATE_TABLES true
);
CREATE SCHEMA IF NOT EXISTS my_datalake.default;

DROP TABLE IF EXISTS my_datalake.default.rewrite_empty_copy_repro;

CREATE TABLE my_datalake.default.rewrite_empty_copy_repro (id INTEGER, project_id VARCHAR)
WITH ('format-version' = 2);

INSERT INTO my_datalake.default.rewrite_empty_copy_repro VALUES (1, 'p1');
INSERT INTO my_datalake.default.rewrite_empty_copy_repro VALUES (2, 'p1');
INSERT INTO my_datalake.default.rewrite_empty_copy_repro VALUES (3, 'p2');
INSERT INTO my_datalake.default.rewrite_empty_copy_repro VALUES (4, 'p2');
INSERT INTO my_datalake.default.rewrite_empty_copy_repro VALUES (5, 'p2');

DELETE FROM my_datalake.default.rewrite_empty_copy_repro;

CALL iceberg_rewrite_data_files('my_datalake.default.rewrite_empty_copy_repro', rewrite_all => true);
