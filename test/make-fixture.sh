#!/usr/bin/env bash
# Builds test/fixture/repo with the fdl CLI (https://github.com/queria-io/fdl).
# Usage: FDL="uvx --from fdl-cli fdl" test/make-fixture.sh
# PYTHON is a Python with duckdb, such as the fdl-cli tool environment
set -euo pipefail
cd "$(dirname "$0")/fixture"
FDL=${FDL:-fdl}
rm -rf repo work && mkdir work
$FDL init repo/
attach() {
  [ -f work/lake.sqlite ] || $FDL clone repo/ work/lake.sqlite >/dev/null
  printf "ATTACH 'ducklake:work/lake.sqlite' AS lake (DATA_PATH '%s', OVERRIDE_DATA_PATH true, META_TYPE 'sqlite', DATA_INLINING_ROW_LIMIT 0)" "$PWD/repo/tables/"
}
sql() {
  ${PYTHON:-python} - "$1" "$(attach)" <<'PY'
import sys, duckdb
con = duckdb.connect()
con.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
con.execute(sys.argv[2])
con.execute(sys.argv[1])
PY
}
cat > work/dataset.json <<'JSON'
{ "name": "towns", "title": "Towns" }
JSON
sql "
CALL ducklake_set_option('lake', 'parquet_row_group_size', 2048);
CALL ducklake_set_option('lake', 'parquet_compression', 'zstd');
CREATE SCHEMA lake.geo;
CREATE TABLE lake.geo.town AS
  SELECT printf('%06d', i // 1000) AS lg_code, i AS n, 'town ' || i AS name
  FROM range(10000) t(i) ORDER BY lg_code;
CREATE TABLE lake.main.gone AS SELECT i FROM range(10) t(i);
CREATE TABLE lake.main.renamed AS SELECT i AS a FROM range(10) t(i);
CREATE TABLE lake.main.empty (x INTEGER);
CREATE TABLE lake.main.nested AS
  SELECT 1 AS id, {'a': 1, 'b': 'x'} AS s, [{'p': 1}] AS l, MAP {'k': {'q': 2}} AS m;
"
$FDL commit work/lake.sqlite -m "First" --author test --dataset work/dataset.json
$FDL push work/lake.sqlite
rm work/lake.sqlite*
sql "
DELETE FROM lake.main.gone WHERE i < 3;
ALTER TABLE lake.main.renamed RENAME COLUMN a TO b;
INSERT INTO lake.main.renamed SELECT i AS b FROM range(10, 13) t(i);
ALTER TABLE lake.main.renamed ADD COLUMN c VARCHAR;
UPDATE lake.main.renamed SET c = 'x' WHERE b = 12;
ALTER TABLE lake.main.nested RENAME COLUMN s.a TO a2;
ALTER TABLE lake.main.nested ADD COLUMN s.c INTEGER DEFAULT 5;
ALTER TABLE lake.main.nested RENAME COLUMN l.element.p TO p2;
ALTER TABLE lake.main.nested RENAME COLUMN m.value.q TO q2;
ALTER TABLE lake.main.nested ADD COLUMN d DATE DEFAULT DATE '2020-01-02';
ALTER TABLE lake.main.nested ADD COLUMN ts TIMESTAMP DEFAULT TIMESTAMP '2020-01-02 03:04:05.123456';
ALTER TABLE lake.main.nested ADD COLUMN tz TIMESTAMPTZ DEFAULT TIMESTAMPTZ '2020-01-02 03:04:05+09';
ALTER TABLE lake.main.nested ADD COLUMN dec DECIMAL(10,2) DEFAULT 12.34;
ALTER TABLE lake.main.nested ADD COLUMN tm TIME DEFAULT TIME '03:04:05.5';
ALTER TABLE lake.main.nested ADD COLUMN bl BLOB DEFAULT '\\x01B'::BLOB;
ALTER TABLE lake.main.nested ADD COLUMN h HUGEINT DEFAULT 3;
ALTER TABLE lake.main.nested ADD COLUMN js JSON DEFAULT '{\"a\":1}';
INSERT INTO lake.main.nested (id, s, l, m) VALUES (2, {'a2': 3, 'b': 'y', 'c': 4}, [{'p2': 5}], MAP {'j': {'q2': 6}});
USE lake;
CREATE VIEW main.town_count AS SELECT lg_code, count(*) AS towns FROM geo.town GROUP BY lg_code;
CREATE SCHEMA _fdl;
CREATE TABLE _fdl.dataset AS SELECT 'Towns' AS title, DATE '2020-01-02' AS issued,
  [{'id': 'cc-by-4.0', 'spdx': 'CC-BY-4.0', 'commercial_use': true}] AS licenses;
CREATE TABLE _fdl.tables AS SELECT * FROM (VALUES
  ('geo', 'town', 'Towns', ['n'], NULL::BOOLEAN),
  ('main', 'town_count', 'Towns by code', NULL, false)
) t(schema, name, title, primary_key, profile);
CREATE TABLE _fdl.columns AS SELECT * FROM (VALUES
  ('geo', 'town', 'lg_code', 'Code', 'lg_code')
) t(schema, \"table\", name, title, entity);
CREATE TABLE _fdl.catalogs AS SELECT 'census' AS name, 'https://example.org/census/' AS url;
"
$FDL commit work/lake.sqlite -m "Second" --author test --dataset work/dataset.json
$FDL push work/lake.sqlite
rm work/lake.sqlite*
rm -rf work dataset.json
# The reader never opens the metadata or the profiles; leave them out to keep the fixture small
rm -r repo/versions/*/metadata.db repo/versions/*/profiles
