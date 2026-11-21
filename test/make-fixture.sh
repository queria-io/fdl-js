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
"
$FDL commit work/lake.sqlite -m "Second" --author test --dataset work/dataset.json
$FDL push work/lake.sqlite
rm work/lake.sqlite*
rm -rf work dataset.json
# The reader never opens the metadata or the profiles; leave them out to keep the fixture small
rm -r repo/versions/*/metadata.db repo/versions/*/profiles
