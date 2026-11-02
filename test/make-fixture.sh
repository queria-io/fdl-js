#!/usr/bin/env bash
# Builds test/fixture/repo with the fdl CLI (https://github.com/queria-io/fdl).
# Usage: FDL="uvx --from fdl-cli fdl" test/make-fixture.sh
set -euo pipefail
cd "$(dirname "$0")/fixture"
FDL=${FDL:-fdl}
rm -rf repo work && mkdir work
$FDL init repo/
attach() {
  $FDL checkout repo/ work/lake.sqlite | grep '^ *ATTACH'
}
sql() {
  python - "$1" "$(attach)" <<'PY'
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
CALL lake.set_option('parquet_row_group_size', 2048);
CALL lake.set_option('parquet_compression', 'zstd');
CREATE SCHEMA lake.geo;
CREATE TABLE lake.geo.town AS
  SELECT printf('%06d', i // 1000) AS lg_code, i AS n, 'town ' || i AS name
  FROM range(10000) t(i) ORDER BY lg_code;
CREATE TABLE lake.main.gone AS SELECT i FROM range(10) t(i);
CREATE TABLE lake.main.renamed AS SELECT i AS a FROM range(10) t(i);
CREATE TABLE lake.main.empty (x INTEGER);
"
$FDL commit work/lake.sqlite -m "First" --author test --dataset work/dataset.json
sql "
DELETE FROM lake.main.gone WHERE i < 3;
ALTER TABLE lake.main.renamed RENAME COLUMN a TO b;
"
$FDL commit work/lake.sqlite -m "Second" --author test --dataset work/dataset.json
rm -rf work
# The reader never opens the catalogs; leave them out to keep the fixture small
rm repo/versions/*/ducklake.duckdb
