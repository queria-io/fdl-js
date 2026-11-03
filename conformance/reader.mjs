// The reader command for FDL's conformance suite (queria-io/fdl, conformance/):
//   node conformance/reader.mjs <root> <ref>
// Prints the version and the rows of each table, read from the Parquet footers.
// A table with deleted rows or renamed columns is left out: fdl-js does not apply them.
import { FdlError, open, openParquet, tableFiles, tableNames } from "../dist/index.js";
import { fileStore } from "../dist/node.js";

const [root, ref] = process.argv.slice(2);
const store = fileStore(root);

const rows = async (version, name) => {
  try {
    const files = tableFiles(version, name) ?? [];
    const opened = await Promise.all(files.map((f) => openParquet(store, f)));
    return opened.reduce((sum, p) => sum + Number(p.metadata.num_rows), 0);
  } catch (e) {
    if (e instanceof FdlError) return undefined;
    throw e;
  }
};

try {
  const version = await open(store, ref);
  const counted = await Promise.all(
    tableNames(version).map(async (name) => [name, await rows(version, name)]),
  );
  const tables = Object.fromEntries(
    counted.filter(([, n]) => n !== undefined).map(([name, n]) => [name, { rows: n }]),
  );
  console.log(JSON.stringify({ version: version.record.id, seq: version.record.seq, tables }));
} catch (e) {
  console.error(`refused: ${e instanceof Error ? e.message : e}`);
  process.exit(1);
}
