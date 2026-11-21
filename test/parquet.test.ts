import { describe, expect, it } from "vitest";
import { fileStore } from "../src/node.js";
import {
  memoryFooterCache,
  openParquet,
  readRows,
  readWhere,
  rowRangesFor,
} from "../src/parquet.js";
import { open, tableFiles } from "../src/repo.js";

const store = fileStore(new URL("./fixture/repo/", import.meta.url).pathname);
const town = async () => tableFiles(await open(store), "geo.town")![0]!;

type Town = { lg_code: string; n: bigint; name: string };

describe("openParquet", () => {
  it("reads the footer in one read when footer_bytes is known", async () => {
    const reads: number[] = [];
    const parquet = await openParquet(store, await town(), {
      onRead: (bytes) => reads.push(bytes),
    });
    expect(reads).toHaveLength(1);
    expect(parquet.metadata.row_groups).toHaveLength(5);
  });

  it("reads the footer in two reads without it", async () => {
    const reads: number[] = [];
    const { footerBytes: _, ...file } = await town();
    await openParquet(store, file, { onRead: (bytes) => reads.push(bytes) });
    expect(reads).toHaveLength(2);
  });

  it("reads nothing when the footer is cached", async () => {
    const cache = memoryFooterCache();
    await openParquet(store, await town(), { cache });
    const reads: number[] = [];
    await openParquet(store, await town(), { cache, onRead: (bytes) => reads.push(bytes) });
    expect(reads).toHaveLength(0);
  });
});

describe("reading rows", () => {
  it("picks the row groups that can hold a value", async () => {
    const { metadata } = await openParquet(store, await town());
    // 2,048 rows a group, 1,000 rows a code: 000003 is in the second group only
    expect(rowRangesFor(metadata, "lg_code", "000003")).toEqual([{ rowStart: 2048, rowEnd: 4096 }]);
    // 000002 sits on the border of the first two, which are merged
    expect(rowRangesFor(metadata, "lg_code", "000002")).toEqual([{ rowStart: 0, rowEnd: 4096 }]);
    expect(rowRangesFor(metadata, "lg_code", "999999")).toEqual([]);
  });

  it("reads the rows of one value", async () => {
    const parquet = await openParquet(store, await town());
    const rows = await readWhere<Town>(parquet, "lg_code", "000003", ["name"]);
    expect(rows).toHaveLength(1000);
    expect(rows[0]).toEqual({ lg_code: "000003", name: "town 3000" });
  });

  it("matches integers given as numbers", async () => {
    const parquet = await openParquet(store, await town());
    expect(await readWhere<Town>(parquet, "n", 42)).toEqual([
      { lg_code: "000000", n: 42n, name: "town 42" },
    ]);
  });

  it("finds nothing, without throwing, for a fraction on an integer column", async () => {
    const parquet = await openParquet(store, await town());
    expect(await readWhere<Town>(parquet, "n", 42.5)).toEqual([]);
  });

  it("compares strings in UTF-8 byte order, as Parquet statistics do", () => {
    // U+FF21 sorts before U+1F600 in UTF-8, after it in UTF-16
    const group = (min: string, max: string) => ({
      num_rows: 10n,
      columns: [{ meta_data: { statistics: { min_value: min, max_value: max } } }],
    });
    const metadata = {
      schema: [{ name: "root" }, { name: "s" }],
      row_groups: [group("\uFF21", "\u{1F600}")],
    } as unknown as Parameters<typeof rowRangesFor>[0];
    expect(rowRangesFor(metadata, "s", "\uFF21")).toEqual([{ rowStart: 0, rowEnd: 10 }]);
    expect(rowRangesFor(metadata, "s", "\uFF22")).toEqual([{ rowStart: 0, rowEnd: 10 }]);
    expect(rowRangesFor(metadata, "s", "\u{1F601}")).toEqual([]);
  });

  it("skips columns the file lacks", async () => {
    const parquet = await openParquet(store, await town());
    const rows = await readRows(parquet, {
      rowStart: 0,
      rowEnd: 2,
      columns: ["name", "added_later"],
    });
    expect(rows).toEqual([{ name: "town 0" }, { name: "town 1" }]);
  });
});

describe("tables changed after their files were written", () => {
  const read = async (table: string, rev?: string) => {
    const version = await open(store, rev);
    const files = await Promise.all(tableFiles(version, table)!.map((f) => openParquet(store, f)));
    return (await Promise.all(files.map((f) => readRows(f)))).flat();
  };

  it("leaves out deleted rows", async () => {
    const rows = await read("main.gone");
    expect(rows.map((r) => r.i)).toEqual([3n, 4n, 5n, 6n, 7n, 8n, 9n]);
  });

  it("reads a renamed column under its new name and fills a column added later", async () => {
    const rows = await read("main.renamed");
    expect(rows).toHaveLength(13);
    expect(rows[0]).toEqual({ b: 0n, c: null });
    expect(rows.at(-1)).toEqual({ b: 12n, c: "x" });
    expect(rows.filter((r) => r.b === 12n)).toHaveLength(1);
  });

  it("reads a range with the deleted positions of that range", async () => {
    const [file] = tableFiles(await open(store), "main.gone")!;
    const parquet = await openParquet(store, file!);
    expect(await readRows(parquet, { rowStart: 2, rowEnd: 5, columns: ["i"] })).toEqual([
      { i: 3n },
      { i: 4n },
    ]);
  });

  it("finds rows by a renamed column", async () => {
    const version = await open(store);
    const [first] = tableFiles(version, "main.renamed")!;
    const parquet = await openParquet(store, first!);
    expect(await readWhere(parquet, "b", 4)).toEqual([{ b: 4n, c: null }]);
    expect(await readWhere(parquet, "c", "x")).toEqual([]);
    expect(await readWhere(parquet, "c", 1)).toEqual([]);
  });

  it("fills a column added later with its default", async () => {
    const version = await open(store);
    const entry = version.files.tables["main.renamed"]!;
    const columns = [
      entry.columns![0]!,
      { id: 2, name: "c", type: "varchar", nullable: true, default: "none" },
    ];
    const [first] = tableFiles(version, "main.renamed")!;
    const parquet = await openParquet(store, { ...first!, columns });
    expect((await readRows(parquet))[0]).toEqual({ b: 0n, c: "none" });
    expect(await readWhere(parquet, "c", "none")).toHaveLength(10);
  });

  it("reads a range past the end of a file that lacks every column asked for", async () => {
    const [first] = tableFiles(await open(store), "main.renamed")!;
    const parquet = await openParquet(store, first!);
    expect(await readRows(parquet, { rowStart: 5, rowEnd: 100, columns: ["c"] })).toHaveLength(5);
    expect(await readRows(parquet, { rowStart: 50, columns: ["c"] })).toEqual([]);
  });

  it("reads DuckDB's text for an infinite default", async () => {
    const version = await open(store);
    const entry = version.files.tables["main.renamed"]!;
    const columns = [
      entry.columns![0]!,
      { id: 2, name: "c", type: "float64", nullable: true, default: "-inf" },
    ];
    const [first] = tableFiles(version, "main.renamed")!;
    const parquet = await openParquet(store, { ...first!, columns });
    expect((await readRows(parquet))[0]).toEqual({ b: 0n, c: -Infinity });
  });

  it("reads renamed and added fields inside structs, lists and maps", async () => {
    const rows = await read("main.nested");
    expect(rows.map(({ id, s, l, m }) => ({ id, s, l, m }))).toEqual([
      { id: 1, s: { a2: 1, b: "x", c: 5 }, l: [{ p2: 1 }], m: { k: { q2: 2 } } },
      { id: 2, s: { a2: 3, b: "y", c: 4 }, l: [{ p2: 5 }], m: { j: { q2: 6 } } },
    ]);
  });

  it("fills defaults as the values a file written after holds them", async () => {
    const [old, written] = await read("main.nested");
    const added = ({ d, ts, tz, dec, tm, bl, h, js }: Record<string, unknown>) => ({
      d,
      ts,
      tz,
      dec,
      tm,
      bl,
      h,
      js,
    });
    expect(added(old!)).toEqual(added(written!));
    expect(added(old!)).toEqual({
      d: new Date("2020-01-02T00:00:00Z"),
      ts: new Date("2020-01-02T03:04:05.123Z"),
      tz: new Date("2020-01-01T18:04:05Z"),
      dec: 12.34,
      tm: 11_045_500_000n,
      bl: "\u0001B",
      h: 3,
      js: { a: 1 },
    });
  });
});
