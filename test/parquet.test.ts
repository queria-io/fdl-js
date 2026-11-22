import { describe, expect, it } from "vitest";
import { fileStore } from "../src/node.js";
import {
  memoryFooterCache,
  openParquet,
  readRows,
  readWhere,
  rowRangesFor,
} from "../src/parquet.js";
import { FdlError, open, tableFiles } from "../src/repo.js";

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

  const fillWith = async (column: { type: string; default: string }) => {
    const version = await open(store);
    const entry = version.files.tables["main.renamed"]!;
    const columns = [entry.columns![0]!, { id: 2, name: "c", nullable: true, ...column }];
    const [first] = tableFiles(version, "main.renamed")!;
    return openParquet(store, { ...first!, columns });
  };

  it("fills timestamps before 1970, before the common era and after 9999 as hyparquet reads them", async () => {
    const c = async (type: string, text: string) =>
      (await readRows(await fillWith({ type, default: text }), { rowEnd: 1 }))[0]!.c;
    // hyparquet divides -876544 microseconds by 1000 toward zero
    expect(await c("timestamp", "1969-12-31 23:59:59.123456")).toEqual(new Date(-876));
    expect(await c("date", "0044-03-15 (BC)")).toEqual(
      new Date(new Date(0).setUTCFullYear(-43, 2, 15)),
    );
    expect(await c("timestamptz", "0044-03-15 (BC) 12:00:00.5+00")).toEqual(
      new Date(new Date(0).setUTCFullYear(-43, 2, 15) + 12 * 3_600_000 + 500),
    );
    expect(await c("date", "10000-01-01")).toEqual(
      new Date(new Date(0).setUTCFullYear(10000, 0, 1)),
    );
  });

  it("fills times as hyparquet reads them, a time with a time zone in UTC", async () => {
    const c = async (type: string, text: string) =>
      (await readRows(await fillWith({ type, default: text }), { rowEnd: 1 }))[0]!.c;
    // The values hyparquet reads from files DuckDB writes with these defaults
    expect(await c("timetz", "03:04:05+09")).toBe(65_045_000_000n);
    expect(await c("time_ns", "01:02:03.123456789")).toBe(3_723_123_456_789n);
  });

  it("refuses a default of a type DuckLake takes no default for", async () => {
    const parquet = await fillWith({ type: "struct", default: "{'a': 1}" });
    await expect(readRows(parquet)).rejects.toThrow(FdlError);
  });
});

describe("conditions", () => {
  // 5 row groups of 2,048 rows. kind is a or z in every group, and b to f in one group each, in that order
  const kinds = async (reads?: number[]) => {
    const key = "kinds.parquet";
    const fixture = fileStore(new URL("./fixture/", import.meta.url).pathname);
    const bytes = await fixture.size(key);
    return openParquet(
      fixture,
      { key, rows: 10_240, bytes: bytes! },
      {
        onRead: (b) => reads?.push(b),
      },
    );
  };
  type Kind = { n: bigint; kind: string; addr: string };
  const ns = (rows: Kind[]) => rows.map((r) => Number(r.n));

  it("reads a range from the row groups that can hold it", async () => {
    const { metadata } = await kinds();
    expect(rowRangesFor(metadata, "n", { gte: 2048, lt: 4096 })).toEqual([
      { rowStart: 2048, rowEnd: 4096 },
    ]);
    expect(rowRangesFor(metadata, "n", { gt: 10_239 })).toEqual([]);
    const rows = await readWhere<Kind>(await kinds(), "n", { gte: 4000, lte: 4100 }, ["kind"]);
    expect(ns(rows)).toEqual(Array.from({ length: 101 }, (_, i) => 4000 + i));
  });

  it("reads the values of IN", async () => {
    const parquet = await kinds();
    expect(rowRangesFor(parquet.metadata, "n", { in: [1, 9000] })).toEqual([
      { rowStart: 0, rowEnd: 2048 },
      { rowStart: 8192, rowEnd: 10_240 },
    ]);
    expect(ns(await readWhere<Kind>(parquet, "n", { in: [1, 9000n, 20_000] }))).toEqual([1, 9000]);
  });

  it("reads a prefix, comparing in code point order", async () => {
    const parquet = await kinds();
    // 大 < 東 < 神 in code point order
    expect(rowRangesFor(parquet.metadata, "addr", { prefix: "東京都" })).toEqual([
      { rowStart: 4096, rowEnd: 8192 },
    ]);
    expect(rowRangesFor(parquet.metadata, "addr", { prefix: "東京都0" })).toEqual([
      { rowStart: 4096, rowEnd: 8192 },
    ]);
    expect(rowRangesFor(parquet.metadata, "addr", { prefix: "京都" })).toEqual([]);
    const rows = await readWhere<Kind>(parquet, "addr", { prefix: "東京都0410" });
    expect(rows.map((r) => r.addr)).toEqual(Array.from({ length: 10 }, (_, i) => `東京都0410${i}`));
  });

  it("skips the row groups whose bloom filter lacks every value", async () => {
    const reads: number[] = [];
    const parquet = await kinds(reads);
    // min/max (a to z) keep every group
    expect(rowRangesFor(parquet.metadata, "kind", "c")).toHaveLength(1);
    expect(rowRangesFor(parquet.metadata, "kind", "c")[0]).toEqual({ rowStart: 0, rowEnd: 10_240 });
    reads.length = 0;
    const rows = await readWhere<Kind>(parquet, "kind", "c", ["n"]);
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((r) => Number(r.n) >= 2048 && Number(r.n) < 4096)).toBe(true);
    // The five bloom filters in one read, then the one row group
    expect(reads[0]).toBe(5 * 47);
    const bf = ns(await readWhere<Kind>(parquet, "kind", { in: ["b", "f"] }, ["n"]));
    expect(new Set(bf.map((n) => Math.floor(n / 2048)))).toEqual(new Set([0, 4]));
  });

  it("reads nothing of the data when no bloom filter holds the value", async () => {
    const reads: number[] = [];
    const parquet = await kinds(reads);
    reads.length = 0;
    expect(await readWhere(parquet, "kind", "y")).toEqual([]);
    expect(reads).toEqual([5 * 47]);
  });

  it("matches a condition against a column added after the file was written", async () => {
    const version = await open(store);
    const entry = version.files.tables["main.renamed"]!;
    const columns = [
      entry.columns![0]!,
      { id: 2, name: "c", type: "varchar", nullable: true, default: "none" },
    ];
    const [first] = tableFiles(version, "main.renamed")!;
    const parquet = await openParquet(store, { ...first!, columns });
    expect(await readWhere(parquet, "c", { prefix: "no" })).toHaveLength(10);
    expect(await readWhere(parquet, "c", { in: ["x", "y"] })).toEqual([]);
    expect(await readWhere(parquet, "b", { gte: 8 })).toEqual([
      { b: 8n, c: "none" },
      { b: 9n, c: "none" },
    ]);
  });
});
