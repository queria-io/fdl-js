import { describe, expect, it } from "vitest";
import { fileStore } from "../src/node";
import { memoryFooterCache, openParquet, readRows, readWhere, rowRangesFor } from "../src/parquet";
import { open, tableFiles } from "../src/repo";

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
