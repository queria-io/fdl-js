import { describe, expect, it } from "vitest";
import { fileStore } from "../src/node.js";
import {
  FdlError,
  open,
  readVersion,
  refreshing,
  resolve,
  tableFiles,
  tableNames,
} from "../src/repo.js";
import type { Store } from "../src/store.js";

const store = fileStore(new URL("./fixture/repo/", import.meta.url).pathname);

// The fixture (test/make-fixture.sh) has two versions; main holds the second
const versions = async () => {
  const head = await resolve(store);
  const { record } = await readVersion(store, head);
  return { first: record.parents[0]!, second: head };
};

describe("resolve", () => {
  it("follows HEAD, full and short names to the same version", async () => {
    const head = await resolve(store);
    expect(await resolve(store, "refs/heads/main")).toBe(head);
    expect(await resolve(store, "main")).toBe(head);
    expect(await resolve(store, head)).toBe(head);
  });

  it("refuses a ref that is not there", async () => {
    await expect(resolve(store, "nope")).rejects.toThrow(FdlError);
  });
});

describe("open", () => {
  it("lists the tables of the version main holds", async () => {
    const version = await open(store);
    expect(version.record.seq).toBe(2);
    expect(tableNames(version).sort()).toEqual([
      "geo.town",
      "main.empty",
      "main.gone",
      "main.renamed",
    ]);
  });

  it("gives data file keys under data_dir", async () => {
    const files = tableFiles(await open(store), "geo.town")!;
    expect(files).toHaveLength(1);
    expect(files[0]!.key).toMatch(/^data\/geo\/town\/ducklake-.*\.parquet$/);
    expect(files[0]!.rows).toBe(10000);
    expect(await store.size(files[0]!.key)).toBe(files[0]!.bytes);
  });

  it("returns undefined for a missing table and [] for an empty one", async () => {
    const version = await open(store);
    expect(tableFiles(version, "main.none")).toBeUndefined();
    expect(tableFiles(version, "main.empty")).toEqual([]);
  });

  it("refuses tables with deleted rows or renamed columns", async () => {
    const version = await open(store);
    expect(() => tableFiles(version, "main.gone")).toThrow(/deleted rows/);
    expect(() => tableFiles(version, "main.renamed")).toThrow(/renamed columns/);
  });

  it("reads them in the version before the change", async () => {
    const version = await readVersion(store, (await versions()).first);
    expect(tableFiles(version, "main.gone")).toHaveLength(1);
    expect(tableFiles(version, "main.renamed")).toHaveLength(1);
  });
});

/** The fixture with some keys replaced */
const patched = (over: Record<string, string | null>): Store => ({
  ...store,
  text: async (key) => (key in over ? over[key]! : store.text(key)),
});

describe("readVersion", () => {
  it("refuses an expired version", async () => {
    const { second } = await versions();
    const s = patched({ [`versions/${second}/expired.json`]: '{"reason":"retention"}' });
    await expect(readVersion(s, second)).rejects.toThrow(/past retention/);
  });

  it("refuses an unknown major version", async () => {
    const s = patched({ "fdl.json": '{"fdl":"1.0"}' });
    await expect(open(s)).rejects.toThrow(/unknown/);
  });

  it("refuses a version without files.json", async () => {
    const { second } = await versions();
    await expect(
      readVersion(patched({ [`versions/${second}/files.json`]: null }), second),
    ).rejects.toThrow(/no files.json/);
  });

  it("refuses a path that leaves the repository", async () => {
    const { second } = await versions();
    const list = { tables: { "main.t": { files: [{ path: "../x.parquet", rows: 1, bytes: 1 }] } } };
    const version = await readVersion(
      patched({ [`versions/${second}/files.json`]: JSON.stringify(list) }),
      second,
    );
    expect(() => tableFiles(version, "main.t")).toThrow(/outside/);
  });

  it("refuses a path that leaves the repository percent-encoded", async () => {
    const { second } = await versions();
    const list = {
      tables: { "main.t": { files: [{ path: "%2e%2e/%2E%2e/x.parquet", rows: 1, bytes: 1 }] } },
    };
    const version = await readVersion(
      patched({ [`versions/${second}/files.json`]: JSON.stringify(list) }),
      second,
    );
    expect(() => tableFiles(version, "main.t")).toThrow(/outside/);
  });

  it("refuses a data_dir outside the repository", async () => {
    await expect(open(patched({ "fdl.json": '{"fdl":"0.1","data_dir":"../x/"}' }))).rejects.toThrow(
      /outside/,
    );
  });
});

describe("refreshing", () => {
  it("keeps the result for the ttl and the last one when a reload fails", async () => {
    let t = 0;
    let calls = 0;
    let fail = false;
    const errors: string[] = [];
    const get = refreshing(
      async () => {
        calls += 1;
        if (fail) throw new Error("down");
        return calls;
      },
      100,
      { now: () => t, onError: (e) => errors.push(String(e)) },
    );
    expect(await get()).toBe(1);
    t = 50;
    expect(await get()).toBe(1);
    t = 150;
    fail = true;
    expect(await get()).toBe(1);
    expect(errors).toEqual(["Error: down"]);
    t = 300;
    fail = false;
    expect(await get()).toBe(3);
  });
});
