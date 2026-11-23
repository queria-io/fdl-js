import { describe, expect, it } from "vitest";
import { fileStore } from "../src/node.js";
import {
  FdlError,
  open,
  readVersion,
  refreshing,
  viewNames,
  viewOf,
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
      "main.nested",
      "main.renamed",
    ]);
  });

  it("gives data file keys under data_dir", async () => {
    const files = tableFiles(await open(store), "geo.town")!;
    expect(files).toHaveLength(1);
    expect(files[0]!.key).toMatch(/^tables\/geo\/town\/ducklake-.*\.parquet$/);
    expect(files[0]!.rows).toBe(10000);
    expect(await store.size(files[0]!.key)).toBe(files[0]!.bytes);
  });

  it("returns undefined for a missing table and [] for an empty one", async () => {
    const version = await open(store);
    expect(tableFiles(version, "main.none")).toBeUndefined();
    expect(tableFiles(version, "main.empty")).toEqual([]);
  });

  it("gives each data file its delete file and the table's columns", async () => {
    const version = await open(store);
    const [gone] = tableFiles(version, "main.gone")!;
    expect(gone!.deletes).toMatchObject({ rows: 3 });
    expect(gone!.deletes!.key).toMatch(/^tables\/main\/gone\/ducklake-.*-delete\.parquet$/);
    expect(gone!.columns!.map((c) => [c.id, c.name])).toEqual([[1, "i"]]);
    const renamed = tableFiles(version, "main.renamed")!;
    expect(renamed[0]!.columns!.map((c) => c.name)).toEqual(["b", "c"]);
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

  it("refuses a version without tables.json", async () => {
    const { second } = await versions();
    await expect(
      readVersion(patched({ [`versions/${second}/tables.json`]: null }), second),
    ).rejects.toThrow(/no tables.json/);
  });

  it("refuses a path that leaves the dataset", async () => {
    const { second } = await versions();
    const list = { tables: { "main.t": { files: [{ path: "../x.parquet", rows: 1, bytes: 1 }] } } };
    const version = await readVersion(
      patched({ [`versions/${second}/tables.json`]: JSON.stringify(list) }),
      second,
    );
    expect(() => tableFiles(version, "main.t")).toThrow(/outside/);
  });

  it("refuses a path that leaves the dataset percent-encoded", async () => {
    const { second } = await versions();
    const list = {
      tables: { "main.t": { files: [{ path: "%2e%2e/%2E%2e/x.parquet", rows: 1, bytes: 1 }] } },
    };
    const version = await readVersion(
      patched({ [`versions/${second}/tables.json`]: JSON.stringify(list) }),
      second,
    );
    expect(() => tableFiles(version, "main.t")).toThrow(/outside/);
  });

  it("refuses a data_dir outside the dataset", async () => {
    await expect(open(patched({ "fdl.json": '{"fdl":"0.1","data_dir":"../x/"}' }))).rejects.toThrow(
      /not a folder under the root/,
    );
  });
});

describe("views", () => {
  const view = {
    sql: "SELECT lg_code, count(*) AS towns FROM geo.town GROUP BY lg_code",
    columns: [
      { name: "lg_code", type: "VARCHAR" },
      { name: "towns", type: "BIGINT" },
    ],
  };

  it("lists the views of tables.json with their SQL and columns", async () => {
    const version = await open(store);
    const listed = { ...version, files: { ...version.files, views: { "main.town_count": view } } };
    expect(viewNames(listed)).toEqual(["main.town_count"]);
    expect(viewOf(listed, "main.town_count")).toEqual(view);
    expect(viewOf(listed, "main.none")).toBeUndefined();
    expect(viewOf(listed, "constructor")).toBeUndefined();
    expect(viewOf(listed, "__proto__")).toBeUndefined();
    expect(tableFiles(listed, "constructor")).toBeUndefined();
  });

  it("gives no views when tables.json lists none", async () => {
    const version = await open(store);
    expect(viewNames({ ...version, files: { tables: {} } })).toEqual([]);
  });
});

describe("tables.json", () => {
  // The second version with its tables.json replaced
  const withTables = async (list: object) => {
    const { second } = await versions();
    return readVersion(
      patched({ [`versions/${second}/tables.json`]: JSON.stringify(list) }),
      second,
    );
  };
  const list = {
    tables: {
      "geo.town": {
        files: [{ path: "geo/town/a.parquet", rows: 1, bytes: 1 }],
        primary_key: ["n"],
        assets: ["image"],
      },
      "main.raw": { files: [], profile: false },
    },
    views: { "main.town_count": { sql: "SELECT 1 AS n", profile: false } },
    dependencies: {
      census: {
        url: "https://example.org/census/",
        version: "01a20000-0000-7000-8000-000000000000",
      },
    },
  };

  it("reads tables.json with its declarations, views and dependencies", async () => {
    const version = await withTables(list);
    expect(tableNames(version).sort()).toEqual(["geo.town", "main.raw"]);
    expect(version.files.tables["geo.town"]).toMatchObject({
      primary_key: ["n"],
      assets: ["image"],
    });
    expect(version.files.tables["main.raw"]!.profile).toBe(false);
    expect(viewOf(version, "main.town_count")).toEqual({ sql: "SELECT 1 AS n", profile: false });
    expect(version.files.dependencies).toEqual(list.dependencies);
  });

  it("refuses a version.json that names tables.json outside the version", async () => {
    const { second } = await versions();
    const dir = `versions/${second}/`;
    const record = JSON.parse((await store.text(dir + "version.json"))!);
    record.tables = { path: "../x.json", bytes: 0, sha256: "" };
    await expect(
      readVersion(patched({ [dir + "version.json"]: JSON.stringify(record) }), second),
    ).rejects.toThrow(/outside/);
  });

  it("refuses a version.json whose tables names no file", async () => {
    const { second } = await versions();
    const dir = `versions/${second}/`;
    const record = JSON.parse((await store.text(dir + "version.json"))!);
    record.tables = { bytes: 0, sha256: "" };
    await expect(
      readVersion(patched({ [dir + "version.json"]: JSON.stringify(record) }), second),
    ).rejects.toThrow(/outside/);
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
