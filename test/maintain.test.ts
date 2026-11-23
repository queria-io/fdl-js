import { readdir, readFile } from "node:fs/promises";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";
import {
  collect,
  expire,
  type MaintainedStore,
  type Placed,
  readAssetValues,
} from "../src/maintain.js";
import { fileStore } from "../src/node.js";
import { openParquet, readRows } from "../src/parquet.js";
import { readVersion, resolve, tableFiles, tableNames } from "../src/repo.js";

const fixture = new URL("./fixture/repo/", import.meta.url).pathname;
const OLD = new Date("2026-01-01T00:00:00Z");
const NOW = new Date("2026-12-01T00:00:00Z");
const DAY = 86_400_000;

const files = async (dir: string): Promise<string[]> =>
  (await readdir(dir, { recursive: true, withFileTypes: true }))
    .filter((e) => e.isFile())
    .map((e) => relative(fixture, join(e.parentPath, e.name)));

// The fixture in memory, every object written at OLD
const memory = async (): Promise<
  MaintainedStore & { objects: Map<string, Placed & { body: string }> }
> => {
  const objects = new Map(
    await Promise.all(
      (await files(fixture)).map(
        async (key) =>
          [key, { key, uploaded: OLD, body: await readFile(join(fixture, key), "utf8") }] as const,
      ),
    ),
  );
  return {
    objects,
    text: async (key) => objects.get(key)?.body ?? null,
    size: async (key) => objects.get(key)?.body.length ?? null,
    range: async () => new ArrayBuffer(0),
    list: async (prefix) => [...objects.values()].filter((o) => o.key.startsWith(prefix)),
    put: async (key, body) => {
      objects.set(key, { key, uploaded: NOW, body });
    },
    delete: async (keys) => {
      for (const key of keys) objects.delete(key);
    },
  };
};

const dataKeys = async (store: MaintainedStore, id: string) => {
  const version = await readVersion(store, id);
  return tableNames(version).flatMap((t) =>
    version.files.tables[t]!.files.flatMap((f) => [
      version.config.data_dir + f.path,
      ...(f.deletes ? [version.config.data_dir + f.deletes.path] : []),
    ]),
  );
};

describe("expire", () => {
  it("takes the versions behind the newest out of retention", async () => {
    const store = await memory();
    const head = await resolve(store);
    const first = (await readVersion(store, head)).record.parents[0]!;
    expect(await expire(store, { keep: 1, graceMs: DAY, now: NOW })).toEqual([first]);
    expect(store.objects.has(`versions/${first}/expired.json`)).toBe(true);
    expect(store.objects.has(`versions/${first}/tables.json`)).toBe(false);
    expect(store.objects.has(`versions/${first}/version.json`)).toBe(true);
    // Already expired: nothing more to do
    expect(await expire(store, { keep: 1, graceMs: DAY, now: NOW })).toEqual([]);
  });

  it("records who took the versions out and why", async () => {
    const store = await memory();
    const first = (await readVersion(store, await resolve(store))).record.parents[0]!;
    await expire(store, {
      keep: 1,
      graceMs: DAY,
      by: "nightly retention",
      message: "Keep the newest version",
      now: NOW,
    });
    expect(JSON.parse((await store.text(`versions/${first}/expired.json`))!)).toEqual({
      expired_at: NOW.toISOString(),
      reason: "retention",
      by: "nightly retention",
      message: "Keep the newest version",
    });
  });

  it("deletes the metadata file at the name the record gives, and the profiles", async () => {
    const store = await memory();
    const first = (await readVersion(store, await resolve(store))).record.parents[0]!;
    const dir = `versions/${first}/`;
    const record = JSON.parse((await store.text(`${dir}version.json`))!);
    await store.put(
      `${dir}version.json`,
      JSON.stringify({ ...record, metadata: { ...record.metadata, path: "metadata-1.db" } }),
    );
    await store.put(`${dir}metadata-1.db`, "x");
    await store.put(`${dir}profiles/geo.town.json`, "x");
    expect(await expire(store, { keep: 1, graceMs: DAY, now: NOW })).toEqual([first]);
    expect(store.objects.has(`${dir}metadata-1.db`)).toBe(false);
    expect(store.objects.has(`${dir}profiles/geo.town.json`)).toBe(false);
  });

  it("lists what it would expire without changing anything", async () => {
    const store = await memory();
    const first = (await readVersion(store, await resolve(store))).record.parents[0]!;
    expect(await expire(store, { keep: 1, graceMs: DAY, now: NOW, dryRun: true })).toEqual([first]);
    expect(store.objects.has(`versions/${first}/expired.json`)).toBe(false);
  });

  it("keeps the versions created since a time", async () => {
    const store = await memory();
    expect(
      await expire(store, {
        keep: 1,
        graceMs: DAY,
        since: new Date("2000-01-01T00:00:00Z"),
        now: NOW,
      }),
    ).toEqual([]);
  });

  it("keeps a version a ref points at", async () => {
    const store = await memory();
    const first = (await readVersion(store, await resolve(store))).record.parents[0]!;
    await store.put("refs/tags/v1", `${first}\n`);
    expect(await expire(store, { keep: 1, graceMs: DAY, now: NOW })).toEqual([]);
  });

  it("expires nothing when a ref cannot be read", async () => {
    const store = await memory();
    const first = (await readVersion(store, await resolve(store))).record.parents[0]!;
    await store.put("refs/tags/v1", `${first}\n`);
    const text = store.text;
    store.text = async (key) => {
      if (key === "refs/tags/v1") throw new Error("unavailable");
      return text(key);
    };
    await expect(expire(store, { keep: 1, graceMs: DAY, now: NOW })).rejects.toThrow(/unavailable/);
    expect(store.objects.has(`versions/${first}/expired.json`)).toBe(false);
  });
});

describe("collect", () => {
  it("deletes what no live version reaches, once past the grace period", async () => {
    const store = await memory();
    const head = await resolve(store);
    const live = new Set(await dataKeys(store, head));
    store.objects.set("tables/main/towns/ducklake-unpushed.parquet", {
      key: "tables/main/towns/ducklake-unpushed.parquet",
      uploaded: OLD,
      body: "",
    });
    store.objects.set("tables/main/towns/ducklake-recent.parquet", {
      key: "tables/main/towns/ducklake-recent.parquet",
      uploaded: new Date(NOW.getTime() - 3_600_000),
      body: "",
    });
    await expire(store, { keep: 1, graceMs: DAY, now: NOW });

    const planned = await collect(store, { graceMs: DAY, now: NOW, dryRun: true });
    expect(planned).toContain("tables/main/towns/ducklake-unpushed.parquet");
    expect(planned).not.toContain("tables/main/towns/ducklake-recent.parquet");
    expect(planned.filter((key) => live.has(key))).toEqual([]);
    expect(store.objects.has("tables/main/towns/ducklake-unpushed.parquet")).toBe(true);

    expect(await collect(store, { graceMs: DAY, now: NOW })).toEqual(planned);
    expect(store.objects.has("tables/main/towns/ducklake-unpushed.parquet")).toBe(false);
    for (const key of live) expect(store.objects.has(key)).toBe(true);
  });

  it("keeps the assets the asset columns of a live version's tables.json hold", async () => {
    const store = await memory();
    const head = await resolve(store);
    const key = `versions/${head}/tables.json`;
    const list = JSON.parse(store.objects.get(key)!.body);
    list.tables["geo.town"].assets = ["name"];
    await store.put(key, JSON.stringify(list));
    const held = `assets/${"a".repeat(64)}`;
    const unheld = `assets/${"b".repeat(64)}`;
    for (const asset of [held, unheld])
      store.objects.set(asset, { key: asset, uploaded: OLD, body: "" });
    const deleted = await collect(store, {
      graceMs: DAY,
      now: NOW,
      assetValues: async (_, { id }, table, columns) =>
        id === head && table === "geo.town" && columns[0] === "name" ? ["a".repeat(64), null] : [],
    });
    expect(deleted).toContain(unheld);
    expect(deleted).not.toContain(held);
    expect(store.objects.has(held)).toBe(true);
  });

  it("deletes no asset while a live version's asset columns cannot be read", async () => {
    const store = await memory();
    const key = `versions/${await resolve(store)}/tables.json`;
    const list = JSON.parse(store.objects.get(key)!.body);
    list.tables["geo.town"].assets = ["name"];
    await store.put(key, JSON.stringify(list));
    const asset = `assets/${"a".repeat(64)}`;
    store.objects.set(asset, { key: asset, uploaded: OLD, body: "" });
    const deleted = await collect(store, {
      graceMs: DAY,
      now: NOW,
      assetValues: async () => {
        throw new Error("unavailable");
      },
    });
    expect(deleted).not.toContain(asset);
  });

  it("reads the asset columns from the data files", async () => {
    const store = fileStore(fixture);
    const version = await readVersion(store, await resolve(store));
    const rows = await Promise.all(
      tableFiles(version, "geo.town")!.map(async (f) => readRows(await openParquet(store, f))),
    );
    expect(
      await readAssetValues(store, { id: version.record.id, ...version }, "geo.town", ["name"]),
    ).toEqual(rows.flat().map((r) => r.name));
  });

  it("keeps what a version pushed while it runs reaches", async () => {
    const store = await memory();
    const head = await resolve(store);
    const file = "tables/main/towns/ducklake-reused.parquet";
    store.objects.set(file, { key: file, uploaded: OLD, body: "" });
    const pushed = "01a19990-ffff-7000-8000-000000000000";
    const text = store.text;
    store.text = async (key) => {
      if (
        key === `versions/${head}/tables.json` &&
        !store.objects.has(`versions/${pushed}/version.json`)
      ) {
        await store.put(
          `versions/${pushed}/version.json`,
          JSON.stringify({ id: pushed, parents: [head], tables: { path: "tables.json" } }),
        );
        await store.put(
          `versions/${pushed}/tables.json`,
          JSON.stringify({
            tables: { "main.towns": { files: [{ path: "main/towns/ducklake-reused.parquet" }] } },
          }),
        );
      }
      return text(key);
    };
    expect(await collect(store, { graceMs: DAY, now: NOW })).not.toContain(file);
    expect(store.objects.has(file)).toBe(true);
  });

  it("deletes only data files under data_dir", async () => {
    const store = await memory();
    store.objects.set("tables/README.md", { key: "tables/README.md", uploaded: OLD, body: "" });
    expect(await collect(store, { graceMs: DAY, now: NOW })).not.toContain("tables/README.md");
  });

  it("refuses when a live version has no tables.json", async () => {
    const store = await memory();
    await store.delete([`versions/${await resolve(store)}/tables.json`]);
    await expect(collect(store, { graceMs: DAY, now: NOW })).rejects.toThrow(/no tables.json/);
  });

  it("refuses a live version whose record names tables.json without a path", async () => {
    const store = await memory();
    const key = `versions/${await resolve(store)}/version.json`;
    const record = JSON.parse(store.objects.get(key)!.body);
    await store.put(key, JSON.stringify({ ...record, tables: {} }));
    await expect(collect(store, { graceMs: DAY, now: NOW })).rejects.toThrow(/outside/);
  });

  it("deletes the tables.json the record names when it takes a version out", async () => {
    const store = await memory();
    const first = (await readVersion(store, await resolve(store))).record.parents[0]!;
    const dir = `versions/${first}/`;
    const record = JSON.parse(store.objects.get(`${dir}version.json`)!.body);
    const listing = store.objects.get(`${dir}tables.json`)!;
    store.objects.delete(`${dir}tables.json`);
    store.objects.set(`${dir}tables-1.json`, { ...listing, key: `${dir}tables-1.json` });
    await store.put(
      `${dir}version.json`,
      JSON.stringify({ ...record, tables: { ...record.tables, path: "tables-1.json" } }),
    );
    expect(await expire(store, { keep: 1, graceMs: DAY, now: NOW })).toEqual([first]);
    expect(store.objects.has(`${dir}tables-1.json`)).toBe(false);
  });

  it("refuses a live version whose tables.json path leaves its folder", async () => {
    const store = await memory();
    const key = `versions/${await resolve(store)}/version.json`;
    const record = JSON.parse(store.objects.get(key)!.body);
    await store.put(
      key,
      JSON.stringify({ ...record, tables: { ...record.tables, path: "../x.json" } }),
    );
    await expect(collect(store, { graceMs: DAY, now: NOW })).rejects.toThrow(/outside/);
  });

  it("skips a version folder whose name is not a version ID", async () => {
    const store = await memory();
    await store.put("versions/not-an-id/version.json", "{}");
    await expect(collect(store, { graceMs: DAY, now: NOW })).resolves.toBeDefined();
  });
});

describe("expire with a malformed parent", () => {
  it("does not follow a parent that is not a version ID", async () => {
    const store = await memory();
    const head = await resolve(store);
    const key = `versions/${head}/version.json`;
    const record = JSON.parse(store.objects.get(key)!.body);
    await store.put(key, JSON.stringify({ ...record, parents: ["../../refs/heads/main"] }));
    // The head has no parent it follows, so the version that was its parent is on no branch
    expect(await expire(store, { keep: 1, graceMs: DAY, now: NOW, dryRun: true })).toEqual([
      record.parents[0],
    ]);
  });
});
