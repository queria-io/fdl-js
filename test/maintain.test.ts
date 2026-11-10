import { readdir, readFile } from "node:fs/promises";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";
import { collect, expire, type MaintainedStore, type Placed } from "../src/maintain.js";
import { readVersion, resolve, tableNames } from "../src/repo.js";

const fixture = new URL("./fixture/repo/", import.meta.url).pathname;
const OLD = new Date("2026-01-01T00:00:00Z");
const NOW = new Date("2026-02-01T00:00:00Z");
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
    expect(await expire(store, { keep: 1, now: NOW })).toEqual([first]);
    expect(store.objects.has(`versions/${first}/expired.json`)).toBe(true);
    expect(store.objects.has(`versions/${first}/files.json`)).toBe(false);
    expect(store.objects.has(`versions/${first}/version.json`)).toBe(true);
    // Already expired: nothing more to do
    expect(await expire(store, { keep: 1, now: NOW })).toEqual([]);
  });

  it("keeps a version a ref points at", async () => {
    const store = await memory();
    const first = (await readVersion(store, await resolve(store))).record.parents[0]!;
    await store.put("refs/tags/v1", `${first}\n`);
    expect(await expire(store, { keep: 1, now: NOW })).toEqual([]);
  });
});

describe("collect", () => {
  it("deletes what no live version reaches, once past the grace period", async () => {
    const store = await memory();
    const head = await resolve(store);
    const live = new Set(await dataKeys(store, head));
    store.objects.set("data/main/towns/ducklake-unpushed.parquet", {
      key: "data/main/towns/ducklake-unpushed.parquet",
      uploaded: OLD,
      body: "",
    });
    store.objects.set("data/main/towns/ducklake-recent.parquet", {
      key: "data/main/towns/ducklake-recent.parquet",
      uploaded: new Date(NOW.getTime() - 3_600_000),
      body: "",
    });
    await expire(store, { keep: 1, now: NOW });

    const planned = await collect(store, { graceMs: DAY, now: NOW, dryRun: true });
    expect(planned).toContain("data/main/towns/ducklake-unpushed.parquet");
    expect(planned).not.toContain("data/main/towns/ducklake-recent.parquet");
    expect(planned.filter((key) => live.has(key))).toEqual([]);
    expect(store.objects.has("data/main/towns/ducklake-unpushed.parquet")).toBe(true);

    expect(await collect(store, { graceMs: DAY, now: NOW })).toEqual(planned);
    expect(store.objects.has("data/main/towns/ducklake-unpushed.parquet")).toBe(false);
    for (const key of live) expect(store.objects.has(key)).toBe(true);
  });

  it("refuses when a live version has no files.json", async () => {
    const store = await memory();
    await store.delete([`versions/${await resolve(store)}/files.json`]);
    await expect(collect(store, { graceMs: DAY, now: NOW })).rejects.toThrow(/no files.json/);
  });
});
