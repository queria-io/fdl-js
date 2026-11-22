// The gc cases every implementation of the FDL spec agrees on. test/cases/gc.json
// is a copy of cases/gc.json in queria-io/fdl: update it from there, never by hand
import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { collect, type MaintainedStore, type Placed } from "../src/maintain.js";

type Case = {
  name: string;
  files: Record<string, unknown>;
  objects: Record<string, string>;
  /** The tables of each metadata file, by its key, with their rows */
  metadata?: Record<string, Record<string, { rows: Record<string, unknown>[] }>>;
  deleted: string[];
};
const { now, grace_seconds, cases } = JSON.parse(
  await readFile(new URL("./cases/gc.json", import.meta.url), "utf8"),
) as { now: string; grace_seconds: number; cases: Case[] };

const store = (c: Case): MaintainedStore & { keys: () => string[] } => {
  const objects = new Map<string, Placed & { body: string }>([
    ...Object.entries(c.files).map(
      ([key, value]) =>
        [key, { key, uploaded: new Date(now), body: JSON.stringify(value) }] as const,
    ),
    ...Object.entries(c.objects).map(
      ([key, placed]) => [key, { key, uploaded: new Date(placed), body: "" }] as const,
    ),
  ]);
  return {
    keys: () => [...objects.keys()],
    text: async (key) => objects.get(key)?.body ?? null,
    size: async (key) => objects.get(key)?.body.length ?? null,
    range: async () => new ArrayBuffer(0),
    list: async (prefix) => [...objects.values()].filter((o) => o.key.startsWith(prefix)),
    put: async (key, body) => {
      objects.set(key, { key, uploaded: new Date(now), body });
    },
    delete: async (keys) => {
      for (const key of keys) objects.delete(key);
    },
  };
};

describe("gc cases", () => {
  for (const c of cases) {
    it(c.name, async () => {
      const s = store(c);
      // Stopping with an error is how an implementation deletes nothing
      const deleted = await collect(s, {
        graceMs: grace_seconds * 1000,
        now: new Date(now),
        // The rows come from the case's metadata, which only DuckDB can open
        assetValues: async (_, { id }, table, columns) => {
          const tables = c.metadata?.[`versions/${id}/metadata.db`];
          if (!tables) throw new Error(`no metadata for ${id}`);
          return (tables[table]?.rows ?? []).flatMap((row) => columns.map((col) => row[col]));
        },
      }).catch(() => []);
      expect([...deleted].sort()).toEqual([...c.deleted].sort());
      expect(
        Object.keys(c.objects)
          .filter((key) => !s.keys().includes(key))
          .sort(),
      ).toEqual([...c.deleted].sort());
    });
  }
});
