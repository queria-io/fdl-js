// The expire cases every implementation of the FDL spec agrees on. test/cases/expire.json
// is a copy of cases/expire.json in queria-io/fdl: update it from there, never by hand
import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { expire, type MaintainedStore } from "../src/maintain.js";

type Case = {
  name: string;
  keep: number;
  since: string | null;
  versions: { id: string; parents: string[]; created_at: string }[];
  expired: string[];
  refs: Record<string, string>;
  head?: string;
  take_out: string[];
};
const { now, grace_seconds, record, cases } = JSON.parse(
  await readFile(new URL("./cases/expire.json", import.meta.url), "utf8"),
) as { now: string; grace_seconds: number; record: object; cases: Case[] };

const store = (c: Case): MaintainedStore => {
  const objects = new Map<string, string>([
    ["fdl.json", JSON.stringify({ fdl: "0.1", data_dir: "data/" })],
    ...c.versions.map(
      (v) => [`versions/${v.id}/version.json`, JSON.stringify({ ...record, ...v })] as const,
    ),
    ...c.expired.map(
      (id) =>
        [
          `versions/${id}/expired.json`,
          JSON.stringify({ expired_at: now, reason: "retention" }),
        ] as const,
    ),
    ...Object.entries(c.refs).map(([name, id]) => [name, `${id}\n`] as const),
    ["HEAD", `ref: ${c.head ?? "refs/heads/main"}\n`],
  ]);
  return {
    text: async (key) => objects.get(key) ?? null,
    size: async (key) => objects.get(key)?.length ?? null,
    range: async () => new ArrayBuffer(0),
    list: async (prefix) =>
      [...objects.keys()]
        .filter((key) => key.startsWith(prefix))
        .map((key) => ({ key, uploaded: new Date(now) })),
    put: async (key, body) => {
      objects.set(key, body);
    },
    delete: async (keys) => {
      for (const key of keys) objects.delete(key);
    },
  };
};

describe("expire cases", () => {
  for (const c of cases) {
    it(c.name, async () => {
      const taken = await expire(store(c), {
        keep: c.keep,
        since: c.since === null ? undefined : new Date(c.since),
        graceMs: grace_seconds * 1000,
        now: new Date(now),
        dryRun: true,
      });
      expect([...taken].sort()).toEqual([...c.take_out].sort());
    });
  }
});
