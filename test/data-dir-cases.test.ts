// The data_dir cases every implementation of the FDL spec agrees on. test/cases/data-dir.json
// is a copy of cases/data-dir.json in queria-io/fdl: update it from there, never by hand
import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { readConfig } from "../src/repo.js";
import type { Store } from "../src/store.js";

type Case = { name: string; fdl_json: unknown; data_dir: string | null };
const { cases } = JSON.parse(
  await readFile(new URL("./cases/data-dir.json", import.meta.url), "utf8"),
) as { cases: Case[] };

const store = (config: unknown) =>
  ({ text: async (key: string) => (key === "fdl.json" ? JSON.stringify(config) : null) }) as Store;

describe("data_dir cases", () => {
  for (const c of cases) {
    it(c.name, async () => {
      const read = readConfig(store(c.fdl_json));
      if (c.data_dir === null) await expect(read).rejects.toThrow(/data_dir/);
      else expect((await read).data_dir).toBe(c.data_dir);
    });
  }
});
