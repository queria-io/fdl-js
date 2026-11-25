import { describe, expect, it } from "vitest";
import { attachSql, planAttach } from "../src/duckdb.js";
import { FdlError } from "../src/repo.js";
import type { Store } from "../src/store.js";

const V = (n: number) => `019a2f10-0c4d-7e88-a1b2-${String(n).padStart(12, "0")}`;

// A dataset in memory: main holds the last version, each with the dependencies given
const dataset = (
  versions: Record<string, unknown>[],
  dataDir = "tables/",
): Record<string, string> =>
  Object.fromEntries([
    ["fdl.json", JSON.stringify({ fdl: "0.1", data_dir: dataDir })],
    ["refs/heads/main", V(versions.length)],
    ["refs/tags/first", V(1)],
    ...versions.flatMap((dependencies, i) => [
      [`versions/${V(i + 1)}/version.json`, JSON.stringify({ fdl: "0.1", id: V(i + 1) })],
      [`versions/${V(i + 1)}/tables.json`, JSON.stringify({ tables: {}, dependencies })],
    ]),
  ]);

const opener =
  (roots: Record<string, Record<string, string>>) =>
  (url: string): Store => ({
    text: async (key) => roots[url]?.[key] ?? null,
    size: async () => null,
    range: async () => new ArrayBuffer(0),
  });

const census = dataset([{}, {}], "data/");
const towns = dataset([{ census: { url: "https://x.org/census", version: V(1) } }]);

describe("planAttach", () => {
  const open = opener({ "https://x.org/census/": census, "https://x.org/towns/": towns });

  it("attaches what main holds, after the pinned datasets its views read", async () => {
    const plan = await planAttach(open, { towns: { url: "https://x.org/towns/" } });
    expect(plan).toEqual([
      {
        name: "census",
        url: "https://x.org/census/",
        version: V(1),
        metadata: `https://x.org/census/versions/${V(1)}/metadata.db`,
        dataPath: "https://x.org/census/data/",
      },
      {
        name: "towns",
        url: "https://x.org/towns/",
        version: V(1),
        metadata: `https://x.org/towns/versions/${V(1)}/metadata.db`,
        dataPath: "https://x.org/towns/tables/",
      },
    ]);
  });

  it("reads a tag or a version ID", async () => {
    const [byTag] = await planAttach(open, {
      c: { url: "https://x.org/census/", version: "first" },
    });
    expect(byTag!.version).toBe(V(1));
    const [main] = await planAttach(open, { c: { url: "https://x.org/census/" } });
    expect(main!.version).toBe(V(2));
  });

  it("attaches a name once when both ask for the same version", async () => {
    const plan = await planAttach(open, {
      census: { url: "https://x.org/census/", version: V(1) },
      towns: { url: "https://x.org/towns/" },
    });
    expect(plan.map((a) => a.name)).toEqual(["census", "towns"]);
  });

  it("refuses one name at two versions", async () => {
    await expect(
      planAttach(open, {
        census: { url: "https://x.org/census/" },
        towns: { url: "https://x.org/towns/" },
      }),
    ).rejects.toThrow(FdlError);
  });

  it("ends at a cycle", async () => {
    const a = dataset([{ b: { url: "https://x.org/b/" } }]);
    const b = dataset([{ a: { url: "https://x.org/a/" } }]);
    const plan = await planAttach(opener({ "https://x.org/a/": a, "https://x.org/b/": b }), {
      a: { url: "https://x.org/a/" },
    });
    expect(plan.map((p) => p.name)).toEqual(["b", "a"]);
  });

  it("refuses a version past retention", async () => {
    const expired = { ...census, [`versions/${V(1)}/expired.json`]: "{}" };
    await expect(
      planAttach(opener({ "https://x.org/census/": expired }), {
        c: { url: "https://x.org/census/", version: V(1) },
      }),
    ).rejects.toThrow(/past retention/);
  });
});

describe("attachSql", () => {
  it("passes the data path and quotes the name and URLs", () => {
    expect(
      attachSql({
        name: 'a"b',
        url: "https://x.org/o'k/",
        version: V(1),
        metadata: `https://x.org/o'k/versions/${V(1)}/metadata.db`,
        dataPath: "https://x.org/o'k/tables/",
      }),
    ).toBe(
      `ATTACH 'ducklake:https://x.org/o''k/versions/${V(1)}/metadata.db' AS "a""b" ` +
        `(DATA_PATH 'https://x.org/o''k/tables/', OVERRIDE_DATA_PATH true, READ_ONLY)`,
    );
  });
});
