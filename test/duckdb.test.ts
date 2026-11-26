import { describe, expect, it } from "vitest";
import { attachSql, planAttach } from "../src/duckdb.js";
import { ExpiredError, FdlError } from "../src/repo.js";
import type { Store } from "../src/store.js";

const V = (n: number) => `019a2f10-0c4d-7e88-a1b2-${String(n).padStart(12, "0")}`;

// A dataset in memory: main holds the last version, each with the dependencies given
const dataset = (
  versions: Record<string, unknown>[],
  dataDir = "tables/",
): Record<string, string> =>
  Object.fromEntries([
    ["fdl.json", JSON.stringify({ fdl: "0.1", data_dir: dataDir })],
    ["HEAD", "ref: refs/heads/main\n"],
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
    const [byId] = await planAttach(open, { c: { url: "https://x.org/census/", version: V(1) } });
    expect(byId!.version).toBe(V(1));
    const [main] = await planAttach(open, { c: { url: "https://x.org/census/" } });
    expect(main!.version).toBe(V(2));
  });

  it("attaches what the branch HEAD names holds", async () => {
    const census = {
      ...dataset([{}, {}]),
      HEAD: "ref: refs/heads/stable\n",
      "refs/heads/stable": `${V(1)}\n`,
    };
    const [found] = await planAttach(opener({ "https://x.org/census/": census }), {
      c: { url: "https://x.org/census/" },
    });
    expect(found!.version).toBe(V(1));
  });

  it("attaches what refs/heads/main holds when the dataset has no HEAD", async () => {
    const { HEAD: _, ...census } = dataset([{}, {}]);
    const [found] = await planAttach(opener({ "https://x.org/census/": census }), {
      c: { url: "https://x.org/census/" },
    });
    expect(found!.version).toBe(V(2));
  });

  it("gives the metadata's sha256 from the record", async () => {
    const record = `versions/${V(2)}/version.json`;
    const withSha = {
      ...census,
      [record]: JSON.stringify({ fdl: "0.1", id: V(2), metadata: { sha256: "abc" } }),
    };
    const [a] = await planAttach(opener({ "https://x.org/census/": withSha }), {
      c: { url: "https://x.org/census/" },
    });
    expect(a!.sha256).toBe("abc");
  });

  it("attaches a name once when a mirror holds the same version", async () => {
    const mirror = opener({
      "https://x.org/census/": census,
      "https://m.org/census/": census,
      "https://x.org/towns/": towns,
    });
    const plan = await planAttach(mirror, {
      census: { url: "https://m.org/census/", version: V(1) },
      towns: { url: "https://x.org/towns/" },
    });
    expect(plan.map((a) => [a.name, a.url])).toEqual([
      ["census", "https://m.org/census/"],
      ["towns", "https://x.org/towns/"],
    ]);
  });

  it("attaches what HEAD names when a recorded version is gone", async () => {
    const gone = { ...census, [`versions/${V(1)}/expired.json`]: "{}" };
    const plan = await planAttach(
      opener({ "https://x.org/census/": gone, "https://x.org/towns/": towns }),
      { towns: { url: "https://x.org/towns/" } },
    );
    expect(plan.map((a) => [a.name, a.version, a.recorded])).toEqual([
      ["census", V(2), V(1)],
      ["towns", V(1), undefined],
    ]);
    // A version the caller names is not swapped for another
    await expect(
      planAttach(opener({ "https://x.org/census/": gone }), {
        c: { url: "https://x.org/census/", version: V(1) },
      }),
    ).rejects.toThrow(ExpiredError);
  });

  it("reads HEAD once for a gone version and an entry that names HEAD", async () => {
    // main moves to V(3) after it is first read
    const moving = { ...dataset([{}, {}, {}]), [`versions/${V(1)}/expired.json`]: "{}" };
    let reads = 0;
    const store: Store = {
      text: async (key) =>
        key === "refs/heads/main" ? (reads++ === 0 ? V(2) : V(3)) : (moving[key] ?? null),
      size: async () => null,
      range: async () => new ArrayBuffer(0),
    };
    const plan = await planAttach(
      (url) => (url === "https://x.org/census/" ? store : opener({ [url]: towns })(url)),
      {
        census: { url: "https://x.org/census/" },
        towns: { url: "https://x.org/towns/" },
      },
    );
    expect(plan.map((a) => [a.name, a.version])).toEqual([
      ["census", V(2)],
      ["towns", V(1)],
    ]);
  });

  it("refuses a name DuckDB would not tell apart or keeps for itself", async () => {
    for (const name of ["Census", "memory", "temp", "a-b"])
      await expect(planAttach(open, { [name]: { url: "https://x.org/census/" } })).rejects.toThrow(
        FdlError,
      );
  });

  it("refuses a dependency at a relative URL or a ref", async () => {
    for (const dep of [{ url: "census/" }, { url: "https://x.org/census/", version: "first" }]) {
      const t = dataset([{ census: dep }]);
      await expect(
        planAttach(opener({ "https://x.org/census/": census, "https://x.org/t/": t }), {
          t: { url: "https://x.org/t/" },
        }),
      ).rejects.toThrow(FdlError);
    }
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

  it("reads the datasets a version's views read side by side, each after the ones it reads", async () => {
    const roots = {
      "https://x.org/census/": census,
      "https://x.org/towns/": towns,
      "https://x.org/report/": dataset([
        {
          towns: { url: "https://x.org/towns/", version: V(1) },
          census: { url: "https://x.org/census/", version: V(1) },
        },
      ]),
    };
    let reading = 0;
    let most = 0;
    const slow = (url: string): Store => {
      const store = opener(roots)(url);
      return {
        ...store,
        text: async (key) => {
          most = Math.max(most, ++reading);
          await new Promise((resolve) => setTimeout(resolve, 5));
          reading -= 1;
          return store.text(key);
        },
      };
    };
    const plan = await planAttach(slow, { report: { url: "https://x.org/report/" } });
    expect(plan.map((p) => p.name)).toEqual(["census", "towns", "report"]);
    expect(most).toBeGreaterThan(2);
  });

  it("keeps the URL an entry names, whichever read finishes first", async () => {
    const roots = {
      "https://x.org/census/": census,
      "https://m.org/census/": census,
      "https://x.org/towns/": towns,
    };
    const slowMirror = (url: string): Store => {
      const store = opener(roots)(url);
      return {
        ...store,
        text: async (key) => {
          if (url.startsWith("https://m.org/")) await new Promise((r) => setTimeout(r, 20));
          return store.text(key);
        },
      };
    };
    const plan = await planAttach(slowMirror, {
      census: { url: "https://m.org/census/", version: V(1) },
      towns: { url: "https://x.org/towns/" },
    });
    expect(plan.map((a) => [a.name, a.url])).toEqual([
      ["census", "https://m.org/census/"],
      ["towns", "https://x.org/towns/"],
    ]);
  });

  it("follows a dataset many others read once", async () => {
    // Each dataset reads all the ones before it: following every path would take 2^n steps
    const n = 40;
    const roots: Record<string, Record<string, string>> = {};
    for (let i = 0; i < n; i++)
      roots[`https://x.org/d${i}/`] = dataset([
        Object.fromEntries(
          Array.from({ length: i }, (_, j) => [
            `d${j}`,
            { url: `https://x.org/d${j}/`, version: V(1) },
          ]),
        ),
      ]);
    const plan = await planAttach(opener(roots), {
      [`d${n - 1}`]: { url: `https://x.org/d${n - 1}/` },
    });
    expect(plan.map((p) => p.name)).toEqual(Array.from({ length: n }, (_, i) => `d${i}`));
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
