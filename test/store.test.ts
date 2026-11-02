import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { openParquet, readWhere } from "../src/parquet";
import { open, tableFiles } from "../src/repo";
import { httpStore, r2Store, type R2Like } from "../src/store";

const dir = new URL("./fixture/repo/", import.meta.url).pathname;
const bytes = (key: string) => readFile(dir + key).catch(() => null);

/** A server for the fixture that answers GET, HEAD and Range, failing the first request */
const server = (): typeof fetch => {
  let first = true;
  return async (input, init) => {
    if (first) {
      first = false;
      return new Response("busy", { status: 503 });
    }
    const body = await bytes(String(input).replace("https://example.org/lake/", ""));
    if (!body) return new Response("missing", { status: 404 });
    if (init?.method === "HEAD")
      return new Response(null, { headers: { "content-length": String(body.length) } });
    const range = /bytes=(\d+)-(\d+)/.exec(new Headers(init?.headers).get("range") ?? "");
    return range
      ? new Response(body.subarray(Number(range[1]), Number(range[2]) + 1), { status: 206 })
      : new Response(body);
  };
};

/** An R2 binding over the fixture */
const bucket: R2Like = {
  get: async (key, options) => {
    const body = await bytes(key.replace(/^lake\//, ""));
    if (!body) return null;
    const { offset = 0, length = body.length } = options?.range ?? {};
    const part = body.subarray(offset, offset + length);
    return {
      text: async () => part.toString("utf8"),
      arrayBuffer: async () => new Uint8Array(part).buffer,
    };
  },
  head: async (key) => {
    const body = await bytes(key.replace(/^lake\//, ""));
    return body && { size: body.length };
  },
};

const readTown = async (store: ReturnType<typeof httpStore>) => {
  const file = tableFiles(await open(store), "geo.town")![0]!;
  expect(await store.size(file.key)).toBe(file.bytes);
  return readWhere(await openParquet(store, file), "lg_code", "000009", ["n"]);
};

describe("stores", () => {
  it("reads over HTTP", async () => {
    expect(
      await readTown(httpStore("https://example.org/lake/", { fetch: server() })),
    ).toHaveLength(1000);
  });

  it("reads from R2 under a prefix", async () => {
    expect(await readTown(r2Store(bucket, "lake/"))).toHaveLength(1000);
  });

  it("takes a Workers R2 binding", () => {
    // Type check only: R2Bucket must fit R2Like
    const binding = null as unknown as R2Bucket;
    expect(() => r2Store(binding)).not.toThrow();
  });
});
