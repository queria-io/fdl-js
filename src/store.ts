// Where a repository is read from. Keys are the layout's paths relative to the root
// (fdl.json, refs/heads/main, versions/<ID>/version.json, data/...)

/** A read-only view of a repository's root */
export type Store = {
  /** The object as text, or null when it is missing */
  text: (key: string) => Promise<string | null>;
  /** The object's size in bytes, or null when it is missing */
  size: (key: string) => Promise<number | null>;
  /** Bytes [offset, offset + length) of the object. Throws when it is missing */
  range: (key: string, offset: number, length: number) => Promise<ArrayBuffer>;
};

export class NotFoundError extends Error {
  constructor(readonly key: string) {
    super(`not found: ${key}`);
  }
}

/** Calls f again once when it fails. Object stores fail now and then for a moment */
export const retryOnce = <T>(f: () => Promise<T>): Promise<T> => f().catch(() => f());

/** The part of an R2 binding (or anything shaped like it) that a reader needs */
export type R2Like = {
  get: (
    key: string,
    options?: { range?: { offset: number; length: number } },
  ) => Promise<{ text: () => Promise<string>; arrayBuffer: () => Promise<ArrayBuffer> } | null>;
  head: (key: string) => Promise<{ size: number } | null>;
};

/** A repository in an R2 bucket (a Workers binding), under prefix (ending in "/", or "") */
export const r2Store = (bucket: R2Like, prefix = ""): Store => ({
  text: async (key) => {
    const o = await retryOnce(() => bucket.get(prefix + key));
    return o ? o.text() : null;
  },
  size: async (key) => (await retryOnce(() => bucket.head(prefix + key)))?.size ?? null,
  range: async (key, offset, length) => {
    const o = await retryOnce(() => bucket.get(prefix + key, { range: { offset, length } }));
    if (!o) throw new NotFoundError(prefix + key);
    return o.arrayBuffer();
  },
});

export type HttpOptions = {
  fetch?: typeof fetch;
  /** Sent with every request, for example an Authorization header */
  headers?: Record<string, string>;
};

/** A repository served over HTTP at root (ending in "/") */
export const httpStore = (
  root: string,
  { fetch: f = fetch, headers = {} }: HttpOptions = {},
): Store => {
  const request = (key: string, init: RequestInit = {}) =>
    retryOnce(async () => {
      const res = await f(root + key, { ...init, headers: { ...headers, ...init.headers } });
      if (res.status >= 500) throw new Error(`${res.status} ${root + key}`);
      return res;
    });
  const ok = (res: Response, key: string) => {
    if (res.status === 404) return false;
    if (!res.ok) throw new Error(`${res.status} ${root + key}`);
    return true;
  };
  return {
    text: async (key) => {
      const res = await request(key, { cache: "no-store" });
      return ok(res, key) ? res.text() : null;
    },
    size: async (key) => {
      const res = await request(key, { method: "HEAD" });
      return ok(res, key) ? Number(res.headers.get("content-length")) : null;
    },
    range: async (key, offset, length) => {
      const res = await request(key, {
        headers: { range: `bytes=${offset}-${offset + length - 1}` },
      });
      if (!ok(res, key)) throw new NotFoundError(root + key);
      const body = await res.arrayBuffer();
      // A server that ignores Range sends the whole object
      return res.status === 206 ? body : body.slice(offset, offset + length);
    },
  };
};
