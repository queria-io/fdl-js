// A repository in a local folder, for Node and other runtimes with node:fs
import { open, readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import { NotFoundError, type Store } from "./store";

const missing = (e: unknown) => (e as { code?: string }).code === "ENOENT";

/** A repository in the folder dir */
export const fileStore = (dir: string): Store => ({
  text: (key) =>
    readFile(join(dir, key), "utf8").catch((e) => (missing(e) ? null : Promise.reject(e))),
  size: (key) =>
    stat(join(dir, key)).then(
      (s) => s.size,
      (e) => (missing(e) ? null : Promise.reject(e)),
    ),
  range: async (key, offset, length) => {
    const file = await open(join(dir, key)).catch((e) =>
      Promise.reject(missing(e) ? new NotFoundError(key) : e),
    );
    try {
      const buffer = new Uint8Array(length);
      const { bytesRead } = await file.read(buffer, 0, length, offset);
      return buffer.buffer.slice(0, bytesRead);
    } finally {
      await file.close();
    }
  },
});
