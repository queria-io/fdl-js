// Read a version's Parquet files by range with hyparquet: the footer first (cached, since data
// files never change), then only the row groups whose min/max can hold the value asked for
import { decompress } from "fzstd";
import {
  type AsyncBuffer,
  type Compressors,
  type FileMetaData,
  parquetMetadata,
  parquetReadObjects,
} from "hyparquet";
import { hashParquetValue, readBloomFilter, sbbfContains } from "hyparquet/src/bloom.js";
import { type Column, type DataFile, FdlError } from "./repo.js";
import type { Store } from "./store.js";

/** DuckDB writes zstd by default. fzstd is plain JS, so it runs in Workers and browsers */
export const compressors: Compressors = { ZSTD: (input) => decompress(input) };

/** Called after every read, with the bytes read and the milliseconds spent waiting */
export type OnRead = (bytes: number, ms: number) => void;

/** A data file as hyparquet's AsyncBuffer */
export const asyncBuffer = (
  store: Store,
  key: string,
  byteLength: number,
  onRead?: OnRead,
): AsyncBuffer => ({
  byteLength,
  slice: async (start, end = byteLength) => {
    const t0 = Date.now();
    const body = await store.range(key, start, end - start);
    onRead?.(end - start, Date.now() - t0);
    return body;
  },
});

/** Where footers are kept between requests. Keys are data file keys; files never change */
export type FooterCache = {
  get: (key: string) => Promise<ArrayBuffer | undefined>;
  put: (key: string, footer: ArrayBuffer) => Promise<void>;
};

/** A FooterCache on a Cache API cache (caches.default in Workers, caches.open() in browsers) */
export const cacheApiFooterCache = (
  cache: Cache,
  base = "https://fdl.invalid/footer/",
): FooterCache => ({
  get: async (key) => (await cache.match(base + encodeURIComponent(key)))?.arrayBuffer(),
  put: (key, footer) =>
    cache.put(
      base + encodeURIComponent(key),
      new Response(footer, {
        headers: {
          "content-type": "application/octet-stream",
          "cache-control": "public, max-age=31536000, immutable",
        },
      }),
    ),
});

/** A FooterCache in memory, for one process or isolate */
export const memoryFooterCache = (): FooterCache => {
  const kept = new Map<string, ArrayBuffer>();
  return {
    get: async (key) => kept.get(key),
    put: async (key, footer) => void kept.set(key, footer),
  };
};

const MAGIC = 0x31524150; // "PAR1"

/** The footer (metadata and the last 8 bytes). One read when files.json gives footer_bytes */
const readFooter = async (file: AsyncBuffer, footerBytes?: number): Promise<ArrayBuffer> => {
  const size = file.byteLength;
  const guess = footerBytes !== undefined && footerBytes + 8 <= size ? footerBytes + 8 : 8;
  const tail = await file.slice(size - guess, size);
  const view = new DataView(tail);
  if (view.getUint32(guess - 4, true) !== MAGIC)
    throw new Error("not a Parquet file (no PAR1 at the end)");
  const length = view.getUint32(guess - 8, true) + 8;
  if (length > size) throw new Error("Parquet footer is longer than the file");
  return length <= guess ? tail.slice(guess - length) : file.slice(size - length, size);
};

/**
 * A table's column as one data file holds it: its name in the file (undefined when the file was
 * written before the column was added), the value rows of such a file hold, and the conversion
 * from the file's values to the column as it is now (renamed or added struct fields, integers
 * widened to int64)
 */
export type FileColumn = {
  name: string;
  physical: string | undefined;
  fill: () => unknown;
  convert: (value: unknown) => unknown;
};

export type ParquetFile = {
  key: string;
  file: AsyncBuffer;
  metadata: FileMetaData;
  /** The table's columns, in files.json's order, when the DataFile gave them */
  columns?: FileColumn[];
  /** Positions of the deleted rows, when the DataFile has a delete file */
  deleted?: Set<number>;
};

export type OpenOptions = { cache?: FooterCache; onRead?: OnRead };

/** An element of the file's schema with its children */
type Node = { name: string; id?: number; children: Node[] };

/** The file's schema as a tree. schema[0] is the root */
const schemaTree = (metadata: FileMetaData): Node => {
  const build = (i: number): [Node, number] => {
    const s = metadata.schema[i]!;
    const node: Node = {
      name: s.name,
      ...(s.field_id !== undefined ? { id: s.field_id } : {}),
      children: [],
    };
    let next = i + 1;
    for (let n = 0; n < (s.num_children ?? 0); n++) {
      const [child, after] = build(next);
      node.children.push(child);
      next = after;
    }
    return [node, next];
  };
  return build(0)[0];
};

/** A direct child for a column: by field_id, or by name when the file has no field_ids */
const childFor = (node: Node, column: Column): Node | undefined =>
  node.children.find((n) => n.id === column.id) ??
  node.children.find((n) => n.id === undefined && n.name === column.name);

/**
 * The node of a list's element or a map's value: below the repeated group that Parquet puts
 * between them and the column
 */
const nestedFor = (node: Node, column: Column): Node | undefined =>
  node.children.flatMap((g) => g.children).find((n) => n.id === column.id) ??
  node.children
    .flatMap((g) => g.children)
    .find((n) => n.id === undefined && n.name === column.name);

const INTEGER = new Set(["int8", "int16", "int32", "uint8", "uint16", "uint32"]);
const BIGINT = new Set(["int64", "uint64"]);
// hyparquet returns DOUBLE, which DuckDB writes for 128-bit integers, and decimals as numbers
const NUMBER = /^(float32|float64|int128|uint128|decimal\(.*\))$/;
const TIMESTAMP = /^timestamp(tz|_s|_ms|_ns)?$/;

/** DuckDB writes infinities and NaN as inf, -inf and nan, which Number() does not read */
const SPECIAL: Record<string, number> = { inf: Infinity, "-inf": -Infinity, nan: NaN };
const float = (text: string) => SPECIAL[text.toLowerCase()] ?? Number(text);

/**
 * A date or timestamp as DuckDB writes it ("2020-01-02 03:04:05.123456", "...+09",
 * "0044-03-15 (BC) 12:00:00", "10000-01-01"), as a Date. Like hyparquet, the microseconds since
 * the epoch are divided by 1000 toward zero, so times before 1970 land on the same millisecond
 */
const timestamp = (text: string): Date => {
  const m =
    /^(\d{4,})-(\d\d)-(\d\d)( \(BC\))?(?:[ T](\d\d):(\d\d):(\d\d)(\.\d+)?)?(?:([+-])(\d\d)(?::?(\d\d))?)?$/.exec(
      text,
    );
  if (!m) return new Date(text);
  const [, y, mo, d, bc, h = "0", mi = "0", s = "0", fraction = "", sign, oh = "0", om = "0"] = m;
  const base = new Date(0);
  // DuckDB writes year 0 as 0001 (BC)
  base.setUTCFullYear(bc ? 1 - Number(y) : Number(y), Number(mo) - 1, Number(d));
  base.setUTCHours(Number(h), Number(mi), Number(s), 0);
  const offset = (sign === "-" ? -1 : 1) * (Number(oh) * 60 + Number(om)) * 60_000;
  const micros =
    BigInt(base.getTime() - offset) * 1000n + BigInt(fraction.slice(1).padEnd(6, "0").slice(0, 6));
  return new Date(Number(micros / 1000n));
};

/**
 * A time of day ("03:04:05.123456", "03:04:05+09") in units of 1e-digits seconds, as hyparquet
 * returns TIME. A time with a time zone is moved to UTC, as DuckDB writes it
 */
const timeOfDay = (text: string, digits: number): bigint => {
  const m = /^(\d\d):(\d\d):(\d\d)(?:\.(\d+))?(?:([+-])(\d\d)(?::?(\d\d))?)?$/.exec(text);
  if (!m) throw new FdlError(`not a time: ${text}`);
  const [, h, mi, sec, fraction = "", sign, oh = "0", om = "0"] = m;
  const unit = 10n ** BigInt(digits);
  const day = 86_400n * unit;
  const offset = (sign === "-" ? -1n : 1n) * BigInt(Number(oh) * 3600 + Number(om) * 60) * unit;
  const value =
    BigInt((Number(h) * 60 + Number(mi)) * 60 + Number(sec)) * unit +
    BigInt(fraction.padEnd(digits, "0").slice(0, digits)) -
    (sign ? offset : 0n);
  return ((value % day) + day) % day;
};

/** A blob as DuckDB writes it (\\x01 for a byte), decoded as hyparquet decodes BYTE_ARRAY */
const blob = (text: string): string =>
  new TextDecoder().decode(
    new Uint8Array(
      text
        .split(/(\\x[0-9a-fA-F]{2})/)
        .flatMap((part) =>
          /^\\x[0-9a-fA-F]{2}$/.test(part)
            ? [parseInt(part.slice(2), 16)]
            : [...new TextEncoder().encode(part)],
        ),
    ),
  );

/**
 * The value a column's default text stands for, typed as hyparquet returns the column. This
 * covers every type DuckLake takes a default for; DuckLake refuses defaults of struct, list and
 * map columns, so another type means a writer this reader does not know
 */
const parseDefault = ({ name, type }: Column, text: string): unknown => {
  if (INTEGER.has(type)) return Number(text);
  if (BIGINT.has(type)) return BigInt(text);
  if (NUMBER.test(type)) return float(text);
  if (type === "boolean") return text === "true";
  if (type === "date" || TIMESTAMP.test(type)) return timestamp(text);
  if (type === "time" || type === "timetz") return timeOfDay(text, 6);
  if (type === "time_ns") return timeOfDay(text, 9);
  if (type === "blob") return blob(text);
  if (type === "json") return JSON.parse(text);
  // hyparquet returns UUIDs as lower-case text, as DuckDB writes them
  if (type === "varchar" || type === "uuid") return text;
  throw new FdlError(`cannot fill ${name}: a default of type ${type}`);
};

const defaultValue = (column: Column): (() => unknown) => {
  const text = column.default;
  return text === undefined ? () => null : () => parseDefault(column, text);
};

/**
 * Converts a file's value of a column to the column as it is now. Struct fields are found by
 * field_id, so renamed fields take their new names and fields added later their defaults
 */
const converter = (column: Column, node: Node): ((value: unknown) => unknown) => {
  const children = column.children ?? [];
  if (column.type === "struct") {
    const fields = children.map((c) => {
      const n = childFor(node, c);
      return {
        name: c.name,
        physical: n?.name,
        convert: n && converter(c, n),
        fill: defaultValue(c),
      };
    });
    return (value) =>
      value === null || typeof value !== "object"
        ? value
        : Object.fromEntries(
            fields.map((f) => [
              f.name,
              f.physical === undefined || !f.convert
                ? f.fill()
                : f.convert((value as Record<string, unknown>)[f.physical]),
            ]),
          );
  }
  if (column.type === "list") {
    const element = children[0] && nestedFor(node, children[0]);
    const convert = element && converter(children[0]!, element);
    return (value) => (convert && Array.isArray(value) ? value.map(convert) : value);
  }
  if (column.type === "map") {
    const valueColumn = children[1];
    const valueNode = valueColumn && nestedFor(node, valueColumn);
    const convert = valueNode && converter(valueColumn!, valueNode);
    return (value) =>
      convert && value !== null && typeof value === "object" && !Array.isArray(value)
        ? Object.fromEntries(Object.entries(value).map(([k, v]) => [k, convert(v)]))
        : value;
  }
  // A column widened from a narrower integer type keeps numbers in the files written before
  if (BIGINT.has(column.type)) return (v) => (typeof v === "number" ? BigInt(v) : v);
  return (v) => v;
};

/** Finds each column of the table in the file by field_id (by name when the file has none) */
const fileColumns = (metadata: FileMetaData, columns: Column[]): FileColumn[] => {
  const root = schemaTree(metadata);
  return columns.map((c) => {
    const node = childFor(root, c);
    return {
      name: c.name,
      physical: node?.name,
      fill: defaultValue(c),
      convert: node ? converter(c, node) : (v) => v,
    };
  });
};

const deletedPositions = async (
  store: Store,
  deletes: NonNullable<DataFile["deletes"]>,
  options: OpenOptions,
): Promise<Set<number>> => {
  const parquet = await openParquet(store, deletes, options);
  const rows = await readRows<{ pos: number | bigint }>(parquet, { columns: ["pos"] });
  return new Set(rows.map((r) => Number(r.pos)));
};

/**
 * Opens a data file: reads its footer, from the cache when it is there, and the positions in its
 * delete file when it has one
 */
export const openParquet = async (
  store: Store,
  { key, bytes, footerBytes, deletes, columns }: DataFile,
  options: OpenOptions = {},
): Promise<ParquetFile> => {
  const { cache, onRead } = options;
  const file = asyncBuffer(store, key, bytes, onRead);
  const footer = async () => {
    // The cache only saves reads: when it fails, the footer is read from the file
    const hit = await cache?.get(key).catch(() => undefined);
    if (hit) return hit;
    const read = await readFooter(file, footerBytes);
    await cache?.put(key, read).catch(() => {});
    return read;
  };
  const [metadata, deleted] = await Promise.all([
    footer().then(parquetMetadata),
    deletes ? deletedPositions(store, deletes, options) : undefined,
  ]);
  return {
    key,
    file,
    metadata,
    ...(columns ? { columns: fileColumns(metadata, columns) } : {}),
    ...(deleted ? { deleted } : {}),
  };
};

export type RowRange = { rowStart: number; rowEnd: number };
type Value = string | number | bigint;

/**
 * What readWhere matches a column against: a value (=), `{ in: [...] }` (IN), bounds such as
 * `{ gte: a, lte: b }` (>=, >, <=, <, BETWEEN) or `{ prefix: "xxx" }` (LIKE 'xxx%')
 */
export type Condition =
  | Value
  | { in: readonly Value[] }
  | { gt?: Value; gte?: Value; lt?: Value; lte?: Value }
  | { prefix: string };

/** A top-level leaf column: its index in a row group's columns and its schema element */
const leafColumn = (metadata: FileMetaData, column: string) => {
  let leaf = 0;
  let i = 1;
  while (i < metadata.schema.length) {
    const s = metadata.schema[i]!;
    if (!s.num_children) {
      if (s.name === column) return { index: leaf, element: s };
      leaf++;
      i++;
      continue;
    }
    if (s.name === column) throw new Error(`${column} is not a leaf column`);
    // Skip the group's subtree, counting its leaves
    let pending = s.num_children;
    i++;
    while (pending > 0) {
      const child = metadata.schema[i++]!.num_children ?? 0;
      if (child === 0) leaf++;
      pending += child - 1;
    }
  }
  throw new Error(`no column ${column}`);
};

// A UTF-16 code unit, shifted so that units compare in code point order: surrogates (U+10000
// and above) after U+E000-U+FFFF
const unit = (s: string, i: number) => {
  const u = s.charCodeAt(i);
  return u >= 0xe000 ? u - 0x800 : u >= 0xd800 ? u + 0x2000 : u;
};

// Strings compare in code point order, which is UTF-8 byte order, the order of Parquet's
// statistics. JavaScript's < compares UTF-16 code units, which differs past U+FFFF
const atMost = (a: Value, b: Value): boolean => {
  if (typeof a !== "string" || typeof b !== "string") return a <= b;
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const d = unit(a, i) - unit(b, i);
    if (d !== 0) return d < 0;
  }
  return a.length <= b.length;
};

const within = (value: Value, min: unknown, max: unknown) =>
  (min === undefined || min === null || atMost(min as Value, value)) &&
  (max === undefined || max === null || atMost(value, max as Value));

const known = (v: unknown): v is Value => v !== undefined && v !== null;
const below = (a: Value, b: Value) => !atMost(b, a);

/**
 * Whether values between min and max can match the condition. A bound the statistics lack
 * (undefined or null) can hold anything
 */
const mayHold = (condition: Condition, min: unknown, max: unknown): boolean => {
  if (typeof condition !== "object") return within(condition, min, max);
  if ("in" in condition) return condition.in.some((v) => within(v, min, max));
  if ("prefix" in condition) {
    // The strings with a prefix sort together, from the prefix itself on
    const p = condition.prefix;
    return (
      (!known(max) || atMost(p, max)) &&
      (!known(min) || atMost(min, p) || (typeof min === "string" && min.startsWith(p)))
    );
  }
  const { gt, gte, lt, lte } = condition;
  return (
    (gt === undefined || !known(max) || below(gt, max)) &&
    (gte === undefined || !known(max) || atMost(gte, max)) &&
    (lt === undefined || !known(min) || below(min, lt)) &&
    (lte === undefined || !known(min) || atMost(min, lte))
  );
};

/** Whether a row's value matches the condition. Null matches nothing */
const matches =
  (condition: Condition) =>
  (v: unknown): boolean => {
    if (!known(v)) return false;
    if (typeof condition !== "object") return same(v, condition);
    if ("in" in condition) return condition.in.some((c) => same(v, c));
    if ("prefix" in condition) return typeof v === "string" && v.startsWith(condition.prefix);
    return mayHold(condition, v, v);
  };

type Group = RowRange & { index: number };

/** The row groups whose min/max for column can match the condition. Groups without statistics are kept */
const groupsFor = (metadata: FileMetaData, column: string, condition: Condition): Group[] => {
  const { index } = leafColumn(metadata, column);
  const starts = metadata.row_groups.reduce<number[]>(
    (s, g) => [...s, s.at(-1)! + Number(g.num_rows)],
    [0],
  );
  return metadata.row_groups.flatMap((group, i) => {
    const stats = group.columns[index]?.meta_data?.statistics;
    return mayHold(condition, stats?.min_value ?? stats?.min, stats?.max_value ?? stats?.max)
      ? [{ index: i, rowStart: starts[i]!, rowEnd: starts[i + 1]! }]
      : [];
  });
};

/** Row ranges with adjacent ones merged into one */
const merged = (ranges: RowRange[]): RowRange[] =>
  ranges.reduce<RowRange[]>((out, { rowStart, rowEnd }) => {
    const last = out.at(-1);
    return last?.rowEnd === rowStart
      ? [...out.slice(0, -1), { rowStart: last.rowStart, rowEnd }]
      : [...out, { rowStart, rowEnd }];
  }, []);

/**
 * The rows of the row groups whose min/max for column can match the condition. Row groups
 * without statistics are kept. Adjacent row groups are merged into one range
 */
export const rowRangesFor = (
  metadata: FileMetaData,
  column: string,
  condition: Condition,
): RowRange[] => merged(groupsFor(metadata, column, condition));

/** The values an equality condition (= or IN) asks for */
const equalTo = (condition: Condition): readonly Value[] | undefined =>
  typeof condition !== "object" ? [condition] : "in" in condition ? condition.in : undefined;

/** Bloom filters closer together than this are read in one request */
const GAP = 64 * 1024;

type Span = { start: number; end: number };

/** Byte spans sorted and joined where the gap between them is at most GAP */
const coalesced = (spans: Span[]): Span[] =>
  [...spans]
    .sort((a, b) => a.start - b.start)
    .reduce<Span[]>((out, s) => {
      const last = out.at(-1);
      return last && s.start - last.end <= GAP
        ? [...out.slice(0, -1), { start: last.start, end: Math.max(last.end, s.end) }]
        : [...out, s];
    }, []);

/**
 * Leaves out the row groups whose bloom filter for column holds none of the values. A bloom
 * filter can only tell that a value is absent, so groups without one, and values it cannot hash
 * for the column's type, keep every group. DuckDB writes bloom filters for dictionary-encoded
 * columns, next to each other before the footer, so they are usually one read
 */
const bloomFiltered = async (
  { file, metadata }: ParquetFile,
  column: string,
  values: readonly Value[],
  groups: Group[],
): Promise<Group[]> => {
  const { index, element } = leafColumn(metadata, column);
  const hashes = values.map((v) => hashParquetValue(v, element));
  if (hashes.some((h) => h === undefined)) return groups;
  const spans = groups.flatMap((g) => {
    const meta = metadata.row_groups[g.index]!.columns[index]?.meta_data;
    const offset = meta?.bloom_filter_offset;
    const length = meta?.bloom_filter_length;
    return offset !== undefined && length
      ? [{ group: g.index, start: Number(offset), end: Number(offset) + length }]
      : [];
  });
  if (spans.length === 0) return groups;
  const reads = await Promise.all(
    coalesced(spans).map(async (r) => ({
      ...r,
      view: new DataView(await file.slice(r.start, r.end)),
    })),
  );
  const absent = new Set(
    spans.flatMap(({ group, start, end }) => {
      const read = reads.find((r) => r.start <= start && end <= r.end)!;
      const bloom = readBloomFilter({ view: read.view, offset: start - read.start });
      return bloom && hashes.every((h) => !sbbfContains(bloom.blocks, h!)) ? [group] : [];
    }),
  );
  return groups.filter((g) => !absent.has(g.index));
};

/**
 * Reads the rows given (all of them by default), the columns given (all by default), leaving out
 * deleted rows. With the table's columns, columns are found by field_id, so a renamed column is
 * read under its new name, a dropped one is left out, and one added after the file was written
 * holds its default (or null). Without them, columns the file lacks are left out
 */
export const readRows = async <T = Record<string, unknown>>(
  { file, metadata, columns: table, deleted }: ParquetFile,
  { columns, ...range }: Partial<RowRange> & { columns?: string[] } = {},
): Promise<T[]> => {
  const start = range.rowStart ?? 0;
  const keep = <R>(rows: R[]) => (deleted ? rows.filter((_, i) => !deleted.has(start + i)) : rows);
  if (!table) {
    const names = new Set(metadata.schema.slice(1).map((s) => s.name));
    const rows = await parquetReadObjects({
      file,
      metadata,
      compressors,
      ...range,
      // Columns the file lacks are left out, so a reader can ask for a column added later
      ...(columns ? { columns: columns.filter((c) => names.has(c)) } : {}),
    });
    return keep(rows) as T[];
  }
  // Columns the table lacks are left out, so a reader can ask for a column added later
  const wanted = columns ? columns.flatMap((name) => table.filter((t) => t.name === name)) : table;
  const physical = wanted.flatMap((c) => (c.physical === undefined ? [] : [c.physical]));
  const rows =
    physical.length > 0
      ? await parquetReadObjects({ file, metadata, compressors, ...range, columns: physical })
      : Array.from(
          {
            length: Math.max(
              0,
              Math.min(range.rowEnd ?? Infinity, Number(metadata.num_rows)) - start,
            ),
          },
          (): Record<string, unknown> => ({}),
        );
  return keep(rows).map((row) =>
    Object.fromEntries(
      wanted.map((c) => [c.name, c.physical === undefined ? c.fill() : c.convert(row[c.physical])]),
    ),
  ) as T[];
};

// INT64 columns come back as bigint; let 5 and 5n match. == compares a bigint with a number
// by value, and unlike BigInt() it does not throw for 1.5
const same = (a: unknown, b: Value) =>
  a === b ||
  ((typeof a === "bigint" || typeof b === "bigint") &&
    (typeof a === "bigint" || typeof a === "number") &&
    typeof b !== "string" &&
    // oxlint-disable-next-line eqeqeq
    a == b);

/**
 * The rows where column matches the condition: a value, `{ in: [...] }`, bounds such as
 * `{ gte: a, lt: b }`, or `{ prefix: "xxx" }`. Reads only the row groups whose min/max can match,
 * and for = and IN, when more than one row group is left, also leaves out those whose bloom
 * filter holds none of the values. Then drops the other rows. Strings compare in code point
 * order, as Parquet's statistics do. Works best on files sorted by column
 */
export const readWhere = async <T = Record<string, unknown>>(
  parquet: ParquetFile,
  column: string,
  condition: Condition,
  columns?: string[],
): Promise<T[]> => {
  const c = parquet.columns?.find((t) => t.name === column);
  if (parquet.columns && !c) throw new Error(`no column ${column}`);
  const match = matches(condition);
  const physical = c ? c.physical : column;
  // A column added after the file was written holds the same value in every row
  if (physical === undefined && !match(c!.fill())) return [];
  const ranges = await (async () => {
    if (physical === undefined) return [{ rowStart: 0, rowEnd: Number(parquet.metadata.num_rows) }];
    const groups = groupsFor(parquet.metadata, physical, condition);
    const values = equalTo(condition);
    return merged(
      values && groups.length > 1 ? await bloomFiltered(parquet, physical, values, groups) : groups,
    );
  })();
  const wanted = columns && [column, ...columns.filter((c) => c !== column)];
  const rows = await Promise.all(
    ranges.map((r) =>
      readRows<Record<string, unknown>>(parquet, { ...r, ...(wanted ? { columns: wanted } : {}) }),
    ),
  );
  return rows.flat().filter((row) => match(row[column])) as T[];
};
