/**
 * IcechunkStore - zarrita.js compatible store for reading icechunk repositories.
 *
 * Implements the AsyncReadable interface required by zarrita.js.
 */

import { HttpBackend, translateUrl } from "./backend/http.js";
import { LRUCache } from "./cache/lru.js";
import { getBranchRefPath, getTagRefPath, parseRefJson } from "./core/ref.js";
import {
  decodeSnapshot,
  findNode,
  getSnapshotUrl,
  encodeZarrJson,
} from "./core/snapshot.js";
import {
  decodeManifest,
  getManifestUrl,
  getChunkUrl,
  findChunk,
  nodeIdToHex,
  isChunkInExtent,
} from "./core/manifest.js";
import type {
  Snapshot,
  Manifest,
  ChunkPayload,
  ManifestRef,
  NodeSnapshot,
} from "./core/types.js";

/**
 * Options for opening an IcechunkStore.
 */
export interface IcechunkStoreOptions {
  /** Branch name to read from (default: "main") */
  ref?: string;
  /** Tag name to read from */
  tag?: string;
  /** Direct snapshot ID to read from */
  snapshot?: string;
  /** Cache configuration */
  cache?: {
    /** Maximum number of manifests to cache (default: 100) */
    manifests?: number;
  };
  /** Abort signal for cancellation */
  signal?: AbortSignal;
}

/**
 * AsyncReadable interface from zarrita.js.
 */
export interface AsyncReadable {
  get(key: string): Promise<Uint8Array | undefined>;
}

/**
 * Parsed zarr key structure.
 */
interface ParsedKey {
  type: "metadata" | "chunk";
  path: string;
  chunkCoords?: number[];
}

/**
 * IcechunkStore - read-only store for icechunk repositories.
 *
 * @example
 * ```typescript
 * import { IcechunkStore } from 'icechunk-js';
 * import * as zarr from 'zarrita';
 *
 * const store = await IcechunkStore.open(
 *   'https://storage.googleapis.com/ismip6-icechunk/12-07-2025/',
 *   { ref: 'main' }
 * );
 *
 * const arr = await zarr.open(store.resolve('VUB_AISMPALEO/ctrl_proj_std/base'), { kind: 'array' });
 * ```
 */
export class IcechunkStore implements AsyncReadable {
  private readonly rootUrl: string;
  private readonly backend: HttpBackend;
  private readonly manifestCache: LRUCache<string, Manifest>;
  private snapshot: Snapshot | null = null;
  private basePath: string = "";

  private constructor(rootUrl: string, options: IcechunkStoreOptions = {}) {
    this.rootUrl = rootUrl.endsWith("/") ? rootUrl : rootUrl + "/";
    this.backend = new HttpBackend();
    this.manifestCache = new LRUCache(options.cache?.manifests ?? 100);
  }

  /**
   * Open an icechunk store.
   */
  static async open(
    rootUrl: string,
    options: IcechunkStoreOptions = {}
  ): Promise<IcechunkStore> {
    const store = new IcechunkStore(rootUrl, options);
    await store.initialize(options);
    return store;
  }

  private async initialize(options: IcechunkStoreOptions): Promise<void> {
    const snapshotId = await this.resolveSnapshotId(options);
    const snapshotUrl = getSnapshotUrl(this.rootUrl, snapshotId);
    const snapshotData = await this.backend.fetch(snapshotUrl, {
      signal: options.signal,
    });
    this.snapshot = decodeSnapshot(snapshotData);
  }

  private async resolveSnapshotId(
    options: IcechunkStoreOptions
  ): Promise<string> {
    if (options.snapshot) {
      return options.snapshot;
    }

    const refPath = options.tag
      ? getTagRefPath(options.tag)
      : getBranchRefPath(options.ref ?? "main");

    const refUrl = this.rootUrl + refPath;
    const refText = await this.backend
      .fetch(refUrl, { signal: options.signal })
      .then((data) => new TextDecoder().decode(data));

    return parseRefJson(refText);
  }

  /**
   * Get data for a zarr key.
   * Keys are paths like "zarr.json", "group/zarr.json", "group/array/c/0/1/2"
   */
  async get(key: string): Promise<Uint8Array | undefined> {
    if (!this.snapshot) {
      throw new Error("Store not initialized");
    }

    const fullKey = this.basePath ? `${this.basePath}/${key}` : key;
    const parsed = this.parseKey(fullKey);

    if (parsed.type === "metadata") {
      return this.getMetadata(parsed.path);
    }

    if (parsed.type === "chunk" && parsed.chunkCoords) {
      return this.getChunk(parsed.path, parsed.chunkCoords);
    }

    return undefined;
  }

  /**
   * Create a store scoped to a subpath.
   */
  resolve(path: string): IcechunkStore {
    const resolved = new IcechunkStore(this.rootUrl);
    resolved.snapshot = this.snapshot;
    resolved.basePath = this.basePath
      ? `${this.basePath}/${path}`.replace(/\/+/g, "/").replace(/^\/|\/$/g, "")
      : path.replace(/^\/|\/$/g, "");
    // Share the manifest cache
    (resolved as any).manifestCache = this.manifestCache;
    (resolved as any).backend = this.backend;
    return resolved;
  }

  private parseKey(key: string): ParsedKey {
    // "zarr.json" -> metadata for root
    // "group/zarr.json" -> metadata for group
    // "array/c/0/1/2" -> chunk at [0, 1, 2]

    if (key === "zarr.json" || key.endsWith("/zarr.json")) {
      const path = key === "zarr.json" ? "" : key.slice(0, -10);
      return { type: "metadata", path: path.replace(/\/$/, "") };
    }

    // Look for chunk pattern: /c/ followed by numbers
    const chunkMatch = key.match(/^(.*)\/c\/(.+)$/);
    if (chunkMatch) {
      const path = chunkMatch[1];
      const coordsStr = chunkMatch[2];
      const chunkCoords = coordsStr.split("/").map(Number);

      if (chunkCoords.some(isNaN)) {
        throw new Error(`Invalid chunk coordinates: ${coordsStr}`);
      }

      return { type: "chunk", path, chunkCoords };
    }

    // Default to metadata
    return { type: "metadata", path: key };
  }

  private async getMetadata(path: string): Promise<Uint8Array | undefined> {
    const node = findNode(this.snapshot!, path);
    if (!node) {
      return undefined;
    }

    const json = encodeZarrJson(node);
    return new TextEncoder().encode(json);
  }

  private async getChunk(
    arrayPath: string,
    coords: number[]
  ): Promise<Uint8Array | undefined> {
    const node = findNode(this.snapshot!, arrayPath);
    if (!node || node.nodeData.type !== "array") {
      return undefined;
    }

    const { manifests } = node.nodeData;

    // Find the manifest containing this chunk
    const manifestRef = this.findManifestForChunk(manifests, coords);
    if (!manifestRef) {
      return undefined;
    }

    // Fetch manifest (cached)
    const manifest = await this.fetchManifest(manifestRef.id);

    // Find chunk in manifest
    const chunkPayload = findChunk(manifest, node.id, coords);
    if (!chunkPayload) {
      return undefined;
    }

    // Fetch chunk data
    return this.fetchChunkData(chunkPayload);
  }

  private findManifestForChunk(
    manifests: ManifestRef[],
    coords: number[]
  ): ManifestRef | undefined {
    for (const ref of manifests) {
      if (isChunkInExtent(coords, ref.extents)) {
        return ref;
      }
    }
    return undefined;
  }

  private async fetchManifest(id: string): Promise<Manifest> {
    // Check cache
    const cached = this.manifestCache.get(id);
    if (cached) {
      return cached;
    }

    // Fetch and decode
    const url = getManifestUrl(this.rootUrl, id);
    const data = await this.backend.fetch(url);
    const manifest = decodeManifest(data);

    // Cache and return
    this.manifestCache.set(id, manifest);
    return manifest;
  }

  private async fetchChunkData(payload: ChunkPayload): Promise<Uint8Array> {
    switch (payload.type) {
      case "inline":
        return payload.data;

      case "native": {
        const url = getChunkUrl(this.rootUrl, payload.id);
        return this.backend.fetchRange(url, {
          offset: payload.offset,
          length: payload.length,
        });
      }

      case "virtual": {
        // Translate cloud URLs to HTTPS
        const url = translateUrl(payload.location);
        return this.backend.fetchRange(url, {
          offset: payload.offset,
          length: payload.length,
        });
      }

      default:
        throw new Error(`Unknown chunk payload type: ${(payload as any).type}`);
    }
  }

  /**
   * Get the snapshot metadata.
   */
  getSnapshot(): Snapshot | null {
    return this.snapshot;
  }

  /**
   * List all nodes in the snapshot.
   */
  listNodes(): NodeSnapshot[] {
    return this.snapshot?.nodes ?? [];
  }

  /**
   * List child paths under a given path.
   */
  listChildren(path: string): string[] {
    if (!this.snapshot) return [];

    const normalizedPath = path.replace(/^\/|\/$/g, "");
    const prefix = normalizedPath ? normalizedPath + "/" : "";

    const children = new Set<string>();
    for (const node of this.snapshot.nodes) {
      if (node.path.startsWith(prefix)) {
        const rest = node.path.slice(prefix.length);
        const nextPart = rest.split("/")[0];
        if (nextPart) {
          children.add(nextPart);
        }
      }
    }

    return Array.from(children);
  }
}
