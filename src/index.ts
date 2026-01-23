/**
 * icechunk-js - TypeScript read-only client for Icechunk stores.
 *
 * Compatible with zarrita.js for reading Zarr v3 data from Icechunk repositories.
 *
 * @packageDocumentation
 */

// Main store class
export { IcechunkStore } from "./store.js";
export type { IcechunkStoreOptions, AsyncReadable } from "./store.js";

// Core types
export type {
  Snapshot,
  Manifest,
  NodeSnapshot,
  NodeData,
  GroupNodeData,
  ArrayNodeData,
  ChunkPayload,
  InlineChunkPayload,
  VirtualChunkRef,
  NativeChunkRef,
  ManifestRef,
  ZarrArrayMetadata,
  SnapshotId,
  ManifestId,
  ChunkId,
  NodeId,
} from "./core/types.js";

// Utilities
export {
  crockfordBase32Encode,
  crockfordBase32Decode,
  isValidSnapshotId,
  isValidId,
} from "./encoding/crockford-base32.js";

export {
  translateUrl,
  translateGcsUrl,
  translateS3Url,
} from "./backend/http.js";

// Low-level APIs for advanced usage
export { decodeSnapshot, findNode, encodeZarrJson } from "./core/snapshot.js";
export { decodeManifest, findChunk, isChunkInExtent } from "./core/manifest.js";
export { parseRefJson, getBranchRefPath, getTagRefPath } from "./core/ref.js";
