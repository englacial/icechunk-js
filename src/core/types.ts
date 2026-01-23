/**
 * Core type definitions for icechunk data structures.
 */

/** 12-byte ID encoded as Crockford Base32 (20 characters) */
export type SnapshotId = string;
export type ManifestId = string;
export type ChunkId = string;

/** 8-byte node ID */
export type NodeId = Uint8Array;

/** Chunk payload types */
export interface InlineChunkPayload {
  type: "inline";
  data: Uint8Array;
}

export interface VirtualChunkRef {
  type: "virtual";
  location: string;
  offset: number;
  length: number;
}

export interface NativeChunkRef {
  type: "native";
  id: ChunkId;
  offset: number;
  length: number;
}

export type ChunkPayload =
  | InlineChunkPayload
  | VirtualChunkRef
  | NativeChunkRef;

/** Manifest reference with extent boundaries */
export interface ManifestRef {
  id: ManifestId;
  /** Extent boundaries: [start, end] for each dimension */
  extents: Array<[number, number]>;
}

/** Zarr array metadata */
export interface ZarrArrayMetadata {
  shape: number[];
  chunkShape: number[];
  dataType: string;
  fillValue: unknown;
  codecs: unknown[];
  dimensionNames?: string[];
  chunkKeyEncoding: "slash" | "dot";
  storageTransformers?: unknown[];
}

/** Node types */
export interface GroupNodeData {
  type: "group";
}

export interface ArrayNodeData {
  type: "array";
  zarrMetadata: ZarrArrayMetadata;
  manifests: ManifestRef[];
}

export type NodeData = GroupNodeData | ArrayNodeData;

/** Node snapshot in the hierarchy */
export interface NodeSnapshot {
  id: NodeId;
  path: string;
  userAttributes: Record<string, unknown>;
  nodeData: NodeData;
}

/** Complete snapshot structure */
export interface Snapshot {
  id: SnapshotId;
  parentId: SnapshotId | null;
  flushedAt: string;
  message: string;
  metadata: Record<string, string>;
  manifestFiles: Map<ManifestId, { id: ManifestId }>;
  attributeFiles: Array<{ id: string }>;
  nodes: NodeSnapshot[];
}

/** Manifest structure */
export interface Manifest {
  id: ManifestId;
  /** Map of nodeId (hex) -> chunkKey -> ChunkPayload */
  chunks: Map<string, Map<string, ChunkPayload>>;
}

/** Icechunk file types */
export type IcechunkFileType =
  | "snapshot"
  | "manifest"
  | "transactionLog"
  | "attributeFile";

/** Compression methods */
export type CompressionMethod = "uncompressed" | "zstd";
