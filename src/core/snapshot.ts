/**
 * Snapshot decoding from FlatBuffers format.
 *
 * Based on the icechunk FlatBuffers schema:
 * - Snapshot contains: id, parent_id, nodes, flushed_at, message, metadata, manifest_files
 * - NodeSnapshot contains: id, path, user_data, node_data (union of Array/Group)
 */

import * as flatbuffers from "flatbuffers";
import { crockfordBase32Encode } from "../encoding/crockford-base32.js";
import { decodeEnvelope } from "./decode.js";
import type {
  Snapshot,
  NodeSnapshot,
  NodeData,
  ManifestRef,
  ZarrArrayMetadata,
  SnapshotId,
} from "./types.js";

// FlatBuffers vtable field offsets for Snapshot table
// These are determined by the order of fields in the schema
const SNAPSHOT_ID_OFFSET = 4;
const SNAPSHOT_PARENT_ID_OFFSET = 6;
const SNAPSHOT_NODES_OFFSET = 8;
const SNAPSHOT_FLUSHED_AT_OFFSET = 10;
const SNAPSHOT_MESSAGE_OFFSET = 12;
const SNAPSHOT_METADATA_OFFSET = 14;
const SNAPSHOT_MANIFEST_FILES_OFFSET = 16;

// NodeSnapshot field offsets
const NODE_ID_OFFSET = 4;
const NODE_PATH_OFFSET = 6;
const NODE_USER_DATA_OFFSET = 8;
const NODE_DATA_TYPE_OFFSET = 10;
const NODE_DATA_OFFSET = 12;

// NodeData enum values
const NODE_DATA_NONE = 0;
const NODE_DATA_ARRAY = 1;
const NODE_DATA_GROUP = 2;

// ArrayNodeData field offsets
const ARRAY_SHAPE_OFFSET = 4;
const ARRAY_DIMENSION_NAMES_OFFSET = 6;
const ARRAY_MANIFESTS_OFFSET = 8;

// ManifestRef field offsets
const MANIFEST_REF_ID_OFFSET = 4;
const MANIFEST_REF_EXTENTS_OFFSET = 6;

// ManifestFileInfo is a struct (32 bytes)
const MANIFEST_FILE_INFO_SIZE = 32;

/**
 * Read an ObjectId12 (12-byte ID) and convert to base32 string.
 */
function readObjectId12(bb: flatbuffers.ByteBuffer, offset: number): string {
  const bytes = new Uint8Array(12);
  for (let i = 0; i < 12; i++) {
    bytes[i] = bb.readUint8(offset + i);
  }
  return crockfordBase32Encode(bytes);
}

/**
 * Read an ObjectId8 (8-byte node ID).
 */
function readObjectId8(bb: flatbuffers.ByteBuffer, offset: number): Uint8Array {
  const bytes = new Uint8Array(8);
  for (let i = 0; i < 8; i++) {
    bytes[i] = bb.readUint8(offset + i);
  }
  return bytes;
}

/**
 * Read a string at an offset from a table.
 */
function readString(bb: flatbuffers.ByteBuffer, tableOffset: number, fieldOffset: number): string {
  const o = bb.__offset(tableOffset, fieldOffset);
  if (o === 0) return "";
  return bb.__string(tableOffset + o) || "";
}

/**
 * Read a vector length at an offset from a table.
 */
function readVectorLength(bb: flatbuffers.ByteBuffer, tableOffset: number, fieldOffset: number): number {
  const o = bb.__offset(tableOffset, fieldOffset);
  if (o === 0) return 0;
  return bb.__vector_len(tableOffset + o);
}

/**
 * Get vector element offset.
 */
function getVectorElement(bb: flatbuffers.ByteBuffer, tableOffset: number, fieldOffset: number, index: number): number {
  const o = bb.__offset(tableOffset, fieldOffset);
  if (o === 0) return 0;
  return bb.__vector(tableOffset + o) + index * 4;
}

/**
 * Read a byte vector.
 */
function readByteVector(bb: flatbuffers.ByteBuffer, tableOffset: number, fieldOffset: number): Uint8Array {
  const o = bb.__offset(tableOffset, fieldOffset);
  if (o === 0) return new Uint8Array(0);

  const len = bb.__vector_len(tableOffset + o);
  const start = bb.__vector(tableOffset + o);
  return bb.bytes().slice(start, start + len);
}

/**
 * Parse ArrayNodeData from FlatBuffers.
 */
function parseArrayNodeData(bb: flatbuffers.ByteBuffer, offset: number): NodeData {
  // Read the table offset
  const tableOffset = bb.__indirect(offset);

  // Read shape (vector of DimensionShape structs)
  const shapeLen = readVectorLength(bb, tableOffset, ARRAY_SHAPE_OFFSET);
  const shape: number[] = [];
  const chunkShape: number[] = [];

  if (shapeLen > 0) {
    const shapeVecOffset = bb.__offset(tableOffset, ARRAY_SHAPE_OFFSET);
    const shapeVec = bb.__vector(tableOffset + shapeVecOffset);

    // DimensionShape is a struct with two u64 fields (array_length, chunk_length)
    for (let i = 0; i < shapeLen; i++) {
      const structOffset = shapeVec + i * 16; // Each struct is 16 bytes (2 x u64)
      const arrayLength = Number(bb.readUint64(structOffset));
      const chunkLength = Number(bb.readUint64(structOffset + 8));
      shape.push(arrayLength);
      chunkShape.push(chunkLength);
    }
  }

  // Read dimension names (optional)
  const dimNamesLen = readVectorLength(bb, tableOffset, ARRAY_DIMENSION_NAMES_OFFSET);
  const dimensionNames: string[] = [];
  if (dimNamesLen > 0) {
    for (let i = 0; i < dimNamesLen; i++) {
      const elemOffset = getVectorElement(bb, tableOffset, ARRAY_DIMENSION_NAMES_OFFSET, i);
      const nameTableOffset = bb.__indirect(elemOffset);
      // DimensionName has a single string field
      const name = readString(bb, nameTableOffset, 4);
      dimensionNames.push(name);
    }
  }

  // Read manifests (vector of ManifestRef)
  const manifestsLen = readVectorLength(bb, tableOffset, ARRAY_MANIFESTS_OFFSET);
  const manifests: ManifestRef[] = [];

  if (manifestsLen > 0) {
    for (let i = 0; i < manifestsLen; i++) {
      const elemOffset = getVectorElement(bb, tableOffset, ARRAY_MANIFESTS_OFFSET, i);
      const refTableOffset = bb.__indirect(elemOffset);

      // ManifestRef id is a struct (ObjectId12)
      const idOffset = bb.__offset(refTableOffset, MANIFEST_REF_ID_OFFSET);
      const id = idOffset ? readObjectId12(bb, refTableOffset + idOffset) : "";

      // Extents is a vector of ChunkIndexRange structs (each has start: u32, end: u32)
      const extentsLen = readVectorLength(bb, refTableOffset, MANIFEST_REF_EXTENTS_OFFSET);
      const extents: Array<[number, number]> = [];

      if (extentsLen > 0) {
        const extentsVecOffset = bb.__offset(refTableOffset, MANIFEST_REF_EXTENTS_OFFSET);
        const extentsVec = bb.__vector(refTableOffset + extentsVecOffset);

        for (let j = 0; j < extentsLen; j++) {
          const structOffset = extentsVec + j * 8; // Each ChunkIndexRange is 8 bytes
          const start = bb.readUint32(structOffset);
          const end = bb.readUint32(structOffset + 4);
          extents.push([start, end]);
        }
      }

      manifests.push({ id, extents });
    }
  }

  // We don't have full zarr metadata in the manifest - it's stored in user_data
  // Return a partial array node data
  return {
    type: "array",
    zarrMetadata: {
      shape,
      chunkShape,
      dataType: "", // Will be filled from user_data
      fillValue: null,
      codecs: [],
      dimensionNames: dimensionNames.length > 0 ? dimensionNames : undefined,
      chunkKeyEncoding: "slash",
    },
    manifests,
  };
}

/**
 * Parse a NodeSnapshot from FlatBuffers.
 */
function parseNodeSnapshot(bb: flatbuffers.ByteBuffer, offset: number): NodeSnapshot {
  const tableOffset = bb.__indirect(offset);

  // Read node ID (ObjectId8 struct)
  const idFieldOffset = bb.__offset(tableOffset, NODE_ID_OFFSET);
  const id = idFieldOffset ? readObjectId8(bb, tableOffset + idFieldOffset) : new Uint8Array(8);

  // Read path
  const path = readString(bb, tableOffset, NODE_PATH_OFFSET);

  // Read user_data (byte vector containing JSON)
  const userData = readByteVector(bb, tableOffset, NODE_USER_DATA_OFFSET);
  let userAttributes: Record<string, unknown> = {};

  if (userData.length > 0) {
    try {
      const text = new TextDecoder().decode(userData);
      userAttributes = JSON.parse(text);
    } catch {
      // Ignore JSON parse errors
    }
  }

  // Read node_data_type and node_data
  const nodeDataTypeOffset = bb.__offset(tableOffset, NODE_DATA_TYPE_OFFSET);
  const nodeDataType = nodeDataTypeOffset ? bb.readUint8(tableOffset + nodeDataTypeOffset) : NODE_DATA_NONE;

  let nodeData: NodeData;

  if (nodeDataType === NODE_DATA_ARRAY) {
    const nodeDataOffset = bb.__offset(tableOffset, NODE_DATA_OFFSET);
    if (nodeDataOffset) {
      nodeData = parseArrayNodeData(bb, tableOffset + nodeDataOffset);
      // Merge zarr metadata from user_data if available
      if (userAttributes.zarr_format === 3 || userAttributes.zarr_format === 2) {
        const arrayData = nodeData as { type: "array"; zarrMetadata: ZarrArrayMetadata };
        arrayData.zarrMetadata.dataType = (userAttributes.data_type as string) || "";
        arrayData.zarrMetadata.fillValue = userAttributes.fill_value;
        arrayData.zarrMetadata.codecs = (userAttributes.codecs as unknown[]) || [];
        if (userAttributes.chunk_key_encoding) {
          const encoding = userAttributes.chunk_key_encoding as { name?: string };
          arrayData.zarrMetadata.chunkKeyEncoding = encoding.name === "v2" ? "dot" : "slash";
        }
      }
    } else {
      nodeData = { type: "group" };
    }
  } else {
    nodeData = { type: "group" };
  }

  return {
    id,
    path: normalizePath(path),
    userAttributes,
    nodeData,
  };
}

/**
 * Normalize path: remove leading slash, ensure no trailing slash except for root.
 */
function normalizePath(path: string): string {
  if (path === "/" || path === "") return "";
  return path.replace(/^\//, "").replace(/\/$/, "");
}

/**
 * Decode a snapshot from binary data.
 */
export function decodeSnapshot(data: Uint8Array): Snapshot {
  const { buffer: bb } = decodeEnvelope(data);

  // Get root table offset (first 4 bytes, little-endian)
  const rootOffset = bb.readInt32(0);
  const tableOffset = rootOffset;

  // Read snapshot ID (ObjectId12 struct)
  const idFieldOffset = bb.__offset(tableOffset, SNAPSHOT_ID_OFFSET);
  const id = idFieldOffset ? readObjectId12(bb, tableOffset + idFieldOffset) : "";

  // Read parent ID (optional ObjectId12)
  const parentIdFieldOffset = bb.__offset(tableOffset, SNAPSHOT_PARENT_ID_OFFSET);
  const parentId = parentIdFieldOffset ? readObjectId12(bb, tableOffset + parentIdFieldOffset) : null;

  // Read flushed_at (u64 timestamp)
  const flushedAtOffset = bb.__offset(tableOffset, SNAPSHOT_FLUSHED_AT_OFFSET);
  const flushedAtValue = flushedAtOffset ? bb.readUint64(tableOffset + flushedAtOffset) : BigInt(0);
  const flushedAt = new Date(Number(flushedAtValue)).toISOString();

  // Read message
  const message = readString(bb, tableOffset, SNAPSHOT_MESSAGE_OFFSET);

  // Read metadata (vector of MetadataItem)
  const metadataLen = readVectorLength(bb, tableOffset, SNAPSHOT_METADATA_OFFSET);
  const metadata: Record<string, string> = {};
  // TODO: Parse metadata items

  // Read manifest_files (vector of ManifestFileInfo structs)
  const manifestFilesLen = readVectorLength(bb, tableOffset, SNAPSHOT_MANIFEST_FILES_OFFSET);
  const manifestFiles = new Map<string, { id: string }>();

  if (manifestFilesLen > 0) {
    const vecOffset = bb.__offset(tableOffset, SNAPSHOT_MANIFEST_FILES_OFFSET);
    const vec = bb.__vector(tableOffset + vecOffset);

    for (let i = 0; i < manifestFilesLen; i++) {
      const structOffset = vec + i * MANIFEST_FILE_INFO_SIZE;
      const manifestId = readObjectId12(bb, structOffset);
      manifestFiles.set(manifestId, { id: manifestId });
    }
  }

  // Read nodes (vector of NodeSnapshot)
  const nodesLen = readVectorLength(bb, tableOffset, SNAPSHOT_NODES_OFFSET);
  const nodes: NodeSnapshot[] = [];

  for (let i = 0; i < nodesLen; i++) {
    const elemOffset = getVectorElement(bb, tableOffset, SNAPSHOT_NODES_OFFSET, i);
    const node = parseNodeSnapshot(bb, elemOffset);
    nodes.push(node);
  }

  return {
    id,
    parentId,
    flushedAt,
    message,
    metadata,
    manifestFiles,
    attributeFiles: [],
    nodes,
  };
}

/**
 * Find a node by path in the snapshot (binary search on sorted nodes).
 */
export function findNode(
  snapshot: Snapshot,
  path: string
): NodeSnapshot | undefined {
  const normalizedPath = normalizePath(path);
  const nodes = snapshot.nodes;

  let low = 0;
  let high = nodes.length - 1;

  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const node = nodes[mid];
    const cmp = node.path.localeCompare(normalizedPath);

    if (cmp === 0) {
      return node;
    } else if (cmp < 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return undefined;
}

/**
 * Get the URL for a snapshot file.
 */
export function getSnapshotUrl(baseUrl: string, id: SnapshotId): string {
  const base = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
  return `${base}snapshots/${id}`;
}

/**
 * Encode a node as zarr.json (Zarr v3 format).
 */
export function encodeZarrJson(node: NodeSnapshot): string {
  if (node.nodeData.type === "group") {
    return JSON.stringify({
      zarr_format: 3,
      node_type: "group",
      attributes: node.userAttributes,
    });
  }

  // For arrays, use the user_data directly if it looks like zarr metadata
  if (node.userAttributes.zarr_format) {
    return JSON.stringify(node.userAttributes);
  }

  // Otherwise, construct from parsed metadata
  const { zarrMetadata } = node.nodeData;
  return JSON.stringify({
    zarr_format: 3,
    node_type: "array",
    shape: zarrMetadata.shape,
    data_type: zarrMetadata.dataType,
    chunk_grid: {
      name: "regular",
      configuration: {
        chunk_shape: zarrMetadata.chunkShape,
      },
    },
    chunk_key_encoding: {
      name: zarrMetadata.chunkKeyEncoding === "slash" ? "default" : "v2",
      configuration: {
        separator: zarrMetadata.chunkKeyEncoding === "slash" ? "/" : ".",
      },
    },
    fill_value: zarrMetadata.fillValue,
    codecs: zarrMetadata.codecs,
    dimension_names: zarrMetadata.dimensionNames,
    attributes: {},
  });
}
