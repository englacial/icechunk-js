/**
 * Manifest decoding from FlatBuffers format.
 *
 * Manifests contain chunk references organized by array node.
 */

import * as flatbuffers from "flatbuffers";
import { crockfordBase32Encode } from "../encoding/crockford-base32.js";
import { decodeEnvelope } from "./decode.js";
import type { Manifest, ChunkPayload, ManifestId } from "./types.js";

// Manifest field offsets
const MANIFEST_ID_OFFSET = 4;
const MANIFEST_ARRAYS_OFFSET = 6;

// ArrayManifest field offsets
const ARRAY_MANIFEST_NODE_ID_OFFSET = 4;
const ARRAY_MANIFEST_REFS_OFFSET = 6; // vector of ChunkRef tables

// ChunkRef field vtable offsets (field_index * 2 + 4)
// Field 0: index [u32]        -> vtable offset 4
// Field 1: inline [u8]?       -> vtable offset 6
// Field 2: offset u64         -> vtable offset 8
// Field 3: length u64         -> vtable offset 10
// Field 4: chunk_id ObjectId12? -> vtable offset 12
// Field 5: location string?   -> vtable offset 14
// Field 6: checksum_etag string? -> vtable offset 16
// Field 7: checksum_last_modified u32 -> vtable offset 18
const CHUNK_REF_INDEX_FIELD = 4;
const CHUNK_REF_INLINE_FIELD = 6;
const CHUNK_REF_OFFSET_FIELD = 8;
const CHUNK_REF_LENGTH_FIELD = 10;
const CHUNK_REF_CHUNK_ID_FIELD = 12;
const CHUNK_REF_LOCATION_FIELD = 14;

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
function readString(
  bb: flatbuffers.ByteBuffer,
  tableOffset: number,
  fieldOffset: number,
): string {
  const o = bb.__offset(tableOffset, fieldOffset);
  if (o === 0) return "";
  const result = bb.__string(tableOffset + o);
  if (result === null || result === undefined) return "";
  if (typeof result === "string") return result;
  return new TextDecoder().decode(result);
}

/**
 * Read a vector length.
 */
function readVectorLength(
  bb: flatbuffers.ByteBuffer,
  tableOffset: number,
  fieldOffset: number,
): number {
  const o = bb.__offset(tableOffset, fieldOffset);
  if (o === 0) return 0;
  return bb.__vector_len(tableOffset + o);
}

/**
 * Get vector element offset.
 */
function getVectorElement(
  bb: flatbuffers.ByteBuffer,
  tableOffset: number,
  fieldOffset: number,
  index: number,
): number {
  const o = bb.__offset(tableOffset, fieldOffset);
  if (o === 0) return 0;
  return bb.__vector(tableOffset + o) + index * 4;
}

/**
 * Read a byte vector.
 */
function readByteVector(
  bb: flatbuffers.ByteBuffer,
  tableOffset: number,
  fieldOffset: number,
): Uint8Array {
  const o = bb.__offset(tableOffset, fieldOffset);
  if (o === 0) return new Uint8Array(0);

  const len = bb.__vector_len(tableOffset + o);
  const start = bb.__vector(tableOffset + o);
  return bb.bytes().slice(start, start + len);
}

/**
 * Parse a ChunkRef table into a ChunkPayload.
 *
 * ChunkRef is a flat table with optional fields for three mutually-exclusive
 * storage modes:
 *   - inline:  `inline` byte vector is present
 *   - native:  `chunk_id` (ObjectId12) is present
 *   - virtual: `location` string is present
 *
 * `offset` and `length` are shared fields used by both native and virtual modes.
 */
function parseChunkRef(
  bb: flatbuffers.ByteBuffer,
  tableOffset: number,
): { coords: number[]; payload: ChunkPayload } | null {
  // Field 0: index (coords) - vector of u32
  const coordsLen = readVectorLength(bb, tableOffset, CHUNK_REF_INDEX_FIELD);
  const coords: number[] = [];
  if (coordsLen > 0) {
    const coordsVecOff = bb.__offset(tableOffset, CHUNK_REF_INDEX_FIELD);
    const coordsVec = bb.__vector(tableOffset + coordsVecOff);
    for (let k = 0; k < coordsLen; k++) {
      coords.push(bb.readUint32(coordsVec + k * 4));
    }
  }

  // Field 2: offset (u64)
  const offsetFieldOff = bb.__offset(tableOffset, CHUNK_REF_OFFSET_FIELD);
  const chunkOffset = offsetFieldOff
    ? Number(bb.readUint64(tableOffset + offsetFieldOff))
    : 0;

  // Field 3: length (u64)
  const lengthFieldOff = bb.__offset(tableOffset, CHUNK_REF_LENGTH_FIELD);
  const chunkLength = lengthFieldOff
    ? Number(bb.readUint64(tableOffset + lengthFieldOff))
    : 0;

  // Check field 1: inline data ([u8])
  const inlineData = readByteVector(bb, tableOffset, CHUNK_REF_INLINE_FIELD);
  if (inlineData.length > 0) {
    return {
      coords,
      payload: { type: "inline", data: inlineData },
    };
  }

  // Check field 5: location (string) -> virtual chunk
  const location = readString(bb, tableOffset, CHUNK_REF_LOCATION_FIELD);
  if (location) {
    return {
      coords,
      payload: {
        type: "virtual",
        location,
        offset: chunkOffset,
        length: chunkLength,
      },
    };
  }

  // Check field 4: chunk_id (ObjectId12) -> native chunk
  const chunkIdFieldOff = bb.__offset(tableOffset, CHUNK_REF_CHUNK_ID_FIELD);
  if (chunkIdFieldOff) {
    const id = readObjectId12(bb, tableOffset + chunkIdFieldOff);
    return {
      coords,
      payload: { type: "native", id, offset: chunkOffset, length: chunkLength },
    };
  }

  return null;
}

/**
 * Decode a manifest from binary data.
 */
export function decodeManifest(data: Uint8Array): Manifest {
  const { buffer: bb } = decodeEnvelope(data);

  // Get root table offset
  const rootOffset = bb.readInt32(0);
  const tableOffset = rootOffset;

  // Read manifest ID
  const idFieldOffset = bb.__offset(tableOffset, MANIFEST_ID_OFFSET);
  const id = idFieldOffset
    ? readObjectId12(bb, tableOffset + idFieldOffset)
    : "";

  // Read arrays (vector of ArrayManifest)
  const chunks = new Map<string, Map<string, ChunkPayload>>();
  const arraysLen = readVectorLength(bb, tableOffset, MANIFEST_ARRAYS_OFFSET);

  for (let i = 0; i < arraysLen; i++) {
    const elemOffset = getVectorElement(
      bb,
      tableOffset,
      MANIFEST_ARRAYS_OFFSET,
      i,
    );
    const arrayTableOffset = bb.__indirect(elemOffset);

    // Read node ID (ObjectId8)
    const nodeIdFieldOffset = bb.__offset(
      arrayTableOffset,
      ARRAY_MANIFEST_NODE_ID_OFFSET,
    );
    const nodeId = nodeIdFieldOffset
      ? readObjectId8(bb, arrayTableOffset + nodeIdFieldOffset)
      : new Uint8Array(8);
    const nodeIdHex = uint8ArrayToHex(nodeId);

    // Read refs - vector of ChunkRef tables
    const refsLen = readVectorLength(
      bb,
      arrayTableOffset,
      ARRAY_MANIFEST_REFS_OFFSET,
    );
    const chunkMap = new Map<string, ChunkPayload>();

    for (let j = 0; j < refsLen; j++) {
      const refElemOffset = getVectorElement(
        bb,
        arrayTableOffset,
        ARRAY_MANIFEST_REFS_OFFSET,
        j,
      );
      const refTableOffset = bb.__indirect(refElemOffset);

      const result = parseChunkRef(bb, refTableOffset);
      if (result) {
        const keyStr = result.coords.join("/");
        chunkMap.set(keyStr, result.payload);
      }
    }

    chunks.set(nodeIdHex, chunkMap);
  }

  return { id, chunks };
}

/**
 * Get the URL for a manifest file.
 */
export function getManifestUrl(baseUrl: string, id: ManifestId): string {
  const base = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
  return `${base}manifests/${id}`;
}

/**
 * Get the URL for a chunk file.
 */
export function getChunkUrl(baseUrl: string, id: string): string {
  const base = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
  return `${base}chunks/${id}`;
}

/**
 * Convert a Uint8Array to hex string (browser-compatible).
 */
function uint8ArrayToHex(arr: Uint8Array): string {
  return Array.from(arr)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Convert a node ID (Uint8Array) to hex string for map lookup.
 */
export function nodeIdToHex(nodeId: Uint8Array): string {
  return uint8ArrayToHex(nodeId);
}

/**
 * Find a chunk in a manifest by node ID and chunk coordinates.
 */
export function findChunk(
  manifest: Manifest,
  nodeId: Uint8Array,
  chunkCoords: number[],
): ChunkPayload | undefined {
  const nodeIdHex = nodeIdToHex(nodeId);
  const nodeChunks = manifest.chunks.get(nodeIdHex);

  if (!nodeChunks) {
    return undefined;
  }

  const keyStr = chunkCoords.join("/");
  return nodeChunks.get(keyStr);
}

/**
 * Check if chunk coordinates fall within manifest extent boundaries.
 */
export function isChunkInExtent(
  chunkCoords: number[],
  extents: Array<[number, number]>,
): boolean {
  if (chunkCoords.length !== extents.length) return false;

  for (let i = 0; i < chunkCoords.length; i++) {
    const [start, end] = extents[i];
    if (chunkCoords[i] < start || chunkCoords[i] > end) {
      return false;
    }
  }

  return true;
}
