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
const ARRAY_MANIFEST_CHUNKS_OFFSET = 6;

// ChunkRef is a table with offset/length and payload type
const CHUNK_REF_OFFSET_FIELD = 4;
const CHUNK_REF_LENGTH_FIELD = 6;
const CHUNK_REF_CHECKSUM_FIELD = 8;
const CHUNK_REF_PAYLOAD_TYPE_FIELD = 10;
const CHUNK_REF_PAYLOAD_FIELD = 12;

// Payload types
const PAYLOAD_INLINE = 1;
const PAYLOAD_VIRTUAL = 2;
const PAYLOAD_REF = 3;

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
 * Read a vector length.
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
 * Parse a ChunkRef payload.
 */
function parseChunkPayload(
  bb: flatbuffers.ByteBuffer,
  tableOffset: number
): ChunkPayload | null {
  const payloadTypeOffset = bb.__offset(tableOffset, CHUNK_REF_PAYLOAD_TYPE_FIELD);
  if (payloadTypeOffset === 0) return null;

  const payloadType = bb.readUint8(tableOffset + payloadTypeOffset);
  const payloadOffset = bb.__offset(tableOffset, CHUNK_REF_PAYLOAD_FIELD);

  if (payloadOffset === 0) return null;

  const payloadTableOffset = bb.__indirect(tableOffset + payloadOffset);

  switch (payloadType) {
    case PAYLOAD_INLINE: {
      // InlinePayload: just data bytes
      const data = readByteVector(bb, payloadTableOffset, 4);
      return { type: "inline", data };
    }

    case PAYLOAD_VIRTUAL: {
      // VirtualPayload: location string, offset, length
      const location = readString(bb, payloadTableOffset, 4);
      const offsetFieldOffset = bb.__offset(payloadTableOffset, 6);
      const offset = offsetFieldOffset ? Number(bb.readUint64(payloadTableOffset + offsetFieldOffset)) : 0;
      const lengthFieldOffset = bb.__offset(payloadTableOffset, 8);
      const length = lengthFieldOffset ? Number(bb.readUint64(payloadTableOffset + lengthFieldOffset)) : 0;

      return { type: "virtual", location, offset, length };
    }

    case PAYLOAD_REF: {
      // RefPayload: chunk ID (ObjectId12)
      const idFieldOffset = bb.__offset(payloadTableOffset, 4);
      const id = idFieldOffset ? readObjectId12(bb, payloadTableOffset + idFieldOffset) : "";
      const offsetFieldOffset = bb.__offset(payloadTableOffset, 6);
      const offset = offsetFieldOffset ? Number(bb.readUint64(payloadTableOffset + offsetFieldOffset)) : 0;
      const lengthFieldOffset = bb.__offset(payloadTableOffset, 8);
      const length = lengthFieldOffset ? Number(bb.readUint64(payloadTableOffset + lengthFieldOffset)) : 0;

      return { type: "native", id, offset, length };
    }

    default:
      return null;
  }
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
  const id = idFieldOffset ? readObjectId12(bb, tableOffset + idFieldOffset) : "";

  // Read arrays (vector of ArrayManifest)
  const chunks = new Map<string, Map<string, ChunkPayload>>();
  const arraysLen = readVectorLength(bb, tableOffset, MANIFEST_ARRAYS_OFFSET);

  for (let i = 0; i < arraysLen; i++) {
    const elemOffset = getVectorElement(bb, tableOffset, MANIFEST_ARRAYS_OFFSET, i);
    const arrayTableOffset = bb.__indirect(elemOffset);

    // Read node ID (ObjectId8)
    const nodeIdFieldOffset = bb.__offset(arrayTableOffset, ARRAY_MANIFEST_NODE_ID_OFFSET);
    const nodeId = nodeIdFieldOffset
      ? readObjectId8(bb, arrayTableOffset + nodeIdFieldOffset)
      : new Uint8Array(8);
    const nodeIdHex = Buffer.from(nodeId).toString("hex");

    // Read chunks (vector of indexed ChunkRef)
    const chunksLen = readVectorLength(bb, arrayTableOffset, ARRAY_MANIFEST_CHUNKS_OFFSET);
    const chunkMap = new Map<string, ChunkPayload>();

    for (let j = 0; j < chunksLen; j++) {
      const chunkElemOffset = getVectorElement(bb, arrayTableOffset, ARRAY_MANIFEST_CHUNKS_OFFSET, j);
      const chunkTableOffset = bb.__indirect(chunkElemOffset);

      // IndexedChunkRef has: coords (vector of u32), chunk_ref
      // First, read the coords
      const coordsLen = readVectorLength(bb, chunkTableOffset, 4);
      const coords: number[] = [];

      if (coordsLen > 0) {
        const coordsVecOffset = bb.__offset(chunkTableOffset, 4);
        const coordsVec = bb.__vector(chunkTableOffset + coordsVecOffset);

        for (let k = 0; k < coordsLen; k++) {
          coords.push(bb.readUint32(coordsVec + k * 4));
        }
      }

      // Read the chunk_ref (offset, length, payload)
      const chunkRefOffset = bb.__offset(chunkTableOffset, 6);
      if (chunkRefOffset) {
        const refTableOffset = bb.__indirect(chunkTableOffset + chunkRefOffset);
        const payload = parseChunkPayload(bb, refTableOffset);

        if (payload) {
          const keyStr = coords.join("/");
          chunkMap.set(keyStr, payload);
        }
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
 * Convert a node ID (Uint8Array) to hex string for map lookup.
 */
export function nodeIdToHex(nodeId: Uint8Array): string {
  return Buffer.from(nodeId).toString("hex");
}

/**
 * Find a chunk in a manifest by node ID and chunk coordinates.
 */
export function findChunk(
  manifest: Manifest,
  nodeId: Uint8Array,
  chunkCoords: number[]
): ChunkPayload | undefined {
  const nodeIdHex = nodeIdToHex(nodeId);
  const nodeChunks = manifest.chunks.get(nodeIdHex);
  if (!nodeChunks) return undefined;

  const keyStr = chunkCoords.join("/");
  return nodeChunks.get(keyStr);
}

/**
 * Check if chunk coordinates fall within manifest extent boundaries.
 */
export function isChunkInExtent(
  chunkCoords: number[],
  extents: Array<[number, number]>
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
