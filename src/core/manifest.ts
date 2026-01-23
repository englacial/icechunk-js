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
const ARRAY_MANIFEST_CHUNKS_OFFSET = 6; // ChunkIndices - just coords
const ARRAY_MANIFEST_CHUNK_REFS_OFFSET = 8; // ChunkRef - actual references (parallel array)

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
  debug?: string,
): number {
  const o = bb.__offset(tableOffset, fieldOffset);
  if (debug) {
    console.log(
      `[readVectorLength:${debug}] tableOffset=${tableOffset}, fieldOffset=${fieldOffset}, o=${o}`,
    );
  }
  if (o === 0) return 0;
  const len = bb.__vector_len(tableOffset + o);
  if (debug) {
    console.log(`[readVectorLength:${debug}] len=${len}`);
  }
  return len;
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
 * Parse a ChunkRef payload.
 */
function parseChunkPayload(
  bb: flatbuffers.ByteBuffer,
  tableOffset: number,
): ChunkPayload | null {
  const payloadTypeOffset = bb.__offset(
    tableOffset,
    CHUNK_REF_PAYLOAD_TYPE_FIELD,
  );
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
      const offset = offsetFieldOffset
        ? Number(bb.readUint64(payloadTableOffset + offsetFieldOffset))
        : 0;
      const lengthFieldOffset = bb.__offset(payloadTableOffset, 8);
      const length = lengthFieldOffset
        ? Number(bb.readUint64(payloadTableOffset + lengthFieldOffset))
        : 0;

      return { type: "virtual", location, offset, length };
    }

    case PAYLOAD_REF: {
      // RefPayload: chunk ID (ObjectId12)
      const idFieldOffset = bb.__offset(payloadTableOffset, 4);
      const id = idFieldOffset
        ? readObjectId12(bb, payloadTableOffset + idFieldOffset)
        : "";
      const offsetFieldOffset = bb.__offset(payloadTableOffset, 6);
      const offset = offsetFieldOffset
        ? Number(bb.readUint64(payloadTableOffset + offsetFieldOffset))
        : 0;
      const lengthFieldOffset = bb.__offset(payloadTableOffset, 8);
      const length = lengthFieldOffset
        ? Number(bb.readUint64(payloadTableOffset + lengthFieldOffset))
        : 0;

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
  const id = idFieldOffset
    ? readObjectId12(bb, tableOffset + idFieldOffset)
    : "";

  // Read arrays (vector of ArrayManifest)
  const chunks = new Map<string, Map<string, ChunkPayload>>();
  const arraysLen = readVectorLength(
    bb,
    tableOffset,
    MANIFEST_ARRAYS_OFFSET,
    "manifest.arrays",
  );

  for (let i = 0; i < arraysLen; i++) {
    const elemOffset = getVectorElement(
      bb,
      tableOffset,
      MANIFEST_ARRAYS_OFFSET,
      i,
    );
    const arrayTableOffset = bb.__indirect(elemOffset);

    // Debug: dump vtable info for this ArrayManifest table
    const vtableOffset = arrayTableOffset - bb.readInt32(arrayTableOffset);
    const vtableSize = bb.readInt16(vtableOffset);
    const tableSize = bb.readInt16(vtableOffset + 2);
    console.log(
      `[decodeManifest] Array ${i}: elemOffset=${elemOffset}, arrayTableOffset=${arrayTableOffset}, vtableOffset=${vtableOffset}, vtableSize=${vtableSize}, tableSize=${tableSize}`,
    );

    // Read node ID (ObjectId8)
    const nodeIdFieldOffset = bb.__offset(
      arrayTableOffset,
      ARRAY_MANIFEST_NODE_ID_OFFSET,
    );
    const nodeId = nodeIdFieldOffset
      ? readObjectId8(bb, arrayTableOffset + nodeIdFieldOffset)
      : new Uint8Array(8);
    const nodeIdHex = uint8ArrayToHex(nodeId);

    // Read chunks - each entry contains coords AND the chunk reference
    const chunksLen = readVectorLength(
      bb,
      arrayTableOffset,
      ARRAY_MANIFEST_CHUNKS_OFFSET,
      `array[${i}].chunks`,
    );
    console.log(
      `[decodeManifest] Array ${i}: nodeId=${nodeIdHex}, chunksLen=${chunksLen}`,
    );
    const chunkMap = new Map<string, ChunkPayload>();

    for (let j = 0; j < chunksLen; j++) {
      // Read from chunks array - each entry is an IndexedChunk with coords AND payload
      const chunkElemOffset = getVectorElement(
        bb,
        arrayTableOffset,
        ARRAY_MANIFEST_CHUNKS_OFFSET,
        j,
      );
      const chunkTableOffset = bb.__indirect(chunkElemOffset);

      // Debug: dump vtable info for this chunk entry
      if (j < 3) {
        const chunkVtableOffset =
          chunkTableOffset - bb.readInt32(chunkTableOffset);
        const chunkVtableSize = bb.readInt16(chunkVtableOffset);
        const chunkTableSize = bb.readInt16(chunkVtableOffset + 2);
        // Check what fields are present
        const numFields = (chunkVtableSize - 4) / 2;
        const fieldOffsets: number[] = [];
        for (let f = 0; f < numFields; f++) {
          fieldOffsets.push(bb.readInt16(chunkVtableOffset + 4 + f * 2));
        }
        console.log(
          `[decodeManifest] Chunk ${j} vtable: size=${chunkVtableSize}, tableSize=${chunkTableSize}, numFields=${numFields}, fieldOffsets=[${fieldOffsets}]`,
        );
      }

      // Field 0: coords (vector of u32)
      const coordsLen = readVectorLength(bb, chunkTableOffset, 4);
      const coords: number[] = [];

      if (coordsLen > 0) {
        const coordsVecOffset = bb.__offset(chunkTableOffset, 4);
        const coordsVec = bb.__vector(chunkTableOffset + coordsVecOffset);

        for (let k = 0; k < coordsLen; k++) {
          coords.push(bb.readUint32(coordsVec + k * 4));
        }
      }

      // The chunk entry has fields embedded directly (not in nested table)
      // Based on vtable: fieldOffsets=[4,0,16,24,0,8,0,12]
      // Field 0 (offset 4): coords - working
      // Field 2 (offset 8): present at table+16 - likely offset (u64)
      // Field 3 (offset 10): present at table+24 - likely length (u64)
      // Field 5 (offset 14): present at table+8 - likely location string offset
      // Field 7 (offset 18): present at table+12 - maybe payload type or checksum

      // Read location string (field 5, vtable offset 14)
      const locationOffset = bb.__offset(chunkTableOffset, 14);
      let location = "";
      if (locationOffset) {
        const strResult = bb.__string(chunkTableOffset + locationOffset);
        if (strResult) {
          location =
            typeof strResult === "string"
              ? strResult
              : new TextDecoder().decode(strResult);
        }
      }

      // Read offset (field 2, vtable offset 8)
      const offsetFieldOffset = bb.__offset(chunkTableOffset, 8);
      const chunkOffset = offsetFieldOffset
        ? Number(bb.readUint64(chunkTableOffset + offsetFieldOffset))
        : 0;

      // Read length (field 3, vtable offset 10)
      const lengthFieldOffset = bb.__offset(chunkTableOffset, 10);
      const chunkLength = lengthFieldOffset
        ? Number(bb.readUint64(chunkTableOffset + lengthFieldOffset))
        : 0;

      if (j < 3) {
        console.log(
          `[decodeManifest] Chunk ${j}: coords=[${coords}], location="${location}", offset=${chunkOffset}, length=${chunkLength}`,
        );
      }

      // Create payload based on what we found
      let payload: ChunkPayload | null = null;
      if (location && chunkLength > 0) {
        // Virtual chunk reference
        payload = {
          type: "virtual",
          location,
          offset: chunkOffset,
          length: chunkLength,
        };
      } else if (chunkLength > 0) {
        // Might be inline or native - need more investigation
        if (j < 3) {
          console.log(
            `[decodeManifest] Chunk ${j}: No location but has length - checking for inline/native`,
          );
        }
      }

      if (payload) {
        const keyStr = coords.join("/");
        chunkMap.set(keyStr, payload);
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

  console.log(`[findChunk] Looking for nodeId=${nodeIdHex}, coords=${chunkCoords.join("/")}`);
  console.log(`[findChunk] Available nodes in manifest: ${Array.from(manifest.chunks.keys()).join(", ")}`);

  if (!nodeChunks) {
    console.log(`[findChunk] No chunks found for this node ID`);
    return undefined;
  }

  console.log(`[findChunk] Node has ${nodeChunks.size} chunks, keys: ${Array.from(nodeChunks.keys()).slice(0, 5).join(", ")}...`);

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
