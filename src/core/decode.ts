/**
 * Envelope decoding for icechunk binary files.
 *
 * Icechunk files have a 39-byte header:
 * - 12 bytes: magic "ICE🧊CHUNK"
 * - 24 bytes: version string (e.g., "ic-0.3.16" padded with spaces)
 * - 1 byte: spec version
 * - 1 byte: file type
 * - 1 byte: compression method
 * - Then zstd-compressed FlatBuffers data
 */

import { decompress as zstdDecompress } from "fzstd";
import * as flatbuffers from "flatbuffers";

// Magic bytes for "ICE🧊CHUNK" - the ice cube emoji is 4 bytes in UTF-8
const MAGIC = new Uint8Array([
  0x49, 0x43, 0x45, // "ICE"
  0xf0, 0x9f, 0xa7, 0x8a, // 🧊 (ice cube emoji)
  0x43, 0x48, 0x55, 0x4e, 0x4b, // "CHUNK"
]);

const MAGIC_SIZE = 12;
const VERSION_STRING_SIZE = 24;
const HEADER_SIZE = MAGIC_SIZE + VERSION_STRING_SIZE + 3; // 39 bytes
const LATEST_SPEC_VERSION = 1;

// FlatBuffers file identifier for icechunk
const ICECHUNK_FILE_ID = "Ichk";

export enum FileType {
  Snapshot = 0,
  Manifest = 1,
  TransactionLog = 2,
  AttributeFile = 3,
}

export enum CompressionMethod {
  Uncompressed = 0,
  Zstd = 1,
}

export interface EnvelopeHeader {
  versionString: string;
  specVersion: number;
  fileType: FileType;
  compression: CompressionMethod;
}

/**
 * Parse the envelope header from icechunk binary data.
 */
export function parseEnvelopeHeader(data: Uint8Array): EnvelopeHeader {
  if (data.length < HEADER_SIZE) {
    throw new Error(
      `Data too short for icechunk header: ${data.length} < ${HEADER_SIZE}`
    );
  }

  // Verify magic bytes
  for (let i = 0; i < MAGIC.length; i++) {
    if (data[i] !== MAGIC[i]) {
      throw new Error("Invalid icechunk magic bytes");
    }
  }

  // Extract version string (24 bytes after magic, trim padding spaces)
  const versionBytes = data.slice(MAGIC_SIZE, MAGIC_SIZE + VERSION_STRING_SIZE);
  const versionString = new TextDecoder().decode(versionBytes).trim();

  // Read spec version, file type, and compression (1 byte each)
  const specVersion = data[MAGIC_SIZE + VERSION_STRING_SIZE];
  if (specVersion > LATEST_SPEC_VERSION) {
    throw new Error(
      `Unsupported icechunk spec version: ${specVersion} > ${LATEST_SPEC_VERSION}`
    );
  }

  const fileType = data[MAGIC_SIZE + VERSION_STRING_SIZE + 1] as FileType;
  const compression = data[MAGIC_SIZE + VERSION_STRING_SIZE + 2] as CompressionMethod;

  return {
    versionString,
    specVersion,
    fileType,
    compression,
  };
}

/**
 * Decompress data if needed (zstd compression).
 */
function decompress(
  data: Uint8Array,
  compression: CompressionMethod
): Uint8Array {
  switch (compression) {
    case CompressionMethod.Uncompressed:
      return data;
    case CompressionMethod.Zstd:
      return zstdDecompress(data);
    default:
      throw new Error(`Unknown compression method: ${compression}`);
  }
}

/**
 * Decode an icechunk binary file envelope and return the decompressed FlatBuffers data.
 */
export function decodeEnvelope(data: Uint8Array): {
  header: EnvelopeHeader;
  buffer: flatbuffers.ByteBuffer;
} {
  const header = parseEnvelopeHeader(data);
  const payload = data.slice(HEADER_SIZE);
  const decompressed = decompress(payload, header.compression);

  // Verify FlatBuffers file identifier
  const bb = new flatbuffers.ByteBuffer(decompressed);

  // Skip first 4 bytes (root offset), check next 4 bytes for file ID
  const fileIdBytes = decompressed.slice(4, 8);
  const fileId = new TextDecoder().decode(fileIdBytes);
  if (fileId !== ICECHUNK_FILE_ID) {
    throw new Error(`Invalid FlatBuffers file identifier: expected "${ICECHUNK_FILE_ID}", got "${fileId}"`);
  }

  return { header, buffer: bb };
}

/**
 * Read a string from a FlatBuffers buffer.
 */
export function readFbString(bb: flatbuffers.ByteBuffer, offset: number): string {
  const strOffset = bb.__offset(offset, bb.position());
  if (strOffset === 0) return "";
  return bb.__string(bb.position() + strOffset) || "";
}

/**
 * Read a byte vector from a FlatBuffers buffer.
 */
export function readFbByteVector(bb: flatbuffers.ByteBuffer, offset: number): Uint8Array {
  const vecOffset = bb.__offset(offset, bb.position());
  if (vecOffset === 0) return new Uint8Array(0);

  const pos = bb.position() + vecOffset;
  const length = bb.readInt32(pos);
  return bb.bytes().slice(pos + 4, pos + 4 + length);
}
