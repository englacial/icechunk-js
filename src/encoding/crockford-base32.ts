/**
 * Crockford Base32 encoding/decoding for icechunk IDs.
 *
 * Icechunk uses 12-byte IDs encoded as 20-character Base32 strings.
 * The Crockford alphabet excludes I, L, O, U to reduce confusion.
 */

const ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

// Decoding map (case-insensitive, handles common substitutions)
const DECODE_MAP: Record<string, number> = {};
for (let i = 0; i < ALPHABET.length; i++) {
  DECODE_MAP[ALPHABET[i]] = i;
  DECODE_MAP[ALPHABET[i].toLowerCase()] = i;
}
// Common substitutions
DECODE_MAP["O"] = 0;
DECODE_MAP["o"] = 0;
DECODE_MAP["I"] = 1;
DECODE_MAP["i"] = 1;
DECODE_MAP["L"] = 1;
DECODE_MAP["l"] = 1;

/**
 * Encode bytes to Crockford Base32 string.
 * 12 bytes -> 20 characters
 */
export function crockfordBase32Encode(input: Uint8Array): string {
  const numBytes = input.length;
  let value = 0;
  let output = "";
  let bits = 0;

  for (let i = 0; i < numBytes; i++) {
    value = (value << 8) | input[i];
    for (bits += 8; bits >= 5; bits -= 5) {
      output += ALPHABET[(value >>> (bits - 5)) & 31];
    }
  }

  if (bits > 0) {
    output += ALPHABET[(value << (5 - bits)) & 31];
  }

  return output;
}

/**
 * Decode Crockford Base32 string to bytes.
 * 20 characters -> 12 bytes
 */
export function crockfordBase32Decode(input: string): Uint8Array {
  const numChars = input.length;
  const numBytes = Math.floor((numChars * 5) / 8);
  const output = new Uint8Array(numBytes);

  let value = 0;
  let bits = 0;
  let byteIndex = 0;

  for (let i = 0; i < numChars; i++) {
    const char = input[i];
    const decoded = DECODE_MAP[char];
    if (decoded === undefined) {
      throw new Error(`Invalid Crockford Base32 character: ${char}`);
    }
    value = (value << 5) | decoded;
    bits += 5;

    if (bits >= 8) {
      bits -= 8;
      output[byteIndex++] = (value >>> bits) & 0xff;
    }
  }

  return output;
}

/**
 * Validate a snapshot ID string (20 Crockford Base32 characters).
 */
export function isValidSnapshotId(id: string): boolean {
  return /^[0-9A-HJ-NP-TV-Z]{20}$/i.test(id);
}

/**
 * Validate a manifest/chunk ID string (20 Crockford Base32 characters).
 */
export function isValidId(id: string): boolean {
  return /^[0-9A-HJ-NP-TV-Z]{20}$/i.test(id);
}
