import { describe, it, expect } from "vitest";
import {
  crockfordBase32Encode,
  crockfordBase32Decode,
  isValidSnapshotId,
} from "../src/encoding/crockford-base32.js";

describe("Crockford Base32", () => {
  describe("encode", () => {
    it("should encode 12 bytes to 20 characters", () => {
      const bytes = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
      const encoded = crockfordBase32Encode(bytes);
      expect(encoded).toHaveLength(20);
    });

    it("should produce valid base32 characters only", () => {
      const bytes = new Uint8Array(12).fill(0xff);
      const encoded = crockfordBase32Encode(bytes);
      expect(encoded).toMatch(/^[0-9A-HJ-NP-TV-Z]+$/);
    });

    it("should not contain I, L, O, U characters", () => {
      // Test with various byte patterns
      for (let i = 0; i < 256; i++) {
        const bytes = new Uint8Array(12).fill(i);
        const encoded = crockfordBase32Encode(bytes);
        expect(encoded).not.toMatch(/[ILOU]/);
      }
    });
  });

  describe("decode", () => {
    it("should decode 20 characters to 12 bytes", () => {
      const encoded = "00000000000000000000";
      const decoded = crockfordBase32Decode(encoded);
      expect(decoded).toHaveLength(12);
    });

    it("should roundtrip encode/decode", () => {
      const original = new Uint8Array([
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44,
      ]);
      const encoded = crockfordBase32Encode(original);
      const decoded = crockfordBase32Decode(encoded);
      expect(decoded).toEqual(original);
    });

    it("should be case-insensitive", () => {
      const upper = crockfordBase32Decode("ABCDEFGHJKMNPQRSTVWX");
      const lower = crockfordBase32Decode("abcdefghjkmnpqrstvwx");
      expect(upper).toEqual(lower);
    });

    it("should throw on invalid characters", () => {
      expect(() => crockfordBase32Decode("ILOU0000000000000000")).toThrow();
    });
  });

  describe("isValidSnapshotId", () => {
    it("should accept valid 20-character IDs", () => {
      expect(isValidSnapshotId("00000000000000000000")).toBe(true);
      expect(isValidSnapshotId("ZZZZZZZZZZZZZZZZZZZZ")).toBe(true);
      expect(isValidSnapshotId("1CECHNKREP0F1RSTCMT0")).toBe(true);
    });

    it("should reject invalid IDs", () => {
      expect(isValidSnapshotId("")).toBe(false);
      expect(isValidSnapshotId("short")).toBe(false);
      expect(isValidSnapshotId("0000000000000000000I")).toBe(false); // Contains I
      expect(isValidSnapshotId("000000000000000000000")).toBe(false); // Too long
    });
  });
});
