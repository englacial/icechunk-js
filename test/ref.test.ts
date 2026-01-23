import { describe, it, expect } from "vitest";
import {
  parseRefJson,
  getBranchRefPath,
  getTagRefPath,
} from "../src/core/ref.js";

describe("Ref parsing", () => {
  describe("parseRefJson", () => {
    it("should parse valid ref JSON", () => {
      const json = '{"snapshot":"1CECHNKREP0F1RSTCMT0"}';
      const snapshotId = parseRefJson(json);
      expect(snapshotId).toBe("1CECHNKREP0F1RSTCMT0");
    });

    it("should reject invalid JSON", () => {
      expect(() => parseRefJson("not json")).toThrow();
    });

    it("should reject missing snapshot property", () => {
      expect(() => parseRefJson('{"other":"value"}')).toThrow();
    });

    it("should reject extra properties", () => {
      expect(() =>
        parseRefJson('{"snapshot":"1CECHNKREP0F1RSTCMT0","extra":"value"}'),
      ).toThrow();
    });

    it("should reject invalid snapshot ID format", () => {
      expect(() => parseRefJson('{"snapshot":"invalid"}')).toThrow();
    });
  });

  describe("getBranchRefPath", () => {
    it("should construct correct path", () => {
      expect(getBranchRefPath("main")).toBe("refs/branch.main/ref.json");
      expect(getBranchRefPath("feature-x")).toBe(
        "refs/branch.feature-x/ref.json",
      );
    });
  });

  describe("getTagRefPath", () => {
    it("should construct correct path", () => {
      expect(getTagRefPath("v1.0.0")).toBe("refs/tag.v1.0.0/ref.json");
    });
  });
});
