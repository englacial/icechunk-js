import { describe, it, expect } from "vitest";
import {
  translateGcsUrl,
  translateS3Url,
  translateUrl,
} from "../src/backend/http.js";

describe("URL translation", () => {
  describe("translateGcsUrl", () => {
    it("should translate gs:// URLs to HTTPS", () => {
      expect(translateGcsUrl("gs://bucket/path/to/file")).toBe(
        "https://storage.googleapis.com/bucket/path/to/file"
      );
    });

    it("should pass through non-gs:// URLs", () => {
      expect(translateGcsUrl("https://example.com/file")).toBe(
        "https://example.com/file"
      );
    });
  });

  describe("translateS3Url", () => {
    it("should translate s3:// URLs to HTTPS", () => {
      expect(translateS3Url("s3://bucket/path/to/file")).toBe(
        "https://bucket.s3.us-east-1.amazonaws.com/path/to/file"
      );
    });

    it("should support custom regions", () => {
      expect(translateS3Url("s3://bucket/file", "eu-west-1")).toBe(
        "https://bucket.s3.eu-west-1.amazonaws.com/file"
      );
    });
  });

  describe("translateUrl", () => {
    it("should handle all URL types", () => {
      expect(translateUrl("gs://bucket/file")).toBe(
        "https://storage.googleapis.com/bucket/file"
      );
      expect(translateUrl("s3://bucket/file")).toContain("amazonaws.com");
      expect(translateUrl("https://example.com")).toBe("https://example.com");
    });
  });
});
