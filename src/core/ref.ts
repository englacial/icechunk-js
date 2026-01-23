/**
 * Branch and tag reference parsing.
 *
 * Refs are JSON files at:
 * - refs/branch.{name}/ref.json
 * - refs/tag.{name}/ref.json
 *
 * Format: { "snapshot": "CROCKFORD_BASE32_ID" }
 */

import { isValidSnapshotId } from "../encoding/crockford-base32.js";

export interface Ref {
  snapshot: string;
}

/**
 * Parse a ref JSON object and extract the snapshot ID.
 */
export function decodeRef(obj: unknown): string {
  if (typeof obj !== "object" || obj === null) {
    throw new Error(`Expected ref object, got: ${typeof obj}`);
  }

  const keys = Object.keys(obj);
  if (keys.length !== 1 || keys[0] !== "snapshot") {
    throw new Error(
      `Expected object with only a "snapshot" property, got: ${JSON.stringify(obj)}`,
    );
  }

  const snapshot = (obj as Record<string, unknown>).snapshot;
  if (typeof snapshot !== "string") {
    throw new Error(
      `Expected snapshot to be a string, got: ${typeof snapshot}`,
    );
  }

  if (!isValidSnapshotId(snapshot)) {
    throw new Error(`Invalid snapshot ID format: ${snapshot}`);
  }

  return snapshot;
}

/**
 * Parse ref JSON text.
 */
export function parseRefJson(text: string): string {
  const obj = JSON.parse(text);
  return decodeRef(obj);
}

/**
 * Build the URL path for a branch ref.
 */
export function getBranchRefPath(branchName: string): string {
  return `refs/branch.${branchName}/ref.json`;
}

/**
 * Build the URL path for a tag ref.
 */
export function getTagRefPath(tagName: string): string {
  return `refs/tag.${tagName}/ref.json`;
}
