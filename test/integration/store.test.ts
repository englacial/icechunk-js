import { describe, it, expect, beforeAll } from "vitest";
import { IcechunkStore } from "../../src/store.js";

const TEST_URL =
  process.env.ICECHUNK_TEST_URL ||
  "https://storage.googleapis.com/ismip6-icechunk/12-07-2025/";

describe("IcechunkStore Integration", () => {
  let store: IcechunkStore;

  beforeAll(async () => {
    store = await IcechunkStore.open(TEST_URL, { ref: "main" });
  }, 30000); // Allow 30s for initial load

  it("should open the store successfully", () => {
    expect(store).toBeDefined();
    expect(store.getSnapshot()).not.toBeNull();
  });

  it("should have nodes in the snapshot", () => {
    const nodes = store.listNodes();
    expect(nodes.length).toBeGreaterThan(0);
  });

  it("should list top-level children", () => {
    const children = store.listChildren("");
    expect(children.length).toBeGreaterThan(0);
    // Should have model names like "VUB_AISMPALEO", "AWI_PISM1", etc.
    console.log("Top-level children:", children);
  });

  it("should read root zarr.json metadata", async () => {
    const data = await store.get("zarr.json");
    expect(data).toBeDefined();

    const text = new TextDecoder().decode(data!);
    const meta = JSON.parse(text);

    expect(meta.zarr_format).toBe(3);
    expect(meta.node_type).toBe("group");
  });

  it("should read nested group metadata", async () => {
    const children = store.listChildren("");
    if (children.length === 0) return;

    const firstModel = children[0];
    const modelChildren = store.listChildren(firstModel);

    if (modelChildren.length === 0) return;

    const firstExperiment = modelChildren[0];
    const path = `${firstModel}/${firstExperiment}/zarr.json`;

    const data = await store.get(path);
    expect(data).toBeDefined();

    const text = new TextDecoder().decode(data!);
    const meta = JSON.parse(text);

    expect(meta.zarr_format).toBe(3);
    console.log(`Metadata for ${firstModel}/${firstExperiment}:`, meta.node_type);
  });

  it("should resolve subpaths correctly", async () => {
    const children = store.listChildren("");
    if (children.length === 0) return;

    const firstModel = children[0];
    const resolved = store.resolve(firstModel);

    const data = await resolved.get("zarr.json");
    expect(data).toBeDefined();

    const text = new TextDecoder().decode(data!);
    const meta = JSON.parse(text);
    expect(meta.zarr_format).toBe(3);
  });
});
