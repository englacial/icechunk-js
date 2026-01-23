# icechunk-js

TypeScript read-only client for [Icechunk](https://icechunk.io/) stores, compatible with [zarrita.js](https://zarrita.dev/).

## Overview

Icechunk is a transactional storage engine for Zarr data, designed for cloud object storage. This library provides a browser-compatible TypeScript client for reading Icechunk repositories, implementing kylebarron's suggestion from [neuroglancer PR #718](https://github.com/google/neuroglancer/pull/718).

## Features

- **Read-only access** to Icechunk stores
- **zarrita.js compatible** - implements the `AsyncReadable` interface
- **Browser and Node.js** support
- **Virtual chunk references** - supports reading chunks from external files (NetCDF, HDF5)
- **Cloud storage** - translates `gs://` and `s3://` URLs to HTTPS
- **Caching** - LRU cache for manifests

## Installation

```bash
npm install icechunk-js
```

## Usage

### Basic Usage

```typescript
import { IcechunkStore } from 'icechunk-js';

// Open a store
const store = await IcechunkStore.open(
  'https://storage.googleapis.com/ismip6-icechunk/12-07-2025/',
  { ref: 'main' }
);

// Read metadata
const metadata = await store.get('zarr.json');
console.log(JSON.parse(new TextDecoder().decode(metadata!)));

// List children
const models = store.listChildren('');
console.log('Available models:', models);
```

### With zarrita.js

```typescript
import { IcechunkStore } from 'icechunk-js';
import * as zarr from 'zarrita';

const store = await IcechunkStore.open(
  'https://storage.googleapis.com/ismip6-icechunk/12-07-2025/',
  { ref: 'main' }
);

// Open an array
const arr = await zarr.open(
  store.resolve('VUB_AISMPALEO/ctrl_proj_std/base'),
  { kind: 'array' }
);

console.log('Shape:', arr.shape);
console.log('Dtype:', arr.dtype);

// Read data
const data = await zarr.get(arr);
```

## API

### `IcechunkStore.open(url, options?)`

Opens an Icechunk store.

**Parameters:**
- `url` - Base URL of the Icechunk repository
- `options.ref` - Branch name (default: `"main"`)
- `options.tag` - Tag name (alternative to `ref`)
- `options.snapshot` - Direct snapshot ID (alternative to `ref`/`tag`)
- `options.cache.manifests` - Max cached manifests (default: `100`)

**Returns:** `Promise<IcechunkStore>`

### `store.get(key)`

Read data for a zarr key.

**Parameters:**
- `key` - Zarr path like `"zarr.json"`, `"group/array/zarr.json"`, or `"array/c/0/1/2"`

**Returns:** `Promise<Uint8Array | undefined>`

### `store.resolve(path)`

Create a store scoped to a subpath.

**Parameters:**
- `path` - Path prefix

**Returns:** `IcechunkStore`

### `store.listChildren(path)`

List child paths under a given path.

**Returns:** `string[]`

### `store.listNodes()`

List all nodes in the snapshot.

**Returns:** `NodeSnapshot[]`

## Development

```bash
# Install dependencies
npm install

# Run tests
npm test

# Build
npm run build

# Format code
npm run format
```

## License

Apache-2.0

## Credits

Based on the Icechunk TypeScript implementation in [neuroglancer](https://github.com/google/neuroglancer) by Google Inc.
