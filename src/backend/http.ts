/**
 * HTTP backend for fetching icechunk data.
 *
 * Supports byte-range requests for efficient chunk loading.
 */

export interface FetchOptions {
  signal?: AbortSignal;
  headers?: Record<string, string>;
}

export interface ByteRangeOptions extends FetchOptions {
  offset: number;
  length: number;
}

/**
 * HTTP backend for fetching data from URLs.
 */
export class HttpBackend {
  private baseHeaders: Record<string, string>;

  constructor(headers: Record<string, string> = {}) {
    this.baseHeaders = headers;
  }

  /**
   * Fetch a complete resource.
   */
  async fetch(url: string, options: FetchOptions = {}): Promise<Uint8Array> {
    console.log(`[HttpBackend.fetch] Fetching URL: ${url}`);
    const response = await fetch(url, {
      method: "GET",
      headers: { ...this.baseHeaders, ...options.headers },
      signal: options.signal,
    });

    console.log(
      `[HttpBackend.fetch] Response: ${response.status} ${response.statusText} (${response.url})`,
    );

    if (!response.ok) {
      throw new Error(
        `HTTP ${response.status}: ${response.statusText} for ${url}`,
      );
    }

    const buffer = await response.arrayBuffer();
    console.log(`[HttpBackend.fetch] Received ${buffer.byteLength} bytes`);
    return new Uint8Array(buffer);
  }

  /**
   * Fetch a byte range from a resource.
   */
  async fetchRange(
    url: string,
    options: ByteRangeOptions,
  ): Promise<Uint8Array> {
    const { offset, length, signal, headers = {} } = options;
    const end = offset + length - 1;

    const response = await fetch(url, {
      method: "GET",
      headers: {
        ...this.baseHeaders,
        ...headers,
        Range: `bytes=${offset}-${end}`,
      },
      signal,
    });

    // Accept both 200 (full content) and 206 (partial content)
    if (!response.ok && response.status !== 206) {
      throw new Error(
        `HTTP ${response.status}: ${response.statusText} for ${url}`,
      );
    }

    const buffer = await response.arrayBuffer();
    return new Uint8Array(buffer);
  }

  /**
   * Fetch JSON data.
   */
  async fetchJson<T>(url: string, options: FetchOptions = {}): Promise<T> {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        ...this.baseHeaders,
        ...options.headers,
        Accept: "application/json",
      },
      signal: options.signal,
    });

    if (!response.ok) {
      throw new Error(
        `HTTP ${response.status}: ${response.statusText} for ${url}`,
      );
    }

    return response.json() as Promise<T>;
  }
}

/**
 * Translate a gs:// URL to an https:// URL for Google Cloud Storage.
 */
export function translateGcsUrl(url: string): string {
  if (url.startsWith("gs://")) {
    const path = url.slice(5); // Remove "gs://"
    return `https://storage.googleapis.com/${path}`;
  }
  return url;
}

/**
 * Translate a s3:// URL to an https:// URL for AWS S3.
 */
export function translateS3Url(url: string, region = "us-east-1"): string {
  if (url.startsWith("s3://")) {
    const path = url.slice(5); // Remove "s3://"
    const [bucket, ...keyParts] = path.split("/");
    const key = keyParts.join("/");
    return `https://${bucket}.s3.${region}.amazonaws.com/${key}`;
  }
  return url;
}

/**
 * Translate cloud storage URLs to HTTPS.
 */
export function translateUrl(url: string): string {
  if (url.startsWith("gs://")) {
    return translateGcsUrl(url);
  }
  if (url.startsWith("s3://")) {
    return translateS3Url(url);
  }
  return url;
}
