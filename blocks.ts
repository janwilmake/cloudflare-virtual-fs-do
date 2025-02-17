/* eslint-disable @typescript-eslint/no-explicit-any */
/**
 * @file blocks.ts
 * @description Provides a Durable Object-based virtual file system implementation which
 *              stores file content in fixed-size blocks (default: 4KB). This design allows
 *              for managing larger files by splitting them into multiple blocks.
 *
 * @remarks
 * The refactored version includes comprehensive TSDoc comments, robust error handling, and modernized code structure.
 */

import { DurableObject } from "cloudflare:workers";

const BLOCK_SIZE = 4096; // 4KB block size constant

/**
 * Environment interface for binding the Durable Object namespace.
 */
export interface Env {
  VIRTUAL_FS: DurableObjectNamespace;
}

/**
 * Represents the status information of a file or directory.
 */
export interface FileStat {
  /**
   * Determines if the stat belongs to a file.
   * @returns True if it is a file.
   */
  isFile(): boolean;
  /**
   * Determines if the stat belongs to a directory.
   * @returns True if it is a directory.
   */
  isDirectory(): boolean;
  size?: number;
  created: Date;
  modified: Date;
}

/**
 * Durable Object for a block-based virtual filesystem.
 *
 * This class makes use of a SQLite database to store file metadata and file contents split into blocks.
 */
export class VirtualFileSystemBlockDO extends DurableObject {
  private sql: SqlStorage;
  private state: DurableObjectState;

  /**
   * Initializes the VirtualFileSystemBlockDO instance.
   * @param state - The DurableObjectState instance.
   * @param env - The environment object.
   */
  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    this.state = state;
    this.sql = state.storage.sql;

    // Initialize the database schema
    this.sql.exec(`
      -- Files table stores file and directory metadata.
      CREATE TABLE IF NOT EXISTS files (
        path TEXT PRIMARY KEY,
        type TEXT NOT NULL CHECK (type IN ('file', 'directory')),
        size INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );

      -- Blocks table stores file content in chunks.
      CREATE TABLE IF NOT EXISTS blocks (
        path TEXT NOT NULL,
        block_number INTEGER NOT NULL,
        content BLOB NOT NULL,
        PRIMARY KEY (path, block_number),
        FOREIGN KEY (path) REFERENCES files(path) ON DELETE CASCADE
      );

      -- Index for faster listing of directory entries.
      CREATE INDEX IF NOT EXISTS idx_parent_path ON files(path);
    `);
  }

  /**
   * Splits an ArrayBuffer into fixed-size blocks.
   * @param content - The content to split.
   * @returns An array of block objects with block number and data.
   */
  private splitIntoBlocks(content: ArrayBuffer): Array<{ number: number; data: Uint8Array }> {
    const blocks: Array<{ number: number; data: Uint8Array }> = [];
    const data = new Uint8Array(content);

    for (let i = 0; i < data.length; i += BLOCK_SIZE) {
      const blockData = data.slice(i, i + BLOCK_SIZE);
      blocks.push({
        number: Math.floor(i / BLOCK_SIZE),
        data: blockData,
      });
    }
    return blocks;
  }

  /**
   * Reassembles file content from its stored blocks.
   * @param path - The file path.
   * @returns The complete file content as an ArrayBuffer.
   * @throws Error if the file does not exist.
   */
  private async assembleBlocks(path: string): Promise<ArrayBuffer> {
    const fileInfo = await this.sql.exec(
      "SELECT size FROM files WHERE path = ? AND type = 'file'",
      [path]
    ).one();

    if (!fileInfo) {
      throw new Error(`ENOENT: File not found: ${path}`);
    }

    const blocks = await this.sql.exec(
      "SELECT block_number, content FROM blocks WHERE path = ? ORDER BY block_number",
      [path]
    ).toArray();

    const fileSize = fileInfo.size as number;
    const result = new Uint8Array(fileSize);
    blocks.forEach((block) => {
      const blockData = new Uint8Array(block.content as ArrayBuffer);
      const offset = (block.block_number as number) * BLOCK_SIZE;
      result.set(blockData, offset);
    });
    return result.buffer;
  }

  /**
   * Writes a file by splitting its content into blocks.
   * @param path - The file path.
   * @param content - The content as a string or ArrayBuffer.
   */
  async writeFile(path: string, content: string | ArrayBuffer): Promise<void> {
    const now = Date.now();
    // Ensure the parent directory exists.
    const parentDir = path.split("/").slice(0, -1).join("/");
    if (parentDir) {
      await this.mkdir(parentDir);
    }

    const contentBuffer = typeof content === "string"
      ? new TextEncoder().encode(content).buffer
      : content;

    await this.state.storage.transaction(async () => {
      // Upsert file metadata.
      await this.sql.exec(
        `INSERT INTO files (path, type, size, created_at, updated_at)
         VALUES (?, 'file', ?, ?, ?)
         ON CONFLICT (path) DO UPDATE SET
           size = excluded.size,
           updated_at = excluded.updated_at`,
        path,
        contentBuffer.byteLength,
        now,
        now
      );

      // Remove old blocks if any.
      await this.sql.exec("DELETE FROM blocks WHERE path = ?", path);

      // Insert new blocks.
      const blocks = this.splitIntoBlocks(contentBuffer);
      for (const block of blocks) {
        await this.sql.exec(
          "INSERT INTO blocks (path, block_number, content) VALUES (?, ?, ?)",
          path,
          block.number,
          block.data
        );
      }
    });
  }

  /**
   * Reads a file from the virtual filesystem.
   * @param path - The file path.
   * @param encoding - Optional encoding (e.g., "utf8").
   * @returns The file content as a string (if encoding is specified) or as an ArrayBuffer.
   */
  async readFile(path: string, encoding?: string): Promise<string | ArrayBuffer> {
    const content = await this.assembleBlocks(path);
    return encoding === "utf8" ? new TextDecoder().decode(content) : content;
  }

  /**
   * Deletes a file from the filesystem.
   * @param path - The file path.
   * @throws Error if the file does not exist.
   */
  async unlink(path: string): Promise<void> {
    const result = await this.sql.exec(
      "DELETE FROM files WHERE path = ? AND type = 'file'",
      path
    );
    if (result.rowsWritten === 0) {
      throw new Error(`ENOENT: File not found: ${path}`);
    }
  }

  /**
   * Creates a directory in the filesystem.
   * Recursively creates parent directories if needed.
   * @param path - The directory path.
   */
  async mkdir(path: string): Promise<void> {
    const now = Date.now();
    const parentDir = path.split("/").slice(0, -1).join("/");
    if (parentDir) {
      await this.mkdir(parentDir);
    }
    await this.sql.exec(
      `INSERT OR IGNORE INTO files (path, type, created_at, updated_at)
       VALUES (?, 'directory', ?, ?)`,
      path,
      now,
      now
    );
  }

  /**
   * Removes a directory from the filesystem.
   * @param path - The directory path.
   * @throws Error if the directory is not empty or does not exist.
   */
  async rmdir(path: string): Promise<void> {
    const hasChildren = await this.sql.exec(
      "SELECT 1 FROM files WHERE path LIKE ? || '/%' LIMIT 1",
      [path]
    ).one();

    if (hasChildren) {
      throw new Error(`ENOTEMPTY: Directory not empty: ${path}`);
    }

    const result = await this.sql.exec(
      "DELETE FROM files WHERE path = ? AND type = 'directory'",
      path
    );

    if (result.rowsWritten === 0) {
      throw new Error(`ENOENT: Directory not found: ${path}`);
    }
  }

  /**
   * Lists immediate children of a directory.
   * @param path - The directory path.
   * @returns An array of child file or directory names.
   * @throws Error if the directory does not exist.
   */
  async readdir(path: string): Promise<string[]> {
    const dirExists = await this.sql.exec(
      "SELECT 1 FROM files WHERE path = ? AND type = 'directory'",
      [path]
    ).one();

    if (!dirExists) {
      throw new Error(`ENOENT: Directory not found: ${path}`);
    }

    const children = await this.sql.exec(
      `SELECT path FROM files 
       WHERE path LIKE ? || '/%' AND path NOT LIKE ? || '/%/%'`,
      path,
      path
    ).toArray();

    return children.map((row) => {
      const parts = (row.path as string).split("/");
      return parts[parts.length - 1];
    });
  }

  /**
   * Retrieves file or directory statistics.
   * @param path - The filesystem path.
   * @returns A FileStat object containing metadata.
   * @throws Error if the file/directory does not exist.
   */
  async stat(path: string): Promise<FileStat> {
    const result = await this.sql.exec(
      "SELECT type, size, created_at, updated_at FROM files WHERE path = ?",
      [path]
    ).one();

    if (!result) {
      throw new Error(`ENOENT: Path not found: ${path}`);
    }

    return {
      isFile: () => result.type === "file",
      isDirectory: () => result.type === "directory",
      size: result.size as number,
      created: new Date(result.created_at as number),
      modified: new Date(result.updated_at as number),
    };
  }
}

/**
 * A client wrapper providing a friendly API for file system operations.
 *
 * It maps filesystem calls to the correct Durable Object based on the root directory.
 */
export class VirtualFS {
  /**
   * @param env - The environment containing the VIRTUAL_FS binding.
   */
  constructor(private env: any) {}

  /**
   * Retrieves the corresponding Durable Object stub for a given path.
   * @param path - The filesystem path.
   * @returns The Durable Object stub.
   * @throws Error if the path is invalid.
   */
  private async getDOStub(path: string): Promise<any> {
    const [rootDir] = path.split("/");
    if (!rootDir) {
      throw new Error("Invalid path: Must start with a root directory");
    }
    const id = this.env.VIRTUAL_FS.idFromName(rootDir);
    return this.env.VIRTUAL_FS.get(id);
  }

  /**
   * Writes a file.
   * @param path - The file path.
   * @param content - The file content.
   */
  async writeFile(path: string, content: string | ArrayBuffer): Promise<void> {
    const stub = await this.getDOStub(path);
    return stub.writeFile(path, content);
  }

  /**
   * Reads a file.
   * @param path - The file path.
   * @param encoding - Optional encoding.
   * @returns The file content.
   */
  async readFile(path: string, encoding?: string): Promise<string | ArrayBuffer> {
    const stub = await this.getDOStub(path);
    return stub.readFile(path, encoding);
  }

  /**
   * Deletes a file.
   * @param path - The file path.
   */
  async unlink(path: string): Promise<void> {
    const stub = await this.getDOStub(path);
    return stub.unlink(path);
  }

  /**
   * Creates a directory.
   * @param path - The directory path.
   */
  async mkdir(path: string): Promise<void> {
    const stub = await this.getDOStub(path);
    return stub.mkdir(path);
  }

  /**
   * Removes a directory.
   * @param path - The directory path.
   */
  async rmdir(path: string): Promise<void> {
    const stub = await this.getDOStub(path);
    return stub.rmdir(path);
  }

  /**
   * Lists contents of a directory.
   * @param path - The directory path.
   * @returns An array of child names.
   */
  async readdir(path: string): Promise<string[]> {
    const stub = await this.getDOStub(path);
    return stub.readdir(path);
  }

  /**
   * Retrieves file or directory statistics.
   * @param path - The filesystem path.
   * @returns A FileStat object.
   */
  async stat(path: string): Promise<FileStat> {
    const stub = await this.getDOStub(path);
    return stub.stat(path);
  }
}