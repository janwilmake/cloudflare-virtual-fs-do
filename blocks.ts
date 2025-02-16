import { DurableObject } from "cloudflare:workers";

const BLOCK_SIZE = 4096; // 4KB blocks

export interface Env {
  VIRTUAL_FS: DurableObjectNamespace;
}

export class VirtualFileSystemBlockDO extends DurableObject {
  private sql: SqlStorage;
  private state: DurableObjectState;

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    this.state = state;
    this.sql = state.storage.sql;

    // Create our tables
    this.sql.exec(`
      -- Files table stores metadata
      CREATE TABLE IF NOT EXISTS files (
        path TEXT PRIMARY KEY,
        type TEXT NOT NULL CHECK (type IN ('file', 'directory')),
        size INTEGER,  -- Total file size in bytes
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      
      -- Blocks table stores actual file content in chunks
      CREATE TABLE IF NOT EXISTS blocks (
        path TEXT NOT NULL,
        block_number INTEGER NOT NULL,
        content BLOB NOT NULL,
        PRIMARY KEY (path, block_number),
        FOREIGN KEY (path) REFERENCES files(path) ON DELETE CASCADE
      );
      
      -- Index for faster directory listings
      CREATE INDEX IF NOT EXISTS idx_parent_path ON files(path);
    `);
  }

  // Helper to split content into blocks
  private splitIntoBlocks(
    content: ArrayBuffer,
  ): Array<{ number: number; data: Uint8Array }> {
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

  // Helper to reassemble blocks into complete content
  private async assembleBlocks(path: string): Promise<ArrayBuffer> {
    const fileInfo = await this.sql
      .exec("SELECT size FROM files WHERE path = ? AND type = 'file'", [path])
      .one();

    if (!fileInfo) {
      throw new Error(`ENOENT: no such file or directory: ${path}`);
    }

    const blocks = await this.sql
      .exec(
        "SELECT block_number, content FROM blocks WHERE path = ? ORDER BY block_number",
        [path],
      )
      .toArray();

    // Create a buffer of the file's total size
    const result = new Uint8Array(fileInfo.size as number);

    // Copy each block into the correct position
    blocks.forEach((block) => {
      const blockData = new Uint8Array(block.content as ArrayBuffer);
      const offset = (block.block_number as number) * BLOCK_SIZE;
      result.set(blockData, offset);
    });

    return result.buffer;
  }

  async writeFile(path: string, content: string | ArrayBuffer): Promise<void> {
    const now = Date.now();

    // Ensure parent directory exists
    const parentDir = path.split("/").slice(0, -1).join("/");
    if (parentDir) {
      await this.mkdir(parentDir);
    }

    // Convert string content to ArrayBuffer if necessary
    const contentBuffer =
      typeof content === "string"
        ? (new TextEncoder().encode(content).buffer as ArrayBuffer)
        : content;

    // Use the proper transaction API
    await this.state.storage.transaction(async () => {
      // Insert or update file metadata
      await this.sql.exec(
        `INSERT INTO files (path, type, size, created_at, updated_at)
         VALUES (?, 'file', ?, ?, ?)
         ON CONFLICT (path) DO UPDATE SET
         size = excluded.size,
         updated_at = excluded.updated_at`,
        path,
        contentBuffer.byteLength,
        now,
        now,
      );

      // Delete existing blocks for this file
      await this.sql.exec("DELETE FROM blocks WHERE path = ?", path);

      // Split content into blocks and insert them
      const blocks = this.splitIntoBlocks(contentBuffer);
      for (const block of blocks) {
        await this.sql.exec(
          "INSERT INTO blocks (path, block_number, content) VALUES (?, ?, ?)",
          path,
          block.number,
          block.data,
        );
      }
    });
  }

  async readFile(
    path: string,
    encoding?: string,
  ): Promise<string | ArrayBuffer> {
    const content = await this.assembleBlocks(path);
    return encoding === "utf8" ? new TextDecoder().decode(content) : content;
  }

  async unlink(path: string): Promise<void> {
    // The blocks will be automatically deleted due to CASCADE
    const result = await this.sql.exec(
      "DELETE FROM files WHERE path = ? AND type = 'file'",
      path,
    );

    if (result.rowsWritten === 0) {
      throw new Error(`ENOENT: no such file or directory: ${path}`);
    }
  }

  // Directory operations remain largely the same
  async mkdir(path: string): Promise<void> {
    const now = Date.now();

    // Ensure parent directory exists
    const parentDir = path.split("/").slice(0, -1).join("/");
    if (parentDir) {
      await this.mkdir(parentDir);
    }

    await this.sql.exec(
      `INSERT OR IGNORE INTO files (path, type, size, created_at, updated_at)
       VALUES (?, 'directory', NULL, ?, ?)`,
      path,
      now,
      now,
    );
  }

  async rmdir(path: string): Promise<void> {
    // Check if directory is empty
    const hasChildren = await this.sql
      .exec("SELECT 1 FROM files WHERE path LIKE ? || '/%' LIMIT 1", [path])
      .one();

    if (hasChildren) {
      throw new Error(`ENOTEMPTY: directory not empty: ${path}`);
    }

    const result = await this.sql.exec(
      "DELETE FROM files WHERE path = ? AND type = 'directory'",
      path,
    );

    if (result.rowsWritten === 0) {
      throw new Error(`ENOENT: no such file or directory: ${path}`);
    }
  }

  async readdir(path: string): Promise<string[]> {
    // First check if directory exists
    const dirExists = await this.sql
      .exec("SELECT 1 FROM files WHERE path = ? AND type = 'directory'", [path])
      .one();

    if (!dirExists) {
      throw new Error(`ENOENT: no such file or directory: ${path}`);
    }

    // Get immediate children of the directory
    const children = await this.sql
      .exec(
        `SELECT path FROM files 
         WHERE path LIKE ? || '/%' 
         AND path NOT LIKE ? || '/%/%'`,
        path,
        path,
      )
      .toArray();

    return children.map((row) => {
      const parts = (row.path as string).split("/");
      return parts[parts.length - 1];
    });
  }

  async stat(path: string): Promise<FileStat> {
    const result = await this.sql
      .exec(
        "SELECT type, size, created_at, updated_at FROM files WHERE path = ?",
        [path],
      )
      .one();

    if (!result) {
      throw new Error(`ENOENT: no such file or directory: ${path}`);
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

// The VirtualFS wrapper class remains the same
export class VirtualFS {
  constructor(private env: any) {}

  private async getDOStub(path: string) {
    const rootDir = path.split("/")[0];
    if (!rootDir) {
      throw new Error("Invalid path: must start with a root directory");
    }

    const id = this.env.VIRTUAL_FS.idFromName(rootDir);
    return this.env.VIRTUAL_FS.get(id);
  }

  async writeFile(path: string, content: string | ArrayBuffer): Promise<void> {
    const stub = await this.getDOStub(path);
    return stub.writeFile(path, content);
  }

  async readFile(
    path: string,
    encoding?: string,
  ): Promise<string | ArrayBuffer> {
    const stub = await this.getDOStub(path);
    return stub.readFile(path, encoding);
  }

  async unlink(path: string): Promise<void> {
    const stub = await this.getDOStub(path);
    return stub.unlink(path);
  }

  async mkdir(path: string): Promise<void> {
    const stub = await this.getDOStub(path);
    return stub.mkdir(path);
  }

  async rmdir(path: string): Promise<void> {
    const stub = await this.getDOStub(path);
    return stub.rmdir(path);
  }

  async readdir(path: string): Promise<string[]> {
    const stub = await this.getDOStub(path);
    return stub.readdir(path);
  }

  async stat(path: string): Promise<FileStat> {
    const stub = await this.getDOStub(path);
    return stub.stat(path);
  }
}

interface FileStat {
  isFile(): boolean;
  isDirectory(): boolean;
  size?: number;
  created: Date;
  modified: Date;
}
