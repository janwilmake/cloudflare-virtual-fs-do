import { DurableObject } from "cloudflare:workers";

export interface Env {
  VIRTUAL_FS: DurableObjectNamespace;
}

export class VirtualFileSystemDO extends DurableObject {
  private sql: SqlStorage;

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    // Initialize the SQLite database with our schema

    this.sql = state.storage.sql;

    // Create our files table
    // path: The full path to the file
    // type: 'file' or 'directory'
    // content: File contents (null for directories)
    // created_at: Creation timestamp
    // updated_at: Last modification timestamp
    this.sql.exec(`
        CREATE TABLE IF NOT EXISTS files (
          path TEXT PRIMARY KEY,
          type TEXT NOT NULL CHECK (type IN ('file', 'directory')),
          content BLOB,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        
        -- Index for faster directory listings
        CREATE INDEX IF NOT EXISTS idx_parent_path ON files(path);
      `);
  }

  // File System Operations
  async writeFile(path: string, content: string | ArrayBuffer): Promise<void> {
    const now = Date.now();

    // Ensure parent directory exists
    const parentDir = path.split("/").slice(0, -1).join("/");
    if (parentDir) {
      await this.mkdir(parentDir);
    }

    // Convert string content to Uint8Array if necessary
    const contentBuffer =
      typeof content === "string"
        ? new TextEncoder().encode(content)
        : new Uint8Array(content);

    await this.sql.exec(
      `INSERT INTO files (path, type, content, created_at, updated_at)
         VALUES (?, 'file', ?, ?, ?)
         ON CONFLICT (path) DO UPDATE SET
         content = excluded.content,
         updated_at = excluded.updated_at`,
      path,
      contentBuffer,
      now,
      now,
    );
  }

  readFile(path: string, encoding?: string): string | ArrayBuffer {
    const result = this.sql
      .exec("SELECT content FROM files WHERE path = ? AND type = 'file'", [
        path,
      ])
      .one();

    if (!result) {
      throw new Error(`ENOENT: no such file or directory: ${path}`);
    }

    const content = result.content as ArrayBuffer;
    return encoding === "utf8" ? new TextDecoder().decode(content) : content;
  }

  async unlink(path: string): Promise<void> {
    const result = this.sql.exec(
      "DELETE FROM files WHERE path = ? AND type = 'file'",
      path,
    );

    result.rowsWritten;

    if (result.rowsWritten === 0) {
      throw new Error(`ENOENT: no such file or directory: ${path}`);
    }
  }

  async mkdir(path: string): Promise<void> {
    const sql = this.sql;
    const now = Date.now();

    // Ensure parent directory exists
    const parentDir = path.split("/").slice(0, -1).join("/");
    if (parentDir) {
      await this.mkdir(parentDir);
    }

    // Create the directory (ignore if it already exists)
    await sql.exec(
      `INSERT OR IGNORE INTO files (path, type, created_at, updated_at)
         VALUES (?, 'directory', ?, ?)`,
      path,
      now,
      now,
    );
  }

  async rmdir(path: string): Promise<void> {
    // Check if directory is empty
    const hasChildren = this.sql
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
    const sql = this.sql;

    // First check if directory exists
    const dirExists = sql
      .exec("SELECT 1 FROM files WHERE path = ? AND type = 'directory'", [path])
      .one();

    if (!dirExists) {
      throw new Error(`ENOENT: no such file or directory: ${path}`);
    }

    // Get immediate children of the directory
    const children = sql
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
    const result = this.sql
      .exec("SELECT type, created_at, updated_at FROM files WHERE path = ?", [
        path,
      ])
      .one();

    if (!result) {
      throw new Error(`ENOENT: no such file or directory: ${path}`);
    }

    return {
      isFile: () => result.type === "file",
      isDirectory: () => result.type === "directory",
      created: new Date(result.created_at as string),
      modified: new Date(result.updated_at as string),
    };
  }
}

// Now let's create a friendly API wrapper that handles DO creation and routing
export class VirtualFS {
  constructor(private env: any) {}

  private async getDOStub(path: string) {
    // Use the first directory component as the DO name
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

// Types
interface FileStat {
  isFile(): boolean;
  isDirectory(): boolean;
  created: Date;
  modified: Date;
}
