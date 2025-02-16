import { Env, VirtualFileSystemDO, VirtualFS } from "./fs";

export { VirtualFileSystemDO };

// Example Worker that uses the Virtual File System
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const fs = new VirtualFS(env);

    // Example usage:
    try {
      // Create a directory
      await fs.mkdir("myapp/config");

      // Write a file
      await fs.writeFile(
        "myapp/config/settings.json",
        JSON.stringify({
          theme: "dark",
          language: "en",
        }),
      );

      // Read directory contents
      const files = await fs.readdir("myapp/config");

      // Read file contents
      const content = await fs.readFile("myapp/config/settings.json", "utf8");

      return new Response(
        JSON.stringify({
          files,
          content,
        }),
        {
          headers: { "Content-Type": "application/json" },
        },
      );
    } catch (error: any) {
      return new Response(error.message, { status: 500 });
    }
  },
};
