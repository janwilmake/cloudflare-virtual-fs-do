import { Env, VirtualFileSystemBlockDO, VirtualFS } from "./blocks";

export { VirtualFileSystemBlockDO };

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

      await fs.writeFile("myapp/config/hello-world.txt", "I love DO 🧡");

      // Read directory contents
      const files = await fs.readdir("myapp/config");

      // Read file contents
      const settings = await fs.readFile("myapp/config/settings.json", "utf8");
      const hello = await fs.readFile("myapp/config/hello-world.txt", "utf8");

      await fs.writeFile(
        "image/desired-ui.png",
        await fetch(
          "https://raw.githubusercontent.com/janwilmake/cloudflare-virtual-fs-do/refs/heads/main/desired-ui.png",
        ).then((res) => res.arrayBuffer()),
      );

      return new Response(
        JSON.stringify(
          {
            files,
            settings,
            hello,
          },
          undefined,
          2,
        ),
        {
          headers: { "Content-Type": "application/json;charset=utf-8" },
        },
      );
    } catch (error: any) {
      return new Response(error.message, { status: 500 });
    }
  },
};
