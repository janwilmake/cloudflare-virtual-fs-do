/**
 * @file main.ts
 * @description A Cloudflare Worker entry that demonstrates usage of the refactored
 *              block-based virtual file system.
 *
 * It creates directories, writes/reads files, and even serves an image if requested.
 */

import { Env, VirtualFileSystemBlockDO, VirtualFS } from "./blocks";

/**
 * Cloudflare Worker fetch event handler.
 */
export default {
  /**
   * Handles incoming requests and demonstrates the VirtualFS operations.
   * @param request - The incoming Request.
   * @param env - The environment containing Durable Object bindings.
   * @returns A Response after performing file system operations.
   */
  async fetch(request: Request, env: Env): Promise<Response> {
    const fs = new VirtualFS(env);

    try {
      // Create a directory structure
      await fs.mkdir("myapp/config");

      // Write JSON settings into a file
      const settingsContent = JSON.stringify(
        {
          theme: "dark",
          language: "en",
        },
        null,
        2
      );
      await fs.writeFile("myapp/config/settings.json", settingsContent);

      // Write a text file
      await fs.writeFile("myapp/config/hello-world.txt", "I love DO 🧡");

      // List directory contents
      const files = await fs.readdir("myapp/config");

      // Read file contents
      const settings = await fs.readFile("myapp/config/settings.json", "utf8");
      const hello = await fs.readFile("myapp/config/hello-world.txt", "utf8");

      // Endpoint to fetch and serve an image file stored virtually.
      if (new URL(request.url).pathname === "/image") {
        const imageUrl = "https://raw.githubusercontent.com/janwilmake/cloudflare-virtual-fs-do/main/desired-ui.png";
        const imageResponse = await fetch(imageUrl);
        const imageBuffer = await imageResponse.arrayBuffer();

        // Write image to virtual fs and then read it back.
        await fs.writeFile("image/desired-ui.png", imageBuffer);
        const imageBlob = await fs.readFile("image/desired-ui.png");

        return new Response(imageBlob, {
          headers: { "content-type": "image/png" },
        });
      }

      return new Response(
        JSON.stringify(
          {
            files,
            settings,
            hello,
          },
          null,
          2
        ),
        {
          headers: { "Content-Type": "application/json;charset=utf-8" },
        }
      );
    } catch (error: any) {
      return new Response(error.message, { status: 500 });
    }
  },
};