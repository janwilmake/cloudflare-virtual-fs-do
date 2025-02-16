Inspired by [this post](https://x.com/samwillis/status/1891152072575173100) I wanted to make a DO-based FS that uses SQLite to emulate a virtual file system. As the sqlite is async, and we can use rpc, we should be able to make all regular fs calls very fast to a durable object. it'd be great if it becomes an API that looks super similar to fs. Potentially we can put the creation of the DO stub inside of the api, such that the DO name is actually the first folder name.

Running main and going to localhost:3000 shows the initial version seems to be working! You can use it easily by copying `fs.ts` into your worker!

Maybe I'll make a package out of this!
