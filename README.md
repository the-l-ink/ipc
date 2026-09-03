# The Link IPC

Local inter-process communication adapters for The Link.

## Install

```sh
npm install @the-link/ipc
```

## Process

`ProcessLink` adapts an existing Node.js process IPC channel. The channel may
come from `fork()` or from `spawn()` with an `ipc` standard-I/O entry.

```ts
import { spawn } from "node:child_process"
import { ProcessLink } from "@the-link/ipc/process"

const child = spawn(process.execPath, ["worker.js"], {
    serialization: "advanced",
    stdio: ["inherit", "inherit", "inherit", "ipc"]
})

const link = new ProcessLink(child)
```

Inside the child, the current process is the default target:

```ts
import { ProcessLink } from "@the-link/ipc/process"

const link = new ProcessLink()
```

`ProcessLink` uses the serialization policy selected when the channel was
created. It does not create, terminate, or disconnect the process.

## Socket

The Socket adapters communicate through a Unix domain socket on POSIX systems
or a named pipe on Windows.

```ts
import { SocketServer } from "@the-link/ipc/socket-server"

const server = new SocketServer(address)

server.onConnection(link => {
    link.$inbound.subscribe("sum", (left: number, right: number) => left + right)
})

await server.listen()
```

```ts
import { SocketClient } from "@the-link/ipc/socket-client"

const client = new SocketClient(address)

await client.connect()

const total = await client.$outbound.publishFirst<number>("sum", 20, 22)

await client.disconnect()
```

Socket publications are bidirectional and carry remote results and failures.
UTF-8 JSON bytes are used by default. Applications may replace the Socket
serialization policy through `setSerialize()` and `setDeserialize()`.

POSIX sockets use mode `0600` by default. Applications remain responsible for
selecting and managing their addresses.

## Development

```sh
bun install --frozen-lockfile
bun run verify
```

## License

MIT
