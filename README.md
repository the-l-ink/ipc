# The Link IPC

Local IPC adapter for The Link. It uses Unix domain sockets on POSIX systems
and named pipes on Windows. Client and Server entry points remain isolated
while both expose the same `TheLink` routing model.

## Install

```sh
npm install @the-link/ipc
```

## Server

```ts
import { IpcServer } from "@the-link/ipc/server"

const address = process.platform === "win32"
  ? "\\\\.\\pipe\\application"
  : "/tmp/application.sock"

const server = new IpcServer(address)

server.onConnection(link => {
  link.$inbound.subscribe("sum", (left: number, right: number) => left + right)
})

await server.listen()
```

## Client

```ts
import { IpcClient } from "@the-link/ipc/client"

const client = new IpcClient(address)

await client.connect()

const total = await client.$outbound.publishFirst<number>("sum", 20, 22)

await client.disconnect()
```

Publications are bidirectional and return the values produced by remote Link
subscribers. A remote failure rejects the publication locally.

## Joining a Link

The Server receives a private Link for every accepted connection. It can be
joined to an application Link for the lifetime of that connection:

```ts
server.onConnection(link => link.autoJoin(application))
```

## Serialization

Each side uses JSON encoded as UTF-8 bytes by default and allows its policy to
be replaced:

```ts
client.setSerialize(serialize)
client.setDeserialize(deserialize)

server.setSerialize(serialize)
server.setDeserialize(deserialize)
```

The adapter does not decide which serialization policy an application must use.

## Addresses

The application owns address selection. Pass a filesystem socket path on POSIX
or a named-pipe path on Windows. POSIX sockets are restricted to mode `0600` by
default; `mode` may be changed in the Server options. Windows named pipes are
opened with `readableAll: false` and `writableAll: false`.

## Development

```sh
bun install --frozen-lockfile
bun run verify
```

## License

MIT
