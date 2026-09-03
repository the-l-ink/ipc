import { afterEach, describe, expect, test } from "bun:test"
import { randomUUID } from "node:crypto"
import { mkdtemp, rm, stat } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { TheLink } from "@the-link/core"
import { IpcClient } from "../src/client.js"
import { FrameReader } from "../src/framing.js"
import { IpcServer } from "../src/server.js"

const directories: string[] = []
const servers: IpcServer[] = []
const clients: IpcClient[] = []

afterEach(async () => {

    await Promise.all(clients.splice(0).map(client => client.disconnect()))
    await Promise.all(servers.splice(0).map(server => server.close()))
    await Promise.all(directories.splice(0).map(directory => rm(directory, { recursive: true, force: true })))
})

describe("IPC adapter", () => {

    test("joins a Link and preserves publications in both directions", async () => {

        const { client, peer } = await connected(function (link) {

            link.$inbound.subscribe("sum", (left: number, right: number) => left + right)
        })

        client.$inbound.subscribe("double", (value: number) => value * 2)

        expect(await client.$outbound.publishFirst("sum", 20, 22)).toBe(42)
        expect(await peer.$outbound.publishFirst("double", 21)).toBe(42)
    })

    test("connects an application Link for the socket lifetime", async () => {

        const application = new TheLink()

        application.$inbound.subscribe("sum", (left: number, right: number) => left + right)

        const { client } = await connected(link => link.autoJoin(application))

        expect(await client.$outbound.publishFirst("sum", 19, 23)).toBe(42)
    })

    test("returns remote failures", async () => {

        const { client } = await connected(function (link) {

            link.$inbound.subscribe("fail", () => {

                throw new TypeError("Expected failure")
            })
        })

        await expect(client.$outbound.publish("fail")).rejects.toThrow("Expected failure")
    })

    test("keeps accepted clients isolated", async () => {

        const address = await ipcAddress()
        const server = keep(new IpcServer(address), servers)
        let identity = 0

        server.onConnection(link => {

            const current = ++identity

            link.$inbound.subscribe("identity", () => current)
        })

        await server.listen()

        const first = keep(new IpcClient(address), clients)
        const second = keep(new IpcClient(address), clients)

        await Promise.all([first.connect(), second.connect()])

        const values = await Promise.all([
            first.$outbound.publishFirst<number>("identity"),
            second.$outbound.publishFirst<number>("identity")
        ])

        expect(new Set(values)).toEqual(new Set([1, 2]))
    })

    test("uses the serialization policy selected by the application", async () => {

        const address = await ipcAddress()
        const server = keep(new IpcServer(address), servers)
        const client = keep(new IpcClient(address), clients)
        let serialized = 0
        let deserialized = 0
        const encoder = new TextEncoder()
        const decoder = new TextDecoder()
        const serialize = (value: unknown) => {

            serialized++

            return Uint8Array.from(encoder.encode(JSON.stringify(value))).reverse()
        }
        const deserialize = (bytes: Uint8Array) => {

            deserialized++

            return JSON.parse(decoder.decode(Uint8Array.from(bytes).reverse()))
        }

        server.setSerialize(serialize)
        server.setDeserialize(deserialize)
        client.setSerialize(serialize)
        client.setDeserialize(deserialize)
        server.onConnection(link => link.$inbound.subscribe("value", (value: number) => value))

        await server.listen()
        await client.connect()

        expect(await client.$outbound.publishFirst("value", 42)).toBe(42)
        expect(serialized).toBeGreaterThanOrEqual(2)
        expect(deserialized).toBeGreaterThanOrEqual(2)
    })

    test("rejects pending publications when disconnected", async () => {

        const { client } = await connected(link => {

            link.$inbound.subscribe("pending", () => new Promise(() => undefined))
        })

        const publication = client.$outbound.publish("pending")

        await Bun.sleep(10)
        await client.disconnect()

        await expect(publication).rejects.toThrow("closed")
    })

    test("reads fragmented and combined frames", () => {

        const reader = new FrameReader(1024)
        const first = frame(new Uint8Array([1, 2, 3]))
        const second = frame(new Uint8Array([4, 5]))
        const combined = Buffer.concat([first, second])

        expect(reader.read(combined.subarray(0, 2))).toEqual([])
        expect(reader.read(combined.subarray(2, 6))).toEqual([])
        expect(reader.read(combined.subarray(6))).toEqual([
            new Uint8Array([1, 2, 3]),
            new Uint8Array([4, 5])
        ])
    })

    test.skipIf(process.platform === "win32")("creates a private POSIX socket by default", async () => {

        const address = await ipcAddress()
        const server = keep(new IpcServer(address), servers)

        await server.listen()

        expect((await stat(address)).mode & 0o777).toBe(0o600)
    })
})

async function connected(accept: Parameters<IpcServer["onConnection"]>[0]) {

    const address = await ipcAddress()
    const server = keep(new IpcServer(address), servers)
    let resolvePeer!: (peer: Parameters<typeof accept>[0]) => void
    const accepted = new Promise<Parameters<typeof accept>[0]>(resolve => { resolvePeer = resolve })

    server.onConnection(peer => {

        accept(peer)
        resolvePeer(peer)
    })

    await server.listen()

    const client = keep(new IpcClient(address), clients)

    await client.connect()

    return { client, peer: await accepted }
}

async function ipcAddress() {

    if (process.platform === "win32") return `\\\\.\\pipe\\the-link-ipc-${process.pid}-${randomUUID()}`

    const directory = await mkdtemp(join(tmpdir(), "the-link-ipc-"))

    directories.push(directory)

    return join(directory, "link.sock")
}

function keep<Value>(value: Value, values: Value[]) {

    values.push(value)

    return value
}

function frame(bytes: Uint8Array) {

    const result = Buffer.alloc(4 + bytes.length)

    result.writeUInt32BE(bytes.length)
    result.set(bytes, 4)

    return result
}
