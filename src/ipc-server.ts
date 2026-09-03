import { chmod } from "node:fs/promises"
import { createServer, type Server } from "node:net"
import { deserializeJSON, serializeJSON, TheLink, Tunnel, type Deserialize, type Serialize, type Subscriber } from "@the-link/core"
import { limit } from "./framing.js"
import Peer from "./server-peer.js"

const defaultMaximumFrameSize = 16 * 1024 * 1024
const defaultMaximumPending = 1024

/** Local IPC listener that presents each accepted peer as a private Link. */
export class IpcServer extends TheLink {

    public readonly $internal = new Tunnel()

    private serialize: Serialize = serializeJSON
    private deserialize: Deserialize = deserializeJSON
    private readonly maximumFrameSize: number
    private readonly maximumPending: number
    private readonly mode: number
    private readonly server: Server
    private readonly peers = new Set<Peer>()

    public constructor(private readonly address: string, options: { maximumFrameSize?: number, maximumPending?: number, mode?: number } = {}) {

        super()

        if (!address) throw new TypeError("An IPC address is required")

        this.maximumFrameSize = limit(options.maximumFrameSize, defaultMaximumFrameSize, "maximumFrameSize")
        this.maximumPending = limit(options.maximumPending, defaultMaximumPending, "maximumPending")
        this.mode = mode(options.mode)
        this.server = createServer(socket => this.accept(socket))

        this.server.on("error", error => this.$internal.publish("error", error).catch(() => undefined))
    }

    public setSerialize(serialize: Serialize) {

        this.serialize = serialize
    }

    public setDeserialize(deserialize: Deserialize) {

        this.deserialize = deserialize
    }

    public onConnection(subscriber: Subscriber<[Peer]>) {

        return this.$internal.subscribe("connection", subscriber)
    }

    public async listen() {

        if (this.server.listening) return

        await new Promise<void>((resolve, reject) => {

            const failed = (error: Error) => {

                this.server.off("listening", ready)
                reject(error)
            }

            const ready = () => {

                this.server.off("error", failed)
                resolve()
            }

            this.server.once("error", failed)
            this.server.once("listening", ready)
            this.server.listen({ path: this.address, readableAll: false, writableAll: false })
        })

        if (process.platform === "win32") return

        try { await chmod(this.address, this.mode) }
        catch (error) {

            await this.close()
            throw error
        }
    }

    public async close() {

        await Promise.all([...this.peers].map(peer => peer.disconnect()))

        if (!this.server.listening) return

        await new Promise<void>((resolve, reject) => this.server.close(error => error ? reject(error) : resolve()))
    }

    private accept(socket: import("node:net").Socket) {

        let peer: Peer

        peer = new Peer(
            socket,
            () => this.serialize,
            () => this.deserialize,
            this.maximumFrameSize,
            this.maximumPending,
            () => this.peers.delete(peer)
        )

        this.peers.add(peer)

        this.$internal.publish("connection", peer).catch(() => peer.disconnect())
    }
}

function mode(value: unknown) {

    const resolved = value ?? 0o600

    if (!Number.isInteger(resolved) || Number(resolved) < 0 || Number(resolved) > 0o777) {

        throw new TypeError("mode must be an integer between 0 and 511")
    }

    return Number(resolved)
}
