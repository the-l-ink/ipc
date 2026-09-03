import { connect, type Socket } from "node:net"
import { TheLink, Tunnel } from "@the-link/core"
import { FrameReader, limit, writeFrame } from "./framing.js"
import { describe, exception, parseEnvelope, type Envelope } from "./protocol.js"
import { defaultDeserialize, defaultSerialize, type Deserialize, type Serialize } from "./codec.js"

const defaultMaximumFrameSize = 16 * 1024 * 1024
const defaultMaximumPending = 1024

interface Pending {

    resolve(values: unknown[]): void
    reject(error: Error): void
}

/** One persistent Client half of a local IPC Link. */
export class IpcClient extends TheLink {

    public readonly $internal = new Tunnel()

    private serialize: Serialize = defaultSerialize
    private deserialize: Deserialize = defaultDeserialize
    private readonly maximumFrameSize: number
    private readonly maximumPending: number
    private socket: Socket | null = null
    private connecting: Promise<void> | null = null
    private reader: FrameReader | null = null
    private sequence = 0
    private readonly pending = new Map<number, Pending>()
    private writes = Promise.resolve()
    private socketError: Error | null = null

    public constructor(private readonly address: string, options: { maximumFrameSize?: number, maximumPending?: number } = {}) {

        super()

        if (!address) throw new TypeError("An IPC address is required")

        this.maximumFrameSize = limit(options.maximumFrameSize, defaultMaximumFrameSize, "maximumFrameSize")
        this.maximumPending = limit(options.maximumPending, defaultMaximumPending, "maximumPending")

        this.$outbound.forwardTo(this.publish.bind(this))
    }

    public setSerialize(serialize: Serialize) {

        this.serialize = serialize
    }

    public setDeserialize(deserialize: Deserialize) {

        this.deserialize = deserialize
    }

    public async connect(signal?: AbortSignal) {

        if (this.socket && !this.socket.destroyed) return
        if (this.connecting) return this.connecting

        const connecting = this.open(signal)

        this.connecting = connecting

        try { await connecting }
        finally { if (this.connecting === connecting) this.connecting = null }
    }

    public async disconnect() {

        if (this.connecting) await this.connecting.catch(() => undefined)

        const socket = this.socket

        if (!socket) return

        await new Promise<void>(resolve => {

            socket.once("close", () => resolve())
            socket.destroy()
        })
    }

    private open(signal?: AbortSignal) {

        return new Promise<void>((resolve, reject) => {

            if (signal?.aborted) return reject(abortReason(signal))

            const socket = connect(this.address)

            const abort = () => {

                socket.destroy()
                reject(abortReason(signal!))
            }

            const fail = (error: Error) => {

                cleanup()
                reject(error)
            }

            const cleanup = () => {

                signal?.removeEventListener("abort", abort)
                socket.off("error", fail)
            }

            signal?.addEventListener("abort", abort, { once: true })
            socket.once("error", fail)
            socket.once("connect", () => {

                cleanup()
                this.attach(socket)
                resolve()
                this.$internal.publish("connect").catch(() => undefined)
            })
        })
    }

    private attach(socket: Socket) {

        this.socket = socket
        this.reader = new FrameReader(this.maximumFrameSize)
        this.socketError = null

        socket.on("data", chunk => this.receive(chunk))
        socket.on("error", error => { this.socketError = error })
        socket.once("close", () => this.closed(socket))
    }

    private receive(chunk: Uint8Array) {

        try {

            for (const frame of this.reader!.read(chunk)) this.received(parseEnvelope(this.deserialize(frame)))
        }

        catch (error) {

            this.socketError = error instanceof Error ? error : new Error(String(error))
            this.socket?.destroy()
        }
    }

    private received(envelope: Envelope) {

        if (envelope.type === "publish") {

            this.answer(envelope).catch(error => {

                this.socketError = error instanceof Error ? error : new Error(String(error))
                this.socket?.destroy()
            })

            return
        }

        const pending = this.pending.get(envelope.id)

        if (!pending) return

        this.pending.delete(envelope.id)

        if (envelope.type === "resolve") pending.resolve(envelope.values)
        else pending.reject(exception(envelope.error))
    }

    private async answer(envelope: Extract<Envelope, { type: "publish" }>) {

        try {

            const values = await this.$inbound.publish(envelope.event, ...envelope.values)

            await this.send({ type: "resolve", id: envelope.id, values })
        }

        catch (error) {

            await this.send({ type: "reject", id: envelope.id, error: describe(error) })
        }
    }

    private publish(event: string, ...values: unknown[]) {

        if (!this.socket || this.socket.destroyed) return Promise.reject(new Error("The IPC Client is not connected"))
        if (this.pending.size >= this.maximumPending) return Promise.reject(new Error(`The IPC Client has ${this.maximumPending} pending publications`))

        const id = ++this.sequence

        return new Promise<unknown[]>((resolve, reject) => {

            this.pending.set(id, { resolve, reject })

            this.send({ type: "publish", id, event, values }).catch(error => {

                if (!this.pending.delete(id)) return

                reject(error instanceof Error ? error : new Error(String(error)))
            })
        })
    }

    private send(envelope: Envelope) {

        const socket = this.socket

        if (!socket) return Promise.reject(new Error("The IPC Client is not connected"))

        const write = this.writes.then(() => writeFrame(socket, this.serialize(envelope), this.maximumFrameSize))

        this.writes = write.catch(() => undefined)

        return write
    }

    private closed(socket: Socket) {

        if (this.socket !== socket) return

        this.socket = null
        this.reader = null

        const error = this.socketError ?? new Error("The IPC connection is closed")

        this.socketError = null

        for (const pending of this.pending.values()) pending.reject(error)

        this.pending.clear()
        this.$internal.publish("disconnect", error).catch(() => undefined)
    }
}

function abortReason(signal: AbortSignal) {

    return signal.reason instanceof Error ? signal.reason : new Error("The IPC connection was cancelled")
}
