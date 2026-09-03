import type { Socket } from "node:net"
import { TheLink, Tunnel, type Deserialize, type Serialize } from "@the-link/core"
import { FrameReader, writeFrame } from "./framing.js"
import { describe, exception, parseEnvelope, type Envelope } from "./protocol.js"

interface Pending {

    resolve(values: unknown[]): void
    reject(error: Error): void
}

/** Private Server-side Link for one accepted IPC connection. */
export default class Peer extends TheLink {

    public readonly $internal = new Tunnel()

    private readonly reader: FrameReader
    private readonly pending = new Map<number, Pending>()
    private sequence = 0
    private writes = Promise.resolve()
    private socketError: Error | null = null

    public constructor(
        private readonly socket: Socket,
        private readonly serialize: () => Serialize,
        private readonly deserialize: () => Deserialize,
        private readonly maximumFrameSize: number,
        private readonly maximumPending: number,
        private readonly released: () => void
    ) {

        super()

        this.reader = new FrameReader(maximumFrameSize)

        this.$outbound.forwardTo(this.publish.bind(this))

        socket.on("data", chunk => this.receive(chunk))
        socket.on("error", error => { this.socketError = error })
        socket.once("close", () => this.closed())
    }

    public autoJoin(link: TheLink, fromPrefix: string = "", toPrefix: string = "") {

        const disconnect = link.connectTo(this, fromPrefix, toPrefix)
        const unsubscribe = this.$internal.subscribeOnce("disconnect", disconnect)

        return () => {

            disconnect()
            unsubscribe()
        }
    }

    public async disconnect() {

        if (this.socket.destroyed) return

        await new Promise<void>(resolve => {

            this.socket.once("close", () => resolve())
            this.socket.destroy()
        })
    }

    private receive(chunk: Uint8Array) {

        try {

            for (const frame of this.reader.read(chunk)) this.received(parseEnvelope(this.deserialize()(frame)))
        }

        catch (error) {

            this.socketError = error instanceof Error ? error : new Error(String(error))
            this.socket.destroy()
        }
    }

    private received(envelope: Envelope) {

        if (envelope.type === "publish") {

            this.answer(envelope).catch(error => {

                this.socketError = error instanceof Error ? error : new Error(String(error))
                this.socket.destroy()
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

        if (this.socket.destroyed) return Promise.reject(new Error("The IPC connection is closed"))
        if (this.pending.size >= this.maximumPending) return Promise.reject(new Error(`The IPC connection has ${this.maximumPending} pending publications`))

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

        const write = this.writes.then(() => writeFrame(this.socket, this.serialize()(envelope), this.maximumFrameSize))

        this.writes = write.catch(() => undefined)

        return write
    }

    private closed() {

        const error = this.socketError ?? new Error("The IPC connection is closed")

        for (const pending of this.pending.values()) pending.reject(error)

        this.pending.clear()
        this.released()
        this.$internal.publish("disconnect", error).catch(() => undefined)
    }
}
