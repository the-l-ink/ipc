import type { ChildProcess } from "node:child_process"
import { TheLink } from "@the-link/core"

interface ProcessChannel {

    readonly connected: boolean

    send(message: unknown, callback: (error: Error | null) => void): boolean

    on(event: "message", listener: (message: unknown) => void): void

    off(event: "message", listener: (message: unknown) => void): void
}

/** Bidirectional Link over an existing Node.js process IPC channel. */
export class ProcessLink extends TheLink {

    private readonly target: ProcessChannel

    public constructor(target: NodeJS.Process | ChildProcess = process) {

        super()

        if (typeof target.send !== "function") throw new Error("IPC channel not available. Ensure the process was spawned with IPC support.")

        this.target = target as ProcessChannel

        target.on("message", this.receive)
        this.$outbound.forwardTo(this.publish)
    }

    /** Detach this Link without terminating or disconnecting the process. */
    public disconnect() {

        this.target.off("message", this.receive)
        this.$outbound.stopForwardingTo(this.publish)
    }

    private readonly receive = async (message: unknown) => {

        if (!Array.isArray(message) || typeof message[0] !== "string") return

        const [event, ...values] = message as [string, ...unknown[]]

        await this.$inbound.publish(event, ...values)
    }

    private readonly publish = (event: string, ...values: unknown[]) => {

        if (!this.target.connected) {

            return Promise.reject(new Error("The process IPC channel is not connected"))
        }

        return new Promise<void>((resolve, reject) => {

            this.target.send([event, ...values], error => error ? reject(error) : resolve())
        })
    }
}
