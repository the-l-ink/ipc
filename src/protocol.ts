export type Envelope =
    | { type: "publish", id: number, event: string, values: unknown[] }
    | { type: "resolve", id: number, values: unknown[] }
    | { type: "reject", id: number, error: { name: string, message: string } }

export function parseEnvelope(value: unknown): Envelope {

    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("The IPC message must be an object")

    const envelope = value as Record<string, unknown>

    if (!Number.isSafeInteger(envelope.id) || Number(envelope.id) < 0) throw new Error("The IPC message identifier is invalid")

    if (envelope.type === "publish") {

        if (typeof envelope.event !== "string" || !Array.isArray(envelope.values)) throw new Error("The IPC publication is invalid")

        return { type: envelope.type, id: Number(envelope.id), event: envelope.event, values: envelope.values }
    }

    if (envelope.type === "resolve") {

        if (!Array.isArray(envelope.values)) throw new Error("The IPC result is invalid")

        return { type: envelope.type, id: Number(envelope.id), values: envelope.values }
    }

    if (envelope.type === "reject") {

        const error = envelope.error

        if (!error || typeof error !== "object" || Array.isArray(error)) throw new Error("The IPC failure is invalid")

        const failure = error as Record<string, unknown>

        if (typeof failure.name !== "string" || typeof failure.message !== "string") throw new Error("The IPC failure is invalid")

        return { type: envelope.type, id: Number(envelope.id), error: { name: failure.name, message: failure.message } }
    }

    throw new Error("The IPC message type is invalid")
}

export function describe(error: unknown) {

    return error instanceof Error
        ? { name: error.name, message: error.message }
        : { name: "Error", message: String(error) }
}

export function exception(failure: { name: string, message: string }) {

    const error = new Error(failure.message)

    error.name = failure.name

    return error
}
