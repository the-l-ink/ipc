const headerSize = 4

export class FrameReader {

    private buffer = new Uint8Array()

    public constructor(private readonly maximum: number) { }

    public read(chunk: Uint8Array) {

        this.buffer = this.buffer.length
            ? concatenate(this.buffer, chunk)
            : Uint8Array.from(chunk)

        const frames: Uint8Array<ArrayBuffer>[] = []

        while (this.buffer.length >= headerSize) {

            const length = new DataView(this.buffer.buffer, this.buffer.byteOffset, headerSize).getUint32(0)

            if (length > this.maximum) throw new Error(`The IPC frame exceeds ${this.maximum} bytes`)
            if (this.buffer.length < headerSize + length) break

            frames.push(Uint8Array.from(this.buffer.subarray(headerSize, headerSize + length)))

            this.buffer = this.buffer.subarray(headerSize + length)
        }

        return frames
    }
}

export function writeFrame(socket: FrameWriter, bytes: Uint8Array, maximum: number) {

    if (bytes.byteLength > maximum) return Promise.reject(new Error(`The IPC frame exceeds ${maximum} bytes`))
    if (!socket.writable || socket.destroyed) return Promise.reject(new Error("The IPC connection is closed"))

    const frame = new Uint8Array(headerSize + bytes.byteLength)

    new DataView(frame.buffer).setUint32(0, bytes.byteLength)
    frame.set(bytes, headerSize)

    return new Promise<void>((resolve, reject) => {

        socket.write(frame, error => error ? reject(error) : resolve())
    })
}

function concatenate(left: Uint8Array, right: Uint8Array) {

    const result = new Uint8Array(left.byteLength + right.byteLength)

    result.set(left)
    result.set(right, left.byteLength)

    return result
}

interface FrameWriter {
    readonly destroyed?: boolean
    readonly writable?: boolean
    write(bytes: Uint8Array, callback: (error?: Error | null) => void): unknown
}

export function limit(value: unknown, fallback: number, name: string) {

    const resolved = value ?? fallback

    if (!Number.isSafeInteger(resolved) || Number(resolved) < 1 || Number(resolved) > 0xffffffff) {

        throw new TypeError(`${name} must be a positive safe integer no greater than 4294967295`)
    }

    return Number(resolved)
}
