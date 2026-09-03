import type { Socket } from "node:net"

const headerSize = 4

export class FrameReader {

    private buffer = Buffer.alloc(0)

    public constructor(private readonly maximum: number) { }

    public read(chunk: Uint8Array) {

        this.buffer = this.buffer.length
            ? Buffer.concat([this.buffer, chunk])
            : Buffer.from(chunk)

        const frames: Uint8Array<ArrayBuffer>[] = []

        while (this.buffer.length >= headerSize) {

            const length = this.buffer.readUInt32BE(0)

            if (length > this.maximum) throw new Error(`The IPC frame exceeds ${this.maximum} bytes`)
            if (this.buffer.length < headerSize + length) break

            frames.push(Uint8Array.from(this.buffer.subarray(headerSize, headerSize + length)))

            this.buffer = this.buffer.subarray(headerSize + length)
        }

        return frames
    }
}

export function writeFrame(socket: Socket, bytes: Uint8Array, maximum: number) {

    if (bytes.byteLength > maximum) return Promise.reject(new Error(`The IPC frame exceeds ${maximum} bytes`))
    if (!socket.writable || socket.destroyed) return Promise.reject(new Error("The IPC connection is closed"))

    const frame = Buffer.allocUnsafe(headerSize + bytes.byteLength)

    frame.writeUInt32BE(bytes.byteLength, 0)
    frame.set(bytes, headerSize)

    return new Promise<void>((resolve, reject) => {

        socket.write(frame, error => error ? reject(error) : resolve())
    })
}

export function limit(value: unknown, fallback: number, name: string) {

    const resolved = value ?? fallback

    if (!Number.isSafeInteger(resolved) || Number(resolved) < 1 || Number(resolved) > 0xffffffff) {

        throw new TypeError(`${name} must be a positive safe integer no greater than 4294967295`)
    }

    return Number(resolved)
}
