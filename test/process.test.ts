import { afterEach, describe, expect, test } from "bun:test"
import { fork, spawn, type ChildProcess } from "node:child_process"
import { fileURLToPath } from "node:url"
import { ProcessLink } from "../src/process.js"

const children: ChildProcess[] = []
const entry = fileURLToPath(new URL("./process-child.mjs", import.meta.url))

afterEach(() => {

    for (const child of children.splice(0)) {

        if (child.connected) child.disconnect()
        if (!child.killed) child.kill()
    }
})

describe("Process IPC adapter", () => {

    test("uses an IPC channel created by fork", async () => {

        await verify(fork(entry, { serialization: "advanced" }))
    })

    test("uses an IPC channel created by spawn", async () => {

        await verify(spawn(process.execPath, [entry], {
            serialization: "advanced",
            stdio: ["ignore", "ignore", "ignore", "ipc"]
        }))
    })

    test("rejects a process without an IPC channel", () => {

        const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 1000)"], {
            stdio: "ignore"
        })

        children.push(child)

        expect(() => new ProcessLink(child)).toThrow("IPC channel not available")
    })
})

async function verify(child: ChildProcess) {

    children.push(child)

    const link = new ProcessLink(child)

    await link.$inbound.waitFor("ready")

    const echoed = link.$inbound.waitFor<[number, Map<string, number>]>("echoed")

    await link.$outbound.publish("echo", 42, new Map([["answer", 42]]))

    expect(await echoed).toEqual([42, new Map([["answer", 42]])])

    link.disconnect()
}
