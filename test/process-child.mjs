process.on("message", message => {

    if (!Array.isArray(message) || typeof message[0] !== "string") return

    const [event, ...values] = message

    if (event === "echo") process.send?.(["echoed", ...values])
})

process.send?.(["ready"])
