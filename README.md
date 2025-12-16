# BIDC - Bidirectional Channels for JavaScript

A bidirectional channel library for __asynchronous communication__ support across JavaScript contexts.

## Overview

Unlike traditional `postMessage` APIs, BIDC enables seamless communication between different JavaScript execution contexts (workers, iframes, service workers) with full support for complex data types, with __Promises__ and __Async Functions__, via a custom streaming protocol.

BIDC automatically establishes a channel between the current context and the target context, regardless of which side initiates first. It also handles re-establishing connections if one side is reloaded (e.g., an iframe) and buffers messages until the other side is ready to receive them.

<p align="center">
  <img src="demo.svg" />
</p>

## Features

- **Automatic Handshake**: Robust connection establishment with collision resolution
- **Complex Data Types**: Seamlessly transfer Dates, RegExp, Maps, Sets, ArrayBuffers, TypedArrays, and more
- **Asynchronous**: Promises and Async Functions remain as first-class citizens during transfer
- **Concurrent Operations**: Handle multiple simultaneous messages without blocking
- **TypeScript**: Easy to infer types for the RPC-style method calls

Examples

- [Basic Data Transfer](#basic-data-transfer)
- [Two-Way Communication](#two-way-communication)
- [Complex Data Types](#complex-data-types)
- [Advanced Use Cases: Nested Promises](#advanced-use-cases-nested-promises)
- [Advanced Use Cases: Remote Callbacks](#advanced-use-cases-remote-callbacks)
- [Advanced Use Cases: Worker Communication](#advanced-use-cases-worker-communication)
- [Advanced Use Cases: Namespaced Channels](#advanced-use-cases-namespaced-channels)

## Installation

```bash
pnpm i bidc
```

## Examples

### Basic Data Transfer

BIDC automatically establishes a channel between the current context and the target context (e.g., an iframe or worker),
__no matter which side initiates first__. It will also re-establish the connection if one side (such as an iframe) is reloaded.

Parent window:

```js
import { createChannel } from 'bidc'

// Connect to an iframe
const { send } = createChannel(iframe.contentWindow)

// Send a simple message to the iframe
const result = await send({ value: 'Hello, iframe!' })

console.assert(result === 'HELLO, IFRAME!')
```

Inside the iframe:

```js
import { createChannel } from 'bidc'

// Omitting the target will create a channel to the parent window by default
// This is equivalent to `createChannel(window.parent)`
const { receive } = createChannel()

// Handle incoming messages and return a response
receive(payload => {
  return payload.value.toUpperCase()
})
```

`await send(...)` will return the response handled by the `receive` function in the iframe.
Before the connection is established, `send` will wait until the other side is ready to receive and handle the message.

### Two-Way Communication

You can use `send` and `receive` methods on both sides to establish a two-way communication channel.

```js
import { createChannel } from 'bidc'

const { send, receive } = createChannel(iframe.contentWindow)

// Handle incoming messages from the iframe
receive((payload) => {
  console.log('Received from iframe:', payload)
  return { response: 'Message received!' }
})

// Send a message to the iframe and wait for its response
const responseFromIframe = await send({ value: 'Hello, iframe!' })
```

Inside the iframe:

```js
import { createChannel } from 'bidc'

const { send, receive } = createChannel()

// Handle incoming messages from the parent window
receive((payload) => {
  console.log('Received from parent:', payload)
  return { response: 'Hello, parent!' }
})

// Send a message to the parent window and wait for its response
const responseFromParent = await send({ value: 'Hello, parent!' })
```

### Complex Data Types

Complex data types are supported, including:
- string, number, boolean, bigint, null, undefined
- Date, RegExp, ArrayBuffer, TypedArray
- Map, Set, Object, Array
- __Promise__ and __Async Function__

Promises must resolve/reject to values that are supported by BIDC (can be another Promise). Likewise, async functions can accept and return any supported value, including new Promises and async functions.

```js
// Send complex objects with various JavaScript types
const response = await send({
  date: new Date(),
  map: new Map([['key', 'value']]),
  set: new Set([1, 2, 3]),
  arrayBuffer: new Uint8Array([1, 2, 3]).buffer,
  promise: Promise.resolve('resolved value'),
  function: async (x) => {
    await sleep(1000)
    return x * 2
  }
})

console.assert(response.status === 'success')
```

Receiving side:

```js
receive(async (payload) => {
  // Handle the promise
  const resolvedValue = await payload.promise
  console.assert(resolvedValue === 'resolved value')
  
  // Call the function
  const result = await payload.function(5)
  console.assert(result === 10)

  return { status: 'success' }
})
```

### Advanced Use Cases: Nested Promises

This example demonstrates how to handle nested Promises and async functions, allowing for complex asynchronous workflows.

Parent window:

```js
import { createChannel } from 'bidc'

const { send } = createChannel(iframe.contentWindow)

await send({
  foo: new Promise(resolve => {
    setTimeout(() => {
      resolve({
        bar: new Promise(resolve => {
          setTimeout(() => {
            resolve('Hello from the parent!')
          }, 1000)
        }),
      })
    }, 1000)
  }),
})
```

On the receiving side, you can handle the data payload like this:

```js
// ...
receive(async (data) => {
  const result = await data.foo
  const finalResult = await result.bar

  console.assert(finalResult === 'Hello from the parent!')
})
```

### Advanced Use Cases: Remote Callbacks

This example demonstrates how to create nested remote callback mechanisms where two targets (parent and iframe) can both
send and receive async functions (which can also take or return async functions) to each other.

Parent window:

```js
import { createChannel } from 'bidc'

const { send } = createChannel(iframe.contentWindow)

// Give an async function to the iframe
// It will always be executed in the parent window's context
const calc = await send(
  async x => {
    return x * 3
  }
)

console.log(await calc(1))
```

Iframe:

```js
import { createChannel } from 'bidc'

const { receive } = createChannel()

receive(parentCalc => {
  return async y => {
    const result = await parentCalc(y * 2)
    return result * 4
  }
})
```

Calling `send(...)` will give that function to the iframe receiver as `parentCalc`. Which then
wraps it in another async function and returns it back to the parent window as `calc`.

So the final result of `calc(1)` will be `1 * 2 * 3 * 4 = 24`. `1` will be passed as `y`, which is then multiplied by `2` in the iframe, then passed to the parent window's `parentCalc` function as `x`, which multiplies it by `3`, and finally the `result` is multiplied by `4` in the iframe.

### Advanced Use Cases: Worker Communication

You can also use BIDC to communicate with Web Workers too. Simply pass the worker instance to `createChannel` as the target context:

```js
import { createChannel } from 'bidc'

const worker = new Worker('./worker.js')

const { send, receive } = createChannel(worker)

// ...
```

Inside the worker, it will automatically create a channel to the parent context:

```js
import { createChannel } from 'bidc'

const { send, receive } = createChannel()
```

### Advanced Use Cases: Namespaced Channels

You can create namespaced channels to avoid conflicts between different parts of your application. This is useful when you have multiple components or modules that need to communicate without interfering with each other.

For example, you can create 2 channels between the parent and the iframe by specifying different channel IDs (namespaces):

```js
import { createChannel } from 'bidc'

const { send: sendA, receive: receiveA } = createChannel(iframe.contentWindow, 'namespaceA')
const { send: sendB, receive: receiveB } = createChannel(iframe.contentWindow, 'namespaceB')
```

Inside the iframe, you can handle messages from both namespaces:

```js
import { createChannel } from 'bidc'

const { send: sendA, receive: receiveA } = createChannel('namespaceA')
const { send: sendB, receive: receiveB } = createChannel('namespaceB')
```

Just ensure that the namespace IDs match on both sides.

## To-do

You are welcome to contribute! Some potential improvements and features to consider:

- Refactor to avoid using `MessageChannel` to support React Native
- Stable object references across messages (via `WeakMap`)
- Include function reference clean-up information (via `WeakRef`) in the protocol to avoid memory leaks
- Benchmarking and performance optimizations

## Acknowledgements

This library is a recreation of RSC (React Server Components)'s serialization mechanism and React Server Actions reference passing idea.

The underlying serialization and deserialization logic is implemented using the [devalue](https://github.com/Rich-Harris/devalue) library created by Rich Harris.

This library is authored by [Shu Ding](https://x.com/shuding_).

## License

MIT
