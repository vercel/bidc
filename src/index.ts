// Simplified Promise Stream Library with compact protocol
import * as devalue from 'devalue'

// A global store to track function references and their IDs
// TODO: Solve this with WeakRef maybe? Track refs on the receiver side and
// send back removed functions to the sender side along with the return value.
const functionRefIds = new Map<Function, string>()
const functionRefsById = new Map<string, Function>()

// Custom stringify and parse functions using devalue
function stringify(value: any, promiseMap: Map<Promise<any>, string>): string {
  return devalue.stringify(value, {
    F: (fn: Function) => {
      if (typeof fn === 'function') {
        // Use devalue's Function serializer to handle function detection and replacement
        if (!functionRefIds.has(fn)) {
          const id = functionRefIds.size.toString()
          functionRefIds.set(fn, id)
          functionRefsById.set(id, fn)
          if (functionRefIds.size > 50000) {
            console.warn(
              'Function reference store is getting large, it is not recommended to send anonymous and inline functions through the channel as they cannot be cached.'
            )
          }
        }

        return functionRefIds.get(fn)
      }

      return undefined
    },
    P: (promise) => {
      // Use devalue's Promise serializer to handle promise detection and replacement
      if (
        promise &&
        typeof promise === 'object' &&
        typeof promise.then === 'function'
      ) {
        if (!promiseMap.has(promise))
          promiseMap.set(promise, promiseMap.size.toString())
        return promiseMap.get(promise)
      }
      return undefined
    },
  })
}

function parse(
  text: string,
  promiseResolvers: Map<
    string,
    { resolve: Function; reject: Function; promise: Promise<any> }
  >,
  send?: (data: any) => Promise<any>
): any {
  return devalue.parse(text, {
    F: (id: string) => {
      if (!send) return null

      // Resolve function references by ID
      return async function (...args: any[]) {
        return send({ $$type: `bidc-fn:${id}`, args })
      }
    },
    P: (promiseId: string) => {
      // Check if promise already exists to avoid creating duplicates
      if (promiseResolvers.has(promiseId)) {
        return promiseResolvers.get(promiseId)!.promise
      }

      let resolvePromise: Function
      let rejectPromise: Function

      const promise = new Promise((resolve, reject) => {
        resolvePromise = resolve
        rejectPromise = reject
      })

      promiseResolvers.set(promiseId, {
        resolve: resolvePromise!,
        reject: rejectPromise!,
        promise: promise, // Store the promise instance for reuse
      })

      return promise
    },
  })
}

export type SerializableValue =
  | void
  | string
  | number
  | boolean
  | null
  | undefined
  | RegExp
  | Date
  | Map<any, any>
  | Set<any>
  | bigint
  | ArrayBuffer
  | TypedArray
  | SerializableObject
  | SerializableArray
  | SerializableFunction
  | Promise<SerializableValue>

// TypedArray type definition
type TypedArray =
  | Int8Array
  | Uint8Array
  | Uint8ClampedArray
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float32Array
  | Float64Array
  | BigInt64Array
  | BigUint64Array

export interface SerializableObject {
  [key: string]: SerializableValue
}

export interface SerializableArray extends Array<SerializableValue> {}

export interface SerializableFunction {
  <Args extends SerializableValue[]>(
    ...args: Args
  ): void | Promise<SerializableValue>
}

/**
 * Encode a value with promises to an async iterator of string chunks
 * Uses r[id]: for return data and p0:, p1: etc for promise data
 */
export async function* encode<T extends SerializableValue>(
  value: T
): AsyncGenerator<string> {
  const pendingPromises = new Map<string, Promise<any>>()
  const promiseMap = new Map<Promise<any>, string>()
  // Track resolved promise IDs to prevent re-adding them
  const resolvedPromiseIds = new Set<string>()

  // First, yield the main structure with promise placeholders using devalue's serializer
  const serializedValue = stringify(value, promiseMap)
  yield `r:${serializedValue}\n`

  // Build pendingPromises map from the promiseMap created during stringify
  for (const [promise, promiseId] of promiseMap.entries()) {
    pendingPromises.set(promiseId, promise)
  }

  // Then process and yield promise resolutions
  while (pendingPromises.size > 0) {
    const promiseEntries = Array.from(pendingPromises.entries())

    // Replace Promise.allSettled with Promise.race for immediate chunk sending when any promise resolves
    // Create promises that resolve with their ID and result
    const racingPromises = promiseEntries.map(async ([promiseId, promise]) => {
      try {
        const resolvedValue = await promise
        return { promiseId, status: 'fulfilled', value: resolvedValue }
      } catch (error) {
        return { promiseId, status: 'rejected', reason: error }
      }
    })

    // Race all promises and handle the first one that completes
    const result = await Promise.race(racingPromises)

    // Remove the completed promise from pending
    pendingPromises.delete(result.promiseId)
    // Mark this promise ID as resolved to prevent re-adding
    resolvedPromiseIds.add(result.promiseId)

    if (result.status === 'fulfilled') {
      // Use stringify with promiseMap for resolved values that might contain more promises
      const processedValue = stringify(result.value, promiseMap)
      yield `p${result.promiseId}:${processedValue}\n`

      // Add any new promises found in resolved value to pending promises
      // Only add promises that haven't been resolved yet
      for (const [promise, newPromiseId] of promiseMap.entries()) {
        if (
          !pendingPromises.has(newPromiseId) &&
          !resolvedPromiseIds.has(newPromiseId)
        ) {
          pendingPromises.set(newPromiseId, promise)
        }
      }
    } else {
      const errorMessage =
        result.reason instanceof Error
          ? result.reason.message
          : String(result.reason)
      yield `e${result.promiseId}:${stringify(errorMessage, promiseMap)}\n`
    }
  }
}

/**
 * Decode an async iterator of string chunks back to the original value with promises
 * Returns immediately with unresolved promises that will resolve as chunks arrive
 */
export async function decode<T = any>(
  chunks: AsyncIterable<string>,
  options?: {
    send?: (key: string, ...args: any[]) => Promise<any>
  }
): Promise<T> {
  let result: any = undefined
  // Updated type to include promise instance
  const promiseResolvers = new Map<
    string,
    { resolve: Function; reject: Function; promise: Promise<any> }
  >()

  // Process chunks asynchronously after receiving the first "r:" chunk
  const chunkIterator = chunks[Symbol.asyncIterator]()

  // Get the first chunk and validate it starts with "r"
  const firstIteratorResult = await chunkIterator.next()
  if (firstIteratorResult.done) {
    throw new Error('Stream ended without any chunks')
  }

  const firstChunk = firstIteratorResult.value

  // Validate that the first chunk starts with "r" (could be "r:" or "r123:")
  if (!firstChunk.startsWith('r')) {
    throw new Error("First chunk must start with 'r' (return data)")
  }

  // Extract data from first chunk (everything after the first colon)
  const colonIndex = firstChunk.indexOf(':')
  if (colonIndex === -1) {
    throw new Error('Invalid first chunk format - missing colon')
  }

  // Parse using devalue with custom Promise deserializer
  const serializedData = firstChunk.slice(colonIndex + 1)
  result = parse(serializedData, promiseResolvers, options?.send)

  // Continue with remaining chunks asynchronously
  const processRemainingChunks = async () => {
    try {
      // Continue processing remaining chunks
      let iteratorResult = await chunkIterator.next()
      while (!iteratorResult.done) {
        const line = iteratorResult.value
        if (line.startsWith('p')) {
          // Promise resolution data
          const colonIndex = line.indexOf(':')
          const promiseId = line.slice(1, colonIndex)
          const data = parse(
            line.slice(colonIndex + 1),
            promiseResolvers,
            options?.send
          )

          const resolver = promiseResolvers.get(promiseId)
          if (resolver) {
            resolver.resolve(data)
          }
        } else if (line.startsWith('e')) {
          // Promise error data
          const colonIndex = line.indexOf(':')
          const promiseId = line.slice(1, colonIndex)
          const errorMessage = parse(
            line.slice(colonIndex + 1),
            promiseResolvers,
            options?.send
          )

          const resolver = promiseResolvers.get(promiseId)
          if (resolver) {
            resolver.reject(new Error(errorMessage))
          }
        }

        iteratorResult = await chunkIterator.next()
      }
    } catch (error) {
      // Reject any remaining promises if there's an error
      for (const resolver of promiseResolvers.values()) {
        resolver.reject(error)
      }
    }
  }

  // Start processing remaining chunks asynchronously (don't await)
  processRemainingChunks()

  return result as T
}

// Target interface that can postMessage
type MessageTarget =
  | Window
  | Worker
  | MessagePort
  | ServiceWorker
  | BroadcastChannel
  | MessageEventSource

// We only make one message channel per target. This maximizes performance
// and avoids issues with multiple connections to the same target, as well as
// supporting initialization inside useEffect hooks.
const connectionCache = new WeakMap<MessageTarget, Promise<MessagePort>>()

type TChannel = {
  send: <
    TReceiver,
    T = TReceiver extends (data: infer TT) => any ? TT : never,
    R = TReceiver extends (data: any) => infer TR
      ? TR
      : TReceiver extends (data: any) => Promise<infer TR>
      ? TR
      : never
  >(
    data: T
  ) => Promise<R>
  receive: <T extends SerializableValue, R extends SerializableValue>(
    callback: (data: T) => R
  ) => Promise<void>
  cleanup: () => void
  onResetPort: (callback: (port: MessagePort) => void) => void
}

/**
 * Create a bidirectional channel between self and target using a channelId for handshake
 */
function createChannel(): TChannel
function createChannel(targetOrChannelId: MessageTarget | string): TChannel
function createChannel(channelId: string, origin: string): TChannel
function createChannel(maybeTarget: MessageTarget, channelId: string): TChannel
function createChannel(
  maybeTarget: MessageTarget,
  channelId: string,
  origin: string
): TChannel
function createChannel(
  targetOrChannelId?: MessageTarget | string,
  channelIdOrOrigin?: string,
  maybeOrigin?: string
) {
  let maybeTarget: MessageTarget | undefined = undefined
  let channelId: string | undefined
  // Expected origin of the other endpoint. When set, incoming connections
  // from any other origin are rejected. Must be declared by the caller since
  // it can't be derived from a cross-origin target.
  let expectedOrigin: string | undefined

  if (typeof targetOrChannelId === 'object') {
    // (target, channelId?, origin?)
    maybeTarget = targetOrChannelId
    channelId = channelIdOrOrigin
    expectedOrigin = maybeOrigin
  } else {
    // (channelId?, origin?) — no target, uses window.parent / self
    channelId = targetOrChannelId
    expectedOrigin = channelIdOrOrigin
  }

  // Namespaced channelId to avoid conflicts with other libraries / multiple
  // connections.
  channelId = 'bidc_' + (channelId ?? 'default')

  // Cache key for the message port of the connection
  const cacheKey = maybeTarget || self

  const onResetPortCallbacks: ((port: MessagePort) => void)[] = []
  function onResetPort(callback: (port: MessagePort) => void) {
    onResetPortCallbacks.push(callback)
  }

  function initPort() {
    let connected = false
    let connectionResolver: ((port: MessagePort) => void) | null = null
    const connectionPromise = new Promise<MessagePort>((resolve) => {
      connectionResolver = resolve
    })

    function sendMessageWithTransfer(message: any, transfer: MessagePort) {
      if (maybeTarget) {
        if ('self' in maybeTarget && maybeTarget.self === maybeTarget) {
          // Restrict delivery to the expected origin when declared
          maybeTarget.postMessage(message, expectedOrigin || '*', [transfer])
        } else {
          ;(maybeTarget as Exclude<MessageTarget, Window>).postMessage(
            message,
            [transfer]
          )
        }
      } else {
        // If no target is provided, this might be an iframe or worker context

        if (typeof window === 'undefined' && typeof self !== 'undefined') {
          // In a worker context, we can use self directly
          ;(self as unknown as Worker).postMessage(message, [transfer])
        } else if (
          typeof window !== 'undefined' &&
          window.parent &&
          window.parent !== window
        ) {
          // Inside an iframe, we can use window.parent
          window.parent.postMessage(message, expectedOrigin || '*', [transfer])
        } else {
          throw new Error('No target provided and no global context available')
        }
      }
    }

    const messageChannel = new MessageChannel()

    const connectMessage = {
      type: 'bidc-connect',
      channelId,
      timestamp: Date.now(),
    } as const
    const confirmMessage = {
      type: 'bidc-confirm',
      channelId,
    } as const

    // Handle handshake requests
    function handleConnect(event: MessageEvent) {
      const port = event.ports[0] as MessagePort | undefined
      if (!port) return

      // Reject connections from any origin other than the expected one
      if (expectedOrigin && event.origin !== expectedOrigin) return

      const data = event.data as typeof connectMessage | typeof confirmMessage
      if (data?.channelId !== channelId) return
      if (data.type !== connectMessage.type) return

      // If the received connect message is older, ignore it. This is because
      // our connect message should be received already.
      // Let's wait for the confirmation message instead.
      if (connectMessage.timestamp <= data.timestamp) {
        // Send confirmation back to the other side via the port
        port.postMessage(confirmMessage)

        if (connected) {
          // The other side refreshed, we need to reinitialize the connection
          connectionCache.set(cacheKey, Promise.resolve(port))
          onResetPortCallbacks.forEach((callback) => callback(port))
        } else {
          // Connection sent from the other side
          connectionResolver!(port)
          connected = true
        }
      }
    }

    function handleConfirm(event: MessageEvent) {
      if (
        event.data?.type === confirmMessage.type &&
        event.data.channelId === channelId
      ) {
        // Confirm connection established
        connectionResolver!(messageChannel.port1)
        connected = true

        // Remove confirmation listener
        messageChannel.port1.removeEventListener('message', handleConfirm)
      }
    }

    // Listen for connect messages
    // These are registered once per target, but we can't unregister them
    // because we don't know when the target will be restarted or refreshed.
    if (
      maybeTarget &&
      typeof Worker !== 'undefined' &&
      maybeTarget instanceof Worker
    ) {
      maybeTarget.addEventListener('message', handleConnect)
    } else if (typeof window !== 'undefined') {
      window.addEventListener('message', handleConnect)
    } else if (typeof self !== 'undefined') {
      self.addEventListener('message', handleConnect)
    }

    // Listen for confirmation responses
    messageChannel.port1.addEventListener('message', handleConfirm)
    messageChannel.port1.start()

    // Try to connect to the other side
    sendMessageWithTransfer(connectMessage, messageChannel.port2)

    return connectionPromise
  }

  function getPort() {
    if (!connectionCache.has(cacheKey)) {
      connectionCache.set(cacheKey, initPort())
    }

    return connectionCache.get(cacheKey)!
  }

  const responses = new Map<
    string,
    [resolve: (value: any) => void, promise: Promise<any>]
  >()

  // Send function
  const send = async function <
    TReceiver,
    T = TReceiver extends (data: infer TT) => any ? TT : never,
    R = TReceiver extends (data: any) => infer TR
      ? TR
      : TReceiver extends (data: any) => Promise<infer TR>
      ? TR
      : never
  >(data: T): Promise<R> {
    // Wait for connection to be established
    const port = await getPort()

    // Generate a unique ID for this message
    // Ensure that fast concurrent messages don't collide
    const id =
      Date.now().toString(36) + Math.random().toString(36).substring(2, 5)

    // Send chunks with ID prefix for concurrent message support
    for await (const chunk of encode(data as any)) {
      // Prefix each chunk with <id>@ to support concurrent messages
      const prefixedChunk = `${id}@${chunk}`

      port.postMessage(prefixedChunk)
    }

    // Wait for a response from receive
    let resolve: (value: R) => void
    const response = new Promise<R>((r) => {
      // Store the response resolver
      resolve = r
    })
    responses.set(id, [resolve!, response])

    return response
  }

  // Track multiple concurrent decodings by message ID
  const activeDecodings = new Map<
    string,
    {
      pendingChunks: string[]
      chunkResolver: null | (() => void)
    }
  >()

  // Things to clean up when the channel is closed
  let canceled = false
  const disposables: (() => void)[] = []

  let globalReceiveCallback:
    | ((data: SerializableValue) => SerializableValue)
    | null = null

  // Receive function
  const receive = async function <
    T extends SerializableValue,
    R extends SerializableValue
  >(callback: (data: T) => R) {
    // Wait for connection to be established
    await getPort()
    if (canceled) return
    globalReceiveCallback = callback as any
  }

  // Automatically set up the message handler for the port
  getPort().then((activePort) => {
    if (canceled) return

    const messageHandler = async (event: MessageEvent) => {
      const rawChunk = event.data as string

      // Skip handshake messages
      if (typeof rawChunk !== 'string') {
        return
      }

      // Parse ID from chunk prefix: <id>@<chunk>
      const atIndex = rawChunk.indexOf('@')
      if (atIndex === -1) {
        console.error('Invalid chunk format - missing @ delimiter:', rawChunk)
        return
      }

      const messageId = rawChunk.slice(0, atIndex)
      const chunk = rawChunk.slice(atIndex + 1)

      // Check if this is the first chunk of a new message (starts with "r:")
      if (chunk.startsWith('r:')) {
        // Initialize tracking for this message ID
        activeDecodings?.set(messageId, {
          pendingChunks: [],
          chunkResolver: null,
        })

        // Start decoding immediately with async generator
        const processDecoding = async () => {
          try {
            const decoded = await decode(
              (async function* () {
                // Yield the first chunk immediately
                yield chunk

                // Wait for and yield subsequent chunks for this message ID
                while (true) {
                  const newChunk = await waitForNewChunkFromMessageEvent(
                    messageId
                  )
                  if (newChunk === null) {
                    break // End of stream
                  }
                  yield newChunk
                }

                // Clean up tracking for this message ID
                activeDecodings?.delete(messageId)
              })(),
              {
                send,
              }
            )

            // If the decoded data is a function call, we need to handle it
            // differently.
            if (
              typeof decoded === 'object' &&
              decoded !== null &&
              typeof decoded.$$type === 'string' &&
              decoded.$$type.startsWith('bidc-fn:')
            ) {
              // This is a function call, we need to resolve it
              const fnId = decoded.$$type.slice(8)
              const fn = functionRefsById.get(fnId)
              if (fn) {
                // Call the function with the provided arguments
                const response = fn(...decoded.args)
                void send({ $$type: `bidc-res:${messageId}`, response })
              } else {
                console.error(`Function reference not found for ID: ${fnId}`)
              }
            } else if (
              typeof decoded === 'object' &&
              decoded !== null &&
              typeof decoded.$$type === 'string' &&
              decoded.$$type.startsWith('bidc-res:')
            ) {
              const responseMessageId = decoded.$$type.slice(9)
              const response = decoded.response
              const responseResolver = responses.get(responseMessageId)
              if (responseResolver) {
                // Resolve the response promise with the decoded data
                responseResolver[0](response)
                responses.delete(responseMessageId)
              }
            } else {
              // Call the callback with the ID and decoded data
              if (!globalReceiveCallback) {
                throw new Error(
                  'Global receive callback is not set. This is a bug in BIDC.'
                )
              }
              try {
                const response = globalReceiveCallback(decoded)
                void send({ $$type: `bidc-res:${messageId}`, response })
              } catch (error) {
                console.error(error)
              }
            }
          } catch (error) {
            console.error(`Error decoding stream for ID ${messageId}:`, error)

            // Clean up tracking for this message ID
            activeDecodings?.delete(messageId)
          }
        }

        processDecoding()
      } else {
        // This is a continuation chunk for an existing message
        const decoding = activeDecodings?.get(messageId)
        if (decoding) {
          // Add chunk to pending queue and notify any waiting generators
          decoding.pendingChunks.push(chunk)
          decoding.chunkResolver?.()
        } else {
          console.warn(`No active decoding found for ID: ${messageId}`)
        }
      }
    }

    // Helper function to wait for new chunks from MessageEvents for a specific message ID
    async function waitForNewChunkFromMessageEvent(
      messageId: string
    ): Promise<string | null> {
      const decoding = activeDecodings?.get(messageId)
      if (!decoding) {
        return null
      }

      // If we have pending chunks, return the next one
      if (decoding.pendingChunks.length > 0) {
        const nextChunk = decoding.pendingChunks.shift()!
        return nextChunk
      }

      // Otherwise, wait for a new chunk to arrive
      await new Promise<void>((resolve) => {
        decoding.chunkResolver = resolve
      })

      if (decoding.pendingChunks.length === 0) {
        // If no chunks are pending, return null to indicate end of stream
        return null
      }
      return decoding.pendingChunks.shift()!
    }

    activePort.addEventListener('message', messageHandler)
    disposables.push(() => {
      activePort.removeEventListener('message', messageHandler)
    })

    // Start the MessagePort to begin receiving messages
    activePort.start()

    onResetPort((newPort) => {
      // If the port is reset, we need to reinitialize the connection
      if (canceled) return

      activePort.removeEventListener('message', messageHandler)
      activePort = newPort
      activePort.addEventListener('message', messageHandler)

      // Start the new port to begin receiving messages
      activePort.start()
    })
  })

  // Cleanup function to remove listeners and close ports
  const cleanup = () => {
    canceled = true
    disposables.forEach((dispose) => dispose())
    disposables.length = 0
  }

  return { send, receive, cleanup }
}

export { createChannel }
