import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { createChannel, encode, decode } from '../index'

// Helper to convert async iterator to array
async function collectAsyncIterator<T>(iter: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = []
  for await (const chunk of iter) {
    result.push(chunk)
  }
  return result
}

// Helper to create async iterable from array
async function* arrayToAsyncIterable<T>(array: T[]): AsyncGenerator<T> {
  for (const item of array) {
    yield item
  }
}

// Helper to simulate message passing between two contexts
function createMockMessageChannel() {
  const listeners1: ((event: MessageEvent) => void)[] = []
  const listeners2: ((event: MessageEvent) => void)[] = []

  const port1 = {
    postMessage: (data: any) => {
      listeners2.forEach((listener) => {
        listener(new MessageEvent('message', { data }))
      })
    },
    addEventListener: (
      type: string,
      listener: (event: MessageEvent) => void
    ) => {
      if (type === 'message') listeners1.push(listener)
    },
    removeEventListener: (
      type: string,
      listener: (event: MessageEvent) => void
    ) => {
      if (type === 'message') {
        const index = listeners1.indexOf(listener)
        if (index > -1) listeners1.splice(index, 1)
      }
    },
    start: vi.fn(),
  }

  const port2 = {
    postMessage: (data: any) => {
      listeners1.forEach((listener) => {
        listener(new MessageEvent('message', { data }))
      })
    },
    addEventListener: (
      type: string,
      listener: (event: MessageEvent) => void
    ) => {
      if (type === 'message') listeners2.push(listener)
    },
    removeEventListener: (
      type: string,
      listener: (event: MessageEvent) => void
    ) => {
      if (type === 'message') {
        const index = listeners2.indexOf(listener)
        if (index > -1) listeners2.splice(index, 1)
      }
    },
    start: vi.fn(),
  }

  return { port1, port2 }
}

describe('encode/decode', () => {
  it('should encode and decode primitive values', async () => {
    const values = ['hello', 42, true, null, undefined]

    for (const value of values) {
      const chunks = await collectAsyncIterator(encode(value))

      // Each chunk should be a string.
      chunks.forEach((chunk) => expect(chunk).toBeTypeOf('string'))
      expect(chunks.length).toBe(1)

      const decoded = await decode(arrayToAsyncIterable(chunks))
      expect(decoded).toBe(value)
    }
  })

  it('should encode and decode complex types', async () => {
    const array = [1, 'two', { three: 3 }, [4, 5]]
    const date = new Date('2024-01-01')
    const map = new Map<any, any>([
      ['key', 'value'],
      ['num', 42],
    ])
    const set = new Set([1, 2, 3])
    const regex = /test.*pattern/gi
    const bigint = 12345678901234567890n
    const buffer = new ArrayBuffer(8)
    const uint8 = new Uint8Array([1, 2, 3, 4])

    const complexData = {
      array,
      date,
      map,
      set,
      regex,
      bigint,
      buffer,
      uint8,
    }

    const chunks = await collectAsyncIterator(encode(complexData))

    expect(chunks).toMatchInlineSnapshot(`
      [
        "r:[{"array":1,"date":9,"map":10,"set":15,"regex":17,"bigint":18,"buffer":19,"uint8":20},[2,3,4,6],1,"two",{"three":5},3,[7,8],4,5,["Date","2024-01-01T00:00:00.000Z"],["Map",11,12,13,14],"key","value","num",42,["Set",2,16,5],2,["RegExp","test.*pattern","gi"],["BigInt","12345678901234567890"],["ArrayBuffer","AAAAAAAAAAA="],["Uint8Array",21],["ArrayBuffer","AQIDBA=="]]
      ",
      ]
    `)

    const decoded = await decode(arrayToAsyncIterable(chunks))

    expect(decoded.array).toEqual(array)
    expect(decoded.date).toEqual(date)
    expect(decoded.map).toEqual(map)
    expect(decoded.set).toEqual(set)
    expect(decoded.regex).toEqual(regex)
    expect(decoded.bigint).toBe(bigint)
    expect(decoded.buffer).toBeInstanceOf(ArrayBuffer)
    expect(decoded.buffer.byteLength).toBe(8)
    expect(decoded.uint8).toEqual(uint8)
  })

  it('should handle promises', async () => {
    const value = {
      immediate: 'sync',
      delayed: Promise.resolve('async value'),
    }

    const chunks = await collectAsyncIterator(encode(value))
    expect(chunks).toMatchInlineSnapshot(`
      [
        "r:[{"immediate":1,"delayed":2},"sync",["P",3],"0"]
      ",
        "p0:["async value"]
      ",
      ]
    `)

    const decoded = await decode(arrayToAsyncIterable(chunks))
    expect(decoded.immediate).toBe('sync')
    expect(decoded.delayed).toBeInstanceOf(Promise)
    await expect(decoded.delayed).resolves.toBe('async value')
  })

  it('should handle promise rejections', async () => {
    const value = {
      willFail: Promise.reject(new Error('Test error')),
    }

    const chunks = await collectAsyncIterator(encode(value))
    expect(chunks).toMatchInlineSnapshot(`
      [
        "r:[{"willFail":1},["P",2],"0"]
      ",
        "e0:["Test error"]
      ",
      ]
    `)

    const decoded = await decode(arrayToAsyncIterable(chunks))
    expect(decoded.willFail).toBeInstanceOf(Promise)
    await expect(decoded.willFail).rejects.toThrow('Test error')
  })

  it('should handle nested promises', async () => {
    const value = {
      level1: Promise.resolve({
        level2: Promise.resolve({
          level3: Promise.resolve('deeply nested'),
        }),
      }),
    }

    const chunks = await collectAsyncIterator(encode(value))
    expect(chunks).toMatchInlineSnapshot(`
      [
        "r:[{"level1":1},["P",2],"0"]
      ",
        "p0:[{"level2":1},["P",2],"1"]
      ",
        "p1:[{"level3":1},["P",2],"2"]
      ",
        "p2:["deeply nested"]
      ",
      ]
    `)

    const decoded = await decode(arrayToAsyncIterable(chunks))
    const level1 = await decoded.level1
    const level2 = await level1.level2
    const level3 = await level2.level3
    expect(level3).toBe('deeply nested')
  })

  it('should handle circular promises', async () => {
    let resolve
    const promise = new Promise<{ promise: Promise<any> }>((res) => {
      resolve = res
    })
    resolve({ promise, data: 'hello' })

    const chunks = await collectAsyncIterator(encode({ promise }))
    expect(chunks).toMatchInlineSnapshot(`
      [
        "r:[{"promise":1},["P",2],"0"]
      ",
        "p0:[{"promise":1,"data":3},["P",2],"0","hello"]
      ",
      ]
    `)

    const decoded = await decode(arrayToAsyncIterable(chunks))
    const result = await decoded.promise
    expect(result).toEqual({ promise: decoded.promise, data: 'hello' })
    const nestedResult = await result.promise
    expect(nestedResult).toEqual({ promise: decoded.promise, data: 'hello' })
  })
})

describe('createChannel integration tests', () => {
  let mockParent: any
  let mockIframe: any
  let originalWorker = global.Worker

  class MockWorker {
    listeners1: ((event: MessageEvent) => void)[]
    listeners2: ((event: MessageEvent) => void)[]

    constructor(
      listners1: ((event: MessageEvent) => void)[],
      listners2: ((event: MessageEvent) => void)[]
    ) {
      this.listeners1 = listners1
      this.listeners2 = listners2
    }

    postMessage(message: any, transfer?: any[]) {
      this.listeners2.forEach((listener) => {
        listener(
          new MessageEvent('message', {
            data: message,
            ports: transfer || [],
          })
        )
      })
    }

    addEventListener(type: string, listener: any) {
      if (type === 'message') {
        this.listeners1.push(listener)
      }
    }
  }

  beforeEach(() => {
    global.Worker = MockWorker as any

    // Create mock windows that can communicate
    const messageListeners1: ((event: MessageEvent) => void)[] = []
    const messageListeners2: ((event: MessageEvent) => void)[] = []

    mockParent = new MockWorker(messageListeners1, messageListeners2)
    mockIframe = new MockWorker(messageListeners2, messageListeners1)
  })

  afterEach(() => {
    // Restore original Worker
    global.Worker = originalWorker

    vi.clearAllMocks()
  })

  it('should establish basic connection and send data', async () => {
    const channel1 = createChannel(mockIframe)
    const channel2 = createChannel(mockParent)

    // Set up receiver
    const receiver = (data: { value: string }) => {
      return data.value.toUpperCase()
    }
    channel2.receive(receiver)

    // Send data from parent
    const result = await channel1.send<typeof receiver>({ value: 'hello' })
    expect(result).toMatchInlineSnapshot(`"HELLO"`)

    // Cleanup
    channel1.cleanup()
    channel2.cleanup()
  })

  it('should handle two-way communication', async () => {
    const channel1 = createChannel(mockIframe)
    const channel2 = createChannel(mockParent)

    // Set up receivers on both sides
    channel1.receive((data: any) => {
      return { from: 'parent', echo: data }
    })

    channel2.receive((data: any) => {
      return { from: 'iframe', echo: data }
    })

    // Send from parent to iframe
    const result1 = await channel1.send({ message: 'hello iframe' })
    expect(result1).toMatchInlineSnapshot(`
      {
        "echo": {
          "message": "hello iframe",
        },
        "from": "iframe",
      }
    `)

    // Send from iframe to parent
    const result2 = await channel2.send({ message: 'hello parent' })
    expect(result2).toMatchInlineSnapshot(`
      {
        "echo": {
          "message": "hello parent",
        },
        "from": "parent",
      }
    `)

    // Cleanup
    channel1.cleanup()
    channel2.cleanup()
  })

  it('should handle complex data types through channel', async () => {
    const channel1 = createChannel(mockIframe)
    const channel2 = createChannel(mockParent)

    channel2.receive(async (data: any) => {
      // Verify complex types are preserved
      expect(data.date).toBeInstanceOf(Date)
      expect(data.map).toBeInstanceOf(Map)
      expect(data.set).toBeInstanceOf(Set)
      expect(data.buffer).toBeInstanceOf(ArrayBuffer)

      const promiseValue = await data.promise
      expect(promiseValue).toBe('resolved value')

      const fnResult = await data.function(10)
      expect(fnResult).toBe(20)

      return { success: true }
    })

    const complexData = {
      date: new Date('2024-01-01'),
      map: new Map([['key', 'value']]),
      set: new Set([1, 2, 3]),
      buffer: new Uint8Array([1, 2, 3]).buffer,
      promise: Promise.resolve('resolved value'),
      function: async (x: number) => x * 2,
    }

    const result = await channel1.send(complexData)
    expect(result).toMatchInlineSnapshot(`
      {
        "success": true,
      }
    `)

    // Cleanup
    channel1.cleanup()
    channel2.cleanup()
  })

  it('should handle concurrent messages', async () => {
    const channel1 = createChannel(mockIframe)
    const channel2 = createChannel(mockParent)

    channel2.receive(async (data: any) => {
      // Simulate different processing times
      await new Promise((resolve) => setTimeout(resolve, data.delay))
      return { id: data.id, processed: true, delay: data.delay }
    })

    // Send multiple messages concurrently
    const promises = [
      channel1.send({ id: 1, delay: 50 }),
      channel1.send({ id: 2, delay: 10 }),
      channel1.send({ id: 3, delay: 30 }),
    ]

    const results = await Promise.all(promises)

    // All should complete successfully despite different delays
    expect(results).toMatchInlineSnapshot(`
      [
        {
          "delay": 50,
          "id": 1,
          "processed": true,
        },
        {
          "delay": 10,
          "id": 2,
          "processed": true,
        },
        {
          "delay": 30,
          "id": 3,
          "processed": true,
        },
      ]
    `)

    // Cleanup
    channel1.cleanup()
    channel2.cleanup()
  })

  it('should handle messages with nested async functions', async () => {
    const channel1 = createChannel(mockIframe)
    const channel2 = createChannel(mockParent)

    const receiver = async ({
      mul,
    }: {
      mul: (a: number, b: number) => Promise<number>
    }) => {
      return async (x: number, inc: (y: number) => Promise<number>) => {
        return await inc(await mul(x, 2))
      }
    }

    channel2.receive(receiver as any)

    const mul2 = await channel1.send<typeof receiver>({
      mul: async (a: number, b: number) => a * b,
    })

    expect(await mul2(4, async (x) => x + 1)).toMatchInlineSnapshot(`9`)
  })
})

describe('origin validation', () => {
  function createMockWindowTarget() {
    const target: any = { self: null as any }
    target.self = target
    target.postMessage = vi.fn()
    return target
  }

  function mockPort() {
    return {
      postMessage: vi.fn(),
      start: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    }
  }

  it('should reject connections from an origin other than the declared one', async () => {
    const mockTarget = createMockWindowTarget()
    const channel = createChannel(
      mockTarget,
      'reject-test',
      'https://trusted.com'
    )

    const port = mockPort()
    window.dispatchEvent(
      new MessageEvent('message', {
        data: {
          type: 'bidc-connect',
          channelId: 'bidc_reject-test',
          timestamp: Date.now() + 1000,
        },
        origin: 'https://evil.com',
        ports: [port] as any,
      })
    )

    await new Promise((r) => setTimeout(r, 50))
    expect(port.postMessage).not.toHaveBeenCalled()

    channel.cleanup()
  })

  it('should accept connections from the declared origin', async () => {
    const mockTarget = createMockWindowTarget()
    const channel = createChannel(
      mockTarget,
      'accept-test',
      'https://trusted.com'
    )

    const port = mockPort()
    window.dispatchEvent(
      new MessageEvent('message', {
        data: {
          type: 'bidc-connect',
          channelId: 'bidc_accept-test',
          timestamp: Date.now() + 1000,
        },
        origin: 'https://trusted.com',
        ports: [port] as any,
      })
    )

    await new Promise((r) => setTimeout(r, 50))
    expect(port.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'bidc-confirm' })
    )

    channel.cleanup()
  })

  it('should accept any origin when none is declared (back-compat)', async () => {
    const mockTarget = createMockWindowTarget()
    const channel = createChannel(mockTarget, 'no-origin-test')

    const port = mockPort()
    window.dispatchEvent(
      new MessageEvent('message', {
        data: {
          type: 'bidc-connect',
          channelId: 'bidc_no-origin-test',
          timestamp: Date.now() + 1000,
        },
        origin: 'https://any-origin.com',
        ports: [port] as any,
      })
    )

    await new Promise((r) => setTimeout(r, 50))
    expect(port.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'bidc-confirm' })
    )

    channel.cleanup()
  })

  it('should use declared origin as targetOrigin in postMessage', () => {
    const mockTarget = createMockWindowTarget()
    const channel = createChannel(
      mockTarget,
      'target-origin-test',
      'https://trusted.com'
    )

    expect(mockTarget.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'bidc-connect' }),
      'https://trusted.com',
      expect.any(Array)
    )

    channel.cleanup()
  })

  it('should use * as targetOrigin when no origin is declared', () => {
    const mockTarget = createMockWindowTarget()
    const channel = createChannel(mockTarget, 'wildcard-test')

    expect(mockTarget.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'bidc-connect' }),
      '*',
      expect.any(Array)
    )

    channel.cleanup()
  })

  it('should validate origin without a target (iframe context)', async () => {
    // Simulate an iframe context where window.parent is the embedding page
    const fakeParent = { postMessage: vi.fn() }
    Object.defineProperty(window, 'parent', {
      value: fakeParent,
      configurable: true,
    })

    try {
      const channel = createChannel('iframe-origin-test', 'https://parent.com')

      // Outbound connect message is restricted to the declared origin
      expect(fakeParent.postMessage).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'bidc-connect' }),
        'https://parent.com',
        expect.any(Array)
      )

      // Inbound connect from a different origin is rejected
      const port = mockPort()
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            type: 'bidc-connect',
            channelId: 'bidc_iframe-origin-test',
            timestamp: Date.now() + 1000,
          },
          origin: 'https://evil.com',
          ports: [port] as any,
        })
      )

      await new Promise((r) => setTimeout(r, 50))
      expect(port.postMessage).not.toHaveBeenCalled()

      channel.cleanup()
    } finally {
      Object.defineProperty(window, 'parent', {
        value: window,
        configurable: true,
      })
    }
  })
})

describe('edge cases', () => {
  it('should handle circular references with promises', async () => {
    const obj: any = { name: 'circular' }
    obj.self = Promise.resolve(obj)

    const chunks = await collectAsyncIterator(encode(obj))
    const decoded = await decode(arrayToAsyncIterable(chunks))

    expect(decoded.name).toBe('circular')
    const resolved = await decoded.self
    expect(resolved.name).toBe('circular')
  })

  it('should handle very large data sets', async () => {
    const largeArray = new Array(1000).fill(null).map((_, i) => ({
      id: i,
      data: `item-${i}`,
      nested: {
        value: i * 2,
        promise: Promise.resolve(`async-${i}`),
      },
    }))

    const chunks = await collectAsyncIterator(encode(largeArray))
    const decoded = await decode(arrayToAsyncIterable(chunks))

    expect(decoded.length).toBe(1000)
    expect(decoded[500].id).toBe(500)
    await expect(decoded[500].nested.promise).resolves.toBe('async-500')
  })

  it('should handle empty objects and arrays', async () => {
    const empty = {
      obj: {},
      arr: [],
      map: new Map(),
      set: new Set(),
    }

    const chunks = await collectAsyncIterator(encode(empty))
    const decoded = await decode(arrayToAsyncIterable(chunks))

    expect(decoded.obj).toEqual({})
    expect(decoded.arr).toEqual([])
    expect(decoded.map).toEqual(new Map())
    expect(decoded.set).toEqual(new Set())
  })

  it('should handle special number values', async () => {
    const special = {
      infinity: Infinity,
      negInfinity: -Infinity,
      nan: NaN,
      zero: 0,
      negZero: -0,
    }

    const chunks = await collectAsyncIterator(encode(special))
    const decoded = await decode(arrayToAsyncIterable(chunks))

    expect(decoded.infinity).toBe(Infinity)
    expect(decoded.negInfinity).toBe(-Infinity)
    expect(decoded.nan).toBeNaN()
    expect(decoded.zero).toBe(0)
    // Note: devalue may not preserve negative zero distinction
    expect(decoded.negZero === 0 || Object.is(decoded.negZero, -0)).toBe(true)
  })
})
