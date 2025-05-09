const test = require('brittle')
const b4a = require('b4a')
const { create, collect, createCore } = require('./helpers')

const Spacebee = require('..')

test('basic properties', async function (t) {
  const db = await create(t)

  t.is(typeof db.replicate, 'function')

  t.is(db.id, null)
  t.is(db.key, null)
  t.is(db.discoveryKey, null)

  t.is(db.writable, false)
  t.is(db.readable, true)

  await db.ready()

  t.is(db.writable, true)

  t.is(db.id.length, 52)
  t.is(db.key.byteLength, 32)
  t.is(db.discoveryKey.byteLength, 32)

  t.is(db.id, db.core.id)
  t.is(db.key, db.core.key)
  t.is(db.discoveryKey, db.core.discoveryKey)
})

test('out of bounds iterator', async function (t) {
  const db = await create(t)

  const b = db.batch()

  await b.put('a', null)
  await b.put('b', null)
  await b.put('c', null)

  await b.flush()

  const s = db.createReadStream({ gt: b4a.from('c') })
  let count = 0

  s.on('data', function (data) {
    count++
  })

  return new Promise(resolve => {
    s.on('end', function () {
      t.is(count, 0, 'no out of bounds reads')
      resolve()
    })
  })
})

test('createHistoryStream reverse', async function (t) {
  const db = await create(t)

  const b = db.batch()

  await b.put('a', null)
  await b.put('b', null)
  await b.put('c', null)

  await b.flush()

  const s = db.createHistoryStream({ reverse: true })

  let res = ''
  s.on('data', function (data) {
    const { key } = data
    res += key
  })

  return new Promise(resolve => {
    s.on('end', function () {
      t.is(res, 'cba', 'reversed correctly')
      resolve()
    })
  })
})

test('out of bounds iterator, string encoding', async function (t) {
  const db = await create(t, { keyEncoding: 'utf8' })

  const b = db.batch()

  await b.put('a', null)
  await b.put('b', null)
  await b.put('c', null)

  await b.flush()

  const s = db.createReadStream({ gte: 'f' })
  let count = 0

  s.on('data', function (data) {
    count++
  })

  return new Promise(resolve => {
    s.on('end', function () {
      t.is(count, 0, 'no out of bounds reads')
      resolve()
    })
  })
})

test('out of bounds iterator, larger db', async function (t) {
  const db = await create(t, { keyEncoding: 'utf8' })

  for (let i = 0; i < 8; i++) {
    await db.put('' + i, 'hello world')
  }

  const s = db.createReadStream({ gte: 'a' })
  let count = 0

  s.on('data', function (data) {
    count++
  })

  return new Promise(resolve => {
    s.on('end', function () {
      t.is(count, 0, 'no out of bounds reads')
      resolve()
    })
  })
})

test('test all short iterators', async function (t) {
  const db = await create(t, { keyEncoding: 'utf8' })

  const MAX = 25

  for (let size = 1; size <= MAX; size++) {
    const reference = []
    for (let i = 0; i < size; i++) {
      const key = '' + i
      await db.put(key, 'hello world')
      reference.push(key)
    }
    reference.sort()

    for (let i = 0; i < size; i++) {
      for (let j = 0; j <= i; j++) {
        for (let k = 0; k < 8; k++) {
          const greater = (k & 1) ? 'gte' : 'gt'
          const lesser = (k >> 1 & 1) ? 'lte' : 'lt'
          const reverse = !!(k >> 2 & 1)
          const opts = {
            [greater]: '' + j,
            [lesser]: '' + i,
            reverse
          }
          const entries = await collect(db.createReadStream(opts))
          if (!validate(size, reference, opts, entries)) {
            return
          }
        }
      }
    }
  }

  t.pass('all iterations passed')

  function validate (size, reference, opts, entries) {
    const start = opts.gt ? reference.indexOf(opts.gt) + 1 : reference.indexOf(opts.gte)
    const end = opts.lt ? reference.indexOf(opts.lt) : reference.indexOf(opts.lte) + 1
    const range = reference.slice(start, end)
    if (opts.reverse) range.reverse()
    for (let i = 0; i < range.length; i++) {
      if (!entries[i] || range[i] !== entries[i].key) {
        console.log('========')
        console.log('SIZE:', size)
        console.log('FAILED WITH OPTS:', opts)
        console.log('  expected:', range, 'start:', start, 'end:', end)
        console.log('  actual:', entries.map(e => e.key))
        t.fail('ranges did not match')
        return false
      }
    }
    return true
  }
})

test('test all short iterators, sub database', async function (t) {
  const parent = await create(t, { keyEncoding: 'utf8' })
  const db = parent.sub('sub1')

  const MAX = 25

  for (let size = 1; size <= MAX; size++) {
    const reference = []
    for (let i = 0; i < size; i++) {
      const key = '' + i
      await db.put(key, 'hello world')
      await parent.put(key, 'parent hello world')
      reference.push(key)
    }
    reference.sort()

    for (let i = 0; i < size; i++) {
      for (let j = 0; j <= i; j++) {
        for (let k = 0; k < 8; k++) {
          const greater = (k & 1) ? 'gte' : 'gt'
          const lesser = (k >> 1 & 1) ? 'lte' : 'lt'
          const reverse = !!(k >> 2 & 1)
          const opts = {
            [greater]: '' + j,
            [lesser]: '' + i,
            reverse
          }
          const entries = await collect(db.createReadStream(opts))
          if (!validate(size, reference, opts, entries)) {
            return
          }
        }
      }
    }
  }

  t.pass('all iterations passed')

  await db.close()

  function validate (size, reference, opts, entries) {
    const start = opts.gt ? reference.indexOf(opts.gt) + 1 : reference.indexOf(opts.gte)
    const end = opts.lt ? reference.indexOf(opts.lt) : reference.indexOf(opts.lte) + 1
    const range = reference.slice(start, end)
    if (opts.reverse) range.reverse()
    for (let i = 0; i < range.length; i++) {
      if (!entries[i] || range[i] !== entries[i].key) {
        console.log('========')
        console.log('SIZE:', size)
        console.log('FAILED WITH OPTS:', opts)
        console.log('  expected:', range, 'start:', start, 'end:', end)
        console.log('  actual:', entries.map(e => e.key))
        t.fail('ranges did not match')
        return false
      }
    }
    return true
  }
})

test('custom key/value encodings in get/put', async function (t) {
  const db = await create(t)
  await db.put(b4a.from('hello'), b4a.from('world'), {
    keyEncoding: 'binary',
    valueEncoding: 'binary'
  })
  const node = await db.get(b4a.from('hello'), {
    keyEncoding: 'binary',
    valueEncoding: 'binary'
  })
  t.alike(node.key, b4a.from('hello'))
  t.alike(node.value, b4a.from('world'))
})

test('custom key/value encodings in range iterator', async function (t) {
  const db = await create(t)
  await db.put(b4a.from('hello1'), b4a.from('world1'), {
    keyEncoding: 'binary',
    valueEncoding: 'binary'
  })
  await db.put(b4a.from('hello2'), b4a.from('world2'), {
    keyEncoding: 'binary',
    valueEncoding: 'binary'
  })

  const s = db.createReadStream({
    gt: b4a.from('hello1'),
    keyEncoding: 'binary',
    valueEncoding: 'binary'
  })
  let count = 0
  let node = null

  s.on('data', function (data) {
    count++
    node = data
  })

  await new Promise(resolve => s.on('end', resolve))

  t.is(count, 1)
  t.alike(node.key, b4a.from('hello2'))
  t.alike(node.value, b4a.from('world2'))
})

test('simple sub put/get', async function (t) {
  const db = await create(t)
  const sub = db.sub('hello')
  await sub.put('world', 'hello world')
  const node = await sub.get('world')
  t.is(node && node.key, 'world')
  t.is(node && node.value, 'hello world')

  await sub.close()
})

test('multiple levels of sub', async function (t) {
  const db = await create(t, { sep: '!' })
  const a = db.sub('hello')
  const sub = a.sub('world')
  await sub.put('a', 'b')

  const encoded = sub.keyEncoding.encode('a')

  {
    const node = await sub.get('a')
    t.is(node && node.key, 'a')
    t.is(node && node.value, 'b')
  }

  {
    const node = await db.get(encoded)
    t.is(node && node.key, b4a.toString(encoded, 'utf-8'))
    t.is(node && node.value, 'b')
  }

  {
    const key = 'hello' + db.sep + 'world' + db.sep + 'a'
    t.is(key, b4a.toString(encoded, 'utf-8'))
    const node = await db.get(key)
    t.is(node && node.key, key)
    t.is(node && node.value, 'b')
  }

  await a.close()
  await sub.close()
})

test('multiple levels of sub, entries outside sub', async function (t) {
  const db = await create(t, { sep: '!' })
  const helloSub = db.sub('hello')
  const worldSub = helloSub.sub('world')
  await helloSub.put('a', 'b')
  await worldSub.put('b', 'c')

  const expected = [['b', 'c']]
  for await (const { key, value } of worldSub.createReadStream()) {
    const next = expected.shift()
    if (!next) {
      t.fail('iterated unexpected value')
      break
    }
    t.is(key, next[0])
    t.is(value, next[1])
  }
  t.is(expected.length, 0)

  await helloSub.close()
  await worldSub.close()
})

test('sub respects keyEncoding', async function (t) {
  t.plan(2)

  const db = await create(t, { sep: '!' })
  const helloSub = db.sub('hello', {
    keyEncoding: {
      encode (key) {
        return b4a.from(key.key)
      },
      decode (buf) {
        return { key: b4a.toString(buf) }
      }
    }
  })

  await helloSub.put({ key: 'hello' }, 'val')

  for await (const data of helloSub.createReadStream()) {
    t.alike(data.key, { key: 'hello' })
  }

  const node = await helloSub.get({ key: 'hello' })

  t.ok(node)

  await helloSub.close()
})

test('sub with a key that starts with 0xff', async function (t) {
  t.plan(2)

  const db = await create(t, { sep: '!', keyEncoding: 'binary' })
  const helloSub = db.sub('hello')
  const key = b4a.from([0xff, 0x01, 0x02])

  await helloSub.put(key, 'val')

  for await (const data of helloSub.createReadStream()) {
    t.alike(data.key, key)
  }

  const node = await helloSub.get(key)

  t.ok(node)

  await helloSub.close()
})

test('read stream on sub checkout returns only sub keys', async function (t) {
  t.plan(3)

  const db = await create(t, { sep: '!', keyEncoding: 'utf-8' })
  const sub = db.sub('sub')

  await db.put('a', 'a')
  await sub.put('sa', 'sa')
  await sub.put('sb', 'sb')

  const checkout = sub.snapshot()

  await db.put('b', 'b')

  const keys = []
  for await (const { key } of checkout.createReadStream()) {
    keys.push(key)
  }

  t.is(keys.length, 2)
  t.is(keys[0], 'sa')
  t.is(keys[1], 'sb')

  await sub.close()
  await checkout.close()
})

test('read stream on double sub checkout', async function (t) {
  t.plan(3)

  const db = await create(t, { sep: '!', keyEncoding: 'utf-8' })
  const sub = db.sub('sub')

  await db.put('a', 'a')
  await sub.put('sa', 'sa')
  await sub.put('sb', 'sb')

  const a = sub.snapshot()
  const checkout = a.snapshot()

  await db.put('b', 'b')

  const keys = []
  for await (const { key } of checkout.createReadStream()) {
    keys.push(key)
  }

  t.is(keys.length, 2)
  t.is(keys[0], 'sa')
  t.is(keys[1], 'sb')

  await sub.close()
  await checkout.close()
  await a.close()
})

test('no session leak after read stream closes', async function (t) {
  const db = await create(t)
  await db.put('e1', 'entry1')
  await db.put('e2', 'entry2')

  const checkout = db.checkout(2)

  const nrSessions = db.core.sessions.length
  const stream = db.createReadStream()
  const stream2 = checkout.createReadStream()

  const entries = await collect(stream)
  await collect(stream2)

  t.is(entries.length, 2) // Sanity check
  t.is(nrSessions, db.core.sessions.length)

  await checkout.close()
})

test('setting read-only flag to false disables header write', async function (t) {
  const db = await create(t, { readonly: true })
  await db.ready()
  t.is(db.core.length, 0)
  t.ok(db.readonly)
})

test('cannot append to read-only db', async function (t) {
  const db = await create(t, { readonly: true })
  await db.ready()
  await t.exception(() => db.put('hello', 'world'))
})

test('get header out', async function (t) {
  const db = await create(t)
  await db.ready()
  await db.put('hi', 'ho')
  const h = await db.getHeader()
  t.is(h.protocol, 'spacebee')
})

test('isSpacebee throws for empty spacecore and wait false', async function (t) {
  const core = await createCore(t)
  await t.exception(Spacebee.isSpacebee(core, { wait: false }), 'Block is not available')
})

test('isSpacebee is false for non-empty spacecore', async function (t) {
  const core = await createCore(t)
  await core.append('something')
  t.is(await Spacebee.isSpacebee(core), false)
})

test('isSpacebee is false for spacecore with 1st entry spacebee', async function (t) {
  const core = await createCore(t)
  await core.append('spacebee')
  t.is(await Spacebee.isSpacebee(core), false)
})

test('isSpacebee is true for core of actual spacebee', async function (t) {
  const db = await create(t)
  await db.put('hi', 'ho') // Adds the header on the first put
  t.ok(await Spacebee.isSpacebee(db.core))
})

test('supports encodings in checkout', async function (t) {
  const db = await create(t)
  await db.put('hi', 'there')

  const checkout1 = db.checkout(db.version, { keyEncoding: 'binary' })
  const checkout2 = db.checkout(db.version, { valueEncoding: 'binary' })

  t.alike(await checkout1.get('hi'), { seq: 1, key: b4a.from('hi'), value: 'there' })
  t.alike(await checkout2.get('hi'), { seq: 1, key: 'hi', value: b4a.from('there') })

  await checkout1.close()
  await checkout2.close()
})

test('supports encodings in snapshot', async function (t) {
  const db = await create(t)
  await db.put('hi', 'there')

  const snap1 = db.snapshot({ keyEncoding: 'binary' })
  const snap2 = db.snapshot({ valueEncoding: 'binary' })

  t.alike(await snap1.get('hi'), { seq: 1, key: b4a.from('hi'), value: 'there' })
  t.alike(await snap2.get('hi'), { seq: 1, key: 'hi', value: b4a.from('there') })

  await snap1.close()
  await snap2.close()
})

test('get by seq', async function (t) {
  const db = await create(t)

  await db.put('/a', '1')
  await db.put('/b', '2')

  t.alike(await db.getBySeq(1), { key: '/a', value: '1' })
  t.alike(await db.getBySeq(2), { key: '/b', value: '2' })
})
