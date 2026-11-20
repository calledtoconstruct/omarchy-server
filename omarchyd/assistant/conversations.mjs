import {AsyncLocalStorage} from 'node:async_hooks'
import {constants} from 'node:fs'
import {lstat, mkdir, open, readdir, realpath, rm} from 'node:fs/promises'
import {randomBytes} from 'node:crypto'
import {basename, join} from 'node:path'

import {removeAtomic, writeAtomic} from '../atomic.mjs'
import {RpcError} from '../rpc.mjs'

const clipTitle = (s) => String(s ?? '').replace(/\s+/g, ' ').trim().slice(0, 48) || 'Chat'
const ID = () => 'c_' + randomBytes(6).toString('hex')
const AID = () => 'a_' + randomBytes(6).toString('hex')
// Generated ids only. The id is a path segment, and the id stored in the file is not trusted.
const CHAT_ID = /^c_[0-9a-f]{12}$/
const ACTION_ID = /^a_[0-9a-f]{12}$/
const missingChat = () => new RpcError('NOT_FOUND', 'That chat isn’t here')
const missingStep = () => new RpcError('NOT_FOUND', 'That step isn’t here')

// One read-modify-write at a time per chat, so a snapshot from before Approve cannot land after it.
// Re-entrant so writeRow can take the same lock the caller already holds.
const locks = new Map()
const held = new AsyncLocalStorage()
function withLock(id, fn) {
	const owned = held.getStore()
	if (owned?.has(id)) return fn()
	const prev = locks.get(id) ?? Promise.resolve()
	const run = prev.then(
		() => {
			const next = new Set(owned)
			next.add(id)
			return held.run(next, fn)
		},
		() => {
			const next = new Set(owned)
			next.add(id)
			return held.run(next, fn)
		},
	)
	const settled = run.then(() => {}, () => {})
	locks.set(id, settled)
	settled.then(() => locks.get(id) === settled && locks.delete(id))
	return run
}

// A late save from an earlier snapshot must not move a step backwards.
function canReplace(current, next) {
	const from = current?.status
	const to = next?.status
	if (from === to) return true
	if (from === 'pending' && (to === 'running' || to === 'rejected' || to === 'error' || to === 'done')) return true
	if (from === 'running' && (to === 'done' || to === 'error')) return true
	return false
}

export function createConversations({dataDir, now = () => Date.now()}) {
	const dir = join(dataDir, 'assistant', 'conversations')
	const loaded = mkdir(dir, {recursive: true, mode: 0o700})
	// Factory reset bumps this before deleting. A write records the generation when it is queued
	// and writes nothing if a reset has moved on, so the queued write cannot recreate the chat.
	let epoch = 0
	// Paths whose write has been queued but may not be renamed yet, so reset can delete them on that same queue.
	const writing = new Set()
	let dirReal = null
	const root = async () => {
		await loaded
		dirReal ??= await realpath(dir)
		return dirReal
	}
	// The path is the real conversations directory plus a safe file name. Symlinks are refused.
	async function fileFor(id) {
		if (typeof id !== 'string' || !CHAT_ID.test(id)) throw missingChat()
		const base = await root()
		const file = join(base, `${id}.json`)
		if ((await realpath(join(file, '..'))) !== base) throw missingChat()
		try {
			const st = await lstat(file)
			if (!st.isFile()) throw missingChat()
		} catch (error) {
			if (error instanceof RpcError) throw error
			if (error.code !== 'ENOENT') throw missingChat()
		}
		return file
	}
	async function readText(file) {
		let fh
		try {
			fh = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW)
		} catch {
			throw missingChat()
		}
		try {
			return await fh.readFile({encoding: 'utf8'})
		} finally {
			await fh.close()
		}
	}
	async function readOwned(userId, id) {
		const file = await fileFor(id)
		let row
		try {
			row = JSON.parse(await readText(file))
		} catch (error) {
			if (error instanceof RpcError) throw error
			throw missingChat()
		}
		if (!row || typeof row !== 'object' || Array.isArray(row) || row.userId !== userId) throw missingChat()
		row.id = id
		if (!Array.isArray(row.messages)) row.messages = []
		if (!Array.isArray(row.actions)) row.actions = []
		for (const action of row.actions) if (action && typeof action === 'object') action.conversationId = id
		return row
	}
	async function writeRow(id, row) {
		return withLock(id, async () => {
			// Taken before the path check awaits, so a reset in that gap still belongs to this write.
			const seen = epoch
			const file = await fileFor(id)
			row.id = id
			writing.add(file)
			try {
				await writeAtomic(file, () => (seen !== epoch ? null : JSON.stringify(row, null, '\t')), {mode: 0o600})
			} finally {
				writing.delete(file)
			}
		})
	}
	async function findAction(userId, actionId) {
		if (typeof actionId !== 'string' || !ACTION_ID.test(actionId)) throw missingStep()
		const base = await root()
		for (const name of await readdir(base).catch(() => [])) {
			if (!name.endsWith('.json')) continue
			const id = name.slice(0, -5)
			if (!CHAT_ID.test(id)) continue
			const row = await readOwned(userId, id).catch(() => null)
			if (!row) continue
			const action = row.actions.find((x) => x && x.id === actionId)
			if (action) return {row, action}
		}
		throw missingStep()
	}

	return {
		// Factory reset: every saved chat goes. The folder stays. A write already queued does not put the file back.
		async reset() {
			epoch++
			const base = await root()
			const files = new Set(writing)
			for (const name of await readdir(base).catch(() => [])) {
				if (name.endsWith('.json')) files.add(join(base, name))
			}
			for (const file of files) {
				const id = basename(file).slice(0, -5)
				const drop = () => removeAtomic(file)
				if (CHAT_ID.test(id)) await withLock(id, drop)
				else await drop()
			}
		},
		async list(userId) {
			const base = await root()
			const names = (await readdir(base).catch(() => [])).filter((n) => n.endsWith('.json'))
			const rows = []
			for (const name of names) {
				const id = name.slice(0, -5)
				if (!CHAT_ID.test(id)) continue
				const row = await readOwned(userId, id).catch(() => null)
				if (row) rows.push({id: row.id, title: row.title, updatedAt: row.updatedAt})
			}
			return rows.sort((a, b) => b.updatedAt - a.updatedAt)
		},
		async get(userId, id) {
			return readOwned(userId, id)
		},
		async create(userId, {keyId, title} = {}) {
			const t = now()
			const row = {id: ID(), userId, keyId: keyId ?? null, title: clipTitle(title), createdAt: t, updatedAt: t, messages: [], actions: []}
			await writeRow(row.id, row)
			return row
		},
		async append(userId, id, message) {
			if (typeof id !== 'string' || !CHAT_ID.test(id)) throw missingChat()
			return withLock(id, async () => {
				const row = await readOwned(userId, id)
				row.messages.push(message)
				row.updatedAt = now()
				await writeRow(id, row)
				return row
			})
		},
		// The summary of messages[0, upTo). It only moves forward; an older one arriving late is ignored.
		async setSummary(userId, id, {text, upTo}) {
			if (typeof id !== 'string' || !CHAT_ID.test(id)) throw missingChat()
			return withLock(id, async () => {
				const row = await readOwned(userId, id)
				const current = Number.isInteger(row.summary?.upTo) ? row.summary.upTo : 0
				if (!Number.isInteger(upTo) || upTo <= current || upTo > row.messages.length || typeof text !== 'string' || !text.trim()) return row
				row.summary = {text, upTo, at: now()}
				await writeRow(id, row)
				return row
			})
		},
		async setKey(userId, id, keyId) {
			if (typeof id !== 'string' || !CHAT_ID.test(id)) throw missingChat()
			return withLock(id, async () => {
				const row = await readOwned(userId, id)
				row.keyId = keyId
				row.updatedAt = now()
				await writeRow(id, row)
				return row
			})
		},
		async putAction(userId, id, action) {
			if (typeof id !== 'string' || !CHAT_ID.test(id)) throw missingChat()
			const requested = action && typeof action.id === 'string' ? action.id : ''
			const aid = requested || AID()
			if (!ACTION_ID.test(aid)) throw new RpcError('BAD_REQUEST', 'Missing action')
			return withLock(id, async () => {
				const row = await readOwned(userId, id)
				const a = {status: 'pending', ...(action && typeof action === 'object' ? action : {}), id: aid, conversationId: id, userId, createdAt: now()}
				row.actions.push(a)
				row.messages.push({role: 'action', actionId: a.id, at: a.createdAt})
				row.updatedAt = now()
				await writeRow(id, row)
				return a
			})
		},
		async getAction(userId, actionId) {
			return findAction(userId, actionId)
		},
		async saveAction(userId, action) {
			if (!action || typeof action.id !== 'string' || !ACTION_ID.test(action.id)) throw missingStep()
			const found = await findAction(userId, action.id)
			return withLock(found.row.id, async () => {
				const row = await readOwned(userId, found.row.id)
				const current = row.actions.find((x) => x && x.id === action.id)
				if (!current) throw missingStep()
				if (!canReplace(current, action)) return current
				const next = {...current, ...action, id: current.id, conversationId: row.id, userId: row.userId}
				row.actions = row.actions.map((x) => (x && x.id === action.id ? next : x))
				row.updatedAt = now()
				await writeRow(row.id, row)
				return next
			})
		},
		async remove(userId, id) {
			if (typeof id !== 'string' || !CHAT_ID.test(id)) throw missingChat()
			return withLock(id, async () => {
				await readOwned(userId, id)
				await rm(await fileFor(id), {force: true})
				return true
			})
		},
	}
}
