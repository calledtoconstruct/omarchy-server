import {mkdir, readFile} from 'node:fs/promises'
import {randomBytes} from 'node:crypto'
import {join} from 'node:path'

import {removeAtomic, writeAtomic} from '../atomic.mjs'
import {RpcError} from '../rpc.mjs'
import {ANTHROPIC} from './anthropic.mjs'

const SPACEXAI = 'https://api.x.ai/v1'
const OPENAI = 'https://api.openai.com/v1'
// Fixed addresses of the named providers; a custom endpoint brings its own.
export const presetBaseUrl = (provider) => ({spacexai: SPACEXAI, openai: OPENAI, anthropic: ANTHROPIC})[provider] ?? null
// Which API the assistant streams from. OpenAI recommends Responses; the others default to Chat
// Completions, and a key can switch (most local servers now serve both).
const APIS = ['chat', 'responses']
export const apiOf = (k) => (APIS.includes(k?.api) ? k.api : k?.provider === 'openai' ? 'responses' : 'chat')
// How hard a reasoning model thinks. Empty leaves the model's own default and sends nothing.
const EFFORTS = ['low', 'medium', 'high']
export const effortOf = (k) => (EFFORTS.includes(k?.effort) ? k.effort : '')
const ID = () => 'k_' + randomBytes(6).toString('hex')
const last4 = (s) => String(s).slice(-4)
// A pasted endpoint (…/v1/chat/completions, …/v1/responses, …/v1/models) is trimmed back to its base.
export const normalizeBaseUrl = (raw) => {
	const s = String(raw ?? '').trim().replace(/\/+$/, '').replace(/\/(?:chat\/completions|completions|responses|models)$/i, '')
	try {
		const u = new URL(s)
		if (!u.pathname || u.pathname === '/') return `${u.origin}/v1`
	} catch {}
	return s
}
// The last live check of a key, so Settings can show whether it still works.
const publicCheck = (c) => (c && typeof c === 'object' ? {ok: !!c.ok, status: String(c.status ?? ''), message: String(c.message ?? '').slice(0, 300), at: Number(c.at) || 0} : null)
const publicOf = (k, userId) => ({
	id: k.id,
	name: k.name,
	provider: k.provider,
	baseUrl: k.baseUrl,
	model: k.model,
	api: apiOf(k),
	effort: effortOf(k),
	check: publicCheck(k.check),
	last4: k.last4,
	ownerUserId: k.ownerUserId,
	sharedWith: k.sharedWith,
	systemDefault: !!k.systemDefault,
	mine: k.ownerUserId === userId,
	createdAt: k.createdAt,
	updatedAt: k.updatedAt,
})
const canUse = (k, userId) => k.ownerUserId === userId || k.sharedWith.includes(userId) || k.systemDefault

export function createKeyStore({dataDir, now = () => Date.now()}) {
	const file = join(dataDir, 'assistant', 'keys.json')
	let state = {keys: []}
	// Factory reset bumps this before it deletes. A write records the generation when it is queued
	// and writes nothing if a reset has moved on, so the queued write cannot recreate the file.
	let epoch = 0
	const loaded = mkdir(join(dataDir, 'assistant'), {recursive: true, mode: 0o700})
		.then(() => readFile(file, 'utf8'))
		.then((t) => {
			state = JSON.parse(t)
			if (!Array.isArray(state.keys)) state.keys = []
			// Custom endpoints saved with a pasted endpoint path (…/v1/responses) work without re-adding them.
			for (const k of state.keys) if (k?.provider === 'openai-compatible' && k.baseUrl) k.baseUrl = normalizeBaseUrl(k.baseUrl)
		})
		.catch(() => {})
	const save = () => {
		const seen = epoch
		return writeAtomic(file, () => (seen !== epoch ? null : JSON.stringify(state, null, '\t')), {mode: 0o600})
	}
	const find = (id) => state.keys.find((k) => k.id === id)

	return {
		async listPublic(userId) {
			await loaded
			return state.keys.filter((k) => canUse(k, userId)).map((k) => publicOf(k, userId))
		},
		async add(actor, input) {
			await loaded
			const ownerUserId = actor.userId
			const provider = ['openai-compatible', 'spacexai', 'openai', 'anthropic'].includes(input?.provider) ? input.provider : null
			if (!provider) throw new RpcError('BAD_REQUEST', 'Choose Grok, OpenAI, Anthropic or an OpenAI-compatible endpoint')
			// A custom endpoint makes omarchyd send requests to any address it can reach (your network
			// included), so only the server's owner can add one.
			if (provider === 'openai-compatible' && actor.role !== 'owner') throw new RpcError('FORBIDDEN', 'Only the server’s owner can add a custom endpoint')
			const apiKey = String(input?.apiKey ?? '').trim()
			if (apiKey.length > 512) throw new RpcError('BAD_REQUEST', 'Enter an API key')
			if (provider !== 'openai-compatible' && apiKey.length < 4) throw new RpcError('BAD_REQUEST', 'Enter an API key')
			const model = String(input?.model ?? '').trim()
			if (!model || model.length > 80) throw new RpcError('BAD_REQUEST', 'Enter a model id')
			const name = String(input?.name ?? '').trim().slice(0, 48) || ({spacexai: 'Grok', openai: 'OpenAI', anthropic: 'Anthropic'}[provider] ?? 'API')
			let baseUrl = presetBaseUrl(provider) ?? normalizeBaseUrl(input?.baseUrl)
			if (provider === 'openai-compatible') {
				if (!/^https?:\/\/[^\s]+/i.test(baseUrl) || baseUrl.length > 200) throw new RpcError('BAD_REQUEST', 'Enter the API base URL')
			}
			const t = now()
			const row = {id: ID(), ownerUserId, name, provider, baseUrl, api: apiOf({provider, api: input?.api}), effort: effortOf(input), model, apiKey, last4: last4(apiKey), sharedWith: [], systemDefault: false, createdAt: t, updatedAt: t}
			state.keys.push(row)
			await save()
			return publicOf(row, ownerUserId)
		},
		async update(actor, input) {
			await loaded
			const k = find(String(input?.id ?? ''))
			if (!k) throw new RpcError('NOT_FOUND', 'That key isn’t here')
			if (input?.systemDefault === true) {
				if (actor.role === 'member') throw new RpcError('FORBIDDEN', 'Only the server’s owner can set the default key')
				if (k.ownerUserId !== actor.userId) throw new RpcError('FORBIDDEN', 'You can only make a key you own the default')
				for (const x of state.keys) x.systemDefault = x.id === k.id
			} else if (input?.systemDefault === false && k.ownerUserId === actor.userId && actor.role !== 'member') {
				k.systemDefault = false
			}
			if (k.ownerUserId !== actor.userId && input?.sharedWith === undefined && input?.name === undefined && input?.model === undefined && input?.apiKey === undefined && input?.baseUrl === undefined && input?.api === undefined && input?.effort === undefined && input?.systemDefault === undefined) {
				throw new RpcError('FORBIDDEN', 'Only the person who added this key can change it')
			}
			if (k.ownerUserId !== actor.userId && (input?.sharedWith || input?.name || input?.model || input?.apiKey || input?.baseUrl || input?.api || input?.effort !== undefined)) {
				throw new RpcError('FORBIDDEN', 'Only the person who added this key can change it')
			}
			if (k.ownerUserId === actor.userId) {
				if (typeof input?.name === 'string') k.name = input.name.trim().slice(0, 48) || k.name
				if (typeof input?.model === 'string' && input.model.trim() && input.model.trim() !== k.model) {
					k.model = input.model.trim().slice(0, 80)
					delete k.check
				}
				if (APIS.includes(input?.api)) k.api = input.api
				if (input?.effort === '' || EFFORTS.includes(input?.effort)) k.effort = input.effort
				if (typeof input?.apiKey === 'string') {
					const next = input.apiKey.trim()
					if (next.length > 512) throw new RpcError('BAD_REQUEST', 'Enter an API key')
					if (k.provider !== 'openai-compatible' && next.length < 4) throw new RpcError('BAD_REQUEST', 'Enter an API key')
					if (k.provider === 'openai-compatible' || next.length >= 4) {
						k.apiKey = next
						k.last4 = last4(k.apiKey)
						delete k.check
					}
				}
				if (k.provider === 'openai-compatible' && typeof input?.baseUrl === 'string') {
					if (actor.role !== 'owner') throw new RpcError('FORBIDDEN', 'Only the server’s owner can change a custom endpoint')
					k.baseUrl = normalizeBaseUrl(input.baseUrl)
					delete k.check
				}
				if (Array.isArray(input?.sharedWith)) k.sharedWith = [...new Set(input.sharedWith.map((id) => String(id)).filter((id) => id && id !== k.ownerUserId))].slice(0, 32)
			}
			k.updatedAt = now()
			await save()
			return publicOf(k, actor.userId)
		},
		async remove(actor, id) {
			await loaded
			const k = find(id)
			if (!k) throw new RpcError('NOT_FOUND', 'That key isn’t here')
			if (k.ownerUserId !== actor.userId) throw new RpcError('FORBIDDEN', 'Only the person who added this key can delete it')
			state.keys = state.keys.filter((x) => x.id !== id)
			await save()
			return true
		},
		async recordCheck(id, check) {
			await loaded
			const k = find(id)
			if (!k) return null
			k.check = {...publicCheck(check), at: now()}
			await save()
			return publicCheck(k.check)
		},
		async getSecret(id) {
			await loaded
			return find(id) ?? null
		},
		// Factory reset: the key list is empty in memory and the file is gone, so the next owner
		// does not inherit these keys. A write queued before this bump does not put the file back.
		async reset() {
			await loaded
			epoch++
			state = {keys: []}
			await removeAtomic(file)
		},
		async resolveForUser(userId, keyId) {
			await loaded
			if (keyId) {
				const k = find(keyId)
				if (!k || !canUse(k, userId)) throw new RpcError('NOT_FOUND', 'That key isn’t available')
				return k
			}
			const own = state.keys.filter((k) => k.ownerUserId === userId)
			if (own.length) return own.sort((a, b) => b.updatedAt - a.updatedAt)[0]
			const shared = state.keys.filter((k) => k.sharedWith.includes(userId))
			if (shared.length) return shared[0]
			const def = state.keys.find((k) => k.systemDefault)
			if (def) return def
			throw new RpcError('PRECONDITION_FAILED', 'Add an API key in Settings → Assistant, or ask the owner to share one')
		},
	}
}
