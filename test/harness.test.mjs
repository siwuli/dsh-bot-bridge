/**
 * 模拟集成测试: 在 mock webServer/apiProxy/cordis-ctx 环境中完整验证
 * dsh-bot-bridge 的令牌认证、SSE 流(非思考过滤)、提问/授权应答、
 * 历史查询与 clientId 会话绑定。
 */
import assert from 'node:assert';
import { Writable } from 'node:stream';
import { once } from 'node:events';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { apply } from '../lib/index.js';

const TOKEN = 'tok-123';
const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-bot-bridge-harness-'));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitUntil(fn, timeout = 2000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (fn()) return;
    await sleep(5);
  }
  throw new Error('waitUntil timeout');
}

/* ---------------- 模拟 DSH 环境 ---------------- */

function makeQueue(signal) {
  const buf = [];
  let waiter = null;
  let done = false;
  signal.addEventListener('abort', () => { done = true; waiter?.(); }, { once: true });
  return {
    push(item) { if (done) return; buf.push(item); waiter?.(); },
    end() { done = true; waiter?.(); },
    iterate: (async function* iterate() {
      while (true) {
        while (buf.length > 0) yield buf.shift();
        if (done || signal.aborted) return;
        await new Promise((resolve) => { waiter = resolve; });
        waiter = null;
      }
    })(),
  };
}

function makeEnv(config) {
  const prefixes = new Map();
  const webServer = {
    register(route) {
      if (route.kind !== 'prefix') throw new Error('only prefix routes in mock');
      if (prefixes.has(route.path)) throw new Error('duplicate prefix route ' + route.path);
      prefixes.set(route.path, route);
      return () => prefixes.delete(route.path);
    },
    match(pathname) {
      let best;
      for (const [p, route] of prefixes) {
        if (pathname !== p && !pathname.startsWith(p + '/')) continue;
        if (!best || p.length > best.path.length) best = route;
      }
      return best;
    },
  };

  const state = {
    createCalls: [], promptCalls: [], historyCalls: [], respondCalls: [], cancelCalls: [], selectModelCalls: [],
    queues: [], sessionSeq: 0, persistedCwds: {},
  };
  const api = {
    sessions: {
      async create(request) {
        state.createCalls.push(request.payload);
        state.sessionSeq += 1;
        return { rpcId: request.rpcId, result: { ok: true, value: { sessionId: 'session-' + state.sessionSeq } } };
      },
      async prompt(request) {
        state.promptCalls.push(request.payload);
        return { rpcId: request.rpcId, result: { ok: true, value: { accepted: true } } };
      },
      async cancel(request) {
        state.cancelCalls.push(request.payload);
        return { rpcId: request.rpcId, result: { ok: true, value: { accepted: true } } };
      },
      async selectModel(request) {
        state.selectModelCalls.push(request.payload);
        return { rpcId: request.rpcId, result: { ok: true, value: { selected: { provider: request.payload.provider, model: request.payload.model } } } };
      },
      async history(request) {
        state.historyCalls.push(request.payload);
        return {
          rpcId: request.rpcId,
          result: { ok: true,
          value: {
            hasMore: false,
            events: [
              { event: { type: 'user/message', seq: 0, time: 1, data: { message: { content: [{ type: 'text', text: '你好' }] } } } },
              { event: { type: 'assistant/message', seq: 1, time: 2, data: { message: { content: [
                { type: 'thinking', text: '内心活动' },
                { type: 'text', text: '你好呀' },
              ] } } } },
            ],
          } },
        };
      },
    },
    workspace: {
      async list(request) {
        return { rpcId: request.rpcId, result: { ok: true, value: { items: [{ workspaceId: 'w1', path: '/data/w1', title: 'W1' }], archivedSessionIds: [] } } };
      },
    },
    llm: {
      async models(request) {
        return { rpcId: request.rpcId, result: { ok: true, value: { groups: [{ id: 'deepseek', name: 'DeepSeek', models: [{ id: 'deepseek-chat', name: 'DeepSeek Chat' }] }], failures: [] } } };
      },
    },
    events: {
      mux(request, signal) {
        const q = makeQueue(signal);
        state.queues.push(q);
        return q.iterate;
      },
    },
    async respond(message) {
      state.respondCalls.push(message);
      return { accepted: true };
    },
  };

  const handlers = new Map();
  const ctx = {
    webServer,
    apiProxy: api,
    sessions: new Map(),
    sessionPersistence: {
      async inspect(sessionId) {
        const cwd = state.persistedCwds[sessionId];
        return cwd === undefined ? undefined : { meta: { cwd } };
      },
    },
    get: (name) => {
      if (name === 'apiProxy') return api;
      if (name === 'sessions') return ctx.sessions;
      if (name === 'sessionPersistence') return ctx.sessionPersistence;
      return undefined;
    },
    effect: (fn) => { const d = fn(); return d; },
    on(name, cb) {
      const list = handlers.get(name) ?? [];
      list.push(cb);
      handlers.set(name, list);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter((x) => x !== cb));
    },
    emit(name, ...args) {
      for (const cb of [...(handlers.get(name) ?? [])]) {
        try { cb(...args); } catch (e) { console.error('handler error', e); }
      }
    },
  };

  apply(ctx, config);
  return { webServer, state, ctx };
}

function fakeReq({ method = 'GET', url = '/', headers = {}, body = Buffer.alloc(0), remoteAddress = '127.0.0.1' }) {
  const req = { method, url, headers: { host: '127.0.0.1:3080', ...headers }, socket: { remoteAddress } };
  let served = false;
  req[Symbol.asyncIterator] = async function* iterator() {
    if (!served) {
      served = true;
      yield body;
    }
  };
  return req;
}

function fakeRes() {
  const out = { status: 0, headers: {}, chunks: [], done: false };
  const res = new Writable({
    write(chunk, enc, cb) { out.chunks.push(Buffer.from(chunk)); cb(); },
  });
  res.writeHead = (status, headers) => { out.status = status; out.headers = headers ?? {}; };
  Object.defineProperty(res, 'result', {
    get() {
      return { status: out.status, headers: out.headers, body: Buffer.concat(out.chunks).toString('utf8') };
    },
  });
  return res;
}

async function dispatch(env, req) {
  const route = env.webServer.match(new URL(req.url ?? '/', 'http://dsh.local').pathname);
  assert.ok(route, 'no route matched for ' + req.url);
  const res = fakeRes();
  const done = route.handler(req, res);
  if (!res.writableFinished) {
    await Promise.race([once(res, 'finish').catch(() => {}), sleep(3000).then(() => { if (!res.writableFinished) throw new Error('response timeout: ' + req.url); })]);
  }
  await done;
  return res.result;
}

function sseFrames(body) {
  return body.split('\n\n').map((b) => b.trim()).filter(Boolean)
    .map((b) => (b.startsWith('data: ') ? JSON.parse(b.slice(6)) : null)).filter(Boolean);
}

function finishTurn(env, res, q, sid, marker) {
  q.push({ rpcId: 'rt1', payload: { type: 'session/event', sessionId: sid, event: { type: 'turn/start', seq: 90, time: 90, data: { turn: 1 } } } });
  q.push({ rpcId: 'rt2', payload: { type: 'session/event', sessionId: sid, event: { type: 'turn/end', seq: 91, time: 91, data: { turn: 1 } } } });
  q.push({ rpcId: 'rt3', payload: { type: 'session/event', sessionId: sid, event: { type: 'assistant/chunk', seq: 92, time: 92, data: { turn: 1, step: 1, chunk: { type: 'text-delta', index: 0, text: marker } } } } });
  return waitUntil(() => sseFrames(res.result.body).some((fr) => fr.type === 'text' && fr.text === marker))
    .then(() => env.ctx.emit('agent/status', { agent: { id: sid }, status: 'idle' }));
}

const authHeaders = { 'x-bot-token': TOKEN };

async function runPromptToDone(env, body, marker) {
  const before = env.state.promptCalls.length;
  const req = jsonPost('/api/bot/prompt', body);
  const res = fakeRes();
  const done = env.webServer.match('/api/bot/prompt').handler(req, res);
  await waitUntil(() => env.state.promptCalls.length === before + 1);
  const sid = env.state.promptCalls.at(-1).sessionId;
  const q = env.state.queues.at(-1);
  await finishTurn(env, res, q, sid, marker);
  await done;
  if (!res.writableFinished) await once(res, 'finish').catch(() => {});
  assert.ok(sseFrames(res.result.body).some((fr) => fr.type === 'done'));
  return sid;
}

/* ---------------- 测试 ---------------- */

const env = makeEnv({
  botToken: TOKEN,
  storageFile: path.join(TMP, 'store.json'),
  doneQuietMs: 50,
  maxStreamMs: 5000,
  includeThinking: false,
  includeToolEvents: true,
});

const jsonPost = (url, body, headers = {}) => fakeReq({
  method: 'POST', url,
  headers: { 'content-type': 'application/json', ...authHeaders, ...headers },
  body: Buffer.from(JSON.stringify(body)),
});

// ---- 1. health: 无令牌 401 / 有令牌 200 ----
let r = await dispatch(env, fakeReq({ url: '/api/bot/health' }));
assert.strictEqual(r.status, 401);
r = await dispatch(env, fakeReq({ url: '/api/bot/health', headers: authHeaders }));
assert.strictEqual(r.status, 200);
assert.strictEqual(JSON.parse(r.body).name, 'dsh-bot-bridge');
console.log('✓ 1. health 令牌门禁');

// ---- 2. prompt: 创建会话 + 非思考 SSE 流 ----
{
  const req = jsonPost('/api/bot/prompt', { clientId: 'qq:1', text: '你好', modelProvider: 'deepseek', modelName: 'deepseek-chat' });
  const res = fakeRes();
  const done = env.webServer.match('/api/bot/prompt').handler(req, res);
  await waitUntil(() => env.state.promptCalls.length === 1);
  assert.strictEqual(env.state.createCalls.length, 1);
  assert.deepStrictEqual(env.state.promptCalls[0].content, [{ type: 'text', text: '你好' }]);
  await waitUntil(() => env.state.selectModelCalls.length === 1);
  assert.strictEqual(env.state.selectModelCalls[0].sessionId, 'session-1');
  assert.strictEqual(env.state.selectModelCalls[0].model, 'deepseek-chat');
  const q = env.state.queues[0];
  const sid = env.state.promptCalls[0].sessionId;
  q.push({ rpcId: 'r1', payload: { type: 'session/event', sessionId: sid, event: { type: 'assistant/chunk', seq: 1, time: 1, data: { turn: 1, step: 1, chunk: { type: 'reasoning-delta', index: 0, text: '内心思考' } } } } });
  q.push({ rpcId: 'r2', payload: { type: 'session/event', sessionId: sid, event: { type: 'assistant/chunk', seq: 2, time: 2, data: { turn: 1, step: 1, chunk: { type: 'text-delta', index: 0, text: '你好' } } } } });
  q.push({ rpcId: 'r3', payload: { type: 'session/event', sessionId: sid, event: { type: 'assistant/chunk', seq: 3, time: 3, data: { turn: 1, step: 1, chunk: { type: 'text-delta', index: 1, text: '，世界' } } } } });
  q.push({ rpcId: 'r4', payload: { type: 'session/event', sessionId: sid, event: { type: 'tool/call', seq: 4, time: 4, data: { name: 'bash', arguments: '{}', callId: 'c1' } }, view: { for: 'call', view: { card: 'terminal', title: 'ls -la' } } } });
  q.push({ rpcId: 'r5', payload: { type: 'session/event', sessionId: sid, event: { type: 'tool/result', seq: 5, time: 5, data: { message: { role: 'tool', content: [] } } }, view: { for: 'result', view: { card: 'generic', title: '完成' } } } });
  await finishTurn(env, res, q, sid, 'MARK2');
  await done;
  if (!res.writableFinished) await once(res, 'finish').catch(() => {});
  const frames = sseFrames(res.result.body);
  const text = frames.filter((f) => f.type === 'text').map((f) => f.text).join('');
  assert.ok(text.startsWith('你好，世界'), '文本增量应流式拼接');
  assert.ok(!frames.some((f) => f.type === 'thinking'), 'thinking 不应转发');
  const tools = frames.filter((f) => f.type === 'tool');
  assert.strictEqual(tools[0]?.name, 'ls -la');
  assert.ok(frames.some((f) => f.type === 'done'), '应有 done 帧');
  console.log('✓ 2. prompt 创建会话并流式转发非思考输出');
}

// ---- 3. 同一 clientId 复用会话 ----
{
  const req = jsonPost('/api/bot/prompt', { clientId: 'qq:1', text: '继续' });
  const res = fakeRes();
  const done = env.webServer.match('/api/bot/prompt').handler(req, res);
  await waitUntil(() => env.state.promptCalls.length === 2);
  assert.strictEqual(env.state.createCalls.length, 1, '不应重复创建会话');
  assert.strictEqual(env.state.promptCalls[1].sessionId, 'session-1');
  const sid = 'session-1';
  const q = env.state.queues.at(-1);
  await finishTurn(env, res, q, sid, 'MARK3');
  await done;
  if (!res.writableFinished) await once(res, 'finish').catch(() => {});
  assert.ok(sseFrames(res.result.body).some((f) => f.type === 'done'));
  console.log('✓ 3. 同一 clientId 复用会话 (不重复创建)');
}

// ---- 4. 提问 → 应答 → 继续 → done ----
{
  const req = jsonPost('/api/bot/prompt', { clientId: 'qq:2', text: '帮我做件事' });
  const res = fakeRes();
  const done = env.webServer.match('/api/bot/prompt').handler(req, res);
  await waitUntil(() => env.state.promptCalls.length === 3);
  const sid = env.state.promptCalls[2].sessionId;
  const q = env.state.queues.at(-1);
  q.push({ rpcId: 'q1', payload: { type: 'question/requested', sessionId: sid, questions: [{ id: 'q', question: '选哪个?', options: [{ label: '甲' }, { label: '乙' }] }] } });
  await waitUntil(() => sseFrames(res.result.body).some((f) => f.type === 'question'));
  assert.ok(sseFrames(res.result.body).some((f) => f.type === 'question' && f.rpcId === 'q1'));

  const ar = await dispatch(env, jsonPost('/api/bot/answer', { sessionId: sid, rpcId: 'q1', answers: [{ id: 'q', selected: [], custom: '甲' }] }));
  assert.strictEqual(ar.status, 200);
  assert.strictEqual(env.state.respondCalls.at(-1).result.value.answer.answers[0].custom, '甲');

  q.push({ rpcId: 'q2', payload: { type: 'question/resolved', sessionId: sid, outcome: 'answered' } });
  await finishTurn(env, res, q, sid, 'MARK4');
  await done;
  if (!res.writableFinished) await once(res, 'finish').catch(() => {});
  assert.ok(sseFrames(res.result.body).some((f) => f.type === 'done'));
  console.log('✓ 4. 提问应答链路 (question → answer → 继续 → done)');
}

// ---- 5. 授权请求 → 批准 ----
{
  const req = jsonPost('/api/bot/prompt', { clientId: 'qq:3', text: '执行危险操作' });
  const res = fakeRes();
  const done = env.webServer.match('/api/bot/prompt').handler(req, res);
  await waitUntil(() => env.state.promptCalls.length === 4);
  const sid = env.state.promptCalls[3].sessionId;
  const q = env.state.queues.at(-1);
  q.push({ rpcId: 'a1', payload: { type: 'approval/requested', sessionId: sid, approvalId: 'ap1', toolName: 'write', reason: '需要授权' } });
  await waitUntil(() => sseFrames(res.result.body).some((f) => f.type === 'approval'));
  const ar = await dispatch(env, jsonPost('/api/bot/approve', { sessionId: sid, rpcId: 'a1', approvalId: 'ap1', outcome: 'allowed-once' }));
  assert.strictEqual(ar.status, 200);
  assert.strictEqual(env.state.respondCalls.at(-1).result.value.outcome, 'allowed-once');
  q.push({ rpcId: 'a2', payload: { type: 'approval/resolved', sessionId: sid, outcome: 'allowed-once' } });
  await finishTurn(env, res, q, sid, 'MARK5');
  await done;
  if (!res.writableFinished) await once(res, 'finish').catch(() => {});
  assert.ok(sseFrames(res.result.body).some((f) => f.type === 'done'));
  console.log('✓ 5. 工具授权请求应答链路');
}

// ---- 6. 历史查询 (默认排除 thinking) ----
{
  r = await dispatch(env, fakeReq({ url: '/api/bot/history?sessionId=session-1&limit=10', headers: authHeaders }));
  assert.strictEqual(r.status, 200);
  const items = JSON.parse(r.body).items;
  assert.strictEqual(items.length, 2);
  assert.strictEqual(items[0].role, 'user');
  assert.strictEqual(items[1].text, '你好呀');
  console.log('✓ 6. 历史查询抽取 assistant 文本 (排除 thinking)');
}

// ---- 6b. cancel ----
{
  r = await dispatch(env, jsonPost('/api/bot/cancel', { sessionId: 'session-1' }));
  assert.strictEqual(r.status, 200);
  assert.strictEqual(env.state.cancelCalls.length, 1);
  console.log('✓ 6b. cancel 端点');
}

// ---- 6c. 工作区列表 ----
{
  r = await dispatch(env, fakeReq({ url: '/api/bot/workspaces', headers: authHeaders }));
  assert.strictEqual(r.status, 200);
  assert.strictEqual(JSON.parse(r.body).items[0].workspaceId, 'w1');
  console.log('✓ 6c. 工作区列表端点');
}

// ---- 6d. 模型目录 ----
{
  r = await dispatch(env, fakeReq({ url: '/api/bot/models', headers: authHeaders }));
  assert.strictEqual(r.status, 200);
  assert.strictEqual(JSON.parse(r.body).groups[0].models[0].id, 'deepseek-chat');
  console.log('✓ 6d. 模型目录端点');
}

// ---- 6e. 会话切模型 ----
{
  r = await dispatch(env, jsonPost('/api/bot/model', { sessionId: 'session-1', provider: 'deepseek', model: 'deepseek-reasoner', reasoningEffort: 'high' }));
  assert.strictEqual(r.status, 200);
  assert.strictEqual(env.state.selectModelCalls.at(-1).reasoningEffort, 'high');
  console.log('✓ 6e. 会话切模型端点');
}

// ---- 7. reset + session 查询 ----
{
  r = await dispatch(env, jsonPost('/api/bot/reset', { clientId: 'qq:1' }));
  assert.strictEqual(JSON.parse(r.body).forgotten, 'session-1');
  r = await dispatch(env, fakeReq({ url: '/api/bot/session?clientId=qq:1', headers: authHeaders }));
  assert.strictEqual(JSON.parse(r.body).sessionId, null);
  console.log('✓ 7. clientId 会话解绑/查询');
}

// ---- 7b. 工作区不一致时自动换绑新会话 ----
{
  // 第一步: qq:ws 不带工作区, 绑定旧会话 (cwd 记录为旧目录)
  let req = jsonPost('/api/bot/prompt', { clientId: 'qq:ws', text: 'hi' });
  let res = fakeRes();
  let done = env.webServer.match('/api/bot/prompt').handler(req, res);
  await waitUntil(() => env.state.createCalls.length === 4);
  const oldId = 'session-' + env.state.sessionSeq;
  env.state.persistedCwds[oldId] = 'D:\\work\\old';
  let q = env.state.queues.at(-1);
  await finishTurn(env, res, q, oldId, 'MARK7B1');
  await done;
  if (!res.writableFinished) await once(res, 'finish').catch(() => {});

  // 第二步: 同 clientId 但工作区不同 → 应解绑并新建会话(带新工作区)
  req = jsonPost('/api/bot/prompt', { clientId: 'qq:ws', text: '在工作区A干活', workspacePath: 'D:/Projects' });
  res = fakeRes();
  done = env.webServer.match('/api/bot/prompt').handler(req, res);
  await waitUntil(() => env.state.createCalls.length === 5);
  const createdPayload = env.state.createCalls[4];
  assert.strictEqual(createdPayload.cwd, 'D:/Projects', '换绑新会话应带新工作区');
  const newId = 'session-' + env.state.sessionSeq;
  q = env.state.queues.at(-1);
  await finishTurn(env, res, q, newId, 'MARK7B2');
  await done;
  if (!res.writableFinished) await once(res, 'finish').catch(() => {});
  assert.ok(sseFrames(res.result.body).some((fr) => fr.type === 'done'));
  // reset 端点返回的 forgotten 应为新会话 id
  const rr2 = await dispatch(env, jsonPost('/api/bot/reset', { clientId: 'qq:ws' }));
  assert.strictEqual(JSON.parse(rr2.body).forgotten, newId);
  console.log('✓ 7b. 工作区不一致自动换绑新会话');
}

// ---- 7c. 多实例共享存储: 合并写, 绑定不丢 ----
{
  const storePath = path.join(TMP, 'shared-' + Date.now() + '.json');
  const envA = makeEnv({ botToken: TOKEN, storageFile: storePath, doneQuietMs: 50, maxStreamMs: 5000 });
  const envB = makeEnv({ botToken: TOKEN, storageFile: storePath, doneQuietMs: 50, maxStreamMs: 5000 });
  // A 绑定 qq:a 并保存; 此时 B 的内存 store 还是空的(旧快照)
  await runPromptToDone(envA, { clientId: 'qq:a', text: 'hi' }, 'MARK7C1');
  // B 绑定 qq:b 并保存(旧快照 + 合并写) → 文件里两个绑定都在
  await runPromptToDone(envB, { clientId: 'qq:b', text: 'hi' }, 'MARK7C2');
  let fileData = JSON.parse(fs.readFileSync(storePath, 'utf8'));
  assert.ok(fileData['qq:a'], 'A 的绑定不应被 B 覆盖');
  assert.ok(fileData['qq:b'], 'B 的绑定应存在');
  // A 显式 reset qq:a → 墓碑生效, qq:b 保留
  const rr = await dispatch(envA, jsonPost('/api/bot/reset', { clientId: 'qq:a' }));
  assert.strictEqual(JSON.parse(rr.body).forgotten, fileData['qq:a']);
  fileData = JSON.parse(fs.readFileSync(storePath, 'utf8'));
  assert.strictEqual(fileData['qq:a'], undefined);
  assert.ok(fileData['qq:b'], 'reset 一个 key 不应影响其它 key');
  fs.rmSync(storePath, { force: true });
  console.log('✓ 7c. 多实例存储合并写, 绑定不丢失');
}

// ---- 8. 错误路径: 缺令牌 / 缺文本 / prompt 被拒 ----
{
  r = await dispatch(env, fakeReq({ method: 'POST', url: '/api/bot/prompt', headers: { 'content-type': 'application/json' }, body: Buffer.from(JSON.stringify({ text: 'x' })) }));
  assert.strictEqual(r.status, 401);
  r = await dispatch(env, jsonPost('/api/bot/prompt', { clientId: 'qq:4', text: '   ' }));
  assert.strictEqual(r.status, 400);

  // prompt 拒绝: mock 返回错误
  const origPrompt = env.state.promptCalls;
  const apiProxy = env.ctx.apiProxy;
  const origPromptFn = apiProxy.sessions.prompt;
  apiProxy.sessions.prompt = async () => ({ rpcId: 'x', result: { ok: false, error: { code: 'agent-busy', message: 'prompt rejected' } } });
  const req = jsonPost('/api/bot/prompt', { clientId: 'qq:4', text: '试试' });
  const res = fakeRes();
  const done = env.webServer.match('/api/bot/prompt').handler(req, res);
  await done;
  if (!res.writableFinished) await once(res, 'finish').catch(() => {});
  const frames = sseFrames(res.result.body);
  assert.ok(frames.some((f) => f.type === 'error'));
  assert.ok(frames.some((f) => f.type === 'done' && f.reason === 'prompt-rejected'));
  apiProxy.sessions.prompt = origPromptFn;
  console.log('✓ 8. 错误路径 (401/400/prompt-rejected)');
}

fs.rmSync(TMP, { recursive: true, force: true });
console.log('\n🎉 dsh-bot-bridge 集成测试全部通过');