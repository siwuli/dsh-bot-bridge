/**
 * dsh-bot-bridge — 宿主端 (Host) 插件入口
 *
 * 为机器人后端 (AstrBot 等) 提供一条稳定的 HTTP 桥:
 *   - POST /api/bot/prompt   → SSE 流: 发送提示词并实时推送"非思考"输出
 *   - POST /api/bot/answer   → 回答 DSH 的 ask_user_question 提问
 *   - POST /api/bot/approve  → 批准/拒绝工具授权请求
 *   - GET  /api/bot/history  → 读取会话历史 (抽取 assistant 文本)
 *   - GET  /api/bot/session  → 查询 clientId 绑定的会话
 *   - POST /api/bot/reset    → 解绑 clientId 的会话 (下次自动新建)
 *   - GET  /api/bot/health   → 健康检查
 *
 * 认证: Authorization: Bearer <token> 或 x-bot-token 头 (常量时间比较)。
 * 流内容: 复用 ctx.apiProxy.events.mux 的 frame 流, 仅转发本会话事件,
 * 默认过滤 reasoning/thinking 增量 (includeThinking 可开启)。
 *
 * 依赖: webServer (HTTP 路由), apiProxy (sessions.create/prompt/history、
 * events.mux、respond)。与 Web GUI 共用同一会话存储与 Agent 运行时。
 */

import { randomUUID } from 'node:crypto';
import { homedir } from 'node:os';
import { join } from 'node:path';
import * as core from './core.js';

export const name = 'bot-bridge';
export const inject = ['webServer', 'apiProxy'];

const VERSION = '0.1.0';

export function apply(ctx, config) {
  const botToken = typeof config?.botToken === 'string' ? config.botToken.trim() : '';
  if (!botToken) {
    throw new Error('[dsh-bot-bridge] config.botToken is required (set it in the profile cordis.patch.yml)');
  }
  const cfg = {
    botToken,
    defaultWorkspacePath: typeof config?.defaultWorkspacePath === 'string' && config.defaultWorkspacePath.trim()
      ? config.defaultWorkspacePath.trim()
      : undefined,
    defaultAgentPreset: typeof config?.defaultAgentPreset === 'string' && config.defaultAgentPreset.trim()
      ? config.defaultAgentPreset.trim()
      : undefined,
    includeThinking: config?.includeThinking === true,
    includeToolEvents: config?.includeToolEvents !== false,
    doneQuietMs: Number.isFinite(config?.doneQuietMs) ? Math.max(0, config.doneQuietMs) : 3000,
    maxStreamMs: Number.isFinite(config?.maxStreamMs) ? Math.max(1000, config.maxStreamMs) : 30 * 60 * 1000,
    maxPromptChars: Number.isFinite(config?.maxPromptChars) ? Math.max(100, config.maxPromptChars) : 20000,
    maxHistoryItems: Number.isFinite(config?.maxHistoryItems) ? Math.max(1, config.maxHistoryItems) : 50,
    storageFile: typeof config?.storageFile === 'string' && config.storageFile.trim()
      ? config.storageFile.trim()
      : join(homedir(), '.dsh', 'storages', 'dsh-bot-bridge.json'),
    loopbackOnly: config?.loopbackOnly === true,
  };

  const store = core.loadStore(cfg.storageFile);
  const persist = () => core.saveStore(cfg.storageFile, store);
  const newRpcId = () => randomUUID();

  const isLoopback = (req) => {
    const ip = req.socket?.remoteAddress ?? '';
    return ip === '127.0.0.1' || ip === '::1' || ip === '::ffff:127.0.0.1';
  };

  const reject = (res, status, code, message) =>
    core.writeJson(res, { ok: false, error: { code, message } }, status);

  const guarded = (req, res) => {
    if (!core.requestAuthed(req.headers, cfg.botToken)) {
      reject(res, 401, 'unauthorized', 'invalid or missing bot token');
      return false;
    }
    if (cfg.loopbackOnly && !isLoopback(req)) {
      reject(res, 403, 'forbidden', 'bot bridge is loopback-only');
      return false;
    }
    return true;
  };

  const sessions = () => ctx.apiProxy.sessions;
  const mux = (signal) => ctx.apiProxy.events.mux({ rpcId: newRpcId(), payload: {} }, signal);
  /** 域方法返回完整 RpcResponse {rpcId, result} 或裸 RpcResult {ok,value}——统一取 result */
  const unwrap = (resp) => (resp && typeof resp === 'object' && 'result' in resp ? resp.result : resp);

  /* ---------------------------------------------------------------- */
  /* 简单端点                                                          */
  /* ---------------------------------------------------------------- */

  const handleHealth = (req, res) => {
    if (!guarded(req, res)) return;
    core.writeJson(res, { ok: true, name: 'dsh-bot-bridge', version: VERSION });
  };

  const handleSession = (req, res) => {
    if (!guarded(req, res)) return;
    const url = new URL(req.url ?? '/', 'http://dsh.local');
    const clientId = (url.searchParams.get('clientId') ?? '').trim();
    core.writeJson(res, { ok: true, sessionId: clientId ? (store[clientId] ?? null) : null });
  };

  const handleReset = async (req, res) => {
    if (!guarded(req, res)) return;
    let body;
    try { body = await core.readJsonBody(req); } catch (e) {
      return reject(res, 400, e.code ?? 'bad-request', e.message);
    }
    const clientId = typeof body?.clientId === 'string' ? body.clientId.trim() : '';
    if (!clientId) return reject(res, 400, 'missing-client-id', 'clientId is required');
    const old = store[clientId];
    delete store[clientId];
    persist();
    core.writeJson(res, { ok: true, forgotten: old ?? null });
  };

  const handleHistory = async (req, res) => {
    if (!guarded(req, res)) return;
    const url = new URL(req.url ?? '/', 'http://dsh.local');
    const sessionId = (url.searchParams.get('sessionId') ?? '').trim();
    if (!sessionId) return reject(res, 400, 'missing-session-id', 'sessionId is required');
    const limit = Math.min(Math.max(parseInt(url.searchParams.get('limit') ?? '', 10) || cfg.maxHistoryItems, 1), 200);
    const result = unwrap(await sessions().history({
      rpcId: newRpcId(),
      payload: { sessionId, maxMessages: limit * 4 },
    }));
    if (result?.ok !== true) {
      const e = result?.error ?? {};
      return reject(res, 502, e.code ?? 'internal', 'history failed: ' + (e.message ?? 'unknown'));
    }
    const items = [];
    for (const entry of result.value.events ?? []) {
      const event = entry?.event;
      if (!event) continue;
      if (event.type === 'user/message') {
        const text = core.textOfBlocks(event.data?.message?.content, true);
        if (text) items.push({ role: 'user', text: core.truncate(text, 4000), time: event.time });
      } else if (event.type === 'assistant/message') {
        const text = core.textOfBlocks(event.data?.message?.content, cfg.includeThinking);
        if (text) items.push({ role: 'assistant', text: core.truncate(text, 8000), time: event.time });
      }
    }
    core.writeJson(res, { ok: true, sessionId, items: items.slice(-limit) });
  };

  const handleAnswer = async (req, res) => {
    if (!guarded(req, res)) return;
    let body;
    try { body = await core.readJsonBody(req); } catch (e) {
      return reject(res, 400, e.code ?? 'bad-request', e.message);
    }
    const sessionId = typeof body?.sessionId === 'string' ? body.sessionId.trim() : '';
    const rpcId = typeof body?.rpcId === 'string' ? body.rpcId : '';
    const answers = body?.answers;
    if (!sessionId || !rpcId || !Array.isArray(answers)) {
      return reject(res, 400, 'bad-request', 'sessionId, rpcId and answers[] are required');
    }
    const receipt = await ctx.apiProxy.respond({
      rpcId,
      result: { ok: true, value: { sessionId, answer: { answers } } },
    });
    if (receipt?.accepted === true) return core.writeJson(res, { ok: true, accepted: true });
    return reject(res, 409, 'bad-response', 'question answer rejected: ' + (receipt?.reason ?? 'unknown'));
  };

  const handleApprove = async (req, res) => {
    if (!guarded(req, res)) return;
    let body;
    try { body = await core.readJsonBody(req); } catch (e) {
      return reject(res, 400, e.code ?? 'bad-request', e.message);
    }
    const sessionId = typeof body?.sessionId === 'string' ? body.sessionId.trim() : '';
    const rpcId = typeof body?.rpcId === 'string' ? body.rpcId : '';
    const approvalId = typeof body?.approvalId === 'string' ? body.approvalId : '';
    const outcome = body?.outcome === 'rejected' ? 'rejected' : 'allowed-once';
    if (!sessionId || !rpcId || !approvalId) {
      return reject(res, 400, 'bad-request', 'sessionId, rpcId and approvalId are required');
    }
    const receipt = await ctx.apiProxy.respond({
      rpcId,
      result: { ok: true, value: { sessionId, approvalId, outcome } },
    });
    if (receipt?.accepted === true) return core.writeJson(res, { ok: true, accepted: true });
    return reject(res, 409, 'bad-response', 'approval decision rejected: ' + (receipt?.reason ?? 'unknown'));
  };

  /* ---------------------------------------------------------------- */
  /* POST /api/bot/prompt — SSE 流                                     */
  /* ---------------------------------------------------------------- */

  const handlePrompt = async (req, res) => {
    if (!guarded(req, res)) return;
    let body;
    try { body = await core.readJsonBody(req); } catch (e) {
      return reject(res, 400, e.code ?? 'bad-request', e.message);
    }

    const clientId = typeof body?.clientId === 'string' && body.clientId.trim()
      ? body.clientId.trim().slice(0, 120)
      : undefined;
    const text = String(body?.text ?? '').replace(/\r\n/g, '\n').trim();
    if (!text) return reject(res, 400, 'missing-text', 'text is required');
    if (text.length > cfg.maxPromptChars) {
      return reject(res, 400, 'text-too-long', 'text exceeds ' + cfg.maxPromptChars + ' characters');
    }
    const reset = body?.reset === true;
    const sessionIdProvided = typeof body?.sessionId === 'string' && body.sessionId.trim()
      ? body.sessionId.trim()
      : undefined;
    const workspacePath = typeof body?.workspacePath === 'string' && body.workspacePath.trim()
      ? body.workspacePath.trim()
      : cfg.defaultWorkspacePath;
    const agentPreset = typeof body?.agentPreset === 'string' && body.agentPreset.trim()
      ? body.agentPreset.trim()
      : cfg.defaultAgentPreset;

    if (reset && clientId) {
      delete store[clientId];
      persist();
    }

    let sessionId;
    if (sessionIdProvided) sessionId = sessionIdProvided;
    else if (clientId) sessionId = store[clientId];
    if (!sessionId) {
      const created = unwrap(await sessions().create({
        rpcId: newRpcId(),
        payload: {
          ...(workspacePath ? { cwd: workspacePath } : {}),
          ...(agentPreset ? { agentPreset } : {}),
        },
      }));
      if (created?.ok !== true) {
        const e = created?.error ?? {};
        return reject(res, 502, e.code ?? 'internal', 'create session failed: ' + (e.message ?? 'unknown'));
      }
      sessionId = created.value?.sessionId;
      if (!sessionId) return reject(res, 502, 'internal', 'create session returned no sessionId');
      if (clientId) {
        store[clientId] = sessionId;
        persist();
      }
    }

    core.sseStart(res);
    const abort = new AbortController();
    const signal = abort.signal;
    let closed = false;
    let turnsStarted = 0;
    let turnsEnded = 0;
    let pendingQuestion = false;
    let pendingApproval = false;
    const toolNames = new Map(); // callId → 展示名 (供 tool/result 复用)
    let doneTimer = null;
    let hardTimer = null;
    let heartbeat = null;
    const disposers = [];

    const send = (obj) => {
      if (closed) return;
      try { core.sseSend(res, obj); } catch (_) { /* ignore */ }
    };

    const cleanup = () => {
      if (heartbeat) { clearInterval(heartbeat); heartbeat = null; }
      if (doneTimer) { clearTimeout(doneTimer); doneTimer = null; }
      if (hardTimer) { clearTimeout(hardTimer); hardTimer = null; }
      abort.abort();
      for (const dispose of disposers.splice(0)) {
        try { dispose(); } catch (_) { /* ignore */ }
      }
    };

    const close = (reason) => {
      if (closed) return;
      send({ type: 'done', sessionId, ...(reason ? { reason } : {}) });
      closed = true;
      try { core.sseEnd(res); } catch (_) { /* ignore */ }
      cleanup();
    };

    res.on('close', () => {
      if (!closed && !res.writableEnded) cleanup();
    });

    let idleSince = null;
    const checkDone = () => {
      if (closed || idleSince === null) return;
      const quiet = Date.now() - idleSince;
      if (turnsStarted > 0 && turnsEnded >= turnsStarted && !pendingQuestion && !pendingApproval) {
        if (quiet >= cfg.doneQuietMs) { close(); return; }
      } else if (quiet >= 15000) {
        // 长时间空闲且无法确认 turn 平衡 (异常兜底), 避免流永久挂起
        close('idle-timeout');
        return;
      }
      doneTimer = setTimeout(checkDone, 200);
    };
    disposers.push(ctx.on('agent/status', ({ agent, status }) => {
      if (closed || !agent || agent.id !== sessionId) return;
      if (status === 'running') {
        idleSince = null;
        if (doneTimer) { clearTimeout(doneTimer); doneTimer = null; }
        return;
      }
      if (status === 'error') { close('agent-error'); return; }
      // idle: 启动周期检查 (turn 平衡且静默达到 doneQuietMs 后收尾)
      idleSince = Date.now();
      if (doneTimer === null) checkDone();
    }));
    disposers.push(ctx.on('agent/error', ({ agent, error }) => {
      if (closed || !agent || agent.id !== sessionId) return;
      send({ type: 'error', message: error?.message ?? String(error) });
      close('agent-error');
    }));

    heartbeat = setInterval(() => send({ type: 'ping' }), 15000);

    send({ type: 'status', status: 'starting', sessionId });

    const frames = mux(signal);
    const pump = (async () => {
      try {
        for await (const frame of frames) {
          if (closed) break;
          const payload = frame?.payload;
          if (!payload || payload.sessionId !== sessionId) continue;
          if (payload.type === 'session/event') {
            const event = payload.event;
            if (!event) continue;
            if (event.type === 'assistant/chunk') {
              const piece = core.chunkText(event);
              if (piece === null) continue;
              if (typeof piece === 'string') send({ type: 'text', text: piece });
              else if (cfg.includeThinking && piece.thinking) send({ type: 'thinking', text: piece.thinking });
            } else if (event.type === 'tool/call' && cfg.includeToolEvents) {
              const name = core.toolBrief(event, payload.view, 300) || event.data?.name || 'tool';
              const callId = typeof event.data?.callId === 'string' ? event.data.callId : undefined;
              if (callId) toolNames.set(callId, name);
              send({ type: 'tool', status: 'call', name, callId });
            } else if (event.type === 'tool/result' && cfg.includeToolEvents) {
              const callId = typeof event.data?.message?.source?.callId === 'string'
                ? event.data.message.source.callId
                : undefined;
              const known = callId ? toolNames.get(callId) : undefined;
              send({
                type: 'tool',
                status: 'result',
                name: known || core.toolBrief(event, payload.view, 300) || '工具完成',
                callId,
              });
            } else if (event.type === 'turn/start') {
              turnsStarted += 1;
              send({ type: 'status', status: 'running' });
            } else if (event.type === 'turn/end') {
              turnsEnded += 1;
              toolNames.clear();
            }
          } else if (payload.type === 'question/requested') {
            pendingQuestion = true;
            send({ type: 'question', rpcId: frame.rpcId, questions: payload.questions ?? [] });
          } else if (payload.type === 'approval/requested') {
            pendingApproval = true;
            send({
              type: 'approval',
              rpcId: frame.rpcId,
              approvalId: payload.approvalId,
              toolName: payload.toolName,
              ...(payload.reason === undefined ? {} : { reason: payload.reason }),
            });
          } else if (payload.type === 'question/resolved') {
            pendingQuestion = false;
            send({ type: 'status', status: 'answered' });
          } else if (payload.type === 'approval/resolved') {
            pendingApproval = false;
            send({ type: 'status', status: 'approval-resolved', outcome: payload.outcome });
          }
        }
      } catch (err) {
        if (!closed && !signal.aborted) {
          send({ type: 'error', message: err?.message ?? String(err) });
        }
      }
    })();

    const result = unwrap(await sessions().prompt({
      rpcId: newRpcId(),
      payload: { sessionId, mode: 'queue', content: [{ type: 'text', text }] },
    }));
    if (result?.ok !== true) {
      const e = result?.error ?? {};
      send({ type: 'error', message: e.message ?? 'prompt rejected' });
      close('prompt-rejected');
      return;
    }
    send({ type: 'status', status: 'accepted' });
    hardTimer = setTimeout(() => close('timeout'), cfg.maxStreamMs);
    await pump;
  };

  /* ---------------------------------------------------------------- */
  /* 路由注册                                                          */
  /* ---------------------------------------------------------------- */

  const router = async (req, res) => {
    const pathname = new URL(req.url ?? '/', 'http://dsh.local').pathname;
    switch (pathname) {
      case '/api/bot/health': return req.method === 'GET' ? handleHealth(req, res) : reject(res, 405, 'method-not-allowed', 'GET only');
      case '/api/bot/session': return req.method === 'GET' ? handleSession(req, res) : reject(res, 405, 'method-not-allowed', 'GET only');
      case '/api/bot/history': return req.method === 'GET' ? handleHistory(req, res) : reject(res, 405, 'method-not-allowed', 'GET only');
      case '/api/bot/reset': return req.method === 'POST' ? handleReset(req, res) : reject(res, 405, 'method-not-allowed', 'POST only');
      case '/api/bot/answer': return req.method === 'POST' ? handleAnswer(req, res) : reject(res, 405, 'method-not-allowed', 'POST only');
      case '/api/bot/approve': return req.method === 'POST' ? handleApprove(req, res) : reject(res, 405, 'method-not-allowed', 'POST only');
      case '/api/bot/prompt': return req.method === 'POST' ? handlePrompt(req, res) : reject(res, 405, 'method-not-allowed', 'POST only');
      default: return reject(res, 404, 'not-found', 'no such bot endpoint');
    }
  };

  ctx.effect(
    () => ctx.webServer.register({ kind: 'prefix', path: '/api/bot', handler: router }),
    'dsh-bot-bridge: /api/bot route',
  );
}