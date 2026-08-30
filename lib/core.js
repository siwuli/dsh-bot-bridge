/**
 * dsh-bot-bridge 核心纯函数 (无 DSH 依赖, 便于单测)
 *
 * 职责: 令牌校验 / 请求体解析 / SSE 帧写入 / clientId→sessionId 存储 /
 * 会话历史文本抽取 / 工具视图摘要。
 */

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

/** 常量时间字符串比较 (令牌/口令校验) */
export function safeEqual(a, b) {
  const ba = Buffer.from(String(a), 'utf8');
  const bb = Buffer.from(String(b), 'utf8');
  if (ba.length !== bb.length) return false;
  return crypto.timingSafeEqual(ba, bb);
}

/** 从 Authorization 头提取 Bearer 令牌 */
export function bearerToken(headers, name) {
  const raw = headers[name];
  if (typeof raw !== 'string') return null;
  const m = /^Bearer\s+(.+)$/i.exec(raw.trim());
  if (!m) return null;
  return m[1].trim();
}

/** 从请求头表读取一个字符串头 */
export function headerValue(headers, name) {
  const v = headers[name];
  return typeof v === 'string' ? v : undefined;
}

/** 判断请求是否携带合法令牌 (支持 Authorization: Bearer 或 x-bot-token 头) */
export function requestAuthed(headers, token) {
  const bearer = bearerToken(headers, 'authorization');
  if (bearer !== null && bearer !== '') return safeEqual(bearer, token);
  const plain = headerValue(headers, 'x-bot-token');
  if (plain !== undefined && plain !== '') return safeEqual(plain, token);
  return false;
}

/** 读取并解析 JSON 请求体 (限流/超限拒绝) */
export async function readJsonBody(req, capBytes = 1024 * 1024) {
  const chunks = [];
  let total = 0;
  for await (const chunk of req) {
    total += chunk.length;
    if (total > capBytes) {
      const err = new Error('request body too large');
      err.code = 'body-too-large';
      throw err;
    }
    chunks.push(chunk);
  }
  if (total === 0) {
    const err = new Error('empty request body');
    err.code = 'empty-body';
    throw err;
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    const err = new Error('body is not valid JSON');
    err.code = 'bad-json';
    throw err;
  }
}

/** 写一行 JSON (普通 HTTP 响应) */
export function writeJson(res, body, status = 200) {
  const payload = Buffer.from(JSON.stringify(body), 'utf8');
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': payload.length,
    'cache-control': 'no-store',
  });
  res.end(payload);
}

/** 开始一个 SSE 响应 */
export function sseStart(res) {
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
  });
}

/** 发送一个 SSE 帧 (JSON) */
export function sseSend(res, obj) {
  res.write('data: ' + JSON.stringify(obj) + '\n\n');
}

/** 正常结束 SSE 响应 */
export function sseEnd(res) {
  res.end();
}

/** 是否绝对路径 (Windows 盘符/UNC 或 POSIX /) */
export function isAbsolutePath(p) {
  const s = String(p ?? '').trim();
  return /^[A-Za-z]:[\\/]/.test(s) || s.startsWith('\\\\') || s.startsWith('//') || s.startsWith('/');
}

/** 路径规范化 (统一分隔符/盘符大小写, 用于工作区比较) */
/** 路径规范化 (统一分隔符/盘符大小写, 用于工作区比较) */
export function normalizePath(p) {
  return String(p ?? '').replace(/\\/g, '/').replace(/^([A-Za-z]):/, (m) => m.toLowerCase());
}

/** 文本截断 (按码点, 保留尾部提示) */
export function truncate(text, max) {
  if (typeof text !== 'string') return '';
  const t = text.replace(/\r\n/g, '\n').trim();
  if (t.length <= max) return t;
  const head = t.slice(0, max);
  return head.slice(0, Math.max(0, head.length - 12)) + '…(截断)';
}

/* ------------------------------------------------------------------ */
/* clientId → sessionId 存储                                            */
/* ------------------------------------------------------------------ */

export function loadStore(file) {
  try {
    if (fs.existsSync(file)) {
      const raw = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (raw && typeof raw === 'object' && !Array.isArray(raw)) return raw;
    }
  } catch (_) { /* ignore */ }
  return {};
}

export function saveStore(file, store) {
  try {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify(store, null, 2), 'utf8');
  } catch (_) { /* ignore */ }
}

/* ------------------------------------------------------------------ */
/* 会话历史 / 工具视图摘要                                              */
/* ------------------------------------------------------------------ */

/** 从 content blocks 里抽出文本 (默认跳过 thinking/reasoning) */
export function textOfBlocks(blocks, includeThinking = false) {
  if (!Array.isArray(blocks)) return '';
  const parts = [];
  for (const block of blocks) {
    if (!block || typeof block !== 'object') continue;
    if (block.type === 'text' && typeof block.text === 'string') parts.push(block.text);
    if (includeThinking && (block.type === 'reasoning' || block.type === 'thinking') && typeof block.text === 'string') parts.push(block.text);
  }
  return parts.join('\n').trim();
}

/** 工具调用/结果的一行摘要 (优先 presenter view) */
export function toolBrief(event, view, max = 300) {
  const data = event?.data;
  const name = typeof data?.name === 'string' ? data.name : '';
  const card = view?.view;
  if (card && typeof card.title === 'string' && card.title) return truncate(card.title, max);
  if (card && typeof card.description === 'string' && card.description) return truncate(card.description, max);
  if (name) return truncate(name, max);
  return '';
}

/** 从 assistant/chunk 事件里取文本增量 (text-delta 返回文本, thinking 返回对象, 其余返回 null) */
export function chunkText(event) {
  const data = event?.data;
  if (!data || typeof data !== 'object') return null;
  const chunk = data.chunk;
  if (!chunk || typeof chunk !== 'object') return null;
  if (chunk.type === 'text-delta' && typeof chunk.text === 'string') return chunk.text;
  if ((chunk.type === 'reasoning-delta' || chunk.type === 'thinking-delta') && typeof chunk.text === 'string') {
    return { thinking: chunk.text };
  }
  return null;
}

export default {
  safeEqual,
  bearerToken,
  headerValue,
  requestAuthed,
  readJsonBody,
  writeJson,
  sseStart,
  sseSend,
  sseEnd,
  truncate,
  isAbsolutePath,
  normalizePath,
  loadStore,
  saveStore,
  textOfBlocks,
  toolBrief,
  chunkText,
};