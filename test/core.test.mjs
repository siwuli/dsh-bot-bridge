import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  safeEqual, bearerToken, requestAuthed, truncate,
  textOfBlocks, toolBrief, chunkText, loadStore, saveStore,
} from '../lib/core.js';

// 1. 常量时间比较
assert.ok(safeEqual('abc', 'abc'));
assert.ok(!safeEqual('abc', 'abd'));
assert.ok(!safeEqual('abc', 'abcd'));

// 2. Bearer 令牌提取
assert.strictEqual(bearerToken({ authorization: 'Bearer tok-1' }, 'authorization'), 'tok-1');
assert.strictEqual(bearerToken({ authorization: 'bearer   tok-2 ' }, 'authorization'), 'tok-2');
assert.strictEqual(bearerToken({}, 'authorization'), null);
assert.strictEqual(bearerToken({ authorization: 'Basic x' }, 'authorization'), null);

// 3. 请求认证
assert.ok(requestAuthed({ authorization: 'Bearer tok-1' }, 'tok-1'));
assert.ok(requestAuthed({ 'x-bot-token': 'tok-1' }, 'tok-1'));
assert.ok(!requestAuthed({ authorization: 'Bearer wrong' }, 'tok-1'));
assert.ok(!requestAuthed({}, 'tok-1'));

// 4. 截断
assert.strictEqual(truncate('hi', 10), 'hi');
assert.ok(truncate('x'.repeat(100), 40).length <= 40);
assert.ok(truncate('x'.repeat(100), 40).includes('截断'));

// 5. 文本块抽取 (默认排除 thinking)
const blocks = [
  { type: 'text', text: '回答A' },
  { type: 'thinking', text: '内心活动' },
  { type: 'reasoning', text: '推理过程' },
];
assert.strictEqual(textOfBlocks(blocks), '回答A');
assert.strictEqual(textOfBlocks(blocks, true), '回答A\n内心活动\n推理过程');
assert.strictEqual(textOfBlocks(null), '');

// 6. chunk 文本增量
assert.strictEqual(chunkText({ data: { turn: 1, step: 1, chunk: { type: 'text-delta', index: 0, text: '你好' } } }), '你好');
assert.deepStrictEqual(chunkText({ data: { turn: 1, step: 1, chunk: { type: 'reasoning-delta', index: 0, text: '想' } } }), { thinking: '想' });
assert.strictEqual(chunkText({ data: { turn: 1, step: 1, chunk: { type: 'tool-call-delta', index: 0 } } }), null);
assert.strictEqual(chunkText({}), null);

// 7. 工具摘要
assert.strictEqual(toolBrief({ data: { name: 'bash' } }, null, 300), 'bash');
assert.strictEqual(toolBrief({}, { view: { title: '运行命令 ls' } }, 300), '运行命令 ls');
assert.strictEqual(toolBrief({}, null, 300), '');

// 8. store 读写
const tmp = path.join(os.tmpdir(), 'dsh-bot-bridge-test-' + Date.now() + '.json');
saveStore(tmp, { 'qq:1': 's1' });
assert.deepStrictEqual(loadStore(tmp), { 'qq:1': 's1' });
fs.unlinkSync(tmp);
assert.deepStrictEqual(loadStore(tmp), {});

console.log('✓ 全部 8 组核心测试通过 (core ESM)');
