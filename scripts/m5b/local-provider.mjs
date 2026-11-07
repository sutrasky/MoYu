#!/usr/bin/env node
/**
 * Loopback-only deterministic OpenAI-completions provider for an isolated M5-B
 * runtime. It is test infrastructure: never point a production profile at it.
 *
 * Usage: LOCAL_PROVIDER_PORT=3801 node scripts/m5b/local-provider.mjs
 * The profile must use baseURL http://127.0.0.1:<port>/v1, model `local-test`
 * and a dummy apiKeyEnv. Requests are logged to LOCAL_PROVIDER_LOG.
 */
import http from 'node:http';
import fs from 'node:fs';

const port = Number(process.env.LOCAL_PROVIDER_PORT ?? 3801);
const log = process.env.LOCAL_PROVIDER_LOG ?? '/tmp/moyu-local-provider.jsonl';
let sequence = 0;

const readBody = (request) =>
  new Promise((resolve, reject) => {
    let body = '';
    request.on('data', (chunk) => {
      body += chunk;
    });
    request.on('end', () => resolve(body));
    request.on('error', reject);
  });

function completion(model, choice) {
  return {
    id: `chatcmpl-local-${++sequence}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [choice],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  };
}

const server = http.createServer(async (request, response) => {
  if (request.method === 'GET' && request.url === '/v1/models') {
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(
      JSON.stringify({
        object: 'list',
        data: [{ id: 'local-test', object: 'model', owned_by: 'local' }],
      }),
    );
    return;
  }
  if (request.method !== 'POST' || request.url !== '/v1/chat/completions') {
    response.writeHead(404);
    response.end('not found');
    return;
  }
  const raw = await readBody(request);
  let body = {};
  try {
    body = JSON.parse(raw);
  } catch {
    /* a malformed request is answered deterministically below */
  }
  const tools = (body.tools ?? []).map((tool) => tool?.function?.name).filter(Boolean);
  fs.appendFileSync(
    log,
    JSON.stringify({
      at: new Date().toISOString(),
      stream: !!body.stream,
      model: body.model,
      tools,
      roles: (body.messages ?? []).map((message) => message.role),
    }) + '\n',
  );
  const id = `chatcmpl-local-${++sequence}`;
  // Deterministic route decision for an isolated probe. Replace the trigger id
  // with the candidate card's id when driving a different fixture.
  const decision = {
    decisions: [
      {
        kind: 'accept',
        trigger_id: process.env.LOCAL_PROVIDER_TRIGGER_ID ?? 'workspace:probe-1',
        title: 'Fix README probe line',
        worktree_name: 'fix-readme-probe',
        acknowledgement: 'On it: updating the README probe line.',
      },
    ],
  };
  const sawRouteResult = (body.messages ?? []).some(
    (message) =>
      message.role === 'tool' &&
      /accepted|ticket|decision|refused/i.test(String(message.content ?? '')),
  );
  const choice =
    tools.includes('orchestrator_route') && !sawRouteResult
      ? {
          index: 0,
          message: {
            role: 'assistant',
            content: null,
            tool_calls: [
              {
                id: `call_${id}`,
                type: 'function',
                function: { name: 'orchestrator_route', arguments: JSON.stringify(decision) },
              },
            ],
          },
          finish_reason: 'tool_calls',
        }
      : {
          index: 0,
          message: { role: 'assistant', content: 'Deterministic local provider response.' },
          finish_reason: 'stop',
        };
  fs.appendFileSync(
    log,
    JSON.stringify({
      at: new Date().toISOString(),
      response: {
        id,
        tools,
        sawRouteResult,
        choice: {
          finish: choice.finish_reason,
          tool: choice.message.tool_calls?.[0]?.function?.name ?? null,
          arguments: choice.message.tool_calls?.[0]?.function?.arguments ?? null,
        },
      },
    }) + '\n',
  );
  if (!body.stream) {
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify(completion(body.model ?? 'local-test', choice)));
    return;
  }
  response.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
  });
  const delta =
    choice.finish_reason === 'tool_calls'
      ? {
          role: 'assistant',
          tool_calls: choice.message.tool_calls.map((call, index) => ({ index, ...call })),
        }
      : { role: 'assistant', content: choice.message.content };
  for (const value of [
    { choices: [{ index: 0, delta, finish_reason: null }] },
    { choices: [{ index: 0, delta: {}, finish_reason: choice.finish_reason }] },
  ])
    response.write(
      `data: ${JSON.stringify({
        id,
        object: 'chat.completion.chunk',
        created: Math.floor(Date.now() / 1000),
        model: body.model ?? 'local-test',
        ...value,
      })}\n\n`,
    );
  response.write('data: [DONE]\n\n');
  response.end();
});
server.listen(port, '127.0.0.1', () =>
  console.log(`local provider listening on 127.0.0.1:${port}`),
);
