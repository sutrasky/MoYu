#!/usr/bin/env node
/* global localStorage */
/** Records the DOM facts next to the M5-B before/after screenshots. */
import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

import { chromium } from 'playwright';

const TOKEN = process.env.M5B_SESSION_TOKEN;
const OUT = resolve(process.env.M5B_OUT ?? 'docs/evidence/m5b');
if (!TOKEN) throw new Error('M5B_SESSION_TOKEN is required');
const bases = {
  before: process.env.M5B_BEFORE_URL ?? 'http://127.0.0.1:4601',
  after: process.env.M5B_AFTER_URL ?? 'http://127.0.0.1:4600',
};
const browser = await chromium.launch({ headless: true });
const texts = {};
for (const [key, base] of Object.entries(bases)) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await context.addInitScript(() => localStorage.setItem('moyu.theme', 'light'));
  await context.addCookies([
    { name: 'moyu_session', value: TOKEN, domain: '127.0.0.1', path: '/' },
  ]);
  const page = await context.newPage();
  await page.goto(base, { waitUntil: 'domcontentloaded' });
  await page.waitForSelector('select', { timeout: 30_000 });
  const options = await page.locator('select').first().locator('option').allTextContents();
  const match = options.find((text) => text.includes('m5b-ui-beta'));
  await page.locator('select').first().selectOption({ label: match });
  await page.waitForTimeout(900);
  texts[key] = await page.locator('aside').first().innerText();
  await context.close();
}
await browser.close();
const records = JSON.parse(readFileSync(resolve(OUT, 'records.json'), 'utf8'));
const result = {
  capturedAt: new Date().toISOString(),
  beforeUrl: bases.before,
  afterUrl: bases.after,
  beforeHasExecutionGeneration: texts.before.includes('执行代次'),
  afterHasExecutionGeneration: texts.after.includes('执行代次'),
  beforeHasMemoryWait: texts.before.includes('等待内存余量'),
  afterHasMemoryWait: texts.after.includes('等待内存余量'),
  beforeSidebar: texts.before,
  afterSidebar: texts.after,
  records,
};
writeFileSync(resolve(OUT, 'ui-checks.json'), JSON.stringify(result, null, 2));
console.log(
  JSON.stringify({
    beforeHasExecutionGeneration: result.beforeHasExecutionGeneration,
    afterHasExecutionGeneration: result.afterHasExecutionGeneration,
    beforeHasMemoryWait: result.beforeHasMemoryWait,
    afterHasMemoryWait: result.afterHasMemoryWait,
  }),
);
