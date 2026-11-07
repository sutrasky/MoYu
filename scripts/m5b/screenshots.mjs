#!/usr/bin/env node
/* global localStorage */
/**
 * M5-B UI comparison: the main build's web app (before) and this branch's build
 * (after), both served by real API processes against the same isolated database.
 * Runtime rows are deterministic fixtures for the visual states; backend lease
 * and resource behaviour is covered by the integration tests, not this script.
 */
import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

import { chromium } from 'playwright';

const AFTER = process.env.M5B_AFTER_URL ?? 'http://127.0.0.1:4600';
const BEFORE = process.env.M5B_BEFORE_URL ?? 'http://127.0.0.1:4601';
const TOKEN = process.env.M5B_SESSION_TOKEN;
const OUT = resolve(process.env.M5B_OUT ?? 'docs/evidence/m5b');
const ALPHA = 'Bouyer12/m5b-ui-alpha';
const BETA = 'Bouyer12/m5b-ui-beta';
const GAMMA = 'Bouyer12/m5b-ui-gamma';
if (!TOKEN) throw new Error('M5B_SESSION_TOKEN is required');
mkdirSync(OUT, { recursive: true });

const browser = await chromium.launch({ headless: true });
const records = [];

async function open(base, theme, viewport, repo, mobileNav = false) {
  const context = await browser.newContext({ viewport, colorScheme: theme });
  await context.addInitScript((mode) => localStorage.setItem('moyu.theme', mode), theme);
  await context.addCookies([
    { name: 'moyu_session', value: TOKEN, domain: '127.0.0.1', path: '/' },
  ]);
  const page = await context.newPage();
  await page.goto(base, { waitUntil: 'domcontentloaded' });
  await page.waitForSelector('select', { timeout: 30_000 });
  await page.waitForTimeout(800);
  if (mobileNav) {
    await page.getByRole('button', { name: '打开导航' }).click();
    await page.waitForTimeout(300);
  }
  const options = await page.locator('select').first().locator('option').allTextContents();
  const match = options.find((text) => text.includes(repo));
  if (!match) throw new Error(`repository option not found: ${repo}`);
  await page.locator('select').first().selectOption({ label: match });
  await page.waitForTimeout(900);
  return { context, page };
}

async function shot(name, base, theme, viewport, repo, assertions = [], mobileNav = false) {
  const { context, page } = await open(base, theme, viewport, repo, mobileNav);
  const text = await page
    .locator('aside')
    .first()
    .innerText()
    .catch(() => page.locator('body').innerText());
  for (const assertion of assertions) {
    if (!text.includes(assertion)) {
      throw new Error(`${name}: missing expected text ${JSON.stringify(assertion)} in sidebar`);
    }
  }
  const file = resolve(OUT, `${name}.png`);
  await page.screenshot({ path: file, fullPage: false });
  records.push({ name, file, base, theme, viewport, repo });
  writeFileSync(resolve(OUT, 'records.json'), JSON.stringify(records, null, 2));
  await context.close();
}

const desktop = { width: 1440, height: 900 };
const mobile = { width: 390, height: 844 };
const afterBeta = ['等待内存余量', '内存不足，等待实测余量'];
const afterGamma = ['自动重试已用尽', '已发生 OOM', '等待运行时'];

for (const theme of ['light', 'dark']) {
  await shot(`before-desktop-${theme}-beta-resource-wait`, BEFORE, theme, desktop, BETA);
  await shot(`after-desktop-${theme}-beta-resource-wait`, AFTER, theme, desktop, BETA, afterBeta);
  await shot(`before-mobile-${theme}-beta-resource-wait`, BEFORE, theme, mobile, BETA, [], true);
  await shot(
    `after-mobile-${theme}-beta-resource-wait`,
    AFTER,
    theme,
    mobile,
    BETA,
    afterBeta,
    true,
  );
}
await shot('before-desktop-light-gamma-retry-oom', BEFORE, 'light', desktop, GAMMA);
await shot('after-desktop-light-gamma-retry-oom', AFTER, 'light', desktop, GAMMA, afterGamma);
await shot('before-desktop-light-alpha-online', BEFORE, 'light', desktop, ALPHA);
await shot('after-desktop-light-alpha-online', AFTER, 'light', desktop, ALPHA, ['执行代次']);

// The same DOM facts are recorded next to the images so the comparison is
// reviewable without reading the pixels.
const beforeText = await (async () => {
  const { context, page } = await open(BEFORE, 'light', desktop, BETA);
  const text = await page.locator('aside').first().innerText();
  await context.close();
  return text;
})();
const afterText = await (async () => {
  const { context, page } = await open(AFTER, 'light', desktop, BETA);
  const text = await page.locator('aside').first().innerText();
  await context.close();
  return text;
})();
writeFileSync(
  resolve(OUT, 'ui-checks.json'),
  JSON.stringify(
    {
      capturedAt: new Date().toISOString(),
      beforeUrl: BEFORE,
      afterUrl: AFTER,
      beforeHasExecutionGeneration: beforeText.includes('执行代次'),
      afterHasExecutionGeneration: afterText.includes('执行代次'),
      beforeHasMemoryWait: beforeText.includes('等待内存余量'),
      afterHasMemoryWait: afterText.includes('等待内存余量'),
      records,
    },
    null,
    2,
  ),
);
await browser.close();
console.log(
  JSON.stringify({
    shots: records.length,
    beforeHasExecutionGeneration: beforeText.includes('执行代次'),
    afterHasExecutionGeneration: afterText.includes('执行代次'),
  }),
);
