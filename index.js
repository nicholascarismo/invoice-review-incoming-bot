import 'dotenv/config';
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import express from 'express';
import boltPkg from '@slack/bolt';

const { App } = boltPkg;
// ESM-safe file/dir resolution for this module
const __FILENAME = new URL(import.meta.url).pathname;
const __DIRNAME = path.dirname(__FILENAME);

/* =========================
   Env & Config
========================= */
const {
  SLACK_BOT_TOKEN,          // xoxb-***
  SLACK_APP_TOKEN,          // xapp-*** (Socket Mode)
  SLACK_SIGNING_SECRET,     // optional in Socket Mode; kept for completeness
  WATCH_CHANNEL_ID,         // channel to watch (C#**** messages)
  FLOWBOT_USER_ID,          // optional: exact user ID for FlowBot (e.g., U01234567)
  SHOPIFY_DOMAIN,           // e.g., mystore.myshopify.com
  SHOPIFY_ADMIN_TOKEN,      // Admin API access token
  SHOPIFY_API_VERSION = '2025-01',
  PORT = 3000
} = process.env;

if (!SLACK_BOT_TOKEN || !SLACK_APP_TOKEN) {
  console.error('Missing SLACK_BOT_TOKEN or SLACK_APP_TOKEN');
  process.exit(1);
}
if (!SHOPIFY_DOMAIN || !SHOPIFY_ADMIN_TOKEN) {
  console.error('Missing SHOPIFY_DOMAIN or SHOPIFY_ADMIN_TOKEN');
  process.exit(1);
}

/* =========================
   Data Store (./data)
========================= */
const DATA_DIR = path.resolve('./data');
const ORDERS_DIR = path.join(DATA_DIR, 'orders');

// Suppliers list (editable JSON file, lives NEXT TO index.js)
const SUPPLIERS_FILE = path.join(__DIRNAME, 'suppliers.json');

async function ensureSuppliersFile() {
  try {
    await fsp.access(SUPPLIERS_FILE, fs.constants.F_OK);
  } catch {
    const defaultSuppliers = [
      "OHC",
      "Bospeed",
      "Acme Parts",
      "Contoso Components"
    ];
    await fsp.writeFile(SUPPLIERS_FILE, JSON.stringify(defaultSuppliers, null, 2), 'utf8');
    console.log(`[init] Created ${SUPPLIERS_FILE} with default suppliers`);
  }
}

async function loadSuppliers() {
  try {
    const txt = await fsp.readFile(SUPPLIERS_FILE, 'utf8');
    const list = JSON.parse(txt);
    if (Array.isArray(list)) {
      // normalize to non-empty trimmed strings, dedup, and cap at Slack's option limit (100)
      const normalized = Array.from(
        new Set(
          list
            .map(s => (typeof s === 'string' ? s.trim() : ''))
            .filter(Boolean)
        )
      );
      return normalized.slice(0, 100);
    }
    return [];
  } catch (e) {
    console.error(`[suppliers] failed to load ${SUPPLIERS_FILE}:`, e?.message || e);
    return [];
  }
}

async function ensureDirs() {
  await fsp.mkdir(DATA_DIR, { recursive: true });
  await fsp.mkdir(ORDERS_DIR, { recursive: true });
}

// Atomic JSON write
async function writeJsonAtomic(filePath, data) {
  const tmp = `${filePath}.tmp-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const json = JSON.stringify(data, null, 2);
  await fsp.writeFile(tmp, json, 'utf8');
  await fsp.rename(tmp, filePath);
}

async function readJsonSafe(filePath, fallback = null) {
  try {
    const txt = await fsp.readFile(filePath, 'utf8');
    return JSON.parse(txt);
  } catch {
    return fallback;
  }
}

// Run a worker over an array with a max concurrency
async function runWithConcurrency(max, items, worker) {
  const results = new Array(items.length);
  let i = 0;

  async function runner() {
    while (true) {
      const idx = i++;
      if (idx >= items.length) break;
      try {
        results[idx] = await worker(items[idx], idx);
      } catch (err) {
        results[idx] = err;
      }
    }
  }

  const workers = Array.from({ length: Math.min(max, items.length) }, runner);
  await Promise.all(workers);
  return results;
}

// ---- Global throttle for ALL Shopify calls (serialize + small gap) ----
let __shopifyGate = Promise.resolve();
const __SHOPIFY_MIN_GAP_MS = 400; // increase if you still see 429s

async function __withShopifyThrottle(fn) {
  const prev = __shopifyGate;
  let release;
  __shopifyGate = new Promise(res => { release = res; });
  await prev;            // wait for the previous Shopify call to finish & release
  try {
    return await fn();   // perform the wrapped Shopify call
  } finally {
    setTimeout(release, __SHOPIFY_MIN_GAP_MS); // wait a bit before allowing next call
  }
}

/* =========================
   Shopify Helpers
========================= */
const SHOPIFY_BASE = `https://${SHOPIFY_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}`;

async function shopifyFetch(pathname, { method = 'GET', headers = {}, body } = {}, attempt = 1) {
  const url = `${SHOPIFY_BASE}${pathname}`;
  const res = await __withShopifyThrottle(() => fetch(url, {
    method,
    headers: {
      'X-Shopify-Access-Token': SHOPIFY_ADMIN_TOKEN,
      'Content-Type': 'application/json',
      ...headers
    },
    body: body ? JSON.stringify(body) : undefined
  }));

  // Handle rate limiting / transient errors with backoff
  if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
    const retryAfterHeader = res.headers.get('Retry-After');
    const retryAfter = retryAfterHeader ? parseFloat(retryAfterHeader) * 1000 : Math.min(2000 * attempt, 10000);
    if (attempt <= 5) {
      console.warn(`Shopify ${res.status}. Retrying in ${retryAfter}ms (attempt ${attempt})...`);
      await new Promise(r => setTimeout(r, retryAfter));
      return shopifyFetch(pathname, { method, headers, body }, attempt + 1);
    }
  }

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Shopify ${method} ${pathname} failed: ${res.status} ${res.statusText} - ${text}`);
  }
  return res.json();
}


// Fetch all metafields for an order and return a map like {"namespace.key": "value"}
async function fetchOrderMetafields(orderId) {
  const data = await shopifyFetch(`/orders/${orderId}/metafields.json`);
  const out = {};
  for (const mf of (data.metafields || [])) {
    const ns = (mf.namespace || '').trim();
    const key = (mf.key || '').trim();
    const val = (mf.value ?? '').toString().trim();
    if (ns && key) out[`${ns}.${key}`] = val;
  }
  return out;
}

// Create or update a single order metafield (REST)
async function upsertOrderMetafield(orderId, namespace, key, value, typeHint) {
  // 1) List existing metafields for this order
  const list = await shopifyFetch(`/orders/${orderId}/metafields.json`);
  const existing = (list.metafields || []).find(m => m.namespace === namespace && m.key === key);

  if (existing) {
    // 2) Update existing metafield (do not force type; keep what's there)
    await shopifyFetch(`/metafields/${existing.id}.json`, {
      method: 'PUT',
      body: {
        metafield: {
          id: existing.id,
          value
        }
      }
    });
  } else {
    // 3) Create the metafield on the order (use provided type hint or single-line by default)
    await shopifyFetch(`/orders/${orderId}/metafields.json`, {
      method: 'POST',
      body: {
        metafield: {
          namespace,
          key,
          type: typeHint || 'single_line_text_field',
          value
        }
      }
    });
  }
}

// Delete a single order metafield if it exists
async function deleteOrderMetafield(orderId, namespace, key) {
  const list = await shopifyFetch(`/orders/${orderId}/metafields.json`);
  const existing = (list.metafields || []).find(m => m.namespace === namespace && m.key === key);
  if (existing) {
    await shopifyFetch(`/metafields/${existing.id}.json`, { method: 'DELETE' });
  }
}

// Build initial modal selections from metafields, following your exact rules
function buildInitialsFromMetafields(mfMap) {
  const v = (k) => (mfMap[k] || '').trim();

  const taggingDone = v('custom.initial_slack_tagging_done').toLowerCase();
  const useMeta = taggingDone === 'yes';

  // Defaults (when blank or "No")
  let partsSelections = ['steering_wheel']; // default pre-check Steering Wheel
  let fulfillment = 'ship';                 // default Ship
  let payment = 'pif';                      // default PIF
  let otherText = '';
  let setAsideText = '';

  if (useMeta) {
    partsSelections = [];
    // 1. Steering Wheel
    if (v('custom.parts_steering_wheel').toLowerCase() === 'steering wheel') {
      partsSelections.push('steering_wheel');
    }
    // 2. Trim
    if (v('custom.parts_trim').toLowerCase() === 'trim') {
      partsSelections.push('trim');
    }
    // 3. Paddles
    if (v('custom.parts_paddles').toLowerCase() === 'paddles') {
      partsSelections.push('paddles');
    }
    // 4. Magnetic Paddles
    if (v('custom.parts_magnetic_paddles').toLowerCase() === 'magnetic paddles') {
      partsSelections.push('magnetic_paddles');
    }
    // 5. DA Module
    if (v('custom.parts_da_module').toLowerCase() === 'da module') {
      partsSelections.push('da_module');
    }
    // 6. Return Label
    if (v('custom.parts_return_label').toLowerCase() === 'return label') {
      partsSelections.push('return_label');
    }
    // 7. Other text
    otherText = v('custom.parts_other'); // pre-fill if present
    // 8. Parts Set Aside text
    setAsideText = v('custom.parts_set_aside_already'); // pre-fill if present

if ((otherText || '').trim()) partsSelections.push('other');
if ((setAsideText || '').trim()) partsSelections.push('set_aside');

    // 9. Fulfillment
    const f = v('custom.ship_install_pickup');
    if (f === 'Ship') fulfillment = 'ship';
    else if (f === 'Install/Pickup') fulfillment = 'install_pickup';
    else if (f === 'TBD') fulfillment = 'tbd';
    else if (!f) fulfillment = 'ship'; // blank -> Ship

    // 10. Payment
    const p = v('custom.pif_or_not');
    if (p === 'PIF') payment = 'pif';
    else if (p === 'Deposit') payment = 'deposit';
    else if (p === 'PIF + Pre-Paid Install') payment = 'pif_prepaid_install';
    else if (p === 'Unpaid') payment = 'unpaid';
    else if (p === 'Unknown') payment = 'unknown';
    else if (!p) payment = 'unknown'; // blank -> Unknown
  }

  return { useMeta, partsSelections, fulfillment, payment, otherText, setAsideText };
}


// Look up by Shopify order "name", which is like "C#1234"
async function findOrderByName(orderNumber4Digits) {
  const encodedName = encodeURIComponent(`C#${orderNumber4Digits}`); // "C%231234"
  const data = await shopifyFetch(`/orders.json?name=${encodedName}&status=any`);
  const expected = `C#${orderNumber4Digits}`;
  const order = (data.orders || []).find(o => typeof o.name === 'string' && o.name === expected);
  if (!order) {
    throw new Error(`Order C#${orderNumber4Digits} not found`);
  }
  return order;
}

// Fetch order tags as an array (Shopify returns a comma-separated string)
async function fetchOrderTags(orderId) {
  const data = await shopifyFetch(`/orders/${orderId}.json?fields=tags`);
  const raw = data?.order?.tags || '';
  return raw.split(',').map(t => t.trim()).filter(Boolean);
}

// Read only the "note" field for an order
async function fetchOrderNote(orderId) {
  const data = await shopifyFetch(`/orders/${orderId}.json?fields=note`);
  return data?.order?.note || '';
}

// Update order "note"
async function updateOrderNote(orderId, note) {
  await shopifyFetch(`/orders/${orderId}.json`, {
    method: 'PUT',
    body: { order: { id: orderId, note } }
  });
}

// Replace full tag set for an order
async function updateOrderTags(orderId, tagsArray) {
  const tags = tagsArray.join(', ');
  await shopifyFetch(`/orders/${orderId}.json`, {
    method: 'PUT',
    body: { order: { id: orderId, tags } }
  });
}

/* =========================
   Slack App (Socket Mode)
========================= */
const app = new App({
  token: SLACK_BOT_TOKEN,
  appToken: SLACK_APP_TOKEN,
  signingSecret: SLACK_SIGNING_SECRET, // optional in Socket Mode
  socketMode: true,
  processBeforeResponse: true
});

// /ping -> pong
app.command('/ping', async ({ ack, respond, command, logger }) => {
  await ack();
  try {
    const where = command.channel_id ? `<#${command.channel_id}>` : 'here';
    await respond({ text: `pong (${where})` });
  } catch (e) {
    logger.error(e);
  }
});

// /invoice-review -> open modal to collect a list of order numbers
app.command('/invoice-review', async ({ ack, body, client, logger }) => {
  await ack();
  try {
    // Load suppliers and build dropdown options
    const suppliers = await loadSuppliers();
    const supplierOptions = suppliers.map(name => ({
      text: { type: 'plain_text', text: name },
      value: name
    }));

    await client.views.open({
      trigger_id: body.trigger_id,
      view: {
        type: 'modal',
        callback_id: 'invoice_review_collect_orders',
        title: { type: 'plain_text', text: 'Invoice Review' },
        submit: { type: 'plain_text', text: 'Confirm' },
        close: { type: 'plain_text', text: 'Cancel' },
        private_metadata: JSON.stringify({
          channel: body.channel_id,
          user: body.user_id
        }),
        blocks: [
          {
            type: 'input',
            block_id: 'supplier_block',
            label: { type: 'plain_text', text: 'Supplier' },
            element: {
              type: 'static_select',
              action_id: 'supplier_select',
              placeholder: { type: 'plain_text', text: supplierOptions.length ? 'Select a supplier' : 'No suppliers found' },
              options: supplierOptions.length
                ? supplierOptions
                : [{ text: { type: 'plain_text', text: '— No suppliers configured —' }, value: 'NO_SUPPLIERS' }]
            }
          },
          {
            type: 'input',
            block_id: 'invoice_date_block',
            label: { type: 'plain_text', text: 'Invoice Date' },
            element: {
              type: 'plain_text_input',
              action_id: 'invoice_date_input',
              multiline: false,
              placeholder: { type: 'plain_text', text: 'e.g. 10-23 or 8-6, etc.' }
            }
          },
          {
            type: 'input',
            block_id: 'orders_block',
            label: { type: 'plain_text', text: 'Paste order numbers (one per line)' },
            element: {
              type: 'plain_text_input',
              action_id: 'orders_input',
              multiline: true,
              placeholder: { type: 'plain_text', text: 'e.g.\nC#1234\nC#1235\nC#1236' }
            }
          }
        ]
      }
    });
  } catch (e) {
    logger.error('open /invoice-review modal failed:', e);
  }
});

// Generic error logger
app.error((e) => {
  console.error('⚠️ Bolt error:', e?.message || e);
});


// Parse the list of order numbers, post a parent message, then one thread reply per order with the existing button
app.view('invoice_review_collect_orders', async ({ ack, body, view, client, logger }) => {
  await ack();

  try {
    const md = JSON.parse(view.private_metadata || '{}');
    const channel = md.channel;
    const userId = md.user;

    // NEW: read supplier + invoice date
    const invoiceSupplier = view.state.values?.supplier_block?.supplier_select?.selected_option?.value || '';
    const invoiceDate = (view.state.values?.invoice_date_block?.invoice_date_input?.value || '').trim();

    // 1) Parse textarea into unique 4-digit order numbers
    const raw = view.state.values?.orders_block?.orders_input?.value || '';
    const inputLines = raw.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
    const orderDigits = Array.from(new Set(
      inputLines
        .map(s => (s.match(/(\d{4})/) || [,''])[1])
        .filter(Boolean)
    ));

    if (!orderDigits.length) {
      await client.chat.postEphemeral({
        channel,
        user: userId,
        text: 'No valid 4-digit order numbers found.'
      });
      return;
    }

    // 2) Look up all orders now
    const found = [];   // [{digits, id, customerName}]
    const failed = [];  // [digits]
    for (const digits of orderDigits) {
      try {
        const order = await findOrderByName(digits);
        const customerName =
          order?.customer ? `${order.customer.first_name || ''} ${order.customer.last_name || ''}`.trim() :
          (order?.shipping_address ? `${order.shipping_address.first_name || ''} ${order.shipping_address.last_name || ''}`.trim() : 'Unknown');
        found.push({ digits, id: order.id, customerName });
      } catch (err) {
        logger.error(`Order C#${digits} lookup failed:`, err);
        failed.push(digits);
      }
    }

    if (!found.length) {
      await client.chat.postEphemeral({
        channel,
        user: userId,
        text: failed.length
          ? `No orders found. Failed lookups: ${failed.map(d => `C#${d}`).join(', ')}`
          : 'No orders found.'
      });
      return;
    }

    // 3) Post ONE parent message in the main channel (no buttons here)
    // CHANGE #1: remove "by <@${userId}>"
    const firstLine = `Invoice review started for ${found.length} order(s)`;

    // CHANGE #2: Invoice line uses a space instead of a long dash between supplier and date
    const headerInvoiceLine = (invoiceSupplier || invoiceDate)
      ? `*Invoice:* ${[invoiceSupplier, invoiceDate].filter(Boolean).join(' ')}`
      : null;

    const headline = [
      firstLine,
      headerInvoiceLine,
      failed.length ? `⚠️ Not found: ${failed.map(d => `C#${d}`).join(', ')}` : null
    ].filter(Boolean).join('\n');

    const parent = await client.chat.postMessage({
      channel,
      text: headline
    });
    const root_ts = parent.ts;

    // 4) Chunk into batches of up to 8 (to keep modal blocks < 100)
    const BATCH_SIZE = 8;
    const batches = [];
    for (let i = 0; i < found.length; i += BATCH_SIZE) {
      batches.push(found.slice(i, i + BATCH_SIZE));
    }

    // 5) For each batch, post a thread reply with its own "Update Metafields (All)" button
    let batchIndex = 1;
    for (const batch of batches) {
      const listLines = batch.map(o => `• C#${o.digits} — ${o.customerName || 'Unknown'}`).join('\n');
      const batchTitle = batches.length > 1 ? `Batch ${batchIndex}/${batches.length}` : 'Batch 1/1';

      await client.chat.postMessage({
        channel,
        thread_ts: root_ts,
        text: `${batchTitle}\n${listLines}`,
        blocks: [
          { type: 'section', text: { type: 'mrkdwn', text: `*${batchTitle}*\n${listLines}` } },
          {
            type: 'actions',
            elements: [
              {
                type: 'button',
                text: { type: 'plain_text', text: 'Update Metafields (All)', emoji: true },
                action_id: 'open_update_modal_bulk',
                value: JSON.stringify({
                  channel,
                  thread_ts: root_ts,
                  invoiceSupplier: invoiceSupplier || '',
                  invoiceDate: invoiceDate || '',
                  orders: batch   // only this batch goes to the modal
                })
              }
            ]
          }
        ]
      });

      batchIndex += 1;
    }

  } catch (e) {
    logger.error('invoice_review_collect_orders error:', e);
  }
});

/* =========================
   Open Modal
========================= */
app.action('open_update_modal', async ({ ack, body, client, logger, action }) => {
  await ack();

  let meta = { orderDigits: '', orderId: '', channel: '', thread_ts: '' };
  try {
    meta = JSON.parse(action.value || '{}');
  } catch (_) {}

  try {
    // 1) Pull order metafields
    const mfMap = await fetchOrderMetafields(meta.orderId);
    // 2) Compute initial selections from metafields per your rules
    const init = buildInitialsFromMetafields(mfMap);

    // Options list for Parts
    const PART_OPTIONS = [
      { text: { type: 'plain_text', text: 'Steering Wheel' }, value: 'steering_wheel' },
      { text: { type: 'plain_text', text: 'Trim' }, value: 'trim' },
      { text: { type: 'plain_text', text: 'Paddles' }, value: 'paddles' },
      { text: { type: 'plain_text', text: 'Magnetic Paddles' }, value: 'magnetic_paddles' },
      { text: { type: 'plain_text', text: 'DA Module' }, value: 'da_module' },
      { text: { type: 'plain_text', text: 'Return Label' }, value: 'return_label' },
      { text: { type: 'plain_text', text: 'Other (requires text)' }, value: 'other' },
      { text: { type: 'plain_text', text: 'Parts Set Aside (requires text)' }, value: 'set_aside' }
    ];

    const initialCheckboxOptions = PART_OPTIONS.filter(o =>
      init.partsSelections.includes(o.value)
    );

    // Build radio initial options
    const fulfillmentInitial = {
      text: { type: 'plain_text', text: init.fulfillment === 'install_pickup' ? 'Install/Pickup' : (init.fulfillment === 'tbd' ? 'TBD' : 'Ship') },
      value: init.fulfillment
    };

    const paymentLabel =
      init.payment === 'deposit' ? 'Deposit' :
      init.payment === 'pif_prepaid_install' ? 'PIF + Pre-Paid Install' :
      init.payment === 'unpaid' ? 'Unpaid' :
      init.payment === 'unknown' ? 'Unknown' : 'PIF';

    const paymentInitial = {
      text: { type: 'plain_text', text: paymentLabel },
      value: init.payment
    };

    await client.views.open({
      trigger_id: body.trigger_id,
      view: {
        type: 'modal',
        callback_id: 'update_meta_modal_submit',
        private_metadata: JSON.stringify(meta),
        title: { type: 'plain_text', text: `Order C#${meta.orderDigits}` },
        submit: { type: 'plain_text', text: 'Done' },
        close: { type: 'plain_text', text: 'Cancel' },
        blocks: [
          // Parts
          {
            type: 'header',
            text: { type: 'plain_text', text: 'Parts' }
          },
          {
            type: 'input',
            block_id: 'parts_block',
            optional: false,
            label: { type: 'plain_text', text: 'Select all that apply' },
            element: {
              type: 'checkboxes',
              action_id: 'parts_check',
              initial_options: initialCheckboxOptions,  // from metafields or default
              options: PART_OPTIONS
            }
          },
          {
            type: 'input',
            block_id: 'parts_other_text',
            optional: true,
            label: { type: 'plain_text', text: 'Other — details (required if selected above)' },
            element: {
              type: 'plain_text_input',
              action_id: 'other_text',
              multiline: false,
              initial_value: init.otherText || '',
              placeholder: { type: 'plain_text', text: 'Enter details if you checked "Other"' }
            }
          },
          {
            type: 'input',
            block_id: 'parts_set_aside_text',
            optional: true,
            label: { type: 'plain_text', text: 'Parts Set Aside — details (required if selected above)' },
            element: {
              type: 'plain_text_input',
              action_id: 'set_aside_text',
              multiline: false,
              initial_value: init.setAsideText || '',
              placeholder: { type: 'plain_text', text: 'Enter details if you checked "Parts Set Aside"' }
            }
          },

          // Fulfillment
          { type: 'divider' },
          { type: 'header', text: { type: 'plain_text', text: 'Fulfillment' } },
          {
            type: 'input',
            block_id: 'fulfillment_block',
            optional: false,
            label: { type: 'plain_text', text: 'Choose one' },
            element: {
              type: 'radio_buttons',
              action_id: 'fulfillment_radio',
              initial_option: fulfillmentInitial,
              options: [
                { text: { type: 'plain_text', text: 'Ship' }, value: 'ship' },
                { text: { type: 'plain_text', text: 'Install/Pickup' }, value: 'install_pickup' },
                { text: { type: 'plain_text', text: 'TBD' }, value: 'tbd' }
              ]
            }
          },

          // Payment
          { type: 'divider' },
          { type: 'header', text: { type: 'plain_text', text: 'Payment' } },
          {
            type: 'input',
            block_id: 'payment_block',
            optional: false,
            label: { type: 'plain_text', text: 'Choose one' },
            element: {
              type: 'radio_buttons',
              action_id: 'payment_radio',
              initial_option: paymentInitial,
              options: [
                { text: { type: 'plain_text', text: 'PIF' }, value: 'pif' },
                { text: { type: 'plain_text', text: 'Deposit' }, value: 'deposit' },
                { text: { type: 'plain_text', text: 'PIF + Pre-Paid Install' }, value: 'pif_prepaid_install' },
                { text: { type: 'plain_text', text: 'Unpaid' }, value: 'unpaid' },
                { text: { type: 'plain_text', text: 'Unknown' }, value: 'unknown' }
              ]
            }
          }
        ]
      }
    });
  } catch (e) {
    logger.error('open_update_modal error:', e);
  }
});

/* =========================
   Bulk: Open Modal for ALL orders on the invoice
========================= */
app.action('open_update_modal_bulk', async ({ ack, body, client, logger, action }) => {
  await ack();

  // Parse payload right away (now expecting supplier/date)
  let payload = { channel: '', thread_ts: '', invoiceSupplier: '', invoiceDate: '', orders: [] };
  try { payload = JSON.parse(action.value || '{}'); } catch (_) {}
  const { channel, thread_ts, invoiceSupplier, invoiceDate, orders } = payload;

  // Immediately open a "Loading..." modal (this uses the live trigger_id)
  const loading = await client.views.open({
    trigger_id: body.trigger_id,
    view: {
      type: 'modal',
      callback_id: 'dummy_loading_modal',
      title: { type: 'plain_text', text: 'Loading...' },
      close: { type: 'plain_text', text: 'Cancel' },
      blocks: [
        {
          type: 'section',
          text: { type: 'mrkdwn', text: 'Fetching current order metafields… Please wait.' }
        }
      ]
    }
  });
  const loadingViewId = loading.view.id;

  // Now proceed with the heavy Shopify fetching below

  // Each order consumes ~11 blocks; cap at 8 per modal to stay < 100
  const MAX_ORDERS = 8;
  const slice = orders.slice(0, MAX_ORDERS);
  const clipped = orders.length > slice.length;

  // Fetch initial metafields for each order to pre-populate
  const initialByOrder = {};
  for (const o of slice) {
    try {
      const mfMap = await fetchOrderMetafields(o.id);
      initialByOrder[o.digits] = buildInitialsFromMetafields(mfMap);
    } catch (e) {
      logger.error('fetchOrderMetafields failed for', o, e);
      initialByOrder[o.digits] = buildInitialsFromMetafields({});
    }
  }

  // Options list for Parts (shared)
  const PART_OPTIONS = [
    { text: { type: 'plain_text', text: 'Steering Wheel' }, value: 'steering_wheel' },
    { text: { type: 'plain_text', text: 'Trim' }, value: 'trim' },
    { text: { type: 'plain_text', text: 'Paddles' }, value: 'paddles' },
    { text: { type: 'plain_text', text: 'Magnetic Paddles' }, value: 'magnetic_paddles' },
    { text: { type: 'plain_text', text: 'DA Module' }, value: 'da_module' },
    { text: { type: 'plain_text', text: 'Return Label' }, value: 'return_label' },
    { text: { type: 'plain_text', text: 'Other (requires text)' }, value: 'other' },
    { text: { type: 'plain_text', text: 'Parts Set Aside (requires text)' }, value: 'set_aside' }
  ];

  // Build the per-order blocks (without header; we'll prepend header when updating the view)
  const orderBlocks = [];
  for (const o of slice) {
    const init = initialByOrder[o.digits];
    const initialCheckboxOptions = PART_OPTIONS.filter(p => init.partsSelections.includes(p.value));
    const fulfillmentInitial = {
      text: { type: 'plain_text', text: init.fulfillment === 'install_pickup' ? 'Install/Pickup' : (init.fulfillment === 'tbd' ? 'TBD' : 'Ship') },
      value: init.fulfillment
    };
    const paymentLabel =
      init.payment === 'deposit' ? 'Deposit' :
      init.payment === 'pif_prepaid_install' ? 'PIF + Pre-Paid Install' :
      init.payment === 'unpaid' ? 'Unpaid' :
      init.payment === 'unknown' ? 'Unknown' : 'PIF';
    const paymentInitial = { text: { type: 'plain_text', text: paymentLabel }, value: init.payment };

    orderBlocks.push({ type: 'divider' });
    orderBlocks.push({
  type: 'header',
  text: { type: 'plain_text', text: `Order C#${o.digits} • ${o.customerName || 'Unknown'}` }
});
    orderBlocks.push({
      type: 'input',
      block_id: `parts_block_${o.digits}`,
      label: { type: 'plain_text', text: 'Parts — select all that apply' },
      element: {
        type: 'checkboxes',
        action_id: `parts_check_${o.digits}`,
        initial_options: initialCheckboxOptions,
        options: PART_OPTIONS
      }
    });
    orderBlocks.push({
      type: 'input',
      block_id: `parts_other_text_${o.digits}`,
      optional: true,
      label: { type: 'plain_text', text: 'Other — details (required if selected above)' },
      element: {
        type: 'plain_text_input',
        action_id: `other_text_${o.digits}`,
        multiline: false,
        initial_value: init.otherText || '',
        placeholder: { type: 'plain_text', text: 'Enter details if you checked "Other"' }
      }
    });
    orderBlocks.push({
      type: 'input',
      block_id: `parts_set_aside_text_${o.digits}`,
      optional: true,
      label: { type: 'plain_text', text: 'Parts Set Aside — details (required if selected above)' },
      element: {
        type: 'plain_text_input',
        action_id: `set_aside_text_${o.digits}`,
        multiline: false,
        initial_value: init.setAsideText || '',
        placeholder: { type: 'plain_text', text: 'Enter details if you checked "Parts Set Aside"' }
      }
    });
    orderBlocks.push({ type: 'divider' });
    orderBlocks.push({ type: 'header', text: { type: 'plain_text', text: 'Fulfillment' } });
    orderBlocks.push({
      type: 'input',
      block_id: `fulfillment_block_${o.digits}`,
      label: { type: 'plain_text', text: 'Choose one' },
      element: {
        type: 'radio_buttons',
        action_id: `fulfillment_radio_${o.digits}`,
        initial_option: fulfillmentInitial,
        options: [
          { text: { type: 'plain_text', text: 'Ship' }, value: 'ship' },
          { text: { type: 'plain_text', text: 'Install/Pickup' }, value: 'install_pickup' },
          { text: { type: 'plain_text', text: 'TBD' }, value: 'tbd' }
        ]
      }
    });
    orderBlocks.push({ type: 'divider' });
    orderBlocks.push({ type: 'header', text: { type: 'plain_text', text: 'Payment' } });
    orderBlocks.push({
      type: 'input',
      block_id: `payment_block_${o.digits}`,
      label: { type: 'plain_text', text: 'Choose one' },
      element: {
        type: 'radio_buttons',
        action_id: `payment_radio_${o.digits}`,
        initial_option: paymentInitial,
        options: [
          { text: { type: 'plain_text', text: 'PIF' }, value: 'pif' },
          { text: { type: 'plain_text', text: 'Deposit' }, value: 'deposit' },
          { text: { type: 'plain_text', text: 'PIF + Pre-Paid Install' }, value: 'pif_prepaid_install' },
          { text: { type: 'plain_text', text: 'Unpaid' }, value: 'unpaid' },
          { text: { type: 'plain_text', text: 'Unknown' }, value: 'unknown' }
        ]
      }
    });
orderBlocks.push({ type: 'divider' });
  }

const invoiceHeader = (invoiceSupplier || invoiceDate)
  ? `${[invoiceSupplier, invoiceDate].filter(Boolean).join(' ')} Invoice`
  : 'Invoice Review';

  // Compose final blocks with header + optional clipped note + per-order blocks
  const finalBlocks = [
    {
      type: 'header',
      text: { type: 'plain_text', text: invoiceHeader }
    },
    ...(clipped ? [{
      type: 'section',
      text: { type: 'mrkdwn', text: `Showing first ${slice.length} orders (Slack modal limit).` }
    }] : []),
    ...orderBlocks
  ];

  await client.views.update({
    view_id: loadingViewId,
    view: {
      type: 'modal',
      callback_id: 'update_meta_modal_submit_bulk',
      private_metadata: JSON.stringify({
        channel,
        thread_ts,
        invoiceSupplier: invoiceSupplier || '',
        invoiceDate: invoiceDate || '',
        orders: slice
      }),
      title: { type: 'plain_text', text: 'Edit Invoice Orders' },
      submit: { type: 'plain_text', text: 'Done' },
      close: { type: 'plain_text', text: 'Cancel' },
      blocks: finalBlocks
    }
  });
});

/* =========================
   Modal Submission
========================= */
app.view('update_meta_modal_submit', async ({ ack, body, view, client, logger }) => {
  // Validate conditional inputs
  try {
    const state = view.state.values;
    // Extract core selections
    const fulfillmentVal = state?.fulfillment_block?.fulfillment_radio?.selected_option?.value || 'ship';
    const paymentVal = state?.payment_block?.payment_radio?.selected_option?.value || 'pif';

    const fulfillmentLabel =
      fulfillmentVal === 'install_pickup' ? 'Install/Pickup' :
      fulfillmentVal === 'tbd' ? 'TBD' : 'Ship';

    const paymentLabel =
      paymentVal === 'deposit' ? 'Deposit' :
      paymentVal === 'pif_prepaid_install' ? 'PIF + Pre-Paid Install' :
      paymentVal === 'unpaid' ? 'Unpaid' :
      paymentVal === 'unknown' ? 'Unknown' : 'PIF';

// Read current checkbox selections from the modal
let partsSelected = (state?.parts_block?.parts_check?.selected_options || []).map(o => o.value);

// Read the two text inputs
const otherText    = state?.parts_other_text?.other_text?.value?.trim();
const setAsideText = state?.parts_set_aside_text?.set_aside_text?.value?.trim();

// Rule #1: If any text is entered, treat the respective checkbox as selected
if ((otherText || '').trim() && !partsSelected.includes('other')) {
  partsSelected.push('other');
}
if ((setAsideText || '').trim() && !partsSelected.includes('set_aside')) {
  partsSelected.push('set_aside');
}

const partsSet = new Set(partsSelected);
const steeringWheelOn = partsSet.has('steering_wheel');
const trimOn          = partsSet.has('trim');
const paddlesOn       = partsSet.has('paddles');
const magPaddlesOn    = partsSet.has('magnetic_paddles');
const daModuleOn      = partsSet.has('da_module');
const returnLabelOn   = partsSet.has('return_label');

const otherSelected    = partsSet.has('other');
const setAsideSelected = partsSet.has('set_aside');

    const errors = {};
    if (otherSelected && !otherText) {
      errors['parts_other_text'] = 'Please provide details for "Other".';
    }
    if (setAsideSelected && !setAsideText) {
      errors['parts_set_aside_text'] = 'Please provide details for "Parts Set Aside".';
    }

    if (Object.keys(errors).length) {
      await ack({ response_action: 'errors', errors });
      return;
    }

    await ack();

    // Persist the selection snapshot
    const meta = JSON.parse(view.private_metadata || '{}');
    await ensureDirs();
    const filePath = path.join(ORDERS_DIR, `${meta.orderDigits}.json`);
    const snapshot = {
      saved_at: new Date().toISOString(),
      order_id: meta.orderId || null,
      order_digits: meta.orderDigits,
      parts: {
        selections: partsSelected,
        other_text: otherText || null,
        set_aside_text: setAsideText || null
      },
      fulfillment: state?.fulfillment_block?.fulfillment_radio?.selected_option?.value || 'ship',
      payment: state?.payment_block?.payment_radio?.selected_option?.value || 'pif'
    };
    // --- Begin Shopify metafield updates based on selections ---

    const yesNo = (on, yes, no) => (on ? yes : no);

    // 1) Parts (single line text)
    const mfOps = [
  upsertOrderMetafield(meta.orderId, 'custom', 'parts_steering_wheel',     yesNo(steeringWheelOn, 'Steering Wheel', 'No Steering Wheel')),
  upsertOrderMetafield(meta.orderId, 'custom', 'parts_trim',               yesNo(trimOn,          'Trim',            'No Trim')),
  upsertOrderMetafield(meta.orderId, 'custom', 'parts_paddles',            yesNo(paddlesOn,       'Paddles',         'No Paddles')),
  upsertOrderMetafield(meta.orderId, 'custom', 'parts_magnetic_paddles',   yesNo(magPaddlesOn,    'Magnetic Paddles','No Magnetic Paddles')),
  upsertOrderMetafield(meta.orderId, 'custom', 'parts_da_module',          yesNo(daModuleOn,      'DA Module',       'No DA Module')),
  upsertOrderMetafield(meta.orderId, 'custom', 'parts_return_label',       yesNo(returnLabelOn,   'Return Label',    'No Return Label')),
];

// parts_other: write when selected, otherwise DELETE if exists
if (otherSelected && (otherText || '').trim() !== '') {
  mfOps.push(upsertOrderMetafield(meta.orderId, 'custom', 'parts_other', (otherText || '').trim()));
} else {
  mfOps.push(deleteOrderMetafield(meta.orderId, 'custom', 'parts_other'));
}

// parts_set_aside_already: write when selected, otherwise DELETE if exists
if (setAsideSelected && (setAsideText || '').trim() !== '') {
  mfOps.push(upsertOrderMetafield(meta.orderId, 'custom', 'parts_set_aside_already', (setAsideText || '').trim()));
} else {
  mfOps.push(deleteOrderMetafield(meta.orderId, 'custom', 'parts_set_aside_already'));
}

    // 2) Fulfillment (single line text)
    mfOps.push(upsertOrderMetafield(meta.orderId, 'custom', 'ship_install_pickup', fulfillmentLabel));

    // 3) Payment (single line text)
    mfOps.push(upsertOrderMetafield(meta.orderId, 'custom', 'pif_or_not', paymentLabel));

    // 4) who_contacts = "Nick" (single line text)
    mfOps.push(upsertOrderMetafield(meta.orderId, 'custom', 'who_contacts', 'Nick'));

    // 5) parts_suppliers from tags starting with PartsSupplier_
    const tags = await fetchOrderTags(meta.orderId);
    const suppliers = tags
      .filter(t => t.startsWith('PartsSupplier_'))
      .map(t => t.substring('PartsSupplier_'.length))
      .filter(Boolean);
const suppliersCsv = suppliers.join(', ');
if (suppliersCsv) {
  mfOps.push(upsertOrderMetafield(meta.orderId, 'custom', 'parts_suppliers', suppliersCsv));
} else {
  mfOps.push(deleteOrderMetafield(meta.orderId, 'custom', 'parts_suppliers'));
}

    // 6) packing_slip_notes (multi-line text)
    const partsListForLine3 = [];
    if (steeringWheelOn) partsListForLine3.push('Steering Wheel');
    if (trimOn)          partsListForLine3.push('Trim');
    if (paddlesOn)       partsListForLine3.push('Paddles');
    if (magPaddlesOn)    partsListForLine3.push('Magnetic Paddles');
    if (daModuleOn)      partsListForLine3.push('DA Module');
    if (returnLabelOn)   partsListForLine3.push('Return Label');
    if (otherSelected && otherText) partsListForLine3.push(otherText);

    let line3 = partsListForLine3.join(', ');
    if (partsListForLine3.length === 1) {
      line3 = `${partsListForLine3[0]} only`;
    }

    const line1 = `${fulfillmentLabel.toUpperCase()} — ${paymentLabel.toUpperCase()}`;
const line2 = ''; // blank spacer line
const line4 = setAsideSelected && setAsideText ? `Should Be Set Aside Already: ${setAsideText}` : '';

const packingLines = [line1, line2, line3];
if (line4) {
  packingLines.push('');        // extra blank line before the set-aside note
  packingLines.push(line4);     // the set-aside note line
}
const packingSlipNotes = packingLines.join('\n');

    // Use multi_line_text_field for creation
    mfOps.push(upsertOrderMetafield(meta.orderId, 'custom', 'packing_slip_notes', packingSlipNotes, 'multi_line_text_field'));

    // 7) initial_slack_tagging_done = "Yes"
    mfOps.push(upsertOrderMetafield(meta.orderId, 'custom', 'initial_slack_tagging_done', 'Yes'));

    // Execute all metafield writes in parallel
    for (const op of mfOps) {
  await op;
}

    // --- End metafield updates ---
    await writeJsonAtomic(filePath, snapshot);


    // Confirm in thread
    if (meta.channel && meta.thread_ts) {
      await client.chat.postMessage({
        channel: meta.channel,
        thread_ts: meta.thread_ts,
        text: `Updated selections for Order C#${meta.orderDigits}: Parts / Fulfillment / Payment captured.`
      });
    }
  } catch (e) {
    logger.error('modal submit error:', e);
  }
});

/* =========================
   Bulk: Modal Submission
========================= */
app.view('update_meta_modal_submit_bulk', async ({ ack, body, view, client, logger }) => {
  // Per-order validation and save
  const md = JSON.parse(view.private_metadata || '{}');
const { channel, thread_ts, invoiceSupplier, invoiceDate, orders } = md;

  // Build a per-order extractor using the same rules as your single-order flow
  function parseOne(state, digits) {
    const getSel = (arr) => (arr || []).map(o => o.value);

    let partsSelected = getSel(state?.[`parts_block_${digits}`]?.[`parts_check_${digits}`]?.selected_options);
    const otherText    = state?.[`parts_other_text_${digits}`]?.[`other_text_${digits}`]?.value?.trim();
    const setAsideText = state?.[`parts_set_aside_text_${digits}`]?.[`set_aside_text_${digits}`]?.value?.trim();

    if ((otherText || '').trim() && !partsSelected.includes('other')) partsSelected.push('other');
    if ((setAsideText || '').trim() && !partsSelected.includes('set_aside')) partsSelected.push('set_aside');

    const fulfillmentVal = state?.[`fulfillment_block_${digits}`]?.[`fulfillment_radio_${digits}`]?.selected_option?.value || 'ship';
    const paymentVal     = state?.[`payment_block_${digits}`]?.[`payment_radio_${digits}`]?.selected_option?.value || 'pif';

    const fulfillmentLabel =
      fulfillmentVal === 'install_pickup' ? 'Install/Pickup' :
      fulfillmentVal === 'tbd' ? 'TBD' : 'Ship';

    const paymentLabel =
      paymentVal === 'deposit' ? 'Deposit' :
      paymentVal === 'pif_prepaid_install' ? 'PIF + Pre-Paid Install' :
      paymentVal === 'unpaid' ? 'Unpaid' :
      paymentVal === 'unknown' ? 'Unknown' : 'PIF';

    return {
      partsSelected,
      otherText: otherText || '',
      setAsideText: setAsideText || '',
      fulfillmentVal,
      fulfillmentLabel,
      paymentVal,
      paymentLabel
    };
  }

  // Validate all orders first (Other/Set Aside text requirements)
  const errors = {};
  const state = view.state.values || {};
  for (const o of orders) {
    const p = parseOne(state, o.digits);
    const set = new Set(p.partsSelected);
    const needsOther    = set.has('other') && !p.otherText;
    const needsSetAside = set.has('set_aside') && !p.setAsideText;
    if (needsOther)    errors[`parts_other_text_${o.digits}`] = 'Please provide details for "Other".';
    if (needsSetAside) errors[`parts_set_aside_text_${o.digits}`] = 'Please provide details for "Parts Set Aside".';
  }

  if (Object.keys(errors).length) {
    await ack({ response_action: 'errors', errors });
    return;
  }

  await ack();

// Save per order with limited concurrency and sequential metafield writes per order
const yesNo = (on, yes, no) => (on ? yes : no);

// Process ONE order at a time to avoid Shopify 429s
const results = await runWithConcurrency(1, orders, async (o) => {
  try {
    const mfOps = [];
  const p = parseOne(state, o.digits);
  const set = new Set(p.partsSelected);

  const steeringWheelOn = set.has('steering_wheel');
  const trimOn          = set.has('trim');
  const paddlesOn       = set.has('paddles');
  const magPaddlesOn    = set.has('magnetic_paddles');
  const daModuleOn      = set.has('da_module');
  const returnLabelOn   = set.has('return_label');
  const otherSelected    = set.has('other');
  const setAsideSelected = set.has('set_aside');

// ===== Arrange / Incoming / Back-end Incoming Invoice (prep) =====
const mfMap = await fetchOrderMetafields(o.id);
const currentArrangedWith = (mfMap['custom._nc_arranged_with'] || '').trim();

// Arrange status/tag action tracker
let arrangeTagAction = 'leave'; // 'leave' | 'remove'

// 2) Arrange logic
if (currentArrangedWith) {
  if (currentArrangedWith.includes('&')) {
    // multiple suppliers (future-proof: any number of "&")
    const parts = currentArrangedWith.split('&').map(s => s.trim()).filter(Boolean);
    const supplierToRemove = (invoiceSupplier || '').trim();
    const filtered = parts.filter(s => s !== supplierToRemove);
    const newValue = filtered.join(' & ');

    if (newValue && newValue !== currentArrangedWith) {
      // Try update; if Shopify rejects (enum not allowed), throw a helpful error
      mfOps.push((async () => {
        try {
          await upsertOrderMetafield(o.id, 'custom', '_nc_arranged_with', newValue);
        } catch (err) {
          throw new Error(
            `Order C#${o.digits}: cannot set custom._nc_arranged_with="${newValue}". ` +
            `Add this exact value to the allowed list in Shopify Admin, then retry.`
          );
        }
      })());
    }
    // Keep arrange_status as-is (should remain "Arranged") and DO NOT remove ArrangeStatus_Arranged tag.
  } else {
    // single supplier
    mfOps.push(deleteOrderMetafield(o.id, 'custom', 'arrange_status'));
    mfOps.push(deleteOrderMetafield(o.id, 'custom', '_nc_arranged_with'));
    arrangeTagAction = 'remove'; // remove ArrangeStatus_Arranged later
  }
}

// 3) Set custom.nc_incoming => "INCOMING" (always), and also update any other *nc_incoming* variants if present
mfOps.push(upsertOrderMetafield(o.id, 'custom', 'nc_incoming', 'INCOMING'));

const incomingKeys = Object.keys(mfMap)
  .filter(k => k.startsWith('custom.') && k.includes('nc_incoming'))
  .map(k => k.split('.')[1]) // keep only the key part
  .filter(k => k !== 'nc_incoming'); // we've already set the canonical key above
for (const k of incomingKeys) {
  mfOps.push(upsertOrderMetafield(o.id, 'custom', k, 'INCOMING'));
}

// 4) Append to custom._back_end_incoming_invoice
const currentBackEnd = (mfMap['custom._back_end_incoming_invoice'] || '').trim();
const appendLabel = `${(invoiceSupplier || '').trim()} ${(invoiceDate || '').trim()} Invoice`.trim();
const newBackEnd = currentBackEnd ? `${currentBackEnd}; ${appendLabel}` : appendLabel;
mfOps.push(upsertOrderMetafield(o.id, 'custom', '_back_end_incoming_invoice', newBackEnd));

  // 5) parts_suppliers from tags starting with PartsSupplier_
  const tags = await fetchOrderTags(o.id);
  const suppliers = tags
    .filter(t => t.startsWith('PartsSupplier_'))
    .map(t => t.substring('PartsSupplier_'.length))
    .filter(Boolean);
  const suppliersCsv = suppliers.join(', ');

  // 6) packing_slip_notes (multi-line text)
  const partsListForLine3 = [];
  if (steeringWheelOn) partsListForLine3.push('Steering Wheel');
  if (trimOn)          partsListForLine3.push('Trim');
  if (paddlesOn)       partsListForLine3.push('Paddles');
  if (magPaddlesOn)    partsListForLine3.push('Magnetic Paddles');
  if (daModuleOn)      partsListForLine3.push('DA Module');
  if (returnLabelOn)   partsListForLine3.push('Return Label');
  if (otherSelected && p.otherText) partsListForLine3.push(p.otherText);

  let line3 = partsListForLine3.join(', ');
  if (partsListForLine3.length === 1) line3 = `${partsListForLine3[0]} only`;

  const line1 = `${p.fulfillmentLabel.toUpperCase()} — ${p.paymentLabel.toUpperCase()}`;
const line2 = '';
const line4 = setAsideSelected && p.setAsideText ? `Should Be Set Aside Already: ${p.setAsideText}` : '';
const packingLines = [line1, line2, line3];
if (line4) {
  packingLines.push('');        // extra blank line before the set-aside note
  packingLines.push(line4);     // the set-aside note line
}
const packingSlipNotes = packingLines.join('\n');

    mfOps.push(
    upsertOrderMetafield(o.id, 'custom', 'parts_steering_wheel',   yesNo(steeringWheelOn, 'Steering Wheel', 'No Steering Wheel')),
    upsertOrderMetafield(o.id, 'custom', 'parts_trim',             yesNo(trimOn,          'Trim',            'No Trim')),
    upsertOrderMetafield(o.id, 'custom', 'parts_paddles',          yesNo(paddlesOn,       'Paddles',         'No Paddles')),
    upsertOrderMetafield(o.id, 'custom', 'parts_magnetic_paddles', yesNo(magPaddlesOn,    'Magnetic Paddles','No Magnetic Paddles')),
    upsertOrderMetafield(o.id, 'custom', 'parts_da_module',        yesNo(daModuleOn,      'DA Module',       'No DA Module')),
    upsertOrderMetafield(o.id, 'custom', 'parts_return_label',     yesNo(returnLabelOn,   'Return Label',    'No Return Label')),
    upsertOrderMetafield(o.id, 'custom', 'ship_install_pickup',    p.fulfillmentLabel),
    upsertOrderMetafield(o.id, 'custom', 'pif_or_not',             p.paymentLabel),
    upsertOrderMetafield(o.id, 'custom', 'who_contacts',           'Nick'),
    upsertOrderMetafield(o.id, 'custom', 'packing_slip_notes',     packingSlipNotes, 'multi_line_text_field'),
    upsertOrderMetafield(o.id, 'custom', 'initial_slack_tagging_done', 'Yes')
  );

  if (otherSelected && (p.otherText || '').trim() !== '') {
    mfOps.push(upsertOrderMetafield(o.id, 'custom', 'parts_other', (p.otherText || '').trim()));
  } else {
    mfOps.push(deleteOrderMetafield(o.id, 'custom', 'parts_other'));
  }

  if (setAsideSelected && (p.setAsideText || '').trim() !== '') {
    mfOps.push(upsertOrderMetafield(o.id, 'custom', 'parts_set_aside_already', (p.setAsideText || '').trim()));
  } else {
    mfOps.push(deleteOrderMetafield(o.id, 'custom', 'parts_set_aside_already'));
  }

  if (suppliersCsv) {
    mfOps.push(upsertOrderMetafield(o.id, 'custom', 'parts_suppliers', suppliersCsv));
  } else {
    mfOps.push(deleteOrderMetafield(o.id, 'custom', 'parts_suppliers'));
  }

  // SEQUENTIAL writes per order to avoid burst 429s
  for (const op of mfOps) {
    await op;
  }

// (2c) Remove the ArrangeStatus_Arranged tag ONLY if we cleared _nc_arranged_with (single-supplier case)
if (arrangeTagAction === 'remove') {
  const currentTags = await fetchOrderTags(o.id);
  const newTags = currentTags.filter(t => t !== 'ArrangeStatus_Arranged');
  if (newTags.length !== currentTags.length) {
    await updateOrderTags(o.id, newTags);
  }
}

// (2d) Ensure tag "Incoming_Yes" is present
{
  const currentTags2 = await fetchOrderTags(o.id);
  if (!currentTags2.includes('Incoming_Yes')) {
    const added = currentTags2.concat(['Incoming_Yes']);
    await updateOrderTags(o.id, added);
  }
}

// (5) Prepend the invoice update line to the order note
const existingNote = await fetchOrderNote(o.id);
const now = new Date();
const mm = String(now.getMonth() + 1).padStart(2, '0');
const dd = String(now.getDate()).padStart(2, '0');
const yyyy = String(now.getFullYear());
const headerLine = `Update ${mm}/${dd}/${yyyy}: Invoiced with ${(invoiceSupplier || '').trim()} ${(invoiceDate || '').trim()} Invoice`;
const dashLine = '————————————'; // em-dash line
const newNote = `${headerLine}\n${dashLine}\n${existingNote || ''}`;
await updateOrderNote(o.id, newNote);

  // Persist a tiny snapshot file per order
  await ensureDirs();
  const filePath = path.join(ORDERS_DIR, `${o.digits}.json`);
  await writeJsonAtomic(filePath, {
    saved_at: new Date().toISOString(),
    order_id: o.id,
    order_digits: o.digits,
    parts: {
      selections: Array.from(set),
      other_text: p.otherText || null,
      set_aside_text: p.setAsideText || null
    },
    fulfillment: p.fulfillmentVal,
    payment: p.paymentVal
  });

  return { ok: true, id: o.digits };
} catch (e) {
  logger?.error?.('bulk worker failed for order', { order_digits: o.digits, order_id: o.id, error: e });
  console.error('bulk worker failed for order', o, e);
  return { ok: false, id: o.digits, error: (e && e.message) ? e.message : String(e) };
}
});

// Collate successes/failures with reasons
const ok = [];
const fail = [];
const failDetails = [];

results.forEach((r, idx) => {
  if (r && r.ok) {
    ok.push(`C#${r.id}`);
  } else {
    const digits = orders[idx].digits;
    fail.push(`C#${digits}`);
    const reason = (r && r.error) ? r.error
                  : (r && r.message) ? r.message
                  : 'Unknown error';
    const line = `C#${digits}: ${reason}`;
    failDetails.push(line);

    // Log to PM2/stdout with structured context
    console.error('invoice bulk failed', {
      order_digits: digits,
      order_id: orders[idx].id,
      reason
    });
    logger?.error?.('invoice bulk failed', {
      order_digits: digits,
      order_id: orders[idx].id,
      reason
    });
  }
});

// Confirm in thread (summary)
if (channel && thread_ts) {
  const lines = [];
  if (ok.length)   lines.push(`✅ Updated: ${ok.join(', ')}`);
  if (fail.length) lines.push(`❌ Failed: ${fail.join(', ')}`);
  await client.chat.postMessage({
    channel,
    thread_ts,
    text: lines.join('\n') || 'Done.'
  });

  // Post detailed reasons as a follow-up (code block), if any
  if (failDetails.length) {
    await client.chat.postMessage({
      channel,
      thread_ts,
      text: '*Failure details:*\n```' + failDetails.join('\n') + '```'
    });
  }
}
});

/* =========================
   Express HTTP (health)
========================= */
const server = express();
server.use(express.json({ limit: '1mb' }));

server.get('/health', (_req, res) => {
  res.status(200).send('ok');
});

// Optional: expose minimal diagnostics (no secrets)
server.get('/diag', async (_req, res) => {
  res.json({
    uptime_s: Math.round(process.uptime()),
    data_dir_exists: fs.existsSync(DATA_DIR),
    orders_dir_exists: fs.existsSync(ORDERS_DIR),
    watch_channel_id_present: Boolean(WATCH_CHANNEL_ID)
  });
});

server.listen(PORT, () => {
  console.log(`[http] listening on :${PORT}`);
});

/* =========================
   Start the Slack App
========================= */
(async () => {
  await ensureDirs();
  await ensureSuppliersFile(); // <-- ensure data/suppliers.json exists

  await app.start();
  console.log('[slack] app started (Socket Mode)');

  // Non-fatal post-start check: lightweight Shopify ping
  try {
    await shopifyFetch('/shop.json');
    console.log('[shopify] connectivity ok');
  } catch (e) {
    console.error('⚠️ Post-start Shopify check failed:', e?.message || e);
  }
})();