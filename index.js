import 'dotenv/config';
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import express from 'express';
import boltPkg from '@slack/bolt';

const { App } = boltPkg;

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

/* =========================
   Shopify Helpers
========================= */
const SHOPIFY_BASE = `https://${SHOPIFY_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}`;

async function shopifyFetch(pathname, { method = 'GET', headers = {}, body } = {}, attempt = 1) {
  const url = `${SHOPIFY_BASE}${pathname}`;
  const res = await fetch(url, {
    method,
    headers: {
      'X-Shopify-Access-Token': SHOPIFY_ADMIN_TOKEN,
      'Content-Type': 'application/json',
      ...headers
    },
    body: body ? JSON.stringify(body) : undefined
  });

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

// Generic error logger
app.error((e) => {
  console.error('⚠️ Bolt error:', e?.message || e);
});

/* =========================
   Message Listener
========================= */
// Matches "C#1234" at start (optionally preceded by whitespace)
const ORDER_REGEX = /^\s*C#(\d{4})\b/;

app.event('message', async ({ event, client, logger, say }) => {
  try {
    // Only public channel messages we care about; ignore thread broadcasts, edits, etc.
    if (!event || event.hidden || event.subtype === 'message_changed' || event.subtype === 'message_deleted') return;
    if (WATCH_CHANNEL_ID && event.channel !== WATCH_CHANNEL_ID) return;

    const text = (event.text || '').trim();
    const m = text.match(ORDER_REGEX);
    if (!m) return;

    // Confirm message is from FlowBot (either by exact user ID or bot profile name)
    const isFromFlowBotById = FLOWBOT_USER_ID && event.user === FLOWBOT_USER_ID;
    const isFromFlowBotByName =
      event?.bot_profile?.name === 'FlowBot' ||
      event?.username === 'FlowBot' ||
      event?.bot_profile?.display_name === 'FlowBot';

    if (!(isFromFlowBotById || isFromFlowBotByName)) return;

    const orderDigits = m[1]; // "1234"
    let order;
    try {
      order = await findOrderByName(orderDigits);
    } catch (err) {
      logger.error(err);
      // Reply in thread with not found
      await client.chat.postMessage({
        channel: event.channel,
        thread_ts: event.ts,
        text: `Order C#${orderDigits} not found in Shopify.`
      });
      return;
    }

    const customerName =
      order?.customer ? `${order.customer.first_name || ''} ${order.customer.last_name || ''}`.trim() :
      (order?.shipping_address ? `${order.shipping_address.first_name || ''} ${order.shipping_address.last_name || ''}`.trim() : 'Unknown');

    // Post thread reply with "Update Metafields" button
    await client.chat.postMessage({
      channel: event.channel,
      thread_ts: event.ts,
      text: `Order C#${orderDigits} Found - ${customerName || 'Unknown'}`,
blocks: [
  {
    type: 'section',
    text: { type: 'mrkdwn', text: `*Order C#${orderDigits}* Found — *${customerName || 'Unknown'}*` }
  },
        {
          type: 'actions',
          elements: [
            {
              type: 'button',
              text: { type: 'plain_text', text: 'Update Metafields', emoji: true },
              action_id: 'open_update_modal',
              value: JSON.stringify({
                orderDigits: orderDigits,
                orderId: order.id,
                channel: event.channel,
                thread_ts: event.ts
              })
            }
          ]
        }
      ]
    });
  } catch (e) {
    console.error('message handler error:', e?.message || e);
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
   Modal Submission
========================= */
app.view('update_meta_modal_submit', async ({ ack, body, view, client, logger }) => {
  // Validate conditional inputs
  try {
    const state = view.state.values;
    const partsSelected = (state?.parts_block?.parts_check?.selected_options || []).map(o => o.value);
    const otherSelected = partsSelected.includes('other');
    const setAsideSelected = partsSelected.includes('set_aside');
    const otherText = state?.parts_other_text?.other_text?.value?.trim();
    const setAsideText = state?.parts_set_aside_text?.set_aside_text?.value?.trim();

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