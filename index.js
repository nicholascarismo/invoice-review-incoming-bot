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
    block_id: 'invoice_block',
    label: { type: 'plain_text', text: 'Invoice name' },
    element: {
      type: 'plain_text_input',
      action_id: 'invoice_input',
      multiline: false,
      placeholder: { type: 'plain_text', text: 'e.g. INV-2025-001 or “January Install Batch”' }
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
      placeholder: { type: 'plain_text', text: 'e.g.\nC#1234\nC#1235\n1236' }
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
const invoiceName = (view.state.values?.invoice_block?.invoice_input?.value || '').trim();

    // 1) Parse the textarea into unique order digits
    const raw = view.state.values?.orders_block?.orders_input?.value || '';
    const lines = raw.split(/\r?\n/).map(s => s.trim()).filter(Boolean);

    // Accept formats like "C#1234" or "1234". (Your findOrderByName expects 4 digits.)
    const orderDigits = Array.from(new Set(
      lines
        .map(s => (s.match(/(\d{4})/) || [,''])[1])  // extract 4 digits if present
        .filter(Boolean)
    ));

    if (!orderDigits.length) {
      await client.chat.postEphemeral({
        channel,
        user: md.user,
        text: 'No valid 4-digit order numbers found.'
      });
      return;
    }

// 2) Post a parent (root) message (include invoice name if provided)
const parent = await client.chat.postMessage({
  channel,
  text: `Invoice review started for ${orderDigits.length} order(s) by <@${md.user}>` + (invoiceName ? `\n*Invoice:* ${invoiceName}` : '')
});
    const root_ts = parent.ts;

    // 3) For each order: look it up and post a thread reply with the **Update Metafields** button
    for (const digits of orderDigits) {
      try {
        const order = await findOrderByName(digits);

        const customerName =
          order?.customer ? `${order.customer.first_name || ''} ${order.customer.last_name || ''}`.trim() :
          (order?.shipping_address ? `${order.shipping_address.first_name || ''} ${order.shipping_address.last_name || ''}`.trim() : 'Unknown');

        await client.chat.postMessage({
          channel,
          thread_ts: root_ts,
          text: `Order C#${digits} Found - ${customerName || 'Unknown'}`,
          blocks: [
            {
              type: 'section',
              text: { type: 'mrkdwn', text: `*Order C#${digits}* Found — *${customerName || 'Unknown'}*` }
            },
            {
              type: 'actions',
              elements: [
                {
                  type: 'button',
                  text: { type: 'plain_text', text: 'Update Metafields', emoji: true },
                  action_id: 'open_update_modal',
                  value: JSON.stringify({
                    orderDigits: digits,
                    orderId: order.id,
                    channel,
                    thread_ts: root_ts
                  })
                }
              ]
            }
          ]
        });
      } catch (err) {
        logger.error(`Order C#${digits} lookup failed:`, err);
        await client.chat.postMessage({
          channel,
          thread_ts: root_ts,
          text: `❌ Order C#${digits} not found in Shopify.`
        });
      }
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
    const line4 = setAsideSelected && setAsideText ? `Set Aside For Now: ${setAsideText}` : '';

    const packingLines = [line1, line2, line3];
    if (line4) packingLines.push(line4);
    const packingSlipNotes = packingLines.join('\n');

    // Use multi_line_text_field for creation
    mfOps.push(upsertOrderMetafield(meta.orderId, 'custom', 'packing_slip_notes', packingSlipNotes, 'multi_line_text_field'));

    // 7) initial_slack_tagging_done = "Yes"
    mfOps.push(upsertOrderMetafield(meta.orderId, 'custom', 'initial_slack_tagging_done', 'Yes'));

    // Execute all metafield writes in parallel
    await Promise.all(mfOps);

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