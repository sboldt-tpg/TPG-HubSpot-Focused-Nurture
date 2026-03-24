const express = require('express');
const axios = require('axios');
const https = require('https');

const app = express();
app.use(express.json());

// =============================
// CONFIG
// =============================
const MAX_SUBJECT_RETRIES = 3;
const PROCESS_INTERVAL_MS = 500;
const CONCURRENCY = 10;
const SCRAPE_TIMEOUT_MS = 4000;
const MAX_CONTENT_BYTES = 300_000;
const DOMAIN_CACHE_TTL_MS = 60 * 60 * 1000;

const HUBSPOT_TOKEN = process.env.HUBSPOT_TOKEN;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

// =============================
// BLOCKED DOMAINS
// =============================
const BLOCKED_DOMAINS = new Set([
  'salesforce.com', 'microsoft.com', 'google.com', 'apple.com',
  'amazon.com', 'linkedin.com', 'facebook.com', 'twitter.com',
  'instagram.com', 'youtube.com', 'wikipedia.org',
  'bankofamerica.com', 'chase.com', 'wellsfargo.com', 'citibank.com',
  'jpmorgan.com', 'goldmansachs.com', 'morganstanley.com',
  'bloomberg.com', 'reuters.com', 'wsj.com', 'nytimes.com',
  'oracle.com', 'sap.com', 'ibm.com', 'cisco.com', 'adobe.com',
  'workday.com', 'servicenow.com', 'zendesk.com', 'slack.com',
]);

// =============================
// DOMAIN SCRAPE CACHE
// =============================
const domainCache = new Map();

function getCachedScrape(domain) {
  const entry = domainCache.get(domain);
  if (!entry) return null;
  if (Date.now() - entry.timestamp > DOMAIN_CACHE_TTL_MS) {
    domainCache.delete(domain);
    return null;
  }
  return entry.content;
}

function setCachedScrape(domain, content) {
  domainCache.set(domain, { content, timestamp: Date.now() });
}

// =============================
// QUEUE & IN-FLIGHT TRACKING
// =============================
let queue = [];
let inFlight = 0;

// =============================
// HEALTH CHECK
// =============================
app.get("/", (req, res) => {
  res.json({
    status: "ok",
    queueLength: queue.length,
    inFlight,
    openSlots: Math.max(0, CONCURRENCY - inFlight)
  });
});

// =============================
// DASHBOARD
// =============================
app.get("/dashboard", (req, res) => {
  res.send(`<!DOCTYPE html>
<html>
<head>
  <title>General TPG Nurture - Dashboard</title>
  <meta http-equiv="refresh" content="5">
  <style>
    body { font-family: Arial, sans-serif; background: #1a1a1a; color: #f0f0f0; padding: 40px; }
    h1 { color: #a2cf23; }
    .stat { display: inline-block; background: #2a2a2a; border: 1px solid #a2cf23;
            border-radius: 8px; padding: 20px 32px; margin: 12px; text-align: center; }
    .stat-value { font-size: 2.4em; font-weight: bold; color: #a2cf23; }
    .stat-label { font-size: 0.85em; color: #aaa; margin-top: 4px; }
    .footer { margin-top: 32px; color: #555; font-size: 0.8em; }
  </style>
</head>
<body>
  <h1>General TPG Nurture</h1>
  <div class="stat"><div class="stat-value">${queue.length}</div><div class="stat-label">Queue Depth</div></div>
  <div class="stat"><div class="stat-value">${inFlight}</div><div class="stat-label">In Flight</div></div>
  <div class="stat"><div class="stat-value">${Math.max(0, CONCURRENCY - inFlight)}</div><div class="stat-label">Open Slots</div></div>
  <div class="stat"><div class="stat-value">${CONCURRENCY}</div><div class="stat-label">Concurrency</div></div>
  <div class="footer">Auto-refreshes every 5 seconds - ${new Date().toLocaleString()}</div>
</body>
</html>`);
});

// =============================
// ENQUEUE FROM HUBSPOT
// =============================
app.post("/enqueue", (req, res) => {
  queue.push({ ...req.body, retries: 0 });
  res.status(200).json({
    status: "queued",
    queuePosition: queue.length
  });
});

// =============================
// WORKER LOOP (concurrent fire-and-forget)
// =============================
setInterval(() => {
  while (inFlight < CONCURRENCY && queue.length > 0) {
    const job = queue.shift();
    processJob(job);
  }
}, PROCESS_INTERVAL_MS);

async function processJob(job) {
  inFlight++;
  try {
    await updateStatus(job.contactId, "IN_PROGRESS");

    const scrapedContent = await scrapeWebsite(job.website);
    const result = await runClaude(job, scrapedContent);

    await writeResults(job.contactId, result, job.sequenceStep || 1);
    await updateStatus(job.contactId, "SENT");

    console.log(`Completed: ${job.contactId} - Step ${job.sequenceStep}`);
  } catch (err) {
    console.error(`Error for ${job.contactId}:`, err.message);

    if (err.response?.status === 429) {
      console.log(`Rate limited, requeuing ${job.contactId}`);
      queue.push(job);
    } else {
      job.retries = (job.retries || 0) + 1;
      if (job.retries <= 2) {
        await updateStatus(job.contactId, "RETRY_PENDING");
        queue.push(job);
      } else {
        await updateStatus(job.contactId, "FAILED");
      }
    }
  } finally {
    inFlight--;
  }
}

// =============================
// WEBSITE SCRAPER
// =============================
async function scrapeWebsite(rawUrl) {
  if (!rawUrl) return "";

  let url = rawUrl.trim();
  if (!/^https?:\/\//i.test(url)) url = "https://" + url;

  let domain = "";
  try {
    domain = new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }

  if (BLOCKED_DOMAINS.has(domain)) return "";

  const cached = getCachedScrape(domain);
  if (cached) return cached;

  const agent = new https.Agent({ secureOptions: 0x4, rejectUnauthorized: false });
  const axiosOpts = {
    timeout: SCRAPE_TIMEOUT_MS,
    maxContentLength: MAX_CONTENT_BYTES,
    httpsAgent: agent,
    headers: { "User-Agent": "Mozilla/5.0 (compatible; TPGBot/1.0)" }
  };

  const pathsToTry = [
    "",
    "/about",
    "/about-us",
    "/news",
    "/newsroom",
    "/blog",
    "/insights",
    "/press",
    "/press-releases",
  ];

  let combined = "";

  for (const path of pathsToTry) {
    try {
      const resp = await axios.get(url + path, axiosOpts);
      const text = (resp.data || "")
        .toString()
        .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "")
        .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, "")
        .replace(/<[^>]+>/g, " ")
        .replace(/\s{2,}/g, " ")
        .trim()
        .slice(0, 3000);
      if (text.length > 100) combined += "\n\n[" + (path || "/") + "]\n" + text;
    } catch {
      // silently skip unavailable paths
    }
    if (combined.length > 8000) break;
  }

  const result = combined.trim().slice(0, 8000);
  if (result) setCachedScrape(domain, result);
  return result;
}

// =============================
// SEQUENCE ARC
// =============================
const SEQUENCE_ARC = {
  1:  { purpose: "Pattern interrupt. Open with a hyper-specific observation from their website or industry that reframes a problem they likely already feel. Goal: earn a second read.", ctaStyle: "calendar" },
  2:  { purpose: "Credibility builder. Lead with a brief, concrete TPG client outcome relevant to their industry or role. No fluff. Let the result do the persuading.", ctaStyle: "calendar" },
  3:  { purpose: "Social proof. Reference how peers in their industry are using HubSpot and TPG to solve a specific operational problem. Make them feel the movement.", ctaStyle: "resource" },
  4:  { purpose: "Problem deepener. Name a hidden cost or downstream consequence of the problem from a new angle. Do not pitch yet. Just make the pain more real.", ctaStyle: "question" },
  5:  { purpose: "Insight and POV. Share a sharp, non-obvious opinion Jeff holds about revenue marketing in their space. Position Jeff as a practitioner, not a vendor.", ctaStyle: "calendar" },
  6:  { purpose: "Objection handling. Address a likely reason they have not replied (too busy, already have a solution, unclear ROI). Be direct and empathetic, not defensive.", ctaStyle: "resource" },
  7:  { purpose: "Urgency without pressure. Reference a real market shift, hiring signal, or technology trend that makes inaction more costly. Ground it in their specific context.", ctaStyle: "calendar" },
  8:  { purpose: "Case study hook. Open with a one-sentence client story (no name needed) that mirrors their situation. Let the parallel do the work.", ctaStyle: "calendar" },
  9:  { purpose: "Soft check-in. Acknowledge the sequence, be self-aware and human. Ask a genuine yes/no question about whether this is still relevant for them right now.", ctaStyle: "question" },
  10: { purpose: "Breakup email. Brief, gracious, no hard sell. Leave the door open. Make it the kind of email they would forward to a colleague.", ctaStyle: "calendar" },
};

// =============================
// CLAUDE LOGIC
// =============================
async function runClaude(job, scrapedContent = "") {
  const SEQUENCE_STEP = job.sequenceStep || 1;

  const {
    firstname = '',
    company = '',
    jobtitle = '',
    industry = '',
    numemployees = '',
    annualrevenue = '',
    hs_linkedin_url = '',
    website = '',
    hs_intent_signals_enabled = '',
    web_technologies = '',
    description = ''
  } = job;

  const IntentContext =
    hs_intent_signals_enabled === "true"
      ? "Buyer intent signals are active for this account."
      : "Buyer intent signals are not active or unavailable.";

  const priorEmailsText = [];
  for (let i = 1; i < SEQUENCE_STEP; i++) {
    const field = job["claude_ai_generated_email_text_" + i];
    if (field) priorEmailsText.push("EMAIL " + i + ":\n" + field);
  }
  const priorEmailsBlock = priorEmailsText.length
    ? priorEmailsText.join("\n\n---\n\n")
    : "N/A";

  const researchBlock = scrapedContent
    ? "LIVE WEBSITE RESEARCH (scraped):\n" + scrapedContent
    : "LIVE WEBSITE RESEARCH: Not available.";

  const arc = SEQUENCE_ARC[SEQUENCE_STEP] || SEQUENCE_ARC[1];

  const ctaInstruction = arc.ctaStyle === "question"
    ? "End with a single direct yes/no question instead of a calendar link. No scheduling link in this email."
    : arc.ctaStyle === "resource"
    ? "Include ONE paragraph with exactly ONE single-word hyperlink to a resource URL (choose randomly from the list below). Do NOT include a calendar link in this email."
    : "Include ONE separate paragraph with a calendar CTA using: <a href=\"https://meetings.hubspot.com/jeff-pedowitz\" style=\"font-weight:bold;text-decoration:underline;color:#A2CF23;\">word</a>";

  const userContent =
    "You are Jeff Pedowitz, founder of The Pedowitz Group, writing EMAIL " + SEQUENCE_STEP + " of 10 in a personalized outbound nurture sequence.\n\n" +

    "PROSPECT DATA:\n" +
    "- Name: " + firstname + "\n" +
    "- Title: " + jobtitle + "\n" +
    "- Company: " + company + "\n" +
    "- Industry: " + industry + "\n" +
    "- Employee Count: " + numemployees + "\n" +
    "- Annual Revenue: " + annualrevenue + "\n" +
    "- LinkedIn: " + hs_linkedin_url + "\n" +
    "- Website: " + website + "\n" +
    "- Intent Signals: " + IntentContext + "\n" +
    "- Web Technologies: " + (web_technologies || "Not listed") + "\n" +
    "- Company Description: " + (description || "Not provided") + "\n\n" +

    researchBlock + "\n\n" +

    "PRIOR EMAILS (ALREADY SENT - do not repeat anything from these):\n" +
    priorEmailsBlock + "\n\n" +

    "THIS EMAIL'S PURPOSE (Email " + SEQUENCE_STEP + " of 10):\n" +
    arc.purpose + "\n" +
    "The sequence arc is: awareness > credibility > social proof > deepen pain > POV > objection handling > urgency > case study > soft check-in > breakup. You are at step " + SEQUENCE_STEP + ". Write accordingly. Do not jump ahead or repeat a prior arc theme.\n\n" +

    "NON-REPETITION RULES (HARD FAIL):\n" +
    "- Do NOT repeat any idea, insight, pain point, framing, or analogy from ANY prior email.\n" +
    "- Do NOT reuse sentence structure, paragraph structure, or opening style from prior emails.\n" +
    "- Do NOT use a rhetorical question as your opening line.\n" +
    "- Subject line must be entirely unique. No reuse, close paraphrase, or structural similarity to prior subjects.\n\n" +

    "BANNED PHRASES (using any of these = INVALID response):\n" +
    "Do not use any of the following or close variants:\n" +
    "\"I wanted to reach out\", \"I hope this finds you well\", \"I hope you're doing well\",\n" +
    "\"touching base\", \"just checking in\", \"circling back\", \"following up\",\n" +
    "\"in today's landscape\", \"in today's competitive landscape\", \"in today's fast-paced\",\n" +
    "\"it's no secret that\", \"as you know\", \"I'm sure you're aware\",\n" +
    "\"navigating [anything]\", \"the ever-changing\", \"rapidly evolving\",\n" +
    "\"synergy\", \"leverage\" (as a verb), \"utilize\", \"holistic\", \"robust\",\n" +
    "\"game-changer\", \"game changer\", \"move the needle\", \"at the end of the day\",\n" +
    "\"take it to the next level\", \"best-in-class\", \"cutting-edge\", \"world-class\",\n" +
    "\"I'd love to connect\", \"would love to chat\", \"hoping we can connect\",\n" +
    "\"don't hesitate to reach out\", \"feel free to reach out\",\n" +
    "\"looking forward to hearing from you\", \"let me know if you have any questions\"\n\n" +

    "WRITING RULES:\n" +
    "- Subject: 8 words or fewer. Plaintext. No punctuation gimmicks.\n" +
    "- Salutation on its own line: \"" + firstname + ",\"\n" +
    "- One blank line after salutation.\n" +
    "- Body: 75-110 words. Tight. Every sentence earns its place.\n" +
    "- Each paragraph separated by ONE blank line.\n" +
    "- No bullets. No signature block.\n" +
    "- HTML-safe text. Use <a> tags only for links. No other HTML.\n" +
    "- Write like a confident practitioner, not a vendor. Declarative sentences over questions.\n" +
    "- No em dashes (the long dash character). Use a comma or period instead.\n\n" +

    "PERSONALIZATION (MANDATORY):\n" +
    "- If scraped website content is available above, your OPENING LINE must reference a specific, concrete detail from it (a product, initiative, market, recent announcement, or stated priority). Do not open generically.\n" +
    "- If no scraped content is available, open with a role-specific and company-contextual observation grounded in the prospect data.\n" +
    "- Personalization must be woven into the narrative, not dropped in as a standalone sentence.\n" +
    "- Generic emails applicable to any company are INVALID.\n\n" +

    "MESSAGING STRATEGY:\n" +
    "- Do NOT assume HubSpot usage.\n" +
    "- Reference HubSpot as a platform companies in their industry are using.\n" +
    "- Position The Pedowitz Group as HubSpot Elite Partners and revenue marketing experts.\n" +
    "- Choose ONE problem domain not used in any prior email:\n" +
    "  (forecasting accuracy, RevOps governance, attribution trust, data hygiene, lifecycle stage alignment, scale readiness, pipeline velocity, sales-marketing handoff, lead quality, customer retention marketing)\n\n" +

    "LINKS AND CTA:\n" +
    "- Resource URLs (choose ONE randomly when needed):\n" +
    "  * https://www.pedowitzgroup.com/hubspot-main\n" +
    "  * https://www.pedowitzgroup.com/hubspot-move-it\n" +
    "  * https://www.pedowitzgroup.com/hubspot-tune-it\n" +
    "  * https://www.pedowitzgroup.com/hubspot-run-it\n" +
    "  * https://www.pedowitzgroup.com/solutions/martech/hubSpot\n" +
    "- Single-word hyperlink format: <a href=\"URL\" style=\"font-weight:bold;text-decoration:underline;color:#A2CF23;\">word</a>\n" +
    "- CTA instruction for this email: " + ctaInstruction + "\n\n" +

    "COMPLIANCE:\n" +
    "- No dollar amounts unless publicly stated.\n" +
    "- No fabricated company news or quotes.\n" +
    "- Speak to industry patterns when specifics are unknown.\n\n" +

    "OUTPUT FORMAT (exactly):\n" +
    "Subject: <subject>\n\n" +
    "Body:\n" +
    "<body>";

  let subject = "";
  let bodyText = "";
  let attempt = 0;

  while (attempt < MAX_SUBJECT_RETRIES && !subject) {
    attempt++;

    const res = await axios.post(
      "https://api.anthropic.com/v1/messages",
      {
        model: "claude-sonnet-4-20250514",
        max_tokens: 1500,
        temperature: 0.7,
        system: "You write long-sequence B2B nurture emails with strict non-repetition, a defined narrative arc, and genuine 1:1 personalization. You never use banned phrases, em dashes, or rhetorical openers. You write like a confident practitioner.",
        messages: [{ role: "user", content: userContent }]
      },
      {
        headers: {
          "x-api-key": ANTHROPIC_API_KEY,
          "anthropic-version": "2023-06-01",
          "content-type": "application/json"
        },
        timeout: 30000
      }
    );

    const text = res.data?.content?.find(p => p.type === "text")?.text || "";

    const subjectMatch = text.match(/^\s*Subject:\s*(.+)\s*$/mi);
    const bodyMatch =
      text.match(/^\s*Body:\s*([\s\S]+)$/mi) ||
      text.match(/^\s*Subject:[\s\S]*?\n\n([\s\S]+)$/mi);

    subject = subjectMatch ? subjectMatch[1].trim().replace(/<[^>]+>/g, '') : "";
    bodyText = bodyMatch ? bodyMatch[1].trim() : "";
  }

  if (!subject) {
    throw new Error("Missing subject after retries");
  }

  subject = sanitizeEmDashes(subject);
  bodyText = sanitizeEmDashes(bodyText);

  return { subject, bodyText };
}

// =============================
// EM DASH SANITIZATION
// =============================
function sanitizeEmDashes(text) {
  if (!text) return text;
  return text
    .replace(/\u2014/g, ', ')
    .replace(/\u2013/g, ' to ')
    .replace(/&mdash;/g, ', ')
    .replace(/&ndash;/g, ' to ')
    .replace(/&#8212;/g, ', ')
    .replace(/&#8211;/g, ' to ')
    .replace(/ {2,}/g, ' ')
    .trim();
}

// =============================
// HUBSPOT WRITE-BACK
// =============================
async function writeResults(contactId, { subject, bodyText }, sequenceStep = 1) {
  const bodyHtml = bodyText
    .replace(/\r\n/g, "\n")
    .split(/\n{2,}/)
    .map(p => "<p style=\"margin:0 0 16px;\">" + p.replace(/\n/g, "<br>") + "</p>")
    .join("\n");

  await axios.patch(
    "https://api.hubapi.com/crm/v3/objects/contacts/" + contactId,
    {
      properties: {
        ["prospect_email_" + sequenceStep + "_subject_line"]: subject,
        ["prospect_email_" + sequenceStep]: bodyHtml,
        ["claude_ai_generated_email_text_" + sequenceStep]: bodyText
      }
    },
    {
      headers: {
        Authorization: "Bearer " + HUBSPOT_TOKEN,
        "Content-Type": "application/json"
      },
      timeout: 10000
    }
  );
}

// =============================
// STATUS UPDATE
// =============================
async function updateStatus(contactId, status) {
  try {
    await axios.patch(
      "https://api.hubapi.com/crm/v3/objects/contacts/" + contactId,
      { properties: { ai_email_step_status: status } },
      {
        headers: {
          Authorization: "Bearer " + HUBSPOT_TOKEN,
          "Content-Type": "application/json"
        },
        timeout: 5000
      }
    );
  } catch (err) {
    console.error("Status update failed for " + contactId + ":", err.message);
  }
}

// =============================
// SERVER STARTUP
// =============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("General TPG Nurture running on port " + PORT);
  console.log("Concurrency: " + CONCURRENCY + " | Tick: " + PROCESS_INTERVAL_MS + "ms");
});
