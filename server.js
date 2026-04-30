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
// SLEEP UTILITY — throttles HubSpot API calls
// =============================
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

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
// QUEUE, IN-FLIGHT & ERROR TRACKING
// =============================
let queue = [];
let inFlight = 0;
let errorCount = 0;

// =============================
// HEALTH CHECK
// =============================
app.get("/", (req, res) => {
  res.json({
    status: "ok",
    queueLength: queue.length,
    inFlight,
    openSlots: Math.max(0, CONCURRENCY - inFlight),
    errorCount
  });
});

// =============================
// DASHBOARD
// =============================
app.get("/dashboard", (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>HubSpot Focused Nurture — Queue Monitor</title>
      <meta http-equiv="refresh" content="5">
      <style>
        body { font-family: monospace; background: #0f0f0f; color: #a2cf23; padding: 40px; }
        h1 { font-size: 18px; margin-bottom: 30px; color: #fff; }
        .grid { display: flex; gap: 60px; margin-bottom: 40px; }
        .block {}
        .stat { font-size: 64px; font-weight: bold; margin: 0; line-height: 1; }
        .label { font-size: 13px; color: #555; margin-top: 8px; }
        .green { color: #a2cf23; }
        .orange { color: #f0a500; }
        .red { color: #e05252; }
        .grey { color: #333; }
        .footer { font-size: 12px; color: #333; margin-top: 40px; border-top: 1px solid #1a1a1a; padding-top: 20px; }
      </style>
    </head>
    <body>
      <h1>HubSpot Focused Nurture &mdash; Queue Monitor</h1>

      <div class="grid">
        <div class="block">
          <div class="stat ${queue.length > 0 ? "orange" : "grey"}">${queue.length}</div>
          <div class="label">contacts waiting in queue</div>
        </div>
        <div class="block">
          <div class="stat green">${inFlight}</div>
          <div class="label">in-flight (of ${CONCURRENCY} max slots)</div>
        </div>
        <div class="block">
          <div class="stat green">${CONCURRENCY - inFlight}</div>
          <div class="label">open slots available</div>
        </div>
        <div class="block">
          <div class="stat ${errorCount > 0 ? "red" : "grey"}">${errorCount}</div>
          <div class="label">processing errors (since last restart)</div>
        </div>
      </div>

      <div class="footer">
        Last refreshed: ${new Date().toLocaleTimeString()} &nbsp;&middot;&nbsp; Auto-refreshes every 5 seconds
      </div>
    </body>
    </html>
  `);
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

    // merged writeResults + status "SENT" into a single HubSpot PATCH
    await writeResultsAndComplete(job.contactId, result, job.sequenceStep || 1);

    console.log(`Completed: ${job.contactId} - Step ${job.sequenceStep}`);
  } catch (err) {
    console.error(`Error for ${job.contactId}:`, err.message);
    if (err.response?.data) {
      console.error(`  HubSpot says:`, JSON.stringify(err.response.data));
    }

    if (err.response?.status === 429) {
      console.log(`Rate limited, requeuing ${job.contactId}`);
      queue.push(job);
    } else {
      job.retries = (job.retries || 0) + 1;
      if (job.retries <= 2) {
        await updateStatus(job.contactId, "RETRY_PENDING");
        queue.push(job);
      } else {
        errorCount++;
        await updateStatus(job.contactId, "FAILED");
      }
    }
  } finally {
    inFlight--;
  }
}

// =============================
// WEBSITE SCRAPER — all paths fetched concurrently via Promise.allSettled
// Previously: sequential for loop, worst-case 9 paths x 4s timeout = 36s per contact
// Now: all paths fire simultaneously, worst-case = 4s regardless of path count
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

  const fetchResults = await Promise.allSettled(
    pathsToTry.map(path =>
      axios.get(url + path, axiosOpts)
        .then(resp => {
          const text = (resp.data || "")
            .toString()
            .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "")
            .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, "")
            .replace(/<[^>]+>/g, " ")
            .replace(/\s{2,}/g, " ")
            .trim()
            .slice(0, 3000);
          return text.length > 100 ? `\n\n[${path || "/"}]\n${text}` : "";
        })
        .catch(() => "")
    )
  );

  const combined = fetchResults
    .map(r => r.status === "fulfilled" ? r.value : "")
    .join("")
    .trim()
    .slice(0, 8000);

  if (combined) setCachedScrape(domain, combined);
  return combined;
}

// =============================
// SEQUENCE ARC
//
// AEO (Answer Engine Optimization) is woven into steps 2, 4, 5, 6, and 9.
// Rationale for each placement:
//
//   Step 2 — Credibility: a TPG outcome that spans both HubSpot pipeline impact AND
//     AEO-driven visibility, showing TPG delivers on both fronts from day one.
//
//   Step 4 — Problem deepener: the hidden cost angle is that buyers researching
//     solutions in ChatGPT, Perplexity, and Google AI Overviews never make it into
//     the HubSpot CRM at all — because the company isn't present in AI-generated
//     answers. This reframes pipeline gaps as a content-structure problem, not just
//     a HubSpot problem.
//
//   Step 5 — Jeff's POV: his sharpest take on the intersection of AEO and HubSpot.
//     HubSpot is where you close revenue; AEO is how modern buyers find you before
//     they ever reach your CRM. The two are not competing strategies — one feeds the
//     other.
//
//   Step 6 — Objection handling: directly addresses the "AEO sounds too new / not
//     proven" pushback. Reframe: first-movers are already appearing in AI-generated
//     answers and building pipeline from it. Waiting is not neutral — it is ceding
//     ground to competitors who are moving now.
//
//   Step 9 — Soft check-in: the gentle close. Acknowledge the sequence covered both
//     HubSpot and AEO, then ask a single yes/no question about whether either is on
//     the radar next quarter. Low friction, high signal.
// =============================
const SEQUENCE_ARC = {
  1: {
    purpose: "Pattern interrupt. Open with a hyper-specific observation from their website or industry that reframes a problem they likely already feel. Goal: earn a second read.",
    ctaStyle: "calendar"
  },

  2: {
    // CHANGE: AEO added to credibility step.
    // The TPG client outcome should span both dimensions — HubSpot operational improvement
    // AND a measurable gain in content visibility or inbound pipeline that AEO drove.
    // This establishes early that TPG is not just a HubSpot shop but a full revenue
    // marketing and AI search firm. Keep AEO brief here — one sentence woven naturally
    // into the outcome story, not a separate pitch.
    purpose: "Credibility builder. Lead with a brief, concrete TPG client outcome relevant to their industry or role. No fluff — let the result do the persuading. Weave in one natural mention of AEO (Answer Engine Optimization): specifically that part of the result included the client's content becoming visible in AI-generated search responses (ChatGPT, Perplexity, or Google AI Overviews), which fed new pipeline that had not existed before. Keep the AEO reference to one sentence — it should feel like a detail that deepens the outcome, not a separate service pitch.",
    ctaStyle: "calendar"
  },

  3: {
    purpose: "Social proof. Reference how peers in their industry are using HubSpot and TPG to solve a specific operational problem. Make them feel the movement.",
    ctaStyle: "resource"
  },

  4: {
    // CHANGE: AEO added as the hidden cost / problem deepener.
    // The angle: the most expensive pipeline gap is the one that never shows up in
    // the CRM at all. B2B buyers increasingly start their vendor research in AI tools,
    // and companies whose content is not structured for AI extraction are invisible
    // at the top of that funnel. These prospects never enter HubSpot — so the CRM
    // data makes the problem invisible. Name this clearly. Do not pitch a solution yet.
    purpose: "Problem deepener. Name a hidden cost or downstream consequence of the problem from a new angle. Do not pitch yet — just make the pain more real. The angle for this email: the most expensive pipeline leak is the one that never shows up in the CRM at all. B2B buyers increasingly use ChatGPT, Perplexity, and Google AI Overviews to research vendors before ever visiting a website. Companies whose content is not structured for AI extraction are invisible at that stage, so those prospects never enter HubSpot, never trigger a workflow, and never appear in pipeline reports. The data looks fine. The problem is invisible. Name this cost clearly and specifically in the context of their industry or role. Do not offer a solution in this email.",
    ctaStyle: "question"
  },

  5: {
    // CHANGE: AEO is the centerpiece of Jeff's POV in this step.
    // His sharpest, most non-obvious opinion: HubSpot is where you close revenue,
    // but AEO is how buyers find you before they ever reach your CRM. The two are
    // not competing priorities — AEO feeds HubSpot. Most revenue marketers optimize
    // the middle and bottom of the funnel while the top-of-funnel is being restructured
    // by AI search tools they have never optimized for. This is where the leverage is.
    purpose: "Insight and POV. Share a sharp, non-obvious opinion Jeff holds about revenue marketing in their space. Position Jeff as a practitioner, not a vendor. The POV for this email centers on AEO (Answer Engine Optimization) and HubSpot as connected systems: HubSpot is where you close revenue, but AEO is how modern buyers find you before they ever reach your CRM. Most revenue marketers spend all their optimization energy from the lead stage forward, while the stage before that — AI-assisted vendor research in ChatGPT, Perplexity, and Google SGE — is reshaping who even enters the funnel. Companies appearing in AI-generated answers are building brand credibility and pipeline from a channel most of their competitors have not touched. This is not a content trend. It is a structural shift in how B2B buyers screen vendors, and HubSpot data will never show you what it is costing you.",
    ctaStyle: "calendar"
  },

  6: {
    // CHANGE: AEO objection handling added alongside the standard non-reply objections.
    // The secondary objection to address: "AEO sounds experimental / too early to invest in."
    // Reframe: companies appearing in AI-generated answers right now are not running
    // experiments — they are building pipeline. The window for first-mover advantage
    // is open today and will narrow as more competitors move. This is not a prediction;
    // it is observable in the results TPG clients are already seeing.
    purpose: "Objection handling. Address a likely reason they have not replied (too busy, already have a solution, unclear ROI). Be direct and empathetic, not defensive. Also address a secondary objection specifically about AEO: the belief that it is too new, too experimental, or not worth investing in yet. Reframe this directly: companies that structured their content for AI extraction six months ago are appearing in ChatGPT and Perplexity answers today and generating pipeline from it. Waiting is not a neutral position — it is letting competitors establish presence in a channel that B2B buyers are already using to screen vendors. TPG clients are seeing this in their results now, not in a future state. Keep both objection responses tight and confident, not defensive.",
    ctaStyle: "resource"
  },

  7: {
    purpose: "Urgency without pressure. Reference a real market shift, hiring signal, or technology trend that makes inaction more costly. Ground it in their specific context.",
    ctaStyle: "calendar"
  },

  8: {
    purpose: "Case study hook. Open with a one-sentence client story (no name needed) that mirrors their situation. Let the parallel do the work.",
    ctaStyle: "calendar"
  },

  9: {
    // CHANGE: AEO added to the soft check-in.
    // After acknowledging the sequence, briefly note that the emails covered two
    // connected topics — HubSpot optimization and AEO for AI search visibility —
    // and ask a single yes/no question about whether either is on the radar next
    // quarter. This surfaces AEO one final time without pressure and creates a
    // natural opening for a conversation that could start with either topic.
    purpose: "Soft check-in. Acknowledge the sequence, be self-aware and human. In one sentence, note that across this sequence you covered two connected topics: HubSpot optimization and AEO (getting visible in AI-generated search results). Then ask a single, genuine yes/no question: is either of those on their radar for the next quarter? Keep it short, warm, and low-pressure. No hard sell. The goal is to surface whether there is an opening — on either topic — before the final email.",
    ctaStyle: "question"
  },

  10: {
    purpose: "Breakup email. Brief, gracious, no hard sell. Leave the door open. Make it the kind of email they would forward to a colleague.",
    ctaStyle: "calendar"
  },
};

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
// SIGNATURE REMOVER — POST-PROCESS SAFETY NET
// =============================
function removeSignature(text) {
  return text
    .replace(/\n+\s*(Jeff|Scott)\s*$/i, '')
    .replace(/\n+\s*(Best|Best regards|Thanks|Thank you|Regards|Sincerely|Cheers|Warm regards)[^\n]*/gi, '')
    .trim();
}

// =============================
// TPG RESOURCE URL POOL
//
// CHANGE: Expanded from 5 original URLs to 10, sourced from a live crawl of
// https://www.pedowitzgroup.com on 2026-04-30. Each entry includes a context
// hint so Claude can choose the URL that best fits each email's topic rather
// than picking blindly from a flat list.
//
// URL reference:
//   hubspot-main         General HubSpot overview; all three service tiers
//   hubspot-move-it      Platform migration from Marketo/Eloqua/Pardot; 1,000+ migrations
//   hubspot-tune-it      HubSpot optimization/audit; fixing broken or underused setups
//   hubspot-run-it       Managed HubSpot services; TPG runs it for you
//   /solutions/martech/hubSpot  Enterprise HubSpot: CRM implementation, RevOps, large teams
//   /aeo                 AEO services: AI search visibility, ChatGPT/Perplexity/SGE content
//
// The URL pool is defined inline in the Claude prompt below so the model can
// read the context hints and select the most relevant URL per email.
// =============================

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

  console.log(`Running Claude for ${job.contactId} | company: "${company}" | website: "${website}" | step: ${SEQUENCE_STEP}`);

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

  // Steps where AEO is part of the email purpose.
  const AEO_STEPS = new Set([2, 4, 5, 6, 9]);

  // CHANGE: Two AEO resource offers now alternate across the 5 AEO steps rather than
  // always using the same URL. Each resource is matched to the step where it lands most
  // naturally given the arc theme:
  //
  //   Content Analyzer (steps 2, 5, 9) — interactive tool, lower commitment ask.
  //     Step 2 (credibility): "see how your own content scores" pairs well with a client outcome story.
  //     Step 5 (Jeff's POV): after a sharp insight, the analyzer lets them test the premise themselves.
  //     Step 9 (soft check-in): a low-friction tool offer fits the gentle, no-pressure tone of the close.
  //
  //   Complete Guide (steps 4, 6) — educational deep-dive, fits higher-education moments.
  //     Step 4 (problem deepener): after naming the pain, the guide gives them somewhere to go for context.
  //     Step 6 (objection handling): a comprehensive guide helps address "I need to learn more first."
  //
  // Both resources are value offers (not CTAs) and must be embedded naturally as a second
  // hyperlink alongside the standard pool URL hyperlink. Both must appear in every AEO email.

  const AEO_RESOURCE = {
    // Content Analyzer — interactive scoring tool
    2: {
      url: "https://www.pedowitzgroup.com/content-analyzer",
      label: "content analyzer",
      instruction: "Position it as an interactive tool the reader can use right now to see how their own content scores for AI search visibility. Frame it as a quick, useful diagnostic — not a sales step.",
      example: "'To see where your content stands today, TPG built a free <a href=\"https://www.pedowitzgroup.com/content-analyzer\" style=\"font-weight:bold;text-decoration:underline;color:#A2CF23;\">content analyzer</a> that scores pages for AI search visibility in under a minute.'"
    },
    // Complete Guide — educational resource
    4: {
      url: "https://www.pedowitzgroup.com/the-complete-guide-to-answer-engine-optimization-aeo",
      label: "complete guide to AEO",
      instruction: "After naming the pain of invisible pipeline, position the guide as a resource for understanding how AEO works and what structured content looks like. Frame it as context, not a pitch.",
      example: "'If AEO is new to you, we put together <a href=\"https://www.pedowitzgroup.com/the-complete-guide-to-answer-engine-optimization-aeo\" style=\"font-weight:bold;text-decoration:underline;color:#A2CF23;\">a complete guide to AEO</a> that explains how it works and where to start.'"
    },
    // Content Analyzer — interactive scoring tool
    5: {
      url: "https://www.pedowitzgroup.com/content-analyzer",
      label: "content analyzer",
      instruction: "After sharing Jeff's POV, invite the reader to test the premise themselves with a free tool. Frame it as 'see where you stand' — intellectually curious, not salesy.",
      example: "'The fastest way to see this gap in your own content is to run it through TPG's free <a href=\"https://www.pedowitzgroup.com/content-analyzer\" style=\"font-weight:bold;text-decoration:underline;color:#A2CF23;\">content analyzer</a>, which flags exactly what AI tools can and cannot extract.'"
    },
    // Complete Guide — educational resource for the "I need to learn more" objection
    6: {
      url: "https://www.pedowitzgroup.com/the-complete-guide-to-answer-engine-optimization-aeo",
      label: "complete guide to AEO",
      instruction: "After handling the 'AEO is too new' objection, offer the guide as something they can read on their own time to form their own view. Frame it as education, not a sales move.",
      example: "'For anyone still forming a view on this, <a href=\"https://www.pedowitzgroup.com/the-complete-guide-to-answer-engine-optimization-aeo\" style=\"font-weight:bold;text-decoration:underline;color:#A2CF23;\">our complete guide to AEO</a> walks through the evidence and what early movers are doing differently.'"
    },
    // Content Analyzer — low-friction tool for the gentle check-in close
    9: {
      url: "https://www.pedowitzgroup.com/content-analyzer",
      label: "content analyzer",
      instruction: "In the soft check-in, offer the analyzer as a no-commitment way to explore AEO before deciding if it is worth a conversation. Keep it light and low-pressure — it is a tool, not a form.",
      example: "'If you want a quick read on where your content stands for AI search before we talk, the <a href=\"https://www.pedowitzgroup.com/content-analyzer\" style=\"font-weight:bold;text-decoration:underline;color:#A2CF23;\">content analyzer</a> takes about a minute.'"
    }
  };

  const aeoGuideBlock = AEO_STEPS.has(SEQUENCE_STEP)
    ? (() => {
        const r = AEO_RESOURCE[SEQUENCE_STEP];
        return (
          "- AEO RESOURCE OFFER (REQUIRED in this email — AEO step): In addition to the standard pool URL hyperlink, you MUST embed a second hyperlink to the AEO resource assigned to this step. This is a value offer, not a CTA. The link text should be a short descriptive phrase (3-5 words) so the reader knows what they are clicking.\n" +
          "  Resource URL: " + r.url + "\n" +
          "  Link phrase to use: \"" + r.label + "\" (or a close natural variant)\n" +
          "  How to position it: " + r.instruction + "\n" +
          "  Hyperlink format: <a href=\"" + r.url + "\" style=\"font-weight:bold;text-decoration:underline;color:#A2CF23;\">phrase</a>\n" +
          "  Good example: " + r.example + "\n" +
          "  Bad example: 'Click here to learn more.' — generic, feels like an ad, INVALID.\n"
        );
      })()
    : "";

  const ctaInstruction = arc.ctaStyle === "question"
    ? "End with a single direct yes/no question instead of a calendar link. No scheduling link in this email."
    : arc.ctaStyle === "resource"
    ? "Include ONE paragraph with exactly ONE single-word hyperlink to a resource URL chosen from the URL pool below (pick the URL whose context best fits this email's topic). Do NOT include a calendar link in this email."
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
    "- Where the email purpose calls for AEO, position TPG as a certified AEO provider that helps companies structure content to appear in ChatGPT, Perplexity, and Google AI Overview responses — and explain why this feeds HubSpot pipeline rather than competing with it.\n" +
    "- Choose ONE problem domain not used in any prior email:\n" +
    "  (forecasting accuracy, RevOps governance, attribution trust, data hygiene, lifecycle stage alignment, scale readiness, pipeline velocity, sales-marketing handoff, lead quality, customer retention marketing)\n\n" +

    "TPG RESOURCE URL POOL — choose ONE URL for the required hyperlink in this email.\n" +
    "Pick the URL whose context description best fits this email's topic. Do not pick randomly — match the URL to the email's angle.\n\n" +
    "  URL 1: https://www.pedowitzgroup.com/hubspot-main\n" +
    "  Context: General HubSpot overview covering all three service tiers (Move It, Tune It, Run It). Good default when the email discusses HubSpot broadly or does not fit a more specific page.\n\n" +
    "  URL 2: https://www.pedowitzgroup.com/hubspot-move-it\n" +
    "  Context: Platform migration page. 1,000+ migrations, zero failures, 73% average cost reduction. Use when the email angle involves escaping Marketo, Eloqua, Pardot, or high platform costs.\n\n" +
    "  URL 3: https://www.pedowitzgroup.com/hubspot-tune-it\n" +
    "  Context: HubSpot optimization and audit page. Fixing broken workflows, underutilized platform, lead scoring, attribution gaps. Use when the pain is about HubSpot not performing or being used at low capacity.\n\n" +
    "  URL 4: https://www.pedowitzgroup.com/hubspot-run-it\n" +
    "  Context: HubSpot managed services page. TPG runs HubSpot for you — campaigns, workflows, reporting, database hygiene, 24/7 admin. Use when the pain is team capacity, lack of in-house expertise, or the desire to delegate execution.\n\n" +
    "  URL 5: https://www.pedowitzgroup.com/solutions/martech/hubSpot\n" +
    "  Context: Enterprise HubSpot services page. CRM implementation, RevOps alignment, multi-team governance, large-team complexity. Use for enterprise-sized companies or when discussing CRM governance and organizational scale.\n\n" +
    "  URL 6: https://www.pedowitzgroup.com/aeo\n" +
    "  Context: TPG's AEO (Answer Engine Optimization) services page. Helps companies structure content to appear in ChatGPT, Perplexity, and Google AI Overview responses. Use for any email where AEO is mentioned — this is the natural destination URL for that topic.\n\n" +

    "LINKS AND CTA:\n" +
    "- EVERY email MUST include exactly ONE hyperlinked word linking to a URL from the pool above.\n" +
    "- The hyperlink must be embedded naturally in a sentence that flows with the surrounding content. It should read like a reference or supporting detail, not a call-to-action. The reader should barely notice it is a link until they see the styling.\n" +
    "- Hyperlink format (single word only): <a href=\"URL\" style=\"font-weight:bold;text-decoration:underline;color:#A2CF23;\">word</a>\n" +
    "- Good example: 'The companies getting the most from HubSpot treat it as a revenue <a href=\"https://www.pedowitzgroup.com/hubspot-main\" style=\"font-weight:bold;text-decoration:underline;color:#A2CF23;\">infrastructure</a> problem, not a marketing one.'\n" +
    "- Good AEO example: 'Most of that research now happens in AI tools before the buyer ever reaches a website, which is exactly why <a href=\"https://www.pedowitzgroup.com/aeo\" style=\"font-weight:bold;text-decoration:underline;color:#A2CF23;\">AEO</a> has become the missing top-of-funnel layer for revenue marketers.'\n" +
    "- Bad example: 'Click <a href=\"...\">here</a> to learn more.' — generic, feels like an ad, INVALID.\n" +
    aeoGuideBlock +
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

  console.log(`Claude result for ${job.contactId}: subject="${subject}" | bodyLength=${bodyText.length}`);

  if (!subject) {
    throw new Error("Missing subject after retries");
  }

  subject = sanitizeEmDashes(subject);
  bodyText = removeSignature(sanitizeEmDashes(bodyText));

  return { subject, bodyText };
}

// =============================
// HUBSPOT WRITE-BACK + STATUS — single PATCH call
// Sets all email fields AND ai_email_step_status = "SENT" together.
// updateStatus() is still used separately for IN_PROGRESS, RETRY_PENDING, FAILED.
// =============================
async function writeResultsAndComplete(contactId, { subject, bodyText }, sequenceStep = 1) {
  await sleep(300); // throttle HubSpot writes — prevents burst rate limit (100 req/10s)

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
        ["claude_ai_generated_email_text_" + sequenceStep]: bodyText,
        ai_email_step_status: "SENT"
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
// STATUS UPDATE — used for IN_PROGRESS, RETRY_PENDING, FAILED
// SENT status is handled inside writeResultsAndComplete above.
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
    if (err.response?.data) {
      console.error("  HubSpot says:", JSON.stringify(err.response.data));
    }
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
