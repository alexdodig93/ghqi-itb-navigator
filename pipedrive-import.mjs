#!/usr/bin/env node
/**
 * GHQI ITB 2025 - Pipedrive Import + Deep Gemini Enrichment Pipeline
 *
 * Phase 1: Website Scraping - fetches hotel websites, extracts text
 * Phase 2: Gemini Deep Analysis - structured JSON with hotel intel:
 *          type, USPs, target guests, specialties, OTA dependency signals,
 *          personalized email sentence, sales talking points
 * Phase 3: Pipedrive Import - Org + Person + Deal + Rich Note + Activity
 *
 * Usage: node pipedrive-import.mjs [--dry-run] [--skip-gemini] [--skip-scrape] [--limit N] [--offset N]
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';

// ============================================================
// CONFIG
// ============================================================
const PIPEDRIVE_API_KEY = process.env.PIPEDRIVE_API_KEY || '';
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';
const PIPEDRIVE_BASE = 'https://api.pipedrive.com/v1';
const GEMINI_URL = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent';

const CSV_INPUT = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Hotel_Kontaktdaten.csv';
const SCRAPE_CACHE = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Website_Scrape_Cache.json';
const ENRICHMENT_CACHE = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Deep_Enrichment_Cache.json';
const IMPORT_LOG = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Pipedrive_Import_Log.json';

const SETTER_PIPELINE_ID = 1;
const STAGE_QUALIFIZIERT_ID = 1;
const ACTIVITY_TYPE_EMAIL = 'email';
const ACTIVITY_DUE_DATE = '2025-03-11';
const ACTIVITY_DUE_TIME = '09:00';

const SCRAPE_CONCURRENCY = 10;
const SCRAPE_TIMEOUT_MS = 8000;
const GEMINI_CONCURRENCY = 3;  // Lower for deep analysis (bigger prompts)
const GEMINI_DELAY_MS = 500;
const PIPEDRIVE_DELAY_MS = 300;
const PIPEDRIVE_CONCURRENCY = 3;

// Country code sets
const DACH_CODES = new Set(['DE', 'AT', 'CH']);
const EU_CODES = new Set([
  'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'GR',
  'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT',
  'RO', 'SK', 'SI', 'ES', 'SE', 'NO', 'IS', 'GB', 'UK'
]);

// ============================================================
// CLI ARGS
// ============================================================
const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const SKIP_GEMINI = args.includes('--skip-gemini');
const SKIP_SCRAPE = args.includes('--skip-scrape');
const limitIdx = args.indexOf('--limit');
const LIMIT = limitIdx >= 0 ? parseInt(args[limitIdx + 1]) : 0;
const offsetIdx = args.indexOf('--offset');
const OFFSET = offsetIdx >= 0 ? parseInt(args[offsetIdx + 1]) : 0;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ============================================================
// HELPERS
// ============================================================
function getSegment(countryCode, isPremium) {
  const cc = (countryCode || '').toUpperCase();
  if (isPremium === '1' || isPremium === 'true') return 'premium';
  if (DACH_CODES.has(cc)) return 'dach';
  if (EU_CODES.has(cc)) return 'eu';
  return 'international';
}

function getLanguage(segment) {
  return segment === 'dach' ? 'de' : 'en';
}

function cleanUrl(web) {
  if (!web) return '';
  let url = web.trim();
  if (url.includes(',')) url = url.split(',')[0].trim();
  if (!url.startsWith('http')) url = 'https://' + url;
  return url;
}

// Strip HTML tags and extract readable text
function htmlToText(html) {
  return html
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
    .replace(/<nav[^>]*>[\s\S]*?<\/nav>/gi, '')
    .replace(/<footer[^>]*>[\s\S]*?<\/footer>/gi, '')
    .replace(/<header[^>]*>[\s\S]*?<\/header>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#\d+;/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

// ============================================================
// PHASE 1: WEBSITE SCRAPING
// ============================================================
async function scrapeWebsite(url) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), SCRAPE_TIMEOUT_MS);

    const resp = await fetch(url, {
      signal: controller.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9,de;q=0.8',
      },
      redirect: 'follow',
    });

    clearTimeout(timeout);

    if (!resp.ok) return { ok: false, error: `HTTP ${resp.status}` };

    const contentType = resp.headers.get('content-type') || '';
    if (!contentType.includes('text/html') && !contentType.includes('application/xhtml')) {
      return { ok: false, error: `Not HTML: ${contentType}` };
    }

    const html = await resp.text();
    let text = htmlToText(html);

    // Extract title
    const titleMatch = html.match(/<title[^>]*>(.*?)<\/title>/i);
    const title = titleMatch ? titleMatch[1].replace(/<[^>]+>/g, '').trim() : '';

    // Extract meta description
    const metaMatch = html.match(/<meta[^>]*name=["']description["'][^>]*content=["']([^"']*)["']/i)
      || html.match(/<meta[^>]*content=["']([^"']*)["'][^>]*name=["']description["']/i);
    const metaDescription = metaMatch ? metaMatch[1].trim() : '';

    // Truncate text to ~4000 chars for Gemini (cost control)
    if (text.length > 4000) text = text.substring(0, 4000) + '...';

    return {
      ok: true,
      title,
      metaDescription,
      text,
      textLength: text.length,
    };
  } catch (err) {
    return { ok: false, error: err.name === 'AbortError' ? 'Timeout' : err.message };
  }
}

// ============================================================
// PHASE 2: GEMINI DEEP ANALYSIS
// ============================================================
async function deepEnrichWithGemini(hotel, websiteData, retries = 2) {
  const lang = hotel.language === 'de' ? 'Deutsch' : 'Englisch';

  const websiteContext = websiteData?.ok
    ? `
WEBSITE TITLE: ${websiteData.title}
META DESCRIPTION: ${websiteData.metaDescription}
WEBSITE TEXT (first 4000 chars):
${websiteData.text}`
    : `(Website konnte nicht geladen werden - analysiere nur den Hotelnamen und Standort)`;

  const prompt = `Du bist ein Senior Hotel-Sales-Analyst bei GHQI (General Hospitality Quality Institute).

Analysiere dieses Hotel und liefere ein strukturiertes Sales-Dossier als JSON.

HOTEL DATEN:
- Name: ${hotel.name}
- Stadt: ${hotel.city || 'unbekannt'}
- Land: ${hotel.countryCode || 'unbekannt'}
- Kontakt: ${hotel.contactFirstName || ''} ${hotel.contactLastName || ''}
- Website: ${cleanUrl(hotel.web)}
- ITB Halle: ${hotel.hall || 'unbekannt'}, Stand: ${hotel.stand || 'unbekannt'}
${websiteContext}

Liefere EXAKT dieses JSON-Format (alle Texte auf ${lang}):

{
  "hotelType": "z.B. Boutique, Resort, Business, Luxury, Budget, Wellness, City, Airport, Design",
  "starCategory": "Geschätzt 1-5 Sterne oder 'unbekannt'",
  "targetGuests": "Wer kommt hierher? z.B. Business-Reisende, Familien, Paare, Wellness-Gäste",
  "usps": ["USP 1", "USP 2", "USP 3"],
  "specialties": "Was macht das Hotel besonders? Ein Satz.",
  "estimatedRooms": "Geschätzte Zimmeranzahl oder 'unbekannt'",
  "otaDependencySignals": "Hinweise auf OTA-Abhängigkeit: z.B. Booking-Widget auf Website, keine eigene Buchungsmaschine, 'Buchen via Booking.com' Links",
  "salesAngle": "Der beste Ansatzpunkt für GHQI: Warum braucht GENAU dieses Hotel einen Digitalen Zwilling? Ein konkreter Satz.",
  "talkingPoints": ["Gesprächspunkt 1 für den Sales-Call", "Gesprächspunkt 2", "Gesprächspunkt 3"],
  "personalizedEmailSentence": "EIN personalisierter Satz (max 30 Worte) für die Email. Zeigt dass wir das Hotel kennen. Kein 'Ich habe gesehen' oder 'Auf Ihrer Website'. Direkt und konkret.",
  "riskLevel": "low/medium/high - Wie wahrscheinlich ist ein Close? low=schwer, high=gute Chance",
  "riskReason": "Warum diese Einschätzung? Ein Satz."
}

REGELN:
- Nur valides JSON, kein Markdown, keine Erklärungen drumherum
- Wenn du etwas nicht weisst, schreibe "unbekannt" statt zu raten
- personalizedEmailSentence MUSS sich auf ein echtes Detail beziehen
- salesAngle muss GHQI-spezifisch sein (Digitaler Zwilling, 5.000 Datenpunkte, KI-Sichtbarkeit)
- Alle Texte auf ${lang}`;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const resp = await fetch(`${GEMINI_URL}?key=${GEMINI_API_KEY}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: {
            temperature: 0.4,
            maxOutputTokens: 4096,
            topP: 0.9,
            responseMimeType: 'application/json'
          }
        })
      });

      if (resp.status === 503 || resp.status === 429) {
        if (attempt < retries) {
          await sleep(2000 * (attempt + 1));
          continue;
        }
      }

      if (!resp.ok) {
        const err = await resp.text();
        throw new Error(`Gemini ${resp.status}: ${err.substring(0, 200)}`);
      }

      const data = await resp.json();
      const candidate = data.candidates?.[0];
      const finishReason = candidate?.finishReason;
      const text = candidate?.content?.parts?.[0]?.text || '';
      const thoughts = data.usageMetadata?.thoughtsTokenCount || 0;

      if (!text) {
        throw new Error(`Empty response (finishReason: ${finishReason}, thoughts: ${thoughts})`);
      }

      // Parse JSON from response - handle various edge cases
      let jsonText = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
      // Fix common JSON issues: unterminated strings, trailing commas
      jsonText = jsonText.replace(/,\s*}/g, '}').replace(/,\s*]/g, ']');
      // Try to extract JSON if wrapped in other text
      const jsonMatch = jsonText.match(/\{[\s\S]*\}/);
      if (!jsonMatch) {
        throw new Error(`No JSON in response (len=${text.length}, thoughts=${thoughts}, finish=${finishReason})`);
      }

      let parsed;
      try {
        parsed = JSON.parse(jsonMatch[0]);
      } catch (parseErr) {
        // Try to salvage truncated JSON by closing brackets
        let salvage = jsonMatch[0];
        // Count open/close braces
        const openBraces = (salvage.match(/\{/g) || []).length;
        const closeBraces = (salvage.match(/\}/g) || []).length;
        const openBrackets = (salvage.match(/\[/g) || []).length;
        const closeBrackets = (salvage.match(/\]/g) || []).length;
        // Close open strings if needed
        if ((salvage.match(/"/g) || []).length % 2 !== 0) salvage += '"';
        // Close arrays and objects
        for (let b = 0; b < openBrackets - closeBrackets; b++) salvage += ']';
        for (let b = 0; b < openBraces - closeBraces; b++) salvage += '}';
        salvage = salvage.replace(/,\s*}/g, '}').replace(/,\s*]/g, ']');
        try {
          parsed = JSON.parse(salvage);
        } catch {
          throw new Error(`JSON parse failed even after salvage (thoughts=${thoughts}): ${parseErr.message}`);
        }
      }

      // Validate required fields
      if (!parsed.personalizedEmailSentence || !parsed.salesAngle) {
        throw new Error('Missing required fields in Gemini response');
      }

      return parsed;
    } catch (err) {
      if (attempt < retries) {
        await sleep(1500 * (attempt + 1));
        continue;
      }
      console.error(`\n    Gemini error for ${hotel.name}: ${err.message}`);
      return null;
    }
  }
  return null;
}

// ============================================================
// EMAIL TEMPLATES
// ============================================================
function buildEmail(hotel, enrichment) {
  const { segment, language } = hotel;
  const firstName = hotel.contactFirstName || hotel.contactLastName || 'there';
  const hallText = hotel.hall ? (language === 'de' ? `Halle ${hotel.hall}` : `Hall ${hotel.hall}`) : '';
  const personalLine = enrichment?.personalizedEmailSentence || '';

  const personalInsert = personalLine ? `\n${personalLine}\n` : '';

  if (segment === 'dach') {
    return {
      subject: `${firstName}, kurze Frage nach der ITB`,
      body: `Hallo ${firstName},

5.600 Aussteller, 27 Hallen, drei Tage. Wir waren beide da - und haben uns trotzdem verpasst. Das passiert.
${personalInsert}
Ich wollte Ihnen kurz zeigen was wir mit GHQI machen: Wir bauen Hotels einen Digitalen Zwilling mit bis zu 5.000 strukturierten Datenpunkten - damit KI-Systeme wie ChatGPT und Perplexity Ihr Haus direkt empfehlen können.

Booking.com hat von Ihnen gerade 120 generische Datenpunkte. Und nimmt dafuer 18% auf jede Buchung.

Wir nehmen 369 EUR im Jahr. Und 0% Provision. Niemals.

Haetten Sie 15 Minuten fuer ein Gespraech?

https://calendly.com/ghqi/15min

Mit freundlichen Gruessen aus Muenchen,
Alexander Doebereiner
GHQI - General Hospitality Quality Institute
info@ghqi.org | www.ghqi.org`
    };
  }

  if (segment === 'premium') {
    return {
      subject: `${firstName} - data infrastructure question from ITB`,
      body: `Hi ${firstName},

We were at ITB this week and wanted to connect with you specifically - didn't quite make it to ${hallText || 'your booth'} before the floor closed.
${personalInsert}
At GHQI, we work with hotels on structured data infrastructure for the AI era. The core challenge: AI recommendation engines like ChatGPT and Perplexity run on structured data. Most hotels have 80-120 data points indexed across platforms. We build Digital Twins with up to 5,000 structured data points - purpose-built for AI discovery.

For a property like ${hotel.name}, the question isn't whether AI search matters. It's whether your data architecture is ready for it.

I'd welcome a 15-minute conversation with you or the right person on your team.

Would that be worth a call?

https://calendly.com/ghqi/15min

Best regards,
Alexander Doebereiner
CEO, GHQI - General Hospitality Quality Institute
Munich, Germany
info@ghqi.org | www.ghqi.org`
    };
  }

  // EU + International
  return {
    subject: `${firstName}, we were both at ITB - but missed each other`,
    body: `Hi ${firstName},

5,600 exhibitors. 27 halls. Three days. We were both there - and still didn't connect. That's ITB for you.
${personalInsert}
Quick note on what we do at GHQI: We build a Digital Twin for hotels with up to 5,000 structured data points - so AI systems like ChatGPT and Perplexity can actually recommend your property.

Right now, Booking.com has around 120 generic data points about your hotel - and charges 18% commission for the privilege.

We charge EUR 369 per year. 0% commission. Ever.

Would you have 15 minutes for a conversation?

https://calendly.com/ghqi/15min

Best regards,
Alexander Doebereiner
GHQI - General Hospitality Quality Institute
Munich, Germany
info@ghqi.org | www.ghqi.org`
  };
}

// ============================================================
// PIPEDRIVE API HELPERS
// ============================================================
async function pdRequest(method, endpoint, body = null, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const url = `${PIPEDRIVE_BASE}${endpoint}?api_token=${PIPEDRIVE_API_KEY}`;
      const opts = { method, headers: { 'Content-Type': 'application/json' } };
      if (body) opts.body = JSON.stringify(body);

      const resp = await fetch(url, opts);
      const data = await resp.json();

      if (!data.success) {
        // Rate limit
        if (resp.status === 429 && attempt < retries) {
          await sleep(2000 * (attempt + 1));
          continue;
        }
        throw new Error(`Pipedrive ${method} ${endpoint}: ${data.error || JSON.stringify(data)}`);
      }
      return data.data;
    } catch (err) {
      if (attempt < retries && err.message.includes('fetch')) {
        await sleep(1000);
        continue;
      }
      throw err;
    }
  }
}

async function createOrganization(hotel, enrichment) {
  const address = [hotel.street, hotel.zip, hotel.city, hotel.countryCode].filter(Boolean).join(', ');

  return pdRequest('POST', '/organizations', {
    name: hotel.name,
    address: address || undefined,
  });
}

async function createPerson(hotel, orgId) {
  return pdRequest('POST', '/persons', {
    name: [hotel.contactFirstName, hotel.contactLastName].filter(Boolean).join(' ') || hotel.name,
    org_id: orgId,
    email: [hotel.email],
    phone: hotel.phone ? [hotel.phone] : undefined,
  });
}

async function createDeal(hotel, orgId, personId) {
  return pdRequest('POST', '/deals', {
    title: `ITB 2025 Follow-up - ${hotel.name}`,
    org_id: orgId,
    person_id: personId,
    pipeline_id: SETTER_PIPELINE_ID,
    stage_id: STAGE_QUALIFIZIERT_ID,
  });
}

function buildDealNote(hotel, enrichment, emailContent) {
  const e = enrichment || {};
  const usps = (e.usps || []).map(u => `<li>${esc(u)}</li>`).join('');
  const talkingPoints = (e.talkingPoints || []).map(t => `<li>${esc(t)}</li>`).join('');
  const lang = hotel.language === 'de' ? 'DE' : 'EN';

  return `
<h2>ITB 2025 - ${esc(hotel.name)}</h2>
<p><strong>Segment:</strong> ${hotel.segment.toUpperCase()} | <strong>Sprache:</strong> ${lang} | <strong>Halle:</strong> ${hotel.hall || '-'} | <strong>Stand:</strong> ${hotel.stand || '-'}</p>

<h3>Hotel-Profil</h3>
<table style="border-collapse:collapse;width:100%;">
<tr><td style="padding:4px 8px;border:1px solid #ddd;font-weight:bold;">Typ</td><td style="padding:4px 8px;border:1px solid #ddd;">${esc(e.hotelType || 'unbekannt')}</td></tr>
<tr><td style="padding:4px 8px;border:1px solid #ddd;font-weight:bold;">Sterne</td><td style="padding:4px 8px;border:1px solid #ddd;">${esc(e.starCategory || 'unbekannt')}</td></tr>
<tr><td style="padding:4px 8px;border:1px solid #ddd;font-weight:bold;">Zimmer (gesch.)</td><td style="padding:4px 8px;border:1px solid #ddd;">${esc(e.estimatedRooms || 'unbekannt')}</td></tr>
<tr><td style="padding:4px 8px;border:1px solid #ddd;font-weight:bold;">Zielgruppe</td><td style="padding:4px 8px;border:1px solid #ddd;">${esc(e.targetGuests || 'unbekannt')}</td></tr>
<tr><td style="padding:4px 8px;border:1px solid #ddd;font-weight:bold;">Besonderheiten</td><td style="padding:4px 8px;border:1px solid #ddd;">${esc(e.specialties || 'unbekannt')}</td></tr>
<tr><td style="padding:4px 8px;border:1px solid #ddd;font-weight:bold;">OTA-Signale</td><td style="padding:4px 8px;border:1px solid #ddd;">${esc(e.otaDependencySignals || 'unbekannt')}</td></tr>
<tr><td style="padding:4px 8px;border:1px solid #ddd;font-weight:bold;">Close-Chance</td><td style="padding:4px 8px;border:1px solid #ddd;">${esc(e.riskLevel || '?')} - ${esc(e.riskReason || '')}</td></tr>
</table>

${usps ? `<h3>USPs</h3><ul>${usps}</ul>` : ''}

<h3>Sales-Ansatz</h3>
<p style="background:#f0f7f4;padding:10px;border-left:3px solid #1E3A2F;"><strong>${esc(e.salesAngle || 'Kein Sales-Angle generiert')}</strong></p>

${talkingPoints ? `<h3>Talking Points (fuer den Call)</h3><ul>${talkingPoints}</ul>` : ''}

<hr>
<h3>Email-Vorlage (Copy-Paste Ready)</h3>
<p><strong>Betreff:</strong> ${esc(emailContent.subject)}</p>
<pre style="white-space:pre-wrap;font-family:sans-serif;font-size:13px;background:#fafafa;padding:12px;border:1px solid #eee;border-radius:4px;">${esc(emailContent.body)}</pre>

<hr>
<p style="color:#888;font-size:11px;">Generiert am ${new Date().toLocaleDateString('de-DE')} | GHQI Deep Enrichment Pipeline | Gemini 2.5 Flash</p>`;
}

function esc(s) {
  if (!s) return '';
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

async function createNote(dealId, noteHtml) {
  return pdRequest('POST', '/notes', {
    deal_id: dealId,
    content: noteHtml,
    pinned_to_deal_flag: 1,
  });
}

async function createActivity(dealId, orgId, personId, hotel) {
  const lang = hotel.language;
  const subject = lang === 'de'
    ? `ITB Follow-up Email senden: ${hotel.name}`
    : `Send ITB follow-up email: ${hotel.name}`;

  return pdRequest('POST', '/activities', {
    subject,
    type: ACTIVITY_TYPE_EMAIL,
    deal_id: dealId,
    org_id: orgId,
    person_id: personId,
    due_date: ACTIVITY_DUE_DATE,
    due_time: ACTIVITY_DUE_TIME,
    note: `Email-Vorlage liegt als Notiz am Deal. Copy-Paste in Email-Client.\nSegment: ${hotel.segment} | Sprache: ${lang}`,
  });
}

// ============================================================
// MAIN PIPELINE
// ============================================================
async function main() {
  console.log('');
  console.log('  ==========================================================');
  console.log('  GHQI - ITB 2025 Deep Enrichment + Pipedrive Import');
  console.log('  Phase 1: Website Scraping | Phase 2: Gemini Analysis');
  console.log('  Phase 3: Pipedrive Import (Org+Person+Deal+Note+Activity)');
  console.log('  ==========================================================');
  console.log('');

  if (DRY_RUN) console.log('  MODE: DRY RUN (no Pipedrive writes)\n');
  if (SKIP_GEMINI) console.log('  MODE: SKIP GEMINI\n');
  if (SKIP_SCRAPE) console.log('  MODE: SKIP SCRAPE\n');
  if (LIMIT) console.log(`  MODE: LIMIT ${LIMIT} hotels\n`);
  if (OFFSET) console.log(`  MODE: OFFSET ${OFFSET}\n`);

  // ---- Read CSV ----
  console.log('  Reading hotel CSV...');
  let csv = readFileSync(CSV_INPUT, 'utf-8');
  if (csv.charCodeAt(0) === 0xFEFF) csv = csv.slice(1);

  const lines = csv.trim().split('\n');
  const allRows = lines.slice(1).map(l => l.split(';'));
  const hotelsWithEmail = allRows.filter(r => r[2] && r[2].includes('@'));
  console.log(`  Total: ${allRows.length} hotels, ${hotelsWithEmail.length} with email\n`);

  let hotels = hotelsWithEmail.map(r => {
    const [id, name, email, phone, web, street, street2, zip, city, countryCode,
           contactFirstName, contactLastName, hall, stand, isPremium] = r;
    const segment = getSegment(countryCode, isPremium);
    const language = getLanguage(segment);
    return {
      id, name, email: email.trim(), phone, web, street, street2, zip, city,
      countryCode: (countryCode || '').toUpperCase(),
      contactFirstName, contactLastName, hall, stand, isPremium,
      segment, language
    };
  });

  if (OFFSET) hotels = hotels.slice(OFFSET);
  if (LIMIT) hotels = hotels.slice(0, LIMIT);

  const segCounts = { dach: 0, eu: 0, international: 0, premium: 0 };
  hotels.forEach(h => segCounts[h.segment]++);
  console.log('  Segmentation:');
  Object.entries(segCounts).forEach(([k, v]) => console.log(`    ${k.padEnd(15)} ${v}`));
  console.log(`    ${'TOTAL'.padEnd(15)} ${hotels.length}\n`);

  // ======================================================
  // PHASE 1: Website Scraping
  // ======================================================
  let scrapeCache = {};
  if (existsSync(SCRAPE_CACHE)) {
    scrapeCache = JSON.parse(readFileSync(SCRAPE_CACHE, 'utf-8'));
    console.log(`  Scrape cache: ${Object.keys(scrapeCache).length} entries\n`);
  }

  if (!SKIP_SCRAPE) {
    console.log('  PHASE 1: Website Scraping...\n');
    const toScrape = hotels.filter(h => h.web && !scrapeCache[h.id]);
    console.log(`  To scrape: ${toScrape.length} (${hotels.filter(h => scrapeCache[h.id]).length} cached)\n`);

    let scraped = 0, scrapeErrors = 0;

    for (let i = 0; i < toScrape.length; i += SCRAPE_CONCURRENCY) {
      const batch = toScrape.slice(i, i + SCRAPE_CONCURRENCY);
      const results = await Promise.all(batch.map(async (h) => {
        const url = cleanUrl(h.web);
        const result = await scrapeWebsite(url);
        return { id: h.id, result };
      }));

      results.forEach(r => {
        scrapeCache[r.id] = r.result;
        if (r.result.ok) scraped++;
        else scrapeErrors++;
      });

      const done = Math.min(i + SCRAPE_CONCURRENCY, toScrape.length);
      process.stdout.write(
        `\r  Scrape: [${String(done).padStart(4)}/${toScrape.length}] ok: ${scraped} | err: ${scrapeErrors}  `
      );

      if (done % 100 === 0 || done === toScrape.length) {
        writeFileSync(SCRAPE_CACHE, JSON.stringify(scrapeCache), 'utf-8');
      }
    }

    writeFileSync(SCRAPE_CACHE, JSON.stringify(scrapeCache), 'utf-8');
    console.log(`\n\n  Scraping done. ${scraped} ok, ${scrapeErrors} errors.\n`);
  }

  // ======================================================
  // PHASE 2: Gemini Deep Enrichment
  // ======================================================
  let enrichmentCache = {};
  if (existsSync(ENRICHMENT_CACHE)) {
    enrichmentCache = JSON.parse(readFileSync(ENRICHMENT_CACHE, 'utf-8'));
    console.log(`  Enrichment cache: ${Object.keys(enrichmentCache).length} entries\n`);
  }

  if (!SKIP_GEMINI) {
    console.log('  PHASE 2: Gemini Deep Analysis...\n');
    const toEnrich = hotels.filter(h => !enrichmentCache[h.id]);
    console.log(`  To enrich: ${toEnrich.length} (${hotels.filter(h => enrichmentCache[h.id]).length} cached)\n`);

    let enriched = 0, enrichErrors = 0;
    const startTime = Date.now();

    for (let i = 0; i < toEnrich.length; i += GEMINI_CONCURRENCY) {
      const batch = toEnrich.slice(i, i + GEMINI_CONCURRENCY);
      const results = await Promise.all(batch.map(async (h) => {
        const websiteData = scrapeCache[h.id] || null;
        const analysis = await deepEnrichWithGemini(h, websiteData);
        return { id: h.id, analysis };
      }));

      results.forEach(r => {
        if (r.analysis) {
          enrichmentCache[r.id] = r.analysis;
          enriched++;
        } else {
          enrichErrors++;
        }
      });

      const done = Math.min(i + GEMINI_CONCURRENCY, toEnrich.length);
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      const rate = (enriched / (elapsed || 1)).toFixed(1);
      process.stdout.write(
        `\r  Gemini: [${String(done).padStart(4)}/${toEnrich.length}] enriched: ${enriched} | err: ${enrichErrors} | ${elapsed}s | ${rate}/s  `
      );

      if (done % 25 === 0 || done === toEnrich.length) {
        writeFileSync(ENRICHMENT_CACHE, JSON.stringify(enrichmentCache, null, 2), 'utf-8');
      }

      if (i + GEMINI_CONCURRENCY < toEnrich.length) await sleep(GEMINI_DELAY_MS);
    }

    writeFileSync(ENRICHMENT_CACHE, JSON.stringify(enrichmentCache, null, 2), 'utf-8');
    const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n\n  Gemini done. ${enriched} enriched, ${enrichErrors} errors in ${totalTime}s.\n`);
  }

  // ======================================================
  // PHASE 3: Pipedrive Import
  // ======================================================
  console.log('  PHASE 3: Pipedrive Import...\n');

  const importLog = [];
  let success = 0, failed = 0;

  for (let i = 0; i < hotels.length; i += PIPEDRIVE_CONCURRENCY) {
    const batch = hotels.slice(i, i + PIPEDRIVE_CONCURRENCY);

    const results = await Promise.all(batch.map(async (hotel) => {
      const enrichment = enrichmentCache[hotel.id] || null;
      const emailContent = buildEmail(hotel, enrichment);
      const noteHtml = buildDealNote(hotel, enrichment, emailContent);

      const logEntry = {
        hotelId: hotel.id,
        name: hotel.name,
        segment: hotel.segment,
        email: hotel.email,
        hasEnrichment: !!enrichment,
        riskLevel: enrichment?.riskLevel || 'unknown',
        hotelType: enrichment?.hotelType || 'unknown',
        subject: emailContent.subject,
        status: 'pending',
        orgId: null, personId: null, dealId: null,
        error: null,
      };

      if (DRY_RUN) {
        logEntry.status = 'dry-run';
        return logEntry;
      }

      try {
        const org = await createOrganization(hotel, enrichment);
        logEntry.orgId = org.id;

        const person = await createPerson(hotel, org.id);
        logEntry.personId = person.id;

        const deal = await createDeal(hotel, org.id, person.id);
        logEntry.dealId = deal.id;

        await createNote(deal.id, noteHtml);
        await createActivity(deal.id, org.id, person.id, hotel);

        logEntry.status = 'success';
        return logEntry;
      } catch (err) {
        logEntry.status = 'error';
        logEntry.error = err.message;
        return logEntry;
      }
    }));

    results.forEach(r => {
      importLog.push(r);
      if (r.status === 'success' || r.status === 'dry-run') success++;
      else if (r.status === 'error') failed++;
    });

    const done = Math.min(i + PIPEDRIVE_CONCURRENCY, hotels.length);
    process.stdout.write(
      `\r  Pipedrive: [${String(done).padStart(4)}/${hotels.length}] ok: ${success} | err: ${failed}  `
    );

    if (done % 25 === 0 || done === hotels.length) {
      writeFileSync(IMPORT_LOG, JSON.stringify(importLog, null, 2), 'utf-8');
    }

    if (i + PIPEDRIVE_CONCURRENCY < hotels.length) await sleep(PIPEDRIVE_DELAY_MS);
  }

  writeFileSync(IMPORT_LOG, JSON.stringify(importLog, null, 2), 'utf-8');

  // ======================================================
  // SUMMARY
  // ======================================================
  const enrichedCount = hotels.filter(h => enrichmentCache[h.id]).length;
  const highRisk = hotels.filter(h => enrichmentCache[h.id]?.riskLevel === 'high').length;
  const mediumRisk = hotels.filter(h => enrichmentCache[h.id]?.riskLevel === 'medium').length;

  console.log('\n\n  ================ ERGEBNIS ================');
  console.log(`  Hotels verarbeitet:  ${hotels.length}`);
  console.log(`  Pipedrive OK:        ${success}`);
  console.log(`  Pipedrive Fehler:    ${failed}`);
  console.log(`  Enriched (Gemini):   ${enrichedCount}`);
  console.log(`  Close-Chance high:   ${highRisk}`);
  console.log(`  Close-Chance medium: ${mediumRisk}`);
  console.log('');
  console.log('  Segmente:');
  Object.entries(segCounts).forEach(([k, v]) => console.log(`    ${k.padEnd(15)} ${v}`));
  console.log('');
  console.log(`  Scrape-Cache:       ${SCRAPE_CACHE}`);
  console.log(`  Enrichment-Cache:   ${ENRICHMENT_CACHE}`);
  console.log(`  Import-Log:         ${IMPORT_LOG}`);
  console.log('  ==========================================\n');

  if (DRY_RUN) {
    console.log('  DRY RUN complete. Starte live mit:');
    console.log('  node pipedrive-import.mjs\n');
  }

  // Preview
  if (importLog.length > 0) {
    console.log('  --- Erste 3 Hotels ---\n');
    importLog.slice(0, 3).forEach((entry, i) => {
      const e = enrichmentCache[entry.hotelId];
      console.log(`  [${i + 1}] ${entry.name} (${entry.segment})`);
      console.log(`      Typ:    ${e?.hotelType || '-'}`);
      console.log(`      Sterne: ${e?.starCategory || '-'}`);
      console.log(`      Close:  ${e?.riskLevel || '-'} - ${(e?.riskReason || '').substring(0, 60)}`);
      console.log(`      Angle:  ${(e?.salesAngle || '-').substring(0, 80)}`);
      console.log(`      Email:  ${(e?.personalizedEmailSentence || '-').substring(0, 80)}`);
      console.log('');
    });
  }
}

main().catch(err => {
  console.error('\n  FATAL:', err.message);
  console.error(err.stack);
  process.exit(1);
});
