#!/usr/bin/env node
/**
 * GHQI ITB 2025 - Deep Research Pipeline (Phase 2)
 *
 * Nimmt die 880 bereits importierten Leads und reichert sie mit 3 Research-Stufen an:
 *
 * STUFE 1: Portfolio-Sizing
 *   - Ist das ein Einzelhotel oder eine Kette/Gruppe?
 *   - Wie viele Hotels/Properties gehoeren dazu?
 *   - Welche Marken/Sub-Brands?
 *   - Geschaetzter Jahresumsatz-Potential (Anzahl Hotels x 369 EUR)
 *
 * STUFE 2: Persona-Profiling
 *   - Wer ist die Kontaktperson wirklich?
 *   - LinkedIn-Profil-URL (falls findbar)
 *   - Rolle, Senioriaet, Entscheidungsbefugnis
 *   - Empfehlung: Ist das der richtige Ansprechpartner oder muessen wir hoeher?
 *
 * STUFE 3: Company Intelligence
 *   - Aktuelle News, Expansionsplaene
 *   - Pain Points (schlechte Reviews, OTA-Abhaengigkeit, veraltete Website)
 *   - Tech-Stack (PMS, Booking Engine, Channel Manager)
 *   - Wettbewerbsposition im lokalen Markt
 *
 * Nach allen 3 Stufen: Pipedrive Deal-Note updaten mit dem kompletten Dossier.
 *
 * Usage: node deep-research.mjs [--dry-run] [--limit N] [--offset N] [--stufe 1|2|3|all] [--skip-pipedrive]
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';

// ============================================================
// CONFIG
// ============================================================
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';
const GEMINI_URL = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent';
const PIPEDRIVE_API_KEY = process.env.PIPEDRIVE_API_KEY || '';
const PIPEDRIVE_BASE = 'https://api.pipedrive.com/v1';

// Input: bisherige Daten
const ENRICHMENT_CACHE = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Deep_Enrichment_Cache.json';
const IMPORT_LOG = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Pipedrive_Import_Log.json';
const CSV_INPUT = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Hotel_Kontaktdaten.csv';
const SCRAPE_CACHE = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Website_Scrape_Cache.json';

// Output: Deep Research Ergebnisse
const RESEARCH_CACHE = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Deep_Research_Cache.json';

// Concurrency & Timing
const GEMINI_CONCURRENCY = 3;
const GEMINI_DELAY_MS = 600;
const PIPEDRIVE_DELAY_MS = 300;
const SAVE_EVERY = 10;

// ============================================================
// CLI ARGS
// ============================================================
const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const SKIP_PIPEDRIVE = args.includes('--skip-pipedrive');
const limitIdx = args.indexOf('--limit');
const LIMIT = limitIdx >= 0 ? parseInt(args[limitIdx + 1]) : 0;
const offsetIdx = args.indexOf('--offset');
const OFFSET = offsetIdx >= 0 ? parseInt(args[offsetIdx + 1]) : 0;
const stufeIdx = args.indexOf('--stufe');
const STUFE = stufeIdx >= 0 ? args[stufeIdx + 1] : 'all';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ============================================================
// LOAD DATA
// ============================================================
function loadCSV() {
  const raw = readFileSync(CSV_INPUT, 'utf8');
  const lines = raw.replace(/^\uFEFF/, '').split('\n').filter(l => l.trim());
  const header = lines[0].split(';');
  return lines.slice(1).map(line => {
    const cols = line.split(';');
    const obj = {};
    header.forEach((h, i) => { obj[h.trim()] = (cols[i] || '').trim(); });
    return obj;
  });
}

function loadJSON(path) {
  if (!existsSync(path)) return {};
  return JSON.parse(readFileSync(path, 'utf8'));
}

function loadJSONArray(path) {
  if (!existsSync(path)) return [];
  return JSON.parse(readFileSync(path, 'utf8'));
}

function saveJSON(path, data) {
  writeFileSync(path, JSON.stringify(data, null, 2));
}

// ============================================================
// GEMINI HELPER
// ============================================================
async function geminiRequest(prompt, retries = 3) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const resp = await fetch(`${GEMINI_URL}?key=${GEMINI_API_KEY}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: {
            temperature: 0.3,
            maxOutputTokens: 8192,
            topP: 0.9,
            responseMimeType: 'application/json'
          }
        })
      });

      if (resp.status === 503 || resp.status === 429) {
        const wait = 3000 * (attempt + 1);
        console.log(`      Rate limit (${resp.status}), warte ${wait / 1000}s...`);
        if (attempt < retries) { await sleep(wait); continue; }
      }

      if (!resp.ok) {
        const err = await resp.text();
        throw new Error(`Gemini ${resp.status}: ${err.substring(0, 200)}`);
      }

      const data = await resp.json();
      const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '';
      const thoughts = data.usageMetadata?.thoughtsTokenCount || 0;
      const totalTokens = data.usageMetadata?.totalTokenCount || 0;

      if (!text) throw new Error(`Empty response (thoughts: ${thoughts})`);

      // Parse JSON
      let jsonText = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
      jsonText = jsonText.replace(/,\s*}/g, '}').replace(/,\s*]/g, ']');
      const jsonMatch = jsonText.match(/\{[\s\S]*\}/);
      if (!jsonMatch) throw new Error(`No JSON in response (len=${text.length})`);

      let parsed;
      try {
        parsed = JSON.parse(jsonMatch[0]);
      } catch (parseErr) {
        // Salvage truncated JSON
        let salvage = jsonMatch[0];
        if ((salvage.match(/"/g) || []).length % 2 !== 0) salvage += '"';
        const ob = (salvage.match(/\[/g) || []).length - (salvage.match(/\]/g) || []).length;
        for (let b = 0; b < ob; b++) salvage += ']';
        const oc = (salvage.match(/\{/g) || []).length - (salvage.match(/\}/g) || []).length;
        for (let b = 0; b < oc; b++) salvage += '}';
        salvage = salvage.replace(/,\s*}/g, '}').replace(/,\s*]/g, ']');
        parsed = JSON.parse(salvage);
      }

      return { ...parsed, _tokens: totalTokens, _thoughts: thoughts };
    } catch (err) {
      if (attempt < retries) {
        await sleep(2000 * (attempt + 1));
        continue;
      }
      throw err;
    }
  }
}

// ============================================================
// PIPEDRIVE HELPER
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
        if (resp.status === 429 && attempt < retries) {
          await sleep(2000 * (attempt + 1));
          continue;
        }
        throw new Error(`Pipedrive ${method} ${endpoint}: ${data.error || JSON.stringify(data)}`);
      }
      return data.data;
    } catch (err) {
      if (attempt < retries) { await sleep(1000); continue; }
      throw err;
    }
  }
}

function esc(s) { return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

// ============================================================
// STUFE 1: PORTFOLIO-SIZING
// ============================================================
function buildPortfolioPrompt(hotel, enrichment, websiteText) {
  return `Du bist ein Hotel-Industry-Analyst. Recherchiere dieses Unternehmen und bestimme die Portfolio-Groesse.

UNTERNEHMEN:
- Name: ${hotel.Name}
- Stadt: ${hotel.City || 'unbekannt'}
- Land: ${hotel.CountryCode || 'unbekannt'}
- Website: ${hotel.Website || 'keine'}
- Bisherige Analyse - Typ: ${enrichment?.hotelType || 'unbekannt'}
- Bisherige Analyse - Spezialitaet: ${enrichment?.specialties || 'unbekannt'}

${websiteText ? `WEBSITE-TEXT (Auszug):\n${websiteText.substring(0, 3000)}` : ''}

AUFGABE: Analysiere ob dies ein Einzelhotel oder eine Hotel-Gruppe/Kette ist.

Liefere EXAKT dieses JSON:
{
  "isGroup": true/false,
  "totalProperties": <Zahl oder 1 fuer Einzelhotel>,
  "groupName": "Name der Gruppe/Kette falls zutreffend, sonst null",
  "brands": ["Marke1", "Marke2"] oder [] falls Einzelhotel,
  "propertyNames": ["Hotel A", "Hotel B", "Hotel C"] (max 20 - die wichtigsten Properties auflisten),
  "countries": ["DE", "AT", "IT"] (Laender in denen die Gruppe aktiv ist),
  "headquarter": "Stadt, Land",
  "groupType": "independent_single | independent_group | franchise | management_company | chain | dmc_or_operator",
  "annualPotentialEUR": <totalProperties * 369>,
  "priorityTier": "A_enterprise" (>20 Hotels) | "B_group" (5-20) | "C_small_group" (2-4) | "D_single" (1),
  "portfolioNotes": "Ein Satz Kontext zur Gruppe - z.B. Expansionsplaene, Fokus-Regionen etc."
}

REGELN:
- Wenn das Hotel offensichtlich Teil einer grossen Kette ist (Hilton, Marriott, IHG etc.), zaehle NICHT die gesamte Kette sondern schaetze wie viele Properties dieser spezifische Kontakt/Aussteller repraesentiert
- Bei DMCs, Tour Operators oder Reiseveranstaltern: groupType = "dmc_or_operator", totalProperties = Anzahl der direkt betriebenen/verwalteten Hotels
- Nur valides JSON, keine Erklaerungen`;
}

// ============================================================
// STUFE 2: PERSONA-PROFILING
// ============================================================
function buildPersonaPrompt(hotel, enrichment, portfolio) {
  const fullName = [hotel.ContactFirstName, hotel.ContactLastName].filter(Boolean).join(' ');
  return `Du bist ein B2B-Sales-Intelligence-Analyst. Erstelle ein Profil der Kontaktperson.

PERSON:
- Name: ${fullName || 'unbekannt'}
- Email: ${hotel.Email || 'keine'}
- Unternehmen: ${hotel.Name}
- Website: ${hotel.Website || 'keine'}
- Land: ${hotel.CountryCode || 'unbekannt'}
- Unternehmenstyp: ${enrichment?.hotelType || 'unbekannt'}
${portfolio ? `- Portfolio: ${portfolio.totalProperties} Properties, Typ: ${portfolio.groupType}` : ''}

AUFGABE: Recherchiere diese Person. Nutze die Email-Domain und den Namen um Rolle und Senioriaet einzuschaetzen.

Liefere EXAKT dieses JSON:
{
  "fullName": "${fullName}",
  "likelyRole": "Geschaetzte Rolle basierend auf Email-Prefix und Kontext (z.B. General Manager, Director of Sales, Marketing Manager, Owner, CEO)",
  "seniority": "c_level | director | manager | coordinator | unknown",
  "isDecisionMaker": true/false,
  "decisionMakerReason": "Warum diese Einschaetzung? Ein Satz.",
  "linkedinSearchUrl": "https://www.linkedin.com/search/results/people/?keywords=${encodeURIComponent(fullName + ' ' + hotel.Name)}",
  "emailPattern": "Muster der Email z.B. vorname@domain, v.nachname@domain, initialen@domain",
  "languagePreference": "de | en | fr | es | it | other",
  "approachRecommendation": "Wie diese Person am besten ansprechen? Direkt pitchen oder erst Rapport aufbauen? Ein konkreter Tipp.",
  "escalationNeeded": true/false,
  "escalationReason": "Falls escalationNeeded=true: Warum muessen wir hoeher und wen suchen wir? Sonst null."
}

REGELN:
- Email-Prefix analysieren: info@, reservierung@, sales@ = generisch, wahrscheinlich nicht der Entscheider
- vorname.nachname@ oder vorname@ = persoenlich, eher Entscheider
- gm@, director@, ceo@ = starkes Signal fuer Entscheider
- Bei kleinen Hotels (1-3 Properties): Kontaktperson ist oft Owner/GM = Entscheider
- Bei grossen Gruppen: Kontaktperson auf Messe ist oft Sales/Marketing, nicht zwingend Entscheider
- Nur valides JSON`;
}

// ============================================================
// STUFE 3: COMPANY INTELLIGENCE
// ============================================================
function buildIntelPrompt(hotel, enrichment, portfolio, persona) {
  return `Du bist ein Hotel-Industry-Research-Analyst. Erstelle ein Intelligence-Briefing fuer den Sales-Call.

HOTEL/UNTERNEHMEN:
- Name: ${hotel.Name}
- Stadt: ${hotel.City || 'unbekannt'}, Land: ${hotel.CountryCode || 'unbekannt'}
- Website: ${hotel.Website || 'keine'}
- Typ: ${enrichment?.hotelType || 'unbekannt'}
- Sterne: ${enrichment?.starCategory || 'unbekannt'}
- USPs: ${(enrichment?.usps || []).join(', ')}
${portfolio ? `- Portfolio: ${portfolio.totalProperties} Properties (${portfolio.groupType})` : ''}
${persona ? `- Kontakt: ${persona.fullName}, vermutlich ${persona.likelyRole} (${persona.seniority})` : ''}

AUFGABE: Recherchiere alles was fuer einen Sales-Call relevant ist.

Liefere EXAKT dieses JSON:
{
  "recentNews": ["Aktuelle Nachricht oder Entwicklung 1", "Nachricht 2"] oder ["Keine relevanten News gefunden"],
  "expansionPlans": "Bekannte Expansionsplaene oder 'Keine bekannt'",
  "painPoints": ["Vermuteter Pain Point 1 basierend auf Analyse", "Pain Point 2", "Pain Point 3"],
  "techStack": {
    "pms": "Property Management System falls erkennbar, sonst 'unbekannt'",
    "bookingEngine": "Eigene Buchungsmaschine oder Drittanbieter, sonst 'unbekannt'",
    "channelManager": "Falls erkennbar, sonst 'unbekannt'",
    "website": "CMS/Framework falls erkennbar (WordPress, Custom, etc.), sonst 'unbekannt'"
  },
  "onlineReputation": {
    "estimatedGoogleRating": "z.B. 4.2/5 oder 'unbekannt'",
    "reputationSignals": "Positive oder negative Signale basierend auf Website-Qualitaet und Positionierung"
  },
  "competitivePosition": "Wie ist das Hotel im lokalen Markt positioniert? Premium, Mittelfeld, Budget? Ein Satz.",
  "otaPresence": "Geschaetzte OTA-Abhaengigkeit: high (>60% OTA), medium (30-60%), low (<30%), unknown",
  "idealTiming": "Bester Zeitpunkt fuer den Call basierend auf Saisonalitaet und Lage. z.B. 'Jetzt gut - vor der Hauptsaison' oder 'Schwierig - mitten in der Saison'",
  "killArgument": "DAS eine Argument das bei GENAU diesem Hotel am besten zieht fuer GHQI. Ein konkreter Satz.",
  "objectionPrediction": "Welchen Einwand wird diese Person wahrscheinlich bringen? Und die beste Antwort darauf.",
  "dealValue": "low_369" (1 Hotel) | "medium_group" (2-10 Hotels, Potential >1.000 EUR) | "high_enterprise" (>10 Hotels, Potential >5.000 EUR),
  "priorityScore": 1-100 (100 = sofort anrufen, 1 = niedrigste Prio)
}

REGELN:
- Pain Points muessen SPEZIFISCH sein, nicht generisch. "Braucht mehr Sichtbarkeit" ist zu vage. "Website hat keine strukturierten Daten, Google zeigt nur OTA-Ergebnisse fuer den Hotelnamen" ist gut.
- killArgument muss auf EIN konkretes Problem des Hotels eingehen
- objectionPrediction basiert auf Hoteltyp, Groesse und Persona-Senioriaet
- priorityScore gewichtet: Portfolio-Groesse (40%), Entscheider-Zugang (25%), Timing (15%), Pain-Point-Staerke (20%)
- Nur valides JSON`;
}

// ============================================================
// PIPEDRIVE NOTE BUILDER
// ============================================================
function buildResearchNote(hotel, enrichment, research) {
  const p = research.portfolio || {};
  const pe = research.persona || {};
  const intel = research.intel || {};
  const e = enrichment || {};

  const priorityBadge = (intel.priorityScore || 0) >= 70
    ? '🔥 HIGH PRIORITY'
    : (intel.priorityScore || 0) >= 40
      ? '⚡ MEDIUM PRIORITY'
      : '📋 LOW PRIORITY';

  const tierLabel = {
    'A_enterprise': '🏢 ENTERPRISE (20+ Hotels)',
    'B_group': '🏨 GROUP (5-20 Hotels)',
    'C_small_group': '🏠 SMALL GROUP (2-4 Hotels)',
    'D_single': '🏡 SINGLE PROPERTY',
  }[p.priorityTier] || 'unbekannt';

  const dealValueLabel = {
    'high_enterprise': `💰 Enterprise (${p.totalProperties || '?'} x 369 = ${p.annualPotentialEUR || '?'} EUR/Jahr)`,
    'medium_group': `💵 Gruppe (${p.totalProperties || '?'} x 369 = ${p.annualPotentialEUR || '?'} EUR/Jahr)`,
    'low_369': '💶 Einzelhotel (369 EUR/Jahr)',
  }[intel.dealValue] || 'unbekannt';

  const properties = (p.propertyNames || []).map(n => `<li>${esc(n)}</li>`).join('');
  const painPoints = (intel.painPoints || []).map(pp => `<li>${esc(pp)}</li>`).join('');
  const news = (intel.recentNews || []).map(n => `<li>${esc(n)}</li>`).join('');

  return `
<h2>${priorityBadge} - DEEP RESEARCH DOSSIER</h2>
<p><em>Generiert am ${new Date().toISOString().split('T')[0]} via Gemini Deep Research</em></p>

<hr>
<h3>📊 PORTFOLIO-ANALYSE</h3>
<table>
<tr><td><b>Tier</b></td><td>${tierLabel}</td></tr>
<tr><td><b>Gruppe/Kette</b></td><td>${p.isGroup ? `Ja - ${esc(p.groupName || hotel.Name)}` : 'Einzelhotel'}</td></tr>
<tr><td><b>Anzahl Properties</b></td><td><b>${p.totalProperties || 1}</b></td></tr>
<tr><td><b>Typ</b></td><td>${esc(p.groupType || 'unbekannt')}</td></tr>
<tr><td><b>Laender</b></td><td>${(p.countries || []).join(', ') || 'unbekannt'}</td></tr>
<tr><td><b>HQ</b></td><td>${esc(p.headquarter || 'unbekannt')}</td></tr>
<tr><td><b>Deal-Wert</b></td><td>${dealValueLabel}</td></tr>
</table>
${properties ? `<p><b>Properties:</b></p><ul>${properties}</ul>` : ''}
${p.portfolioNotes ? `<p><em>${esc(p.portfolioNotes)}</em></p>` : ''}

<hr>
<h3>👤 PERSONA-PROFIL</h3>
<table>
<tr><td><b>Name</b></td><td>${esc(pe.fullName || 'unbekannt')}</td></tr>
<tr><td><b>Vermutete Rolle</b></td><td>${esc(pe.likelyRole || 'unbekannt')}</td></tr>
<tr><td><b>Senioriaet</b></td><td>${esc(pe.seniority || 'unbekannt')}</td></tr>
<tr><td><b>Entscheider?</b></td><td>${pe.isDecisionMaker ? '✅ Ja' : '❌ Nein'} - ${esc(pe.decisionMakerReason || '')}</td></tr>
<tr><td><b>Sprache</b></td><td>${esc(pe.languagePreference || 'unbekannt')}</td></tr>
<tr><td><b>LinkedIn</b></td><td><a href="${pe.linkedinSearchUrl || '#'}">${esc(pe.fullName || 'Suchen')}</a></td></tr>
</table>
${pe.approachRecommendation ? `<p><b>Approach:</b> ${esc(pe.approachRecommendation)}</p>` : ''}
${pe.escalationNeeded ? `<p>⚠️ <b>ESKALATION NOETIG:</b> ${esc(pe.escalationReason)}</p>` : ''}

<hr>
<h3>🎯 SALES INTELLIGENCE</h3>
<table>
<tr><td><b>Priority Score</b></td><td><b>${intel.priorityScore || '?'}/100</b></td></tr>
<tr><td><b>Deal-Wert</b></td><td>${esc(intel.dealValue || 'unbekannt')}</td></tr>
<tr><td><b>OTA-Abhaengigkeit</b></td><td>${esc(intel.otaPresence || 'unbekannt')}</td></tr>
<tr><td><b>Wettbewerbsposition</b></td><td>${esc(intel.competitivePosition || 'unbekannt')}</td></tr>
<tr><td><b>Ideales Timing</b></td><td>${esc(intel.idealTiming || 'unbekannt')}</td></tr>
</table>

<p><b>🔫 Kill-Argument:</b><br>${esc(intel.killArgument || 'keins')}</p>
<p><b>🛡️ Erwarteter Einwand + Antwort:</b><br>${esc(intel.objectionPrediction || 'keins')}</p>

${painPoints ? `<p><b>Pain Points:</b></p><ul>${painPoints}</ul>` : ''}
${news ? `<p><b>Aktuelle News:</b></p><ul>${news}</ul>` : ''}

<h4>Tech-Stack</h4>
<table>
<tr><td>PMS</td><td>${esc(intel.techStack?.pms || 'unbekannt')}</td></tr>
<tr><td>Booking Engine</td><td>${esc(intel.techStack?.bookingEngine || 'unbekannt')}</td></tr>
<tr><td>Channel Manager</td><td>${esc(intel.techStack?.channelManager || 'unbekannt')}</td></tr>
<tr><td>Website</td><td>${esc(intel.techStack?.website || 'unbekannt')}</td></tr>
</table>

<h4>Online-Reputation</h4>
<table>
<tr><td>Google Rating (geschaetzt)</td><td>${esc(intel.onlineReputation?.estimatedGoogleRating || 'unbekannt')}</td></tr>
<tr><td>Signale</td><td>${esc(intel.onlineReputation?.reputationSignals || 'keine')}</td></tr>
</table>
`;
}

// ============================================================
// BATCH PROCESSOR
// ============================================================
async function processBatch(items, processor, concurrency, delayMs, label) {
  let completed = 0;
  let errors = 0;
  const total = items.length;
  const startTime = Date.now();

  async function worker(queue) {
    while (queue.length > 0) {
      const item = queue.shift();
      try {
        await processor(item);
        completed++;
      } catch (err) {
        errors++;
        console.error(`    ❌ ${label} error: ${err.message.substring(0, 100)}`);
      }
      if (delayMs) await sleep(delayMs);

      if (completed % 25 === 0 || completed === total) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        const rate = (completed / (elapsed || 1)).toFixed(1);
        console.log(`    ${label}: ${completed}/${total} (${errors} errors) - ${elapsed}s - ${rate}/s`);
      }
    }
  }

  const queue = [...items];
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => worker(queue));
  await Promise.all(workers);
  return { completed, errors };
}

// ============================================================
// MAIN
// ============================================================
async function main() {
  console.log('\n=== GHQI ITB 2025 - DEEP RESEARCH PIPELINE ===\n');
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}`);
  console.log(`Stufe: ${STUFE}`);
  console.log(`Pipedrive Update: ${SKIP_PIPEDRIVE ? 'SKIP' : 'ACTIVE'}`);

  // Load all existing data
  const csvRows = loadCSV();
  const enrichment = loadJSON(ENRICHMENT_CACHE);
  const importLog = loadJSONArray(IMPORT_LOG);
  const scrapeCache = loadJSON(SCRAPE_CACHE);
  let researchCache = loadJSON(RESEARCH_CACHE);

  console.log(`\nGeladene Daten:`);
  console.log(`  CSV Rows: ${csvRows.length}`);
  console.log(`  Enrichment Cache: ${Object.keys(enrichment).length}`);
  console.log(`  Import Log: ${importLog.length}`);
  console.log(`  Scrape Cache: ${Object.keys(scrapeCache).length}`);
  console.log(`  Research Cache: ${Object.keys(researchCache).length} (bestehend)`);

  // Build merged dataset: nur Hotels die in Pipedrive sind
  const logByHotelId = {};
  importLog.forEach(l => { logByHotelId[l.hotelId] = l; });

  const csvById = {};
  csvRows.forEach(r => { csvById[r.CompanyID] = r; });

  // Merge: Import Log + CSV + Enrichment + Scrape
  let leads = importLog.map(logEntry => {
    const csv = csvById[logEntry.hotelId] || {};
    const enrich = enrichment[logEntry.hotelId] || null;
    const scrape = scrapeCache[logEntry.hotelId] || null;
    return {
      hotelId: logEntry.hotelId,
      name: logEntry.name,
      csv,
      enrichment: enrich,
      scrapeText: scrape?.ok ? scrape.text : null,
      dealId: logEntry.dealId,
      orgId: logEntry.orgId,
      personId: logEntry.personId,
      segment: logEntry.segment,
    };
  });

  // Apply offset/limit
  if (OFFSET > 0) leads = leads.slice(OFFSET);
  if (LIMIT > 0) leads = leads.slice(0, LIMIT);

  console.log(`\nZu verarbeiten: ${leads.length} Leads`);

  let totalTokens = 0;
  let stufe1Count = 0, stufe2Count = 0, stufe3Count = 0;
  const stufe1Start = Date.now();

  // ----------------------------------------------------------
  // STUFE 1: PORTFOLIO-SIZING
  // ----------------------------------------------------------
  if (STUFE === 'all' || STUFE === '1') {
    console.log('\n--- STUFE 1: PORTFOLIO-SIZING ---');

    const toProcess = leads.filter(l => !researchCache[l.hotelId]?.portfolio);
    console.log(`  Noch zu verarbeiten: ${toProcess.length} (${leads.length - toProcess.length} bereits im Cache)`);

    if (toProcess.length > 0 && !DRY_RUN) {
      await processBatch(toProcess, async (lead) => {
        const prompt = buildPortfolioPrompt(lead.csv, lead.enrichment, lead.scrapeText);
        const result = await geminiRequest(prompt);
        totalTokens += result._tokens || 0;

        if (!researchCache[lead.hotelId]) researchCache[lead.hotelId] = {};
        researchCache[lead.hotelId].portfolio = result;
        delete researchCache[lead.hotelId].portfolio._tokens;
        delete researchCache[lead.hotelId].portfolio._thoughts;
        stufe1Count++;

        if (stufe1Count % SAVE_EVERY === 0) saveJSON(RESEARCH_CACHE, researchCache);
      }, GEMINI_CONCURRENCY, GEMINI_DELAY_MS, 'Portfolio');
      saveJSON(RESEARCH_CACHE, researchCache);
    }

    // Stats
    const portfolios = leads.map(l => researchCache[l.hotelId]?.portfolio).filter(Boolean);
    const tiers = { A_enterprise: 0, B_group: 0, C_small_group: 0, D_single: 0 };
    let totalProperties = 0;
    portfolios.forEach(p => {
      tiers[p.priorityTier] = (tiers[p.priorityTier] || 0) + 1;
      totalProperties += p.totalProperties || 1;
    });
    console.log(`\n  Portfolio-Ergebnis (${portfolios.length} analysiert):`);
    console.log(`    A Enterprise (20+): ${tiers.A_enterprise}`);
    console.log(`    B Group (5-20): ${tiers.B_group}`);
    console.log(`    C Small Group (2-4): ${tiers.C_small_group}`);
    console.log(`    D Single: ${tiers.D_single}`);
    console.log(`    Total Properties: ${totalProperties}`);
    console.log(`    Total Potential: ${totalProperties * 369} EUR/Jahr`);
  }

  // ----------------------------------------------------------
  // STUFE 2: PERSONA-PROFILING
  // ----------------------------------------------------------
  if (STUFE === 'all' || STUFE === '2') {
    console.log('\n--- STUFE 2: PERSONA-PROFILING ---');

    const toProcess = leads.filter(l => !researchCache[l.hotelId]?.persona);
    console.log(`  Noch zu verarbeiten: ${toProcess.length} (${leads.length - toProcess.length} bereits im Cache)`);

    if (toProcess.length > 0 && !DRY_RUN) {
      await processBatch(toProcess, async (lead) => {
        const portfolio = researchCache[lead.hotelId]?.portfolio || null;
        const prompt = buildPersonaPrompt(lead.csv, lead.enrichment, portfolio);
        const result = await geminiRequest(prompt);
        totalTokens += result._tokens || 0;

        if (!researchCache[lead.hotelId]) researchCache[lead.hotelId] = {};
        researchCache[lead.hotelId].persona = result;
        delete researchCache[lead.hotelId].persona._tokens;
        delete researchCache[lead.hotelId].persona._thoughts;
        stufe2Count++;

        if (stufe2Count % SAVE_EVERY === 0) saveJSON(RESEARCH_CACHE, researchCache);
      }, GEMINI_CONCURRENCY, GEMINI_DELAY_MS, 'Persona');
      saveJSON(RESEARCH_CACHE, researchCache);
    }

    // Stats
    const personas = leads.map(l => researchCache[l.hotelId]?.persona).filter(Boolean);
    const seniority = {};
    let decisionMakers = 0, escalations = 0;
    personas.forEach(p => {
      seniority[p.seniority] = (seniority[p.seniority] || 0) + 1;
      if (p.isDecisionMaker) decisionMakers++;
      if (p.escalationNeeded) escalations++;
    });
    console.log(`\n  Persona-Ergebnis (${personas.length} analysiert):`);
    console.log(`    Senioriaet:`, JSON.stringify(seniority));
    console.log(`    Entscheider: ${decisionMakers}`);
    console.log(`    Eskalation noetig: ${escalations}`);
  }

  // ----------------------------------------------------------
  // STUFE 3: COMPANY INTELLIGENCE
  // ----------------------------------------------------------
  if (STUFE === 'all' || STUFE === '3') {
    console.log('\n--- STUFE 3: COMPANY INTELLIGENCE ---');

    const toProcess = leads.filter(l => !researchCache[l.hotelId]?.intel);
    console.log(`  Noch zu verarbeiten: ${toProcess.length} (${leads.length - toProcess.length} bereits im Cache)`);

    if (toProcess.length > 0 && !DRY_RUN) {
      await processBatch(toProcess, async (lead) => {
        const portfolio = researchCache[lead.hotelId]?.portfolio || null;
        const persona = researchCache[lead.hotelId]?.persona || null;
        const prompt = buildIntelPrompt(lead.csv, lead.enrichment, portfolio, persona);
        const result = await geminiRequest(prompt);
        totalTokens += result._tokens || 0;

        if (!researchCache[lead.hotelId]) researchCache[lead.hotelId] = {};
        researchCache[lead.hotelId].intel = result;
        delete researchCache[lead.hotelId].intel._tokens;
        delete researchCache[lead.hotelId].intel._thoughts;
        stufe3Count++;

        if (stufe3Count % SAVE_EVERY === 0) saveJSON(RESEARCH_CACHE, researchCache);
      }, GEMINI_CONCURRENCY, GEMINI_DELAY_MS, 'Intel');
      saveJSON(RESEARCH_CACHE, researchCache);
    }

    // Stats
    const intels = leads.map(l => researchCache[l.hotelId]?.intel).filter(Boolean);
    const scores = intels.map(i => i.priorityScore || 0);
    const avgScore = scores.length ? (scores.reduce((a, b) => a + b, 0) / scores.length).toFixed(1) : 0;
    const hot = scores.filter(s => s >= 70).length;
    const warm = scores.filter(s => s >= 40 && s < 70).length;
    const cold = scores.filter(s => s < 40).length;
    console.log(`\n  Intel-Ergebnis (${intels.length} analysiert):`);
    console.log(`    Avg Priority Score: ${avgScore}/100`);
    console.log(`    Hot (>=70): ${hot}`);
    console.log(`    Warm (40-69): ${warm}`);
    console.log(`    Cold (<40): ${cold}`);
  }

  // ----------------------------------------------------------
  // PIPEDRIVE UPDATE: Research Note an bestehende Deals haengen
  // ----------------------------------------------------------
  if (!SKIP_PIPEDRIVE && !DRY_RUN && (STUFE === 'all' || STUFE === 'pipedrive')) {
    console.log('\n--- PIPEDRIVE UPDATE: Research Notes ---');

    const toUpdate = leads.filter(l => {
      const r = researchCache[l.hotelId];
      return r && r.portfolio && r.persona && r.intel && l.dealId && !r._pipedriveUpdated;
    });

    console.log(`  Deals zu updaten: ${toUpdate.length}`);

    if (toUpdate.length > 0) {
      let updated = 0;
      for (const lead of toUpdate) {
        try {
          const noteHtml = buildResearchNote(lead.csv, lead.enrichment, researchCache[lead.hotelId]);
          await pdRequest('POST', '/notes', {
            content: noteHtml,
            deal_id: lead.dealId,
            pinned_to_deal_flag: 1,
          });
          researchCache[lead.hotelId]._pipedriveUpdated = true;
          updated++;
          if (updated % 25 === 0) {
            console.log(`    Updated: ${updated}/${toUpdate.length}`);
            saveJSON(RESEARCH_CACHE, researchCache);
          }
          await sleep(PIPEDRIVE_DELAY_MS);
        } catch (err) {
          console.error(`    ❌ Pipedrive error for ${lead.name}: ${err.message.substring(0, 100)}`);
        }
      }
      saveJSON(RESEARCH_CACHE, researchCache);
      console.log(`    Fertig: ${updated}/${toUpdate.length} Notes erstellt`);
    }
  }

  // ----------------------------------------------------------
  // FINAL REPORT
  // ----------------------------------------------------------
  const elapsed = ((Date.now() - stufe1Start) / 1000).toFixed(0);
  console.log('\n=== FINAL REPORT ===');
  console.log(`Dauer: ${elapsed}s`);
  console.log(`Gemini Calls: Stufe1=${stufe1Count}, Stufe2=${stufe2Count}, Stufe3=${stufe3Count}`);
  console.log(`Total Tokens (geschaetzt): ${totalTokens.toLocaleString()}`);
  console.log(`Research Cache: ${RESEARCH_CACHE}`);

  // Top 10 by priority score
  const ranked = leads
    .map(l => ({
      name: l.name,
      score: researchCache[l.hotelId]?.intel?.priorityScore || 0,
      properties: researchCache[l.hotelId]?.portfolio?.totalProperties || 1,
      potential: (researchCache[l.hotelId]?.portfolio?.totalProperties || 1) * 369,
      tier: researchCache[l.hotelId]?.portfolio?.priorityTier || '?',
      decisionMaker: researchCache[l.hotelId]?.persona?.isDecisionMaker || false,
    }))
    .sort((a, b) => b.score - a.score || b.potential - a.potential);

  console.log('\n🔥 TOP 20 LEADS:');
  ranked.slice(0, 20).forEach((r, i) => {
    console.log(`  ${(i + 1).toString().padStart(2)}. [${r.score}/100] ${r.name} - ${r.properties} Hotels - ${r.potential} EUR/Jahr - ${r.tier} ${r.decisionMaker ? '✅ DM' : '❌ no DM'}`);
  });

  console.log('\n✅ Deep Research Pipeline fertig.');
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
