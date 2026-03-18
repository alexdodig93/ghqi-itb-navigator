#!/usr/bin/env node
/**
 * ITB 2025 - ALL Exhibitor Contact Scraper
 * Uses entity_orga filter + XML parsing to get all company IDs
 * Then fetches contact details via companydetails API
 *
 * Usage: node scrape-all-exhibitors.mjs
 */

import { writeFileSync, existsSync, readFileSync } from 'fs';

const API_BASE = 'https://live.messebackend.aws.corussoft.de/webservice';
const CONCURRENCY = 8;
const DELAY_MS = 100;
const OUTPUT_DIR = '/Users/j.a.r.v.i.s/Downloads';
const CACHE_FILE = `${OUTPUT_DIR}/ITB_2025_ALL_IDs_cache.json`;
const DETAILS_CACHE = `${OUTPUT_DIR}/ITB_2025_ALL_Details_cache.json`;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// Step 1: Get ALL company IDs via search API (XML response, entity_orga filter)
async function fetchAllCompanyIds() {
  const PAGE_SIZE = 500;

  // First call to get total count
  const firstResp = await fetch(`${API_BASE}/search`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
    body: `topic=2023_itb&os=website&appUrl=navigate.itb.com&lang=de&apiVersion=88&timezoneOffset=-60&numresultrows=1&startresultrow=0&filterlist=entity_orga&order=name&secondaryOrder=&desc=0&alpha=`
  });
  const firstText = await firstResp.text();
  const countMatch = firstText.match(/count="(\d+)"/);
  if (!countMatch) throw new Error('Could not get total count');
  const total = parseInt(countMatch[1]);
  console.log(`  Gesamt: ${total} Aussteller gefunden\n`);

  // Paginate through all results
  const allCompanies = new Map();
  for (let start = 0; start < total; start += PAGE_SIZE) {
    const resp = await fetch(`${API_BASE}/search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
      body: `topic=2023_itb&os=website&appUrl=navigate.itb.com&lang=de&apiVersion=88&timezoneOffset=-60&numresultrows=${PAGE_SIZE}&startresultrow=${start}&filterlist=entity_orga&order=name&secondaryOrder=&desc=0&alpha=`
    });
    const xml = await resp.text();

    // Parse organization entries from XML attributes
    const orgRegex = /<organization\s+id="(\d+)"[^>]*name="([^"]*)"[^>]*countryCode="([^"]*)"[^>]*country="([^"]*)"[^>]*city="([^"]*)"[^>]*postCode="([^"]*)"[^>]*basisPremium="([^"]*)"/g;
    let match;
    while ((match = orgRegex.exec(xml)) !== null) {
      const [, id, name, countryCode, country, city, postCode, premium] = match;
      if (!allCompanies.has(id)) {
        // Also extract hall/stand from stands
        const orgBlock = xml.substring(match.index, xml.indexOf('</organization>', match.index) + 15);
        const hallMatch = orgBlock.match(/hallNr="([^"]*)"/);
        const standMatch = orgBlock.match(/standNr="([^"]*)"/);
        const categoryMatches = [...orgBlock.matchAll(/categoryName="([^"]*)"/g)];

        allCompanies.set(id, {
          id, name,
          countryCode, country, city, postCode, premium,
          hall: hallMatch?.[1] || '',
          stand: standMatch?.[1] || '',
          categories: categoryMatches.map(m => m[1]).join(', ')
        });
      }
    }

    const done = Math.min(start + PAGE_SIZE, total);
    process.stdout.write(`\r  IDs gesammelt: [${String(done).padStart(5)}/${total}] Unique: ${allCompanies.size}`);
    await sleep(150);
  }
  console.log('');
  return allCompanies;
}

// Extract CDATA/text from XML tag
function extractTag(xml, tagName) {
  const re = new RegExp(`<${tagName}>(?:<!\\[CDATA\\[)?(.*?)(?:\\]\\]>)?</${tagName}>`, 's');
  const m = xml.match(re);
  return m ? m[1].trim() : '';
}

function extractNested(xml, parent, child) {
  const parentRe = new RegExp(`<${parent}>(.*?)</${parent}>`, 's');
  const pm = xml.match(parentRe);
  if (!pm) return '';
  return extractTag(pm[1], child);
}

// Fetch company contact details
async function fetchCompanyDetails(companyId) {
  const resp = await fetch(`${API_BASE}/companydetails`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
    body: `topic=2023_itb&os=website&appUrl=navigate.itb.com&lang=de&apiVersion=88&timezoneOffset=-60&companyid=${companyId}`
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  const xml = await resp.text();
  if (xml.includes('<errorCode>')) throw new Error(extractTag(xml, 'errorMessage'));

  return {
    email: extractTag(xml, 'email'),
    phone: extractTag(xml, 'phone'),
    web: extractTag(xml, 'web'),
    street: extractNested(xml, 'adress', 'adress1'),
    street2: extractNested(xml, 'adress', 'adress3'),
    contactFirstName: extractNested(xml, 'contactPerson', 'firstName'),
    contactLastName: extractNested(xml, 'contactPerson', 'lastName'),
  };
}

function csvEscape(val) {
  if (!val) return '';
  val = String(val);
  if (val.includes(';') || val.includes('"') || val.includes('\n') || val.includes('\r')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

async function main() {
  console.log('');
  console.log('  =============================================');
  console.log('  ITB 2025 - ALLE Aussteller Scraper');
  console.log('  GHQI - General Hospitality Quality Institute');
  console.log('  =============================================');
  console.log('');

  // Step 1: Get all IDs
  let companiesMap;

  if (existsSync(CACHE_FILE)) {
    console.log('  ID-Cache gefunden...');
    const cached = JSON.parse(readFileSync(CACHE_FILE, 'utf-8'));
    companiesMap = new Map(cached.map(c => [c.id, c]));
    console.log(`  ${companiesMap.size} IDs aus Cache\n`);
  } else {
    console.log('  SCHRITT 1: Alle Company IDs sammeln\n');
    companiesMap = await fetchAllCompanyIds();
    // Cache
    writeFileSync(CACHE_FILE, JSON.stringify([...companiesMap.values()]), 'utf-8');
    console.log(`\n  ${companiesMap.size} IDs gecacht\n`);
  }

  // Step 2: Fetch contact details
  let allDetails;

  if (existsSync(DETAILS_CACHE)) {
    console.log('  Details-Cache gefunden...');
    allDetails = JSON.parse(readFileSync(DETAILS_CACHE, 'utf-8'));
    console.log(`  ${allDetails.length} Details aus Cache\n`);
  } else {
    console.log(`  SCHRITT 2: Kontaktdaten fuer ${companiesMap.size} Aussteller\n`);
    const companies = [...companiesMap.values()];
    allDetails = [];
    let errors = 0;
    const startTime = Date.now();

    for (let i = 0; i < companies.length; i += CONCURRENCY) {
      const batch = companies.slice(i, i + CONCURRENCY);
      const results = await Promise.all(batch.map(async (c) => {
        try {
          const details = await fetchCompanyDetails(c.id);
          return { ...c, ...details };
        } catch(err) {
          errors++;
          return { ...c, email: '', phone: '', web: '', street: '', street2: '', contactFirstName: '', contactLastName: '' };
        }
      }));
      allDetails.push(...results);

      const done = Math.min(i + CONCURRENCY, companies.length);
      const emailCount = allDetails.filter(d => d.email).length;
      const webCount = allDetails.filter(d => d.web).length;
      process.stdout.write(
        `\r  [${String(done).padStart(5)}/${companies.length}] Emails: ${emailCount} | Web: ${webCount} | Err: ${errors}  `
      );

      if (i + CONCURRENCY < companies.length) await sleep(DELAY_MS);
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n\n  ${allDetails.length} Details in ${duration}s\n`);

    // Cache
    writeFileSync(DETAILS_CACHE, JSON.stringify(allDetails), 'utf-8');
  }

  // Stats
  const withEmail = allDetails.filter(r => r.email).length;
  const withPhone = allDetails.filter(r => r.phone).length;
  const withWeb = allDetails.filter(r => r.web).length;
  const withContact = allDetails.filter(r => r.contactFirstName || r.contactLastName).length;

  // Category stats
  const catCounts = {};
  allDetails.forEach(d => {
    (d.categories || '').split(', ').filter(Boolean).forEach(cat => {
      catCounts[cat] = (catCounts[cat] || 0) + 1;
    });
  });

  console.log('  ========== ERGEBNIS ==========');
  console.log(`  Gesamt:          ${allDetails.length} Aussteller`);
  console.log(`  Mit Email:       ${withEmail} (${(withEmail/allDetails.length*100).toFixed(1)}%)`);
  console.log(`  Mit Telefon:     ${withPhone} (${(withPhone/allDetails.length*100).toFixed(1)}%)`);
  console.log(`  Mit Website:     ${withWeb} (${(withWeb/allDetails.length*100).toFixed(1)}%)`);
  console.log(`  Mit Kontakt:     ${withContact} (${(withContact/allDetails.length*100).toFixed(1)}%)`);
  console.log('  ==============================\n');

  console.log('  --- Top Branchen ---');
  Object.entries(catCounts).sort((a, b) => b[1] - a[1]).slice(0, 20).forEach(([cat, cnt]) => {
    console.log(`    ${cat.padEnd(55)} ${String(cnt).padStart(5)}`);
  });
  console.log('');

  // Write CSV
  const csvHeader = [
    'CompanyID', 'Name', 'Branchen', 'Email', 'Phone', 'Website',
    'Street', 'Street2', 'ZipCode', 'City', 'Country', 'CountryCode',
    'ContactFirstName', 'ContactLastName',
    'Hall', 'Stand', 'Premium'
  ].join(';');

  const csvRows = allDetails.map(r => [
    r.id, r.name, r.categories, r.email, r.phone, r.web,
    r.street, r.street2, r.postCode, r.city, r.country, r.countryCode,
    r.contactFirstName, r.contactLastName,
    r.hall, r.stand, r.premium
  ].map(csvEscape).join(';'));

  const csvPath = `${OUTPUT_DIR}/ITB_2025_ALLE_Aussteller_Kontaktdaten.csv`;
  writeFileSync(csvPath, '\uFEFF' + [csvHeader, ...csvRows].join('\n'), 'utf-8');
  console.log(`  CSV: ${csvPath}`);
  console.log(`  ${csvRows.length} Zeilen\n`);
}

main().catch(err => {
  console.error('\n  FATAL:', err.message);
  process.exit(1);
});
