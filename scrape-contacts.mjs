#!/usr/bin/env node
/**
 * ITB 2025 - Hotel Contact Data Scraper
 * Scrapes contact details for all 1,671 hotels via companydetails API
 * No dependencies - uses native fetch + regex XML parsing
 *
 * Usage: node scrape-contacts.mjs
 * Output: ITB_2025_Hotel_Kontaktdaten.csv
 */

import { readFileSync, writeFileSync } from 'fs';

const CSV_INPUT = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Unterbringung_Unterkunft_1671_Aussteller.csv';
const CSV_OUTPUT = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Hotel_Kontaktdaten.csv';
const API_URL = 'https://live.messebackend.aws.corussoft.de/webservice/companydetails';
const CONCURRENCY = 5;
const DELAY_MS = 150;

// Parse CSV to extract IDs
function loadIds() {
  const csv = readFileSync(CSV_INPUT, 'utf-8');
  const lines = csv.trim().split('\n');
  const cols = lines[0].split(';');
  const idIdx = cols.indexOf('ID');
  const nameIdx = cols.indexOf('Name');

  return lines.slice(1).map(line => {
    const parts = line.split(';');
    return { id: parts[idIdx], name: parts[nameIdx] };
  }).filter(h => h.id && h.id.trim() !== '');
}

// Extract CDATA content from XML tag
function extractTag(xml, tagName) {
  // Handles both <tag><![CDATA[value]]></tag> and <tag>value</tag>
  const re = new RegExp(`<${tagName}>(?:<!\\[CDATA\\[)?(.*?)(?:\\]\\]>)?</${tagName}>`, 's');
  const m = xml.match(re);
  return m ? m[1].trim() : '';
}

// Extract nested tag (e.g. adress > city)
function extractNested(xml, parent, child) {
  const parentRe = new RegExp(`<${parent}>(.*?)</${parent}>`, 's');
  const pm = xml.match(parentRe);
  if (!pm) return '';
  return extractTag(pm[1], child);
}

// Fetch company details from API
async function fetchCompanyDetails(companyId) {
  const body = new URLSearchParams({
    topic: '2023_itb',
    os: 'website',
    appUrl: 'navigate.itb.com',
    lang: 'de',
    apiVersion: '88',
    timezoneOffset: '-60',
    companyid: companyId
  }).toString();

  const resp = await fetch(API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
    body
  });

  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

  const xml = await resp.text();

  // Check for error response
  if (xml.includes('<errorCode>')) {
    const errMsg = extractTag(xml, 'errorMessage');
    throw new Error(`API Error: ${errMsg}`);
  }

  return {
    companyID: extractTag(xml, 'companyID'),
    companyName: extractTag(xml, 'companyName'),
    email: extractTag(xml, 'email'),
    phone: extractTag(xml, 'phone'),
    web: extractTag(xml, 'web'),
    street: extractNested(xml, 'adress', 'adress1'),
    street2: extractNested(xml, 'adress', 'adress3'),
    zipCode: extractNested(xml, 'adress', 'zipCode'),
    city: extractNested(xml, 'adress', 'city'),
    countryCode: extractNested(xml, 'adress', 'countryCode'),
    contactFirstName: extractNested(xml, 'contactPerson', 'firstName'),
    contactLastName: extractNested(xml, 'contactPerson', 'lastName'),
    hallNr: extractTag(xml, 'hallNr'),
    standNr: extractTag(xml, 'standNr'),
    isPremium: extractTag(xml, 'isPremium')
  };
}

// Escape CSV value
function csvEscape(val) {
  if (!val) return '';
  val = String(val);
  if (val.includes(';') || val.includes('"') || val.includes('\n') || val.includes('\r')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

// Sleep helper
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// Process in batches with concurrency control
async function processBatch(hotels) {
  const results = [];
  let done = 0;
  let errors = 0;
  const failed = [];

  for (let i = 0; i < hotels.length; i += CONCURRENCY) {
    const batch = hotels.slice(i, i + CONCURRENCY);
    const promises = batch.map(async (hotel) => {
      try {
        return await fetchCompanyDetails(hotel.id);
      } catch (err) {
        errors++;
        failed.push({ id: hotel.id, name: hotel.name, error: err.message });
        return null;
      }
    });

    const batchResults = await Promise.all(promises);
    results.push(...batchResults);
    done += batch.length;

    const emailCount = results.filter(r => r && r.email).length;
    const webCount = results.filter(r => r && r.web).length;

    process.stdout.write(
      `\r  [${String(done).padStart(4)}/${hotels.length}] ` +
      `Emails: ${emailCount} | Websites: ${webCount} | Fehler: ${errors}  `
    );

    if (i + CONCURRENCY < hotels.length) {
      await sleep(DELAY_MS);
    }
  }

  console.log('');
  return { results, failed };
}

// Main
async function main() {
  console.log('');
  console.log('  ========================================');
  console.log('  ITB 2025 - Hotel Contact Scraper');
  console.log('  GHQI - General Hospitality Quality Institute');
  console.log('  ========================================');
  console.log('');

  // Load IDs
  const hotels = loadIds();
  console.log(`  ${hotels.length} Hotels aus CSV geladen`);
  console.log(`  Concurrency: ${CONCURRENCY} | Delay: ${DELAY_MS}ms`);
  console.log('');
  console.log('  Starte Scraping...');
  console.log('');

  const startTime = Date.now();
  const { results, failed } = await processBatch(hotels);
  const duration = ((Date.now() - startTime) / 1000).toFixed(1);

  // Stats
  const valid = results.filter(r => r !== null);
  const withEmail = valid.filter(r => r.email);
  const withPhone = valid.filter(r => r.phone);
  const withWeb = valid.filter(r => r.web);
  const withContact = valid.filter(r => r.contactFirstName || r.contactLastName);
  const withAddress = valid.filter(r => r.city);

  console.log('');
  console.log('  ========== ERGEBNIS ==========');
  console.log(`  Gesamt:          ${valid.length}/${hotels.length} Hotels (${duration}s)`);
  console.log(`  Mit Email:       ${withEmail.length} (${(withEmail.length/valid.length*100).toFixed(1)}%)`);
  console.log(`  Mit Telefon:     ${withPhone.length} (${(withPhone.length/valid.length*100).toFixed(1)}%)`);
  console.log(`  Mit Website:     ${withWeb.length} (${(withWeb.length/valid.length*100).toFixed(1)}%)`);
  console.log(`  Mit Kontakt:     ${withContact.length} (${(withContact.length/valid.length*100).toFixed(1)}%)`);
  console.log(`  Mit Adresse:     ${withAddress.length} (${(withAddress.length/valid.length*100).toFixed(1)}%)`);
  console.log('  ==============================');

  if (failed.length > 0) {
    console.log(`\n  ${failed.length} Fehler:`);
    failed.slice(0, 10).forEach(f => console.log(`    - ${f.id}: ${f.name} (${f.error})`));
    if (failed.length > 10) console.log(`    ... und ${failed.length - 10} weitere`);
  }

  // Write CSV
  const csvHeader = [
    'CompanyID', 'Name', 'Email', 'Phone', 'Website',
    'Street', 'Street2', 'ZipCode', 'City', 'CountryCode',
    'ContactFirstName', 'ContactLastName',
    'Hall', 'Stand', 'IsPremium'
  ].join(';');

  const csvRows = valid.map(r => [
    r.companyID, r.companyName, r.email, r.phone, r.web,
    r.street, r.street2, r.zipCode, r.city, r.countryCode,
    r.contactFirstName, r.contactLastName,
    r.hallNr, r.standNr, r.isPremium
  ].map(csvEscape).join(';'));

  const csvContent = '\uFEFF' + [csvHeader, ...csvRows].join('\n'); // BOM for Excel
  writeFileSync(CSV_OUTPUT, csvContent, 'utf-8');
  console.log(`\n  CSV gespeichert: ${CSV_OUTPUT}`);
  console.log(`  ${csvRows.length} Zeilen geschrieben`);
  console.log('');
}

main().catch(err => {
  console.error('\n  FATAL ERROR:', err.message);
  process.exit(1);
});
