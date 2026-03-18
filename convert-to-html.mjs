#!/usr/bin/env node
/**
 * Converts ITB Hotel Contact CSV to a searchable, sortable HTML table
 */

import { readFileSync, writeFileSync } from 'fs';

const CSV_INPUT = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Hotel_Kontaktdaten.csv';
const HTML_OUTPUT = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_Hotel_Kontaktdaten.html';

// Read CSV (skip BOM)
let csv = readFileSync(CSV_INPUT, 'utf-8');
if (csv.charCodeAt(0) === 0xFEFF) csv = csv.slice(1);

const lines = csv.trim().split('\n');
const header = lines[0].split(';');
const rows = lines.slice(1).map(l => l.split(';'));

// Count stats
const withEmail = rows.filter(r => r[2]).length;
const withPhone = rows.filter(r => r[3]).length;
const withWeb = rows.filter(r => r[4]).length;

// Get unique halls for filter
const halls = [...new Set(rows.map(r => r[12]).filter(Boolean))].sort((a, b) => {
  const na = parseFloat(a) || 99;
  const nb = parseFloat(b) || 99;
  return na - nb;
});

// Get unique countries
const countries = [...new Set(rows.map(r => r[9]).filter(Boolean))].sort();

// Escape HTML
const esc = (s) => {
  if (!s) return '';
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
};

// Build table rows
const tableRows = rows.map((r, i) => {
  const [id, name, email, phone, web, street, street2, zip, city, country, first, last, hall, stand, premium] = r;
  const contact = [first, last].filter(Boolean).join(' ');
  const addr = [street, zip, city].filter(Boolean).join(', ');

  const emailCell = email
    ? `<a href="mailto:${esc(email)}" class="email-link">${esc(email)}</a>`
    : '<span class="empty">-</span>';

  const phoneCell = phone
    ? `<a href="tel:${esc(phone)}" class="phone-link">${esc(phone)}</a>`
    : '<span class="empty">-</span>';

  let webCell = '<span class="empty">-</span>';
  if (web) {
    let href = web.trim();
    if (!href.startsWith('http')) href = 'https://' + href;
    // Handle multiple URLs
    if (href.includes(',')) {
      href = href.split(',')[0].trim();
      if (!href.startsWith('http')) href = 'https://' + href;
    }
    const display = href.replace(/^https?:\/\//, '').replace(/\/$/, '');
    webCell = `<a href="${esc(href)}" target="_blank" rel="noopener" class="web-link">${esc(display.substring(0, 35))}${display.length > 35 ? '...' : ''}</a>`;
  }

  return `<tr data-hall="${esc(hall)}" data-country="${esc(country)}" data-has-email="${email ? '1' : '0'}">
  <td class="num">${i + 1}</td>
  <td class="name"><strong>${esc(name)}</strong></td>
  <td class="contact">${esc(contact)}</td>
  <td class="email">${emailCell}</td>
  <td class="phone">${phoneCell}</td>
  <td class="web">${webCell}</td>
  <td class="hall"><span class="hall-badge">${esc(hall)}</span></td>
  <td class="stand">${esc(stand)}</td>
  <td class="addr">${esc(addr)}</td>
  <td class="country">${esc(country)}</td>
</tr>`;
}).join('\n');

// Hall options
const hallOptions = halls.map(h => `<option value="${esc(h)}">Halle ${esc(h)}</option>`).join('\n');
const countryOptions = countries.map(c => `<option value="${esc(c)}">${esc(c)}</option>`).join('\n');

const html = `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ITB 2025 - Hotel Kontaktdaten (${rows.length} Hotels)</title>
<style>
  :root {
    --forest: #1E3A2F;
    --brass: #9A7B4F;
    --ivory: #F7F5F0;
    --green-light: #2D5A47;
    --border: #e2e0db;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: Inter, -apple-system, BlinkMacSystemFont, sans-serif;
    background: var(--ivory);
    color: #333;
    font-size: 13px;
    line-height: 1.4;
  }
  .header {
    background: var(--forest);
    color: white;
    padding: 20px 24px;
    position: sticky;
    top: 0;
    z-index: 100;
  }
  .header h1 {
    font-size: 20px;
    font-weight: 700;
    margin-bottom: 4px;
  }
  .header .sub {
    color: rgba(255,255,255,0.7);
    font-size: 13px;
  }
  .stats-bar {
    display: flex;
    gap: 20px;
    padding: 12px 24px;
    background: var(--green-light);
    color: white;
    font-size: 13px;
    flex-wrap: wrap;
  }
  .stat { display: flex; align-items: center; gap: 6px; }
  .stat-num { font-weight: 700; font-size: 16px; }
  .stat-label { opacity: 0.8; }
  .controls {
    display: flex;
    gap: 10px;
    padding: 12px 24px;
    background: white;
    border-bottom: 1px solid var(--border);
    flex-wrap: wrap;
    align-items: center;
    position: sticky;
    top: 52px;
    z-index: 99;
  }
  .search-input {
    flex: 1;
    min-width: 200px;
    padding: 8px 12px;
    border: 1px solid var(--border);
    border-radius: 6px;
    font-size: 13px;
    outline: none;
  }
  .search-input:focus { border-color: var(--brass); box-shadow: 0 0 0 2px rgba(154,123,79,0.15); }
  select {
    padding: 8px 12px;
    border: 1px solid var(--border);
    border-radius: 6px;
    font-size: 13px;
    background: white;
    cursor: pointer;
  }
  .filter-btn {
    padding: 8px 14px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: white;
    cursor: pointer;
    font-size: 13px;
    transition: all 0.15s;
  }
  .filter-btn:hover { background: var(--ivory); }
  .filter-btn.active { background: var(--forest); color: white; border-color: var(--forest); }
  .result-count {
    font-size: 12px;
    color: #888;
    padding: 0 4px;
    white-space: nowrap;
  }
  .table-wrap {
    overflow-x: auto;
    padding: 0 12px 40px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 8px;
    background: white;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
  }
  thead th {
    background: #f8f7f5;
    padding: 10px 12px;
    text-align: left;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: #666;
    border-bottom: 2px solid var(--border);
    white-space: nowrap;
    cursor: pointer;
    user-select: none;
    position: relative;
  }
  thead th:hover { background: #eee; }
  thead th.sorted-asc::after { content: ' \\25B2'; font-size: 9px; }
  thead th.sorted-desc::after { content: ' \\25BC'; font-size: 9px; }
  tbody tr { border-bottom: 1px solid #f0efec; transition: background 0.1s; }
  tbody tr:hover { background: #faf9f6; }
  tbody tr.hidden { display: none; }
  td { padding: 8px 12px; vertical-align: top; }
  td.num { color: #aaa; font-size: 11px; text-align: center; width: 40px; }
  td.name { min-width: 200px; }
  td.contact { min-width: 130px; color: #555; }
  td.email { min-width: 180px; }
  td.phone { min-width: 120px; }
  td.web { min-width: 150px; }
  td.hall { text-align: center; }
  td.stand { text-align: center; color: #888; }
  td.addr { min-width: 180px; color: #888; font-size: 12px; }
  td.country { text-align: center; }
  a { color: var(--forest); text-decoration: none; }
  a:hover { text-decoration: underline; color: var(--brass); }
  .email-link { word-break: break-all; }
  .web-link { font-size: 12px; }
  .phone-link { white-space: nowrap; }
  .empty { color: #ccc; }
  .hall-badge {
    display: inline-block;
    background: var(--forest);
    color: white;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 11px;
    font-weight: 600;
    white-space: nowrap;
  }
  .copy-btn {
    display: inline-block;
    margin-left: 6px;
    padding: 1px 5px;
    border: 1px solid #ddd;
    border-radius: 3px;
    background: #f9f9f9;
    cursor: pointer;
    font-size: 10px;
    color: #888;
    vertical-align: middle;
  }
  .copy-btn:hover { background: #eee; color: #333; }
  @media (max-width: 768px) {
    .controls { flex-direction: column; }
    .search-input { min-width: 100%; }
    td.addr, td.stand { display: none; }
  }
  @media print {
    .header, .controls, .stats-bar { position: static; }
    .copy-btn { display: none; }
    body { font-size: 10px; }
    td { padding: 4px 6px; }
  }
</style>
</head>
<body>

<div class="header">
  <h1>ITB Berlin 2025 - Hotel Kontaktdaten</h1>
  <div class="sub">GHQI - General Hospitality Quality Institute | ${rows.length} Hotels | Exportiert am ${new Date().toLocaleDateString('de-DE')}</div>
</div>

<div class="stats-bar">
  <div class="stat"><span class="stat-num">${rows.length}</span><span class="stat-label">Hotels</span></div>
  <div class="stat"><span class="stat-num">${withEmail}</span><span class="stat-label">Emails</span></div>
  <div class="stat"><span class="stat-num">${withPhone}</span><span class="stat-label">Telefon</span></div>
  <div class="stat"><span class="stat-num">${withWeb}</span><span class="stat-label">Websites</span></div>
  <div class="stat"><span class="stat-num">${halls.length}</span><span class="stat-label">Hallen</span></div>
  <div class="stat"><span class="stat-num">${countries.length}</span><span class="stat-label">Laender</span></div>
</div>

<div class="controls">
  <input type="text" class="search-input" id="search" placeholder="Suche nach Name, Email, Kontakt, Stadt...">
  <select id="hallFilter">
    <option value="">Alle Hallen</option>
    ${hallOptions}
  </select>
  <select id="countryFilter">
    <option value="">Alle Laender</option>
    ${countryOptions}
  </select>
  <button class="filter-btn" id="emailOnly">Nur mit Email</button>
  <span class="result-count" id="resultCount">${rows.length} Hotels</span>
</div>

<div class="table-wrap">
<table>
<thead>
<tr>
  <th data-col="0">#</th>
  <th data-col="1">Hotel</th>
  <th data-col="2">Kontakt</th>
  <th data-col="3">Email</th>
  <th data-col="4">Telefon</th>
  <th data-col="5">Website</th>
  <th data-col="6">Halle</th>
  <th data-col="7">Stand</th>
  <th data-col="8">Adresse</th>
  <th data-col="9">Land</th>
</tr>
</thead>
<tbody id="tbody">
${tableRows}
</tbody>
</table>
</div>

<script>
const tbody = document.getElementById('tbody');
const rows = [...tbody.querySelectorAll('tr')];
const searchInput = document.getElementById('search');
const hallFilter = document.getElementById('hallFilter');
const countryFilter = document.getElementById('countryFilter');
const emailBtn = document.getElementById('emailOnly');
const resultCount = document.getElementById('resultCount');
let emailOnly = false;

function applyFilters() {
  const q = searchInput.value.toLowerCase();
  const hall = hallFilter.value;
  const country = countryFilter.value;
  let visible = 0;

  rows.forEach(row => {
    const text = row.textContent.toLowerCase();
    const rHall = row.dataset.hall;
    const rCountry = row.dataset.country;
    const rEmail = row.dataset.hasEmail;

    const matchText = !q || text.includes(q);
    const matchHall = !hall || rHall === hall;
    const matchCountry = !country || rCountry === country;
    const matchEmail = !emailOnly || rEmail === '1';

    if (matchText && matchHall && matchCountry && matchEmail) {
      row.classList.remove('hidden');
      visible++;
    } else {
      row.classList.add('hidden');
    }
  });

  resultCount.textContent = visible + ' Hotels';
}

searchInput.addEventListener('input', applyFilters);
hallFilter.addEventListener('change', applyFilters);
countryFilter.addEventListener('change', applyFilters);
emailBtn.addEventListener('click', () => {
  emailOnly = !emailOnly;
  emailBtn.classList.toggle('active', emailOnly);
  applyFilters();
});

// Column sorting
let sortCol = -1;
let sortAsc = true;
document.querySelectorAll('thead th').forEach(th => {
  th.addEventListener('click', () => {
    const col = parseInt(th.dataset.col);
    if (sortCol === col) { sortAsc = !sortAsc; }
    else { sortCol = col; sortAsc = true; }

    document.querySelectorAll('thead th').forEach(t => t.classList.remove('sorted-asc', 'sorted-desc'));
    th.classList.add(sortAsc ? 'sorted-asc' : 'sorted-desc');

    rows.sort((a, b) => {
      const aVal = a.children[col].textContent.trim().toLowerCase();
      const bVal = b.children[col].textContent.trim().toLowerCase();
      if (col === 0) return sortAsc ? parseInt(aVal) - parseInt(bVal) : parseInt(bVal) - parseInt(aVal);
      return sortAsc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
    });

    rows.forEach(r => tbody.appendChild(r));
  });
});

// Keyboard shortcut
searchInput.addEventListener('keydown', e => {
  if (e.key === 'Escape') { searchInput.value = ''; applyFilters(); }
});
document.addEventListener('keydown', e => {
  if ((e.metaKey || e.ctrlKey) && e.key === 'f') {
    e.preventDefault();
    searchInput.focus();
    searchInput.select();
  }
});
</script>
</body>
</html>`;

writeFileSync(HTML_OUTPUT, html, 'utf-8');
console.log(`HTML gespeichert: ${HTML_OUTPUT}`);
console.log(`${rows.length} Hotels, ${(html.length / 1024).toFixed(0)} KB`);
