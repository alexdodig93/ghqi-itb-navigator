#!/usr/bin/env node
/**
 * Converts ALL ITB Exhibitor CSV to searchable HTML
 */
import { readFileSync, writeFileSync } from 'fs';

const CSV_INPUT = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_ALLE_Aussteller_Kontaktdaten.csv';
const HTML_OUTPUT = '/Users/j.a.r.v.i.s/Downloads/ITB_2025_ALLE_Aussteller.html';
const DEPLOY_OUTPUT = '/Users/j.a.r.v.i.s/Desktop/REPOS/GHQI REPOS/itb-navigator/alle-aussteller.html';

let csv = readFileSync(CSV_INPUT, 'utf-8');
if (csv.charCodeAt(0) === 0xFEFF) csv = csv.slice(1);

const lines = csv.trim().split('\n');
const header = lines[0].split(';');
const rows = lines.slice(1).map(l => l.split(';'));

const withEmail = rows.filter(r => r[3]).length;
const withPhone = rows.filter(r => r[4]).length;
const withWeb = rows.filter(r => r[5]).length;

// Unique halls, countries, categories
const halls = [...new Set(rows.map(r => r[14]).filter(Boolean))].sort((a, b) => {
  const na = parseFloat(a) || 99; const nb = parseFloat(b) || 99; return na - nb;
});
const countries = [...new Set(rows.map(r => r[10]).filter(Boolean))].sort();
const allCats = new Set();
rows.forEach(r => (r[2] || '').split(', ').filter(Boolean).forEach(c => allCats.add(c)));
const categories = [...allCats].sort();

const esc = s => !s ? '' : s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

const tableRows = rows.map((r, i) => {
  const [id, name, cats, email, phone, web, street, street2, zip, city, country, cc, first, last, hall, stand, premium] = r;
  const contact = [first, last].filter(Boolean).join(' ');

  const emailCell = email ? `<a href="mailto:${esc(email)}" class="eml">${esc(email)}</a>` : '<span class="empty">-</span>';
  const phoneCell = phone ? `<a href="tel:${esc(phone)}" class="phn">${esc(phone)}</a>` : '<span class="empty">-</span>';

  let webCell = '<span class="empty">-</span>';
  if (web) {
    let href = web.trim();
    if (!href.startsWith('http')) href = 'https://' + href;
    if (href.includes(',')) href = href.split(',')[0].trim();
    if (!href.startsWith('http')) href = 'https://' + href;
    const disp = href.replace(/^https?:\/\//, '').replace(/\/$/, '');
    webCell = `<a href="${esc(href)}" target="_blank" rel="noopener" class="web">${esc(disp.substring(0, 30))}${disp.length > 30 ? '...' : ''}</a>`;
  }

  const catBadges = (cats || '').split(', ').filter(Boolean).map(c =>
    `<span class="cat-badge">${esc(c)}</span>`
  ).join(' ');

  return `<tr data-hall="${esc(hall)}" data-country="${esc(country)}" data-cc="${esc(cc)}" data-cats="${esc(cats)}" data-has-email="${email ? '1' : '0'}">
<td class="num">${i + 1}</td>
<td class="nm"><strong>${esc(name)}</strong><div class="cats">${catBadges}</div></td>
<td class="ct">${esc(contact)}</td>
<td class="em">${emailCell}</td>
<td class="ph">${phoneCell}</td>
<td class="wb">${webCell}</td>
<td class="hl"><span class="hall-badge">${esc(hall)}</span></td>
<td class="st">${esc(stand)}</td>
<td class="co">${esc(country)}</td>
</tr>`;
}).join('\n');

const hallOpts = halls.map(h => `<option value="${esc(h)}">Halle ${esc(h)}</option>`).join('\n');
const countryOpts = countries.map(c => `<option value="${esc(c)}">${esc(c)}</option>`).join('\n');
const catOpts = categories.map(c => `<option value="${esc(c)}">${esc(c)}</option>`).join('\n');

const html = `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ITB 2025 - Alle Aussteller (${rows.length})</title>
<style>
:root{--forest:#1E3A2F;--brass:#9A7B4F;--ivory:#F7F5F0;--gl:#2D5A47;--border:#e2e0db}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Inter,-apple-system,sans-serif;background:var(--ivory);color:#333;font-size:13px;line-height:1.4}
.hdr{background:var(--forest);color:#fff;padding:16px 20px;position:sticky;top:0;z-index:100}
.hdr h1{font-size:18px;font-weight:700}
.hdr .sub{color:rgba(255,255,255,.7);font-size:12px}
.stats{display:flex;gap:16px;padding:10px 20px;background:var(--gl);color:#fff;font-size:12px;flex-wrap:wrap}
.stat{display:flex;align-items:center;gap:4px}
.stat b{font-size:15px}
.ctl{display:flex;gap:8px;padding:10px 20px;background:#fff;border-bottom:1px solid var(--border);flex-wrap:wrap;align-items:center;position:sticky;top:52px;z-index:99}
.ctl input[type=text]{flex:1;min-width:180px;padding:7px 10px;border:1px solid var(--border);border-radius:5px;font-size:13px;outline:none}
.ctl input:focus{border-color:var(--brass)}
.ctl select{padding:7px 10px;border:1px solid var(--border);border-radius:5px;font-size:12px;background:#fff;max-width:180px}
.fbtn{padding:7px 12px;border:1px solid var(--border);border-radius:5px;background:#fff;cursor:pointer;font-size:12px}
.fbtn:hover{background:var(--ivory)}
.fbtn.active{background:var(--forest);color:#fff;border-color:var(--forest)}
.rc{font-size:11px;color:#888;white-space:nowrap}
.tw{overflow-x:auto;padding:0 10px 40px}
table{width:100%;border-collapse:collapse;margin-top:6px;background:#fff;border-radius:6px;overflow:hidden;box-shadow:0 1px 2px rgba(0,0,0,.06)}
thead th{background:#f8f7f5;padding:8px 10px;text-align:left;font-weight:600;font-size:10px;text-transform:uppercase;letter-spacing:.4px;color:#666;border-bottom:2px solid var(--border);white-space:nowrap;cursor:pointer;user-select:none}
thead th:hover{background:#eee}
thead th.sa::after{content:' \\25B2';font-size:8px}
thead th.sd::after{content:' \\25BC';font-size:8px}
tbody tr{border-bottom:1px solid #f0efec;transition:background .1s}
tbody tr:hover{background:#faf9f6}
tbody tr.hid{display:none}
td{padding:6px 10px;vertical-align:top}
.num{color:#aaa;font-size:11px;text-align:center;width:35px}
.nm{min-width:180px}
.ct{min-width:110px;color:#555}
.em{min-width:160px}
.ph{min-width:100px}
.wb{min-width:130px}
.hl{text-align:center}
.st{text-align:center;color:#888}
.co{text-align:center;font-size:12px}
a{color:var(--forest);text-decoration:none}
a:hover{text-decoration:underline;color:var(--brass)}
.eml{word-break:break-all;font-size:12px}
.web{font-size:11px}
.phn{white-space:nowrap;font-size:12px}
.empty{color:#ccc}
.hall-badge{display:inline-block;background:var(--forest);color:#fff;padding:2px 7px;border-radius:9px;font-size:10px;font-weight:600;white-space:nowrap}
.cats{margin-top:3px}
.cat-badge{display:inline-block;background:#f0ede7;color:#666;padding:1px 5px;border-radius:3px;font-size:10px;margin:1px 2px 1px 0}
@media(max-width:768px){.ctl{flex-direction:column}.ctl input[type=text]{min-width:100%}.st,.co{display:none}}
@media print{.hdr,.ctl,.stats{position:static}body{font-size:9px}td{padding:3px 5px}}
</style>
</head>
<body>
<div class="hdr">
<h1>ITB Berlin 2025 - Alle Aussteller</h1>
<div class="sub">GHQI | ${rows.length} Aussteller | ${new Date().toLocaleDateString('de-DE')}</div>
</div>
<div class="stats">
<div class="stat"><b>${rows.length}</b> Aussteller</div>
<div class="stat"><b>${withEmail}</b> Emails</div>
<div class="stat"><b>${withPhone}</b> Telefon</div>
<div class="stat"><b>${withWeb}</b> Websites</div>
<div class="stat"><b>${halls.length}</b> Hallen</div>
<div class="stat"><b>${countries.length}</b> Laender</div>
</div>
<div class="ctl">
<input type="text" id="q" placeholder="Suche nach Name, Email, Kontakt, Stadt...">
<select id="fHall"><option value="">Alle Hallen</option>${hallOpts}</select>
<select id="fCountry"><option value="">Alle Laender</option>${countryOpts}</select>
<select id="fCat"><option value="">Alle Branchen</option>${catOpts}</select>
<button class="fbtn" id="fEmail">Nur mit Email</button>
<span class="rc" id="rc">${rows.length} Aussteller</span>
</div>
<div class="tw">
<table>
<thead><tr>
<th data-c="0">#</th><th data-c="1">Aussteller</th><th data-c="2">Kontakt</th>
<th data-c="3">Email</th><th data-c="4">Telefon</th><th data-c="5">Website</th>
<th data-c="6">Halle</th><th data-c="7">Stand</th><th data-c="8">Land</th>
</tr></thead>
<tbody id="tb">${tableRows}</tbody>
</table>
</div>
<script>
const tb=document.getElementById('tb'),rows=[...tb.querySelectorAll('tr')],
q=document.getElementById('q'),fH=document.getElementById('fHall'),
fC=document.getElementById('fCountry'),fCat=document.getElementById('fCat'),
fE=document.getElementById('fEmail'),rc=document.getElementById('rc');
let eo=false;
function af(){const s=q.value.toLowerCase(),h=fH.value,c=fC.value,cat=fCat.value;let v=0;
rows.forEach(r=>{const t=r.textContent.toLowerCase(),rh=r.dataset.hall,rc2=r.dataset.country,
re=r.dataset.hasEmail,rcat=r.dataset.cats||'';
const m=(!s||t.includes(s))&&(!h||rh===h)&&(!c||rc2===c)&&(!cat||rcat.includes(cat))&&(!eo||re==='1');
if(m){r.classList.remove('hid');v++}else{r.classList.add('hid')}});
rc.textContent=v+' Aussteller'}
q.addEventListener('input',af);fH.addEventListener('change',af);
fC.addEventListener('change',af);fCat.addEventListener('change',af);
fE.addEventListener('click',()=>{eo=!eo;fE.classList.toggle('active',eo);af()});
let sc=-1,sa=true;
document.querySelectorAll('thead th').forEach(th=>{th.addEventListener('click',()=>{
const c=parseInt(th.dataset.c);if(sc===c)sa=!sa;else{sc=c;sa=true}
document.querySelectorAll('thead th').forEach(t=>t.classList.remove('sa','sd'));
th.classList.add(sa?'sa':'sd');
rows.sort((a,b)=>{const av=a.children[c].textContent.trim().toLowerCase(),
bv=b.children[c].textContent.trim().toLowerCase();
if(c===0)return sa?parseInt(av)-parseInt(bv):parseInt(bv)-parseInt(av);
return sa?av.localeCompare(bv):bv.localeCompare(av)});
rows.forEach(r=>tb.appendChild(r))})});
q.addEventListener('keydown',e=>{if(e.key==='Escape'){q.value='';af()}});
document.addEventListener('keydown',e=>{if((e.metaKey||e.ctrlKey)&&e.key==='f'){e.preventDefault();q.focus();q.select()}});
</script>
</body>
</html>`;

writeFileSync(HTML_OUTPUT, html, 'utf-8');
writeFileSync(DEPLOY_OUTPUT, html, 'utf-8');
console.log(`HTML: ${HTML_OUTPUT} (${(html.length/1024).toFixed(0)} KB)`);
console.log(`Deploy: ${DEPLOY_OUTPUT}`);
console.log(`${rows.length} Aussteller`);
