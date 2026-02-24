const els = {
  input: document.querySelector('#companySearch'),
  suggestions: document.querySelector('#suggestions'),
  overview: document.querySelector('#companyOverview'),
  competitorBody: document.querySelector('#competitorBody'),
  top5Body: document.querySelector('#top5Body'),
  supplierBody: document.querySelector('#supplierBody'),
  customerBody: document.querySelector('#customerBody'),
  networkStatus: document.querySelector('#networkStatus'),
};

let API_BASE = '';
const apiUrl = (path) => `${API_BASE}${path}`;
const ALLOW_OFFLINE_DEMO = !!window.ENABLE_OFFLINE_DEMO;

const state = {
  suggestTimer: null,
  searchTimer: null,
  seq: 0,
  suggestSeq: 0,
  stream: null,
  etaTimer: null,
  lastSearched: '',
  suggestAbort: null,
  apiReady: Promise.resolve(),
  apiCandidates: [],
  apiIndex: 0,
  offlineMode: false,
  localCompanies: [],
};

init();

function init() {
  setNetworkStatus('checking');
  state.apiReady = ensureApiBase();
  wireEvents();
  resetAll();
}

function wireEvents() {
  els.input.addEventListener('input', (e) => {
    const q = String(e.target.value || '').trim();
    clearTimeout(state.suggestTimer);
    clearTimeout(state.searchTimer);
    if (!q) {
      resetAll();
      return;
    }
    // Suggestion should be fast and frequent.
    state.suggestTimer = setTimeout(() => {
      renderSuggestions(q);
    }, 120);
    // Heavy network search should not run on every keystroke.
    state.searchTimer = setTimeout(() => {
      if (q === state.lastSearched) return;
      runSearch(q);
    }, looksLikeFullCompanyName(q) ? 300 : 900);
  });
}

async function renderSuggestions(q) {
  await state.apiReady;
  if (state.offlineMode || !API_BASE) {
    if (ALLOW_OFFLINE_DEMO) {
      renderLocalSuggestions(q);
    } else {
      els.suggestions.innerHTML = '';
    }
    return;
  }
  const seq = ++state.suggestSeq;
  if (state.suggestAbort) state.suggestAbort.abort();
  const ctl = new AbortController();
  state.suggestAbort = ctl;
  try {
    let res = await fetch(apiUrl(`/api/suggest?q=${encodeURIComponent(q)}`), { signal: ctl.signal });
    // Browser-side tunnel/network policy can fail by domain; rotate backend once.
    if (!res.ok && switchToNextApiBase()) {
      res = await fetch(apiUrl(`/api/suggest?q=${encodeURIComponent(q)}`), { signal: ctl.signal });
    }
    if (!res.ok) throw new Error('suggest failed');
    if (seq !== state.suggestSeq) return;
    const data = await res.json();
    const rows = Array.isArray(data.items) ? data.items : [];
    if (!rows.length) {
      // Don't render a hard error here; enrich may still resolve a valid company context.
      els.suggestions.innerHTML = '';
      return;
    }

    els.suggestions.innerHTML = rows
      .map(
        (x) => `
        <button class="suggestion" data-name="${escapeAttr(x.displayName || x.name)}">
          ${escapeHtml(x.displayName || x.name)}
        </button>
      `,
      )
      .join('');

    [...els.suggestions.querySelectorAll('.suggestion')].forEach((btn) => {
      btn.addEventListener('click', () => {
        const name = btn.dataset.name || '';
        els.input.value = name;
        els.suggestions.innerHTML = '';
        clearTimeout(state.searchTimer);
        runSearch(name);
      });
    });
  } catch (e) {
    if (e?.name === 'AbortError') return;
    if (switchToNextApiBase()) {
      renderSuggestions(q);
      return;
    }
    state.offlineMode = true;
    setNetworkStatus('offline');
    if (ALLOW_OFFLINE_DEMO) renderLocalSuggestions(q);
    else els.suggestions.innerHTML = '';
  }
}

async function runSearch(q, retry = 0) {
  await state.apiReady;
  if (state.offlineMode || !API_BASE) {
    if (ALLOW_OFFLINE_DEMO) runLocalSearch(q);
    else renderOfflineUnavailable(q);
    setNetworkStatus('offline');
    return;
  }
  setNetworkStatus('querying');
  state.lastSearched = q;
  const current = ++state.seq;
  if (state.stream) {
    state.stream.close();
    state.stream = null;
  }
  stopEtaTimer();
  setLoadingState();
  const es = new EventSource(apiUrl(`/api/enrich-stream?q=${encodeURIComponent(q)}`));
  state.stream = es;
  let company = null;
  const progress = {
    competitors: { etaSec: 8, done: false },
    top5: { etaSec: 6, done: false },
    suppliers: { etaSec: 7, done: false },
    customers: { etaSec: 7, done: false },
  };
  renderProgress(progress);
  startEtaTimer(progress);

  es.addEventListener('company', (ev) => {
    if (current !== state.seq) return;
    const payload = parseEventData(ev);
    if (!payload?.company) {
      els.overview.classList.add('hidden');
      els.overview.innerHTML = '';
      resetPanelsOnly();
      return;
    }
    // Once company is resolved, hide any temporary suggestion/empty hints.
    if (els.suggestions) els.suggestions.innerHTML = '';
    company = { ...(company || {}), ...payload.company };
    renderOverview(company);
  });

  es.addEventListener('company_update', (ev) => {
    if (current !== state.seq) return;
    const payload = parseEventData(ev);
    if (!payload?.company) return;
    company = { ...(company || {}), ...payload.company };
    renderOverview(company);
  });

  es.addEventListener('competitors', (ev) => {
    if (current !== state.seq) return;
    const payload = parseEventData(ev);
    progress.competitors.done = true;
    renderCompetitors(payload?.rows || []);
  });

  es.addEventListener('top5', (ev) => {
    if (current !== state.seq) return;
    const payload = parseEventData(ev);
    progress.top5.done = true;
    const c = company || { code: '', industryName: payload?.industryName || '', fiscalYear: '' };
    renderTop5(payload?.rows || [], c);
  });

  es.addEventListener('suppliers', (ev) => {
    if (current !== state.seq) return;
    const payload = parseEventData(ev);
    progress.suppliers.done = true;
    renderSuppliers(payload?.rows || []);
  });

  es.addEventListener('customers', (ev) => {
    if (current !== state.seq) return;
    const payload = parseEventData(ev);
    progress.customers.done = true;
    renderCustomers(payload?.rows || []);
  });

  es.addEventListener('eta', (ev) => {
    if (current !== state.seq) return;
    const p = parseEventData(ev) || {};
    if (Number.isFinite(p.competitorsMs)) progress.competitors.etaSec = Math.max(1, Math.ceil(p.competitorsMs / 1000));
    if (Number.isFinite(p.top5Ms)) progress.top5.etaSec = Math.max(1, Math.ceil(p.top5Ms / 1000));
    if (Number.isFinite(p.suppliersMs)) progress.suppliers.etaSec = Math.max(1, Math.ceil(p.suppliersMs / 1000));
    if (Number.isFinite(p.customersMs)) progress.customers.etaSec = Math.max(1, Math.ceil(p.customersMs / 1000));
    renderProgress(progress);
  });

  es.addEventListener('done', () => {
    if (current !== state.seq) return;
    stopEtaTimer();
    es.close();
    if (state.stream === es) state.stream = null;
    setNetworkStatus('online');
  });

  es.addEventListener('error', () => {
    if (current !== state.seq) return;
    if (!company) {
      if (retry < Math.max(1, state.apiCandidates.length - 1) && switchToNextApiBase()) {
        es.close();
        if (state.stream === es) state.stream = null;
        runSearch(q, retry + 1);
        return;
      }
      els.overview.classList.add('hidden');
      els.overview.innerHTML = '';
      resetPanelsOnly('联网接口暂不可用');
      setNetworkStatus('offline');
    }
    stopEtaTimer();
    es.close();
    if (state.stream === es) state.stream = null;
  });
}

function normalizeBase(x) {
  return String(x || '').trim().replace(/\/+$/, '');
}

function getApiCandidates() {
  const bySameOrigin = window.location.hostname.endsWith('github.io') ? '' : normalizeBase(window.location.origin || '');
  const byQuery = normalizeBase(new URLSearchParams(window.location.search).get('api') || '');
  const byStorage = normalizeBase(localStorage.getItem('APP_API_BASE') || '');
  const byConfigSingle = normalizeBase(window.APP_API_BASE || '');
  const byConfigList = Array.isArray(window.APP_API_BASES) ? window.APP_API_BASES.map(normalizeBase) : [];
  const out = [];
  [byQuery, byStorage, bySameOrigin, byConfigSingle, ...byConfigList].forEach((x) => {
    if (x && !out.includes(x)) out.push(x);
  });
  if (byQuery) localStorage.setItem('APP_API_BASE', byQuery);
  return out;
}

function switchToNextApiBase() {
  if (!state.apiCandidates.length) return false;
  const next = state.apiIndex + 1;
  if (next >= state.apiCandidates.length) return false;
  state.apiIndex = next;
  API_BASE = state.apiCandidates[state.apiIndex];
  localStorage.setItem('APP_API_BASE', API_BASE);
  return true;
}

function timeoutSignal(ms) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), ms);
  return { signal: ctl.signal, clear: () => clearTimeout(t) };
}

async function checkApiHealth(base) {
  const t = timeoutSignal(2800);
  try {
    const res = await fetch(`${base}/api/health`, { signal: t.signal });
    if (!res.ok) return false;
    const data = await res.json().catch(() => ({}));
    return !!data?.ok;
  } catch {
    return false;
  } finally {
    t.clear();
  }
}

async function ensureApiBase() {
  const candidates = getApiCandidates();
  state.apiCandidates = candidates;
  state.apiIndex = 0;
  if (!candidates.length) {
    API_BASE = '';
    state.offlineMode = true;
    setNetworkStatus('offline');
    return;
  }
  const checks = await Promise.all(candidates.map((base) => checkApiHealth(base)));
  const firstOk = checks.findIndex(Boolean);
  if (firstOk >= 0) {
    state.apiIndex = firstOk;
    API_BASE = candidates[firstOk];
    state.offlineMode = false;
    localStorage.setItem('APP_API_BASE', API_BASE);
    setNetworkStatus('online');
    return;
  }
  API_BASE = '';
  state.offlineMode = true;
  setNetworkStatus('offline');
  if (window.location.hostname.endsWith('github.io')) {
    els.suggestions.innerHTML = "<p class='hint'>当前未连接后端，暂不可联网查询。请使用参数：<code>?api=https://你的后端域名</code></p>";
  }
}

function setLoadingState() {
  const loading = "<p class='hint'>正在联网获取数据，请稍候...</p>";
  els.competitorBody.innerHTML = loading;
  els.top5Body.innerHTML = loading;
  els.supplierBody.innerHTML = loading;
  els.customerBody.innerHTML = loading;
}

function renderOverview(company) {
  const revenueText = Number.isFinite(company.revenue) && company.revenue > 0 ? formatMoney(company.revenue) : '未获取';
  const yearText = company.fiscalYear || '未获取';
  const industryL1 = company.industryLevel1 || '未识别';
  const industryL2 = company.industryLevel2 || company.industryName || '未识别';
  const financing = company.financing || { roundsCount: null, events: [] };
  const showFinancing = company.isListed === false;
  const financingHtml = showFinancing
    ? `<li><span>融资轮次</span>${escapeHtml(
        Number.isFinite(financing.roundsCount) ? `${financing.roundsCount} 轮` : financing.events?.length ? `已识别 ${financing.events.length} 条` : '未获取',
      )}</li>
      <li>
        <span>融资信息</span>
        ${
          financing.events?.length
            ? `<div class="financing-list">${financing.events
                .slice(0, 4)
                .map(
                  (x) =>
                    `<div class="financing-item">${escapeHtml(x.date || '日期未知')} ${escapeHtml(x.round || '轮次未知')} ${
                      x.amount ? `· ${escapeHtml(x.amount)}` : ''
                    } ${x.investors?.length ? `· 资方：${escapeHtml(x.investors.join('、'))}` : ''}</div>`,
                )
                .join('')}</div>`
            : '未获取'
        }
      </li>`
    : '';

  els.overview.classList.remove('hidden');
  els.overview.innerHTML = `
    <h2>${escapeHtml(company.name || '')}</h2>
    <ul class="kv">
      <li><span>证券代码</span>${escapeHtml(company.code || '-')}</li>
      <li><span>一级行业</span>${escapeHtml(industryL1)}</li>
      <li><span>二级行业</span>${escapeHtml(industryL2)}</li>
      <li><span>官网</span>${company.website ? `<a class="link" href="${escapeAttr(company.website)}" target="_blank" rel="noreferrer">打开官网</a>` : '未识别'}</li>
      <li><span>财年</span>${escapeHtml(String(yearText))}</li>
      <li><span>营业收入</span>${escapeHtml(revenueText)}</li>
      ${financingHtml}
    </ul>
  `;
}

function renderCompetitors(rows) {
  if (!rows.length) {
    els.competitorBody.innerHTML = "<p class='empty'>暂无可用数据</p>";
    return;
  }
  els.competitorBody.innerHTML = `<ul class='list'>${rows
    .slice(0, 20)
    .map(
      (x) =>
        `<li><strong>${escapeHtml(x.name || '-')}</strong><br/><small>${escapeHtml(x.reason || '同业竞争')}${
          x.reportCount ? ` · 研报数：${escapeHtml(String(x.reportCount))}` : ''
        }${x.brokerCount ? ` · 券商数：${escapeHtml(String(x.brokerCount))}` : ''}${x.confidence ? ` · 置信度：${Math.round((x.confidence || 0) * 100)}%` : ''}${
          x.sample ? ` · 证据：${escapeHtml(x.sample)}` : ''
        }</small></li>`,
    )
    .join('')}</ul>`;
}

function renderTop5(rows, company) {
  const industryL1 = company.industryLevel1 || '未识别';
  const industryL2 = company.industryLevel2 || company.industryName || '未识别';
  if (!rows.length) {
    els.top5Body.innerHTML = `<p class='hint'>一级行业：${escapeHtml(industryL1)} · 二级行业：${escapeHtml(industryL2)}</p><p class='empty'>未获取到行业营收 Top5</p>`;
    return;
  }
  const selfInTop = rows.some((x) => String(x.code) === String(company.code));
  const year = rows[0]?.fiscalYear || company.fiscalYear || '未获取';

  els.top5Body.innerHTML = `
    <p class="hint">一级行业：${escapeHtml(industryL1)} · 二级行业：${escapeHtml(industryL2)} · 财年：${escapeHtml(String(year))}</p>
    <ul class="list">
      ${rows
        .map(
          (x, i) =>
            `<li><span class="rank">#${i + 1}</span>${escapeHtml(x.name || '-')}（${escapeHtml(x.code || '-')}）<br/><small>${escapeHtml(
              Number.isFinite(x.revenue) && x.revenue > 0 ? formatMoney(x.revenue) : '营收未获取',
            )}</small></li>`,
        )
        .join('')}
    </ul>
    ${selfInTop ? '' : "<p class='empty'>目标企业不在行业 Top5 内。</p>"}
  `;
}

function renderSuppliers(rows) {
  if (!rows.length) {
    els.supplierBody.innerHTML = "<p class='empty'>暂无证据链供应商数据</p>";
    return;
  }
  els.supplierBody.innerHTML = renderTieredRelationRows(rows, '上游供货候选');
}

function renderCustomers(rows) {
  if (!rows.length) {
    els.customerBody.innerHTML = "<p class='empty'>暂无证据链客户数据</p>";
    return;
  }
  els.customerBody.innerHTML = renderTieredRelationRows(rows, '下游采购方候选');
}

function renderTieredRelationRows(rows, fallbackReason) {
  const all = Array.isArray(rows) ? rows.slice(0, 30) : [];
  const strong = all.filter((x) => (x.sourceTier || '').toLowerCase() !== 'tier3');
  const weak = all.filter((x) => (x.sourceTier || '').toLowerCase() === 'tier3');
  const renderList = (arr) =>
    `<ul class='list'>${arr
      .map(
        (x) =>
          `<li><strong>${escapeHtml(x.name || '-')}</strong><br/><small>${escapeHtml(
            x.reason || fallbackReason,
          )}${x.amount ? ` · 金额：${escapeHtml(formatMoney(x.amount))}` : ''}${x.ratio ? ` · 占比：${escapeHtml(x.ratio)}` : ''}${
            Number.isFinite(x.evidenceCount) ? ` · 证据数：${escapeHtml(String(x.evidenceCount))}` : ''
          } · 置信度：${Math.round((x.confidence || 0) * 100)}%${
            x.source ? ` · <a class="link" href="${escapeAttr(x.source)}" target="_blank" rel="noreferrer">来源</a>` : ''
          }</small></li>`,
      )
      .join('')}</ul>`;
  let html = '';
  if (strong.length) {
    html += `<p class='hint'>主证据（Tier1/Tier2）</p>${renderList(strong)}`;
  } else {
    html += "<p class='empty'>暂无 Tier1/Tier2 证据</p>";
  }
  if (weak.length) {
    html += `<details><summary class='hint'>弱证据（Tier3）${escapeHtml(String(weak.length))} 条，点击展开</summary>${renderList(weak)}</details>`;
  }
  return html;
}

function resetAll() {
  if (state.stream) {
    state.stream.close();
    state.stream = null;
  }
  if (state.suggestAbort) {
    state.suggestAbort.abort();
    state.suggestAbort = null;
  }
  clearTimeout(state.suggestTimer);
  clearTimeout(state.searchTimer);
  state.lastSearched = '';
  stopEtaTimer();
  els.suggestions.innerHTML = '';
  els.overview.classList.add('hidden');
  resetPanelsOnly();
}

function resetPanelsOnly(msg = '请输入企业名称后展示') {
  const html = `<p class='hint'>${escapeHtml(msg)}</p>`;
  els.competitorBody.innerHTML = html;
  els.top5Body.innerHTML = html;
  els.supplierBody.innerHTML = html;
  els.customerBody.innerHTML = html;
}

function formatMoney(value) {
  return new Intl.NumberFormat('zh-CN', {
    style: 'currency',
    currency: 'CNY',
    maximumFractionDigits: 0,
  }).format(Number(value || 0));
}

function escapeHtml(s) {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function escapeAttr(s) {
  return escapeHtml(String(s));
}

function parseEventData(ev) {
  try {
    return JSON.parse(ev.data || '{}');
  } catch {
    return {};
  }
}

function normalizeStockCode(raw = '') {
  return String(raw || '')
    .replace(/\.(SH|SZ)$/i, '')
    .replace(/\..*$/, '');
}

function startEtaTimer(progress) {
  stopEtaTimer();
  state.etaTimer = setInterval(() => {
    let changed = false;
    for (const k of Object.keys(progress)) {
      const s = progress[k];
      if (!s.done && s.etaSec > 1) {
        s.etaSec -= 1;
        changed = true;
      }
    }
    if (changed) renderProgress(progress);
  }, 1000);
}

function stopEtaTimer() {
  if (state.etaTimer) {
    clearInterval(state.etaTimer);
    state.etaTimer = null;
  }
}

function renderProgress(progress) {
  if (!progress.competitors.done) els.competitorBody.innerHTML = `<p class='hint'>预计还需 ${progress.competitors.etaSec} 秒...</p>`;
  if (!progress.top5.done) els.top5Body.innerHTML = `<p class='hint'>预计还需 ${progress.top5.etaSec} 秒...</p>`;
  if (!progress.suppliers.done) els.supplierBody.innerHTML = `<p class='hint'>预计还需 ${progress.suppliers.etaSec} 秒...</p>`;
  if (!progress.customers.done) els.customerBody.innerHTML = `<p class='hint'>预计还需 ${progress.customers.etaSec} 秒...</p>`;
}

function looksLikeFullCompanyName(q = '') {
  const s = String(q || '').trim();
  if (!s) return false;
  return /(有限责任公司|股份有限公司|集团有限公司|集团股份有限公司|有限公司|交易所|银行|证券|期货|基金)/.test(s);
}

function normalizeCompanyToken(s = '') {
  return String(s || '')
    .trim()
    .toLowerCase()
    .replace(/[()（）\s\-_.]/g, '')
    .replace(/(有限责任公司|股份有限公司|集团有限公司|集团股份有限公司|有限公司|集团|股份|公司)$/g, '');
}

function getLocalCompanies() {
  if (state.localCompanies.length) return state.localCompanies;
  const rows = Array.isArray(window.COMPANIES) ? window.COMPANIES : [];
  state.localCompanies = rows.map((x) => {
    const aliases = Array.isArray(x.aliases) ? x.aliases.filter(Boolean) : [];
    return {
      ...x,
      name: x.fullName || x.shortName || '',
      shortName: x.shortName || '',
      aliasTokens: [x.fullName || '', x.shortName || '', ...aliases].map(normalizeCompanyToken).filter(Boolean),
    };
  });
  return state.localCompanies;
}

function rankLocalCompanies(q) {
  const token = normalizeCompanyToken(q);
  if (!token) return [];
  const rows = getLocalCompanies();
  const scored = [];
  for (const c of rows) {
    let score = 0;
    const full = normalizeCompanyToken(c.fullName || c.name || '');
    const short = normalizeCompanyToken(c.shortName || '');
    if (full === token) score += 120;
    if (short === token) score += 110;
    if (full.startsWith(token)) score += 80;
    if (short.startsWith(token)) score += 70;
    if (full.includes(token)) score += 50;
    if (short.includes(token)) score += 40;
    for (const a of c.aliasTokens || []) {
      if (a === token) score += 55;
      else if (a.includes(token) || token.includes(a)) score += 22;
    }
    if (score > 0) scored.push({ company: c, score });
  }
  scored.sort((a, b) => b.score - a.score || Number(b.company.revenue || 0) - Number(a.company.revenue || 0));
  return scored.map((x) => x.company);
}

function renderLocalSuggestions(q) {
  const rows = rankLocalCompanies(q).slice(0, 8);
  if (!rows.length) {
    els.suggestions.innerHTML = '';
    return;
  }
  els.suggestions.innerHTML = rows
    .map(
      (x) => `
      <button class="suggestion" data-name="${escapeAttr(x.fullName || x.name || '')}">
        ${escapeHtml(x.fullName || x.name || '-')}
      </button>
    `,
    )
    .join('');
  [...els.suggestions.querySelectorAll('.suggestion')].forEach((btn) => {
    btn.addEventListener('click', () => {
      const name = btn.dataset.name || '';
      els.input.value = name;
      els.suggestions.innerHTML = '';
      clearTimeout(state.searchTimer);
      runLocalSearch(name);
    });
  });
}

function toRelationRows(rows = []) {
  return (Array.isArray(rows) ? rows : []).map((x) => ({
    name: x.name,
    reason: x.reason || '',
    amount: x.amount,
    ratio: Number.isFinite(x.ratio) ? `${x.ratio}%` : '',
    confidence: Number.isFinite(x.confidence) ? x.confidence : 0.7,
    sourceTier: 'tier2',
    evidenceCount: Number.isFinite(x.evidenceCount) ? x.evidenceCount : undefined,
    source: x.source || '',
  }));
}

function runLocalSearch(q) {
  state.lastSearched = q;
  stopEtaTimer();
  const rows = rankLocalCompanies(q);
  const c = rows[0];
  if (!c) {
    els.overview.classList.remove('hidden');
    els.overview.innerHTML = `
      <h2>${escapeHtml(q)}</h2>
      <ul class="kv">
        <li><span>证券代码</span>-</li>
        <li><span>一级行业</span>未识别</li>
        <li><span>二级行业</span>未识别</li>
        <li><span>官网</span>未识别</li>
        <li><span>财年</span>未获取</li>
        <li><span>营业收入</span>未获取</li>
      </ul>
    `;
    els.competitorBody.innerHTML = "<p class='empty'>离线模式暂无该企业竞争对手数据</p>";
    els.top5Body.innerHTML = "<p class='empty'>离线模式暂无该行业 Top5 数据</p>";
    els.supplierBody.innerHTML = "<p class='empty'>离线模式暂无该企业供应商数据</p>";
    els.customerBody.innerHTML = "<p class='empty'>离线模式暂无该企业客户数据</p>";
    return;
  }
  const company = {
    name: c.fullName || c.name || '',
    code: normalizeStockCode(c.stockCode || '') || '-',
    industryLevel1: c.industryLevel1 || inferLevel1(c.industryName || c.industryLevel2 || ''),
    industryLevel2: c.industryLevel2 || c.industryName || '未识别',
    industryName: c.industryName || c.industryLevel2 || '未识别',
    website: c.website || '',
    fiscalYear: c.fiscalYear || '',
    revenue: Number(c.revenue || 0),
    isListed: !!c.stockCode,
  };
  renderOverview(company);
  const rel = c.relations || {};
  const localTop = getLocalIndustryTop(company.industryLevel2 || company.industryName || '', c);
  const localPeers = toRelationRows(rel.competitors || []).map((x) => ({ ...x, reportCount: 0, brokerCount: 0 }));
  const fallbackPeers = !localPeers.length ? localTop.filter((x) => x.code !== company.code).slice(0, 5).map((x) => ({ name: x.name, reason: '同属本地行业样本', confidence: 0.55 })) : localPeers;
  renderCompetitors(fallbackPeers);
  renderTop5(localTop, company);
  renderSuppliers(toRelationRows(rel.suppliers || []));
  renderCustomers(toRelationRows(rel.customers || []));
}

function inferLevel1(industryL2 = '') {
  const s = String(industryL2 || '');
  if (/(半导体|芯片|电子|消费电子|通信|软件|信息|互联网|云|大数据|人工智能|网络安全)/.test(s)) return '电子信息';
  if (/(汽车|座舱|智驾|网联)/.test(s)) return '汽车';
  if (/(银行|证券|保险|基金|期货|信托)/.test(s)) return '金融';
  if (/(制造|自动化|设备|机械|工业|装备)/.test(s)) return '工业';
  if (/(化工|纤维|材料|金属|矿|钢|有色)/.test(s)) return '材料';
  if (/(电力|电网|能源|燃气|水务)/.test(s)) return '能源电力';
  return '综合';
}

function getLocalIndustryTop(industryL2, selfCompany) {
  const target = String(industryL2 || '').trim();
  const rows = getLocalCompanies().filter((x) => String(x.industryLevel2 || x.industryName || '').trim() === target);
  const all = rows.length ? rows : getLocalCompanies().filter((x) => String(x.industryName || '').trim() === String(selfCompany.industryName || '').trim());
  const sorted = all
    .filter((x) => Number.isFinite(Number(x.revenue)) && Number(x.revenue) > 0)
    .sort((a, b) => Number(b.revenue || 0) - Number(a.revenue || 0));
  const out = sorted.slice(0, 5).map((x) => ({
    name: x.shortName || x.fullName || x.name || '-',
    code: normalizeStockCode(x.stockCode || '') || '-',
    revenue: Number(x.revenue || 0),
    fiscalYear: x.fiscalYear || selfCompany.fiscalYear || '',
  }));
  const selfCode = normalizeStockCode(selfCompany.stockCode || '');
  if (selfCode && !out.some((x) => String(x.code) === selfCode) && Number(selfCompany.revenue || 0) > 0) {
    out.push({
      name: selfCompany.shortName || selfCompany.fullName || '-',
      code: selfCode,
      revenue: Number(selfCompany.revenue || 0),
      fiscalYear: selfCompany.fiscalYear || '',
    });
    out.sort((a, b) => Number(b.revenue || 0) - Number(a.revenue || 0));
  }
  return out.slice(0, 5);
}

function renderOfflineUnavailable(q) {
  const name = String(q || '').trim() || '未识别企业';
  els.overview.classList.remove('hidden');
  els.overview.innerHTML = `
    <h2>${escapeHtml(name)}</h2>
    <ul class="kv">
      <li><span>证券代码</span>-</li>
      <li><span>一级行业</span>未联网</li>
      <li><span>二级行业</span>未联网</li>
      <li><span>官网</span>未联网</li>
      <li><span>财年</span>未联网</li>
      <li><span>营业收入</span>未联网</li>
    </ul>
  `;
  els.competitorBody.innerHTML = "<p class='empty'>未连接后端，无法查询竞争对手</p>";
  els.top5Body.innerHTML = "<p class='empty'>未连接后端，无法查询行业 Top5</p>";
  els.supplierBody.innerHTML = "<p class='empty'>未连接后端，无法查询供应商</p>";
  els.customerBody.innerHTML = "<p class='empty'>未连接后端，无法查询客户</p>";
}

function setNetworkStatus(mode) {
  if (!els.networkStatus) return;
  els.networkStatus.classList.remove('net-online', 'net-offline', 'net-checking');
  if (mode === 'online') {
    els.networkStatus.classList.add('net-online');
    els.networkStatus.textContent = '已联网';
    return;
  }
  if (mode === 'querying' || mode === 'checking') {
    els.networkStatus.classList.add('net-checking');
    els.networkStatus.textContent = '联网中';
    return;
  }
  els.networkStatus.classList.add('net-offline');
  els.networkStatus.textContent = '未联网';
}
