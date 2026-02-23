const els = {
  input: document.querySelector('#companySearch'),
  suggestions: document.querySelector('#suggestions'),
  overview: document.querySelector('#companyOverview'),
  competitorBody: document.querySelector('#competitorBody'),
  top5Body: document.querySelector('#top5Body'),
  supplierBody: document.querySelector('#supplierBody'),
  customerBody: document.querySelector('#customerBody'),
};

const API_BASE = (window.APP_API_BASE || '').replace(/\/+$/, '');
const apiUrl = (path) => `${API_BASE}${path}`;

const state = {
  suggestTimer: null,
  searchTimer: null,
  seq: 0,
  suggestSeq: 0,
  stream: null,
  etaTimer: null,
  lastSearched: '',
  suggestAbort: null,
};

init();

function init() {
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
  const seq = ++state.suggestSeq;
  if (state.suggestAbort) state.suggestAbort.abort();
  const ctl = new AbortController();
  state.suggestAbort = ctl;
  try {
    const res = await fetch(apiUrl(`/api/suggest?q=${encodeURIComponent(q)}`), { signal: ctl.signal });
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
    els.suggestions.innerHTML = "<p class='empty'>联网建议接口不可用，请稍后再试。</p>";
  }
}

async function runSearch(q) {
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
  });

  es.addEventListener('error', () => {
    if (current !== state.seq) return;
    if (!company) {
      els.overview.classList.add('hidden');
      els.overview.innerHTML = '';
      resetPanelsOnly('联网接口暂不可用');
    }
    stopEtaTimer();
    es.close();
    if (state.stream === es) state.stream = null;
  });
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
