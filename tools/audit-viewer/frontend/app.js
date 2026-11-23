function createTradeRectPrimitive(trades) {
  const primitive = {
    _trades: trades,
    _chart: null,
    _series: null,
    _requestUpdate: null,
    attached(params) {
      this._chart = params.chart;
      this._series = params.series;
      this._requestUpdate = params.requestUpdate || null;
    },
    detached() {
      this._chart = null;
      this._series = null;
      this._requestUpdate = null;
    },
    updateAllViews() {},
    paneViews() {
      const self = this;
      return [{
        renderer() {
          return {
            draw(target) {
              if (!self._chart || !self._series) return;
              target.useMediaCoordinateSpace((scope) => {
                const ctx = scope.context;
                const chart = self._chart;
                const series = self._series;
                const mediaWidth = scope.mediaSize ? scope.mediaSize.width : 800;
                const selected = state.selectedTrade || null;
                for (const t of self._trades) {
                  const entryTime = t.entry_ts;
                  const exitTime = (t.status === 'closed' && t.exit_ts) ? t.exit_ts : (state.candles.length ? state.candles[state.candles.length - 1].ts : entryTime);
                  const x1 = chart.timeScale().timeToCoordinate(entryTime);
                  const x2 = chart.timeScale().timeToCoordinate(exitTime);
                  if (x1 === null || x2 === null) continue;
                  const left = Math.min(x1, x2);
                  const right = Math.max(x1, x2);
                  if (right < 0 || left > mediaWidth) continue;
                  const yEntry = series.priceToCoordinate(t.entry);
                  const yStop = series.priceToCoordinate(t.stop);
                  const yTarget = series.priceToCoordinate(t.target);
                  if (yEntry === null || yStop === null || yTarget === null) continue;

                  // stop rectangle (red): entry -> stop
                  const stopTop = Math.min(yEntry, yStop);
                  const stopBottom = Math.max(yEntry, yStop);
                  ctx.fillStyle = 'rgba(239, 83, 80, 0.25)';
                  ctx.fillRect(left, stopTop, right - left, stopBottom - stopTop);
                  ctx.strokeStyle = 'rgba(239, 83, 80, 0.6)';
                  ctx.lineWidth = 1;
                  ctx.strokeRect(left, stopTop, right - left, stopBottom - stopTop);

                  // target rectangle (green): entry -> target
                  const tgtTop = Math.min(yEntry, yTarget);
                  const tgtBottom = Math.max(yEntry, yTarget);
                  ctx.fillStyle = 'rgba(38, 166, 154, 0.25)';
                  ctx.fillRect(left, tgtTop, right - left, tgtBottom - tgtTop);
                  ctx.strokeStyle = 'rgba(38, 166, 154, 0.6)';
                  ctx.strokeRect(left, tgtTop, right - left, tgtBottom - tgtTop);

                  // entry line (dashed) constrained to entry..exit
                  const isSel = selected === t;
                  ctx.strokeStyle = isSel ? '#f0b429' : 'rgba(255, 255, 255, 0.5)';
                  ctx.lineWidth = isSel ? 2 : 1;
                  ctx.setLineDash([4, 4]);
                  ctx.beginPath();
                  ctx.moveTo(left, yEntry);
                  ctx.lineTo(right, yEntry);
                  ctx.stroke();
                  ctx.setLineDash([]);

                  // selected trade highlight: gold outline over stop..target span
                  if (isSel) {
                    ctx.strokeStyle = '#f0b429';
                    ctx.lineWidth = 2;
                    const hlTop = Math.min(stopTop, tgtTop);
                    const hlBottom = Math.max(stopBottom, tgtBottom);
                    ctx.strokeRect(left, hlTop, right - left, hlBottom - hlTop);
                    if (t.status === 'closed' && t.exit_price && t.exit_ts) {
                      const xExit = chart.timeScale().timeToCoordinate(t.exit_ts);
                      const yExit = series.priceToCoordinate(t.exit_price);
                      if (xExit !== null && yExit !== null) {
                        ctx.fillStyle = t.pnl_r > 0 ? '#26a69a' : '#ef5350';
                        ctx.beginPath();
                        ctx.arc(xExit, yExit, 4, 0, Math.PI * 2);
                        ctx.fill();
                      }
                    }
                  }

                  // markers: entry arrow + exit square
                  const dirUp = t.direction > 0;
                  ctx.fillStyle = dirUp ? '#26a69a' : '#ef5350';
                  ctx.beginPath();
                  if (dirUp) {
                    ctx.moveTo(x1, yEntry + 6);
                    ctx.lineTo(x1 - 4, yEntry + 12);
                    ctx.lineTo(x1 + 4, yEntry + 12);
                    ctx.closePath();
                  } else {
                    ctx.moveTo(x1, yEntry - 6);
                    ctx.lineTo(x1 - 4, yEntry - 12);
                    ctx.lineTo(x1 + 4, yEntry - 12);
                    ctx.closePath();
                  }
                  ctx.fill();
                  if (t.status === 'closed' && t.exit_ts) {
                    const xExit2 = chart.timeScale().timeToCoordinate(t.exit_ts);
                    const yExit2 = series.priceToCoordinate(t.exit_price || t.entry);
                    if (xExit2 !== null && yExit2 !== null) {
                      ctx.fillStyle = t.pnl_r > 0 ? '#26a69a' : '#ef5350';
                      ctx.fillRect(xExit2 - 3, yExit2 - 3, 6, 6);
                    }
                  }
                }
              });
            },
          };
        },
      }];
    },
  };
  return primitive;
}


const state = {
  // audit
  files: [], currentFile: null, meta: null, offset: 0, limit: 200,
  topic: '', type: '', search: '', total: 0, selectedIdx: null,
  // dashboard
  instruments: [], portfolios: [], dashInstr: '', dashTf: '1d', dashPortfolio: '',
  candles: [], trades: [], selectedTrade: null,
  chart: null, candleSeries: null, volumeSeries: null,
  timeline: null, dashAuditFile: '',
  emaSeries: null, atrSeries: null, supSeries: null, resSeries: null,
  showEma: true, showAtr: false, showSupport: true, showSwings: true, showSetups: true,
  // time-machine
  allCandles: [], allTimeline: null, allTrades: [], allChartTrades: [],
  scrubIdx: -1, playing: false, playTimer: null, playSpeed: 125,
  timeMachineActive: false,
};

const $ = (id) => document.getElementById(id);

function setStatus(msg) { const el = $('status'); if (el) el.textContent = msg; }
function fmtTs(ts) {
  if (!ts) return '';
  const d = new Date(ts * 1000);
  return d.toISOString().replace('T', ' ').slice(0, 19);
}
function fmtSize(b) {
  if (b < 1024) return b + ' B';
  if (b < 1024 * 1024) return (b / 1024).toFixed(1) + ' KB';
  return (b / 1024 / 1024).toFixed(2) + ' MB';
}
function msgTs(m) {
  const ht = (m.header && m.header.timestamp) ? m.header.timestamp : 0;
  if (ht) return ht;
  const p = m.payload;
  const inner = (p && typeof p === 'object') ? (Object.keys(p).length === 1 ? p[Object.keys(p)[0]] : p) : p;
  if (inner && inner.candle && inner.candle.timestamp) {
    const v = inner.candle.timestamp;
    if (typeof v === 'number') return v;
    const ms = Date.parse(v);
    if (!isNaN(ms)) return Math.floor(ms / 1000);
  }
  if (inner && typeof inner.timestamp === 'number') return inner.timestamp;
  return 0;
}
function eventType(payload) {
  if (!payload || typeof payload !== 'object') return '?';
  if (payload.type) return payload.type;
  const keys = Object.keys(payload);
  if (keys.length === 1) return keys[0];
  return '?';
}
function eventInner(payload) {
  const t = eventType(payload);
  return (payload && payload[t]) ? payload[t] : payload;
}
function summarize(payload) {
  const t = eventType(payload);
  const d = eventInner(payload);
  switch (t) {
    case 'CandleClosed': return `${d.symbol} ${d.timeframe} O=${d.candle.open} H=${d.candle.high} L=${d.candle.low} C=${d.candle.close}`;
    case 'IndicatorUpdated': return `${d.symbol} ${d.timeframe} ${d.name}=${d.value}`;
    case 'SwingPivotEvent': return `${d.symbol} ${d.timeframe} ${d.kind === 1 ? 'high' : 'low'} pivot ${d.price}`;
    case 'SetupSignal': return `${d.symbol} ${d.timeframe} dir=${d.direction} entry=${d.entry} stop=${d.stop} target=${d.target}`;
    case 'TradeSignal': return `${d.symbol} ${d.timeframe} dir=${d.direction} entry=${d.entry} size=${d.size}`;
    case 'TradeResult': return `pnl_r=${d.pnl_r} reason=${d.reason}`;
    case 'ContextState': return `role=${d.role} ${d.instrument} ${d.timeframe} latest=${d.version}`;
    default: try { return JSON.stringify(d).slice(0, 120); } catch (e) { return ''; }
  }
}

// --- Tab switching ---
function showTab(tab) {
  $('auditView').classList.toggle('hidden', tab !== 'audit');
  $('dashView').classList.toggle('hidden', tab !== 'dash');
  $('auditSidebar').classList.toggle('hidden', tab !== 'audit');
  $('dashSidebar').classList.toggle('hidden', tab !== 'dash');
  document.querySelectorAll('.tab').forEach(b => b.classList.toggle('active', b.id === (tab === 'audit' ? 'tabAudit' : 'tabDash')));
  if (tab === 'dash' && state.instruments.length === 0) loadDashData();
}
$('tabAudit').onclick = () => showTab('audit');
$('tabDash').onclick = () => showTab('dash');

// --- Audit view (same as before) ---
async function loadFiles() {
  const res = await fetch('/api/files');
  state.files = await res.json();
  const ul = $('fileList');
  ul.innerHTML = '';
  for (const f of state.files) {
    const li = document.createElement('li');
    li.innerHTML = `<span class="name">${f.name}</span><span class="size">${fmtSize(f.size)}</span>`;
    li.onclick = () => selectFile(f.name);
    ul.appendChild(li);
  }
  setStatus(`${state.files.length} audit files`);
}
async function selectFile(name) {
  state.currentFile = name; state.offset = 0; state.selectedIdx = null;
  document.querySelectorAll('#fileList li').forEach(li => li.classList.toggle('active', li.querySelector('.name').textContent === name));
  const res = await fetch(`/api/files/${encodeURIComponent(name)}/meta`);
  state.meta = await res.json();
  renderMeta(); populateFilters();
  $('filters').classList.remove('hidden'); $('tableWrap').classList.remove('hidden'); $('pager').classList.remove('hidden');
  await loadMessages();
}
function renderMeta() {
  const m = state.meta;
  const typeStr = Object.entries(m.types).map(([k, v]) => `${k}:${v}`).join('  ');
  $('fileMeta').innerHTML = `<b>${m.name}</b> — ${m.count} messages ${m.first_ts ? ' | ' + fmtTs(m.first_ts) + ' → ' + fmtTs(m.last_ts) : ''}<br/>topics: ${m.topics.join(', ')}<br/>types: ${typeStr}`;
  const tf = $('topicFilter'); tf.innerHTML = '<option value="">all topics</option>';
  for (const t of m.topics) { const o = document.createElement('option'); o.value = t; o.textContent = t; tf.appendChild(o); }
  const tyf = $('typeFilter'); tyf.innerHTML = '<option value="">all types</option>';
  for (const t of Object.keys(m.types).sort()) { const o = document.createElement('option'); o.value = t; o.textContent = t; tyf.appendChild(o); }
}
function populateFilters() {
  $('topicFilter').value = state.topic; $('typeFilter').value = state.type;
  $('searchBox').value = state.search; $('pageSize').value = state.limit;
}
async function loadMessages() {
  const params = new URLSearchParams();
  params.set('offset', state.offset); params.set('limit', state.limit);
  if (state.topic) params.set('topic', state.topic);
  if (state.search) params.set('q', state.search);
  const res = await fetch(`/api/files/${encodeURIComponent(state.currentFile)}/messages?${params}`);
  const data = await res.json();
  state.total = data.total;
  renderTable(data.messages); renderPager();
}
function renderTable(msgs) {
  const tbody = $('msgTable').querySelector('tbody');
  tbody.innerHTML = '';
  for (const m of msgs) {
    const t = eventType(m.payload);
    if (state.type && t !== state.type) continue;
    const tr = document.createElement('tr');
    tr.dataset.idx = m.index;
    if (state.selectedIdx === m.index) tr.classList.add('selected');
    tr.innerHTML = `<td>${m.index}</td><td>${fmtTs(msgTs(m))}</td><td>${m.header.topic}</td><td>${t}</td><td>${summarize(m.payload)}</td>`;
    tr.onclick = () => openDetail(m.index);
    tbody.appendChild(tr);
  }
}
function renderPager() {
  const page = Math.floor(state.offset / state.limit) + 1;
  const pages = Math.max(1, Math.ceil(state.total / state.limit));
  $('pageInfo').textContent = `page ${page} / ${pages}  (${state.total} matches)`;
  $('prevBtn').disabled = state.offset === 0;
  $('nextBtn').disabled = state.offset + state.limit >= state.total;
}
async function openDetail(idx) {
  state.selectedIdx = idx;
  document.querySelectorAll('#msgTable tbody tr').forEach(tr => tr.classList.toggle('selected', Number(tr.dataset.idx) === idx));
  const res = await fetch(`/api/files/${encodeURIComponent(state.currentFile)}/messages/${idx}`);
  const m = await res.json();
  $('detailTitle').textContent = `#${m.index}  ${eventType(m.payload)}  @ ${fmtTs(msgTs(m))}`;
  $('detailJson').textContent = JSON.stringify(m, null, 2);
  $('detail').classList.remove('hidden');
}
$('closeDetail').onclick = () => $('detail').classList.add('hidden');
$('prevBtn').onclick = () => { state.offset = Math.max(0, state.offset - state.limit); loadMessages(); };
$('nextBtn').onclick = () => { if (state.offset + state.limit < state.total) { state.offset += state.limit; loadMessages(); } };
$('searchBtn').onclick = () => {
  state.topic = $('topicFilter').value; state.type = $('typeFilter').value;
  state.search = $('searchBox').value.trim(); state.limit = parseInt($('pageSize').value, 10);
  state.offset = 0; loadMessages();
};
$('searchBox').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('searchBtn').click(); });

// --- Dashboard view ---
async function loadDashData() {
  const [instrs, portfolios, files] = await Promise.all([
    fetch('/api/instruments').then(r => r.json()),
    fetch('/api/portfolios').then(r => r.json()),
    fetch('/api/files').then(r => r.json()),
  ]);
  state.instruments = instrs;
  state.portfolios = portfolios;
  state.files = files;
  // populate audit file selector
  const afsel = $('dashAuditFile');
  if (afsel) {
    afsel.innerHTML = '<option value="">(none)</option>';
    for (const f of files) { const o = document.createElement('option'); o.value = f.name; o.textContent = f.name; afsel.appendChild(o); }
  }
  const isel = $('dashInstr');
  isel.innerHTML = '';
  for (const s of instrs) { const o = document.createElement('option'); o.value = s; o.textContent = s; isel.appendChild(o); }
  const psel = $('dashPortfolio');
  psel.innerHTML = '<option value="">(none)</option>';
  for (const p of portfolios) { const o = document.createElement('option'); o.value = p.id; o.textContent = p.id; psel.appendChild(o); }
  // portfolio change handler: auto-link audit file + trade tree
  $('dashPortfolio').addEventListener('change', () => {
    state.dashPortfolio = $('dashPortfolio').value;
    const pid = state.dashPortfolio;
    // auto-select enrichment audit file for this portfolio
    const afsel = $('dashAuditFile');
    if (afsel) {
      const cand = state.files.find(f => f.name === `enrichment_${pid}.dat` || f.name.includes(pid));
      if (cand) afsel.value = cand.name;
    }
    loadTradeTree();
  });
  $('dashLoad').addEventListener('click', () => loadChart());
  $('dashLoadTimeline').addEventListener('click', () => loadTimeline());
  for (const id of ['showEma','showAtr','showSupport','showSwings','showSetups']) {
    const el = $(id); if (el) el.addEventListener('change', () => { state[id] = el.checked; if (state.timeMachineActive) applyTimeSlice(state.scrubIdx); else renderIndicators(); });
  }
  $('tradeStatusFilter').addEventListener('change', () => loadTradeTree());

  // Time-machine controls
  $('tmPlay').addEventListener('click', togglePlay);
  $('tmStepBack').addEventListener('click', () => stepTime(-1));
  $('tmStepFwd').addEventListener('click', () => stepTime(1));
  $('tmSpeed').addEventListener('change', () => { state.playSpeed = parseInt($('tmSpeed').value, 10); });
  $('tmLive').addEventListener('click', goLive);
  $('tmScrubber').addEventListener('input', (e) => {
    const idx = parseInt(e.target.value, 10);
    pausePlay();
    state.timeMachineActive = true;
    state.scrubIdx = idx;
    applyTimeSlice(idx);
    updateTmLabel();
  });
  $('instrSearch').addEventListener('input', () => {
    const q = $('instrSearch').value.toLowerCase();
    const ul = $('instrList'); ul.innerHTML = '';
    for (const s of instrs) {
      if (q && !s.toLowerCase().includes(q)) continue;
      const li = document.createElement('li');
      li.textContent = s;
      li.onclick = () => { $('dashInstr').value = s; };
      ul.appendChild(li);
    }
  });
  $('instrSearch').dispatchEvent(new Event('input'));
  loadTradeTree();
  setStatus(`${instrs.length} instruments, ${portfolios.length} portfolios`);
}

async function loadTradeTree() {
  const pid = $('dashPortfolio').value;
  state.dashPortfolio = pid;
  const container = $('tradeTree');
  container.innerHTML = '';
  if (!pid) { container.innerHTML = '<div class="meta" style="padding:4px">select a portfolio</div>'; return; }
  const statusFilter = $('tradeStatusFilter').value;
  const tr = await fetch(`/api/portfolios/${encodeURIComponent(pid)}/trades`).then(r => r.json());
  let filtered = tr;
  if (statusFilter) filtered = tr.filter(t => t.status === statusFilter);
  state.allTrades = filtered;

  const byYear = {};
  for (const t of filtered) {
    const d = new Date(t.entry_ts * 1000);
    const y = d.getUTCFullYear();
    const m = d.getUTCMonth();
    if (!byYear[y]) byYear[y] = {};
    if (!byYear[y][m]) byYear[y][m] = [];
    byYear[y][m].push(t);
  }

  for (const y of Object.keys(byYear).sort().reverse()) {
    const months = byYear[y];
    const yearTrades = Object.values(months).flat();
    const yearR = yearTrades.reduce((s, t) => s + (t.status === 'closed' ? t.pnl_r : 0), 0);
    const yearWins = yearTrades.filter(t => t.status === 'closed' && t.pnl_r > 0).length;
    const yearClosed = yearTrades.filter(t => t.status === 'closed').length;

    const yearNode = document.createElement('div');
    yearNode.style.cssText = 'padding:2px 4px;cursor:pointer;font-weight:bold;border-bottom:1px solid #1f242d';
    yearNode.innerHTML = `<span class="ty">▸</span> ${y} <span style="color:#9aa4b2;font-size:11px">${yearTrades.length} trades, ${yearWins}/${yearClosed} wins, ${yearR >= 0 ? '+' : ''}${yearR.toFixed(1)}R</span>`;
    const monthContainer = document.createElement('div');
    monthContainer.style.display = 'none';
    monthContainer.style.paddingLeft = '12px';
    yearNode.querySelector('.ty').onclick = (e) => {
      e.stopPropagation();
      const open = monthContainer.style.display !== 'none';
      monthContainer.style.display = open ? 'none' : 'block';
      yearNode.querySelector('.ty').textContent = open ? '▸' : '▾';
    };
    yearNode.onclick = () => yearNode.querySelector('.ty').click();

    for (const m of Object.keys(months).sort((a,b) => b - a)) {
      const mTrades = months[m];
      const mR = mTrades.reduce((s, t) => s + (t.status === 'closed' ? t.pnl_r : 0), 0);
      const mWins = mTrades.filter(t => t.status === 'closed' && t.pnl_r > 0).length;
      const mClosed = mTrades.filter(t => t.status === 'closed').length;
      const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
      const monthNode = document.createElement('div');
      monthNode.style.cssText = 'padding:2px 4px;cursor:pointer;border-bottom:1px solid #1f242d';
      monthNode.innerHTML = `<span class="tm">▸</span> ${monthNames[m]} <span style="color:#9aa4b2;font-size:11px">${mTrades.length} trades, ${mWins}/${mClosed} wins, ${mR >= 0 ? '+' : ''}${mR.toFixed(1)}R</span>`;
      const tradeContainer = document.createElement('div');
      tradeContainer.style.display = 'none';
      tradeContainer.style.paddingLeft = '12px';
      monthNode.querySelector('.tm').onclick = (e) => {
        e.stopPropagation();
        const open = tradeContainer.style.display !== 'none';
        tradeContainer.style.display = open ? 'none' : 'block';
        monthNode.querySelector('.tm').textContent = open ? '▸' : '▾';
      };
      monthNode.onclick = () => monthNode.querySelector('.tm').click();

      for (const t of mTrades) {
        const tradeNode = document.createElement('div');
        tradeNode.style.cssText = 'padding:2px 4px;cursor:pointer;font-size:11px;border-bottom:1px solid #1f242d';
        const dir = t.direction > 0 ? 'L' : 'S';
        const rstr = t.status === 'open' ? 'OPEN' : (t.pnl_r > 0 ? '+' : '') + t.pnl_r.toFixed(1) + 'R';
        tradeNode.innerHTML = `<span style="color:${t.direction > 0 ? '#26a69a' : '#ef5350'}">${dir}</span> ${t.symbol} ${fmtTs(t.entry_ts).slice(5,10)} <span style="color:${t.status === 'open' ? '#f0b429' : (t.pnl_r > 0 ? '#26a69a' : '#ef5350')}">${rstr}</span>`;
        tradeNode.onclick = async (e) => {
          e.stopPropagation();
          state.selectedTrade = t;
          $('dashInstr').value = t.symbol;
          $('dashTf').value = t.timeframe;
          await loadChart();
          // scroll to entry
          const idx = state.candles.findIndex(c => c.ts >= t.entry_ts);
          if (idx >= 0 && state.chart) {
            const from = Math.max(0, idx - 20);
            const to = Math.min(state.candles.length - 1, idx + 20);
            state.chart.timeScale().setVisibleLogicalRange({ from, to });
          }
        };
        tradeContainer.appendChild(tradeNode);
      }
      monthNode.appendChild(tradeContainer);
      monthContainer.appendChild(monthNode);
    }
    yearNode.appendChild(monthContainer);
    container.appendChild(yearNode);
  }
  if (filtered.length === 0) container.innerHTML = '<div class="meta" style="padding:4px">no trades</div>';
}


function cloneTimeline(tl) {
  if (!tl) return null;
  return {
    symbol: tl.symbol, timeframe: tl.timeframe,
    indicators: Object.fromEntries(Object.entries(tl.indicators || {}).map(([k,v]) => [k, v.map(p => ({...p}))])),
    swings: (tl.swings || []).map(s => ({...s})),
    setups: (tl.setups || []).map(s => ({...s})),
  };
}

function showLoading(on) {
  const el = $('chartLoading'); if (el) el.classList.toggle('hidden', !on);
}

function currentTsForIdx(idx) {
  if (!state.allCandles || state.allCandles.length === 0) return 0;
  const i = Math.max(0, Math.min(idx, state.allCandles.length - 1));
  return state.allCandles[i].ts;
}

function applyTimeSlice(idx) {
  if (!state.chart || !state.candleSeries) return;
  if (!state.allCandles || state.allCandles.length === 0) return;
  const i = Math.max(0, Math.min(idx, state.allCandles.length - 1));
  state.scrubIdx = i;
  state.timeMachineActive = (i < state.allCandles.length - 1);
  const ts = state.allCandles[i].ts;

  // candles slice
  const bars = state.allCandles.slice(0, i + 1);
  state.candles = bars;
  const candleData = bars.map(b => ({ time: b.ts, open: b.open, high: b.high, low: b.low, close: b.close }));
  const volumeData = bars.map(b => ({ time: b.ts, value: b.volume, color: b.close >= b.open ? '#26a69a80' : '#ef535080' }));
  state.candleSeries.setData(candleData);
  state.volumeSeries.setData(volumeData);

  // timeline slice
  if (state.allTimeline) {
    const tl = cloneTimeline(state.allTimeline);
    for (const k of Object.keys(tl.indicators)) {
      tl.indicators[k] = tl.indicators[k].filter(p => p.ts <= ts);
    }
    tl.swings = tl.swings.filter(s => (s.confirm_ts || s.ts) <= ts);
    tl.setups = tl.setups.filter(s => s.ts <= ts);
    state.timeline = tl;
  }
  renderIndicators();

  // trades slice: show trades whose entry_ts <= ts; mark closed only if exit_ts <= ts
  const trades = (state.allChartTrades || []).filter(t => t.entry_ts <= ts).map(t => {
    const copy = {...t};
    if (t.status === 'closed' && t.exit_ts && t.exit_ts > ts) {
      copy.status = 'open';
      copy.exit_ts = null;
      copy.exit_price = null;
    }
    return copy;
  });
  state.trades = trades;
  renderTradeOverlays();
}

function updateTmLabel() {
  const label = $('tmLabel');
  if (!label) return;
  if (!state.allCandles || state.allCandles.length === 0) { label.textContent = 'No data'; return; }
  const swingCount = state.timeline ? (state.timeline.swings || []).length : 0;
  const setupCount = state.timeline ? (state.timeline.setups || []).length : 0;
  if (!state.timeMachineActive) {
    label.textContent = `Live (all bars)  |  swings: ${swingCount}  setups: ${setupCount}`;
  } else {
    const ts = currentTsForIdx(state.scrubIdx);
    const d = new Date(ts * 1000);
    label.textContent = `t = ${d.toISOString().slice(0,10)}  (bar ${state.scrubIdx + 1}/${state.allCandles.length})  |  swings: ${swingCount}  setups: ${setupCount}`;
  }
}

function pausePlay() {
  if (state.playTimer) { clearInterval(state.playTimer); state.playTimer = null; }
  state.playing = false;
  const btn = $('tmPlay'); if (btn) btn.textContent = 'Play';
}

function togglePlay() {
  if (state.playing) { pausePlay(); return; }
  if (!state.allCandles || state.allCandles.length === 0) return;
  state.playing = true;
  const btn = $('tmPlay'); if (btn) btn.textContent = 'Pause';
  if (state.scrubIdx < 0) state.scrubIdx = 0;
  if (!state.timeMachineActive && state.scrubIdx >= state.allCandles.length - 1) {
    state.scrubIdx = 0;
    state.timeMachineActive = true;
  }
  state.playTimer = setInterval(() => {
    if (state.scrubIdx >= state.allCandles.length - 1) { goLive(); return; }
    state.scrubIdx += 1;
    const scrubber = $('tmScrubber');
    if (scrubber) scrubber.value = state.scrubIdx;
    applyTimeSlice(state.scrubIdx);
    updateTmLabel();
  }, state.playSpeed);
}

function stepTime(delta) {
  pausePlay();
  if (!state.allCandles || state.allCandles.length === 0) return;
  const ni = Math.max(0, Math.min(state.allCandles.length - 1, state.scrubIdx + delta));
  state.timeMachineActive = (ni < state.allCandles.length - 1);
  const scrubber = $('tmScrubber');
  if (scrubber) scrubber.value = ni;
  applyTimeSlice(ni);
  updateTmLabel();
}

function goLive() {
  pausePlay();
  state.timeMachineActive = false;
  if (state.allCandles && state.allCandles.length) {
    state.scrubIdx = state.allCandles.length - 1;
    const scrubber = $('tmScrubber');
    if (scrubber) scrubber.value = scrubber.max;
  }
  // restore full data
  state.candles = state.allCandles.slice();
  state.timeline = state.allTimeline ? cloneTimeline(state.allTimeline) : null;
  state.trades = (state.allChartTrades || []).slice();
  if (state.chart && state.candleSeries) {
    const candleData = state.candles.map(b => ({ time: b.ts, open: b.open, high: b.high, low: b.low, close: b.close }));
    const volumeData = state.candles.map(b => ({ time: b.ts, value: b.volume, color: b.close >= b.open ? '#26a69a80' : '#ef535080' }));
    state.candleSeries.setData(candleData);
    state.volumeSeries.setData(volumeData);
  }
  renderIndicators();
  renderTradeOverlays();
  updateTmLabel();
}

function initChart() {
  const container = $('chartContainer');
  container.innerHTML = '';
  const chart = LightweightCharts.createChart(container, {
    layout: { background: { color: '#0e1116' }, textColor: '#d6d9e0' },
    grid: { vertLines: { color: '#1f242d' }, horzLines: { color: '#1f242d' } },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: '#2a313c' },
    timeScale: { borderColor: '#2a313c', timeVisible: true },
  });
  const candleSeries = chart.addCandlestickSeries({
    upColor: '#26a69a', downColor: '#ef5350',
    borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350',
  });
  const volumeSeries = chart.addHistogramSeries({
    color: '#26a69a', priceFormat: { type: 'volume' }, priceScaleId: 'vol',
  });
  chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

  state.chart = chart;
  state.candleSeries = candleSeries;
  state.volumeSeries = volumeSeries;
}

async function loadChart() {
  const symbol = $('dashInstr').value;
  const tf = $('dashTf').value;
  const portfolioId = $('dashPortfolio').value;
  if (!symbol) return;
  state.dashInstr = symbol; state.dashTf = tf; state.dashPortfolio = portfolioId;

  showLoading(true);
  try {
    const res = await fetch(`/api/candles?symbol=${encodeURIComponent(symbol)}&tf=${tf}&limit=5000`);
    const bars = await res.json();
    state.allCandles = bars;
    state.candles = bars.slice();

    if (!state.chart) initChart();
    const candleData = bars.map(b => ({ time: b.ts, open: b.open, high: b.high, low: b.low, close: b.close }));
    const volumeData = bars.map(b => ({ time: b.ts, value: b.volume, color: b.close >= b.open ? '#26a69a80' : '#ef535080' }));
    state.candleSeries.setData(candleData);
    state.volumeSeries.setData(volumeData);

    // Load trades for this portfolio
    state.allChartTrades = [];
    state.trades = [];
    if (portfolioId) {
      const tr = await fetch(`/api/portfolios/${encodeURIComponent(portfolioId)}/trades`).then(r => r.json());
      const filtered = tr.filter(t => t.symbol === symbol && t.timeframe === tf);
      state.allChartTrades = filtered;
      state.trades = filtered.slice();
    }
    renderTradeOverlays();
    if (state.dashPortfolio) await loadTimeline();
    state.chart.timeScale().fitContent();

    // reset time machine to live
    pausePlay();
    state.timeMachineActive = false;
    state.scrubIdx = state.allCandles.length - 1;
    const scrubber = $('tmScrubber');
    if (scrubber) { scrubber.max = Math.max(0, state.allCandles.length - 1); scrubber.value = scrubber.max; }
    updateTmLabel();
  } finally {
    showLoading(false);
  }
}

// --- Timeline / indicators ---
async function loadTimeline() {
  const portfolio = $('dashPortfolio').value;
  const symbol = $('dashInstr').value;
  const tf = $('dashTf').value;
  if (!portfolio || !symbol) { state.timeline = null; state.allTimeline = null; renderIndicators(); return; }
  showLoading(true);
  try {
    const res = await fetch(`/api/indicators?portfolio=${encodeURIComponent(portfolio)}&symbol=${encodeURIComponent(symbol)}&tf=${tf}`);
    const tl = await res.json();
    state.allTimeline = tl;
    state.timeline = cloneTimeline(tl);
    if (state.timeMachineActive) applyTimeSlice(state.scrubIdx);
    else renderIndicators();
    setupCrosshair();
  } finally {
    showLoading(false);
  }
}

function clearIndicatorSeries() {
  const chart = state.chart; if (!chart) return;
  for (const k of ['emaSeries','atrSeries','supSeries','resSeries']) {
    if (state[k]) { try { chart.removeSeries(state[k]); } catch(e){} state[k] = null; }
  }
  // remove swing/setup markers (re-rendered as markers on candle series)
}

function renderIndicators() {
  const chart = state.chart; if (!chart || !state.candleSeries) return;
  clearIndicatorSeries();
  state.candleSeries.setMarkers([]);
  const tl = state.timeline; if (!tl) return;

  // EMA: line series on price scale
  if (state.showEma) {
    // find ema indicator key
    let emaKey = Object.keys(tl.indicators).find(k => k.toLowerCase().includes('ema'));
    if (emaKey) {
      const data = tl.indicators[emaKey].map(p => ({ time: p.ts, value: p.value }));
      const s = chart.addLineSeries({ color: '#f0b429', lineWidth: 2, priceScaleId: 'right' });
      s.setData(data);
      state.emaSeries = s;
    }
  }
  // ATR: separate price scale
  if (state.showAtr) {
    let atrKey = Object.keys(tl.indicators).find(k => k.toLowerCase().includes('atr'));
    if (atrKey) {
      const data = tl.indicators[atrKey].map(p => ({ time: p.ts, value: p.value }));
      const s = chart.addLineSeries({ color: '#ab47bc', lineWidth: 1, priceScaleId: 'atr' });
      chart.priceScale('atr').applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });
      s.setData(data);
      state.atrSeries = s;
    }
  }
  // Support / Resistance: step lines
  if (state.showSupport) {
    if (tl.indicators['support']) {
      const data = tl.indicators['support'].map(p => ({ time: p.ts, value: p.value }));
      const s = chart.addLineSeries({ color: '#26a69a66', lineWidth: 1, lineStyle: 2, priceScaleId: 'right' });
      s.setData(data); state.supSeries = s;
    }
    if (tl.indicators['resistance']) {
      const data = tl.indicators['resistance'].map(p => ({ time: p.ts, value: p.value }));
      const s = chart.addLineSeries({ color: '#ef535066', lineWidth: 1, lineStyle: 2, priceScaleId: 'right' });
      s.setData(data); state.resSeries = s;
    }
  }
  // Swing markers + setup markers on candle series
  const markers = [];
  if (state.showSwings) {
    for (const sw of tl.swings) {
      markers.push({
        time: sw.ts,
        position: sw.kind === -1 ? 'belowBar' : 'aboveBar',
        color: sw.kind === -1 ? '#26a69a' : '#ef5350',
        shape: sw.kind === -1 ? 'arrowUp' : 'arrowDown',
        text: sw.kind === -1 ? 'L' : 'H',
      });
    }
  }
  if (state.showSetups) {
    for (const sp of tl.setups) {
      markers.push({
        time: sp.ts,
        position: sp.direction > 0 ? 'belowBar' : 'aboveBar',
        color: sp.direction > 0 ? '#f0b429' : '#ff7043',
        shape: sp.direction > 0 ? 'arrowUp' : 'arrowDown',
        text: 'S',
      });
    }
  }
  // sort markers by time
  markers.sort((a,b) => a.time - b.time);
  state.candleSeries.setMarkers(markers);
}

function setupCrosshair() {
  const chart = state.chart; if (!chart) return;
  chart.subscribeCrosshairMove((param) => {
    const tooltip = $('chartTooltip');
    if (!tooltip) return;
    if (!param.time || !param.point) { tooltip.classList.add('hidden'); return; }
    const ts = param.time;
    // candle OHLCV
    let ohlc = '';
    const cd = state.candles.find(c => c.ts === ts) || state.candles.reduce((a,c) => (c.ts <= ts && (!a || c.ts > a.ts)) ? c : a, null);
    if (cd) ohlc = `O ${cd.open.toFixed(4)} H ${cd.high.toFixed(4)} L ${cd.low.toFixed(4)} C ${cd.close.toFixed(4)}`;
    // indicator values as-of ts
    const vals = [];
    if (state.timeline) {
      for (const [name, pts] of Object.entries(state.timeline.indicators)) {
        let last = null;
        for (const p of pts) { if (p.ts <= ts) last = p; else break; }
        if (last) vals.push(`${name}=${last.value.toFixed(4)}`);
      }
    }
    const d = new Date(ts * 1000);
    let html = `<b>${d.toISOString().slice(0,10)}</b> ${ohlc}`;
    if (vals.length) html += ' | ' + vals.join(' ');
    tooltip.innerHTML = html;
    tooltip.classList.remove('hidden');
  });
  chart.subscribeCrosshairMove((param) => {
    if (!param.time) {
      const tooltip = $('chartTooltip');
      if (tooltip) tooltip.classList.add('hidden');
    }
  });
}


function renderTradeOverlays() {
  const chart = state.chart;
  if (!chart || !state.candleSeries) return;
  // Detach previous primitive
  if (state._prim) {
    try { state.candleSeries.detachPrimitive(state._prim); } catch (e) {}
    state._prim = null;
  }
  state.candleSeries.setMarkers([]);
  if (!state.trades || !state.trades.length) return;
  const prim = createTradeRectPrimitive(state.trades);
  state.candleSeries.attachPrimitive(prim);
  state._prim = prim;
  if (prim._requestUpdate) prim._requestUpdate();
}

