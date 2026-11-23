/*
 * Copyright the original author or authors.
 * Licensed under the Apache License, Version 2.0.
 *
 * Citrus Test Distribution Dashboard — Zero-Node ESM SPA (htm-mini).
 */

import { html } from './vendor/htm-mini.js';

const STORAGE_TOKEN_KEY = 'citrus.dist.authToken';
const STORAGE_THEME_KEY = 'citrus.dist.theme';
const POLL_INTERVAL_MS = 4000;
// Full session detail can be several MB for large suites: reload it at most this often during live updates.
const DETAIL_MIN_INTERVAL_MS = 2000;
const VALID_DETAIL_TABS = new Set(['waterfall', 'tests', 'failures', 'files']);

const state = {
  view: 'overview', // 'overview' | 'sessions' | 'session-detail' | 'workers' | 'history'
  detailTab: 'waterfall', // 'waterfall' | 'tests' | 'failures' | 'files'
  sidebarCollapsed: false,
  mobileMenuOpen: false,
  theme: localStorage.getItem(STORAGE_THEME_KEY) || 'light',
  authToken: localStorage.getItem(STORAGE_TOKEN_KEY) || '',
  showAuthModal: false,
  authError: '',
  sseConnected: false,
  toastMessage: '',

  sessions: [],
  selectedSessionId: null,
  sessionDetail: null,
  workers: [],
  history: [],

  // Filters
  sessionQuery: '',
  sessionStatusFilter: 'ALL',
  testQuery: '',
  testStatusFilter: 'ALL',
  workerQuery: '',
  workerStateFilter: 'ALL',
  historyQuery: '',
  historyFlakyOnly: false,

  // Modals
  selectedPartition: null,
  pages: {}, // current page per paginated table
  pageContexts: {}, // session and filters each table's current page belongs to
  selectedTestExecution: null
};

document.documentElement.setAttribute('data-theme', state.theme);

let eventSource = null;
let toastTimer = null;
let refreshTimer = null;
let refreshInFlight = false;
let refreshPending = false;
let lastDetailLoadAt = 0;
let detailTimer = null;
// Increases with every session detail request; a response that is no longer the latest is discarded.
let detailRequestSeq = 0;
let pollTimer = null;
let suppressHashChange = false;

function showToast(msg) {
  state.toastMessage = msg;
  renderApp();
  if (toastTimer) clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    state.toastMessage = '';
    renderApp();
  }, 3200);
}

function buildHash() {
  if (state.view === 'session-detail' && state.selectedSessionId) {
    return `#session/${encodeURIComponent(state.selectedSessionId)}/${state.detailTab || 'waterfall'}`;
  }
  return `#${state.view || 'overview'}`;
}

function syncHash() {
  const target = buildHash();
  if (window.location.hash !== target) {
    suppressHashChange = true;
    window.history.replaceState(null, '', target);
    suppressHashChange = false;
  }
}

function safeDecodeSegment(val) {
  if (!val) return null;
  try {
    return decodeURIComponent(val);
  } catch (_) {
    return val;
  }
}

function applyHashFromLocation() {
  const raw = (window.location.hash || '').replace(/^#/, '').trim();
  if (!raw) return false;
  if (raw.startsWith('session/')) {
    const parts = raw.split('/');
    const sid = safeDecodeSegment(parts[1]);
    const tab = parts[2] && VALID_DETAIL_TABS.has(parts[2]) ? parts[2] : 'waterfall';
    if (sid) {
      state.selectedSessionId = sid;
      state.detailTab = tab;
      state.view = 'session-detail';
      return true;
    }
  }
  if (['overview', 'sessions', 'session-detail', 'workers', 'history'].includes(raw)) {
    state.view = raw;
    return true;
  }
  return false;
}

function setView(view, detailTab = null) {
  state.view = view;
  if (detailTab && VALID_DETAIL_TABS.has(detailTab)) {
    state.detailTab = detailTab;
  }
  state.mobileMenuOpen = false;
  syncHash();
  renderApp();
  if (HISTORY_VIEWS.has(view)) {
    loadHistory(true);
  }
}

function apiHeaders() {
  const headers = { 'Accept': 'application/json' };
  if (state.authToken) {
    headers['Authorization'] = `Bearer ${state.authToken}`;
  }
  return headers;
}

async function apiGet(path) {
  const res = await fetch(path, { headers: apiHeaders() });
  if (res.status === 401) {
    state.showAuthModal = true;
    state.authError = 'Bearer token required to access broker API.';
    renderApp();
    throw new Error('Unauthorized');
  }
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return res.json();
}

async function apiPost(path, body = {}) {
  const res = await fetch(path, {
    method: 'POST',
    headers: { ...apiHeaders(), 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (res.status === 401) {
    state.showAuthModal = true;
    state.authError = 'Bearer token required to access broker API.';
    renderApp();
    throw new Error('Unauthorized');
  }
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return res.json();
}

function scheduleRefresh(delayMs = 140) {
  if (refreshTimer) return;
  refreshTimer = setTimeout(() => {
    refreshTimer = null;
    refreshAll();
  }, delayMs);
}

let historyInFlight = false;
let lastHistoryLoadAt = 0;
// Test history aggregates every recorded execution: refresh it at most this often, and only where it is shown.
const HISTORY_MIN_INTERVAL_MS = 10000;
const HISTORY_VIEWS = new Set(['overview', 'history']);

async function loadHistory(force = false) {
  if (historyInFlight) return;
  if (!force && (!HISTORY_VIEWS.has(state.view) || Date.now() - lastHistoryLoadAt < HISTORY_MIN_INTERVAL_MS)) {
    if (state.history.length > 0) return;
  }
  historyInFlight = true;
  lastHistoryLoadAt = Date.now();
  try {
    const history = await apiGet('/api/v1/tests/history?limit=2000');
    state.history = Array.isArray(history) ? history : [];
    renderApp();
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      console.warn('History refresh error:', err.message);
    }
  } finally {
    historyInFlight = false;
  }
}

async function refreshAll() {
  // One refresh at a time: events arriving meanwhile coalesce into a single follow-up refresh.
  if (refreshInFlight) {
    refreshPending = true;
    return;
  }
  refreshInFlight = true;
  try {
    const [sessions, workers] = await Promise.all([
      apiGet('/api/v1/sessions?limit=500'),
      apiGet('/api/v1/workers')
    ]);
    state.sessions = Array.isArray(sessions) ? sessions : [];
    state.workers = Array.isArray(workers) ? workers : [];
    loadHistory(); // independent: a slow history query must not hold back sessions and workers
    state.authError = '';

    // The heavy session detail is only needed on the session page.
    if (state.selectedSessionId && state.view === 'session-detail') {
      const current = state.sessionDetail && state.sessionDetail.session
        && state.sessionDetail.session.sessionId === state.selectedSessionId;
      const sinceDetail = Date.now() - lastDetailLoadAt;
      if (!current || sinceDetail >= DETAIL_MIN_INTERVAL_MS) {
        await loadSessionDetail(state.selectedSessionId, false);
      } else {
        // Keep the KPI counters live from the light session list; reload the heavy detail on the trailing edge.
        const summary = state.sessions.find(s => s.sessionId === state.selectedSessionId);
        if (summary) {
          state.sessionDetail = { ...state.sessionDetail, session: summary };
        }
        if (!detailTimer) {
          detailTimer = setTimeout(() => {
            detailTimer = null;
            if (state.view === 'session-detail' && state.selectedSessionId) {
              loadSessionDetail(state.selectedSessionId, false);
            }
          }, DETAIL_MIN_INTERVAL_MS - sinceDetail);
        }
      }
    } else if (state.selectedSessionId) {
      // keep the selection; its detail is loaded when the session page is shown
    } else if (state.sessions.length > 0) {
      state.selectedSessionId = state.sessions[0].sessionId;
      if (state.view === 'session-detail') {
        await loadSessionDetail(state.selectedSessionId, false);
      }
    }
    syncHash();
    renderApp();
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      console.warn('Refresh error:', err.message);
    }
  } finally {
    refreshInFlight = false;
    if (refreshPending) {
      refreshPending = false;
      scheduleRefresh();
    }
  }
}

async function loadSessionDetail(sessionId, switchView = true) {
  try {
    const requestSeq = ++detailRequestSeq;
    const detail = await apiGet(`/api/v1/sessions/${encodeURIComponent(sessionId)}`);
    if (requestSeq !== detailRequestSeq) {
      return; // a newer request (e.g. the user switched sessions) superseded this one
    }
    lastDetailLoadAt = Date.now();
    state.selectedSessionId = sessionId;
    state.sessionDetail = detail;

    if (state.selectedPartition && Array.isArray(detail.partitions)) {
      const exact = detail.partitions.find(
        p => p.partitionId === state.selectedPartition.partitionId
          && p.attempt === state.selectedPartition.attempt
      );
      const fallback = detail.partitions.find(p => p.partitionId === state.selectedPartition.partitionId);
      state.selectedPartition = exact || fallback || null;
    }
    if (state.selectedTestExecution && Array.isArray(detail.tests)) {
      state.selectedTestExecution = detail.tests.find(t => t.id === state.selectedTestExecution.id) || null;
    }

    if (switchView) {
      state.view = 'session-detail';
      state.mobileMenuOpen = false;
    }
    syncHash();
    renderApp();
  } catch (err) {
    console.warn('Failed to load session detail:', err.message);
  }
}

function ensurePollingLoop() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(() => {
    if (!state.sseConnected && !state.authError) {
      if (!eventSource || eventSource.readyState === EventSource.CLOSED) {
        connectSse();
      }
      refreshAll();
    } else if (state.sessions.some(s => s.status === 'RUNNING')) {
      renderApp();
    }
  }, POLL_INTERVAL_MS);
}

function connectSse() {
  if (eventSource) {
    eventSource.close();
  }
  const tokenQuery = state.authToken ? `?token=${encodeURIComponent(state.authToken)}` : '';
  eventSource = new EventSource(`/api/v1/events/stream${tokenQuery}`);
  eventSource.onopen = () => {
    state.sseConnected = true;
    renderApp();
  };
  eventSource.onerror = () => {
    state.sseConnected = false;
    renderApp();
  };
  const eventTypes = [
    'session.created', 'session.completed', 'session.aborted', 'work.submitted',
    'partition.assigned', 'partition.started', 'partition.requeued',
    'partition.completed', 'partition.lost', 'test.event',
    'worker.registered', 'worker.draining', 'worker.offline', 'worker.disconnected',
    'demo.seeded'
  ];
  for (const evt of eventTypes) {
    eventSource.addEventListener(evt, () => {
      scheduleRefresh();
    });
  }
}

function toggleTheme() {
  state.theme = state.theme === 'light' ? 'dark' : 'light';
  localStorage.setItem(STORAGE_THEME_KEY, state.theme);
  document.documentElement.setAttribute('data-theme', state.theme);
  renderApp();
}

async function handleSeedDemo() {
  try {
    const res = await apiPost('/api/v1/demo/seed', {});
    await refreshAll();
    showToast(res.seededSessions > 0
      ? `Seeded ${res.seededSessions} demo Citrus sessions & 6 workers`
      : 'Demo dataset refreshed');
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      showToast(`Failed to seed demo data: ${err.message}`);
    }
  }
}

async function handleAbortSession(sessionId) {
  try {
    await apiPost(`/api/v1/sessions/${encodeURIComponent(sessionId)}/abort`, {
      reason: 'Aborted from Citrus Distribution UI'
    });
    await refreshAll();
    showToast(`Aborted session ${sessionId}`);
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      showToast(`Failed to abort session: ${err.message}`);
    }
  }
}

function formatDuration(ms) {
  if (!ms || ms <= 0) return '0ms';
  if (ms < 1000) return `${ms}ms`;
  const sec = ms / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const m = Math.floor(sec / 60);
  const rem = (sec % 60).toFixed(0);
  return `${m}m ${rem}s`;
}

function formatAgo(epochMs) {
  if (!epochMs || epochMs <= 0) return '—';
  const diffSec = Math.max(0, Math.floor((Date.now() - epochMs) / 1000));
  if (diffSec < 5) return 'just now';
  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return new Date(epochMs).toLocaleDateString();
}

function shortClass(fqcn) {
  if (!fqcn) return '';
  const parts = fqcn.split('.');
  return parts[parts.length - 1];
}

function statusBadge(status) {
  const s = (status || 'UNKNOWN').toUpperCase();
  return html`<span class=${`badge badge-${s.toLowerCase()}`}>${s}</span>`;
}

function citrusLogoSvg() {
  return html`
    <svg class="brand-logo" viewBox="0 0 64 64" aria-hidden="true">
      <circle cx="32" cy="32" r="30" fill="#f5921d" />
      <circle cx="32" cy="32" r="24" fill="#fff7ed" />
      <g fill="#f7ad19">
        <path d="M32 10 A22 22 0 0 1 46 16 L33 30 Z" />
        <path d="M48 18 A22 22 0 0 1 54 31 L34 31 Z" fill="#f5921d" />
        <path d="M54 33 A22 22 0 0 1 48 46 L34 33 Z" />
        <path d="M46 48 A22 22 0 0 1 33 54 L33 34 Z" fill="#f15a24" />
        <path d="M31 54 A22 22 0 0 1 18 48 L31 34 Z" />
        <path d="M16 46 A22 22 0 0 1 10 33 L30 33 Z" fill="#f5921d" />
        <path d="M10 31 A22 22 0 0 1 16 18 L30 31 Z" />
        <path d="M18 16 A22 22 0 0 1 31 10 L31 30 Z" fill="#f15a24" />
      </g>
    </svg>
  `;
}

function renderTopbar() {
  return html`
    <header class="topbar">
      <div class="topbar-left">
        <button
          class="icon-btn mobile-only"
          aria-label="Toggle navigation menu"
          onClick=${() => { state.mobileMenuOpen = !state.mobileMenuOpen; renderApp(); }}
        >
          ☰ Menu
        </button>
        <button
          class="icon-btn desktop-only"
          aria-label="Collapse sidebar"
          title="Collapse / expand sidebar"
          onClick=${() => { state.sidebarCollapsed = !state.sidebarCollapsed; renderApp(); }}
        >
          ${state.sidebarCollapsed ? '»' : '«'}
        </button>
        <div class="brand" onClick=${() => setView('overview')}>
          ${citrusLogoSvg()}
          <span class="brand-title">Citrus</span>
          <span class="brand-subtitle">Test Distribution</span>
        </div>
      </div>

      <div class="topbar-right">
        <span class=${`sse-pill ${state.sseConnected ? '' : 'offline'}`}>
          <span class="sse-dot"></span>
          ${state.sseConnected ? 'Live SSE' : 'Polling'}
        </span>
        <button class="icon-btn" onClick=${handleSeedDemo} title="Populate realistic Citrus demo dataset">
          ✦ Seed Demo Data
        </button>
        <button class="icon-btn" onClick=${toggleTheme} aria-label="Toggle dark or light theme">
          ${state.theme === 'light' ? '☾ Dark' : '☀ Light'}
        </button>
        <button
          class="icon-btn"
          onClick=${() => { state.showAuthModal = true; renderApp(); }}
          aria-label="Configure Bearer token"
        >
          🔑 ${state.authToken ? 'Auth Set' : 'Auth'}
        </button>
      </div>
    </header>
  `;
}

function renderSidebar() {
  const activeWorkers = state.workers.filter(w => w.state !== 'OFFLINE').length;
  const flakyCount = state.history.filter(h => h.flakyRuns > 0).length;
  const classes = [
    'sidebar',
    state.sidebarCollapsed ? 'collapsed' : '',
    state.mobileMenuOpen ? 'mobile-open' : ''
  ].filter(Boolean).join(' ');

  const navItem = (id, label, iconChar, badgeVal) => html`
    <button
      class=${`nav-btn ${state.view === id ? 'active' : ''}`}
      onClick=${() => {
        if (id === 'session-detail' && !state.sessionDetail && state.sessions.length > 0) {
          loadSessionDetail(state.sessions[0].sessionId, true);
          return;
        }
        setView(id);
      }}
    >
      <span class="nav-btn-left">
        <span class="nav-icon">${iconChar}</span>
        <span class="nav-label">${label}</span>
      </span>
      ${badgeVal !== null && badgeVal !== undefined ? html`<span class="nav-badge">${badgeVal}</span>` : null}
    </button>
  `;

  return html`
    <aside class=${classes}>
      <div class="nav-group">
        <div class="nav-section-label">Observability</div>
        ${navItem('overview', 'Overview', '◈', null)}
        ${navItem('sessions', 'Build Sessions', '▤', state.sessions.length)}
        ${navItem('session-detail', 'Session Waterfall', '⇶', state.sessionDetail ? state.sessionDetail.session.status : null)}
        <div class="nav-section-label" style="margin-top:10px">Infrastructure & Quality</div>
        ${navItem('workers', 'Workers Pool', '⚙', `${activeWorkers}/${state.workers.length}`)}
        ${navItem('history', 'Test History & Flakiness', '⚡', flakyCount > 0 ? `${flakyCount} flaky` : state.history.length)}
      </div>

      <div class="sidebar-footer">
        <div class="sidebar-footer-text">Citrus Framework 5.3 · Apache-2.0</div>
      </div>
    </aside>
  `;
}

function renderOverview() {
  const totalSessions = state.sessions.length;
  const passedSessions = state.sessions.filter(s => s.status === 'PASSED').length;
  const failedSessions = state.sessions.filter(s => s.status === 'FAILED').length;
  const runningSessions = state.sessions.filter(s => s.status === 'RUNNING').length;
  const completedCount = passedSessions + failedSessions;
  const passRate = completedCount > 0 ? Math.round((passedSessions / completedCount) * 100) : 100;

  const onlineWorkers = state.workers.filter(w => w.state !== 'OFFLINE').length;
  const busyWorkers = state.workers.filter(w => w.state === 'BUSY').length;

  let totalBlobs = 0;
  let cachedBlobs = 0;
  let totalSerial = 0;
  let totalWall = 0;
  for (const s of state.sessions) {
    totalBlobs += s.totalBlobs || 0;
    cachedBlobs += s.cachedBlobs || 0;
    if (s.serialDurationMs > 0 && s.wallDurationMs > 0) {
      totalSerial += s.serialDurationMs;
      totalWall += s.wallDurationMs;
    }
  }
  const cacheHitPct = totalBlobs > 0 ? ((cachedBlobs / totalBlobs) * 100).toFixed(1) : '0.0';
  const avgSpeedup = totalWall > 0 ? `${(totalSerial / totalWall).toFixed(1)}x` : '1.0x';
  const flakySuites = state.history.filter(h => h.flakyRuns > 0).length;

  return html`
    <div>
      <div class="page-header">
        <div>
          <h1 class="page-title">Test Distribution Control Plane</h1>
          <p class="page-subtitle">Real-time LPT partition scheduling, content-addressed classpath & workspace caching, and Citrus execution telemetry.</p>
        </div>
        <button class="btn" onClick=${refreshAll}>↻ Refresh</button>
      </div>

      <div class="kpi-grid">
        <div class="kpi-card">
          <div class="kpi-label">Build Sessions</div>
          <div class="kpi-value">${totalSessions}</div>
          <div class="kpi-meta">${runningSessions} active · ${failedSessions} failed</div>
        </div>
        <div class="kpi-card accent-emerald">
          <div class="kpi-label">Session Pass Rate</div>
          <div class="kpi-value">${passRate}%</div>
          <div class="kpi-meta">${passedSessions} passed completed runs</div>
        </div>
        <div class="kpi-card accent-blue">
          <div class="kpi-label">Online Workers</div>
          <div class="kpi-value">${onlineWorkers} / ${state.workers.length}</div>
          <div class="kpi-meta">${busyWorkers} executing partitions now</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Distribution Speedup</div>
          <div class="kpi-value">${avgSpeedup}</div>
          <div class="kpi-meta">${formatDuration(totalSerial)} serial vs ${formatDuration(totalWall)} wall</div>
        </div>
        <div class="kpi-card accent-emerald">
          <div class="kpi-label">Blob Cache Efficiency</div>
          <div class="kpi-value">${cacheHitPct}%</div>
          <div class="kpi-meta">${cachedBlobs} / ${totalBlobs} SHA-256 artifacts reused</div>
        </div>
        <div class="kpi-card accent-amber">
          <div class="kpi-label">Flaky Tests Detected</div>
          <div class="kpi-value">${flakySuites}</div>
          <div class="kpi-meta">Passed on automatic worker retry</div>
        </div>
      </div>

      <div class="overview-grid">
        <div class="card">
          <div class="card-header">
            <h2 class="card-title">Recent Build Sessions</h2>
            <button class="btn btn-sm" onClick=${() => setView('sessions')}>View All →</button>
          </div>
          ${renderSessionsTable(state.sessions.slice(0, 6))}
        </div>

        <div class="card">
          <div class="card-header">
            <h2 class="card-title">Worker Pool Health</h2>
            <button class="btn btn-sm" onClick=${() => setView('workers')}>Inspect Pool →</button>
          </div>
          <div class="card-body" style="display:flex;flex-direction:column;gap:10px">
            ${state.workers.length === 0
              ? html`<div class="empty-state">No workers registered yet.</div>`
              : state.workers.map(w => html`
                  <div style="display:flex;align-items:center;justify-content:space-between;padding:8px 10px;border-radius:8px;background:var(--bg-subtle)">
                    <div>
                      <div class="mono" style="font-weight:700;font-size:12.5px">${w.workerId}</div>
                      <div style="font-size:11.5px;color:var(--text-muted)">${w.host} · heartbeat ${formatAgo(w.lastHeartbeatAt)}</div>
                    </div>
                    <div>
                      ${isLowDisk(w) ? html`<span class="badge badge-failed">LOW DISK</span> ` : null}
                      ${statusBadge(w.state)}
                    </div>
                  </div>
                `)}
          </div>
        </div>
      </div>
    </div>
  `;
}

function renderSessionsTable(rows) {
  if (!rows || rows.length === 0) {
    return html`<div class="empty-state">No matching build sessions found. Click "Seed Demo Data" in the header to explore sample runs.</div>`;
  }
  return html`
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Session</th>
            <th>Branch / Commit</th>
            <th>Status</th>
            <th>Tests</th>
            <th>Cache Hit</th>
            <th>Wall / Speedup</th>
            <th>Started</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map(s => {
            const total = Math.max(1, s.totalTests || (s.passedTests + s.failedTests + s.skippedTests));
            const passPct = Math.round(((s.passedTests || 0) / total) * 100);
            const failPct = Math.round(((s.failedTests || 0) / total) * 100);
            const skipPct = Math.max(0, 100 - passPct - failPct);
            const cachePct = s.totalBlobs > 0 ? `${Math.round((s.cachedBlobs / s.totalBlobs) * 100)}%` : '—';
            const speedup = (s.serialDurationMs > 0 && s.wallDurationMs > 0)
              ? `${(s.serialDurationMs / s.wallDurationMs).toFixed(1)}x`
              : null;
            const wallLabel = s.wallDurationMs > 0
              ? formatDuration(s.wallDurationMs)
              : (s.status === 'RUNNING' ? `live · ${formatDuration(Math.max(0, Date.now() - s.startedAt))}` : '—');
            return html`
              <tr class="clickable-row" onClick=${() => loadSessionDetail(s.sessionId, true)}>
                <td>
                  <div class="mono" style="font-weight:700;color:var(--citrus-orange-deep)">${s.sessionId}</div>
                  <div style="font-size:11.5px;color:var(--text-muted)">${s.projectName || 'citrus'}</div>
                  ${Array.isArray(s.modules) && s.modules.length > 0 ? html`
                    <div class="session-modules" title=${s.modules.join('\n')}>${modulesLabel(s)}</div>
                  ` : null}
                </td>
                <td>
                  <div style="font-weight:600">${s.gitBranch || 'local'}</div>
                  <div class="mono" style="font-size:11.5px;color:var(--text-muted)">${(s.gitCommit || '').slice(0, 10) || 'working-tree'}</div>
                </td>
                <td title=${s.failureReason || ''}>${statusBadge(s.status)}</td>
                <td>
                  <div style="font-size:12.5px;font-weight:600;margin-bottom:4px">
                    <span style="color:var(--emerald-text)">${s.passedTests}✓</span> ·
                    <span style="color:var(--rose-text)"> ${s.failedTests}✗</span> ·
                    <span style="color:var(--text-muted)"> ${s.skippedTests}↷</span>
                    ${s.totalTests > 0 ? html` <span style="color:var(--text-muted);font-weight:500">/ ${s.totalTests}</span>` : null}
                  </div>
                  <div class="progress-mini">
                    <span class="progress-seg-pass" style=${`width:${passPct}%`}></span>
                    <span class="progress-seg-fail" style=${`width:${failPct}%`}></span>
                    <span class="progress-seg-skip" style=${`width:${skipPct}%`}></span>
                  </div>
                </td>
                <td class="mono">${cachePct}</td>
                <td>
                  <span class="mono">${wallLabel}</span>
                  ${speedup ? html` <span class="speedup-badge">${speedup}</span>` : null}
                </td>
                <td style="color:var(--text-muted);font-size:12.5px">${formatAgo(s.startedAt)}</td>
              </tr>
            `;
          })}
        </tbody>
      </table>
    </div>
  `;
}

function renderSessionsExplorer() {
  const statuses = ['ALL', 'RUNNING', 'PASSED', 'FAILED', 'ABORTED'];
  const q = state.sessionQuery.trim().toLowerCase();
  const filtered = state.sessions.filter(s => {
    if (state.sessionStatusFilter !== 'ALL' && s.status !== state.sessionStatusFilter) return false;
    if (!q) return true;
    return (s.sessionId || '').toLowerCase().includes(q)
      || (s.gitBranch || '').toLowerCase().includes(q)
      || (s.gitCommit || '').toLowerCase().includes(q)
      || (s.projectName || '').toLowerCase().includes(q);
  });

  return html`
    <div>
      <div class="page-header">
        <div>
          <h1 class="page-title">Build Sessions Explorer</h1>
          <p class="page-subtitle">Inspect distributed Maven & JBang reactor runs, classpath cache hit rates, and test outcomes.</p>
        </div>
      </div>

      <div class="toolbar">
        <input
          class="search-input"
          data-focus-id="sessions-search"
          type="search"
          placeholder="Filter by session ID, branch, commit, or project…"
          value=${state.sessionQuery}
          onInput=${e => { state.sessionQuery = e.target.value; renderApp(); }}
        />
        <div class="pill-group">
          ${statuses.map(st => html`
            <button
              class=${`filter-pill ${state.sessionStatusFilter === st ? 'active' : ''}`}
              onClick=${() => { state.sessionStatusFilter = st; renderApp(); }}
            >${st}</button>
          `)}
        </div>
      </div>

      <div class="card">
        ${(() => {
          const page = paginate('sessions', filtered, 25, `${state.sessionStatusFilter}|${q}`);
          return html`${renderSessionsTable(page.items)}${page.controls}`;
        })()}
      </div>
    </div>
  `;
}

// One color per module, stable for a session: modules in the order they started (reactor order).
const MODULE_PALETTE = ['#10b981', '#6366f1', '#f97316', '#0ea5e9', '#d946ef', '#84cc16', '#eab308', '#14b8a6', '#f43f5e', '#8b5cf6'];

function moduleOrder(session, partitions) {
  const ordered = Array.isArray(session.modules) ? [...session.modules] : [];
  const byStart = [...partitions].sort((a, b) => (a.startedAt || 0) - (b.startedAt || 0));
  for (const p of byStart) {
    const m = p.moduleName || 'default';
    if (!ordered.includes(m)) ordered.push(m);
  }
  return ordered;
}

function moduleColor(modules, moduleName) {
  const idx = modules.indexOf(moduleName || 'default');
  return MODULE_PALETTE[(idx < 0 ? 0 : idx) % MODULE_PALETTE.length];
}

function modulesLabel(session) {
  const modules = Array.isArray(session.modules) ? session.modules : [];
  return modules.length === 1 ? modules[0] : `${modules.length} modules`;
}

// Client-side pagination: one page of items plus pager controls; the page is remembered per table and clamped when
// filters shrink the list. `context` names the session and filters the list depends on: when it changes, the table
// returns to its first page.
function paginate(key, items, pageSize = 25, context = '') {
  if (state.pageContexts[key] !== context) {
    state.pageContexts[key] = context;
    state.pages[key] = 0;
  }
  const total = items.length;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const page = Math.min(Math.max(0, state.pages[key] || 0), pageCount - 1);
  const go = target => { state.pages[key] = target; renderApp(); };
  const from = total === 0 ? 0 : page * pageSize + 1;
  const to = Math.min(total, (page + 1) * pageSize);
  return {
    items: items.slice(page * pageSize, (page + 1) * pageSize),
    controls: total <= pageSize ? null : html`
      <div class="pager">
        <span>Showing ${from}–${to} of ${total}</span>
        <button class="btn btn-sm" disabled=${page === 0} onClick=${() => go(0)}>« First</button>
        <button class="btn btn-sm" disabled=${page === 0} onClick=${() => go(page - 1)}>‹ Prev</button>
        <span class="mono">Page ${page + 1} / ${pageCount}</span>
        <button class="btn btn-sm" disabled=${page >= pageCount - 1} onClick=${() => go(page + 1)}>Next ›</button>
        <button class="btn btn-sm" disabled=${page >= pageCount - 1} onClick=${() => go(pageCount - 1)}>Last »</button>
      </div>
    `
  };
}

// Where a partition's time went: before its first test (worker setup: classpath, JVM or class loader, JUnit and
// framework bootstrap), running tests, and after its last test (reporting, workspace cleanup). Derived from the
// recorded test timestamps; null when no test of the partition has finished yet.
function partitionTiming(p, tests) {
  if (!(p.startedAt > 0)) return null;
  const end = p.finishedAt > 0 ? p.finishedAt : Date.now();
  const own = (tests || []).filter(t => t.partitionId === p.partitionId && (!t.attempt || t.attempt === p.attempt)
    && t.status !== 'STARTED' && t.recordedAt > 0);
  if (own.length === 0) return null;
  const firstStart = Math.max(p.startedAt, Math.min(...own.map(t => t.recordedAt - (t.durationMs || 0))));
  const lastEnd = Math.min(end, Math.max(...own.map(t => t.recordedAt)));
  return {
    total: end - p.startedAt,
    setup: Math.max(0, firstStart - p.startedAt),
    tests: Math.max(0, lastEnd - firstStart),
    wrapUp: Math.max(0, end - lastEnd),
    testTime: own.reduce((sum, t) => sum + (t.durationMs || 0), 0)
  };
}

// Bar labels always stay inside their bar: shortened with an ellipsis to fit, or omitted when nothing readable fits.
// The full partition id is always in the bar's tooltip.
const BAR_LABEL_CHAR_PX = 6.8;
const BAR_LABEL_PADDING_PX = 16;

function fitBarLabel(label, barWidth) {
  const maxChars = Math.floor((barWidth - BAR_LABEL_PADDING_PX) / BAR_LABEL_CHAR_PX);
  if (label.length <= maxChars) {
    return label;
  }
  const distinctive = label.replace(/^part-/, '');  // every partition id shares this prefix
  if (distinctive.length <= maxChars) {
    return distinctive;
  }
  return maxChars >= 5 ? `${distinctive.slice(0, maxChars - 1)}…` : '';
}

function renderWaterfallSvg(session, partitions) {
  if (!partitions || partitions.length === 0) {
    return html`<div class="empty-state">No partitions recorded for this session yet.</div>`;
  }

  const minStart = Math.min(...partitions.map(p => p.startedAt > 0 ? p.startedAt : session.startedAt));
  const nowMs = Date.now();
  const estOf = p => Math.max(300, p.estimatedDurationMs || 1000);
  const startOf = p => p.startedAt > 0 ? p.startedAt : minStart;
  const endOf = p => p.finishedAt > 0
    ? p.finishedAt
    : ((p.state === 'RUNNING' || p.state === 'ASSIGNED') ? Math.max(startOf(p) + 300, nowMs) : startOf(p) + estOf(p));
  const maxEnd = Math.max(
    minStart + 1000,
    session.finishedAt > 0 ? session.finishedAt : 0,
    ...partitions.map(endOf)
  );
  const totalSpan = Math.max(1000, maxEnd - minStart);

  // Group partitions by worker swimlane
  const lanes = new Map();
  for (const p of partitions) {
    const laneKey = p.workerId || 'unassigned-queue';
    if (!lanes.has(laneKey)) lanes.set(laneKey, []);
    lanes.get(laneKey).push(p);
  }

  const laneEntries = Array.from(lanes.entries());
  const labelWidth = 205;
  const chartWidth = 760;
  const svgWidth = labelWidth + chartWidth + 30;
  const rowHeight = 46;
  const headerHeight = 34;
  const svgHeight = headerHeight + laneEntries.length * rowHeight + 16;

  const modules = moduleOrder(session, partitions);
  // Fill shows the module; the outline shows the state.
  const outlineFor = st => {
    switch (st) {
      case 'FAILED': return { stroke: '#ef4444', width: 3, dash: '' };
      case 'LOST': return { stroke: '#f59e0b', width: 3, dash: '5,3' };
      case 'RUNNING':
      case 'ASSIGNED': return { stroke: '#3b82f6', width: 2, dash: '4,3' };
      default: return { stroke: 'none', width: 0, dash: '' };
    }
  };

  const ticks = [0, 0.25, 0.5, 0.75, 1];

  return html`
    <div>
      <div class="waterfall-legend">
        ${modules.map(m => html`
          <span class="legend-item"><span class="legend-swatch" style=${`background:${moduleColor(modules, m)}`}></span> ${m}</span>
        `)}
        <span class="legend-item legend-outline"><span class="legend-swatch legend-swatch-outline" style="border-color:#ef4444"></span> Failed</span>
        <span class="legend-item legend-outline"><span class="legend-swatch legend-swatch-outline" style="border-color:#3b82f6;border-style:dashed"></span> Running</span>
        <span class="legend-item legend-outline"><span class="legend-swatch legend-swatch-outline" style="border-color:#f59e0b;border-style:dashed"></span> Lost / Requeued</span>
        <span style="margin-left:auto;font-size:12px;color:var(--text-muted)">Hover a bar for details · click to inspect</span>
      </div>

      <div class="waterfall-scroll">
        <svg style="width:100%;height:auto;display:block" viewBox=${`0 0 ${svgWidth} ${svgHeight}`}>
          ${ticks.map((t, tIdx) => {
            const x = labelWidth + Math.round(t * chartWidth);
            const label = formatDuration(Math.round(t * totalSpan));
            const anchor = tIdx === ticks.length - 1 ? 'end' : 'middle';
            return html`
              <g>
                <line x1=${x} y1=${headerHeight - 6} x2=${x} y2=${svgHeight - 8} stroke="#94a3b8" stroke-opacity="0.25" stroke-dasharray="3,3" />
                <text x=${x} y="20" text-anchor=${anchor} font-size="11" fill="#64748b">${label}</text>
              </g>
            `;
          })}

          ${laneEntries.map(([workerId, laneParts], idx) => {
            const y = headerHeight + idx * rowHeight;
            return html`
              <g>
                <rect x="4" y=${y + 3} width=${svgWidth - 8} height=${rowHeight - 6} rx="6" fill="#94a3b8" fill-opacity="0.07" />
                <text x="14" y=${y + 26} font-size="12" font-weight="600" fill="currentColor">${workerId}</text>
                ${laneParts.map(p => {
                  const pStart = startOf(p);
                  const pEnd = endOf(p);
                  const relX = Math.min(chartWidth - 24, Math.max(0, Math.round(((pStart - minStart) / totalSpan) * chartWidth)));
                  const x = labelWidth + relX;
                  const w = Math.max(44, Math.round(((Math.max(300, pEnd - pStart)) / totalSpan) * chartWidth));
                  const clampedW = Math.max(24, Math.min(w, chartWidth - relX));
                  const barLabel = `${p.partitionId} ${p.attempt > 1 ? `(#${p.attempt})` : p.state === 'LOST' ? `(LOST #${p.attempt || 1})` : ''}`.trim();
                  const shownLabel = fitBarLabel(barLabel, clampedW);
                  const outline = outlineFor(p.state);
                  const timing = partitionTiming(p, state.sessionDetail?.tests);
                  const tooltip = [
                    `${p.moduleName || 'default'} · ${p.partitionId}${p.attempt > 1 ? ` (attempt ${p.attempt})` : ''}`,
                    `Worker ${p.workerId || '—'} · ${p.state}`,
                    `${(p.workUnits || []).length} work unit(s) · ${formatDuration(Math.max(0, pEnd - pStart))}`,
                    timing ? `Setup ${formatDuration(timing.setup)} · tests ${formatDuration(timing.tests)} · wrap-up ${formatDuration(timing.wrapUp)}` : null,
                    p.forkWorkspace ? 'Forked JVM workspace' : 'In worker JVM',
                    p.errorMessage ? `Error: ${p.errorMessage}` : null
                  ].filter(Boolean).join('\n');
                  return html`
                    <g class="waterfall-bar" onClick=${() => { state.selectedPartition = p; renderApp(); }}>
                      <title>${tooltip}</title>
                      <rect x=${x} y=${y + 8} width=${clampedW} height=${rowHeight - 16} rx="6"
                        fill=${moduleColor(modules, p.moduleName)}
                        fill-opacity=${p.state === 'RUNNING' || p.state === 'ASSIGNED' ? 0.55 : 1}
                        stroke=${outline.stroke} stroke-width=${outline.width} stroke-dasharray=${outline.dash} />
                      ${shownLabel ? html`<text
                        x=${x + 8}
                        y=${y + 26}
                        font-size="11"
                        font-weight="700"
                        fill="#ffffff"
                        pointer-events="none"
                      >${shownLabel}</text>` : null}
                    </g>
                  `;
                })}
              </g>
            `;
          })}
        </svg>
      </div>
    </div>
  `;
}

function collectPartitionFailures(partitions) {
  const latestById = new Map();
  for (const p of partitions) {
    const prev = latestById.get(p.partitionId);
    if (!prev || (p.attempt || 1) >= (prev.attempt || 1)) {
      latestById.set(p.partitionId, p);
    }
  }
  const out = [];
  for (const p of latestById.values()) {
    if (p.state === 'FAILED' || p.state === 'LOST') {
      out.push(p);
    }
  }
  return out;
}

function collectLatestTestFailures(tests) {
  const latestByIdentity = new Map();
  for (const t of tests) {
    const key = `${t.workUnitKey || t.className || ''}#${t.className || ''}#${t.testName || ''}`;
    const prev = latestByIdentity.get(key);
    if (!prev || (t.attempt || 1) >= (prev.attempt || 1)) {
      latestByIdentity.set(key, t);
    }
  }
  const out = [];
  for (const t of latestByIdentity.values()) {
    if (t.status === 'FAILED') {
      out.push(t);
    }
  }
  return out;
}

function renderSessionDetail() {
  if (!state.sessionDetail || !state.sessionDetail.session) {
    return html`
      <div class="card">
        <div class="empty-state">Select a build session from Overview or Build Sessions to inspect its waterfall timeline.</div>
      </div>
    `;
  }

  const { session, changedFiles = [], partitions = [], tests = [] } = state.sessionDetail;
  const failures = collectLatestTestFailures(tests);
  const partitionFailures = collectPartitionFailures(partitions);
  const totalFailureItems = failures.length + partitionFailures.length;
  const flakyRecoveredCount = new Set(
    tests.filter(t => t.flaky).map(t => `${t.className || ''}#${t.testName || ''}`)
  ).size;
  const cachePct = session.totalBlobs > 0 ? Math.round((session.cachedBlobs / session.totalBlobs) * 100) : 0;
  const speedup = (session.serialDurationMs > 0 && session.wallDurationMs > 0)
    ? `${(session.serialDurationMs / session.wallDurationMs).toFixed(1)}x`
    : '—';
  const wallValue = session.wallDurationMs > 0
    ? formatDuration(session.wallDurationMs)
    : (session.status === 'RUNNING' ? `Live · ${formatDuration(Math.max(0, Date.now() - session.startedAt))}` : '—');

  const testFilterPills = ['ALL', 'FAILED', 'FLAKY', 'PASSED', 'SKIPPED'];
  const tq = state.testQuery.trim().toLowerCase();
  const filteredTests = tests.filter(t => {
    if (state.testStatusFilter === 'FLAKY' && !t.flaky) return false;
    if (state.testStatusFilter !== 'ALL' && state.testStatusFilter !== 'FLAKY' && t.status !== state.testStatusFilter) return false;
    if (!tq) return true;
    return (t.className || '').toLowerCase().includes(tq)
      || (t.testName || '').toLowerCase().includes(tq)
      || (t.workerId || '').toLowerCase().includes(tq)
      || (t.partitionId || '').toLowerCase().includes(tq)
      || (t.endpoints || []).some(ep => ep.toLowerCase().includes(tq));
  });

  return html`
    <div>
      <div class="page-header">
        <div>
          <div style="margin-bottom:6px">
            <button class="btn btn-sm" onClick=${() => setView('sessions')}>← All Sessions</button>
          </div>
          <h1 class="page-title">
            <span class="mono">${session.sessionId}</span>
            ${statusBadge(session.status)}
            ${speedup !== '—' ? html`<span class="speedup-badge">${speedup} speedup</span>` : null}
          </h1>
          <p class="page-subtitle">
            Project <strong>${session.projectName || 'citrus'}</strong>
            ${Array.isArray(session.modules) && session.modules.length > 0 ? html` · <span class="session-modules" title=${session.modules.join('\n')}>${modulesLabel(session)}</span>` : null}${' '}· Branch <strong class="mono">${session.gitBranch || 'local'}</strong> · Commit <code class="mono">${session.gitCommit || 'HEAD'}</code> · Started ${formatAgo(session.startedAt)}
          </p>
        </div>

        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          ${session.status === 'RUNNING' ? html`
            <button
              class="btn btn-danger"
              onClick=${() => handleAbortSession(session.sessionId)}
              title="Cancel active worker partitions and abort this build session"
            >✕ Abort Session</button>
          ` : null}
          <select
            class="search-input"
            style="min-width:220px"
            aria-label="Switch build session"
            value=${session.sessionId}
            onChange=${e => loadSessionDetail(e.target.value, false)}
          >
            ${state.sessions.map(s => html`
              <option value=${s.sessionId}>${s.sessionId} (${s.status})</option>
            `)}
          </select>
        </div>
      </div>

      ${session.failureReason && (session.status === 'FAILED' || session.status === 'ABORTED') ? html`
        <div
          class=${`alert-banner session-failure-banner ${session.status === 'FAILED' ? 'alert-banner-rose' : 'alert-banner-amber'}`}
          role="alert"
        >
          <div class="session-failure-title">${session.status === 'FAILED' ? 'Why this session failed' : 'Why this session was aborted'}</div>
          <pre class="session-failure-reason">${session.failureReason}</pre>
        </div>
      ` : null}

      <div class="kpi-grid">
        <div class="kpi-card accent-emerald">
          <div class="kpi-label">Passed Tests</div>
          <div class="kpi-value">${session.passedTests}</div>
          <div class="kpi-meta">of ${session.totalTests} total tests</div>
        </div>
        <div class="kpi-card accent-rose">
          <div class="kpi-label">Failed Tests</div>
          <div class="kpi-value">${session.failedTests}</div>
          <div class="kpi-meta">${session.skippedTests} skipped${partitionFailures.length > 0 ? ` · ${partitionFailures.length} failed partition(s)` : ''}</div>
        </div>
        <div class="kpi-card accent-amber">
          <div class="kpi-label">Flaky Recovered</div>
          <div class="kpi-value">${flakyRecoveredCount}</div>
          <div class="kpi-meta">Passed after partition retry</div>
        </div>
        <div class="kpi-card accent-blue">
          <div class="kpi-label">Wall vs Serial Time</div>
          <div class="kpi-value">${wallValue}</div>
          <div class="kpi-meta">Serial sum: ${formatDuration(session.serialDurationMs)}</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Blob Cache Hit</div>
          <div class="kpi-value">${cachePct}%</div>
          <div class="kpi-meta">${session.cachedBlobs}/${session.totalBlobs} blobs cached</div>
        </div>
      </div>

      <div class="subtabs">
        <button class=${`subtab-btn ${state.detailTab === 'waterfall' ? 'active' : ''}`} onClick=${() => setView('session-detail', 'waterfall')}>
          Waterfall & Partitions (${partitions.length})
        </button>
        <button class=${`subtab-btn ${state.detailTab === 'tests' ? 'active' : ''}`} onClick=${() => setView('session-detail', 'tests')}>
          Test Executions (${tests.length})
        </button>
        <button class=${`subtab-btn ${state.detailTab === 'failures' ? 'active' : ''}`} onClick=${() => setView('session-detail', 'failures')}>
          Failing Tests (${totalFailureItems})
        </button>
        <button class=${`subtab-btn ${state.detailTab === 'files' ? 'active' : ''}`} onClick=${() => setView('session-detail', 'files')}>
          Changed Files (${changedFiles.length})
        </button>
      </div>

      ${state.detailTab === 'waterfall' ? html`
        <div>
          <div class="card">
            <div class="card-header">
              <h2 class="card-title">Worker Swimlane Gantt Waterfall</h2>
            </div>
            <div class="card-body">
              ${renderWaterfallSvg(session, partitions)}
            </div>
          </div>

          <div class="card">
            <div class="card-header">
              <h2 class="card-title">Partition Breakdown</h2>
            </div>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Partition</th>
                    <th>Worker</th>
                    <th>State</th>
                    <th>Attempt</th>
                    <th>Work Units</th>
                    <th>Capabilities & Resources</th>
                    <th>Duration</th>
                  </tr>
                </thead>
                <tbody>
                  ${paginate('partitions', partitions, 25, state.selectedSessionId).items.map(p => {
                    const dur = (p.startedAt > 0 && p.finishedAt > 0)
                      ? formatDuration(p.finishedAt - p.startedAt)
                      : (p.state === 'RUNNING' ? 'running…' : formatDuration(p.estimatedDurationMs));
                    const caps = Object.entries(p.requiredCapabilities || {});
                    const inCount = (p.inputPaths || []).length;
                    const outCount = (p.producedOutputs || []).length || (p.outputPaths || []).length;
                    return html`
                      <tr class="clickable-row" onClick=${() => { state.selectedPartition = p; renderApp(); }}>
                        <td>
                          <div class="mono" style="font-weight:700;color:var(--citrus-orange-deep)">${p.partitionId}</div>
                          ${p.moduleName ? html`<div class="mono" style="font-size:11px;color:var(--text-muted)">${p.moduleName}</div>` : null}
                        </td>
                        <td class="mono">${p.workerId || 'pending'}</td>
                        <td>
                          ${statusBadge(p.state)}
                          ${p.errorMessage ? html`<div style="font-size:11px;color:var(--rose-text);margin-top:3px;max-width:260px">${p.errorMessage}</div>` : null}
                        </td>
                        <td class="mono">#${p.attempt}</td>
                        <td>${(p.workUnits || []).length} units</td>
                        <td>
                          ${caps.length === 0 && !p.forkWorkspace && inCount === 0 && outCount === 0
                            ? html`<span style="color:var(--text-muted)">default</span>`
                            : html`
                                ${caps.map(([k, v]) => html`<span class="tag mono">${k}=${v}</span>`)}
                                ${p.forkWorkspace ? html`<span class="tag tag-resource mono">forked-workspace</span>` : null}
                                ${inCount > 0 ? html`<span class="tag tag-resource mono">${inCount} input(s)</span>` : null}
                                ${outCount > 0 ? html`<span class="tag tag-resource mono">${outCount} output(s)</span>` : null}
                              `}
                        </td>
                        <td class="mono">${dur}</td>
                      </tr>
                    `;
                  })}
                </tbody>
              </table>
            </div>
            ${paginate('partitions', partitions, 25, state.selectedSessionId).controls}
          </div>
        </div>
      ` : null}

      ${state.detailTab === 'tests' ? html`
        <div>
          <div class="toolbar">
            <input
              class="search-input"
              data-focus-id="detail-tests-search"
              type="search"
              placeholder="Search class, test method, worker, partition, or Citrus endpoint…"
              value=${state.testQuery}
              onInput=${e => { state.testQuery = e.target.value; renderApp(); }}
            />
            <div class="pill-group">
              ${testFilterPills.map(st => html`
                <button
                  class=${`filter-pill ${state.testStatusFilter === st ? 'active' : ''}`}
                  onClick=${() => { state.testStatusFilter = st; renderApp(); }}
                >${st}</button>
              `)}
            </div>
          </div>

          <div class="card">
            ${filteredTests.length === 0
              ? html`<div class="empty-state">No test executions match the active filter.</div>`
              : html`
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Status</th>
                        <th>Test Case</th>
                        <th>Worker / Partition</th>
                        <th>Citrus Endpoints</th>
                        <th>Attempt</th>
                        <th>Duration</th>
                      </tr>
                    </thead>
                    <tbody>
                      ${paginate('tests', filteredTests, 50, `${state.selectedSessionId}|${state.testStatusFilter}|${tq}`).items.map(t => html`
                        <tr class="clickable-row" onClick=${() => { state.selectedTestExecution = t; renderApp(); }}>
                          <td>
                            ${statusBadge(t.status)}
                            ${t.flaky ? html` <span class="badge badge-flaky">FLAKY #${t.attempt}</span>` : null}
                          </td>
                          <td>
                            <div style="font-weight:700">${t.testName}</div>
                            <div class="mono" style="font-size:11.5px;color:var(--text-muted)">${t.className}</div>
                          </td>
                          <td>
                            <div class="mono" style="font-size:12px">${t.workerId || 'local'}</div>
                            <div class="mono" style="font-size:11px;color:var(--text-muted)">${t.partitionId || '—'}</div>
                          </td>
                          <td>
                            ${(t.endpoints || []).length === 0
                              ? html`<span style="color:var(--text-muted)">—</span>`
                              : (t.endpoints || []).map(ep => html`<span class="tag tag-endpoint mono">${ep}</span>`)}
                          </td>
                          <td class="mono">#${t.attempt}</td>
                          <td class="mono">${formatDuration(t.durationMs)}</td>
                        </tr>
                      `)}
                    </tbody>
                  </table>
                </div>
                ${paginate('tests', filteredTests, 50, `${state.selectedSessionId}|${state.testStatusFilter}|${tq}`).controls}
              `}
          </div>
        </div>
      ` : null}

      ${state.detailTab === 'failures' ? html`
        <div>
          ${totalFailureItems === 0
            ? html`<div class="card"><div class="empty-state">Zero failing tests or partitions in this build session.</div></div>`
            : html`
                ${partitionFailures.map(p => html`
                  <div class="failure-card">
                    <div class="failure-card-header">
                      <div>
                        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
                          ${statusBadge(p.state)}
                          <strong class="mono" style="font-size:15px">Partition ${p.partitionId} (attempt #${p.attempt})</strong>
                          <span class="mono" style="font-size:12px;color:var(--text-muted)">on ${p.workerId || 'unassigned'} · ${(p.workUnits || []).length} work unit(s)</span>
                        </div>
                        <div style="margin-top:6px;font-weight:600;color:var(--rose-text)">
                          ${p.errorMessage || `Partition ended in ${p.state} state without completing all work units.`}
                        </div>
                      </div>
                      <button class="btn btn-sm" onClick=${() => { state.selectedPartition = p; renderApp(); }}>Inspect Partition</button>
                    </div>
                  </div>
                `)}
                ${failures.map(f => html`
                  <div class="failure-card">
                    <div class="failure-card-header">
                      <div>
                        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
                          ${statusBadge('FAILED')}
                          <strong style="font-size:15px">${shortClass(f.className)}#${f.testName}</strong>
                          <span class="mono" style="font-size:12px;color:var(--text-muted)">on ${f.workerId} (${f.partitionId} #${f.attempt}) · ${formatDuration(f.durationMs)}</span>
                        </div>
                        <div style="margin-top:6px;font-weight:600;color:var(--rose-text)">${f.errorMessage || 'Assertion / validation failure'}</div>
                        <div style="margin-top:6px">
                          ${(f.endpoints || []).map(ep => html`<span class="tag tag-endpoint mono">endpoint: ${ep}</span>`)}
                        </div>
                      </div>
                      <button
                        class="btn btn-sm"
                        onClick=${() => {
                          if (navigator.clipboard && (f.stackTrace || f.errorMessage)) {
                            navigator.clipboard.writeText(f.stackTrace || f.errorMessage);
                            showToast('Copied stack trace to clipboard');
                          }
                        }}
                      >Copy Trace</button>
                    </div>
                    <pre class="stacktrace-box">${f.stackTrace || f.errorMessage || 'No stack trace captured.'}</pre>
                  </div>
                `)}
              `}
        </div>
      ` : null}

      ${state.detailTab === 'files' ? html`
        <div class="card">
          <div class="card-header">
            <h2 class="card-title">Git Changed Workspace Files (${changedFiles.length})</h2>
          </div>
          <div class="card-body">
            ${changedFiles.length === 0
              ? html`<div class="empty-state">Clean workspace — no modified files recorded.</div>`
              : html`
                <div style="display:flex;flex-direction:column;gap:8px">
                  ${changedFiles.map(fp => html`
                    <div class="mono" style="padding:8px 12px;border-radius:8px;background:var(--bg-subtle);font-size:12.5px">
                      Δ ${fp}
                    </div>
                  `)}
                </div>
              `}
          </div>
        </div>
      ` : null}
    </div>
  `;
}

function formatBytes(bytes) {
  if (typeof bytes !== 'number' || !Number.isFinite(bytes) || bytes < 0) return 'N/A';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit++;
  }
  return unit === 0 ? `${Math.round(value)} B` : `${value.toFixed(1)} ${units[unit]}`;
}

/** An OFFLINE worker keeps its last reported cache stats, but no longer counts as low on disk. */
function isLowDisk(w) {
  return !!(w && w.state !== 'OFFLINE' && w.cacheStats && w.cacheStats.lowDiskSpace);
}

/** Usable disk as a percentage of the free-space buffer, clamped to 0-100; an unknown disk or no buffer reads as full. */
function cacheDiskPercent(cacheStats) {
  const buffer = Number(cacheStats.freeSpaceBufferBytes);
  const usable = Number(cacheStats.usableDiskBytes);
  if (!Number.isFinite(buffer) || buffer <= 0) return 100;
  if (!Number.isFinite(usable) || usable < 0) return 0;
  return Math.max(0, Math.min(100, Math.round((usable / buffer) * 100)));
}

function renderWorkerCacheTelemetry(w) {
  const c = w.cacheStats;
  if (!c) {
    return html`<div class="worker-cache-panel worker-cache-empty">Local file cache: no telemetry reported</div>`;
  }
  const pct = cacheDiskPercent(c);
  const usableLabel = Number(c.usableDiskBytes) < 0 || !Number.isFinite(Number(c.usableDiskBytes))
    ? 'N/A'
    : formatBytes(Number(c.usableDiskBytes));
  const bufferLabel = Number(c.freeSpaceBufferBytes) > 0 ? formatBytes(Number(c.freeSpaceBufferBytes)) : 'none';
  return html`
    <div class=${`worker-cache-panel ${c.lowDiskSpace ? 'low-disk' : ''}`}>
      <div class="worker-cache-head">
        <span class="worker-cache-title">Local File Cache</span>
        <span>
          <span class=${`badge ${c.cleanupEnabled ? 'badge-idle' : 'badge-offline'}`}>${c.cleanupEnabled ? 'AUTO LRU' : 'NO CLEANUP'}</span>
          ${c.lowDiskSpace ? html` <span class="badge badge-failed">LOW DISK</span>` : null}
        </span>
      </div>
      <div class="worker-cache-row">
        <strong>${formatBytes(c.cachedBytes)}</strong> in ${Number(c.blobCount) || 0} blobs
        · evicted <strong>${Number(c.evictedBlobsTotal) || 0}</strong>
      </div>
      <div class="worker-cache-bar" title=${`Usable disk ${usableLabel} vs buffer ${bufferLabel}`}>
        <div class=${`worker-cache-bar-fill ${c.lowDiskSpace ? 'low-disk' : ''}`} style=${`width:${pct}%`}></div>
      </div>
      <div class="worker-cache-row worker-cache-muted">Usable disk ${usableLabel} · buffer ${bufferLabel}</div>
    </div>
  `;
}

function renderWorkersPool() {
  const states = ['ALL', 'IDLE', 'BUSY', 'DRAINING', 'OFFLINE', 'LOW_DISK'];
  const q = state.workerQuery.trim().toLowerCase();
  const filtered = state.workers.filter(w => {
    if (state.workerStateFilter === 'LOW_DISK') {
      if (!isLowDisk(w)) return false;
    } else if (state.workerStateFilter !== 'ALL' && w.state !== state.workerStateFilter) return false;
    if (!q) return true;
    const capStr = Object.entries(w.capabilities || {}).map(([k, v]) => `${k}=${v}`).join(' ').toLowerCase();
    return (w.workerId || '').toLowerCase().includes(q)
      || (w.host || '').toLowerCase().includes(q)
      || capStr.includes(q);
  });

  return html`
    <div>
      <div class="page-header">
        <div>
          <h1 class="page-title">Workers Pool</h1>
          <p class="page-subtitle">Connected Citrus remote worker agents, host capability tags, and active partition assignments.</p>
        </div>
      </div>

      <div class="toolbar">
        <input
          class="search-input"
          data-focus-id="workers-search"
          type="search"
          placeholder="Filter by worker ID, host, or capability (e.g. gpu, docker, macos)…"
          value=${state.workerQuery}
          onInput=${e => { state.workerQuery = e.target.value; renderApp(); }}
        />
        <div class="pill-group">
          ${states.map(st => html`
            <button
              class=${`filter-pill ${state.workerStateFilter === st ? 'active' : ''}`}
              onClick=${() => { state.workerStateFilter = st; renderApp(); }}
            >${st}</button>
          `)}
        </div>
      </div>

      ${filtered.length === 0
        ? html`<div class="card"><div class="empty-state">No workers match the current filter.</div></div>`
        : html`
          <div class="workers-grid">
            ${filtered.map(w => html`
              <div class="worker-card">
                <div class="worker-card-header">
                  <div>
                    <div class="mono" style="font-size:15px;font-weight:700">${w.workerId}</div>
                    <div class="mono" style="font-size:12px;color:var(--text-muted)">${w.host}</div>
                  </div>
                  ${statusBadge(w.state)}
                </div>

                <div style="font-size:12.5px;color:var(--text-secondary)">
                  Last heartbeat: <strong>${formatAgo(w.lastHeartbeatAt)}</strong>
                  ${w.activePartitionId ? html` · Active: <span class="mono" style="color:var(--citrus-orange-deep);font-weight:700">${w.activePartitionId}</span>` : null}
                </div>

                <div>
                  ${Object.entries(w.capabilities || {}).map(([k, v]) => html`
                    <span class="tag mono">${k}=${v}</span>
                  `)}
                </div>

                ${renderWorkerCacheTelemetry(w)}
              </div>
            `)}
          </div>
        `}
    </div>
  `;
}

function renderTestHistory() {
  const q = state.historyQuery.trim().toLowerCase();
  const filtered = state.history.filter(h => {
    if (state.historyFlakyOnly && (!h.flakyRuns || h.flakyRuns <= 0)) return false;
    if (!q) return true;
    return (h.className || '').toLowerCase().includes(q)
      || (h.testName || '').toLowerCase().includes(q)
      || (h.workUnitKey || '').toLowerCase().includes(q);
  });

  return html`
    <div>
      <div class="page-header">
        <div>
          <h1 class="page-title">Test History & Flakiness Telemetry</h1>
          <p class="page-subtitle">Historical LPT duration estimates, pass rates, and retry-recovered flaky tests across sessions.</p>
        </div>
      </div>

      <div class="toolbar">
        <input
          class="search-input"
          data-focus-id="history-search"
          type="search"
          placeholder="Filter by test class or method…"
          value=${state.historyQuery}
          onInput=${e => { state.historyQuery = e.target.value; renderApp(); }}
        />
        <button
          class=${`btn ${state.historyFlakyOnly ? 'btn-primary' : ''}`}
          onClick=${() => { state.historyFlakyOnly = !state.historyFlakyOnly; renderApp(); }}
        >
          ⚡ Flaky Only
        </button>
      </div>

      <div class="card">
        ${filtered.length === 0
          ? html`<div class="empty-state">No historical test records match the filter.</div>`
          : html`
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Test Case</th>
                    <th>Runs</th>
                    <th>Pass Rate</th>
                    <th>Flaky Recoveries</th>
                    <th>Avg Duration</th>
                    <th>Max Duration</th>
                    <th>Last Outcome</th>
                  </tr>
                </thead>
                <tbody>
                  ${paginate('history', filtered, 25, `${state.historyFlakyOnly}|${q}`).items.map(h => {
                    const passedRuns = typeof h.passedRuns === 'number'
                      ? h.passedRuns
                      : Math.max(0, (h.totalRuns || 0) - (h.failedRuns || 0));
                    const rate = h.totalRuns > 0 ? Math.round((passedRuns / h.totalRuns) * 100) : 0;
                    const maxMs = h.maxDurationMs > 0 ? h.maxDurationMs : h.avgDurationMs;
                    return html`
                      <tr>
                        <td>
                          <div style="font-weight:700">${h.testName}</div>
                          <div class="mono" style="font-size:11.5px;color:var(--text-muted)">${h.className}</div>
                        </td>
                        <td class="mono">${h.totalRuns}</td>
                        <td>
                          <div style="display:flex;align-items:center;gap:8px">
                            <div class="progress-mini">
                              <span class="progress-seg-pass" style=${`width:${rate}%`}></span>
                              <span class="progress-seg-fail" style=${`width:${100 - rate}%`}></span>
                            </div>
                            <span class="mono" style="font-size:12px;font-weight:700">${rate}%</span>
                          </div>
                        </td>
                        <td>
                          ${h.flakyRuns > 0
                            ? html`<span class="badge badge-flaky">${h.flakyRuns} flaky</span>`
                            : html`<span style="color:var(--text-muted)">0</span>`}
                        </td>
                        <td class="mono">${formatDuration(h.avgDurationMs)}</td>
                        <td class="mono">${formatDuration(maxMs)}</td>
                        <td>${statusBadge(h.lastStatus)}</td>
                      </tr>
                    `;
                  })}
                </tbody>
              </table>
            </div>
            ${paginate('history', filtered, 25, `${state.historyFlakyOnly}|${q}`).controls}
          `}
      </div>
    </div>
  `;
}

function renderModals() {
  if (state.selectedPartition) {
    const p = state.selectedPartition;
    const partTests = (state.sessionDetail?.tests || []).filter(
      t => t.partitionId === p.partitionId && (!t.attempt || t.attempt === p.attempt)
    );
    const inputPaths = p.inputPaths || [];
    const outputPaths = p.outputPaths || [];
    const producedOutputs = p.producedOutputs || [];
    const hasWorkspaceDetails = Boolean(
      p.moduleName
      || p.forkWorkspace
      || (p.workingDirectory && p.workingDirectory !== '.')
      || inputPaths.length > 0
      || outputPaths.length > 0
      || producedOutputs.length > 0
    );

    return html`
      <div class="modal-backdrop" onClick=${() => { state.selectedPartition = null; renderApp(); }}>
        <div class="modal-panel" onClick=${e => e.stopPropagation()}>
          <div class="modal-header">
            <div>
              <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
                <strong class="mono" style="font-size:16px">${p.partitionId}</strong>
                ${statusBadge(p.state)}
                <span class="tag mono">attempt #${p.attempt}</span>
                ${p.forkWorkspace ? html`<span class="tag tag-resource mono">forked-workspace</span>` : null}
              </div>
              <div style="font-size:12px;color:var(--text-muted);margin-top:3px">
                Assigned worker: <strong class="mono">${p.workerId || 'unassigned'}</strong> · Est. ${formatDuration(p.estimatedDurationMs)}
              </div>
            </div>
            <button class="btn btn-sm" onClick=${() => { state.selectedPartition = null; renderApp(); }}>✕ Close</button>
          </div>
          <div class="modal-body">
            ${(() => {
              const timing = partitionTiming(p, partTests);
              if (!timing) return null;
              const pct = v => `${Math.max(0, Math.min(100, (v / Math.max(1, timing.total)) * 100)).toFixed(1)}%`;
              return html`
                <h3 style="margin:0 0 8px;font-size:13px;text-transform:uppercase;color:var(--text-muted)">Where the time went</h3>
                <div class="partition-timing">
                  <div class="partition-timing-bar">
                    <span class="timing-setup" style=${`width:${pct(timing.setup)}`}></span>
                    <span class="timing-tests" style=${`width:${pct(timing.tests)}`}></span>
                    <span class="timing-wrapup" style=${`width:${pct(timing.wrapUp)}`}></span>
                  </div>
                  <div class="partition-timing-legend">
                    <span><span class="timing-swatch timing-setup"></span> Setup ${formatDuration(timing.setup)}</span>
                    <span><span class="timing-swatch timing-tests"></span> Tests ${formatDuration(timing.tests)} (sum of test times ${formatDuration(timing.testTime)})</span>
                    <span><span class="timing-swatch timing-wrapup"></span> Wrap-up ${formatDuration(timing.wrapUp)}</span>
                  </div>
                  <div style="font-size:12px;color:var(--text-muted);margin-top:6px">
                    Setup is the worker preparing the partition before its first test: fetching and unpacking the
                    classpath, starting a JVM or class loader, and bootstrapping JUnit and the test framework.
                  </div>
                </div>
              `;
            })()}
            ${p.errorMessage ? html`
              <div class=${`alert-banner ${p.state === 'FAILED' ? 'alert-banner-rose' : 'alert-banner-amber'}`}>
                ${p.errorMessage}
              </div>
            ` : null}

            <h3 style="margin:0 0 8px;font-size:13px;text-transform:uppercase;color:var(--text-muted)">Required Capabilities</h3>
            <div style="margin-bottom:16px">
              ${Object.entries(p.requiredCapabilities || {}).length === 0
                ? html`<span style="color:var(--text-muted)">No special placement constraints</span>`
                : Object.entries(p.requiredCapabilities).map(([k, v]) => html`<span class="tag mono">${k}=${v}</span>`)}
            </div>

            ${hasWorkspaceDetails ? html`
              <div style="margin-bottom:16px">
                <h3 style="margin:0 0 8px;font-size:13px;text-transform:uppercase;color:var(--text-muted)">Workspace & External Test Resources</h3>
                <div class="kv-grid">
                  <div class="kv-box">
                    <div class="kv-label">Module</div>
                    <div class="kv-val mono">${p.moduleName || '—'}</div>
                  </div>
                  <div class="kv-box">
                    <div class="kv-label">Isolation Mode</div>
                    <div class="kv-val">${p.forkWorkspace ? 'Forked JVM + Workspace' : 'In-Process ClassLoader'}</div>
                  </div>
                  <div class="kv-box">
                    <div class="kv-label">Working Directory</div>
                    <div class="kv-val mono">${p.workingDirectory || '.'}</div>
                  </div>
                </div>
                ${inputPaths.length > 0 ? html`
                  <div style="margin-bottom:8px">
                    <div style="font-size:11.5px;font-weight:700;color:var(--text-muted);margin-bottom:4px">Materialized Inputs (${inputPaths.length})</div>
                    <div>${inputPaths.map(ip => html`<span class="tag mono">${ip}</span>`)}</div>
                  </div>
                ` : null}
                ${outputPaths.length > 0 ? html`
                  <div style="margin-bottom:8px">
                    <div style="font-size:11.5px;font-weight:700;color:var(--text-muted);margin-bottom:4px">Declared Outputs (${outputPaths.length})</div>
                    <div>${outputPaths.map(op => html`<span class="tag mono">${op}</span>`)}</div>
                  </div>
                ` : null}
                ${producedOutputs.length > 0 ? html`
                  <div>
                    <div style="font-size:11.5px;font-weight:700;color:var(--emerald-text);margin-bottom:4px">Collected Outputs (${producedOutputs.length})</div>
                    <div>${producedOutputs.map(op => html`<span class="tag tag-resource mono">${op}</span>`)}</div>
                  </div>
                ` : null}
              </div>
            ` : null}

            <h3 style="margin:0 0 8px;font-size:13px;text-transform:uppercase;color:var(--text-muted)">Scheduled Work Units (${(p.workUnits || []).length})</h3>
            <div style="display:flex;flex-direction:column;gap:6px;margin-bottom:16px">
              ${(p.workUnits || []).map(u => html`
                <div style="padding:8px 12px;border-radius:8px;background:var(--bg-subtle);display:flex;justify-content:space-between;gap:8px">
                  <span class="mono" style="font-size:12.5px">${u.key}</span>
                  <span class="mono" style="font-size:12px;color:var(--text-muted)">est. ${formatDuration(u.estimatedDurationMs)}</span>
                </div>
              `)}
            </div>

            ${partTests.length > 0 ? html`
              <div>
                <h3 style="margin:0 0 8px;font-size:13px;text-transform:uppercase;color:var(--text-muted)">Recorded Test Outcomes (${partTests.length})</h3>
                <div style="display:flex;flex-direction:column;gap:6px">
                  ${partTests.map(t => html`
                    <div style="padding:8px 12px;border-radius:8px;border:1px solid var(--border-color);display:flex;justify-content:space-between;align-items:center">
                      <div>
                        <div style="font-weight:600">${t.testName}</div>
                        <div class="mono" style="font-size:11px;color:var(--text-muted)">${shortClass(t.className)}</div>
                      </div>
                      <div style="display:flex;align-items:center;gap:8px">
                        ${t.flaky ? html`<span class="badge badge-flaky">FLAKY</span>` : null}
                        ${statusBadge(t.status)}
                        <span class="mono" style="font-size:12px">${formatDuration(t.durationMs)}</span>
                      </div>
                    </div>
                  `)}
                </div>
              </div>
            ` : null}
          </div>
        </div>
      </div>
    `;
  }

  if (state.selectedTestExecution) {
    const t = state.selectedTestExecution;
    return html`
      <div class="modal-backdrop" onClick=${() => { state.selectedTestExecution = null; renderApp(); }}>
        <div class="modal-panel" onClick=${e => e.stopPropagation()}>
          <div class="modal-header">
            <div>
              <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
                ${statusBadge(t.status)}
                ${t.flaky ? html`<span class="badge badge-flaky">FLAKY #${t.attempt}</span>` : null}
                <strong style="font-size:15px">${t.testName}</strong>
              </div>
              <div class="mono" style="font-size:12px;color:var(--text-muted);margin-top:3px">${t.className}</div>
            </div>
            <button class="btn btn-sm" onClick=${() => { state.selectedTestExecution = null; renderApp(); }}>✕ Close</button>
          </div>
          <div class="modal-body">
            <div style="margin-bottom:12px;font-size:13px">
              Worker: <strong class="mono">${t.workerId}</strong> · Partition: <strong class="mono">${t.partitionId}</strong> (#${t.attempt}) · Duration: <strong class="mono">${formatDuration(t.durationMs)}</strong>
            </div>
            <div style="margin-bottom:14px">
              ${(t.endpoints || []).map(ep => html`<span class="tag tag-endpoint mono">endpoint: ${ep}</span>`)}
            </div>
            ${t.errorMessage ? html`<div style="margin-bottom:10px;font-weight:600;color:var(--rose-text)">${t.errorMessage}</div>` : null}
            ${t.stackTrace ? html`<pre class="stacktrace-box">${t.stackTrace}</pre>` : html`<div style="color:var(--text-muted)">No failure stack trace recorded for this test execution.</div>`}
          </div>
        </div>
      </div>
    `;
  }

  if (state.showAuthModal) {
    return html`
      <div class="modal-backdrop" onClick=${() => { state.showAuthModal = false; renderApp(); }}>
        <div class="modal-panel" style="max-width:440px" onClick=${e => e.stopPropagation()}>
          <div class="modal-header">
            <strong style="font-size:16px">Broker Bearer Authentication</strong>
            <button class="btn btn-sm" onClick=${() => { state.showAuthModal = false; renderApp(); }}>✕</button>
          </div>
          <div class="modal-body">
            ${state.authError ? html`<div style="margin-bottom:12px;padding:8px 12px;border-radius:8px;background:var(--rose-bg);color:var(--rose-text);font-weight:600">${state.authError}</div>` : null}
            <label style="display:block;font-size:12.5px;font-weight:600;margin-bottom:6px">Shared Bearer Token</label>
            <input
              id="auth-token-input"
              data-focus-id="auth-token-input"
              class="search-input"
              style="width:100%;margin-bottom:16px"
              type="password"
              placeholder="Enter broker --auth-token value…"
              value=${state.authToken}
              onInput=${e => { state.authToken = e.target.value; }}
            />
            <div style="display:flex;justify-content:flex-end;gap:8px">
              <button
                class="btn"
                onClick=${() => {
                  state.authToken = '';
                  localStorage.removeItem(STORAGE_TOKEN_KEY);
                  state.showAuthModal = false;
                  connectSse();
                  refreshAll();
                }}
              >Clear</button>
              <button
                class="btn btn-primary"
                onClick=${() => {
                  localStorage.setItem(STORAGE_TOKEN_KEY, state.authToken.trim());
                  state.authToken = state.authToken.trim();
                  state.showAuthModal = false;
                  state.authError = '';
                  connectSse();
                  refreshAll();
                }}
              >Save & Connect</button>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  return null;
}

function renderMainView() {
  switch (state.view) {
    case 'sessions': return renderSessionsExplorer();
    case 'session-detail': return renderSessionDetail();
    case 'workers': return renderWorkersPool();
    case 'history': return renderTestHistory();
    default: return renderOverview();
  }
}

export function renderApp() {
  const root = document.getElementById('app');
  if (!root) return;

  // Preserve focused input and cursor position across DOM replacement
  const active = document.activeElement;
  const focusId = active && active.getAttribute ? active.getAttribute('data-focus-id') : null;
  const selStart = active && typeof active.selectionStart === 'number' ? active.selectionStart : null;
  const selEnd = active && typeof active.selectionEnd === 'number' ? active.selectionEnd : null;

  const tree = html`
    <div class="app-layout">
      ${renderTopbar()}
      <div class="shell">
        ${renderSidebar()}
        <main class="main-content">
          ${renderMainView()}
        </main>
      </div>
      ${renderModals()}
      ${state.toastMessage ? html`<div class="toast">${state.toastMessage}</div>` : null}
    </div>
  `;

  root.replaceChildren(tree);

  if (focusId) {
    const nextInput = root.querySelector(`[data-focus-id="${focusId}"]`);
    if (nextInput) {
      nextInput.focus();
      if (selStart !== null && typeof nextInput.setSelectionRange === 'function') {
        try {
          nextInput.setSelectionRange(selStart, selEnd);
        } catch (_) {
          // Ignore for input types without selection range
        }
      }
    }
  }
}

window.addEventListener('hashchange', () => {
  if (suppressHashChange) return;
  const prevSid = state.selectedSessionId;
  if (applyHashFromLocation()) {
    if (state.view === 'session-detail'
        && state.selectedSessionId
        && (state.selectedSessionId !== prevSid || state.sessionDetail?.session?.sessionId !== state.selectedSessionId)) {
      loadSessionDetail(state.selectedSessionId, false);
      return;
    }
    renderApp();
  }
});

window.addEventListener('keydown', e => {
  if (e.key === 'Escape' && (state.selectedPartition || state.selectedTestExecution || state.showAuthModal)) {
    state.selectedPartition = null;
    state.selectedTestExecution = null;
    state.showAuthModal = false;
    renderApp();
  }
});

applyHashFromLocation();
renderApp();
ensurePollingLoop();
connectSse();
refreshAll();
