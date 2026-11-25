const $ = (id) => document.getElementById(id);
const fmtBytes = (n) => {
  if (!n && n !== 0) return '-';
  const u = ['B','KiB','MiB','GiB']; let i=0; let x=Number(n);
  while (x >= 1024 && i < u.length-1) { x/=1024; i++; }
  return `${x.toFixed(i === 0 ? 0 : 1)} ${u[i]}`;
};
const fmtNum = (n) => (n || 0).toLocaleString('zh-CN');
const fmtTime = (s) => new Date(s).toLocaleTimeString('zh-CN', {hour12:false});
const shortSeq = (n) => n >= 1048576 ? `${(n/1048576).toFixed(2)}M` : n >= 1024 ? `${(n/1024).toFixed(1)}K` : `${n}`;
let lastAck = {t: 0, v: 0};

function renderMetrics(s) {
  const now = performance.now();
  let rate = 0;
  if (s && lastAck.t) {
    const dt = (now-lastAck.t)/1000;
    rate = Math.max(0, ((s.total_acked-lastAck.v)/dt)/1e6);
  }
  if (s) { lastAck = {t: now, v: s.total_acked}; }
  const m = [
    ['连接 DSN', s ? `${shortSeq(s.dsn_una)} / ${shortSeq(s.dsn_next)}` : '-', 'UNA/NXT'],
    ['连接在途', s ? fmtBytes(s.conn_flight_bytes) : '-', '所有子流共享窗口'],
    ['已收 / 已交付', s ? `${fmtBytes(s.total_received)} / ${fmtBytes(s.total_delivered)}` : '-', 'reorder → Read'],
    ['确认速率', rate.toFixed(1), 'MB/s 浏览器采样'],
    ['重传 / 救援', s ? `${s.retransmissions} / ${s.rescue_events}` : '-', `重注入 ${s? s.reinjections:0}`],
    ['接收窗口', s ? fmtBytes(s.local_rwnd) : '-', `远端 advertised ${s?fmtBytes(s.remote_rwnd):'-'}`],
  ];
  $('metrics').innerHTML = m.map(([k,v,u]) => `<div class="metric"><div class="k">${k}</div><div class="v">${v}</div><div class="u">${u}</div></div>`).join('');
}

function statusPill(st) {
  const el = $('statusPill');
  const s = st.snapshot || st.last_snapshot;
  if (st.running) {
    el.className = 'pill running';
    el.textContent = `运行中 · ${st.active_scheduler}`;
  } else if (s && s.mode !== 'multipath') {
    el.className = 'pill fallback'; el.textContent = '已回退单路径';
  } else {
    el.className = 'pill idle'; el.textContent = st.loop ? '循环演示间隔' : '空闲';
  }
}

function renderPaths(s) {
  $('alpha').textContent = s ? `α=${s.alpha.toFixed(4)} · Reno参考cwnd=${s.reference_cwnd_mss.toFixed(2)} MSS` : '';
  if (!s || !s.paths?.length) { $('paths').innerHTML = '<span class="hint">等待子流…</span>'; return; }
  $('paths').innerHTML = s.paths.map(p => {
    const width = Math.min(100, Math.max(3, p.cwnd_mss / 40 * 100));
    const avail = p.cwnd_bytes - p.subflight_bytes;
    return `<div class="path ${p.status}">
      <div class="path-head"><div class="path-name">路径 ${p.path_id} · 子流 ${p.subflow_id}</div><span class="badge ${p.status}">${p.status}</span></div>
      <div class="hint">${p.name || ''} · 优先级 ${p.priority}</div>
      <div class="cwnd-track"><div class="cwnd-fill" style="width:${width}%"></div></div>
      <div class="kv">
        <div>CWND<b>${p.cwnd_mss.toFixed(2)} MSS</b></div>
        <div>窗口字节<b>${fmtBytes(p.cwnd_bytes)}</b></div>
        <div>RTT<b>${(p.rtt_ns/1e6).toFixed(1)} ms</b></div>
        <div>在途 / 可用<b>${fmtBytes(p.subflight_bytes)} / ${fmtBytes(Math.max(0,avail))}</b></div>
        <div>SSN NXT/ACK<b>${shortSeq(p.ssn_next)} / ${shortSeq(p.ssn_cum_ack)}</b></div>
        <div>速率/丢包<b>${(p.rate_bytes_sec*8/1e6).toFixed(0)}Mbps / ${(p.loss_rate*100).toFixed(1)}%</b></div>
        <div>发送/ACK<b>${fmtBytes(p.sent_bytes)} / ${fmtBytes(p.acked_bytes)}</b></div>
        <div>重传/重注入<b>${p.retransmissions} / ${p.reinjections}</b></div>
        <div>断线/重加入<b>${p.down_events} / ${p.rejoins}</b></div>
        <div>RR轮次<b>${p.round_robin_turns}</b></div>
      </div>
    </div>`;
  }).join('');
}

function renderMappings(s) {
  if (!s) return;
  const rows = (s.mappings || []).slice().reverse().slice(0, 80);
  $('mappings').innerHTML = rows.map(m => {
    const tags = [];
    if (m.flags & 1) tags.push('<span class="tag retrans">重传</span>');
    if (m.flags & 2) tags.push('<span class="tag redundant">冗余/救援</span>');
    return `<tr>
      <td>${fmtTime(m.at)}</td><td>P${m.path_id}/S${m.subflow_id}</td>
      <td class="mono">${shortSeq(m.dsn_start)}–${shortSeq(m.dsn_end)}</td>
      <td class="mono">${shortSeq(m.ssn_start)}–${shortSeq(m.ssn_end)}</td>
      <td>${fmtBytes(m.length)}</td><td>${tags.join(' ')} ${m.reason || ''}</td>
    </tr>`;
  }).join('');
}

function renderReorder(s) {
  if (!s) return;
  const r = s.reorder;
  const pct = Math.min(100, r.buffered_bytes / r.capacity_bytes * 100);
  $('bufferFill').style.width = `${pct}%`;
  $('reorderSummary').textContent = `next DSN ${shortSeq(r.next_dsn)} · 缓存 ${fmtBytes(r.buffered_bytes)} / ${fmtBytes(r.capacity_bytes)} · 乱序段 ${r.segment_count}`;
  const chunks = [];
  (r.segments || []).slice(0, 70).forEach(x => chunks.push(`<div class="chunk" title="DSN ${x.dsn_start}">D:${shortSeq(x.dsn_start)}</div>`));
  (r.gaps || []).slice(0, 20).forEach(x => chunks.push(`<div class="chunk gap" title="缺口 ${x.start}-${x.end}">缺:${shortSeq(x.start)}</div>`));
  $('reorder').innerHTML = chunks.join('') || '<span class="hint">缓冲为空，数据流按序推进。</span>';
}

function renderComparison(st) {
  const rows = st.comparison || [];
  const max = Math.max(1, ...rows.map(r => r.throughput_mbps));
  $('comparison').innerHTML = rows.length ? rows.map(r => `
    <div class="strategy">
      <h3><span>${r.strategy}</span><span>${r.throughput_mbps.toFixed(1)} MB/s</span></h3>
      <div class="bar-row"><span>吞吐</span><div class="bar ${r.strategy.includes('round') ? 'rr' : ''}"><i style="width:${r.throughput_mbps/max*100}%"></i></div><b>${r.duration_ms} ms</b></div>
      <div class="bar-row"><span>平均时延</span><div class="bar ${r.strategy.includes('round') ? 'rr' : ''}"><i style="width:${Math.min(100,r.latency_avg_ms/80*100)}%"></i></div><b>${r.latency_avg_ms.toFixed(1)} ms</b></div>
      <div class="hint">P95 ${r.latency_p95_ms.toFixed(1)} ms · 重传 ${r.retransmissions} · 救援 ${r.rescue_events}${r.fallback ? ' · 已回退' : ''}${r.error ? ' · '+r.error : ''}</div>
    </div>`).join('') : '<span class="hint">点击“运行两种调度对比”，或开始循环演示。</span>';

  const q = st.snapshot || st.last_snapshot;
  $('queueSummary').textContent = q ? `上限 ${q.max_subflows} · 活动 ${q.active_subflows} · 排队 ${q.queued_count}` : '';
  $('queued').innerHTML = q ? (q.queued_addresses || []).map(a => `<div class="addr"><span>${a.name || ('路径'+a.path_id)}</span><b>优先级 ${a.priority}</b></div>`).join('') || '<div class="addr"><span>没有超出上限的新地址</span><b>—</b></div>' : '';
}

function renderSteps(s) {
  if (!s) return;
  const rows = (s.formula_steps || []).slice().reverse().slice(0, 30);
  $('steps').innerHTML = rows.map(x => `<tr>
    <td>${x.kind}</td><td>${x.path_id}</td><td>${x.before.toFixed(3)}</td>
    <td>${(x.alpha || 0).toFixed(5)}</td><td>${(x.increment || 0).toFixed(5)}</td>
    <td>${x.after.toFixed(3)}</td><td>${x.reason || ''}</td></tr>`).join('');
}

function renderEvents(s) {
  if (!s) return;
  $('events').innerHTML = (s.events || []).slice().reverse().map(e =>
    `<div class="event ${e.level}"><time>${fmtTime(e.at)}</time>${e.message}</div>`).join('');
}

async function tick() {
  try {
    const st = await (await fetch('/api/state')).json();
    statusPill(st);
    const s = st.snapshot || st.last_snapshot;
    renderMetrics(s);
    renderPaths(s); renderMappings(s); renderReorder(s); renderComparison(st); renderSteps(s); renderEvents(s);
  } catch (e) { console.error(e); }
}
$('btnCompare').onclick = async () => { await fetch('/api/compare?sizeMB=32'); };
$('btnLoop').onclick = async () => { await fetch('/api/loop'); };
$('btnStop').onclick = async () => { await fetch('/api/loop?stop=1'); };
tick(); setInterval(tick, 700);
