(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const base = (window.AGCNLIVE_API_BASE || '').replace(/\/$/, '');
  const elements = { input: $('live-input'), help: $('input-help'), start: $('start-button'), stop: $('stop-button'), notice: $('notice'), status: $('global-status') };
  let sid = sessionStorage.getItem('agcn-live-session') || '';
  let state = null;
  let connectedToServer = false;
  let requestPending = false;
  let selectedStyle = 'equilibrado';
  let stream = null;
  let fallback = null;
  let commentSignature = '';
  let coachIds = { live: '', sales: '' };

  const platform = () => document.querySelector('input[name="platform"]:checked').value;
  const text = (id, value) => { $(id).textContent = value == null ? '' : String(value); };
  const fmt = value => value == null || value === '' ? '—' : typeof value === 'number' ? new Intl.NumberFormat('pt-BR').format(value) : String(value);
  const label = key => ({ shopee: 'SHOPEE', tiktok: 'TIKTOK' })[key] || '—';
  const readable = value => ({ iniciando: 'Iniciando', conectando: 'Conectando', ativo: 'Monitorando', encerrando: 'Encerrando', encerrado: 'Encerrado', finalizado: 'Finalizado', erro: 'Erro', parado: 'Desconectado' })[value] || 'Desconectado';
  const currencySafe = v => String(v || '').slice(0, 100);

  function showNotice(message, kind = 'error') {
    elements.notice.textContent = message;
    elements.notice.classList.remove('hidden');
    elements.notice.classList.toggle('info', kind === 'info');
  }
  function clearNotice() { elements.notice.classList.add('hidden'); elements.notice.textContent = ''; }

  async function api(path, payload) {
    const headers = { 'Accept': 'application/json' };
    if (sid) headers['X-AGCN-Session'] = sid;
    if (payload !== undefined) headers['Content-Type'] = 'application/json';
    const response = await fetch(base + path, { method: payload === undefined ? 'GET' : 'POST', headers, body: payload === undefined ? undefined : JSON.stringify(payload), cache: 'no-store' });
    const next = response.headers.get('X-AGCN-Session');
    if (next && next !== sid) { sid = next; sessionStorage.setItem('agcn-live-session', next); }
    if (!(response.headers.get('Content-Type') || '').includes('application/json')) throw new Error('O servidor Python de monitoramento não está disponível neste endereço.');
    const result = await response.json();
    if (!response.ok || result.ok === false) throw new Error(result.message || result.result?.message || 'Não foi possível concluir esta ação.');
    return result;
  }

  function manualData() {
    const raw = $('seller-facts').value.trim();
    let facts = {};
    if (raw) {
      try { facts = JSON.parse(raw); } catch { throw new Error('Os fatos estruturados precisam ser um JSON válido.'); }
      if (!facts || Array.isArray(facts) || typeof facts !== 'object') throw new Error('Os fatos estruturados precisam ser um objeto JSON.');
    }
    return { price: currencySafe($('seller-price').value.trim()), info: $('seller-info').value.trim(), facts };
  }

  function selectPlatform() {
    const isShopee = platform() === 'shopee';
    elements.input.placeholder = isShopee ? 'Cole o link da LIVE Shopee' : '@username ou link do perfil TikTok';
    elements.input.setAttribute('aria-label', isShopee ? 'Link da LIVE Shopee' : 'Usuário ou link TikTok');
    text('input-help', isShopee ? 'Link curto br.shp.ee ou URL da LIVE com sessão.' : 'Informe @username ou o link do perfil TikTok.');
    text('platform-current', label(platform()) + ' SELECIONADA');
    $('sales-styles').querySelectorAll('button').forEach(button => button.disabled = !isShopee || !!state?.monitorando && state?.platform !== 'shopee');
    $('product-details').classList.toggle('unavailable', !isShopee);
    $('product-details').open = isShopee && $('product-details').open;
    $('product-details').querySelector('summary').setAttribute('aria-disabled', String(!isShopee));
    $('product-form').querySelectorAll('input, textarea, button').forEach(control => control.disabled = !isShopee);
    paint(state);
  }

  function paintConnection(s) {
    const busy = !!s?.monitorando;
    const connected = busy && !!s?.connected;
    const connecting = busy && !connected;
    const disconnected = !connectedToServer;
    elements.status.classList.toggle('connected', connected);
    elements.status.classList.toggle('connecting', connecting);
    text('global-status', (disconnected ? 'Servidor indisponível' : connected ? 'LIVE conectada' : connecting ? 'Conectando à LIVE' : 'Desconectado'));
    const statusDot = document.createElement('span'); statusDot.className = 'state-dot';
    elements.status.prepend(statusDot);
    const statusLabel = $('connection-label');
    statusLabel.classList.toggle('connected', connected); statusLabel.classList.toggle('connecting', connecting);
    text('connection-label', disconnected ? 'Servidor indisponível' : connected ? 'Conexão ativa' : connecting ? 'Aguardando conexão' : 'Aguardando conexão');
    const dot = document.createElement('span'); dot.className = 'tiny-dot'; statusLabel.prepend(dot);
    text('monitor-current', busy ? `${label(s.platform)} / ${connected ? 'LIVE CONECTADA' : readable(s.status).toUpperCase()}` : 'MONITORAMENTO INATIVO');
    text('backend-status', disconnected ? 'Servidor de monitoramento indisponível' : busy ? `${label(s.platform)} • ${readable(s.status)}` : 'Pronto para monitorar');
    elements.start.classList.toggle('hidden', busy); elements.stop.classList.toggle('hidden', !busy);
    elements.start.disabled = disconnected || requestPending;
    elements.stop.disabled = requestPending;
    document.querySelectorAll('input[name="platform"]').forEach(radio => radio.disabled = busy);
    elements.input.disabled = busy;
    if (busy && s.platform !== platform()) {
      const radio = document.querySelector(`input[name="platform"][value="${s.platform}"]`);
      if (radio) radio.checked = true;
    }
    if (s?.last_event) { const d = new Date(s.last_event); text('last-event', Number.isNaN(d.getTime()) ? 'Último evento recebido' : `Último evento às ${d.toLocaleTimeString('pt-BR')}`); }
    else text('last-event', 'Aguardando dados da LIVE');
  }

  function paintMetrics(s) {
    const metrics = s?.monitorando && s?.platform === platform() ? s.metrics || {} : {};
    text('metric-viewers', fmt(metrics.viewers)); text('metric-likes', fmt(metrics.likes));
    text('metric-three-label', platform() === 'tiktok' ? 'PRESENTES' : 'COMPARTILHAMENTOS');
    text('metric-three', fmt(platform() === 'tiktok' ? metrics.gifts : metrics.shares));
    text('metric-three-note', 'Quando disponível');
    text('metric-four-label', platform() === 'tiktok' ? 'SEGUIDORES' : 'PRODUTOS');
    text('metric-four', fmt(platform() === 'tiktok' ? metrics.follows : metrics.products));
    text('metric-four-note', 'Quando disponível');
  }

  function currentMessage(messages, now) {
    if (!Array.isArray(messages)) return null;
    for (let i = messages.length - 1; i >= 0; i--) {
      const m = messages[i];
      const started = Number(m.timestamp) || 0;
      const duration = Number(m.display_seconds) || 8;
      const expires = Math.min(Number(m.expires_at) || started + duration, started + duration);
      if (started <= now + 2 && expires > now && m.texto) return { ...m, expires, started, duration };
    }
    return null;
  }

  function paintCoach(kind, s, now) {
    const sales = kind === 'sales';
    const disabled = sales && platform() === 'tiktok';
    const data = sales ? s?.sales_coach : null;
    const message = !disabled && s?.monitorando ? currentMessage(sales ? data?.messages : s?.coach, now) : null;
    const stage = $(kind + '-stage');
    const body = $(kind + '-message');
    const id = message?.id || '';
    if (id !== coachIds[kind]) { coachIds[kind] = id; body.classList.remove('empty'); if (id) { body.style.animation = 'none'; void body.offsetWidth; body.style.animation = ''; } }
    stage.classList.toggle('has-message', !!message);
    stage.classList.toggle('priority-high', !sales && !!message && (message.priority_level === 'high' || Number(message.priority) >= 85));
    body.classList.toggle('empty', !message);
    let emptyText;
    if (disabled) emptyText = 'Sales Coach disponível exclusivamente para lives da Shopee.';
    else if (!s?.monitorando) emptyText = sales ? 'Aguardando uma LIVE Shopee para acompanhar o produto.' : 'As orientações aparecerão aqui durante a LIVE.';
    else if (sales && s?.product?.state === 'error') emptyText = 'Não foi possível identificar o produto. O Live Coach segue funcionando.';
    else if (sales && !s?.product?.name) emptyText = 'Aguardando identificação do produto da Shopee.';
    else if (sales) emptyText = 'Preparando sugestões a partir dos dados do produto.';
    else emptyText = 'Aguardando sinais úteis da LIVE.';
    text(kind + '-message', message?.texto || emptyText);
    text(kind + '-category', message?.categoria || (sales ? 'PRODUTO EM FOCO' : 'EM TEMPO REAL'));
    text(kind + '-time', message?.hora || '');
    text(kind + '-timer', message ? `${Math.max(0, Math.ceil(message.expires - now))}s` : '');
    const progress = message ? Math.max(0, Math.min(100, (message.expires - now) / message.duration * 100)) : 0;
    $(kind + '-progress').style.width = `${progress}%`;
    if (sales) {
      text('sales-product', disabled ? 'Indisponível no TikTok' : s?.product?.name || (s?.product?.state === 'error' ? 'Produto não identificado' : 'Aguardando produto'));
      const conflict = Array.isArray(s?.product?.conflicts) ? s.product.conflicts.find(item => item.field === 'price') : null;
      const conflictLabel = $('product-conflict');
      conflictLabel.classList.toggle('hidden', !conflict || disabled);
      if (conflict && !disabled) {
        const value = fact => fact && typeof fact === 'object' ? fact.value ?? fact.amount ?? null : fact;
        const original = value(conflict.shopee);
        const seller = value(conflict.seller);
        conflictLabel.textContent = `Preço divergente: Shopee ${original == null ? '(não informado)' : original} · Vendedor ${seller == null ? '(não informado)' : seller}. Os dois foram preservados; as orientações seguem o preço informado pelo vendedor.`;
      }
      text('sales-status', disabled ? 'Indisponível' : !s?.monitorando ? 'Aguardando' : data?.error ? 'Atenção' : message ? 'Orientando' : s?.product?.name ? 'Preparando' : 'Aguardando produto');
      $('sales-panel')?.classList.toggle('unavailable', disabled);
    } else {
      text('live-priority', message?.priority_level === 'high' || Number(message?.priority) >= 85 ? 'PRIORIDADE ALTA' : message ? 'ORIENTAÇÃO ATIVA' : '');
      $('live-priority').classList.toggle('high', !!message && (message.priority_level === 'high' || Number(message.priority) >= 85));
      text('live-status', !s?.monitorando ? 'Aguardando' : message ? 'Orientando' : 'Monitorando');
    }
    const stateLabel = $(kind + '-status');
    stateLabel.classList.toggle('active', !!message);
    stateLabel.prepend(Object.assign(document.createElement('span'), { className: 'tiny-dot' }));
  }

  function paintComments(s) {
    const entries = Array.isArray(s?.comments) ? s.comments : [];
    text('comment-count', entries.length);
    const signature = entries.map(c => `${c.id}:${c.text}`).join('|');
    if (commentSignature === signature) return;
    commentSignature = signature;
    const container = $('comments-list');
    const pinned = container.scrollTop + container.clientHeight >= container.scrollHeight - 40;
    if (!entries.length) {
      const empty = document.createElement('div'); empty.className = 'comments-empty';
      const icon = document.createElement('span'); icon.className = 'empty-icon'; icon.setAttribute('aria-hidden', 'true'); icon.textContent = '☷';
      const strong = document.createElement('strong'); strong.textContent = 'Aguardando comentários';
      const note = document.createElement('p'); note.textContent = 'As mensagens da LIVE aparecerão aqui.';
      empty.append(icon, strong, note); container.replaceChildren(empty); return;
    }
    const items = entries.map(c => {
      const row = document.createElement('div'); row.className = 'comment';
      const top = document.createElement('div'); top.className = 'comment-top';
      const user = document.createElement('span'); user.className = 'comment-user'; user.textContent = c.user || 'Usuário';
      const time = document.createElement('span'); time.className = 'comment-time'; time.textContent = c.time || '';
      const body = document.createElement('p'); body.textContent = c.text || '';
      top.append(user, time); row.append(top, body); return row;
    });
    container.replaceChildren(...items);
    if (pinned) container.scrollTop = container.scrollHeight;
  }

  function paint(s) {
    paintConnection(s); paintMetrics(s);
    const now = Date.now() / 1000;
    paintCoach('live', s, now); paintCoach('sales', s, now);
    paintComments(s);
    $('alert-modes').querySelectorAll('button').forEach(button => button.classList.toggle('selected', button.dataset.mode === (s?.coach_mode?.value || 'all')));
    const style = s?.monitorando && s?.platform === 'shopee' ? s.sales_coach?.style || selectedStyle : selectedStyle;
    $('sales-styles').querySelectorAll('button').forEach(button => { button.classList.toggle('selected', button.dataset.style === style); button.disabled = platform() !== 'shopee'; });
    $('product-details').querySelector('summary').style.pointerEvents = platform() === 'tiktok' ? 'none' : '';
    if (s?.error && s?.platform === platform()) showNotice(s.error);
    else if (s?.monitorando && s?.platform === 'shopee' && s.sales_coach?.error) showNotice(`Sales Coach: ${s.sales_coach.error}`);
    else if (connectedToServer && elements.notice.classList.contains('info')) clearNotice();
  }

  function applyState(next) { state = next; connectedToServer = true; paint(state); }
  function pollOnStreamFailure() {
    if (fallback) return;
    fallback = setInterval(async () => { try { applyState(await api('/api/state')); } catch (e) { connectedToServer = false; showNotice(e.message); paint(state); } }, 3500);
  }
  function streamState() {
    if (!sid || !window.EventSource) { pollOnStreamFailure(); return; }
    if (stream) stream.close();
    stream = new EventSource(base + '/api/events?sid=' + encodeURIComponent(sid));
    stream.addEventListener('state', event => { try { applyState(JSON.parse(event.data)); if (fallback) { clearInterval(fallback); fallback = null; } } catch (e) { console.error('Estado inválido:', e); } });
    stream.onerror = () => { pollOnStreamFailure(); };
  }

  async function command(path, data) {
    if (requestPending) return;
    requestPending = true; paint(state);
    try {
      const result = await api(path, data);
      applyState(result.state);
      if (result.result?.ok === false) throw new Error(result.result.message || 'Operação não concluída.');
      clearNotice();
    } catch (e) { showNotice(e.message); }
    finally { requestPending = false; paint(state); }
  }

  $('start-form').addEventListener('submit', event => {
    event.preventDefault();
    try {
      const seller = platform() === 'shopee' ? manualData() : { price: '', info: '', facts: {} };
      command('/api/start', { platform: platform(), value: elements.input.value.trim(), ...seller, style: selectedStyle });
    } catch (e) { showNotice(e.message); }
  });
  elements.stop.addEventListener('click', () => command('/api/stop', {}));
  document.querySelectorAll('input[name="platform"]').forEach(radio => radio.addEventListener('change', () => { clearNotice(); selectPlatform(); }));
  $('alert-modes').addEventListener('click', event => { const mode = event.target.closest('button')?.dataset.mode; if (mode) command('/api/alert-mode', { mode }); });
  $('sales-styles').addEventListener('click', event => {
    const style = event.target.closest('button')?.dataset.style;
    if (!style || platform() !== 'shopee') return;
    selectedStyle = style;
    if (state?.monitorando) command('/api/sales-style', { style });
    else { paint(state); clearNotice(); }
  });
  $('product-form').addEventListener('submit', event => {
    event.preventDefault();
    try {
      const seller = manualData();
      if (!state?.monitorando) { showNotice('Informações prontas. Inicie uma LIVE Shopee para enviá-las ao Product Extractor.', 'info'); return; }
      command('/api/product-info', seller);
    } catch (e) { showNotice(e.message); }
  });
  setInterval(() => {
    text('clock', new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' }));
    if (state) { paintCoach('live', state, Date.now() / 1000); paintCoach('sales', state, Date.now() / 1000); }
  }, 500);
  selectPlatform();
  (async () => {
    try {
      await api('/api/health');
      applyState(await api('/api/state'));
      streamState();
    } catch (e) { connectedToServer = false; showNotice(e.message); paint(state); }
  })();
})();
