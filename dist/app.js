(() => {
  "use strict";

  // =========================================================
  // AGCN LIVE - INTERFACE V9
  // Frontend real para Runtime/Server V2.1.
  // =========================================================

  const $ = (id) => document.getElementById(id);
  const $$ = (selector, root = document) => (
    Array.from(root.querySelectorAll(selector))
  );

  const API_BASE = String(
    window.AGCNLIVE_API_BASE || ""
  ).replace(/\/$/, "");

  const STORAGE = {
    session: "agcn-live-session",
    owner: "agcn-live-owner-v1",
    route: "agcn-live-route-v9",
    theme: "agcn-live-theme-v9",
    product: "agcn-live-product-v1",
    salesEnabled: "agcn-live-sales-enabled-v1",
    salesMode: "agcn-live-sales-mode-v1",
    liveDraft: "agcn-live-config-v1",
  };

  const ROUTES = {
    home: {
      hash: "#/inicio",
      screen: "screen-home",
    },
    configure: {
      hash: "#/configurar",
      screen: "screen-configure",
    },
    monitor: {
      hash: "#/monitorar",
      screen: "screen-monitor",
    },
    settings: {
      hash: "#/ajustes",
      screen: "screen-settings",
    },
  };

  const HASH_TO_ROUTE = Object.fromEntries(
    Object.entries(ROUTES).map(
      ([key, value]) => [value.hash, key]
    )
  );

  const formatter = new Intl.NumberFormat("pt-BR");

  const compactFormatter = new Intl.NumberFormat(
    "pt-BR",
    {
      notation: "compact",
      maximumFractionDigits: 1,
    }
  );

  const dateFormatter = new Intl.DateTimeFormat(
    "pt-BR",
    {
      day: "2-digit",
      month: "short",
      year: "numeric",
    }
  );

  const dateTimeFormatter = new Intl.DateTimeFormat(
    "pt-BR",
    {
      day: "2-digit",
      month: "short",
      hour: "2-digit",
      minute: "2-digit",
    }
  );

  let sid = storageGet(
    sessionStorage,
    STORAGE.session,
    ""
  );

  const ownerKey = getOrCreateOwnerKey();

  let state = null;
  let serverAvailable = false;
  let requestPending = false;
  let pendingAction = null;

  let stream = null;
  let fallbackTimer = null;
  let reconnectTimer = null;

  let currentRoute = "home";
  let currentHistoryDays = 7;
  let lastHistoryLoad = 0;

  let lastMonitoring = false;
  let formHydrated = false;
  let configDirty = false;

  let timerBaseElapsed = 0;
  let timerBasePerformance = performance.now();
  let timerRunning = false;

  let toastTimer = null;

  // =========================================================
  // STORAGE
  // =========================================================

  function storageGet(
    storage,
    key,
    fallback = null
  ) {
    try {
      const value = storage.getItem(key);
      return value === null ? fallback : value;
    } catch {
      return fallback;
    }
  }

  function storageSet(
    storage,
    key,
    value
  ) {
    try {
      storage.setItem(key, String(value));
      return true;
    } catch {
      return false;
    }
  }

  function storageRemove(
    storage,
    key
  ) {
    try {
      storage.removeItem(key);
    } catch {
      // Sem persistencia neste ambiente.
    }
  }

  function jsonStorageGet(
    key,
    fallback = null
  ) {
    const raw = storageGet(
      localStorage,
      key,
      null
    );

    if (!raw) {
      return fallback;
    }

    try {
      return JSON.parse(raw);
    } catch {
      return fallback;
    }
  }

  function jsonStorageSet(
    key,
    value
  ) {
    try {
      return storageSet(
        localStorage,
        key,
        JSON.stringify(value)
      );
    } catch {
      return false;
    }
  }

  function randomHex(bytes = 20) {
    const values = new Uint8Array(bytes);

    if (
      window.crypto
      && typeof window.crypto.getRandomValues === "function"
    ) {
      window.crypto.getRandomValues(values);
    } else {
      for (let i = 0; i < values.length; i += 1) {
        values[i] = Math.floor(
          Math.random() * 256
        );
      }
    }

    return Array.from(values)
      .map((value) => value.toString(16).padStart(2, "0"))
      .join("");
  }

  function getOrCreateOwnerKey() {
    const existing = storageGet(
      localStorage,
      STORAGE.owner,
      ""
    );

    if (
      /^[A-Za-z0-9_-]{16,160}$/.test(existing)
    ) {
      return existing;
    }

    const created = (
      "browser_"
      + randomHex(20)
    );

    storageSet(
      localStorage,
      STORAGE.owner,
      created
    );

    return created;
  }

  // =========================================================
  // BASIC HELPERS
  // =========================================================

  function setText(
    id,
    value
  ) {
    const element = $(id);

    if (!element) {
      return;
    }

    element.textContent = (
      value === null
      || value === undefined
      ? ""
      : String(value)
    );
  }

  function setHidden(
    target,
    hidden
  ) {
    const element = (
      typeof target === "string"
      ? $(target)
      : target
    );

    if (!element) {
      return;
    }

    element.classList.toggle(
      "hidden",
      Boolean(hidden)
    );
  }

  function finiteNumber(value) {
    if (
      value === null
      || value === undefined
      || value === ""
    ) {
      return null;
    }

    const number = Number(value);

    return (
      Number.isFinite(number)
      ? number
      : null
    );
  }

  function formatMetric(
    value,
    compact = true
  ) {
    const number = finiteNumber(value);

    if (number === null) {
      return "\u2014";
    }

    if (
      compact
      && Math.abs(number) >= 10000
    ) {
      return compactFormatter.format(
        number
      );
    }

    return formatter.format(
      number
    );
  }

  function formatDuration(seconds) {
    const total = Math.max(
      0,
      Math.floor(
        finiteNumber(seconds) || 0
      )
    );

    const hours = Math.floor(
      total / 3600
    );

    const minutes = Math.floor(
      (total % 3600) / 60
    );

    const secs = (
      total % 60
    );

    return [
      hours,
      minutes,
      secs,
    ]
      .map(
        (value) => (
          String(value).padStart(
            2,
            "0"
          )
        )
      )
      .join(":");
  }

  function timestampDate(
    value,
    withTime = false
  ) {
    const number = finiteNumber(value);

    if (number === null) {
      return "\u2014";
    }

    const date = new Date(
      number * 1000
    );

    if (
      Number.isNaN(
        date.getTime()
      )
    ) {
      return "\u2014";
    }

    return (
      withTime
      ? dateTimeFormatter.format(date)
      : dateFormatter.format(date)
    );
  }

  function platformName(value) {
    return (
      value === "tiktok"
      ? "TikTok"
      : value === "shopee"
      ? "Shopee"
      : "Live"
    );
  }

  function platformInitial(value) {
    return (
      value === "tiktok"
      ? "T"
      : value === "shopee"
      ? "S"
      : "L"
    );
  }

  function normalizeText(value) {
    return String(
      value || ""
    ).trim();
  }

  function numberInputValue(value) {
    const number = finiteNumber(value);

    if (number === null) {
      return "";
    }

    return String(number)
      .replace(".", ",");
  }

  function currentPlatform() {
    const checked = document.querySelector(
      'input[name="platform"]:checked'
    );

    return (
      checked
      ? checked.value
      : "shopee"
    );
  }

  function currentSalesMode() {
    const checked = document.querySelector(
      'input[name="sales_mode"]:checked'
    );

    return (
      checked
      ? checked.value
      : "leve"
    );
  }

  function salesToggleEnabled() {
    return Boolean(
      $("sales-coach-toggle")?.checked
    );
  }

  // =========================================================
  // API
  // =========================================================

  async function api(
    path,
    payload
  ) {
    const headers = {
      Accept: "application/json",
      "X-AGCN-Owner": ownerKey,
    };

    if (sid) {
      headers[
        "X-AGCN-Session"
      ] = sid;
    }

    const hasBody = (
      payload !== undefined
    );

    if (hasBody) {
      headers[
        "Content-Type"
      ] = "application/json";
    }

    const response = await fetch(
      API_BASE + path,
      {
        method: (
          hasBody
          ? "POST"
          : "GET"
        ),
        headers,
        body: (
          hasBody
          ? JSON.stringify(payload)
          : undefined
        ),
        cache: "no-store",
      }
    );

    const nextSid = response.headers.get(
      "X-AGCN-Session"
    );

    const sidChanged = (
      nextSid
      && nextSid !== sid
    );

    if (nextSid) {
      sid = nextSid;

      storageSet(
        sessionStorage,
        STORAGE.session,
        sid
      );
    }

    const contentType = (
      response.headers.get(
        "Content-Type"
      )
      || ""
    );

    if (
      !contentType.includes(
        "application/json"
      )
    ) {
      throw new Error(
        "O servidor de monitoramento n\u00e3o respondeu como esperado."
      );
    }

    const result = await response.json();

    if (
      !response.ok
      || result?.ok === false
    ) {
      throw new Error(
        result?.message
        || result?.result?.message
        || "N\u00e3o foi poss\u00edvel concluir esta a\u00e7\u00e3o."
      );
    }

    serverAvailable = true;

    if (
      sidChanged
      && stream
    ) {
      scheduleStreamReconnect();
    }

    return result;
  }

  async function runCommand(
    path,
    payload,
    options = {}
  ) {
    const {
      silent = false,
      action = "command",
    } = options;

    if (
      requestPending
      && !silent
    ) {
      return null;
    }

    if (!silent) {
      requestPending = true;
      pendingAction = action;
      renderAll();
    }

    try {
      const result = await api(
        path,
        payload
      );

      if (result?.state) {
        applyState(
          result.state
        );
      }

      return result;
    } catch (error) {
      if (!silent) {
        showConfigureError(
          error.message
        );

        showToast(
          error.message,
          "error"
        );
      }

      throw error;
    } finally {
      if (!silent) {
        requestPending = false;
        pendingAction = null;
        renderAll();
      }
    }
  }

  // =========================================================
  // ROUTING
  // =========================================================

  function routeFromHash() {
    return (
      HASH_TO_ROUTE[
        window.location.hash
      ]
      || storageGet(
        sessionStorage,
        STORAGE.route,
        "home"
      )
      || "home"
    );
  }

  function navigate(
    route,
    options = {}
  ) {
    const {
      replace = false,
    } = options;

    if (!ROUTES[route]) {
      route = "home";
    }

    const targetHash = (
      ROUTES[route].hash
    );

    if (
      window.location.hash
      !== targetHash
    ) {
      if (replace) {
        history.replaceState(
          null,
          "",
          targetHash
        );
      } else {
        window.location.hash = (
          targetHash
        );
        return;
      }
    }

    renderRoute(
      route
    );
  }

  function renderRoute(route) {
    if (!ROUTES[route]) {
      route = "home";
    }

    currentRoute = route;

    storageSet(
      sessionStorage,
      STORAGE.route,
      route
    );

    for (
      const [key, routeInfo]
      of Object.entries(ROUTES)
    ) {
      const screen = $(
        routeInfo.screen
      );

      if (!screen) {
        continue;
      }

      const active = (
        key === route
      );

      screen.hidden = !active;
      screen.classList.toggle(
        "is-active",
        active
      );
    }

    $$(".bottom-nav-item").forEach(
      (button) => {
        const active = (
          button.dataset.route
          === route
        );

        button.classList.toggle(
          "is-active",
          active
        );

        if (active) {
          button.setAttribute(
            "aria-current",
            "page"
          );
        } else {
          button.removeAttribute(
            "aria-current"
          );
        }
      }
    );

    window.scrollTo({
      top: 0,
      behavior: "auto",
    });

    if (route === "home") {
      const stale = (
        Date.now()
        - lastHistoryLoad
        > 15000
      );

      if (stale) {
        loadHome(
          currentHistoryDays
        );
      }
    }

    renderAll();
  }

  // =========================================================
  // TOAST / FEEDBACK
  // =========================================================

  function showToast(
    message,
    kind = "info"
  ) {
    const toast = $(
      "app-toast"
    );

    if (!toast) {
      return;
    }

    clearTimeout(
      toastTimer
    );

    toast.textContent = (
      String(message || "")
    );

    toast.classList.remove(
      "hidden",
      "is-error",
      "is-success"
    );

    toast.classList.toggle(
      "is-error",
      kind === "error"
    );

    toast.classList.toggle(
      "is-success",
      kind === "success"
    );

    toastTimer = setTimeout(
      () => {
        toast.classList.add(
          "hidden"
        );
      },
      3600
    );
  }

  function showConfigureError(
    message
  ) {
    const error = $(
      "configure-error"
    );

    if (!error) {
      return;
    }

    error.textContent = (
      String(message || "")
    );

    error.classList.remove(
      "hidden"
    );

    setHidden(
      "configure-success",
      true
    );
  }

  function showConfigureSuccess(
    message
  ) {
    const success = $(
      "configure-success"
    );

    if (!success) {
      return;
    }

    success.textContent = (
      String(message || "")
    );

    success.classList.remove(
      "hidden"
    );

    setHidden(
      "configure-error",
      true
    );
  }

  function clearConfigureFeedback() {
    setHidden(
      "configure-error",
      true
    );

    setHidden(
      "configure-success",
      true
    );
  }

  // =========================================================
  // DIALOGS
  // =========================================================

  function openDialog(id) {
    const dialog = $(id);

    if (!dialog) {
      return;
    }

    if (
      typeof dialog.showModal
      === "function"
    ) {
      if (!dialog.open) {
        dialog.showModal();
      }
    } else {
      dialog.setAttribute(
        "open",
        ""
      );
    }
  }

  function closeDialog(id) {
    const dialog = $(id);

    if (!dialog) {
      return;
    }

    if (
      typeof dialog.close
      === "function"
      && dialog.open
    ) {
      dialog.close();
    } else {
      dialog.removeAttribute(
        "open"
      );
    }
  }

  // =========================================================
  // THEME
  // =========================================================

  const systemThemeMedia = (
    window.matchMedia
    ? window.matchMedia(
        "(prefers-color-scheme: dark)"
      )
    : null
  );

  function preferredTheme() {
    const saved = storageGet(
      localStorage,
      STORAGE.theme,
      "light"
    );

    return (
      ["light", "dark", "system"]
        .includes(saved)
      ? saved
      : "light"
    );
  }

  function effectiveTheme(
    preference
  ) {
    if (
      preference === "system"
    ) {
      return (
        systemThemeMedia?.matches
        ? "dark"
        : "light"
      );
    }

    return preference;
  }

  function applyTheme(
    preference,
    persist = true
  ) {
    const normalized = (
      ["light", "dark", "system"]
        .includes(preference)
      ? preference
      : "light"
    );

    if (persist) {
      storageSet(
        localStorage,
        STORAGE.theme,
        normalized
      );
    }

    document.documentElement.dataset.theme = (
      effectiveTheme(
        normalized
      )
    );

    setText(
      "current-theme-label",
      normalized === "dark"
        ? "Escuro"
        : normalized === "system"
        ? "Sistema"
        : "Claro"
    );

    $$(
      'input[name="theme"]'
    ).forEach(
      (radio) => {
        radio.checked = (
          radio.value
          === normalized
        );
      }
    );

    const metaTheme = document.querySelector(
      'meta[name="theme-color"]'
    );

    if (metaTheme) {
      metaTheme.setAttribute(
        "content",
        effectiveTheme(
          normalized
        ) === "dark"
          ? "#0C0E11"
          : "#FFFFFF"
      );
    }
  }

  // =========================================================
  // PLATFORM / CONFIG FORM
  // =========================================================

  function updatePlatformUI() {
    const selected = (
      currentPlatform()
    );

    const isShopee = (
      selected === "shopee"
    );

    setText(
      "live-input-label",
      isShopee
        ? "Link da live"
        : "@username"
    );

    const input = $(
      "live-input"
    );

    if (input) {
      input.placeholder = (
        isShopee
        ? "Cole o link da LIVE Shopee"
        : "@usuario"
      );

      input.setAttribute(
        "aria-label",
        isShopee
          ? "Link da live Shopee"
          : "Username da live TikTok"
      );
    }

    setText(
      "live-input-help",
      isShopee
        ? "Aceita link curto br.shp.ee ou URL da LIVE."
        : "Informe @username ou o link do perfil TikTok."
    );

    persistLiveDraft();
    renderMetrics(
      state
    );
  }

  function renderSalesSection() {
    const enabled = (
      salesToggleEnabled()
    );

    setHidden(
      "sales-product-section",
      !enabled
    );

    const toggle = $(
      "sales-coach-toggle"
    );

    if (toggle) {
      toggle.setAttribute(
        "aria-expanded",
        enabled
          ? "true"
          : "false"
      );
    }

    const name = $(
      "product-name"
    );

    if (name) {
      name.required = enabled;
    }
  }

  function readProductForm() {
    return {
      name:
        normalizeText(
          $("product-name")?.value
        ),

      regular_price:
        normalizeText(
          $("product-regular-price")?.value
        ),

      current_price:
        normalizeText(
          $("product-current-price")?.value
        ),

      discount:
        normalizeText(
          $("product-discount")?.value
        ),

      description:
        normalizeText(
          $("product-description")?.value
        ),

      additional_info:
        normalizeText(
          $("product-additional-info")?.value
        ),

      mode:
        currentSalesMode(),
    };
  }

  function persistProductDraft() {
    const product = readProductForm();

    jsonStorageSet(
      STORAGE.product,
      product
    );

    storageSet(
      localStorage,
      STORAGE.salesEnabled,
      salesToggleEnabled()
        ? "1"
        : "0"
    );

    storageSet(
      localStorage,
      STORAGE.salesMode,
      currentSalesMode()
    );
  }

  function persistLiveDraft() {
    jsonStorageSet(
      STORAGE.liveDraft,
      {
        platform:
          currentPlatform(),

        value:
          normalizeText(
            $("live-input")?.value
          ),
      }
    );
  }

  function hydrateLiveDraft() {
    const saved = jsonStorageGet(
      STORAGE.liveDraft,
      {}
    );

    if (
      saved?.platform
      && ["shopee", "tiktok"]
        .includes(saved.platform)
    ) {
      const radio = document.querySelector(
        'input[name="platform"][value="'
        + saved.platform
        + '"]'
      );

      if (radio) {
        radio.checked = true;
      }
    }

    if (
      typeof saved?.value
      === "string"
      && $("live-input")
    ) {
      $("live-input").value = (
        saved.value
      );
    }

    updatePlatformUI();
  }

  function fillProductForm(
    productContext,
    options = {}
  ) {
    const {
      preferSavedDiscount = false,
    } = options;

    const product = (
      productContext?.product
      || {}
    );

    if ($("product-name")) {
      $("product-name").value = (
        product.name
        || ""
      );
    }

    if ($("product-regular-price")) {
      $("product-regular-price").value = (
        numberInputValue(
          product.regular_price
        )
      );
    }

    if ($("product-current-price")) {
      $("product-current-price").value = (
        numberInputValue(
          product.current_price
        )
      );
    }

    if ($("product-discount")) {
      const manual = (
        product.discount_source
        === "manual"
      );

      $("product-discount").value = (
        manual
        || preferSavedDiscount
        ? numberInputValue(
            product.discount_percent
          )
        : ""
      );
    }

    if ($("product-description")) {
      $("product-description").value = (
        product.description
        || ""
      );
    }

    if ($("product-additional-info")) {
      $("product-additional-info").value = (
        product.additional_info
        || ""
      );
    }

    const mode = (
      productContext?.mode
      || "leve"
    );

    const modeRadio = document.querySelector(
      'input[name="sales_mode"][value="'
      + mode
      + '"]'
    );

    if (modeRadio) {
      modeRadio.checked = true;
    }

    if ($("sales-coach-toggle")) {
      $("sales-coach-toggle").checked = Boolean(
        productContext?.enabled
      );
    }

    renderSalesSection();
  }

  function fillProductFormFromSaved() {
    const saved = jsonStorageGet(
      STORAGE.product,
      {}
    );

    const ids = {
      name:
        "product-name",
      regular_price:
        "product-regular-price",
      current_price:
        "product-current-price",
      discount:
        "product-discount",
      description:
        "product-description",
      additional_info:
        "product-additional-info",
    };

    for (
      const [key, id]
      of Object.entries(ids)
    ) {
      const element = $(id);

      if (
        element
        && saved?.[key] !== undefined
        && saved?.[key] !== null
      ) {
        element.value = String(
          saved[key]
        );
      }
    }

    const savedMode = storageGet(
      localStorage,
      STORAGE.salesMode,
      saved?.mode || "leve"
    );

    const modeRadio = document.querySelector(
      'input[name="sales_mode"][value="'
      + savedMode
      + '"]'
    );

    if (modeRadio) {
      modeRadio.checked = true;
    }

    const enabled = (
      storageGet(
        localStorage,
        STORAGE.salesEnabled,
        "0"
      ) === "1"
    );

    if ($("sales-coach-toggle")) {
      $("sales-coach-toggle").checked = (
        enabled
      );
    }

    renderSalesSection();
  }

  async function hydrateProductContext() {
    if (formHydrated) {
      return;
    }

    formHydrated = true;

    const backend = (
      state?.product_context
    );

    const backendHasProduct = Boolean(
      backend?.product?.name
      || backend?.product?.description
      || backend?.product?.additional_info
      || backend?.product?.regular_price !== null
        && backend?.product?.regular_price !== undefined
      || backend?.product?.current_price !== null
        && backend?.product?.current_price !== undefined
    );

    if (backendHasProduct) {
      fillProductForm(
        backend
      );

      persistProductDraft();

      return;
    }

    fillProductFormFromSaved();

    const savedProduct = (
      readProductForm()
    );

    const hasSavedProduct = Boolean(
      savedProduct.name
      || savedProduct.regular_price
      || savedProduct.current_price
      || savedProduct.description
      || savedProduct.additional_info
    );

    if (!hasSavedProduct) {
      return;
    }

    try {
      const configured = await api(
        "/api/product-context",
        {
          product:
            savedProduct,
          replace:
            true,
        }
      );

      if (configured?.state) {
        applyState(
          configured.state
        );
      }

      if (
        salesToggleEnabled()
        && savedProduct.name
      ) {
        const activated = await api(
          "/api/product-context/activate",
          {
            mode:
              savedProduct.mode,
          }
        );

        if (activated?.state) {
          applyState(
            activated.state
          );
        }
      }
    } catch (error) {
      console.warn(
        "Product Context nao reidratado:",
        error
      );
    }
  }

  async function syncSalesConfiguration() {
    persistProductDraft();

    if (!salesToggleEnabled()) {
      const result = await api(
        "/api/product-context/deactivate",
        {}
      );

      if (result?.state) {
        applyState(
          result.state
        );
      }

      return;
    }

    const product = (
      readProductForm()
    );

    if (!product.name) {
      $("product-name")?.focus();

      throw new Error(
        "Informe o nome do produto para ativar o Sales Coach."
      );
    }

    const configured = await api(
      "/api/product-context",
      {
        product,
        replace:
          true,
      }
    );

    if (configured?.state) {
      applyState(
        configured.state
      );
    }

    const activated = await api(
      "/api/product-context/activate",
      {
        mode:
          product.mode,
      }
    );

    if (activated?.state) {
      applyState(
        activated.state
      );
    }
  }

  // =========================================================
  // TIMER
  // =========================================================

  function updateTimerAnchor(
    nextState
  ) {
    const elapsed = finiteNumber(
      nextState?.live_session?.elapsed_seconds
    );

    if (elapsed !== null) {
      timerBaseElapsed = elapsed;
      timerBasePerformance = (
        performance.now()
      );
    }

    timerRunning = Boolean(
      nextState?.monitorando
    );
  }

  function currentElapsed() {
    if (!timerRunning) {
      return timerBaseElapsed;
    }

    return (
      timerBaseElapsed
      + (
        performance.now()
        - timerBasePerformance
      ) / 1000
    );
  }

  function paintTimer() {
    const label = formatDuration(
      currentElapsed()
    );

    setText(
      "configure-live-timer",
      label
    );

    setText(
      "monitor-live-timer",
      label
    );
  }

  // =========================================================
  // STATE PAINT
  // =========================================================

  function connectionVisualState(
    current
  ) {
    if (!serverAvailable) {
      return "error";
    }

    if (current?.error) {
      return "error";
    }

    if (
      current?.monitorando
      && current?.connected
    ) {
      return "active";
    }

    if (
      current?.monitorando
    ) {
      return "connecting";
    }

    return "idle";
  }

  function renderConnection(
    current
  ) {
    const visualState = (
      connectionVisualState(
        current
      )
    );

    const configureStatus = $(
      "configure-live-status"
    );

    const monitorStatus = $(
      "monitor-live-status"
    );

    if (configureStatus) {
      configureStatus.dataset.state = (
        visualState
      );
    }

    if (monitorStatus) {
      monitorStatus.dataset.state = (
        visualState
      );
    }

    let configureTitle = (
      "Pronto para conectar"
    );

    let configureDescription = (
      "Configure os dados abaixo para iniciar."
    );

    let monitorTitle = (
      "Nenhuma live em andamento"
    );

    if (!serverAvailable) {
      configureTitle = (
        "Servidor indisponivel"
      );

      configureDescription = (
        "Nao foi possivel acessar o monitoramento."
      );

      monitorTitle = (
        "Servidor indisponivel"
      );
    } else if (current?.error) {
      configureTitle = (
        "Nao foi possivel conectar"
      );

      configureDescription = String(
        current.error
      );

      monitorTitle = (
        "Erro no monitoramento"
      );
    } else if (
      current?.monitorando
      && current?.connected
    ) {
      configureTitle = (
        "Live ativa"
      );

      configureDescription = (
        platformName(
          current.platform
        )
        + " conectada ao AGCN Live."
      );

      monitorTitle = (
        "Live em andamento"
      );
    } else if (current?.monitorando) {
      configureTitle = (
        "Conectando..."
      );

      configureDescription = (
        "Aguardando dados da transmissao."
      );

      monitorTitle = (
        "Conectando a live"
      );
    }

    setText(
      "configure-live-status-title",
      configureTitle
    );

    setText(
      "configure-live-status-description",
      configureDescription
    );

    setText(
      "monitor-live-status-text",
      monitorTitle
    );

    const showTimer = Boolean(
      current?.monitorando
      || current?.live_session?.started_at
    );

    setHidden(
      "configure-live-timer",
      !showTimer
    );

    const headerStatus = $(
      "header-live-status"
    );

    const headerActive = Boolean(
      current?.monitorando
      && current?.connected
    );

    setHidden(
      headerStatus,
      !headerActive
    );

    if (headerActive) {
      setText(
        "header-live-status-text",
        "Live ativa"
      );
    }

    const startButton = $(
      "activate-live-button"
    );

    const startText = $(
      "activate-live-button-text"
    );

    const spinner = $(
      "activate-live-spinner"
    );

    const arrow = startButton?.querySelector(
      ".activate-live-arrow"
    );

    if (startButton) {
      startButton.disabled = Boolean(
        !serverAvailable
        || requestPending
      );

      if (
        requestPending
        && pendingAction === "start"
      ) {
        startButton.dataset.state = (
          "connecting"
        );

        startText.textContent = (
          "CONECTANDO..."
        );

        setHidden(
          spinner,
          false
        );

        setHidden(
          arrow,
          true
        );
      } else if (
        requestPending
        && pendingAction === "save"
      ) {
        startButton.dataset.state = (
          current?.monitorando
          ? "active"
          : "connecting"
        );

        startText.textContent = (
          "SALVANDO..."
        );

        setHidden(
          spinner,
          false
        );

        setHidden(
          arrow,
          true
        );
      } else if (
        current?.monitorando
        && configDirty
      ) {
        startButton.dataset.state = (
          "active"
        );

        startText.textContent = (
          "SALVAR AJUSTES"
        );

        setHidden(
          spinner,
          true
        );

        setHidden(
          arrow,
          false
        );
      } else if (
        current?.monitorando
        && current?.connected
      ) {
        startButton.dataset.state = (
          "active"
        );

        startText.textContent = (
          "\u2713 Live ativa"
        );

        setHidden(
          spinner,
          true
        );

        setHidden(
          arrow,
          true
        );
      } else if (
        current?.monitorando
      ) {
        startButton.dataset.state = (
          "connecting"
        );

        startText.textContent = (
          "CONECTANDO..."
        );

        setHidden(
          spinner,
          false
        );

        setHidden(
          arrow,
          true
        );
      } else {
        startButton.dataset.state = (
          "idle"
        );

        startText.textContent = (
          "ATIVAR LIVE"
        );

        setHidden(
          spinner,
          true
        );

        setHidden(
          arrow,
          false
        );
      }
    }

    setHidden(
      "stop-live-button",
      !current?.monitorando
    );

    const lockLiveConfig = Boolean(
      current?.monitorando
    );

    $$(
      'input[name="platform"]'
    ).forEach(
      (radio) => {
        radio.disabled = (
          lockLiveConfig
          || requestPending
        );
      }
    );

    if ($("live-input")) {
      $("live-input").disabled = (
        lockLiveConfig
        || requestPending
      );
    }

    if ($("stop-live-button")) {
      $("stop-live-button").disabled = (
        requestPending
      );
    }
  }

  function renderMetrics(
    current
  ) {
    const active = Boolean(
      current?.monitorando
    );

    const metrics = (
      active
      ? current?.metrics || {}
      : {}
    );

    setText(
      "metric-viewers",
      formatMetric(
        metrics.viewers
      )
    );

    setText(
      "metric-likes",
      formatMetric(
        metrics.likes
      )
    );

    setText(
      "metric-shares",
      formatMetric(
        metrics.shares
      )
    );

    const platform = (
      current?.platform
      || currentPlatform()
    );

    if (platform === "tiktok") {
      setText(
        "metric-four-label",
        "Seguidores"
      );

      setText(
        "metric-four",
        formatMetric(
          metrics.follows
        )
      );

      setText(
        "metric-four-note",
        "Observados na live"
      );
    } else {
      setText(
        "metric-four-label",
        "Produtos"
      );

      setText(
        "metric-four",
        formatMetric(
          metrics.products
        )
      );

      setText(
        "metric-four-note",
        "Quando disponivel"
      );
    }
  }

  function messageText(message) {
    return normalizeText(
      message?.texto
      || message?.message
      || message?.text
    );
  }

  function createCoachMessage(
    message
  ) {
    const row = document.createElement(
      "div"
    );

    row.className = (
      "coach-message-item"
    );

    row.textContent = (
      messageText(
        message
      )
    );

    return row;
  }

  function renderCoachList(
    containerId,
    messages,
    emptyTitle,
    emptyDescription,
    limit = 3
  ) {
    const container = $(
      containerId
    );

    if (!container) {
      return;
    }

    const useful = (
      Array.isArray(messages)
      ? messages.filter(
          (item) => messageText(item)
        )
      : []
    );

    const visible = useful.slice(
      -limit
    );

    if (!visible.length) {
      const empty = document.createElement(
        "div"
      );

      empty.className = (
        "coach-empty"
      );

      const strong = document.createElement(
        "strong"
      );

      strong.textContent = (
        emptyTitle
      );

      const paragraph = document.createElement(
        "p"
      );

      paragraph.textContent = (
        emptyDescription
      );

      empty.append(
        strong,
        paragraph
      );

      container.replaceChildren(
        empty
      );

      return;
    }

    container.replaceChildren(
      ...visible.map(
        createCoachMessage
      )
    );
  }

  function renderCoaches(
    current
  ) {
    const liveMessages = (
      current?.monitorando
      ? current?.coach || []
      : []
    );

    renderCoachList(
      "live-coach-preview",
      liveMessages,
      current?.monitorando
        ? "Aguardando sinais relevantes"
        : "Aguardando sinais da live",
      current?.monitorando
        ? "O Live Coach esta monitorando a transmissao."
        : "As orientacoes aparecerao aqui quando o monitoramento estiver ativo.",
      3
    );

    const salesActive = Boolean(
      current?.product_context?.enabled
      || current?.sales_coach?.active
    );

    setHidden(
      "sales-coach-panel",
      !salesActive
    );

    if (salesActive) {
      renderCoachList(
        "sales-coach-preview",
        current?.monitorando
          ? current?.sales_coach?.messages || []
          : [],
        current?.monitorando
          ? "Aguardando sinais comerciais"
          : "Sales Coach configurado",
        current?.monitorando
          ? "O Sales Coach esta analisando os comentarios e o contexto do produto."
          : "Inicie a live para receber orientacoes comerciais.",
        3
      );
    }
  }

  function commentInitial(
    name
  ) {
    const text = normalizeText(
      name
    );

    return (
      text
        ? text.charAt(0).toUpperCase()
        : "?"
    );
  }

  function createCommentItem(
    comment,
    className = "comment-preview-item"
  ) {
    const row = document.createElement(
      "div"
    );

    row.className = className;

    const avatar = document.createElement(
      "span"
    );

    avatar.className = (
      "comment-avatar"
    );

    avatar.textContent = commentInitial(
      comment?.user
    );

    const body = document.createElement(
      "div"
    );

    body.className = (
      "comment-body"
    );

    const user = document.createElement(
      "strong"
    );

    user.textContent = (
      normalizeText(
        comment?.user
      )
      || "Usuario"
    );

    const text = document.createElement(
      "p"
    );

    text.textContent = (
      normalizeText(
        comment?.text
      )
    );

    body.append(
      user,
      text
    );

    const time = document.createElement(
      "span"
    );

    time.className = (
      "comment-time"
    );

    time.textContent = (
      normalizeText(
        comment?.time
      )
    );

    row.append(
      avatar,
      body,
      time
    );

    return row;
  }

  function renderComments(
    current
  ) {
    const container = $(
      "comments-preview"
    );

    if (!container) {
      return;
    }

    const comments = (
      current?.monitorando
      && Array.isArray(
        current?.comments
      )
      ? current.comments
      : []
    );

    if (!comments.length) {
      const empty = document.createElement(
        "div"
      );

      empty.className = (
        "comments-empty"
      );

      const icon = document.createElement(
        "span"
      );

      icon.className = (
        "soft-icon"
      );

      icon.setAttribute(
        "aria-hidden",
        "true"
      );

      icon.innerHTML = (
        '<svg viewBox="0 0 24 24">'
        + '<path d="M4 5h16v11H9l-5 4V5Z" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round"/>'
        + '<circle cx="9" cy="10.5" r="1" fill="currentColor"/>'
        + '<circle cx="12" cy="10.5" r="1" fill="currentColor"/>'
        + '<circle cx="15" cy="10.5" r="1" fill="currentColor"/>'
        + "</svg>"
      );

      const strong = document.createElement(
        "strong"
      );

      strong.textContent = (
        "Aguardando comentarios"
      );

      const paragraph = document.createElement(
        "p"
      );

      paragraph.textContent = (
        "As mensagens da live aparecerao aqui."
      );

      empty.append(
        icon,
        strong,
        paragraph
      );

      container.replaceChildren(
        empty
      );

      return;
    }

    container.replaceChildren(
      ...comments
        .slice(-4)
        .map(
          (comment) => (
            createCommentItem(
              comment
            )
          )
        )
    );
  }

  function renderAll() {
    renderConnection(
      state
    );

    renderMetrics(
      state
    );

    renderCoaches(
      state
    );

    renderComments(
      state
    );

    renderSalesSection();

    paintTimer();
  }

  function applyState(
    nextState
  ) {
    const wasMonitoring = (
      lastMonitoring
    );

    state = (
      nextState
      && typeof nextState === "object"
      ? nextState
      : null
    );

    serverAvailable = true;

    lastMonitoring = Boolean(
      state?.monitorando
    );

    updateTimerAnchor(
      state
    );

    if (
      state?.monitorando
      && state?.platform
      && state.platform !== currentPlatform()
    ) {
      const radio = document.querySelector(
        'input[name="platform"][value="'
        + state.platform
        + '"]'
      );

      if (radio) {
        radio.checked = true;
        updatePlatformUI();
      }
    }

    renderAll();

    if (
      wasMonitoring
      && !lastMonitoring
    ) {
      configDirty = false;

      loadHome(
        currentHistoryDays
      );
    }
  }

  // =========================================================
  // HOME / HISTORY
  // =========================================================

  function homeInsight(
    summary
  ) {
    const count = Number(
      summary?.total_lives || 0
    );

    const best = finiteNumber(
      summary?.best_audience_peak
    );

    if (!count) {
      return {
        title:
          "Continue monitorando suas lives para formar seu historico.",
        description:
          "Assim que houver dados suficientes, o AGCN Live mostrara sua evolucao aqui.",
      };
    }

    if (count === 1) {
      return {
        title:
          "Seu historico comecou a ser formado.",
        description:
          best === null
          ? "A primeira live ja foi registrada."
          : (
              "Seu maior pico de audiencia registrado foi "
              + formatter.format(best)
              + "."
            ),
      };
    }

    return {
      title:
        "Voce ja tem "
        + formatter.format(count)
        + " lives registradas neste periodo.",
      description:
        best === null
        ? "Continue monitorando para acompanhar a evolucao dos resultados."
        : (
            "O melhor pico de audiencia do periodo foi "
            + formatter.format(best)
            + "."
          ),
    };
  }

  function renderHomeSummary(
    summary
  ) {
    const best = finiteNumber(
      summary?.best_audience_peak
    );

    setText(
      "home-primary-value",
      formatMetric(
        best
      )
    );

    setText(
      "home-primary-label",
      summary?.metric?.label
      || "Pico de audiencia"
    );

    setHidden(
      "home-primary-change",
      true
    );

    const insight = homeInsight(
      summary
    );

    setText(
      "home-insight-title",
      insight.title
    );

    setText(
      "home-insight-description",
      insight.description
    );

    renderPerformanceChart(
      summary?.series || []
    );

    renderLastLive(
      summary?.last_live
    );
  }

  function chartDayLabel(
    isoDate,
    days
  ) {
    if (!isoDate) {
      return "";
    }

    const date = new Date(
      isoDate + "T12:00:00"
    );

    if (
      Number.isNaN(
        date.getTime()
      )
    ) {
      return "";
    }

    if (days > 7) {
      return String(
        date.getDate()
      ).padStart(2, "0");
    }

    return date
      .toLocaleDateString(
        "pt-BR",
        {
          weekday: "short",
        }
      )
      .replace(".", "")
      .slice(0, 3);
  }

  function renderPerformanceChart(
    series
  ) {
    const bars = $(
      "performance-bars"
    );

    const empty = $(
      "performance-empty"
    );

    if (
      !bars
      || !empty
    ) {
      return;
    }

    const normalized = (
      Array.isArray(series)
      ? series
      : []
    ).map(
      (item) => ({
        date:
          item?.date || "",

        value:
          Math.max(
            0,
            finiteNumber(
              item?.audience_peak
            ) || 0
          ),

        lives:
          Math.max(
            0,
            finiteNumber(
              item?.lives
            ) || 0
          ),
      })
    );

    const hasData = normalized.some(
      (item) => (
        item.lives > 0
        && item.value >= 0
      )
    );

    setHidden(
      empty,
      hasData
    );

    setHidden(
      bars,
      !hasData
    );

    if (!hasData) {
      bars.replaceChildren();
      return;
    }

    const max = Math.max(
      1,
      ...normalized.map(
        (item) => item.value
      )
    );

    const nodes = normalized.map(
      (item) => {
        const wrapper = document.createElement(
          "div"
        );

        wrapper.className = (
          "performance-bar-item"
        );

        const track = document.createElement(
          "div"
        );

        track.className = (
          "performance-bar-track"
        );

        const bar = document.createElement(
          "span"
        );

        bar.className = (
          "performance-bar"
        );

        const percent = (
          item.lives > 0
          ? Math.max(
              2,
              item.value / max * 100
            )
          : 0
        );

        bar.style.height = (
          percent + "%"
        );

        bar.title = (
          item.lives > 0
          ? (
              formatter.format(
                item.value
              )
              + " de pico de audiencia"
            )
          : "Sem live registrada"
        );

        track.append(
          bar
        );

        const label = document.createElement(
          "span"
        );

        label.className = (
          "performance-bar-label"
        );

        label.textContent = chartDayLabel(
          item.date,
          currentHistoryDays
        );

        wrapper.append(
          track,
          label
        );

        return wrapper;
      }
    );

    bars.replaceChildren(
      ...nodes
    );
  }

  function renderLastLive(
    live
  ) {
    const hasLive = Boolean(
      live?.id
    );

    setHidden(
      "last-live-empty",
      hasLive
    );

    setHidden(
      "last-live-data",
      !hasLive
    );

    if (!hasLive) {
      return;
    }

    setText(
      "last-live-platform-icon",
      platformInitial(
        live.platform
      )
    );

    setText(
      "last-live-title",
      normalizeText(
        live.subject
      )
      || (
        "Live "
        + platformName(
          live.platform
        )
      )
    );

    setText(
      "last-live-date",
      timestampDate(
        live.started_at,
        true
      )
    );

    setText(
      "last-live-value",
      formatMetric(
        live.audience_peak,
        false
      )
    );
  }

  async function loadHome(
    days = 7
  ) {
    currentHistoryDays = (
      Number(days) === 30
      ? 30
      : 7
    );

    if ($("history-period")) {
      $("history-period").value = (
        String(
          currentHistoryDays
        )
      );
    }

    try {
      const result = await api(
        "/api/history/summary?days="
        + encodeURIComponent(
          String(
            currentHistoryDays
          )
        )
      );

      renderHomeSummary(
        result?.summary || {}
      );

      lastHistoryLoad = (
        Date.now()
      );
    } catch (error) {
      console.warn(
        "Historico indisponivel:",
        error
      );
    }
  }

  function createHistoryItem(
    live
  ) {
    const row = document.createElement(
      "div"
    );

    row.className = (
      "history-item"
    );

    const icon = document.createElement(
      "span"
    );

    icon.className = (
      "platform-mini-icon"
    );

    icon.textContent = (
      platformInitial(
        live?.platform
      )
    );

    const copy = document.createElement(
      "div"
    );

    copy.className = (
      "history-item-copy"
    );

    const title = document.createElement(
      "strong"
    );

    title.textContent = (
      normalizeText(
        live?.subject
      )
      || (
        "Live "
        + platformName(
          live?.platform
        )
      )
    );

    const date = document.createElement(
      "span"
    );

    date.textContent = (
      timestampDate(
        live?.started_at,
        true
      )
      + (
        live?.duration_seconds
        !== null
        && live?.duration_seconds
        !== undefined
        ? (
            " \u00b7 "
            + formatDuration(
                live.duration_seconds
              )
          )
        : ""
      )
    );

    copy.append(
      title,
      date
    );

    const value = document.createElement(
      "div"
    );

    value.className = (
      "history-item-value"
    );

    const strong = document.createElement(
      "strong"
    );

    strong.textContent = formatMetric(
      live?.audience_peak,
      false
    );

    const caption = document.createElement(
      "span"
    );

    caption.textContent = (
      "pico de audiencia"
    );

    value.append(
      strong,
      caption
    );

    row.append(
      icon,
      copy,
      value
    );

    return row;
  }

  async function openHistory() {
    const list = $(
      "history-list"
    );

    if (list) {
      list.innerHTML = (
        '<div class="dialog-empty">Carregando historico...</div>'
      );
    }

    openDialog(
      "history-dialog"
    );

    try {
      const result = await api(
        "/api/history/recent?limit=50"
      );

      const items = (
        Array.isArray(result?.items)
        ? result.items
        : []
      );

      if (!list) {
        return;
      }

      if (!items.length) {
        list.innerHTML = (
          '<div class="dialog-empty">Nenhuma live registrada ainda.</div>'
        );

        return;
      }

      list.replaceChildren(
        ...items.map(
          createHistoryItem
        )
      );
    } catch (error) {
      if (list) {
        list.textContent = (
          error.message
        );
      }
    }
  }

  // =========================================================
  // COACH / COMMENT DIALOGS
  // =========================================================

  function fillCoachDialog(
    listId,
    messages
  ) {
    const list = $(listId);

    if (!list) {
      return;
    }

    const useful = (
      Array.isArray(messages)
      ? messages.filter(
          (message) => messageText(message)
        )
      : []
    );

    if (!useful.length) {
      list.innerHTML = (
        '<div class="dialog-empty">Nenhuma orientacao disponivel.</div>'
      );

      return;
    }

    list.replaceChildren(
      ...useful
        .slice()
        .reverse()
        .map(
          createCoachMessage
        )
    );
  }

  function openLiveCoachDialog() {
    fillCoachDialog(
      "live-coach-dialog-list",
      state?.coach || []
    );

    openDialog(
      "live-coach-dialog"
    );
  }

  function openSalesCoachDialog() {
    fillCoachDialog(
      "sales-coach-dialog-list",
      state?.sales_coach?.messages
      || []
    );

    openDialog(
      "sales-coach-dialog"
    );
  }

  function openCommentsDialog() {
    const list = $(
      "comments-dialog-list"
    );

    const comments = (
      Array.isArray(
        state?.comments
      )
      ? state.comments
      : []
    );

    if (list) {
      if (!comments.length) {
        list.innerHTML = (
          '<div class="dialog-empty">Nenhum comentario disponivel.</div>'
        );
      } else {
        list.replaceChildren(
          ...comments
            .slice()
            .reverse()
            .map(
              (comment) => (
                createCommentItem(
                  comment
                )
              )
            )
        );
      }
    }

    openDialog(
      "comments-dialog"
    );
  }

  // =========================================================
  // SSE / FALLBACK
  // =========================================================

  function clearFallback() {
    if (fallbackTimer) {
      clearInterval(
        fallbackTimer
      );

      fallbackTimer = null;
    }
  }

  function startFallback() {
    if (fallbackTimer) {
      return;
    }

    fallbackTimer = setInterval(
      async () => {
        try {
          const next = await api(
            "/api/state"
          );

          applyState(
            next
          );

          if (
            sid
            && window.EventSource
          ) {
            clearFallback();
            scheduleStreamReconnect(
              300
            );
          }
        } catch (error) {
          serverAvailable = false;
          renderAll();
        }
      },
      3500
    );
  }

  function scheduleStreamReconnect(
    delay = 80
  ) {
    clearTimeout(
      reconnectTimer
    );

    reconnectTimer = setTimeout(
      () => {
        connectStream();
      },
      delay
    );
  }

  function connectStream() {
    if (
      !sid
      || !window.EventSource
    ) {
      startFallback();
      return;
    }

    if (stream) {
      stream.close();
      stream = null;
    }

    const query = new URLSearchParams({
      sid,
      owner:
        ownerKey,
    });

    stream = new EventSource(
      API_BASE
      + "/api/events?"
      + query.toString()
    );

    stream.addEventListener(
      "state",
      (event) => {
        try {
          const next = JSON.parse(
            event.data
          );

          serverAvailable = true;

          applyState(
            next
          );

          clearFallback();
        } catch (error) {
          console.error(
            "Estado SSE invalido:",
            error
          );
        }
      }
    );

    stream.onerror = () => {
      if (stream) {
        stream.close();
        stream = null;
      }

      startFallback();
    };
  }

  // =========================================================
  // CONFIG SUBMIT / LIVE
  // =========================================================

  async function submitConfiguration(
    event
  ) {
    event.preventDefault();

    if (requestPending) {
      return;
    }

    clearConfigureFeedback();

    persistLiveDraft();
    persistProductDraft();

    if (state?.monitorando) {
      requestPending = true;
      pendingAction = "save";
      renderAll();

      try {
        await syncSalesConfiguration();

        configDirty = false;

        showConfigureSuccess(
          "Ajustes da live atualizados."
        );

        showToast(
          "Ajustes atualizados.",
          "success"
        );
      } catch (error) {
        showConfigureError(
          error.message
        );

        showToast(
          error.message,
          "error"
        );
      } finally {
        requestPending = false;
        pendingAction = null;
        renderAll();
      }

      return;
    }

    const platform = (
      currentPlatform()
    );

    const value = normalizeText(
      $("live-input")?.value
    );

    if (!value) {
      $("live-input")?.focus();

      showConfigureError(
        platform === "tiktok"
          ? "Informe o @username da live."
          : "Informe o link da live Shopee."
      );

      return;
    }

    requestPending = true;
    pendingAction = "start";
    renderAll();

    try {
      await syncSalesConfiguration();

      const started = await api(
        "/api/start",
        {
          platform,
          value,
        }
      );

      if (started?.state) {
        applyState(
          started.state
        );
      }

      configDirty = false;

      showConfigureSuccess(
        "Monitoramento iniciado."
      );

      showToast(
        "Monitoramento iniciado.",
        "success"
      );

      navigate(
        "monitor"
      );

      scheduleStreamReconnect(
        100
      );
    } catch (error) {
      showConfigureError(
        error.message
      );

      showToast(
        error.message,
        "error"
      );
    } finally {
      requestPending = false;
      pendingAction = null;
      renderAll();
    }
  }

  async function stopLive() {
    if (
      requestPending
      || !state?.monitorando
    ) {
      return;
    }

    requestPending = true;
    pendingAction = "stop";
    renderAll();

    try {
      const result = await api(
        "/api/stop",
        {}
      );

      if (result?.state) {
        applyState(
          result.state
        );
      }

      showToast(
        "Monitoramento encerrado.",
        "success"
      );
    } catch (error) {
      showToast(
        error.message,
        "error"
      );
    } finally {
      requestPending = false;
      pendingAction = null;
      renderAll();
    }
  }

  // =========================================================
  // EVENT BINDINGS
  // =========================================================

  function bindNavigation() {
    $$("[data-route]").forEach(
      (button) => {
        button.addEventListener(
          "click",
          () => {
            navigate(
              button.dataset.route
            );
          }
        );
      }
    );

    window.addEventListener(
      "hashchange",
      () => {
        renderRoute(
          routeFromHash()
        );
      }
    );
  }

  function bindDialogs() {
    $$(
      "[data-close-dialog]"
    ).forEach(
      (button) => {
        button.addEventListener(
          "click",
          () => {
            closeDialog(
              button.dataset.closeDialog
            );
          }
        );
      }
    );

    $$(".app-dialog").forEach(
      (dialog) => {
        dialog.addEventListener(
          "click",
          (event) => {
            if (
              event.target
              === dialog
            ) {
              dialog.close();
            }
          }
        );
      }
    );

    $("open-history-button")?.addEventListener(
      "click",
      openHistory
    );

    $("live-coach-more-button")?.addEventListener(
      "click",
      openLiveCoachDialog
    );

    $("sales-coach-more-button")?.addEventListener(
      "click",
      openSalesCoachDialog
    );

    $("comments-more-button")?.addEventListener(
      "click",
      openCommentsDialog
    );

    $("theme-settings-button")?.addEventListener(
      "click",
      () => {
        applyTheme(
          preferredTheme(),
          false
        );

        openDialog(
          "theme-dialog"
        );
      }
    );

    $("help-settings-button")?.addEventListener(
      "click",
      () => {
        openDialog(
          "help-dialog"
        );
      }
    );

    $("account-settings-button")?.addEventListener(
      "click",
      () => {
        showToast(
          "Minha conta sera ativada quando a autenticacao estiver pronta."
        );
      }
    );

    $("header-notifications-button")?.addEventListener(
      "click",
      () => {
        showToast(
          "A central de notificacoes ainda nao esta disponivel."
        );
      }
    );
  }

  function bindTheme() {
    $$(
      'input[name="theme"]'
    ).forEach(
      (radio) => {
        radio.addEventListener(
          "change",
          () => {
            if (radio.checked) {
              applyTheme(
                radio.value
              );
            }
          }
        );
      }
    );

    if (systemThemeMedia) {
      const listener = () => {
        if (
          preferredTheme()
          === "system"
        ) {
          applyTheme(
            "system",
            false
          );
        }
      };

      if (
        typeof systemThemeMedia
          .addEventListener
        === "function"
      ) {
        systemThemeMedia.addEventListener(
          "change",
          listener
        );
      } else if (
        typeof systemThemeMedia
          .addListener
        === "function"
      ) {
        systemThemeMedia.addListener(
          listener
        );
      }
    }
  }

  function bindConfigure() {
    $$(
      'input[name="platform"]'
    ).forEach(
      (radio) => {
        radio.addEventListener(
          "change",
          () => {
            clearConfigureFeedback();

            configDirty = true;

            updatePlatformUI();
          }
        );
      }
    );

    $("live-input")?.addEventListener(
      "input",
      () => {
        configDirty = true;
        persistLiveDraft();
      }
    );

    $("sales-coach-toggle")?.addEventListener(
      "change",
      () => {
        configDirty = true;

        renderSalesSection();

        persistProductDraft();

        clearConfigureFeedback();

        renderAll();
      }
    );

    [
      "product-name",
      "product-regular-price",
      "product-current-price",
      "product-discount",
      "product-description",
      "product-additional-info",
    ].forEach(
      (id) => {
        $(id)?.addEventListener(
          "input",
          () => {
            configDirty = true;
            persistProductDraft();
            renderAll();
          }
        );
      }
    );

    $$(
      'input[name="sales_mode"]'
    ).forEach(
      (radio) => {
        radio.addEventListener(
          "change",
          () => {
            configDirty = true;
            persistProductDraft();
            renderAll();
          }
        );
      }
    );

    $("live-config-form")?.addEventListener(
      "submit",
      submitConfiguration
    );

    $("stop-live-button")?.addEventListener(
      "click",
      stopLive
    );
  }

  function bindHome() {
    $("history-period")?.addEventListener(
      "change",
      (event) => {
        loadHome(
          event.target.value
        );
      }
    );
  }

  // =========================================================
  // BOOT
  // =========================================================

  async function boot() {
    applyTheme(
      preferredTheme(),
      false
    );

    bindNavigation();
    bindDialogs();
    bindTheme();
    bindConfigure();
    bindHome();

    hydrateLiveDraft();
    fillProductFormFromSaved();

    renderSalesSection();

    navigate(
      routeFromHash(),
      {
        replace:
          !window.location.hash,
      }
    );

    renderAll();

    try {
      await api(
        "/api/health"
      );

      const initial = await api(
        "/api/state"
      );

      applyState(
        initial
      );

      await hydrateProductContext();

      configDirty = false;

      renderAll();

      await loadHome(
        currentHistoryDays
      );

      connectStream();
    } catch (error) {
      serverAvailable = false;

      renderAll();

      showToast(
        error.message,
        "error"
      );

      startFallback();
    }

    setInterval(
      paintTimer,
      500
    );
  }

  boot();
})();
