# ============================================================
# AGCN LIVE - SALES DECISION COACH V1.0
#
# FLUXO:
# Worker Shopee Produto
#   -> Product Extractor
#   -> Product Sales Builder
#   -> SALES DECISION COACH
#   -> SALES COACH na Interface
#
# RESPONSABILIDADE:
# - recebe o banco de Sales Candidates do Product Sales Builder;
# - escolhe qual orientacao comercial deve aparecer agora;
# - mantem o vendedor com assunto de forma continua;
# - alterna categorias para evitar repeticao cansativa;
# - controla repeticao de preco, CTA e demais grupos;
# - reseta o ciclo quando o produto muda;
# - preserva historico quando o mesmo produto e atualizado;
# - aplica cadencia diferente por estilo;
# - entrega mensagem pronta para a Interface.
#
# CADENCIA V1:
# EQUILIBRADO:
#   mensagem visivel = 8 s
#   intervalo vazio  = 3 s
#   novo inicio      = a cada 11 s
#
# PRESSAO / FEIRA:
#   mensagem visivel = 7 s
#   intervalo vazio  = 2 s
#   novo inicio      = a cada 9 s
#
# NAO FAZ:
# - nao le comentarios;
# - nao interfere no Live Coach;
# - nao inventa fatos;
# - nao cria novas informacoes de produto;
# - nao altera os modulos antigos;
# - nao decide audio. O futuro Audio Orchestrator fara isso.
# ============================================================

import asyncio
import copy
import math
import time
from collections import deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# 1. CONFIGURACOES
# ============================================================

SALES_DECISION_VERSION = "1.0"
SALES_DECISION_TIMEZONE = ZoneInfo("America/Araguaina")
SALES_DECISION_HISTORY_MAXLEN = 300
SALES_DECISION_RECENT_MAXLEN = 20

STYLE_BALANCED = "equilibrado"
STYLE_PRESSURE = "pressao_feira"
ALLOWED_STYLES = {STYLE_BALANCED, STYLE_PRESSURE}

STYLE_CONFIG = {
    STYLE_BALANCED: {
        "display_seconds": 8.0,
        "interval_seconds": 3.0,
        "cycle_seconds": 11.0,
        "recent_category_window": 2,
        "default_repeat_cooldown": 33.0,
        "category_cooldowns": {
            "price_value": 44.0,
            "cta": 55.0,
            "social_proof": 55.0,
            "promotion": 44.0,
            "reinforcement": 33.0,
            "demonstration": 22.0,
            "benefit": 33.0,
            "transition": 22.0,
        },
        "rotation": [
            "presentation",
            "demonstration",
            "benefit",
            "characteristic",
            "usage",
            "price_value",
            "demonstration",
            "social_proof",
            "preventive_objection",
            "benefit",
            "cta",
            "reinforcement",
            "demonstration",
            "price_value",
            "cta",
            "transition",
        ],
    },
    STYLE_PRESSURE: {
        "display_seconds": 7.0,
        "interval_seconds": 2.0,
        "cycle_seconds": 9.0,
        "recent_category_window": 1,
        "default_repeat_cooldown": 18.0,
        "category_cooldowns": {
            "price_value": 27.0,
            "cta": 36.0,
            "social_proof": 36.0,
            "promotion": 27.0,
            "reinforcement": 18.0,
            "demonstration": 9.0,
            "benefit": 18.0,
            "transition": 18.0,
        },
        "rotation": [
            "presentation",
            "demonstration",
            "benefit",
            "price_value",
            "cta",
            "characteristic",
            "demonstration",
            "social_proof",
            "price_value",
            "preventive_objection",
            "benefit",
            "cta",
            "reinforcement",
            "demonstration",
            "price_value",
            "cta",
            "transition",
        ],
    },
}

# Categorias que nao devem ser aceleradas mesmo quando ha poucos candidatos.
HARD_REPEAT_CATEGORIES = {
    "price_value",
    "cta",
    "promotion",
    "warning",
}

# Categorias que merecem atencao assim que aparecem.
PRIORITY_CATEGORIES = {
    "warning": 1000,
    "promotion": 180,
    "availability": 90,
}


# ============================================================
# 2. ESTADO / FILA / HISTORICO
# ============================================================

sales_decision_output_queue = asyncio.Queue()
sales_decision_history = deque(maxlen=SALES_DECISION_HISTORY_MAXLEN)
sales_decision_recent = deque(maxlen=SALES_DECISION_RECENT_MAXLEN)

sales_decision_state = {
    "version": SALES_DECISION_VERSION,
    "running": False,
    "style": STYLE_BALANCED,
    "current_product_key": None,
    "current_bundle": None,
    "candidate_count": 0,
    "active_message": None,
    "next_message_monotonic": None,
    "product_started_monotonic": None,
    "rotation_index": 0,
    "emitted_messages": 0,
    "product_changes": 0,
    "bundle_updates": 0,
    "style_updates": 0,
    "clears": 0,
    "last_input_at": None,
    "last_output_at": None,
    "last_error": None,
}

_candidate_stats = {}
_group_stats = {}
_category_stats = {}
_stop_requested = False
_output_seq = 0


# ============================================================
# 3. UTILITARIOS
# ============================================================

def _sd_copy(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _sd_now():
    return datetime.now(SALES_DECISION_TIMEZONE)


def _sd_now_iso():
    return _sd_now().isoformat()


def _sd_style_config(style=None):
    style = style or sales_decision_state.get("style") or STYLE_BALANCED
    return STYLE_CONFIG.get(style, STYLE_CONFIG[STYLE_BALANCED])


def _sd_normalize_style(style):
    value = str(style or "").strip().lower()
    table = str.maketrans({
        "Ã¡": "a", "Ã ": "a", "Ã¢": "a", "Ã£": "a",
        "Ã©": "e", "Ãª": "e", "Ã­": "i",
        "Ã³": "o", "Ã´": "o", "Ãµ": "o",
        "Ãº": "u", "Ã§": "c",
    })
    value = value.translate(table).replace("/", "_").replace(" ", "_")
    while "__" in value:
        value = value.replace("__", "_")

    aliases = {
        "equilibrado": STYLE_BALANCED,
        "balanced": STYLE_BALANCED,
        "pressao": STYLE_PRESSURE,
        "pressao_feira": STYLE_PRESSURE,
        "feira": STYLE_PRESSURE,
        "pressure": STYLE_PRESSURE,
    }
    resolved = aliases.get(value, value)
    if resolved not in ALLOWED_STYLES:
        raise ValueError("Estilo invalido. Use 'equilibrado' ou 'pressao_feira'.")
    return resolved


def _sd_product_key(bundle):
    if not isinstance(bundle, dict):
        return None
    key = bundle.get("product_key")
    if key:
        return tuple(str(x) for x in key)
    identity = bundle.get("identity") or {}
    platform = identity.get("platform")
    shop_id = identity.get("shop_id")
    item_id = identity.get("item_id")
    if platform is None or shop_id is None or item_id is None:
        return None
    return (str(platform), str(shop_id), str(item_id))


def _sd_candidates(bundle=None):
    bundle = bundle or sales_decision_state.get("current_bundle")
    if not isinstance(bundle, dict):
        return []
    candidates = bundle.get("candidates") or []
    return [x for x in candidates if isinstance(x, dict)]


def _sd_candidate_text(candidate, style=None):
    style = style or sales_decision_state.get("style") or STYLE_BALANCED
    texts = candidate.get("texts") or {}
    return (
        texts.get(style)
        or candidate.get("selected_text")
        or texts.get(STYLE_BALANCED)
    )


def _sd_candidate_stat(candidate_id):
    return _candidate_stats.setdefault(candidate_id, {
        "count": 0,
        "last_shown_monotonic": None,
    })


def _sd_group_stat(group):
    return _group_stats.setdefault(group, {
        "count": 0,
        "last_shown_monotonic": None,
    })


def _sd_category_stat(category):
    return _category_stats.setdefault(category, {
        "count": 0,
        "last_shown_monotonic": None,
    })


def _sd_repeat_cooldown(candidate):
    config = _sd_style_config()
    category = candidate.get("category")
    return float(
        (config.get("category_cooldowns") or {}).get(
            category,
            config.get("default_repeat_cooldown", 30.0),
        )
    )


def _sd_recent_categories():
    return [x.get("category") for x in sales_decision_recent if x.get("category")]


def _sd_recent_candidate_ids():
    return [x.get("candidate_id") for x in sales_decision_recent if x.get("candidate_id")]


# ============================================================
# 4. ELEGIBILIDADE / REPETICAO
# ============================================================

def _sd_candidate_eligible(candidate, now_monotonic, relaxed=False):
    candidate_id = candidate.get("candidate_id")
    category = candidate.get("category")
    repeat_group = candidate.get("repeat_group") or candidate_id or category
    can_repeat = bool(candidate.get("can_repeat"))

    if not candidate_id or not _sd_candidate_text(candidate):
        return False

    stat = _sd_candidate_stat(candidate_id)
    group_stat = _sd_group_stat(repeat_group)

    # Nunca exibido: elegivel imediatamente.
    if stat["count"] <= 0:
        return True

    # Candidato declarado como nao repetivel.
    if not can_repeat:
        return False

    last_candidate = stat.get("last_shown_monotonic")
    last_group = group_stat.get("last_shown_monotonic")
    last = max(
        x for x in [last_candidate, last_group] if x is not None
    ) if any(x is not None for x in [last_candidate, last_group]) else None

    if last is None:
        return True

    elapsed = now_monotonic - last
    cooldown = _sd_repeat_cooldown(candidate)

    if elapsed >= cooldown:
        return True

    # Em produtos com poucos candidatos, podemos flexibilizar apenas
    # categorias seguras. Preco, CTA, promocao e warning mantem cooldown.
    if relaxed and category not in HARD_REPEAT_CATEGORIES:
        minimum = _sd_style_config().get("cycle_seconds", 10.0)
        return elapsed >= minimum

    return False


def _sd_candidate_score(candidate, desired_category, now_monotonic):
    category = candidate.get("category")
    candidate_id = candidate.get("candidate_id")
    importance = int(candidate.get("importance_hint") or 60)
    stat = _sd_candidate_stat(candidate_id)
    category_stat = _sd_category_stat(category)

    score = float(importance)

    # Novidade e pouca repeticao.
    if stat["count"] == 0:
        score += 55.0
    else:
        score -= min(30.0, stat["count"] * 6.0)

    # Categoria desejada pela rotacao.
    if category == desired_category:
        score += 85.0

    # Warnings/promocoes merecem atencao cedo.
    score += PRIORITY_CATEGORIES.get(category, 0)

    # Nao repetir a mesma categoria em sequencia quando existem alternativas.
    recent = _sd_recent_categories()
    recent_window = int(_sd_style_config().get("recent_category_window", 2))
    if category in recent[-recent_window:]:
        score -= 45.0

    # Quanto mais tempo uma categoria nao aparece, mais ela sobe.
    last_category = category_stat.get("last_shown_monotonic")
    if last_category is not None:
        score += min(24.0, max(0.0, (now_monotonic - last_category) / 5.0))

    # Pequeno desempate por menor quantidade de exibicoes do grupo.
    group = candidate.get("repeat_group") or candidate_id
    score -= min(15.0, _sd_group_stat(group)["count"] * 2.0)

    return score


# ============================================================
# 5. SELECAO DO PROXIMO BLOCO
# ============================================================

def _sd_desired_category():
    config = _sd_style_config()
    rotation = config.get("rotation") or ["demonstration", "benefit", "cta"]
    index = int(sales_decision_state.get("rotation_index") or 0)
    return rotation[index % len(rotation)]


def _sd_advance_rotation(selected_category=None):
    config = _sd_style_config()
    rotation = config.get("rotation") or []
    if not rotation:
        return

    current = int(sales_decision_state.get("rotation_index") or 0)

    # Se encontramos a categoria desejada, avanca um passo.
    desired = rotation[current % len(rotation)]
    if selected_category == desired:
        sales_decision_state["rotation_index"] = (current + 1) % len(rotation)
        return

    # Se foi uma prioridade fora da rotacao, mantem o passo desejado.
    if selected_category in {"warning", "promotion", "availability"}:
        return

    # Caso contrario avanca para nao ficar preso tentando uma categoria ausente.
    sales_decision_state["rotation_index"] = (current + 1) % len(rotation)


def _sd_select_candidate(now_monotonic):
    candidates = _sd_candidates()
    if not candidates:
        return None

    desired = _sd_desired_category()

    # 1. Elegibilidade normal.
    eligible = [
        c for c in candidates
        if _sd_candidate_eligible(c, now_monotonic, relaxed=False)
    ]

    # 2. Se nao houver nada, flexibiliza apenas categorias seguras.
    if not eligible:
        eligible = [
            c for c in candidates
            if _sd_candidate_eligible(c, now_monotonic, relaxed=True)
        ]

    if not eligible:
        return None

    # Warning novo sempre vem primeiro.
    warnings = [c for c in eligible if c.get("category") == "warning"]
    if warnings:
        return max(
            warnings,
            key=lambda c: _sd_candidate_score(c, desired, now_monotonic),
        )

    # Na entrada de um produto, prioriza apresentacao se ela existir.
    if sales_decision_state.get("emitted_messages", 0) == 0:
        presentations = [c for c in eligible if c.get("category") == "presentation"]
        if presentations:
            return max(
                presentations,
                key=lambda c: _sd_candidate_score(c, desired, now_monotonic),
            )

    # Depois, segue pontuacao + rotacao.
    return max(
        eligible,
        key=lambda c: _sd_candidate_score(c, desired, now_monotonic),
    )


# ============================================================
# 6. EMISSAO PARA A INTERFACE
# ============================================================

def _sd_build_message(candidate, now_monotonic):
    config = _sd_style_config()
    style = sales_decision_state.get("style") or STYLE_BALANCED
    display_seconds = float(config["display_seconds"])
    interval_seconds = float(config["interval_seconds"])
    cycle_seconds = float(config["cycle_seconds"])

    now_dt = _sd_now()
    expires_dt = now_dt + timedelta(seconds=display_seconds)
    next_dt = now_dt + timedelta(seconds=cycle_seconds)

    return {
        "type": "sales_coach_message",
        "product_key": _sd_copy(sales_decision_state.get("current_product_key")),
        "candidate_id": candidate.get("candidate_id"),
        "category": candidate.get("category"),
        "topic_key": candidate.get("topic_key"),
        "message": _sd_candidate_text(candidate, style),
        "style": style,
        "importance": int(candidate.get("importance_hint") or 60),
        "risk_level": candidate.get("risk_level") or "low",
        "objective": candidate.get("objective"),
        "action": candidate.get("action"),
        "evidence_paths": _sd_copy(candidate.get("evidence_paths") or []),
        "restrictions": _sd_copy(candidate.get("restrictions") or []),
        "display_seconds": display_seconds,
        "interval_seconds": interval_seconds,
        "cycle_seconds": cycle_seconds,
        "started_at": now_dt.isoformat(),
        "expires_at": expires_dt.isoformat(),
        "next_message_not_before": next_dt.isoformat(),
        "_started_monotonic": float(now_monotonic),
        "_expires_monotonic": float(now_monotonic + display_seconds),
        "_next_monotonic": float(now_monotonic + cycle_seconds),
    }


def _sd_emit_message(message):
    global _output_seq

    _output_seq += 1
    stored_message = _sd_copy(message)
    stored_message["seq"] = _output_seq

    # O estado interno guarda monotonic para controlar exibicao/intervalo.
    sales_decision_state["active_message"] = _sd_copy(stored_message)

    # Historico e fila entregam apenas o contrato publico da Interface.
    public_message = _sd_copy(stored_message)
    for key in ["_started_monotonic", "_expires_monotonic", "_next_monotonic"]:
        public_message.pop(key, None)

    sales_decision_history.append(_sd_copy(public_message))
    try:
        sales_decision_output_queue.put_nowait(_sd_copy(public_message))
    except Exception:
        pass
    sales_decision_state["next_message_monotonic"] = message.get("_next_monotonic")
    sales_decision_state["emitted_messages"] += 1
    sales_decision_state["last_output_at"] = time.time()

    sales_decision_recent.append({
        "candidate_id": message.get("candidate_id"),
        "category": message.get("category"),
        "repeat_group": message.get("repeat_group"),
        "shown_monotonic": message.get("_started_monotonic"),
    })

    return public_message


def _sd_register_shown(candidate, now_monotonic):
    candidate_id = candidate.get("candidate_id")
    category = candidate.get("category")
    group = candidate.get("repeat_group") or candidate_id or category

    stat = _sd_candidate_stat(candidate_id)
    stat["count"] += 1
    stat["last_shown_monotonic"] = now_monotonic

    gstat = _sd_group_stat(group)
    gstat["count"] += 1
    gstat["last_shown_monotonic"] = now_monotonic

    cstat = _sd_category_stat(category)
    cstat["count"] += 1
    cstat["last_shown_monotonic"] = now_monotonic


def sales_decision_generate_due(now_monotonic=None, force=False):
    now_monotonic = time.monotonic() if now_monotonic is None else float(now_monotonic)

    if not sales_decision_state.get("current_bundle"):
        return None

    next_due = sales_decision_state.get("next_message_monotonic")
    if not force and next_due is not None and now_monotonic < float(next_due):
        return None

    candidate = _sd_select_candidate(now_monotonic)
    if candidate is None:
        return None

    message = _sd_build_message(candidate, now_monotonic)
    message["repeat_group"] = candidate.get("repeat_group")

    _sd_register_shown(candidate, now_monotonic)
    _sd_advance_rotation(candidate.get("category"))

    return _sd_emit_message(message)


# ============================================================
# 7. MENSAGEM ATUAL / INTERVALO VAZIO
#
# A Interface pode consultar esta funcao.
# Depois do tempo de exibicao ela retorna None, mesmo que a
# proxima mensagem ainda esteja aguardando o intervalo.
# ============================================================

def sales_decision_current_message(now_monotonic=None):
    now_monotonic = time.monotonic() if now_monotonic is None else float(now_monotonic)
    message = sales_decision_state.get("active_message")
    if not isinstance(message, dict):
        return None

    expires = message.get("_expires_monotonic")
    if expires is None:
        # Se veio de um estado serializado, usa expires_at apenas como informacao;
        # durante runtime normal sempre existe _expires_monotonic.
        return _sd_copy(message)

    if now_monotonic >= float(expires):
        return None

    result = _sd_copy(message)
    for key in ["_started_monotonic", "_expires_monotonic", "_next_monotonic"]:
        result.pop(key, None)
    return result


def sales_decision_is_in_blank_interval(now_monotonic=None):
    now_monotonic = time.monotonic() if now_monotonic is None else float(now_monotonic)
    message = sales_decision_state.get("active_message")
    if not isinstance(message, dict):
        return False
    expires = message.get("_expires_monotonic")
    next_due = sales_decision_state.get("next_message_monotonic")
    return (
        expires is not None
        and next_due is not None
        and now_monotonic >= float(expires)
        and now_monotonic < float(next_due)
    )


# ============================================================
# 8. CLEAR PARA TROCA/FIM DO PRODUTO
# ============================================================

def _sd_emit_clear(reason):
    global _output_seq

    _output_seq += 1
    event = {
        "seq": _output_seq,
        "type": "sales_coach_cleared",
        "reason": reason,
        "product_key": _sd_copy(sales_decision_state.get("current_product_key")),
        "emitted_at": _sd_now_iso(),
    }

    sales_decision_history.append(_sd_copy(event))
    try:
        sales_decision_output_queue.put_nowait(_sd_copy(event))
    except Exception:
        pass

    sales_decision_state["active_message"] = None
    sales_decision_state["last_output_at"] = time.time()
    sales_decision_state["clears"] += 1
    return event


def _sd_reset_product_runtime(now_monotonic=None):
    now_monotonic = time.monotonic() if now_monotonic is None else float(now_monotonic)
    _candidate_stats.clear()
    _group_stats.clear()
    _category_stats.clear()
    sales_decision_recent.clear()
    sales_decision_state["rotation_index"] = 0
    sales_decision_state["emitted_messages"] = 0
    sales_decision_state["product_started_monotonic"] = now_monotonic
    sales_decision_state["next_message_monotonic"] = now_monotonic
    sales_decision_state["active_message"] = None


# ============================================================
# 9. RECEBER SAIDA DO PRODUCT SALES BUILDER
# ============================================================

def sales_decision_process_builder_output(builder_output, now_monotonic=None):
    now_monotonic = time.monotonic() if now_monotonic is None else float(now_monotonic)

    if not isinstance(builder_output, dict):
        raise TypeError("Saida do Product Sales Builder deve ser um dicionario.")

    output_type = builder_output.get("type")
    allowed = {
        "sales_candidates_started",
        "sales_candidates_changed",
        "sales_candidates_updated",
        "sales_candidates_style_updated",
        "sales_candidates_cleared",
    }
    if output_type not in allowed:
        return None

    sales_decision_state["last_input_at"] = time.time()
    sales_decision_state["last_error"] = None

    if output_type == "sales_candidates_cleared":
        event = _sd_emit_clear("product_cleared")
        sales_decision_state["current_product_key"] = None
        sales_decision_state["current_bundle"] = None
        sales_decision_state["candidate_count"] = 0
        sales_decision_state["next_message_monotonic"] = None
        _candidate_stats.clear()
        _group_stats.clear()
        _category_stats.clear()
        sales_decision_recent.clear()
        return event

    bundle = builder_output.get("bundle")
    if not isinstance(bundle, dict):
        raise ValueError("Saida do Builder sem bundle valido.")

    new_key = _sd_product_key(bundle)
    if new_key is None:
        raise ValueError("Bundle sem product_key valido.")

    bundle_style = bundle.get("selected_style")
    if bundle_style in ALLOWED_STYLES:
        sales_decision_state["style"] = bundle_style

    old_key = sales_decision_state.get("current_product_key")
    is_new_product = old_key != new_key

    if output_type in {"sales_candidates_started", "sales_candidates_changed"} or is_new_product:
        if old_key is not None and old_key != new_key:
            _sd_emit_clear("product_changed")
            sales_decision_state["product_changes"] += 1

        sales_decision_state["current_product_key"] = new_key
        sales_decision_state["current_bundle"] = _sd_copy(bundle)
        sales_decision_state["candidate_count"] = len(_sd_candidates(bundle))
        _sd_reset_product_runtime(now_monotonic)

        return {
            "status": "new_product",
            "product_key": new_key,
            "candidate_count": sales_decision_state["candidate_count"],
            "next_message_due": now_monotonic,
        }

    # Mesmo produto atualizado: preserva memoria de exibicao.
    sales_decision_state["current_bundle"] = _sd_copy(bundle)
    sales_decision_state["candidate_count"] = len(_sd_candidates(bundle))
    sales_decision_state["bundle_updates"] += 1

    if output_type == "sales_candidates_style_updated":
        sales_decision_state["style_updates"] += 1

    # Se ainda nao havia agenda, libera imediatamente.
    if sales_decision_state.get("next_message_monotonic") is None:
        sales_decision_state["next_message_monotonic"] = now_monotonic

    return {
        "status": "bundle_updated",
        "product_key": new_key,
        "candidate_count": sales_decision_state["candidate_count"],
        "style": sales_decision_state.get("style"),
    }


# ============================================================
# 10. ESTILO
#
# Pode ser chamado pela futura Interface.
# Opcionalmente sincroniza o Product Sales Builder.
# ============================================================

def sales_decision_set_style(style, sync_builder=True):
    normalized = _sd_normalize_style(style)
    old = sales_decision_state.get("style")
    sales_decision_state["style"] = normalized

    if old != normalized:
        sales_decision_state["style_updates"] += 1

    # O Builder ja guarda os dois textos, entao o Decision consegue
    # operar imediatamente. A sincronizacao mantem todo o fluxo coerente.
    if sync_builder and "product_sales_builder_set_style" in globals():
        setter = globals().get("product_sales_builder_set_style")
        if callable(setter):
            try:
                setter(normalized, emit_update=True)
            except Exception:
                # Nao derruba o Decision por falha de sincronizacao de config.
                pass

    return {
        "style": normalized,
        "display_seconds": _sd_style_config(normalized)["display_seconds"],
        "interval_seconds": _sd_style_config(normalized)["interval_seconds"],
        "cycle_seconds": _sd_style_config(normalized)["cycle_seconds"],
    }


# ============================================================
# 11. SUPERVISOR CONTINUO
#
# Um unico consumidor da fila do Product Sales Builder.
# Mesmo sem novos eventos, continua emitindo orientacoes
# conforme a cadencia do produto atual.
# ============================================================

async def sales_decision_supervisor():
    global _stop_requested

    if (
        "product_sales_builder_next_output" not in globals()
        or not callable(globals().get("product_sales_builder_next_output"))
    ):
        raise RuntimeError(
            "Product Sales Builder nao esta carregado. "
            "Execute primeiro o Product Sales Builder V1."
        )

    _stop_requested = False
    sales_decision_state["running"] = True
    sales_decision_state["last_error"] = None

    try:
        while not _stop_requested:
            now = time.monotonic()

            # Emite se chegou o momento.
            try:
                sales_decision_generate_due(now_monotonic=now)
            except Exception as exc:
                sales_decision_state["last_error"] = str(exc)

            next_due = sales_decision_state.get("next_message_monotonic")
            if next_due is None:
                timeout = 1.0
            else:
                timeout = max(0.05, min(1.0, float(next_due) - time.monotonic()))

            try:
                builder_output = await product_sales_builder_next_output(timeout=timeout)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                sales_decision_state["last_error"] = str(exc)
                await asyncio.sleep(0.2)
                continue

            try:
                sales_decision_process_builder_output(
                    builder_output,
                    now_monotonic=time.monotonic(),
                )
                # Produto novo pode emitir imediatamente.
                sales_decision_generate_due(now_monotonic=time.monotonic())
            except Exception as exc:
                sales_decision_state["last_error"] = str(exc)

    finally:
        sales_decision_state["running"] = False


async def executar_sales_decision_coach():
    return await sales_decision_supervisor()


def sales_decision_stop():
    global _stop_requested
    _stop_requested = True


# ============================================================
# 12. API PUBLICA PARA A INTERFACE
# ============================================================

async def sales_decision_next_output(timeout=None):
    if timeout is None:
        return await sales_decision_output_queue.get()
    return await asyncio.wait_for(
        sales_decision_output_queue.get(),
        timeout=float(timeout),
    )


def sales_decision_recent_outputs(limit=30):
    try:
        limit = max(1, int(limit))
    except Exception:
        limit = 30

    result = list(sales_decision_history)[-limit:]
    cleaned = _sd_copy(result)
    for item in cleaned:
        if isinstance(item, dict):
            for key in ["_started_monotonic", "_expires_monotonic", "_next_monotonic"]:
                item.pop(key, None)
    return cleaned


def sales_decision_status(now_monotonic=None):
    now_monotonic = time.monotonic() if now_monotonic is None else float(now_monotonic)
    config = _sd_style_config()
    active = sales_decision_current_message(now_monotonic)

    return {
        "version": sales_decision_state.get("version"),
        "running": sales_decision_state.get("running"),
        "style": sales_decision_state.get("style"),
        "display_seconds": config.get("display_seconds"),
        "interval_seconds": config.get("interval_seconds"),
        "cycle_seconds": config.get("cycle_seconds"),
        "current_product_key": _sd_copy(sales_decision_state.get("current_product_key")),
        "candidate_count": sales_decision_state.get("candidate_count"),
        "emitted_messages": sales_decision_state.get("emitted_messages"),
        "product_changes": sales_decision_state.get("product_changes"),
        "bundle_updates": sales_decision_state.get("bundle_updates"),
        "style_updates": sales_decision_state.get("style_updates"),
        "display_active": active is not None,
        "blank_interval": sales_decision_is_in_blank_interval(now_monotonic),
        "current_message": active,
        "history_size": len(sales_decision_history),
        "output_queue_size": sales_decision_output_queue.qsize(),
        "last_error": sales_decision_state.get("last_error"),
    }


# ============================================================
# 13. RESET
# ============================================================

def sales_decision_reset(
    clear_history=True,
    clear_output_queue=True,
    keep_style=True,
):
    global _stop_requested
    global _output_seq

    style = (
        sales_decision_state.get("style")
        if keep_style
        else STYLE_BALANCED
    )

    _stop_requested = False
    _output_seq = 0
    _candidate_stats.clear()
    _group_stats.clear()
    _category_stats.clear()
    sales_decision_recent.clear()

    sales_decision_state.update({
        "version": SALES_DECISION_VERSION,
        "running": False,
        "style": style,
        "current_product_key": None,
        "current_bundle": None,
        "candidate_count": 0,
        "active_message": None,
        "next_message_monotonic": None,
        "product_started_monotonic": None,
        "rotation_index": 0,
        "emitted_messages": 0,
        "product_changes": 0,
        "bundle_updates": 0,
        "style_updates": 0,
        "clears": 0,
        "last_input_at": None,
        "last_output_at": None,
        "last_error": None,
    })

    if clear_history:
        sales_decision_history.clear()

    if clear_output_queue:
        try:
            while True:
                sales_decision_output_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass


# ============================================================
# 14. DIAGNOSTICO
# ============================================================

def mostrar_sales_decision_coach(now_monotonic=None):
    status = sales_decision_status(now_monotonic)

    print("=" * 76)
    print("AGCN LIVE - SALES DECISION COACH V1.0")
    print("=" * 76)
    print("Executando:", status["running"])
    print("Estilo:", status["style"])
    print("Exibicao:", status["display_seconds"], "s")
    print("Intervalo:", status["interval_seconds"], "s")
    print("Ciclo total:", status["cycle_seconds"], "s")
    print("Produto:", status["current_product_key"])
    print("Candidatos:", status["candidate_count"])
    print("Mensagens emitidas:", status["emitted_messages"])
    print("Intervalo vazio:", status["blank_interval"])
    print("Erro:", status["last_error"])
    print()

    current = status.get("current_message")
    if current:
        print("SALES COACH AGORA")
        print("Categoria:", current.get("category"))
        print("Mensagem:", current.get("message"))
        print("Expira em:", current.get("expires_at"))
    else:
        print("SALES COACH AGORA: sem mensagem visivel")


# ============================================================
# 15. AUTO-TESTE INTERNO
# ============================================================

def sales_decision_self_test():
    checks = 0
    sales_decision_reset(
        clear_history=True,
        clear_output_queue=True,
        keep_style=False,
    )

    def cand(cid, category, importance, can_repeat=True, group=None):
        return {
            "candidate_id": cid,
            "category": category,
            "topic_key": cid,
            "objective": "teste",
            "action": "teste",
            "importance_hint": importance,
            "risk_level": "low",
            "can_repeat": can_repeat,
            "repeat_group": group or category,
            "evidence": [],
            "evidence_paths": [],
            "restrictions": [],
            "tags": [],
            "texts": {
                STYLE_BALANCED: f"EQ {cid}",
                STYLE_PRESSURE: f"PF {cid}",
            },
            "selected_style": STYLE_BALANCED,
            "selected_text": f"EQ {cid}",
        }

    bundle = {
        "product_key": ("shopee", "10", "20"),
        "identity": {
            "platform": "shopee",
            "shop_id": "10",
            "item_id": "20",
        },
        "selected_style": STYLE_BALANCED,
        "candidates": [
            cand("presentation", "presentation", 78, can_repeat=False),
            cand("demo1", "demonstration", 88, True, "demo"),
            cand("benefit1", "benefit", 82, True, "benefit"),
            cand("char1", "characteristic", 70, True, "char"),
            cand("usage1", "usage", 80, True, "usage"),
            cand("price1", "price_value", 90, True, "price"),
            cand("social1", "social_proof", 76, True, "social"),
            cand("objection1", "preventive_objection", 84, True, "obj"),
            cand("cta1", "cta", 86, True, "cta"),
            cand("reinforce1", "reinforcement", 68, True, "reinforce"),
            cand("transition1", "transition", 60, True, "transition"),
        ],
    }

    started = {
        "type": "sales_candidates_started",
        "bundle": bundle,
        "previous_bundle": None,
    }

    result = sales_decision_process_builder_output(started, now_monotonic=1000.0)
    assert result["status"] == "new_product"; checks += 1
    assert sales_decision_state["candidate_count"] == 11; checks += 1

    first = sales_decision_generate_due(now_monotonic=1000.0)
    assert first is not None; checks += 1
    assert first["category"] == "presentation"; checks += 1
    assert first["display_seconds"] == 8.0; checks += 1
    assert first["interval_seconds"] == 3.0; checks += 1
    assert first["cycle_seconds"] == 11.0; checks += 1
    assert first["message"].startswith("EQ "); checks += 1

    # 7.9s: ainda visivel.
    assert sales_decision_current_message(now_monotonic=1007.9) is not None; checks += 1
    # 8s: some da tela.
    assert sales_decision_current_message(now_monotonic=1008.0) is None; checks += 1
    assert sales_decision_is_in_blank_interval(now_monotonic=1009.0) is True; checks += 1
    # Antes de 11s, nada novo.
    assert sales_decision_generate_due(now_monotonic=1010.9) is None; checks += 1

    second = sales_decision_generate_due(now_monotonic=1011.0)
    assert second is not None; checks += 1
    assert second["candidate_id"] != first["candidate_id"]; checks += 1

    # Pressao / Feira: 7 + 2 = 9.
    style_info = sales_decision_set_style(STYLE_PRESSURE, sync_builder=False)
    assert style_info["display_seconds"] == 7.0; checks += 1
    assert style_info["interval_seconds"] == 2.0; checks += 1
    assert style_info["cycle_seconds"] == 9.0; checks += 1

    # Forca proximo apenas para testar texto/cadencia do novo estilo.
    pressure = sales_decision_generate_due(now_monotonic=1020.0, force=True)
    assert pressure is not None; checks += 1
    assert pressure["style"] == STYLE_PRESSURE; checks += 1
    assert pressure["display_seconds"] == 7.0; checks += 1
    assert pressure["interval_seconds"] == 2.0; checks += 1
    assert pressure["message"].startswith("PF "); checks += 1

    # Mesmo produto atualizado preserva memoria.
    updated_bundle = _sd_copy(bundle)
    updated_bundle["selected_style"] = STYLE_PRESSURE
    updated_bundle["candidates"].append(
        cand("newfact", "characteristic", 95, False, "newfact")
    )
    update_result = sales_decision_process_builder_output({
        "type": "sales_candidates_updated",
        "bundle": updated_bundle,
    }, now_monotonic=1021.0)
    assert update_result["status"] == "bundle_updated"; checks += 1
    assert sales_decision_state["emitted_messages"] >= 3; checks += 1
    assert sales_decision_state["candidate_count"] == 12; checks += 1

    # Produto novo reseta sequencia.
    bundle2 = _sd_copy(bundle)
    bundle2["product_key"] = ("shopee", "10", "21")
    bundle2["identity"]["item_id"] = "21"
    changed = sales_decision_process_builder_output({
        "type": "sales_candidates_changed",
        "bundle": bundle2,
    }, now_monotonic=1100.0)
    assert changed["status"] == "new_product"; checks += 1
    assert sales_decision_state["emitted_messages"] == 0; checks += 1
    new_first = sales_decision_generate_due(now_monotonic=1100.0)
    assert new_first["category"] == "presentation"; checks += 1

    # Clear encerra produto e interface deve limpar painel.
    clear = sales_decision_process_builder_output({
        "type": "sales_candidates_cleared",
        "bundle": None,
    }, now_monotonic=1110.0)
    assert clear["type"] == "sales_coach_cleared"; checks += 1
    assert sales_decision_state["current_bundle"] is None; checks += 1
    assert sales_decision_current_message(now_monotonic=1110.0) is None; checks += 1

    # Warning novo tem prioridade sobre apresentacao.
    sales_decision_reset(clear_history=True, clear_output_queue=True, keep_style=False)
    warning_bundle = _sd_copy(bundle)
    warning_bundle["candidates"].append(
        cand("warn1", "warning", 99, False, "warning")
    )
    sales_decision_process_builder_output({
        "type": "sales_candidates_started",
        "bundle": warning_bundle,
    }, now_monotonic=1200.0)
    warning_msg = sales_decision_generate_due(now_monotonic=1200.0)
    assert warning_msg["category"] == "warning"; checks += 1

    return {
        "ok": True,
        "version": SALES_DECISION_VERSION,
        "checks": checks,
    }


# ============================================================
# 16. ORDEM NO COLAB
#
# 1. Worker Shopee Produto V1
# 2. Product Extractor V1
# 3. Product Sales Builder V1
# 4. Sales Decision Coach V1
#
# Depois:
# tarefa_extractor = asyncio.create_task(executar_product_extractor())
# tarefa_builder = asyncio.create_task(executar_product_sales_builder())
# tarefa_sales_decision = asyncio.create_task(executar_sales_decision_coach())
#
# A Interface consumira/consultara:
# - sales_decision_next_output()
# - sales_decision_current_message()
# - sales_decision_status()
#
# NAO existe await automatico neste arquivo.
# ============================================================

print("AGCN Sales Decision Coach V1.0 carregado.")
