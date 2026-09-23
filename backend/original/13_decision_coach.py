# ============================================================
# AGCN LIVE - DECISION COACH V1.2
#
# ARQUITETURA ALVO:
#
# Coach Produto V1.1 --------------------\
# Coach Comercial V1 ---------------------\
# Coach Objecoes/Pos-compra V1 ------------> Context Fusion V1.1.1
# Worker Coach Audiencia V1 ---------------/          |
#                                                     v
#                                              DECISION COACH V1.2
#                                               /            \
#                                      Coach Armazenamento   Interface V7.1
#
# RESPONSABILIDADES:
#
# - receber SOMENTE contexto consolidado do Context Fusion
# - decidir se existe algo que merece aviso ao vendedor
# - calcular prioridade operacional (0..100)
# - definir TTL da decisao
# - definir cooldown por assunto
# - deduplicar sinais/contextos equivalentes
# - evitar repeticao de aviso ja exibido recentemente
# - respeitar configuracoes de avisos do usuario
# - registrar decisoes e saidas no Coach Armazenamento
# - publicar na Interface quando a decisao for "display"
# - preservar as evidencias que levaram a decisao
#
# NAO FAZ:
#
# - nao consome diretamente filas dos Coaches especializados
# - nao consome diretamente o Worker Audiencia
# - nao reclassifica comentarios brutos
# - nao inventa dados de produto, preco, estoque, frete ou promocao
# - nao confirma compra/pagamento apenas porque o usuario relatou
# - nao atribui causalidade entre audiencia e comentarios sem evidencia
# - nao substitui o Context Fusion
# - nao altera dados do Live Engine
#
# IMPORTANTE SOBRE A ORDEM:
#
# Este modulo foi criado antes do Context Fusion V1.1.
# Ele pode ser carregado e testado isoladamente agora.
# A integracao automatica com a LIVE so e anexada quando a funcao
# `context_fusion_next_output` existir no runtime.
#
# Quando o Context Fusion V1.1 for criado, a ordem oficial devera ser:
#
# ... -> Coach Objecoes -> Worker Audiencia -> Context Fusion
#     -> Decision Coach -> Interface V7.1
#
# ============================================================

import asyncio
import copy
import hashlib
import inspect
import json
import re
import time
import unicodedata
import uuid

from collections import Counter, deque
from datetime import datetime
from zoneinfo import ZoneInfo


# ============================================================
# 1. DEPENDENCIAS JA EXISTENTES
#
# Context Fusion NAO e dependencia obrigatoria no momento da carga,
# porque este modulo esta sendo construido antes dele.
# ============================================================

_DC_REQUIRED = [
    "live_engine_is_running",
    "live_engine_platform",
    "executar_shopee_com_live_engine",
    "executar_tiktok_com_live_engine",
    "coach_storage_save_decision",
    "coach_storage_save_output",
    "coach_storage_recent_outputs",
    "coach_storage_was_recently_shown",
]

_DC_MISSING = [
    name
    for name in _DC_REQUIRED
    if name not in globals()
]

if _DC_MISSING:
    raise RuntimeError(
        "Execute primeiro o Live Engine V2 e o Coach Armazenamento V1. "
        "Faltando: " + ", ".join(_DC_MISSING)
    )


# ============================================================
# 2. CONFIGURACAO
# ============================================================

DECISION_COACH_VERSION = "1.2"
DECISION_COACH_TIMEZONE = ZoneInfo("America/Araguaina")

DECISION_COACH_OUTPUT_QUEUE_LIMIT = 5000
DECISION_COACH_MAX_HISTORY = 5000
DECISION_COACH_MAX_SEEN = 20000

# Quanto tempo um mesmo fingerprint fica protegido contra repeticao.
DECISION_COACH_DEDUPE_WINDOW_SECONDS = 45

# Janela usada para limitar quantidade de avisos mostrados.
DECISION_COACH_RATE_WINDOW_SECONDS = 60

# Compatibilidade com a V1.0 / modo customizado.
DECISION_COACH_DEFAULT_MIN_PRIORITY = 60
DECISION_COACH_DEFAULT_GLOBAL_COOLDOWN = 5
DECISION_COACH_DEFAULT_MAX_TIPS_PER_MINUTE = 12

# Contextos mais velhos do que isso sao tratados como expirados,
# salvo quando o Context Fusion fornecer TTL menor.
DECISION_COACH_MAX_CONTEXT_AGE_SECONDS = 90

# Sinais criticos podem romper cooldown global quando forem de outro assunto.
DECISION_COACH_CRITICAL_OVERRIDE_PRIORITY = 90

# A Interface V7.1 exibira uma orientacao por vez. O Decision Coach
# ja limita a cadencia para evitar criar uma fila visual desnecessaria.
DECISION_COACH_INTERFACE_DISPLAY_SECONDS = 8
DECISION_COACH_MIN_DISPLAY_GAP_SECONDS = 11.0

# ============================================================
# MODOS DE AVISO DA INTERFACE
#
# IMPORTANTE:
# "Todos" NAO significa repetir comentarios iguais.
# O Context Fusion V1.1 agrupa perguntas equivalentes antes daqui.
# O Decision Coach recebe uma situacao consolidada e decide se ela
# entra no modo escolhido pelo usuario.
#
# all       -> todas as perguntas/situacoes uteis DISTINTAS
# high      -> maior impacto comercial, repeticao ou necessidade
# essential -> somente sinais muito fortes/urgentes
# ============================================================

DECISION_COACH_ALERT_MODES = {
    "all": {
        "label": "Todos",
        "description": (
            "Mostra todas as perguntas e situacoes uteis distintas; "
            "perguntas equivalentes permanecem agrupadas pelo Context Fusion."
        ),
        "min_priority": 0,
        "global_cooldown_seconds": 5.0,
        "max_tips_per_minute": 12,
        "allow_audience_only": True,
        "show_viewer_stable": False,
    },
    "high": {
        "label": "Prioridade Alta",
        "description": (
            "Mostra situacoes com impacto comercial maior, intencao de compra, "
            "objecoes, pos-compra, perguntas repetidas ou temas naturalmente fortes."
        ),
        "min_priority": 75,
        "global_cooldown_seconds": 7.0,
        "max_tips_per_minute": 8,
        "allow_audience_only": True,
        "show_viewer_stable": False,
    },
    "essential": {
        "label": "Essenciais",
        "description": (
            "Mostra apenas situacoes muito fortes ou urgentes, como problemas "
            "criticos, forte barreira de compra ou contexto com prioridade extrema."
        ),
        "min_priority": 90,
        "global_cooldown_seconds": 10.0,
        "max_tips_per_minute": 6,
        "allow_audience_only": False,
        "show_viewer_stable": False,
    },
}

DECISION_COACH_ALERT_MODE_ALIASES = {
    "all": "all",
    "todos": "all",
    "todas": "all",
    "tudo": "all",
    "high": "high",
    "alta": "high",
    "prioridade_alta": "high",
    "prioridade alta": "high",
    "priority_high": "high",
    "essential": "essential",
    "essencial": "essential",
    "essenciais": "essential",
}

# Durante a fase de testes, "Todos" e o melhor padrao para que nenhuma
# pergunta util distinta desapareca silenciosamente. A Interface V7 podera
# enviar outro modo imediatamente, inclusive com a LIVE em andamento.
DECISION_COACH_DEFAULT_ALERT_MODE = "all"

# ============================================================
# 3. CONTRATO ESPERADO DO FUTURO CONTEXT FUSION V1
#
# O Decision Coach e tolerante a campos extras e a algumas variacoes,
# mas o Context Fusion que sera criado depois deve preferencialmente
# produzir os campos abaixo.
# ============================================================

DECISION_COACH_FUSION_CONTRACT = {
    "accepted_message_types": {
        "fusion_event",
        "fusion_signal",
        "context_snapshot",
    },
    "recommended_fields": [
        "output_id",
        "message_type",
        "platform",
        "live_id",
        "subject",
        "timestamp",
        "iso_time",
        "context_kind",
        "domains",
        "primary",
        "topics",
        "audience",
        "evidence",
        "strength",
        "requires_response",
    ],
}


# ============================================================
# 4. CONFIGURACOES DE AVISO DO USUARIO
#
# A Interface ainda nao possui controles visuais para tudo isso.
# A API abaixo ja deixa o Decision Coach preparado para quando
# essas opcoes forem expostas na interface.
# ============================================================

DECISION_COACH_DEFAULT_SETTINGS = {
    "enabled": True,
    "alert_mode": DECISION_COACH_DEFAULT_ALERT_MODE,
    "min_priority": DECISION_COACH_ALERT_MODES[
        DECISION_COACH_DEFAULT_ALERT_MODE
    ]["min_priority"],
    "global_cooldown_seconds": DECISION_COACH_ALERT_MODES[
        DECISION_COACH_DEFAULT_ALERT_MODE
    ]["global_cooldown_seconds"],
    "max_tips_per_minute": DECISION_COACH_ALERT_MODES[
        DECISION_COACH_DEFAULT_ALERT_MODE
    ]["max_tips_per_minute"],
    "allow_audience_only": DECISION_COACH_ALERT_MODES[
        DECISION_COACH_DEFAULT_ALERT_MODE
    ]["allow_audience_only"],
    "show_viewer_stable": DECISION_COACH_ALERT_MODES[
        DECISION_COACH_DEFAULT_ALERT_MODE
    ]["show_viewer_stable"],
    "categories": {
        "product": True,
        "commercial": True,
        "objections": True,
        "post_purchase": True,
        "audience": True,
        "mixed": True,
    },
}

decision_coach_settings = copy.deepcopy(
    DECISION_COACH_DEFAULT_SETTINGS
)


# ============================================================
# 5. ESTADO
# ============================================================

decision_coach_output_queue = None

decision_coach_output_history = deque(
    maxlen=DECISION_COACH_MAX_HISTORY
)
decision_coach_display_history = deque(
    maxlen=DECISION_COACH_MAX_HISTORY
)
decision_coach_suppressed_history = deque(
    maxlen=DECISION_COACH_MAX_HISTORY
)
decision_coach_input_history = deque(
    maxlen=DECISION_COACH_MAX_HISTORY
)

decision_coach_seen_input_ids = set()
decision_coach_seen_input_order = deque(
    maxlen=DECISION_COACH_MAX_SEEN
)

decision_coach_recent_fingerprints = {}
decision_coach_last_display_by_key = {}
decision_coach_display_times = deque()

decision_coach_running = False
decision_coach_platform = None
decision_coach_live_id = None
decision_coach_subject = None
decision_coach_started_at = None
decision_coach_last_error = None

decision_coach_runtime_attached = False

decision_coach_stats = Counter()


# ============================================================
# 6. UTILITARIOS
# ============================================================

def _dc_now_iso():
    return datetime.now(
        DECISION_COACH_TIMEZONE
    ).isoformat()


def _dc_number(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _dc_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _dc_bool(value):
    return bool(value)


def _dc_clamp(value, minimum=0.0, maximum=100.0):
    try:
        value = float(value)
    except Exception:
        value = float(minimum)
    return max(minimum, min(maximum, value))


def _dc_mojibake_score(value):
    """Pontua sinais tipicos de UTF-8 interpretado como Latin-1/CP1252."""
    value = str(value or "")
    markers = (
        "ÃÂ¡", "ÃÃ ", "ÃÂ¢", "ÃÂ£", "ÃÂ¤", "ÃÃ", "ÃÃ", "ÃÃ", "ÃÃ",
        "ÃÂ©", "ÃÃª", "ÃÃ«", "ÃÃ", "ÃÃ",
        "ÃÃ­", "ÃÃ", "ÃÂ³", "ÃÃ´", "ÃÃµ", "ÃÃ", "ÃÃ", "ÃÃ",
        "ÃÂº", "ÃÃ¼", "ÃÃ", "ÃÃ§", "ÃÃ",
        "Ã", "Ã¢â¬â", "Ã¢â¬â", "Ã¢â¬", "Ã¢â¬â¢", "Ã¢â¬Å", "Ã¢â¬ï¿½", "Ã¯Â¿Â½", "ï¿½",
    )
    return sum(value.count(marker) for marker in markers)


def _dc_repair_text(value):
    """Repara mojibake comum sem alterar texto UTF-8 que ja esta correto."""
    if value is None:
        return ""

    current = str(value)
    current = unicodedata.normalize("NFC", current)

    # Tentamos no maximo duas camadas, porque alguns textos podem ter sido
    # recodificados mais de uma vez ao atravessar notebook/HTML/JSON.
    for _ in range(2):
        current_score = _dc_mojibake_score(current)
        if current_score <= 0:
            break

        candidates = []
        for encoding in ("latin-1", "cp1252"):
            try:
                candidate = current.encode(encoding).decode("utf-8")
                candidate = unicodedata.normalize("NFC", candidate)
                candidates.append(candidate)
            except Exception:
                pass

        if not candidates:
            break

        candidate = min(candidates, key=_dc_mojibake_score)
        if _dc_mojibake_score(candidate) < current_score:
            current = candidate
        else:
            break

    return current


def _dc_safe_text(value):
    if value is None:
        return ""
    return _dc_repair_text(value).strip()


def _dc_local_hms(timestamp=None):
    if timestamp is None:
        timestamp = time.time()
    try:
        return datetime.fromtimestamp(
            float(timestamp),
            DECISION_COACH_TIMEZONE,
        ).strftime("%H:%M:%S")
    except Exception:
        return datetime.now(
            DECISION_COACH_TIMEZONE
        ).strftime("%H:%M:%S")

def _dc_slug(value):
    value = _dc_safe_text(value).lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_:\-]+", "", value)
    return value[:120]


def _dc_unique_strings(values):
    result = []
    seen = set()

    for value in values or []:
        value = _dc_safe_text(value)
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)

    return result


def _dc_queue_output(output):
    global decision_coach_output_queue

    if decision_coach_output_queue is None:
        return False

    try:
        decision_coach_output_queue.put_nowait(
            copy.deepcopy(output)
        )
        return True
    except asyncio.QueueFull:
        decision_coach_stats["dropped_outputs"] += 1
        return False
    except Exception:
        decision_coach_stats["dropped_outputs"] += 1
        return False


# ============================================================
# 6.1 MODOS DE AVISO
# ============================================================

def _dc_normalize_alert_mode(mode):
    raw = _dc_safe_text(mode).lower()
    raw = re.sub(r"\s+", " ", raw).strip()
    if not raw:
        return None
    return DECISION_COACH_ALERT_MODE_ALIASES.get(raw)


def decision_coach_alert_modes_info():
    return copy.deepcopy(DECISION_COACH_ALERT_MODES)


def decision_coach_get_alert_mode():
    return decision_coach_settings.get(
        "alert_mode",
        DECISION_COACH_DEFAULT_ALERT_MODE,
    )


def _dc_apply_alert_mode_to_settings(settings, mode):
    canonical = _dc_normalize_alert_mode(mode)
    if canonical is None:
        raise ValueError(
            "Modo de aviso invalido. Use: all/todos, high/prioridade alta "
            "ou essential/essenciais."
        )

    policy = DECISION_COACH_ALERT_MODES[canonical]
    settings["alert_mode"] = canonical
    settings["min_priority"] = int(policy["min_priority"])
    settings["global_cooldown_seconds"] = float(
        policy["global_cooldown_seconds"]
    )
    settings["max_tips_per_minute"] = int(
        policy["max_tips_per_minute"]
    )
    settings["allow_audience_only"] = bool(
        policy["allow_audience_only"]
    )
    settings["show_viewer_stable"] = bool(
        policy["show_viewer_stable"]
    )
    return settings


def decision_coach_set_alert_mode(mode):
    """API simples para a futura Interface V7."""
    global decision_coach_settings

    current = decision_coach_get_settings()
    _dc_apply_alert_mode_to_settings(current, mode)
    decision_coach_settings = current
    return decision_coach_get_settings()


# ============================================================
# 7. CONFIGURACOES PUBLICAS
# ============================================================

def decision_coach_get_settings():
    return copy.deepcopy(
        decision_coach_settings
    )


def decision_coach_reset_settings():
    global decision_coach_settings

    decision_coach_settings = copy.deepcopy(
        DECISION_COACH_DEFAULT_SETTINGS
    )

    return decision_coach_get_settings()


def decision_coach_update_settings(settings=None, **kwargs):
    """
    Atualiza configuracoes em runtime.

    Uso recomendado pela futura Interface V7:

        decision_coach_set_alert_mode("all")
        decision_coach_set_alert_mode("high")
        decision_coach_set_alert_mode("essential")

    A API antiga por numeros continua compativel. Se min_priority,
    cooldown ou limite forem alterados manualmente sem alert_mode,
    o estado passa a ser "custom".
    """

    global decision_coach_settings

    incoming = {}

    if isinstance(settings, dict):
        incoming.update(copy.deepcopy(settings))

    incoming.update(kwargs)

    current = decision_coach_get_settings()

    requested_mode = incoming.get("alert_mode")
    if requested_mode is not None:
        _dc_apply_alert_mode_to_settings(
            current,
            requested_mode,
        )

    if "enabled" in incoming:
        current["enabled"] = bool(incoming.get("enabled"))

    policy_keys = {
        "min_priority",
        "global_cooldown_seconds",
        "max_tips_per_minute",
        "allow_audience_only",
        "show_viewer_stable",
    }

    manual_policy_override = bool(
        policy_keys.intersection(incoming.keys())
    )

    if "min_priority" in incoming:
        current["min_priority"] = int(
            _dc_clamp(incoming.get("min_priority"), 0, 100)
        )

    if "global_cooldown_seconds" in incoming:
        current["global_cooldown_seconds"] = max(
            0.0,
            _dc_number(
                incoming.get("global_cooldown_seconds"),
                DECISION_COACH_DEFAULT_GLOBAL_COOLDOWN,
            ),
        )

    if "max_tips_per_minute" in incoming:
        current["max_tips_per_minute"] = max(
            1,
            _dc_int(
                incoming.get("max_tips_per_minute"),
                DECISION_COACH_DEFAULT_MAX_TIPS_PER_MINUTE,
            ),
        )

    if "allow_audience_only" in incoming:
        current["allow_audience_only"] = bool(
            incoming.get("allow_audience_only")
        )

    if "show_viewer_stable" in incoming:
        current["show_viewer_stable"] = bool(
            incoming.get("show_viewer_stable")
        )

    incoming_categories = incoming.get("categories")

    if isinstance(incoming_categories, dict):
        categories = current.setdefault("categories", {})
        for key in (
            "product",
            "commercial",
            "objections",
            "post_purchase",
            "audience",
            "mixed",
        ):
            if key in incoming_categories:
                categories[key] = bool(
                    incoming_categories.get(key)
                )

    # Ajustes numericos diretos continuam permitidos, mas deixam claro
    # que o preset foi customizado.
    if manual_policy_override and requested_mode is None:
        current["alert_mode"] = "custom"

    decision_coach_settings = current
    return decision_coach_get_settings()


# ============================================================
# 8. INFERIR DOMINIO
# ============================================================

def _dc_domain_from_category(category, source=None):
    category = _dc_safe_text(category)
    source = _dc_safe_text(source)

    if category in {
        "product_question",
        "demo_request",
    }:
        return "product"

    if category in {
        "commercial_question",
        "buying_intent",
        "purchase_completed",
        "commercial_observation",
    }:
        return "commercial"

    if category == "post_purchase_issue":
        return "post_purchase"

    if category in {
        "objection",
        "purchase_barrier",
    }:
        return "objections"

    if source == "product":
        return "product"

    if source == "commercial":
        return "commercial"

    if source == "objections":
        return "objections"

    if source == "audience":
        return "audience"

    return None


# ============================================================
# 9. EXTRAIR ITEM/TOPICO CANONICO
# ============================================================

def _dc_canonical_item(raw, default_domain=None):
    if not isinstance(raw, dict):
        return None

    category = (
        raw.get("category")
        or raw.get("intent_category")
    )

    subtype = (
        raw.get("subtype")
        or raw.get("topic")
        or raw.get("signal_type")
    )

    source = (
        raw.get("source_coach")
        or raw.get("source_worker")
        or raw.get("source")
    )

    domain = (
        raw.get("domain")
        or default_domain
        or _dc_domain_from_category(
            category,
            source,
        )
    )

    label = (
        raw.get("label")
        or raw.get("topic_label")
        or subtype
        or category
    )

    target = (
        raw.get("action_target")
        or raw.get("answer_target")
        or raw.get("resolution_target")
        or raw.get("information_need")
        or raw.get("need")
    )

    interpretation = (
        raw.get("interpretation_target")
        or raw.get("meaning")
    )

    requires_response = bool(
        raw.get("requires_response")
        or raw.get("response_needed")
        or raw.get("needs_resolution")
    )

    count = _dc_int(
        raw.get("count"),
        1,
    )

    unique_users = _dc_int(
        raw.get("unique_users"),
        0,
    )

    strength = _dc_number(
        raw.get("strength"),
        None,
    )

    if (
        not category
        and not subtype
        and not domain
        and not label
    ):
        return None

    return {
        "domain": domain,
        "category": category,
        "subtype": subtype,
        "label": label,
        "target": target,
        "interpretation": interpretation,
        "requires_response": requires_response,
        "count": max(1, count),
        "unique_users": max(0, unique_users),
        "strength": strength,
    }


# ============================================================
# 10. COLETAR TOPICOS DO CONTEXTO
# ============================================================

def _dc_collect_items(context):
    items = []

    if not isinstance(context, dict):
        return items

    primary = context.get("primary")
    if isinstance(primary, dict):
        item = _dc_canonical_item(primary)
        if item:
            items.append(item)

    topics = context.get("topics")
    if isinstance(topics, list):
        for raw in topics:
            item = _dc_canonical_item(raw)
            if item:
                items.append(item)

    # Context Fusion podera preservar resumos por dominio.
    for domain_key in (
        "product",
        "commercial",
        "objections",
        "post_purchase",
    ):
        block = context.get(domain_key)

        if not isinstance(block, dict):
            continue

        block_topics = block.get("topics")

        if isinstance(block_topics, list):
            for raw in block_topics:
                item = _dc_canonical_item(
                    raw,
                    default_domain=domain_key,
                )
                if item:
                    items.append(item)

        active_topics = block.get("active_topics")

        if isinstance(active_topics, list):
            for raw in active_topics:
                if isinstance(raw, dict):
                    item = _dc_canonical_item(
                        raw,
                        default_domain=domain_key,
                    )
                else:
                    item = _dc_canonical_item(
                        {
                            "domain": domain_key,
                            "subtype": raw,
                            "label": raw,
                            "count": block.get("count", 1),
                            "unique_users": block.get("unique_users", 0),
                        },
                        default_domain=domain_key,
                    )

                if item:
                    items.append(item)

    # Evidencias podem carregar outputs completos dos Coaches.
    evidence = context.get("evidence")

    if isinstance(evidence, list):
        for ev in evidence:
            if not isinstance(ev, dict):
                continue

            default_domain = _dc_domain_from_category(
                ev.get("category"),
                ev.get("source_coach")
                or ev.get("source_worker"),
            )

            ev_topics = ev.get("topics")

            if isinstance(ev_topics, list):
                for raw in ev_topics:
                    item = _dc_canonical_item(
                        raw,
                        default_domain=default_domain,
                    )
                    if item:
                        # Herdar evidencia agregada do envelope.
                        evi = ev.get("evidence") or {}
                        if isinstance(evi, dict):
                            item["count"] = max(
                                item.get("count", 1),
                                _dc_int(evi.get("count"), 1),
                            )
                            item["unique_users"] = max(
                                item.get("unique_users", 0),
                                _dc_int(evi.get("unique_users"), 0),
                            )
                            if item.get("strength") is None:
                                item["strength"] = _dc_number(
                                    evi.get("strength"),
                                    None,
                                )
                        items.append(item)

            ev_topic = ev.get("topic")
            if isinstance(ev_topic, dict):
                item = _dc_canonical_item(
                    ev_topic,
                    default_domain=default_domain,
                )
                if item:
                    evi = ev.get("evidence") or {}
                    if isinstance(evi, dict):
                        item["count"] = max(
                            item.get("count", 1),
                            _dc_int(evi.get("count"), 1),
                        )
                        item["unique_users"] = max(
                            item.get("unique_users", 0),
                            _dc_int(evi.get("unique_users"), 0),
                        )
                        if item.get("strength") is None:
                            item["strength"] = _dc_number(
                                evi.get("strength"),
                                None,
                            )
                    items.append(item)

    # Remover duplicatas mantendo o exemplar mais forte.
    merged = {}

    for item in items:
        key = (
            item.get("domain"),
            item.get("category"),
            item.get("subtype"),
        )

        current = merged.get(key)

        if current is None:
            merged[key] = copy.deepcopy(item)
            continue

        current["count"] = max(
            current.get("count", 1),
            item.get("count", 1),
        )
        current["unique_users"] = max(
            current.get("unique_users", 0),
            item.get("unique_users", 0),
        )
        current["requires_response"] = bool(
            current.get("requires_response")
            or item.get("requires_response")
        )

        s1 = _dc_number(current.get("strength"), None)
        s2 = _dc_number(item.get("strength"), None)

        if s2 is not None and (s1 is None or s2 > s1):
            current["strength"] = s2

        if not current.get("target") and item.get("target"):
            current["target"] = item.get("target")

        if not current.get("interpretation") and item.get("interpretation"):
            current["interpretation"] = item.get("interpretation")

    return list(merged.values())


# ============================================================
# 11. COLETAR SINAIS DE AUDIENCIA
# ============================================================

def _dc_collect_audience_signals(context):
    signals = []

    if not isinstance(context, dict):
        return signals

    audience = context.get("audience")

    if isinstance(audience, dict):
        raw_signals = (
            audience.get("signals")
            or audience.get("active_signals")
            or []
        )

        if isinstance(raw_signals, (list, tuple, set)):
            for signal in raw_signals:
                if isinstance(signal, dict):
                    signal_type = (
                        signal.get("signal_type")
                        or signal.get("subtype")
                        or signal.get("type")
                    )
                else:
                    signal_type = signal

                signal_type = _dc_safe_text(signal_type)
                if signal_type:
                    signals.append(signal_type)

        direct = audience.get("signal_type")
        if direct:
            signals.append(
                _dc_safe_text(direct)
            )

    evidence = context.get("evidence")

    if isinstance(evidence, list):
        for ev in evidence:
            if not isinstance(ev, dict):
                continue

            if (
                ev.get("message_type") == "audience_signal"
                or ev.get("source_worker") == "audience"
            ):
                signal = ev.get("signal_type")
                if signal:
                    signals.append(
                        _dc_safe_text(signal)
                    )

    direct_signal = context.get("audience_signal")
    if direct_signal:
        signals.append(
            _dc_safe_text(direct_signal)
        )

    return _dc_unique_strings(signals)


# ============================================================
# 12. NORMALIZAR CONTEXTO
# ============================================================

def decision_coach_normalize_context(context):
    if not isinstance(context, dict):
        return None

    timestamp = _dc_number(
        context.get("timestamp"),
        time.time(),
    )

    items = _dc_collect_items(context)
    audience_signals = _dc_collect_audience_signals(
        context
    )

    domains = set()

    raw_domains = context.get("domains")
    if isinstance(raw_domains, (list, tuple, set)):
        for domain in raw_domains:
            domain = _dc_safe_text(domain)
            if domain:
                domains.add(domain)

    for item in items:
        domain = item.get("domain")
        if domain:
            domains.add(domain)

    if audience_signals:
        domains.add("audience")

    # Strength do Fusion, ou maior strength dos itens.
    strength = _dc_number(
        context.get("strength"),
        None,
    )

    if strength is None:
        candidate_strengths = [
            _dc_number(item.get("strength"), None)
            for item in items
        ]
        candidate_strengths = [
            value
            for value in candidate_strengths
            if value is not None
        ]
        if candidate_strengths:
            strength = max(candidate_strengths)

    requires_response = bool(
        context.get("requires_response")
        or any(
            item.get("requires_response")
            for item in items
        )
    )

    evidence = context.get("evidence")
    evidence_count = 0

    if isinstance(evidence, list):
        evidence_count = len(evidence)

    if not evidence_count:
        evidence_count = sum(
            max(1, _dc_int(item.get("count"), 1))
            for item in items
        )

    context_id = (
        context.get("output_id")
        or context.get("fusion_id")
        or context.get("context_id")
    )

    message_type = _dc_safe_text(
        context.get("message_type")
    )

    question_group = context.get("question_group")
    if not isinstance(question_group, dict):
        question_group = {}
    else:
        question_group = copy.deepcopy(question_group)

    comments = context.get("comments")
    if not isinstance(comments, list):
        comments = []
    else:
        comments = copy.deepcopy(comments)

    return {
        "context_id": context_id,
        "message_type": message_type,
        "platform": context.get("platform"),
        "live_id": context.get("live_id"),
        "subject": context.get("subject"),
        "timestamp": timestamp,
        "iso_time": context.get("iso_time") or _dc_now_iso(),
        "context_kind": context.get("context_kind"),
        "domains": sorted(domains),
        "items": items,
        "audience_signals": audience_signals,
        "strength": strength,
        "requires_response": requires_response,
        "evidence_count": evidence_count,
        "question_group": question_group,
        "comments": comments,
        "raw": copy.deepcopy(context),
    }


# ============================================================
# 13. BASE DE PRIORIDADE POR CATEGORIA/SUBTIPO
# ============================================================

_DC_BASE_PRIORITY = {
    # Produto
    ("product_question", "safety_suitability"): 78,
    ("product_question", "compatibility"): 66,
    ("product_question", "size_fit"): 66,
    ("product_question", "authenticity"): 70,
    ("product_question", "warranty"): 65,
    ("product_question", "generic_characteristic"): 61,
    ("demo_request", "show_demonstrate"): 70,

    # Comercial
    ("commercial_question", "purchase_path"): 76,
    ("commercial_question", "promotion_problem"): 74,
    ("commercial_question", "promotion_coupon"): 72,
    ("commercial_question", "availability"): 70,
    ("commercial_question", "price"): 70,
    ("commercial_question", "shipping"): 68,
    ("commercial_question", "delivery_time"): 68,
    ("commercial_question", "payment_method"): 67,
    ("commercial_question", "installments"): 66,
    ("commercial_question", "variant_selection"): 65,
    ("commercial_question", "invoice_fiscal_document"): 62,
    ("buying_intent", "conditional_purchase_intent"): 86,
    ("buying_intent", "purchase_intent"): 82,
    ("purchase_completed", "reported_payment"): 76,
    ("purchase_completed", "reported_purchase"): 74,
    ("commercial_observation", "promotion_mention"): 50,
    ("commercial_observation", "availability_mention"): 48,
    ("commercial_observation", "shipping_mention"): 46,
    ("commercial_observation", "price_mention"): 45,
    ("commercial_observation", "contextual_product_reference"): 35,

    # Objecoes
    ("objection", "trust_objection"): 82,
    ("objection", "authenticity_objection"): 80,
    ("objection", "quality_objection"): 74,
    ("objection", "compatibility_objection"): 74,
    ("objection", "size_objection"): 72,
    ("objection", "delivery_time_objection"): 72,
    ("objection", "shipping_objection"): 70,
    ("objection", "price_objection"): 70,
    ("objection", "promotion_objection"): 68,
    ("objection", "purchase_hesitation"): 66,

    # Pos-compra
    ("post_purchase_issue", "payment_charge_problem"): 94,
    ("post_purchase_issue", "delivered_not_received"): 92,
    ("post_purchase_issue", "damaged_item"): 90,
    ("post_purchase_issue", "defective_item"): 90,
    ("post_purchase_issue", "wrong_item"): 86,
    ("post_purchase_issue", "missing_item"): 86,
    ("post_purchase_issue", "not_received"): 86,
    ("post_purchase_issue", "wrong_variant"): 82,
    ("post_purchase_issue", "not_as_described"): 82,
    ("post_purchase_issue", "refund"): 80,
    ("post_purchase_issue", "return"): 78,
    ("post_purchase_issue", "cancellation"): 78,
    ("post_purchase_issue", "exchange"): 76,
    ("post_purchase_issue", "warranty_claim"): 76,
    ("post_purchase_issue", "shipping_delay"): 74,
    ("post_purchase_issue", "invoice_missing"): 68,
    ("post_purchase_issue", "order_status"): 64,
}

_DC_CATEGORY_DEFAULT_PRIORITY = {
    "product_question": 64,
    "demo_request": 70,
    "commercial_question": 68,
    "buying_intent": 82,
    "purchase_completed": 74,
    "commercial_observation": 45,
    "objection": 70,
    "purchase_barrier": 72,
    "post_purchase_issue": 78,
}

_DC_AUDIENCE_PRIORITY = {
    "viewer_growth": 55,
    "viewer_drop": 68,
    "viewer_stable": 20,
    "like_acceleration": 54,
    "like_slowdown": 62,
    "share_activity": 48,
    "follow_activity": 50,
    "gift_activity": 52,
    "gift_received": 58,
}


# ============================================================
# 14. CALCULAR PRIORIDADE
# ============================================================

def _dc_item_priority(item):
    category = item.get("category")
    subtype = item.get("subtype")

    base = _DC_BASE_PRIORITY.get(
        (category, subtype),
        _DC_CATEGORY_DEFAULT_PRIORITY.get(
            category,
            55,
        ),
    )

    score = float(base)

    if item.get("requires_response"):
        score += 5

    count = max(1, _dc_int(item.get("count"), 1))
    unique_users = max(
        0,
        _dc_int(item.get("unique_users"), 0),
    )

    if count >= 2:
        score += 3
    if count >= 4:
        score += 3
    if count >= 7:
        score += 2

    if unique_users >= 2:
        score += 4
    if unique_users >= 3:
        score += 3
    if unique_users >= 5:
        score += 3

    strength = _dc_number(
        item.get("strength"),
        None,
    )

    if strength is not None:
        strength = _dc_clamp(strength, 0, 1)
        score += strength * 6

    return _dc_clamp(score, 0, 100)


def _dc_priority_from_context(normalized):
    items = normalized.get("items") or []
    audience_signals = normalized.get(
        "audience_signals"
    ) or []

    item_scores = [
        _dc_item_priority(item)
        for item in items
    ]

    audience_scores = [
        _DC_AUDIENCE_PRIORITY.get(signal, 45)
        for signal in audience_signals
    ]

    if item_scores:
        score = max(item_scores)
    elif audience_scores:
        score = max(audience_scores)
    else:
        score = 0

    domains = set(
        normalized.get("domains") or []
    )

    # Contexto cruzado vale mais do que um sinal isolado,
    # sem transformar correlacao em causalidade.
    if len(domains) >= 2:
        score += 4

    if len(domains) >= 3:
        score += 3

    # Intencao de compra + objecao/barreira e um caso importante:
    # existe potencial comercial, mas ha algo impedindo a conversao.
    categories = {
        item.get("category")
        for item in items
    }

    if (
        "buying_intent" in categories
        and (
            "objection" in categories
            or "purchase_barrier" in categories
        )
    ):
        score += 6

    # Sinais positivos de audiencia reforcam um contexto de interesse,
    # mas nao criam prioridade alta sozinhos.
    if items and (
        "viewer_growth" in audience_signals
        or "like_acceleration" in audience_signals
    ):
        score += 3

    # Queda/desaceleracao junto a uma objecao pode merecer atencao,
    # sem afirmar que a objecao causou a queda.
    if (
        "objection" in categories
        and (
            "viewer_drop" in audience_signals
            or "like_slowdown" in audience_signals
        )
    ):
        score += 3

    fusion_strength = _dc_number(
        normalized.get("strength"),
        None,
    )

    if fusion_strength is not None:
        fusion_strength = _dc_clamp(
            fusion_strength,
            0,
            1,
        )
        score += fusion_strength * 5

    evidence_count = max(
        0,
        _dc_int(
            normalized.get("evidence_count"),
            0,
        ),
    )

    if evidence_count >= 3:
        score += 2
    if evidence_count >= 6:
        score += 2

    return int(round(_dc_clamp(score, 0, 100)))


# ============================================================
# 15. FAIXA DE PRIORIDADE
# ============================================================

def _dc_priority_level(priority):
    priority = _dc_int(priority, 0)

    if priority >= 90:
        return "critical"
    if priority >= 75:
        return "high"
    if priority >= 60:
        return "medium"
    return "low"


# ============================================================
# 16. TTL
# ============================================================

def _dc_ttl_seconds(priority, normalized):
    level = _dc_priority_level(priority)

    if level == "critical":
        ttl = 40
    elif level == "high":
        ttl = 35
    elif level == "medium":
        ttl = 25
    else:
        ttl = 15

    raw = normalized.get("raw") or {}
    fusion_ttl = _dc_number(
        raw.get("ttl_seconds"),
        None,
    )

    if fusion_ttl is not None and fusion_ttl > 0:
        ttl = min(ttl, int(fusion_ttl))

    return max(5, int(ttl))


# ============================================================
# 17. COOLDOWN
# ============================================================

def _dc_cooldown_seconds(primary_item, priority, domains):
    category = primary_item.get("category") if primary_item else None
    subtype = primary_item.get("subtype") if primary_item else None

    # V1.2: cooldown e por ASSUNTO, nao por evento bruto. Isso e importante
    # principalmente no modo Todos, para evitar o mesmo recado em sequencia.
    if category == "purchase_completed":
        cooldown = 75
    elif category == "post_purchase_issue":
        cooldown = 50
    elif category in {"objection", "purchase_barrier"}:
        cooldown = 45
    elif category == "buying_intent":
        cooldown = 40
    elif category == "commercial_question":
        cooldown = 45
    elif category in {"product_question", "demo_request"}:
        cooldown = 45
    elif category == "commercial_observation":
        cooldown = 75
    elif "audience" in domains:
        cooldown = 60
    else:
        cooldown = 50

    # Problemas graves podem atualizar mais cedo caso exista escalada real.
    if subtype in {
        "payment_charge_problem",
        "delivered_not_received",
        "damaged_item",
        "defective_item",
    }:
        cooldown = min(cooldown, 35)

    if priority >= 95:
        cooldown = min(cooldown, 30)

    return int(cooldown)


# ============================================================
# 18. ESCOLHER ITEM PRINCIPAL
# ============================================================

def _dc_choose_primary_item(normalized):
    items = normalized.get("items") or []

    if not items:
        return None

    ranked = []

    for index, item in enumerate(items):
        ranked.append((
            _dc_item_priority(item),
            bool(item.get("requires_response")),
            _dc_int(item.get("unique_users"), 0),
            _dc_int(item.get("count"), 1),
            -index,
            item,
        ))

    ranked.sort(
        key=lambda row: row[:-1],
        reverse=True,
    )

    return copy.deepcopy(
        ranked[0][-1]
    )


# ============================================================
# 19. CATEGORIA OPERACIONAL / INTERFACE
# ============================================================

def _dc_operational_category(primary_item, domains):
    domains = set(domains or [])

    if len(domains) >= 2:
        return "mixed"

    if primary_item:
        category = primary_item.get("category")

        if category == "post_purchase_issue":
            return "post_purchase"

        domain = primary_item.get("domain")
        if domain in {
            "product",
            "commercial",
            "objections",
            "post_purchase",
        }:
            return domain

    if domains == {"audience"}:
        return "audience"

    if "audience" in domains and len(domains) == 1:
        return "audience"

    return "mixed"


# ============================================================
# 19.1 CONTEXTO DE PERGUNTA AGRUPADA PELO FUSION
# ============================================================

def _dc_question_group_info(normalized):
    qg = normalized.get("question_group")
    if isinstance(qg, dict) and qg:
        return qg

    raw = normalized.get("raw") or {}
    qg = raw.get("question_group")
    return qg if isinstance(qg, dict) else {}


def _dc_question_signature(normalized):
    qg = _dc_question_group_info(normalized)
    raw_text = _dc_safe_text(
        qg.get("normalized_question")
    )

    if not raw_text:
        examples = qg.get("examples") or []
        if examples and isinstance(examples[0], dict):
            raw_text = _dc_safe_text(
                examples[0].get("text")
            )

    if not raw_text:
        return None

    normalized_text = re.sub(
        r"\s+",
        " ",
        raw_text.lower(),
    ).strip()

    if not normalized_text:
        return None

    return hashlib.sha1(
        normalized_text.encode("utf-8")
    ).hexdigest()[:12]


def _dc_is_distinct_question_context(normalized, primary_item=None):
    qg = _dc_question_group_info(normalized)
    if qg:
        return True

    if primary_item:
        return primary_item.get("category") in {
            "product_question",
            "commercial_question",
            "demo_request",
        }

    return False


def _dc_question_lead(normalized, primary_item):
    qg = _dc_question_group_info(normalized)
    if not qg:
        return None

    count = max(1, _dc_int(qg.get("count"), 1))
    unique_users = max(
        0,
        _dc_int(qg.get("unique_users"), 0),
    )
    same_question = bool(
        qg.get("same_question") or count >= 2
    )

    examples = qg.get("examples") or []
    first = examples[0] if examples and isinstance(examples[0], dict) else {}
    user = _dc_safe_text(first.get("user"))
    question = _dc_safe_text(first.get("text"))

    if len(question) > 180:
        question = question[:177].rstrip() + "..."

    label = _dc_safe_text(
        (primary_item or {}).get("label")
        or (primary_item or {}).get("subtype")
        or "esse assunto"
    )

    if same_question:
        if unique_users >= 3:
            lead = f"{unique_users} pessoas estÃ£o perguntando sobre {label}."
        elif unique_users == 2:
            lead = f"Duas pessoas estÃ£o perguntando sobre {label}."
        elif count >= 3:
            lead = f"A mesma pergunta sobre {label} estÃ¡ se repetindo {count} vezes."
        else:
            lead = f"A mesma pergunta sobre {label} estÃ¡ se repetindo."

        if question:
            lead += f' Exemplo: "{question}".'
        return lead

    if question and user:
        return f'{user} perguntou: "{question}".'
    if question:
        return f'Pergunta: "{question}".'
    if user:
        return f"{user} fez uma pergunta sobre {label}."

    return None



# ============================================================
# 19.2 LINGUAGEM NATURAL PARA O VENDEDOR
# ============================================================

_DC_HUMAN_TOPIC_LABELS = {
    # Produto
    "compatibility": "compatibilidade",
    "size_fit": "tamanho ou ajuste",
    "authenticity": "autenticidade",
    "warranty": "garantia",
    "safety_suitability": "uso e seguranÃ§a",
    "generic_characteristic": "caracterÃ­sticas do produto",
    "battery_duration": "duraÃ§Ã£o da bateria",
    "connectivity": "conectividade",
    "functionality": "funcionamento",
    "show_demonstrate": "demonstraÃ§Ã£o do produto",
    # Comercial
    "price": "preÃ§o",
    "promotion_coupon": "cupom ou promoÃ§Ã£o",
    "promotion_problem": "problema com promoÃ§Ã£o ou cupom",
    "availability": "disponibilidade",
    "shipping": "frete",
    "delivery_time": "prazo de entrega",
    "purchase_path": "como comprar",
    "payment_method": "forma de pagamento",
    "installments": "parcelamento",
    "variant_selection": "variaÃ§Ã£o do produto",
    "invoice_fiscal_document": "nota fiscal",
    # Objecoes / pos-compra
    "price_objection": "preÃ§o",
    "shipping_objection": "frete",
    "delivery_time_objection": "prazo de entrega",
    "promotion_objection": "promoÃ§Ã£o",
    "trust_objection": "confianÃ§a na compra",
    "authenticity_objection": "autenticidade",
    "quality_objection": "qualidade",
    "compatibility_objection": "compatibilidade",
    "size_objection": "tamanho ou ajuste",
    "purchase_hesitation": "hesitaÃ§Ã£o de compra",
    "payment_charge_problem": "cobranÃ§a ou pagamento",
    "delivered_not_received": "pedido marcado como entregue, mas nÃ£o recebido",
    "damaged_item": "produto danificado",
    "defective_item": "produto com defeito",
    "wrong_item": "produto errado",
    "missing_item": "item faltando",
    "not_received": "pedido nÃ£o recebido",
    "wrong_variant": "variaÃ§Ã£o errada",
    "not_as_described": "produto diferente do anunciado",
    "refund": "reembolso",
    "return": "devolucao",
    "cancellation": "cancelamento",
    "exchange": "troca",
    "warranty_claim": "garantia",
    "shipping_delay": "atraso na entrega",
    "invoice_missing": "nota fiscal",
    "order_status": "status do pedido",
}


def _dc_human_topic_label(item):
    item = item or {}
    subtype = _dc_safe_text(item.get("subtype"))
    if subtype in _DC_HUMAN_TOPIC_LABELS:
        return _DC_HUMAN_TOPIC_LABELS[subtype]

    label = _dc_safe_text(item.get("label") or subtype or item.get("category"))
    label = label.replace("_", " ")
    return label or "esse assunto"


def _dc_first_comment(normalized):
    qg = _dc_question_group_info(normalized)
    examples = qg.get("examples") or []
    if not examples:
        examples = normalized.get("comments") or []

    for item in examples:
        if not isinstance(item, dict):
            continue
        user = _dc_safe_text(item.get("user"))
        text = _dc_safe_text(item.get("text"))
        if user or text:
            return {"user": user, "text": text}

    return {"user": "", "text": ""}


def _dc_group_counts(normalized, primary_item=None):
    qg = _dc_question_group_info(normalized)
    count = max(
        1,
        _dc_int(qg.get("count"), 0),
        _dc_int((primary_item or {}).get("count"), 1),
    )
    unique_users = max(
        0,
        _dc_int(qg.get("unique_users"), 0),
        _dc_int((primary_item or {}).get("unique_users"), 0),
    )
    return count, unique_users


def _dc_commercial_action(subtype):
    return {
        "price": "Informe o preÃ§o atual.",
        "promotion_coupon": "Explique o cupom ou a promoÃ§Ã£o vigente.",
        "promotion_problem": "Explique como estÃ¡ a promoÃ§Ã£o ou o cupom e o que aparece na plataforma.",
        "availability": "Informe se a opÃ§Ã£o estÃ¡ disponÃ­vel.",
        "shipping": "Explique o frete disponÃ­vel.",
        "delivery_time": "Informe o prazo de entrega mostrado pela plataforma.",
        "purchase_path": "Mostre de forma simples como concluir a compra.",
        "payment_method": "Explique as formas de pagamento disponÃ­veis.",
        "installments": "Informe as opÃ§Ãµes de parcelamento disponÃ­veis.",
        "variant_selection": "Ajude a identificar a variaÃ§Ã£o correta.",
        "invoice_fiscal_document": "Explique como funciona a nota fiscal ou o documento da compra.",
    }.get(subtype, "EsclareÃ§a essa informaÃ§Ã£o comercial.")


def _dc_product_action(subtype):
    return {
        "compatibility": "Explique com quais aparelhos, modelos ou usos o produto Ã© compatÃ­vel.",
        "size_fit": "EsclareÃ§a o tamanho, a medida ou o ajuste solicitado.",
        "authenticity": "EsclareÃ§a a informaÃ§Ã£o de autenticidade disponÃ­vel.",
        "warranty": "Explique a garantia informada para o produto.",
        "safety_suitability": "Explique para quem o produto Ã© indicado e os cuidados relevantes.",
        "battery_duration": "Informe a autonomia ou a duraÃ§Ã£o da bateria disponÃ­vel na descriÃ§Ã£o do produto.",
        "connectivity": "Explique a conectividade ou a funÃ§Ã£o perguntada.",
        "functionality": "Explique para que serve ou como funciona.",
        "generic_characteristic": "EsclareÃ§a a caracterÃ­stica perguntada.",
    }.get(subtype, "EsclareÃ§a a informaÃ§Ã£o do produto.")


def _dc_objection_action(subtype):
    return {
        "price_objection": "EsclareÃ§a o preÃ§o e reforce os pontos de valor do produto sem inventar oferta.",
        "shipping_objection": "EsclareÃ§a o frete e a condiÃ§Ã£o de entrega disponÃ­vel.",
        "delivery_time_objection": "EsclareÃ§a o prazo de entrega mostrado pela plataforma.",
        "promotion_objection": "EsclareÃ§a a promoÃ§Ã£o ou o cupom vigente.",
        "trust_objection": "Reforce informaÃ§Ãµes objetivas da loja e do produto que ajudem a dar seguranÃ§a para a compra.",
        "authenticity_objection": "EsclareÃ§a as informaÃ§Ãµes de autenticidade disponÃ­veis.",
        "quality_objection": "Mostre caracterÃ­sticas concretas de qualidade do produto sem prometer o que nÃ£o estÃ¡ informado.",
        "compatibility_objection": "EsclareÃ§a a compatibilidade antes de incentivar a compra.",
        "size_objection": "EsclareÃ§a tamanho, medidas ou ajuste antes de incentivar a compra.",
        "purchase_hesitation": "Responda a principal dÃºvida antes de reforÃ§ar o caminho de compra.",
    }.get(subtype, "EsclareÃ§a essa barreira antes de reforÃ§ar a compra.")


def _dc_post_purchase_action(subtype):
    return {
        "payment_charge_problem": "HÃ¡ um problema de cobranÃ§a ou pagamento relatado. Oriente o cliente a conferir os dados do pedido e seguir o atendimento da loja.",
        "delivered_not_received": "Um cliente disse que o pedido aparece como entregue, mas nÃ£o foi recebido. Oriente a conferir os dados do pedido e o atendimento da loja.",
        "damaged_item": "Um cliente relatou que o produto chegou danificado. Oriente sobre o procedimento de suporte, troca ou devoluÃ§Ã£o da loja.",
        "defective_item": "Um cliente relatou defeito no produto. Oriente sobre o procedimento de suporte, troca ou garantia da loja.",
        "wrong_item": "Um cliente relatou que recebeu o produto errado. Oriente sobre o procedimento de troca ou atendimento da loja.",
        "missing_item": "Um cliente relatou item faltando no pedido. Oriente a conferir o pedido e seguir o atendimento da loja.",
        "not_received": "Um cliente relatou que ainda nÃ£o recebeu o pedido. Oriente a conferir o status e o atendimento disponÃ­vel.",
        "wrong_variant": "Um cliente relatou que recebeu a variaÃ§Ã£o errada. Oriente sobre o procedimento de troca da loja.",
        "not_as_described": "Um cliente disse que o produto recebido nÃ£o corresponde ao esperado. Oriente a conferir o pedido e seguir o atendimento da loja.",
        "refund": "HÃ¡ uma solicitaÃ§Ã£o ou dÃºvida sobre reembolso. Oriente conforme o procedimento da loja.",
        "return": "HÃ¡ uma solicitaÃ§Ã£o ou dÃºvida sobre devoluÃ§Ã£o. Oriente conforme o procedimento da loja.",
        "cancellation": "HÃ¡ uma solicitaÃ§Ã£o ou dÃºvida sobre cancelamento. Oriente conforme o procedimento da loja.",
        "exchange": "HÃ¡ uma solicitaÃ§Ã£o ou dÃºvida sobre troca. Oriente conforme o procedimento da loja.",
        "warranty_claim": "HÃ¡ uma solicitaÃ§Ã£o relacionada Ã  garantia. Oriente conforme o procedimento da loja.",
        "shipping_delay": "Um cliente relatou atraso na entrega. Oriente a conferir o prazo e o status do pedido.",
        "invoice_missing": "Um cliente relatou problema com a nota fiscal. Oriente a conferir o documento da compra e o atendimento da loja.",
        "order_status": "Um cliente pediu informaÃ§Ã£o sobre o pedido. Oriente a conferir o status na plataforma.",
    }.get(subtype, "HÃ¡ uma questÃ£o de pÃ³s-compra. Oriente a conferir os dados do pedido e seguir o atendimento da loja.")

# ============================================================
# 20. CHAVE DE DECISAO
# ============================================================

def _dc_decision_key(primary_item, normalized):
    if primary_item:
        domain = primary_item.get("domain") or "unknown"
        category = primary_item.get("category") or "unknown"
        subtype = primary_item.get("subtype") or "generic"

        base = ":".join([
            _dc_slug(domain),
            _dc_slug(category),
            _dc_slug(subtype),
        ])

        # Somente perguntas/pedidos que realmente podem ter conteudos
        # diferentes usam assinatura textual. Eventos como "fiz a compra"
        # ou "produto com defeito" precisam compartilhar o mesmo cooldown
        # sem criar uma chave nova a cada frase equivalente.
        if category in {
            "product_question",
            "commercial_question",
            "demo_request",
        }:
            question_signature = _dc_question_signature(
                normalized
            )
            if question_signature:
                return base + ":q:" + question_signature

        return base

    audience_signals = normalized.get("audience_signals") or []

    if audience_signals:
        return "audience:" + _dc_slug(audience_signals[0])

    context_kind = normalized.get("context_kind")

    if context_kind:
        return "fusion:" + _dc_slug(context_kind)

    return "fusion:generic"


# ============================================================
# 21. FINGERPRINT
# ============================================================

def _dc_fingerprint(decision_key, normalized):
    items = normalized.get("items") or []

    signature_items = sorted([
        (
            _dc_safe_text(item.get("domain")),
            _dc_safe_text(item.get("category")),
            _dc_safe_text(item.get("subtype")),
        )
        for item in items
    ])

    payload = {
        "decision_key": decision_key,
        "items": signature_items,
        "audience": sorted(
            normalized.get("audience_signals") or []
        ),
        "context_kind": normalized.get("context_kind"),
        "question_signature": _dc_question_signature(normalized),
    }

    raw = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=True,
        default=str,
    )

    return hashlib.sha1(
        raw.encode("utf-8")
    ).hexdigest()


# ============================================================
# 22. TEXTO DA ORIENTACAO
#
# O texto e orientacao operacional, NAO resposta factual ao cliente.
# Ele nunca inventa o valor/preco/estoque/garantia etc.
# ============================================================

def _dc_plural_prefix(item):
    unique_users = _dc_int(
        item.get("unique_users"),
        0,
    )
    count = _dc_int(
        item.get("count"),
        1,
    )

    if unique_users >= 3:
        return "VÃ¡rias pessoas"
    if unique_users == 2:
        return "Duas pessoas"
    if count >= 3:
        return "O mesmo assunto estÃ¡ se repetindo"
    return "HÃ¡ um comentÃ¡rio"


def _dc_sentence(value):
    value = _dc_safe_text(value)
    if not value:
        return ""
    value = value[0].upper() + value[1:]
    if value[-1] not in ".!?":
        value += "."
    return value


def _dc_compose_tip(normalized, primary_item, priority):
    items = normalized.get("items") or []
    audience_signals = normalized.get("audience_signals") or []

    if primary_item is None:
        if "viewer_drop" in audience_signals:
            return (
                "A audiÃªncia estÃ¡ caindo. Mude o ritmo, retome um produto "
                "ou responda uma dÃºvida que esteja aparecendo no chat."
            )

        if "like_slowdown" in audience_signals:
            return (
                "O ritmo de likes desacelerou. Renove o foco da apresentaÃ§Ã£o "
                "e observe quais assuntos estÃ£o chamando atenÃ§Ã£o."
            )

        if "viewer_growth" in audience_signals:
            return (
                "A audiÃªncia estÃ¡ crescendo. Mantenha o ritmo e apresente "
                "com clareza para quem acabou de chegar."
            )

        if "like_acceleration" in audience_signals:
            return (
                "Os likes estÃ£o acelerando. Mantenha o ritmo e aproveite "
                "os assuntos que estÃ£o gerando interesse."
            )

        if "gift_received" in audience_signals:
            return (
                "Chegou um presente na LIVE. AgradeÃ§a quando isso couber "
                "naturalmente na apresentaÃ§Ã£o."
            )

        # Follow/share/gift_activity/viewer_stable sao contexto interno e
        # nao devem virar orientacao individual na V1.2.
        return "HÃ¡ um contexto relevante na LIVE que merece atenÃ§Ã£o."

    category = primary_item.get("category")
    subtype = primary_item.get("subtype")
    label = _dc_human_topic_label(primary_item)

    question_lead = _dc_question_lead(
        normalized,
        primary_item,
    )

    categories = {
        item.get("category")
        for item in items
    }

    # Caso cruzado: intencao de compra + objecao/barreira.
    if (
        "buying_intent" in categories
        and (
            "objection" in categories
            or "purchase_barrier" in categories
        )
    ):
        barrier = next(
            (
                item
                for item in items
                if item.get("category")
                in {"objection", "purchase_barrier"}
            ),
            None,
        )
        barrier_label = _dc_human_topic_label(barrier)
        return _dc_repair_text(
            "HÃ¡ intenÃ§Ã£o de compra, mas existe uma barreira sobre "
            f"{barrier_label}. EsclareÃ§a esse ponto antes de reforÃ§ar a compra."
        )

    if category == "buying_intent":
        first = _dc_first_comment(normalized)
        user = first.get("user")
        if subtype == "conditional_purchase_intent":
            if user:
                return _dc_repair_text(
                    f"{user} demonstrou interesse em comprar, mas colocou uma condiÃ§Ã£o. "
                    "EsclareÃ§a essa condiÃ§Ã£o e depois mostre como concluir a compra."
                )
            return (
                "HÃ¡ intenÃ§Ã£o de compra condicionada. EsclareÃ§a a condiÃ§Ã£o "
                "mencionada e depois mostre como concluir a compra."
            )
        if user:
            return _dc_repair_text(
                f"{user} demonstrou intenÃ§Ã£o de compra. Mostre de forma simples "
                "como concluir a compra."
            )
        return (
            "HÃ¡ intenÃ§Ã£o de compra. Mostre de forma simples como concluir a compra."
        )

    if category == "purchase_completed":
        first = _dc_first_comment(normalized)
        user = first.get("user")
        count, unique_users = _dc_group_counts(
            normalized,
            primary_item,
        )

        if subtype == "reported_payment":
            if unique_users >= 2:
                return _dc_repair_text(
                    f"{unique_users} clientes disseram que fizeram o pagamento. "
                    "AgradeÃ§a e siga com a apresentaÃ§Ã£o."
                )
            if user:
                return _dc_repair_text(
                    f"{user} disse que fez o pagamento. AgradeÃ§a e siga com a apresentaÃ§Ã£o."
                )
            return (
                "Um cliente disse que fez o pagamento. AgradeÃ§a e siga com a apresentaÃ§Ã£o."
            )

        if unique_users >= 2:
            return _dc_repair_text(
                f"{unique_users} clientes disseram que concluÃ­ram a compra. "
                "AgradeÃ§a e siga com a apresentaÃ§Ã£o."
            )
        if user:
            return _dc_repair_text(
                f"{user} disse que concluiu a compra. AgradeÃ§a e siga com a apresentaÃ§Ã£o."
            )
        return (
            "Um cliente disse que concluiu a compra. AgradeÃ§a e siga com a apresentaÃ§Ã£o."
        )

    if category == "post_purchase_issue":
        count, unique_users = _dc_group_counts(
            normalized,
            primary_item,
        )
        base = _dc_post_purchase_action(subtype)
        if unique_users >= 2:
            return _dc_repair_text(
                f"{unique_users} clientes estÃ£o relatando {label}. "
                + base
            )
        return _dc_repair_text(base)

    if category in {"objection", "purchase_barrier"}:
        count, unique_users = _dc_group_counts(
            normalized,
            primary_item,
        )
        action = _dc_objection_action(subtype)
        if unique_users >= 2:
            return _dc_repair_text(
                f"{unique_users} pessoas estÃ£o mostrando resistÃªncia sobre {label}. "
                + action
            )
        first = _dc_first_comment(normalized)
        if first.get("user"):
            return _dc_repair_text(
                f"{first['user']} mostrou uma dÃºvida ou resistÃªncia sobre {label}. "
                + action
            )
        return _dc_repair_text(
            f"HÃ¡ uma objeÃ§Ã£o sobre {label}. " + action
        )

    if category == "commercial_question":
        base = (question_lead + " ") if question_lead else (
            f"HÃ¡ uma pergunta sobre {label}. "
        )
        return _dc_repair_text(
            base + _dc_commercial_action(subtype)
        )

    if category == "commercial_observation":
        if subtype == "contextual_product_reference":
            return (
                "HÃ¡ uma referÃªncia a produto ou variaÃ§Ã£o que deve ser mantida "
                "como contexto para os prÃ³ximos comentÃ¡rios."
            )
        return _dc_repair_text(
            f"HÃ¡ uma observaÃ§Ã£o comercial sobre {label}. Considere isso no contexto da LIVE."
        )

    if category == "demo_request":
        if question_lead:
            return _dc_repair_text(
                question_lead + " Mostre o que foi pedido."
            )
        return "HÃ¡ um pedido de demonstraÃ§Ã£o. Mostre o que foi solicitado."

    if category == "product_question":
        base = (question_lead + " ") if question_lead else (
            f"HÃ¡ uma pergunta sobre {label}. "
        )
        return _dc_repair_text(
            base + _dc_product_action(subtype)
        )

    return (
        "HÃ¡ um assunto relevante na LIVE que merece uma orientaÃ§Ã£o ao vendedor."
    )


# ============================================================
# 23. MOTIVOS E RESUMO DE EVIDENCIAS
# ============================================================

def _dc_reason_codes(normalized, primary_item, priority):
    reasons = []

    if primary_item:
        if primary_item.get("requires_response"):
            reasons.append("requires_response")

        if _dc_int(primary_item.get("unique_users"), 0) >= 2:
            reasons.append("multiple_users")

        if _dc_int(primary_item.get("count"), 1) >= 2:
            reasons.append("repeated_topic")

        category = primary_item.get("category")

        if category == "buying_intent":
            reasons.append("buying_intent")
        elif category == "post_purchase_issue":
            reasons.append("post_purchase_issue")
        elif category in {"objection", "purchase_barrier"}:
            reasons.append("purchase_barrier")

    domains = normalized.get("domains") or []

    if len(domains) >= 2:
        reasons.append("multi_domain_context")

    audience = normalized.get("audience_signals") or []

    if audience:
        reasons.append("audience_context")

    if priority >= 90:
        reasons.append("critical_priority")

    return _dc_unique_strings(reasons)


def _dc_evidence_summary(normalized, primary_item):
    return {
        "domains": copy.deepcopy(
            normalized.get("domains") or []
        ),
        "evidence_count": _dc_int(
            normalized.get("evidence_count"),
            0,
        ),
        "strength": normalized.get("strength"),
        "audience_signals": copy.deepcopy(
            normalized.get("audience_signals") or []
        ),
        "primary": copy.deepcopy(primary_item),
        "topics": [
            {
                "domain": item.get("domain"),
                "category": item.get("category"),
                "subtype": item.get("subtype"),
                "label": item.get("label"),
                "count": item.get("count"),
                "unique_users": item.get("unique_users"),
            }
            for item in (
                normalized.get("items") or []
            )
        ],
    }


# ============================================================
# 24. EXPIRACAO DO CONTEXTO
# ============================================================

def _dc_context_age(normalized, now=None):
    if now is None:
        now = time.time()

    timestamp = _dc_number(
        normalized.get("timestamp"),
        now,
    )

    return max(0.0, now - timestamp)


# ============================================================
# 25. RATE LIMIT LOCAL
# ============================================================

def _dc_prune_display_times(now=None):
    if now is None:
        now = time.time()

    limit = (
        now
        - DECISION_COACH_RATE_WINDOW_SECONDS
    )

    while (
        decision_coach_display_times
        and decision_coach_display_times[0] < limit
    ):
        decision_coach_display_times.popleft()


def _dc_rate_limited(settings, now=None):
    if now is None:
        now = time.time()

    # No modo Todos, a promessa e mostrar todas as situacoes uteis
    # DISTINTAS. A protecao contra repeticao vem do Context Fusion,
    # fingerprint e cooldown por pergunta/assunto, nao de um corte por minuto.
    if settings.get("alert_mode") == "all":
        return False

    _dc_prune_display_times(now)

    maximum = max(
        1,
        _dc_int(
            settings.get("max_tips_per_minute"),
            DECISION_COACH_DEFAULT_MAX_TIPS_PER_MINUTE,
        ),
    )

    return len(decision_coach_display_times) >= maximum


# ============================================================
# 26. DEDUP LOCAL
# ============================================================

def _dc_prune_fingerprints(now=None):
    if now is None:
        now = time.time()

    expired = [
        fp
        for fp, ts in decision_coach_recent_fingerprints.items()
        if now - ts > DECISION_COACH_DEDUPE_WINDOW_SECONDS
    ]

    for fp in expired:
        decision_coach_recent_fingerprints.pop(
            fp,
            None,
        )


def _dc_is_recent_fingerprint(fingerprint, now=None):
    if now is None:
        now = time.time()

    _dc_prune_fingerprints(now)

    previous = decision_coach_recent_fingerprints.get(
        fingerprint
    )

    if previous is None:
        return False

    return (
        now - previous
        < DECISION_COACH_DEDUPE_WINDOW_SECONDS
    )


# ============================================================
# 27. STORAGE - RUN ID
# ============================================================

def _dc_storage_run_id(platform=None, live_id=None):
    find_session = globals().get(
        "coach_storage_find_session"
    )

    if callable(find_session) and platform and live_id:
        try:
            session = find_session(
                platform,
                live_id,
            )
            if isinstance(session, dict):
                return session.get("run_id")
        except Exception:
            pass

    current_session = globals().get(
        "coach_storage_current_session"
    )

    if callable(current_session):
        try:
            session = current_session()
            if isinstance(session, dict):
                return session.get("run_id")
        except Exception:
            pass

    return None


# ============================================================
# 28. STORAGE - VERIFICAR EXIBICAO RECENTE
# ============================================================

def _dc_storage_was_recently_shown(
    decision_key,
    seconds,
    platform=None,
    live_id=None,
):
    run_id = _dc_storage_run_id(
        platform,
        live_id,
    )

    try:
        return bool(
            coach_storage_was_recently_shown(
                decision_key,
                seconds=seconds,
                run_id=run_id,
            )
        )
    except Exception:
        return False


# ============================================================
# 29. ESCALACAO DURANTE COOLDOWN
#
# Se o mesmo assunto ficou claramente mais forte, permitimos nova
# exibicao sem esperar o cooldown inteiro.
# ============================================================

def _dc_is_escalation(
    decision_key,
    priority,
    evidence_count,
    group_count=0,
    unique_users=0,
    now=None,
):
    if now is None:
        now = time.time()

    previous = decision_coach_last_display_by_key.get(
        decision_key
    )

    if not previous:
        return False

    old_priority = _dc_int(
        previous.get("priority"),
        0,
    )
    old_evidence = _dc_int(
        previous.get("evidence_count"),
        0,
    )
    old_group_count = _dc_int(
        previous.get("group_count"),
        0,
    )
    old_unique_users = _dc_int(
        previous.get("unique_users"),
        0,
    )

    if priority >= old_priority + 10:
        return True

    if evidence_count >= old_evidence + 3:
        return True

    # Se o mesmo assunto cresceu de verdade (mais pessoas/ocorrencias),
    # uma atualizacao pode aparecer antes do cooldown terminar.
    if group_count >= old_group_count + 2:
        return True

    if unique_users >= old_unique_users + 2:
        return True

    return False


# ============================================================
# 30. COOLDOWN GLOBAL
# ============================================================

def _dc_global_cooldown_active(
    settings,
    priority,
    decision_key,
    now=None,
):
    if now is None:
        now = time.time()

    if not decision_coach_display_history:
        return False

    last = decision_coach_display_history[-1]
    last_time = _dc_number(
        last.get("timestamp"),
        0,
    )

    cooldown = max(
        DECISION_COACH_MIN_DISPLAY_GAP_SECONDS,
        _dc_number(
            settings.get("global_cooldown_seconds"),
            DECISION_COACH_DEFAULT_GLOBAL_COOLDOWN,
        ),
    )

    if now - last_time >= cooldown:
        return False

    # Critico e de outro assunto pode romper cooldown global.
    if (
        priority >= DECISION_COACH_CRITICAL_OVERRIDE_PRIORITY
        and last.get("decision_key") != decision_key
    ):
        return False

    return True


# ============================================================
# 31. VALIDAR CONFIGURACAO DA CATEGORIA
# ============================================================

def _dc_category_enabled(category, settings):
    categories = settings.get("categories") or {}
    return bool(
        categories.get(category, True)
    )


# ============================================================
# 31.1 ELEGIBILIDADE POR MODO DE AVISO
# ============================================================

def _dc_alert_mode_policy(settings):
    mode = settings.get("alert_mode") or "custom"

    if mode in DECISION_COACH_ALERT_MODES:
        policy = DECISION_COACH_ALERT_MODES[mode]

        # Compatibilidade: se alguem sobrescreveu diretamente os parametros
        # operacionais do preset, tratamos aquela avaliacao como customizada.
        comparable = {
            "min_priority": int,
            "global_cooldown_seconds": float,
            "max_tips_per_minute": int,
            "allow_audience_only": bool,
            "show_viewer_stable": bool,
        }
        for key, caster in comparable.items():
            if key not in settings:
                continue
            expected = policy.get(key)
            actual = settings.get(key)
            try:
                if caster is float:
                    differs = abs(float(actual) - float(expected)) > 1e-9
                else:
                    differs = caster(actual) != caster(expected)
            except Exception:
                differs = actual != expected
            if differs:
                return "custom", {
                    "label": "Personalizado",
                    "min_priority": _dc_int(
                        settings.get("min_priority"),
                        DECISION_COACH_DEFAULT_MIN_PRIORITY,
                    ),
                }

        return mode, policy

    return "custom", {
        "label": "Personalizado",
        "min_priority": _dc_int(
            settings.get("min_priority"),
            DECISION_COACH_DEFAULT_MIN_PRIORITY,
        ),
    }


def _dc_all_mode_meaningful(normalized, primary_item, priority):
    # Snapshot e memoria interna, nao uma dica em si.
    if normalized.get("message_type") == "context_snapshot":
        return False, "context_snapshot_internal"

    audience_signals = normalized.get("audience_signals") or []
    items = normalized.get("items") or []

    if primary_item is None:
        # V1.2: atividade leve (follow/share/gift counter/stable) fica como
        # contexto interno. So mudancas de audiencia realmente uteis ou
        # presente individual podem virar uma orientacao isolada.
        meaningful_audience = [
            signal
            for signal in audience_signals
            if signal in {
                "viewer_growth",
                "viewer_drop",
                "like_acceleration",
                "like_slowdown",
                "gift_received",
            }
        ]
        if meaningful_audience:
            return True, "all_mode_audience"
        if audience_signals:
            return False, "audience_context_only"
        return False, "no_actionable_content"

    category = primary_item.get("category")
    subtype = primary_item.get("subtype")

    # Regra central pedida para o modo Todos:
    # toda pergunta util DISTINTA passa; duplicadas ja chegaram agrupadas.
    if _dc_is_distinct_question_context(normalized, primary_item):
        return True, "all_mode_distinct_question"

    if category in {
        "buying_intent",
        "purchase_completed",
        "objection",
        "purchase_barrier",
        "post_purchase_issue",
    }:
        return True, "all_mode_actionable_event"

    if category == "commercial_observation":
        # Referencia contextual curta serve para o Fusion interpretar o
        # proximo comentario; sozinha nao precisa interromper o vendedor.
        if subtype == "contextual_product_reference":
            return False, "context_only_reference"

        count = _dc_int(primary_item.get("count"), 1)
        unique_users = _dc_int(
            primary_item.get("unique_users"),
            0,
        )
        if count >= 2 or unique_users >= 2 or priority >= 60:
            return True, "all_mode_meaningful_observation"
        return False, "low_signal_observation"

    if audience_signals and any(
        signal != "viewer_stable"
        for signal in audience_signals
    ):
        return True, "all_mode_audience_context"

    # Correlacoes do Fusion que trazem itens reais tambem sao uteis.
    if normalized.get("raw", {}).get("correlation_only") and items:
        return True, "all_mode_correlation"

    return priority >= 45, (
        "all_mode_priority_floor"
        if priority >= 45
        else "no_actionable_content"
    )


def _dc_mode_eligibility(normalized, primary_item, priority, settings):
    mode, policy = _dc_alert_mode_policy(settings)

    if normalized.get("message_type") == "context_snapshot":
        return False, "context_snapshot_internal", mode

    if mode == "all":
        allowed, reason = _dc_all_mode_meaningful(
            normalized,
            primary_item,
            priority,
        )
        return allowed, reason, mode

    threshold = _dc_int(
        policy.get("min_priority"),
        settings.get(
            "min_priority",
            DECISION_COACH_DEFAULT_MIN_PRIORITY,
        ),
    )

    if priority < threshold:
        return False, "below_mode_priority", mode

    return True, f"{mode}_mode_eligible", mode


# ============================================================
# 32. PREVIEW PURO DA DECISAO
#
# Nao altera historico, fila, Storage ou Interface.
# Ideal para teste unitario.
# ============================================================

def decision_coach_preview_context(
    context,
    now=None,
    settings=None,
    recent_displayed_keys=None,
):
    if now is None:
        now = time.time()

    if settings is None:
        settings = decision_coach_get_settings()
    else:
        base = decision_coach_get_settings()
        custom = copy.deepcopy(settings)

        # Mesclar categories separadamente.
        if isinstance(custom.get("categories"), dict):
            base_categories = base.setdefault(
                "categories",
                {},
            )
            base_categories.update(
                custom.pop("categories")
            )

        base.update(custom)
        settings = base

    normalized = decision_coach_normalize_context(
        context
    )

    if normalized is None:
        return {
            "action": "suppress",
            "reason": "invalid_context",
            "priority": 0,
        }

    primary_item = _dc_choose_primary_item(
        normalized
    )

    priority = _dc_priority_from_context(
        normalized
    )

    priority_level = _dc_priority_level(
        priority
    )

    domains = normalized.get("domains") or []

    operational_category = _dc_operational_category(
        primary_item,
        domains,
    )

    decision_key = _dc_decision_key(
        primary_item,
        normalized,
    )

    fingerprint = _dc_fingerprint(
        decision_key,
        normalized,
    )

    ttl_seconds = _dc_ttl_seconds(
        priority,
        normalized,
    )

    cooldown_seconds = _dc_cooldown_seconds(
        primary_item,
        priority,
        domains,
    )

    context_age = _dc_context_age(
        normalized,
        now,
    )

    reason = "eligible"
    action = "display"
    alert_mode = settings.get("alert_mode") or "custom"

    if not settings.get("enabled", True):
        action = "suppress"
        reason = "settings_disabled"

    elif context_age > min(
        DECISION_COACH_MAX_CONTEXT_AGE_SECONDS,
        max(ttl_seconds * 2, ttl_seconds),
    ):
        action = "suppress"
        reason = "expired_context"

    elif not _dc_category_enabled(
        operational_category,
        settings,
    ):
        action = "suppress"
        reason = "category_disabled"

    elif (
        operational_category == "audience"
        and not settings.get("allow_audience_only", True)
    ):
        action = "suppress"
        reason = "audience_only_disabled"

    elif (
        operational_category == "audience"
        and "viewer_stable" in normalized.get(
            "audience_signals",
            [],
        )
        and not settings.get("show_viewer_stable", False)
    ):
        action = "suppress"
        reason = "viewer_stable_suppressed"

    else:
        mode_allowed, mode_reason, alert_mode = _dc_mode_eligibility(
            normalized,
            primary_item,
            priority,
            settings,
        )
        if not mode_allowed:
            action = "suppress"
            reason = mode_reason

    if (
        action == "display"
        and recent_displayed_keys
        and decision_key in set(recent_displayed_keys)
    ):
        action = "suppress"
        reason = "recently_displayed"

    text = (
        _dc_compose_tip(
            normalized,
            primary_item,
            priority,
        )
        if action == "display"
        else None
    )

    return {
        "action": action,
        "reason": reason,
        "priority": priority,
        "priority_level": priority_level,
        "alert_mode": alert_mode,
        "alert_mode_label": (
            DECISION_COACH_ALERT_MODES.get(alert_mode, {}).get("label")
            or "Personalizado"
        ),
        "ttl_seconds": ttl_seconds,
        "cooldown_seconds": cooldown_seconds,
        "decision_key": decision_key,
        "fingerprint": fingerprint,
        "category": operational_category,
        "text": text,
        "primary": copy.deepcopy(primary_item),
        "reason_codes": _dc_reason_codes(
            normalized,
            primary_item,
            priority,
        ),
        "evidence_summary": _dc_evidence_summary(
            normalized,
            primary_item,
        ),
        "context_age_seconds": round(
            context_age,
            3,
        ),
        "question_group": copy.deepcopy(
            normalized.get("question_group") or {}
        ),
        "normalized": normalized,
    }


# ============================================================
# 33. INPUT ID / DEDUP DE ENVELOPE
# ============================================================

def _dc_seen_input(context):
    context_id = None

    if isinstance(context, dict):
        context_id = (
            context.get("output_id")
            or context.get("fusion_id")
            or context.get("context_id")
        )

    if not context_id:
        return False

    context_id = str(context_id)

    if context_id in decision_coach_seen_input_ids:
        decision_coach_stats["duplicate_inputs"] += 1
        return True

    if (
        decision_coach_seen_input_order.maxlen
        and len(decision_coach_seen_input_order)
        >= decision_coach_seen_input_order.maxlen
    ):
        oldest = decision_coach_seen_input_order[0]
        decision_coach_seen_input_ids.discard(oldest)

    decision_coach_seen_input_order.append(
        context_id
    )
    decision_coach_seen_input_ids.add(
        context_id
    )

    return False


# ============================================================
# 34. REGISTRAR DECISAO NO STORAGE
# ============================================================

def _dc_save_decision_to_storage(decision):
    run_id = _dc_storage_run_id(
        decision.get("platform"),
        decision.get("live_id"),
    )

    try:
        return coach_storage_save_decision(
            decision,
            run_id=run_id,
        )
    except Exception as e:
        decision_coach_stats["storage_errors"] += 1
        decision_coach_last_error_message = (
            f"Storage decision: {type(e).__name__}: {e}"
        )
        return decision_coach_last_error_message


# ============================================================
# 35. REGISTRAR SAIDA NO STORAGE
# ============================================================

def _dc_save_output_to_storage(decision, displayed):
    run_id = _dc_storage_run_id(
        decision.get("platform"),
        decision.get("live_id"),
    )

    text = (
        _dc_repair_text(decision.get("text"))
        or (
            "SUPPRESSED: "
            + _dc_safe_text(
                decision.get("reason")
            )
        )
    )

    try:
        return coach_storage_save_output(
            text=text,
            category=decision.get("decision_key"),
            priority=decision.get("priority"),
            source_signals=[
                decision.get("source_context_id")
            ] if decision.get("source_context_id") else [],
            displayed=bool(displayed),
            run_id=run_id,
        )
    except Exception:
        decision_coach_stats["storage_errors"] += 1
        return None


# ============================================================
# 36. PUBLICAR NA INTERFACE
# ============================================================

def _dc_publish_interface(decision):
    publisher = globals().get(
        "publicar_dica_coach"
    )

    if not callable(publisher):
        return False

    try:
        params = inspect.signature(publisher).parameters
        kwargs = {}
        if "decision" in params:
            kwargs["decision"] = copy.deepcopy(decision)
        elif "metadata" in params:
            kwargs["metadata"] = copy.deepcopy(decision)

        publisher(
            _dc_repair_text(decision.get("text") or ""),
            decision.get("category") or "info",
            **kwargs,
        )
        return True
    except Exception:
        decision_coach_stats["interface_errors"] += 1
        return False


# ============================================================
# 37. PROCESSAR CONTEXTO REAL
# ============================================================

def decision_coach_process_context(
    context,
    publish=True,
    persist=True,
    now=None,
):
    global decision_coach_platform
    global decision_coach_live_id
    global decision_coach_subject
    global decision_coach_last_error

    if now is None:
        now = time.time()

    if not isinstance(context, dict):
        decision_coach_stats["invalid_inputs"] += 1
        return None

    if _dc_seen_input(context):
        return None

    decision_coach_stats["contexts_received"] += 1

    decision_coach_input_history.append(
        copy.deepcopy(context)
    )

    if context.get("platform"):
        decision_coach_platform = context.get("platform")

    if context.get("live_id"):
        decision_coach_live_id = context.get("live_id")

    if context.get("subject"):
        decision_coach_subject = context.get("subject")

    preview = decision_coach_preview_context(
        context,
        now=now,
    )

    normalized = preview.get("normalized") or {}

    decision_key = preview.get("decision_key")
    fingerprint = preview.get("fingerprint")
    priority = _dc_int(
        preview.get("priority"),
        0,
    )
    cooldown_seconds = _dc_int(
        preview.get("cooldown_seconds"),
        40,
    )
    evidence_count = _dc_int(
        (preview.get("evidence_summary") or {}).get(
            "evidence_count"
        ),
        0,
    )
    group_count, group_unique_users = _dc_group_counts(
        normalized,
        preview.get("primary"),
    )

    action = preview.get("action")
    reason = preview.get("reason")

    settings = decision_coach_get_settings()

    # Regras que dependem do estado real/historico.
    if action == "display":
        if _dc_is_recent_fingerprint(
            fingerprint,
            now,
        ):
            action = "suppress"
            reason = "duplicate_fingerprint"

    if action == "display":
        escalation = _dc_is_escalation(
            decision_key,
            priority,
            evidence_count,
            group_count=group_count,
            unique_users=group_unique_users,
            now=now,
        )

        previous = decision_coach_last_display_by_key.get(
            decision_key
        )

        local_cooldown = False

        if previous:
            previous_time = _dc_number(
                previous.get("timestamp"),
                0,
            )
            local_cooldown = (
                now - previous_time
                < cooldown_seconds
            )

        storage_cooldown = _dc_storage_was_recently_shown(
            decision_key,
            cooldown_seconds,
            platform=context.get("platform"),
            live_id=context.get("live_id"),
        )

        if (
            (local_cooldown or storage_cooldown)
            and not escalation
        ):
            action = "suppress"
            reason = "topic_cooldown"

    if action == "display":
        if _dc_global_cooldown_active(
            settings,
            priority,
            decision_key,
            now,
        ):
            action = "suppress"
            reason = "global_cooldown"

    if action == "display":
        if _dc_rate_limited(
            settings,
            now,
        ):
            action = "suppress"
            reason = "rate_limit"

    text = preview.get("text")

    if action == "display" and not text:
        text = _dc_compose_tip(
            normalized,
            preview.get("primary"),
            priority,
        )

    decision = {
        "output_id": uuid.uuid4().hex,
        "message_type": "coach_decision",
        "source_coach": "decision",
        "decision_version": DECISION_COACH_VERSION,
        "platform": context.get("platform") or decision_coach_platform,
        "live_id": context.get("live_id") or decision_coach_live_id,
        "subject": context.get("subject") or decision_coach_subject,
        "timestamp": now,
        "iso_time": _dc_now_iso(),
        "display_time": _dc_local_hms(now),
        "display_seconds": DECISION_COACH_INTERFACE_DISPLAY_SECONDS,
        "source_context_id": (
            context.get("output_id")
            or context.get("fusion_id")
            or context.get("context_id")
        ),
        "source_message_type": context.get("message_type"),
        "context_kind": normalized.get("context_kind"),
        "action": action,
        "reason": reason,
        "priority": priority,
        "priority_level": preview.get("priority_level"),
        "alert_mode": preview.get("alert_mode"),
        "alert_mode_label": preview.get("alert_mode_label"),
        "ttl_seconds": preview.get("ttl_seconds"),
        "expires_at": now + _dc_int(
            preview.get("ttl_seconds"),
            20,
        ),
        "cooldown_seconds": cooldown_seconds,
        "decision_key": decision_key,
        "fingerprint": fingerprint,
        "category": preview.get("category"),
        "text": text if action == "display" else None,
        "reason_codes": copy.deepcopy(
            preview.get("reason_codes") or []
        ),
        "evidence_summary": copy.deepcopy(
            preview.get("evidence_summary") or {}
        ),
        "question_group": copy.deepcopy(
            preview.get("question_group") or {}
        ),
        "settings_snapshot": {
            "alert_mode": settings.get("alert_mode"),
            "min_priority": settings.get("min_priority"),
            "global_cooldown_seconds": settings.get(
                "global_cooldown_seconds"
            ),
            "max_tips_per_minute": settings.get(
                "max_tips_per_minute"
            ),
        },
    }

    # V1.2: fingerprint e marcado somente quando a orientacao realmente
    # passa. Um evento suprimido por cooldown nao pode renovar o bloqueio
    # indefinidamente e impedir uma futura atualizacao legitima.
    if action == "display" and fingerprint:
        decision_coach_recent_fingerprints[
            fingerprint
        ] = now

    if action == "display":
        decision_coach_stats["displayed"] += 1

        decision_coach_display_times.append(
            now
        )

        decision_coach_last_display_by_key[
            decision_key
        ] = {
            "timestamp": now,
            "priority": priority,
            "evidence_count": evidence_count,
            "group_count": group_count,
            "unique_users": group_unique_users,
            "output_id": decision.get("output_id"),
        }

        interface_published = False

        if publish:
            interface_published = _dc_publish_interface(
                decision
            )

        decision["interface_published"] = bool(
            interface_published
        )

        decision_coach_display_history.append(
            copy.deepcopy(decision)
        )

        if persist:
            _dc_save_decision_to_storage(
                decision
            )
            _dc_save_output_to_storage(
                decision,
                displayed=True,
            )

    else:
        decision_coach_stats["suppressed"] += 1
        decision_coach_stats[
            f"suppressed_{reason}"
        ] += 1

        decision["interface_published"] = False

        decision_coach_suppressed_history.append(
            copy.deepcopy(decision)
        )

        if persist:
            _dc_save_decision_to_storage(
                decision
            )
            _dc_save_output_to_storage(
                decision,
                displayed=False,
            )

    decision_coach_output_history.append(
        copy.deepcopy(decision)
    )

    _dc_queue_output(decision)

    return decision


# ============================================================
# 38. RESET PARA NOVA LIVE
# ============================================================

def decision_coach_reset(platform=None):
    global decision_coach_output_queue
    global decision_coach_running
    global decision_coach_platform
    global decision_coach_live_id
    global decision_coach_subject
    global decision_coach_started_at
    global decision_coach_last_error

    decision_coach_output_queue = asyncio.Queue(
        maxsize=DECISION_COACH_OUTPUT_QUEUE_LIMIT
    )

    decision_coach_output_history.clear()
    decision_coach_display_history.clear()
    decision_coach_suppressed_history.clear()
    decision_coach_input_history.clear()

    decision_coach_seen_input_ids.clear()
    decision_coach_seen_input_order.clear()
    decision_coach_recent_fingerprints.clear()
    decision_coach_last_display_by_key.clear()
    decision_coach_display_times.clear()
    decision_coach_stats.clear()

    decision_coach_running = False
    decision_coach_platform = platform
    decision_coach_live_id = None
    decision_coach_subject = None
    decision_coach_started_at = time.time()
    decision_coach_last_error = None


# ============================================================
# 39. PEGAR PROXIMO CONTEXTO DO FUSION
# ============================================================

async def _dc_next_fusion_output(timeout=0.5):
    next_output = globals().get(
        "context_fusion_next_output"
    )

    if not callable(next_output):
        return None

    return await next_output(
        timeout=timeout
    )


# ============================================================
# 40. LOOP PRINCIPAL
# ============================================================

async def decision_coach_loop():
    global decision_coach_running
    global decision_coach_last_error

    decision_coach_running = True

    try:
        while True:
            context = None

            try:
                context = await _dc_next_fusion_output(
                    timeout=0.5
                )

            except asyncio.TimeoutError:
                context = None

            except asyncio.CancelledError:
                raise

            except Exception as e:
                decision_coach_last_error = (
                    f"{type(e).__name__}: {e}"
                )
                await asyncio.sleep(0.1)

            if context is not None:
                try:
                    decision_coach_process_context(
                        context,
                        publish=True,
                        persist=True,
                    )
                except Exception as e:
                    decision_coach_last_error = (
                        f"{type(e).__name__}: {e}"
                    )
                continue

            fusion_running = bool(
                globals().get(
                    "context_fusion_running",
                    False,
                )
            )

            source_queue = globals().get(
                "context_fusion_output_queue"
            )

            source_empty = (
                source_queue is None
                or source_queue.empty()
            )

            if (
                not live_engine_is_running()
                and not fusion_running
                and source_empty
            ):
                break

    finally:
        decision_coach_running = False


# ============================================================
# 41. SUPERVISOR
# ============================================================

async def decision_coach_supervisor(platform):
    global decision_coach_last_error

    start = time.time()

    while True:
        if time.time() - start > 30:
            decision_coach_last_error = (
                "Context Fusion V1.1 nao ficou pronto dentro de 30 segundos."
            )
            return

        next_output = globals().get(
            "context_fusion_next_output"
        )

        try:
            ready = (
                live_engine_is_running()
                and live_engine_platform() == platform
                and callable(next_output)
                and globals().get(
                    "context_fusion_output_queue"
                ) is not None
                and bool(
                    globals().get(
                        "context_fusion_running",
                        False,
                    )
                )
            )
        except Exception:
            ready = False

        if ready:
            break

        await asyncio.sleep(0.05)

    await decision_coach_loop()


# ============================================================
# 42. API DE SAIDA / DIAGNOSTICO
# ============================================================

async def decision_coach_next_output(timeout=None):
    queue = decision_coach_output_queue

    if queue is None:
        return None

    if timeout is None:
        return await queue.get()

    return await asyncio.wait_for(
        queue.get(),
        timeout=timeout,
    )


async def decision_coach_next_decision(timeout=None):
    return await decision_coach_next_output(
        timeout=timeout
    )


def decision_coach_recent_outputs(limit=20):
    if limit <= 0:
        return []
    return list(
        decision_coach_output_history
    )[-limit:]


def decision_coach_recent_displayed(limit=20):
    if limit <= 0:
        return []
    return list(
        decision_coach_display_history
    )[-limit:]


def decision_coach_recent_suppressed(limit=20):
    if limit <= 0:
        return []
    return list(
        decision_coach_suppressed_history
    )[-limit:]


# ============================================================
# 43. DIAGNOSTICO
# ============================================================

def mostrar_decision_coach():
    print("=" * 76)
    print("AGCN - DECISION COACH V1.2")
    print("Versao:", DECISION_COACH_VERSION)
    print("=" * 76)
    print("Rodando:", decision_coach_running)
    print("Runtime anexado:", decision_coach_runtime_attached)
    print("Context Fusion disponivel:", callable(
        globals().get("context_fusion_next_output")
    ))
    print("Plataforma:", decision_coach_platform)
    print("Live ID:", decision_coach_live_id)
    print("Subject:", decision_coach_subject)
    print()

    print("ENTRADA")
    print(
        " Contextos recebidos:",
        decision_coach_stats.get(
            "contexts_received",
            0,
        ),
    )
    print(
        " Duplicados ignorados:",
        decision_coach_stats.get(
            "duplicate_inputs",
            0,
        ),
    )
    print(
        " Entradas invalidas:",
        decision_coach_stats.get(
            "invalid_inputs",
            0,
        ),
    )
    print()

    print("DECISOES")
    print(
        " Exibidas:",
        decision_coach_stats.get("displayed", 0),
    )
    print(
        " Suprimidas:",
        decision_coach_stats.get("suppressed", 0),
    )
    print(
        " Erros de Storage:",
        decision_coach_stats.get("storage_errors", 0),
    )
    print(
        " Erros de Interface:",
        decision_coach_stats.get("interface_errors", 0),
    )
    print(
        " Outputs descartados por fila cheia:",
        decision_coach_stats.get("dropped_outputs", 0),
    )

    if decision_coach_output_queue is None:
        queue_size = None
    else:
        queue_size = decision_coach_output_queue.qsize()

    print(
        " Aguardando consumo diagnostico:",
        queue_size,
    )
    print()

    print("CONFIGURACAO")
    settings = decision_coach_get_settings()
    print(" Ativo:", settings.get("enabled"))
    mode = settings.get("alert_mode") or "custom"
    mode_label = DECISION_COACH_ALERT_MODES.get(mode, {}).get(
        "label",
        "Personalizado",
    )
    print(" Modo de avisos:", mode_label, f"({mode})")
    print(" Prioridade minima efetiva:", settings.get("min_priority"))
    print(
        " Cooldown global:",
        settings.get("global_cooldown_seconds"),
        "s",
    )
    print(
        " Max avisos/min:",
        settings.get("max_tips_per_minute"),
    )
    print(" Categorias:", settings.get("categories"))
    print()

    print("SUPRESSOES")
    suppressions = {
        key.replace("suppressed_", ""): value
        for key, value in decision_coach_stats.items()
        if key.startswith("suppressed_")
    }

    if suppressions:
        for key, value in sorted(
            suppressions.items(),
            key=lambda kv: (-kv[1], kv[0]),
        ):
            print(" ", key + ":", value)
    else:
        print(" nenhuma")

    print()
    print("Erro:", decision_coach_last_error)
    print()
    print("ULTIMAS DECISOES:")

    recent = decision_coach_recent_outputs(8)

    if not recent:
        print(" nenhuma ainda")
        return

    for item in recent:
        print()
        print(
            " ",
            item.get("action"),
            "|",
            item.get("priority"),
            item.get("priority_level"),
            "|",
            item.get("decision_key"),
        )
        print("   motivo:", item.get("reason"))
        if item.get("text"):
            print("   dica:", item.get("text"))
        print(
            "   Interface:",
            item.get("interface_published"),
        )


# Alias em portugues, se preferir.
mostrar_coach_decisao = mostrar_decision_coach


# ============================================================
# 44. TESTE INTERNO
# ============================================================

def testar_decision_coach_v12(verbose=False):
    print("=" * 76)
    print("TESTE INTERNO - DECISION COACH V1.2")
    print("=" * 76)

    approved = 0
    failures = []

    def check(name, condition, data=None):
        nonlocal approved

        if condition:
            approved += 1
            if verbose:
                print("OK  -", name)
        else:
            failures.append({
                "test": name,
                "data": data,
            })
            print("FAIL-", name)
            if verbose:
                print("     ", data)

    now = 1_800_000_000.0

    def ctx(
        category=None,
        subtype=None,
        domain=None,
        label=None,
        count=1,
        unique_users=1,
        requires_response=False,
        strength=None,
        audience=None,
        context_kind="single_domain",
        timestamp=None,
    ):
        item = None

        if category or subtype or domain:
            item = {
                "domain": domain,
                "category": category,
                "subtype": subtype,
                "label": label or subtype or category,
                "count": count,
                "unique_users": unique_users,
                "requires_response": requires_response,
                "strength": strength,
            }

        payload = {
            "output_id": uuid.uuid4().hex,
            "message_type": "fusion_signal",
            "platform": "tiktok",
            "live_id": "live-test",
            "subject": "@teste",
            "timestamp": now if timestamp is None else timestamp,
            "context_kind": context_kind,
            "topics": [item] if item else [],
            "domains": [domain] if domain else [],
            "strength": strength,
            "requires_response": requires_response,
            "evidence": [],
        }

        if audience:
            payload["audience"] = {
                "signals": list(audience),
            }
            if "audience" not in payload["domains"]:
                payload["domains"].append("audience")

        return payload

    # 1. Preco precisa aparecer.
    price = decision_coach_preview_context(
        ctx(
            "commercial_question",
            "price",
            "commercial",
            "preco",
            requires_response=True,
        ),
        now=now,
    )
    check(
        "pergunta de preco elegivel",
        price.get("action") == "display"
        and price.get("priority") >= 60,
        price,
    )

    # 2. Intencao condicional deve ser alta.
    conditional = decision_coach_preview_context(
        ctx(
            "buying_intent",
            "conditional_purchase_intent",
            "commercial",
            "intencao condicionada",
        ),
        now=now,
    )
    check(
        "intencao condicional alta",
        conditional.get("action") == "display"
        and conditional.get("priority") >= 80,
        conditional,
    )

    # 3. Cobranca duplicada deve ser critica.
    charge = decision_coach_preview_context(
        ctx(
            "post_purchase_issue",
            "payment_charge_problem",
            "post_purchase",
            "problema de cobranca",
            requires_response=True,
        ),
        now=now,
    )
    check(
        "problema de cobranca critico",
        charge.get("priority") >= 90
        and charge.get("priority_level") == "critical",
        charge,
    )

    # 4. Demonstracao deve ser elegivel.
    demo = decision_coach_preview_context(
        ctx(
            "demo_request",
            "show_demonstrate",
            "product",
            "demonstracao",
            requires_response=True,
        ),
        now=now,
    )
    check(
        "pedido de demonstracao elegivel",
        demo.get("action") == "display",
        demo,
    )

    # 5. Observacao contextual isolada deve ficar abaixo do minimo.
    contextual = decision_coach_preview_context(
        ctx(
            "commercial_observation",
            "contextual_product_reference",
            "commercial",
            "referencia contextual",
        ),
        now=now,
    )
    check(
        "referencia contextual isolada suprimida",
        contextual.get("action") == "suppress"
        and contextual.get("reason") in {
            "context_only_reference",
            "below_mode_priority",
            "below_min_priority",
        },
        contextual,
    )

    # 6. Viewer stable suprimido por padrao.
    stable = decision_coach_preview_context(
        ctx(
            audience=["viewer_stable"],
            context_kind="audience_state",
        ),
        now=now,
    )
    check(
        "viewer stable suprimido",
        stable.get("action") == "suppress"
        and stable.get("reason")
        in {"viewer_stable_suppressed", "below_min_priority"},
        stable,
    )

    # 7. Viewer drop deve passar no minimo.
    drop = decision_coach_preview_context(
        ctx(
            audience=["viewer_drop"],
            context_kind="audience_change",
        ),
        now=now,
    )
    check(
        "viewer drop elegivel",
        drop.get("action") == "display"
        and drop.get("priority") >= 60,
        drop,
    )

    # 8. Duas pessoas repetindo preco aumenta score.
    repeated_price = decision_coach_preview_context(
        ctx(
            "commercial_question",
            "price",
            "commercial",
            "preco",
            count=2,
            unique_users=2,
            requires_response=True,
        ),
        now=now,
    )
    check(
        "repeticao multiusuario aumenta prioridade",
        repeated_price.get("priority")
        > price.get("priority"),
        {
            "single": price,
            "repeated": repeated_price,
        },
    )

    # 9. Multidominio reforca score.
    mixed_context = ctx(
        "buying_intent",
        "conditional_purchase_intent",
        "commercial",
        "intencao condicionada",
        audience=["viewer_growth"],
        context_kind="commercial_momentum",
    )
    mixed_context["topics"].append({
        "domain": "objections",
        "category": "objection",
        "subtype": "shipping_objection",
        "label": "objecao de frete",
        "count": 1,
        "unique_users": 1,
    })
    mixed_context["domains"].append("objections")

    mixed = decision_coach_preview_context(
        mixed_context,
        now=now,
    )
    check(
        "intencao + barreira + audiencia gera contexto forte",
        mixed.get("action") == "display"
        and mixed.get("priority") >= 90
        and mixed.get("category") == "mixed",
        mixed,
    )

    # 10. Texto misto deve falar em barreira.
    check(
        "texto misto resolve barreira antes da compra",
        "barreira" in _dc_safe_text(
            mixed.get("text")
        ).lower(),
        mixed.get("text"),
    )

    # 11. Categoria pode ser desativada.
    disabled_product = decision_coach_preview_context(
        ctx(
            "product_question",
            "compatibility",
            "product",
            "compatibilidade",
            requires_response=True,
        ),
        now=now,
        settings={
            "categories": {
                "product": False,
            }
        },
    )
    check(
        "configuracao desativa produto",
        disabled_product.get("action") == "suppress"
        and disabled_product.get("reason") == "category_disabled",
        disabled_product,
    )

    # 12. Desativacao global.
    disabled_all = decision_coach_preview_context(
        ctx(
            "commercial_question",
            "price",
            "commercial",
            "preco",
            requires_response=True,
        ),
        now=now,
        settings={"enabled": False},
    )
    check(
        "configuracao global desativa avisos",
        disabled_all.get("reason") == "settings_disabled",
        disabled_all,
    )

    # 13. Prioridade minima configuravel.
    high_threshold = decision_coach_preview_context(
        ctx(
            "commercial_question",
            "price",
            "commercial",
            "preco",
            requires_response=True,
        ),
        now=now,
        settings={"min_priority": 95},
    )
    check(
        "prioridade minima configuravel",
        high_threshold.get("action") == "suppress"
        and high_threshold.get("reason") in {
            "below_mode_priority",
            "below_min_priority",
        },
        high_threshold,
    )

    # 14. Contexto expirado.
    expired = decision_coach_preview_context(
        ctx(
            "buying_intent",
            "purchase_intent",
            "commercial",
            timestamp=now - 200,
        ),
        now=now,
    )
    check(
        "contexto antigo expira",
        expired.get("action") == "suppress"
        and expired.get("reason") == "expired_context",
        expired,
    )

    # 15. TTL existe.
    check(
        "TTL calculado",
        _dc_int(price.get("ttl_seconds"), 0) > 0,
        price,
    )

    # 16. Cooldown existe.
    check(
        "cooldown calculado",
        _dc_int(price.get("cooldown_seconds"), 0) > 0,
        price,
    )

    # 17. Decision key granular.
    check(
        "decision key granular",
        price.get("decision_key")
        == "commercial:commercial_question:price",
        price.get("decision_key"),
    )

    # 18. Fingerprint deterministico.
    price_again = decision_coach_preview_context(
        copy.deepcopy(price.get("normalized", {}).get("raw", {})),
        now=now,
    )
    check(
        "fingerprint deterministico",
        price_again.get("fingerprint") == price.get("fingerprint"),
        {
            "a": price.get("fingerprint"),
            "b": price_again.get("fingerprint"),
        },
    )

    # 19. Simulacao de ja exibido.
    recently = decision_coach_preview_context(
        ctx(
            "commercial_question",
            "price",
            "commercial",
            requires_response=True,
        ),
        now=now,
        recent_displayed_keys={
            "commercial:commercial_question:price"
        },
    )
    check(
        "preview respeita chave ja exibida",
        recently.get("action") == "suppress"
        and recently.get("reason") == "recently_displayed",
        recently,
    )

    # 20. Problema de confianca deve ser alto.
    trust = decision_coach_preview_context(
        ctx(
            "objection",
            "trust_objection",
            "objections",
            "objecao de confianca",
        ),
        now=now,
    )
    check(
        "objecao de confianca alta",
        trust.get("priority") >= 75,
        trust,
    )

    # 21. Compra relatada nao vira confirmacao transacional.
    purchase = decision_coach_preview_context(
        ctx(
            "purchase_completed",
            "reported_purchase",
            "commercial",
            "compra relatada",
        ),
        now=now,
    )
    check(
        "compra relatada recebe texto natural",
        "concluiu a compra" in _dc_safe_text(
            purchase.get("text")
        ).lower()
        and "confirmaÃ§Ã£o transacional" not in _dc_safe_text(
            purchase.get("text")
        ).lower(),
        purchase.get("text"),
    )

    # 22. Audience-only pode ser desativado.
    audience_off = decision_coach_preview_context(
        ctx(
            audience=["viewer_drop"],
        ),
        now=now,
        settings={"allow_audience_only": False},
    )
    check(
        "audience-only configuravel",
        audience_off.get("action") == "suppress"
        and audience_off.get("reason") == "audience_only_disabled",
        audience_off,
    )

    # 23. Safety suitability ganha tratamento mais alto que generico.
    safety = decision_coach_preview_context(
        ctx(
            "product_question",
            "safety_suitability",
            "product",
            "seguranca ou adequacao",
            requires_response=True,
        ),
        now=now,
    )
    generic = decision_coach_preview_context(
        ctx(
            "product_question",
            "generic_characteristic",
            "product",
            "caracteristica",
            requires_response=True,
        ),
        now=now,
    )
    check(
        "seguranca/adequacao supera pergunta generica",
        safety.get("priority") > generic.get("priority"),
        {
            "safety": safety.get("priority"),
            "generic": generic.get("priority"),
        },
    )

    # 24. Pos-compra mapeia categoria operacional correta.
    check(
        "pos-compra usa categoria operacional propria",
        charge.get("category") == "post_purchase",
        charge.get("category"),
    )

    # 25. Multidominio vira mixed.
    check(
        "multidominio vira mixed",
        mixed.get("category") == "mixed",
        mixed.get("category"),
    )

    # 26. Contrato inclui os tres tipos de mensagem.
    check(
        "contrato do Fusion documentado",
        DECISION_COACH_FUSION_CONTRACT[
            "accepted_message_types"
        ] == {
            "fusion_event",
            "fusion_signal",
            "context_snapshot",
        },
        DECISION_COACH_FUSION_CONTRACT,
    )

    # 27. Processamento real sem publicar/persistir.
    decision_coach_reset("tiktok")

    processed = decision_coach_process_context(
        ctx(
            "commercial_question",
            "price",
            "commercial",
            "preco",
            requires_response=True,
        ),
        publish=False,
        persist=False,
        now=now,
    )

    check(
        "processamento real gera coach_decision",
        isinstance(processed, dict)
        and processed.get("message_type") == "coach_decision"
        and processed.get("action") == "display",
        processed,
    )

    # 28. Mesmo contexto exato e deduplicado por input id.
    same_context = ctx(
        "commercial_question",
        "price",
        "commercial",
        "preco",
        requires_response=True,
    )
    same_context["output_id"] = "fixed-input-id"

    first = decision_coach_process_context(
        same_context,
        publish=False,
        persist=False,
        now=now + 10,
    )
    second = decision_coach_process_context(
        same_context,
        publish=False,
        persist=False,
        now=now + 11,
    )

    check(
        "input id duplicado ignorado",
        first is not None
        and second is None
        and decision_coach_stats.get("duplicate_inputs", 0) >= 1,
        {
            "first": first,
            "second": second,
            "stats": dict(decision_coach_stats),
        },
    )

    # 29. Fila recebe decisao.
    check(
        "fila de saida recebe decisoes",
        decision_coach_output_queue is not None
        and decision_coach_output_queue.qsize() >= 1,
        decision_coach_output_queue.qsize()
        if decision_coach_output_queue is not None
        else None,
    )

    # 30. Historico de exibidos existe.
    check(
        "historico de exibidos atualizado",
        len(decision_coach_display_history) >= 1,
        len(decision_coach_display_history),
    )

    # 31. Presets oficiais existem.
    modes = decision_coach_alert_modes_info()
    check(
        "tres modos oficiais disponiveis",
        set(modes) == {"all", "high", "essential"},
        modes,
    )

    # 32. Alias em portugues funciona.
    decision_coach_reset_settings()
    high_settings = decision_coach_set_alert_mode("Prioridade Alta")
    check(
        "alias Prioridade Alta seleciona high",
        high_settings.get("alert_mode") == "high"
        and high_settings.get("min_priority") == 75,
        high_settings,
    )

    # 33. Essenciais configurado em 90.
    essential_settings = decision_coach_set_alert_mode("Essenciais")
    check(
        "Essenciais seleciona threshold 90",
        essential_settings.get("alert_mode") == "essential"
        and essential_settings.get("min_priority") == 90,
        essential_settings,
    )

    # 34. Todos deixa pergunta util de baixa prioridade passar.
    decision_coach_set_alert_mode("Todos")
    low_question_ctx = ctx(
        "product_question",
        "generic_characteristic",
        "product",
        "caracteristica",
        requires_response=True,
    )
    low_question_ctx["question_group"] = {
        "count": 1,
        "unique_users": 1,
        "same_question": False,
        "normalized_question": "o que e isso",
        "examples": [
            {"user": "Ana", "text": "o que e isso?"}
        ],
    }
    low_all = decision_coach_preview_context(
        low_question_ctx,
        now=now,
    )
    check(
        "Todos mostra pergunta util distinta",
        low_all.get("action") == "display"
        and low_all.get("alert_mode") == "all",
        low_all,
    )

    # 35. Pergunta unica usa nome/texto quando o Fusion preservou exemplo.
    check(
        "pergunta unica gera texto com usuario e pergunta",
        "Ana perguntou" in _dc_safe_text(low_all.get("text"))
        and "o que e isso?" in _dc_safe_text(low_all.get("text")),
        low_all.get("text"),
    )

    # 36. Grupo repetido vira uma unica orientacao agregada.
    repeated_ctx = ctx(
        "commercial_question",
        "price",
        "commercial",
        "preco",
        count=3,
        unique_users=3,
        requires_response=True,
    )
    repeated_ctx["question_group"] = {
        "count": 3,
        "unique_users": 3,
        "same_question": True,
        "normalized_question": "qual o valor",
        "examples": [
            {"user": "Ana", "text": "qual o valor?"},
            {"user": "Joao", "text": "quanto custa?"},
            {"user": "Maria", "text": "preco?"},
        ],
    }
    repeated_all = decision_coach_preview_context(
        repeated_ctx,
        now=now,
    )
    check(
        "repeticao agrupada comunica varias pessoas uma vez",
        repeated_all.get("action") == "display"
        and "3 pessoas" in _dc_safe_text(repeated_all.get("text")),
        repeated_all,
    )

    # 37. Duas perguntas diferentes do mesmo subtipo recebem chaves distintas.
    gps_ctx = ctx(
        "product_question",
        "connectivity",
        "product",
        "conectividade",
        requires_response=True,
    )
    gps_ctx["question_group"] = {
        "count": 1,
        "unique_users": 1,
        "same_question": False,
        "normalized_question": "tem gps",
        "examples": [{"user": "A", "text": "tem GPS?"}],
    }
    call_ctx = copy.deepcopy(gps_ctx)
    call_ctx["output_id"] = uuid.uuid4().hex
    call_ctx["question_group"] = {
        "count": 1,
        "unique_users": 1,
        "same_question": False,
        "normalized_question": "faz ligacao",
        "examples": [{"user": "B", "text": "faz ligacao?"}],
    }
    gps_preview = decision_coach_preview_context(gps_ctx, now=now)
    call_preview = decision_coach_preview_context(call_ctx, now=now)
    check(
        "perguntas distintas do mesmo subtipo nao colidem",
        gps_preview.get("decision_key") != call_preview.get("decision_key"),
        {
            "gps": gps_preview.get("decision_key"),
            "call": call_preview.get("decision_key"),
        },
    )

    # 38. High segura pergunta comum de produto.
    decision_coach_set_alert_mode("high")
    ordinary_high = decision_coach_preview_context(
        low_question_ctx,
        now=now,
    )
    check(
        "Prioridade Alta filtra pergunta comum",
        ordinary_high.get("action") == "suppress"
        and ordinary_high.get("reason") == "below_mode_priority",
        ordinary_high,
    )

    # 39. High mostra preco que requer resposta (score >= 75).
    high_price_ctx = ctx(
        "commercial_question",
        "price",
        "commercial",
        "preco",
        requires_response=True,
    )
    high_price = decision_coach_preview_context(
        high_price_ctx,
        now=now,
    )
    check(
        "Prioridade Alta mostra pergunta comercial forte",
        high_price.get("action") == "display"
        and high_price.get("priority") >= 75,
        high_price,
    )

    # 40. High mostra pergunta repetida por varias pessoas.
    repeated_product = ctx(
        "product_question",
        "generic_characteristic",
        "product",
        "caracteristica",
        count=3,
        unique_users=3,
        requires_response=True,
    )
    repeated_product["strength"] = 0.82
    high_repeated = decision_coach_preview_context(
        repeated_product,
        now=now,
    )
    check(
        "Prioridade Alta promove pergunta repetida",
        high_repeated.get("action") == "display"
        and high_repeated.get("priority") >= 75,
        high_repeated,
    )

    # 41. Essenciais segura preco comum.
    decision_coach_set_alert_mode("essential")
    essential_price = decision_coach_preview_context(
        high_price_ctx,
        now=now,
    )
    check(
        "Essenciais filtra preco comum",
        essential_price.get("action") == "suppress"
        and essential_price.get("reason") == "below_mode_priority",
        essential_price,
    )

    # 42. Essenciais mostra cobranca duplicada critica.
    essential_charge = decision_coach_preview_context(
        ctx(
            "post_purchase_issue",
            "payment_charge_problem",
            "post_purchase",
            "problema de cobranca",
            requires_response=True,
        ),
        now=now,
    )
    check(
        "Essenciais mostra problema critico",
        essential_charge.get("action") == "display"
        and essential_charge.get("priority") >= 90,
        essential_charge,
    )

    # 43. Snapshot nunca vira dica direta, nem em Todos.
    decision_coach_set_alert_mode("all")
    snapshot_ctx = ctx(
        "commercial_question",
        "price",
        "commercial",
        "preco",
        requires_response=True,
    )
    snapshot_ctx["message_type"] = "context_snapshot"
    snapshot_preview = decision_coach_preview_context(
        snapshot_ctx,
        now=now,
    )
    check(
        "context_snapshot e interno e nao vira dica",
        snapshot_preview.get("action") == "suppress"
        and snapshot_preview.get("reason") == "context_snapshot_internal",
        snapshot_preview,
    )

    # 44. Referencia contextual isolada continua silenciosa em Todos.
    contextual_all = decision_coach_preview_context(
        ctx(
            "commercial_observation",
            "contextual_product_reference",
            "commercial",
            "referencia contextual",
        ),
        now=now,
    )
    check(
        "Todos nao exibe referencia contextual isolada",
        contextual_all.get("action") == "suppress"
        and contextual_all.get("reason") == "context_only_reference",
        contextual_all,
    )

    # 45. Configuracao numerica direta preserva compatibilidade e vira custom.
    custom_settings = decision_coach_update_settings(
        min_priority=88,
    )
    check(
        "override numerico vira modo custom",
        custom_settings.get("alert_mode") == "custom"
        and custom_settings.get("min_priority") == 88,
        custom_settings,
    )

    # 46. Mojibake deve ser reparado antes de chegar a Interface.
    repaired = _dc_safe_text("HÃÂ¡ confirmaÃÂ§ÃÂ£o e apresentaÃÂ§ÃÂ£o.")
    check(
        "corrige mojibake UTF-8/Latin-1",
        repaired == "HÃ¡ confirmaÃ§Ã£o e apresentaÃ§Ã£o.",
        repaired,
    )

    # 47. Follow/share isolados sao apenas contexto, inclusive em Todos.
    decision_coach_set_alert_mode("all")
    follow_only = decision_coach_preview_context(
        ctx(
            audience=["follow_activity"],
            context_kind="audience_signal",
        ),
        now=now,
    )
    check(
        "follow isolado nao vira dica",
        follow_only.get("action") == "suppress"
        and follow_only.get("reason") == "audience_context_only",
        follow_only,
    )

    # 48. Compra concluida usa texto natural, sem linguagem de engenharia.
    purchase_ctx = ctx(
        "purchase_completed",
        "reported_purchase",
        "commercial",
        "compra concluida",
    )
    purchase_ctx["question_group"] = {
        "count": 1,
        "unique_users": 1,
        "same_question": False,
        "normalized_question": "fiz a compra",
        "examples": [
            {"user": "Eidimar", "text": "fiz a compra."},
        ],
    }
    purchase_preview = decision_coach_preview_context(
        purchase_ctx,
        now=now,
    )
    purchase_text = _dc_safe_text(purchase_preview.get("text"))
    check(
        "compra concluida gera orientacao natural",
        "Eidimar disse que concluiu a compra" in purchase_text
        and "confirmaÃ§Ã£o transacional" not in purchase_text.lower(),
        purchase_text,
    )

    # 49. Frases equivalentes de compra compartilham a mesma decision_key.
    purchase_ctx_2 = copy.deepcopy(purchase_ctx)
    purchase_ctx_2["output_id"] = uuid.uuid4().hex
    purchase_ctx_2["question_group"] = {
        "count": 1,
        "unique_users": 1,
        "same_question": False,
        "normalized_question": "comprei",
        "examples": [
            {"user": "Outra Pessoa", "text": "comprei"},
        ],
    }
    purchase_preview_2 = decision_coach_preview_context(
        purchase_ctx_2,
        now=now,
    )
    check(
        "compra repetida usa chave semantica estavel",
        purchase_preview.get("decision_key")
        == purchase_preview_2.get("decision_key")
        == "commercial:purchase_completed:reported_purchase",
        {
            "a": purchase_preview.get("decision_key"),
            "b": purchase_preview_2.get("decision_key"),
        },
    )

    # 50. O modo Todos respeita cadencia visual minima de cinco segundos.
    all_settings = decision_coach_set_alert_mode("all")
    check(
        "Todos respeita intervalo visual minimo",
        float(all_settings.get("global_cooldown_seconds", 0)) >= 5.0,
        all_settings,
    )

    # 51. Decision inclui horario local e duracao sugerida para Interface.
    decision_coach_reset("tiktok")
    processed_time = decision_coach_process_context(
        copy.deepcopy(purchase_ctx),
        publish=False,
        persist=False,
        now=now,
    )
    check(
        "saida inclui horario local e display de cinco segundos",
        isinstance(processed_time, dict)
        and bool(processed_time.get("display_time"))
        and processed_time.get("display_seconds") == 5,
        processed_time,
    )

    # 52. Fingerprint suprimido nao deve renovar bloqueio indefinidamente.
    decision_coach_reset("tiktok")
    first_ctx = ctx(
        "purchase_completed",
        "reported_purchase",
        "commercial",
        "compra concluida",
    )
    first = decision_coach_process_context(
        first_ctx,
        publish=False,
        persist=False,
        now=now,
    )
    second_ctx = copy.deepcopy(first_ctx)
    second_ctx["output_id"] = uuid.uuid4().hex
    second = decision_coach_process_context(
        second_ctx,
        publish=False,
        persist=False,
        now=now + 20,
    )
    fp = first.get("fingerprint") if isinstance(first, dict) else None
    fp_ts = decision_coach_recent_fingerprints.get(fp)
    check(
        "supressao por repeticao nao renova timestamp do fingerprint",
        isinstance(second, dict)
        and second.get("action") == "suppress"
        and fp_ts == now,
        {
            "first": first,
            "second": second,
            "fingerprint_timestamp": fp_ts,
        },
    )

    # 53. Crescimento real do grupo pode ser tratado como escalada.
    decision_coach_last_display_by_key.clear()
    decision_coach_last_display_by_key["commercial:purchase_completed:reported_purchase"] = {
        "timestamp": now,
        "priority": 74,
        "evidence_count": 1,
        "group_count": 1,
        "unique_users": 1,
    }
    check(
        "crescimento real do grupo permite escalada",
        _dc_is_escalation(
            "commercial:purchase_completed:reported_purchase",
            74,
            1,
            group_count=3,
            unique_users=3,
            now=now + 10,
        ),
        decision_coach_last_display_by_key,
    )

    # 54. Voltar ao padrao oficial antes de finalizar o teste.
    reset_settings = decision_coach_reset_settings()
    check(
        "reset volta para Todos",
        reset_settings.get("alert_mode") == "all",
        reset_settings,
    )

    # Limpar estado do teste para nao contaminar uma LIVE futura.
    decision_coach_reset(None)

    total = approved + len(failures)

    print()
    print("=" * 76)
    print("RESUMO DO TESTE DECISION COACH V1.2")
    print("=" * 76)
    print("Aprovados:", approved)
    print("Falhas:", len(failures))
    print("Total:", total)

    result = {
        "approved": approved,
        "failed": len(failures),
        "total": total,
        "ok": len(failures) == 0,
        "fusion_contract": {
            "message_types": sorted(
                DECISION_COACH_FUSION_CONTRACT[
                    "accepted_message_types"
                ]
            ),
            "context_fusion_available_now": callable(
                globals().get(
                    "context_fusion_next_output"
                )
            ),
        },
        "failures": failures,
    }

    print(result)
    return result


# Compatibilidade com celulas antigas do notebook.
testar_decision_coach_v11 = testar_decision_coach_v12
testar_decision_coach_v1 = testar_decision_coach_v12


# ============================================================
# 45. FINALIZAR TASK
# ============================================================

async def _decision_coach_finish_task(task):
    if task is None:
        return

    try:
        await asyncio.wait_for(
            asyncio.shield(task),
            timeout=2.5,
        )
        return

    except asyncio.TimeoutError:
        pass

    except asyncio.CancelledError:
        pass

    except Exception:
        return

    if not task.done():
        task.cancel()

    try:
        await task
    except BaseException:
        pass


# ============================================================
# 46. ANEXAR AO RUNTIME
#
# So anexamos wrappers quando Context Fusion ja existe.
# Isso permite construir/testar este modulo antes do Fusion sem
# causar timeout ou erro em uma LIVE atual.
# ============================================================

def decision_coach_attach_runtime():
    global decision_coach_runtime_attached
    global executar_shopee_com_live_engine
    global executar_tiktok_com_live_engine

    if not callable(
        globals().get("context_fusion_next_output")
    ):
        decision_coach_runtime_attached = False
        return False

    current_shopee = executar_shopee_com_live_engine

    if getattr(
        current_shopee,
        "_agcn_decision_coach_wrapper",
        False,
    ):
        base_shopee = (
            current_shopee
            ._agcn_decision_coach_base
        )
    else:
        base_shopee = current_shopee

    current_tiktok = executar_tiktok_com_live_engine

    if getattr(
        current_tiktok,
        "_agcn_decision_coach_wrapper",
        False,
    ):
        base_tiktok = (
            current_tiktok
            ._agcn_decision_coach_base
        )
    else:
        base_tiktok = current_tiktok

    async def _wrapped_shopee():
        decision_coach_reset("shopee")

        task = asyncio.create_task(
            decision_coach_supervisor(
                "shopee"
            )
        )

        try:
            return await base_shopee()
        finally:
            await _decision_coach_finish_task(
                task
            )

    _wrapped_shopee._agcn_decision_coach_wrapper = True
    _wrapped_shopee._agcn_decision_coach_base = base_shopee
    _wrapped_shopee._agcn_decision_coach_version = DECISION_COACH_VERSION

    async def _wrapped_tiktok(username):
        decision_coach_reset("tiktok")

        task = asyncio.create_task(
            decision_coach_supervisor(
                "tiktok"
            )
        )

        try:
            return await base_tiktok(username)
        finally:
            await _decision_coach_finish_task(
                task
            )

    _wrapped_tiktok._agcn_decision_coach_wrapper = True
    _wrapped_tiktok._agcn_decision_coach_base = base_tiktok
    _wrapped_tiktok._agcn_decision_coach_version = DECISION_COACH_VERSION

    executar_shopee_com_live_engine = _wrapped_shopee
    executar_tiktok_com_live_engine = _wrapped_tiktok

    decision_coach_runtime_attached = True
    return True


# ============================================================
# 47. TENTAR ANEXAR AUTOMATICAMENTE
# ============================================================

_DECISION_COACH_AUTO_ATTACHED = (
    decision_coach_attach_runtime()
)


# ============================================================
# 48. PRONTO - V1.2
# ============================================================

print("AGCN Decision Coach V1.2 carregado.")
print("Versao:", DECISION_COACH_VERSION)
print("Entrada oficial: Context Fusion V1.1.")
print("Decide:")
print("1. se deve avisar")
print("2. prioridade operacional")
print("3. TTL")
print("4. cooldown")
print("5. deduplicacao")
print("6. repeticao ja exibida")
print("7. configuracoes de avisos do usuario")
print("Modos oficiais: Todos / Prioridade Alta / Essenciais.")
print("Todos = todas as perguntas uteis distintas; repeticoes ficam agrupadas pelo Fusion.")
print("Integra com Coach Armazenamento e Interface V7.1/V7.")

if _DECISION_COACH_AUTO_ATTACHED:
    print("Runtime LIVE: anexado ao Context Fusion.")
else:
    print(
        "Runtime LIVE: nao anexado. Verifique se o Context Fusion V1.1 foi "
        "executado antes desta celula."
    )
    print(
        "O modulo continua disponivel para teste interno; para LIVE real, "
        "execute o Context Fusion antes do Decision Coach."
    )
