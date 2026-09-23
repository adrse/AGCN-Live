# ============================================================
# AGCN LIVE - CONTEXT FUSION V1.1
#
# ARQUITETURA:
#
# Coach Produto V1.1 --------------------\
# Coach Comercial V1 ---------------------\
# Coach Objecoes/Pos-compra V1 ------------> CONTEXT FUSION V1.1
# Worker Coach Audiencia V1 ---------------/
#                                              |
#                                              v
#                                      Decision Coach V1
#
# RESPONSABILIDADES:
#
# - consumir, de forma independente, as quatro fontes acima
# - normalizar os outputs para um contexto comum
# - preservar perguntas/comentarios individuais relevantes
# - JUNTAR repeticoes da mesma pergunta em um unico contexto
# - preservar perguntas diferentes, mesmo quando pertencem ao
#   mesmo subtipo amplo (ex.: GPS x ligacao, ambos connectivity)
# - fundir multi-intencoes do MESMO comentario que passaram por
#   mais de um Coach especializado
# - incorporar sinais locais de repeticao sem duplicar avisos
# - manter uma janela curta do estado atual da LIVE
# - correlacionar dominios que acontecem proximos no tempo
# - combinar contexto de comentarios com sinais de audiencia
# - produzir contextos estruturados para o Decision Coach
#
# V1.1:
# - consolida a mesma situacao mesmo quando reaparece em janelas diferentes
# - colapsa capturas tecnicas duplicadas do mesmo comentario
# - deixa follow/share/gift_activity e viewer_stable como contexto, sem
#   candidato individual de aviso
# - mantem snapshots no historico/API, fora da fila operacional por padrao
# - reemite uma situacao repetida apenas quando existe crescimento real
#
# IMPORTANTE PARA O FUTURO BOTAO DA INTERFACE:
#
# A Interface tera os modos:
#   Todos / Prioridade Alta / Essenciais
#
# O Context Fusion NAO escolhe esse modo e NAO calcula prioridade.
# Ele entrega ao Decision Coach informacao suficiente para que:
#
# - em "Todos", perguntas diferentes possam aparecer individualmente,
#   mas perguntas iguais/repetidas sejam agrupadas;
# - em "Prioridade Alta", o Decision Coach possa preferir movimentos
#   com repeticao, intencao de compra, barreiras, contexto cruzado etc.;
# - em "Essenciais", o Decision Coach possa filtrar apenas situacoes
#   realmente fortes/urgentes.
#
# NAO FAZ:
#
# - nao decide prioridade final
# - nao decide se deve mostrar na Interface
# - nao escreve a orientacao final do Coach
# - nao aplica cooldown final de exibicao
# - nao consulta Coach Armazenamento
# - nao publica na Interface
# - nao inventa informacoes de produto/comerciais
# - nao atribui causalidade entre comentario e audiencia
#
# SAIDAS:
#
# 1. fusion_event
#    Um comentario/pergunta individual ou grupo de perguntas iguais,
#    possivelmente com mais de um dominio (produto + comercial etc.).
#
# 2. fusion_signal
#    Movimento agregado/correlacao contextual sem decisao de prioridade.
#
# 3. context_snapshot
#    Fotografia compacta do estado atual da LIVE.
#
# ============================================================

import asyncio
import copy
import difflib
import hashlib
import json
import re
import time
import unicodedata
import uuid

from collections import Counter, defaultdict, deque
from datetime import datetime
from zoneinfo import ZoneInfo


# ============================================================
# 1. DEPENDENCIAS
# ============================================================

_CF_REQUIRED = [
    "live_engine_is_running",
    "live_engine_platform",
    "executar_shopee_com_live_engine",
    "executar_tiktok_com_live_engine",
    "coach_product_next_output",
    "coach_commercial_next_output",
    "coach_objections_next_output",
    "worker_coach_audience_next_output",
]

_CF_MISSING = [
    name
    for name in _CF_REQUIRED
    if name not in globals()
]

if _CF_MISSING:
    raise RuntimeError(
        "Execute primeiro Coach Produto V1.1, Coach Comercial V1, "
        "Coach Objecoes/Pos-compra V1 e Worker Coach Audiencia V1. "
        "Faltando: " + ", ".join(_CF_MISSING)
    )


# ============================================================
# 2. CONFIGURACAO
# ============================================================

CONTEXT_FUSION_VERSION = "1.1"
CONTEXT_FUSION_TIMEZONE = ZoneInfo("America/Araguaina")

CONTEXT_FUSION_OUTPUT_QUEUE_LIMIT = 5000
CONTEXT_FUSION_MAX_HISTORY = 5000
CONTEXT_FUSION_MAX_SEEN = 30000

# Segura brevemente a pergunta para permitir:
# - a mesma classificacao chegar de mais de um Coach;
# - repeticoes muito proximas serem agrupadas.
CONTEXT_FUSION_COALESCE_SECONDS = 1.20
CONTEXT_FUSION_MAX_HOLD_SECONDS = 2.80

# Sinal agregado de Coach fica um pouco em espera para poder ser anexado
# a uma pergunta que ainda esta sendo fundida.
CONTEXT_FUSION_TOPIC_SIGNAL_HOLD_SECONDS = 0.90

# Janela do estado atual da LIVE.
CONTEXT_FUSION_STATE_WINDOW_SECONDS = 60

# Janela mais curta usada para correlacoes entre dominios.
CONTEXT_FUSION_CORRELATION_WINDOW_SECONDS = 20

# Snapshots sao compactos e somente saem quando o estado muda.
CONTEXT_FUSION_SNAPSHOT_INTERVAL_SECONDS = 10.0

# Nao repetir a mesma correlacao em sequencia.
CONTEXT_FUSION_CORRELATION_COOLDOWN_SECONDS = 20

# Sinal de topico nao e reemitido se um grupo de perguntas equivalente
# acabou de representar o mesmo movimento.
CONTEXT_FUSION_REDUNDANT_TOPIC_SECONDS = 20

# Similaridade de texto para considerar duas perguntas como repeticoes.
CONTEXT_FUSION_TEXT_SIMILARITY = 0.84
CONTEXT_FUSION_TOKEN_SIMILARITY = 0.72

CONTEXT_FUSION_MAX_EVIDENCE_PER_OUTPUT = 20
CONTEXT_FUSION_MAX_COMMENTS_PER_OUTPUT = 12

# V1.1 - memoria semantica entre janelas de coalescencia.
# Evita que a mesma situacao volte como novo aviso a cada poucos segundos.
CONTEXT_FUSION_REPEAT_MEMORY_SECONDS = 30.0
CONTEXT_FUSION_REPEAT_REFRESH_SECONDS = 20.0
CONTEXT_FUSION_REPEAT_REEMIT_MIN_NEW_OCCURRENCES = 2
CONTEXT_FUSION_REPEAT_REEMIT_MIN_NEW_USERS = 2

# Capturas duplicadas do mesmo comentario podem chegar com IDs tecnicos
# diferentes. Nesta janela curta, mesmo usuario + mesmo texto e tratado
# como a mesma evidencia quando nao ha um event_id estavel.
CONTEXT_FUSION_COMMENT_DEDUPE_SECONDS = 3.0

# Nem todo sinal de audiencia deve virar um aviso individual.
# Estes sinais permanecem no contexto para correlacoes/snapshot, mas nao
# sao enviados sozinhos ao Decision Coach.
CONTEXT_FUSION_AUDIENCE_STATE_ONLY_TYPES = {
    "viewer_stable",
    "share_activity",
    "follow_activity",
    "gift_activity",
}
CONTEXT_FUSION_AUDIENCE_DIRECT_COOLDOWN_SECONDS = 15.0

# Snapshot e diagnostico de estado. Em V1.1 ele fica no historico/API e
# nao compete com eventos acionaveis na fila do Decision Coach.
CONTEXT_FUSION_QUEUE_SNAPSHOTS_TO_DECISION = False


# ============================================================
# 3. FONTES
# ============================================================

CONTEXT_FUSION_SOURCES = {
    "product": {
        "next": "coach_product_next_output",
        "queue": "coach_product_output_queue",
        "running": "coach_product_running",
    },
    "commercial": {
        "next": "coach_commercial_next_output",
        "queue": "coach_commercial_output_queue",
        "running": "coach_commercial_running",
    },
    "objections": {
        "next": "coach_objections_next_output",
        "queue": "coach_objections_output_queue",
        "running": "coach_objections_running",
    },
    "audience": {
        "next": "worker_coach_audience_next_output",
        "queue": "worker_coach_audience_output_queue",
        "running": "worker_coach_audience_running",
    },
}

_CONTEXT_FUSION_INDIVIDUAL_TYPES = {
    "product_analysis",
    "commercial_analysis",
    "objections_analysis",
}

_CONTEXT_FUSION_TOPIC_SIGNAL_TYPES = {
    "product_topic_signal",
    "commercial_topic_signal",
    "objections_topic_signal",
}


# ============================================================
# 4. ESTADO
# ============================================================

context_fusion_output_queue = None

context_fusion_output_history = deque(
    maxlen=CONTEXT_FUSION_MAX_HISTORY
)
context_fusion_event_history = deque(
    maxlen=CONTEXT_FUSION_MAX_HISTORY
)
context_fusion_signal_history = deque(
    maxlen=CONTEXT_FUSION_MAX_HISTORY
)
context_fusion_snapshot_history = deque(
    maxlen=CONTEXT_FUSION_MAX_HISTORY
)
context_fusion_input_history = deque(
    maxlen=CONTEXT_FUSION_MAX_HISTORY
)

context_fusion_pending_clusters = {}
context_fusion_root_to_cluster = {}
context_fusion_pending_topic_signals = {}

# Estado temporal por topico.
context_fusion_topic_windows = defaultdict(deque)
context_fusion_latest_topic_signal = {}
context_fusion_recent_question_topic_emissions = {}

# Audiencia recente.
context_fusion_audience_window = deque(
    maxlen=CONTEXT_FUSION_MAX_HISTORY
)

# Dedupe de inputs.
context_fusion_seen_input_ids = set()
context_fusion_seen_input_order = deque(
    maxlen=CONTEXT_FUSION_MAX_SEEN
)

# Correlacoes/snapshot.
context_fusion_last_correlation_emitted = {}
context_fusion_last_snapshot_time = 0.0
context_fusion_last_snapshot_fingerprint = None

# V1.1 - memoria de situacoes ja emitidas.
context_fusion_recent_semantic_events = {}
context_fusion_recent_comment_fingerprints = {}
context_fusion_last_audience_event_emitted = {}

context_fusion_running = False
context_fusion_platform = None
context_fusion_live_id = None
context_fusion_subject = None
context_fusion_started_at = None
context_fusion_last_error = None

context_fusion_stats = Counter()
context_fusion_source_counts = Counter()
context_fusion_message_type_counts = Counter()


# ============================================================
# 5. UTILITARIOS
# ============================================================

def _cf_now_iso():
    return datetime.now(
        CONTEXT_FUSION_TIMEZONE
    ).isoformat()


def _cf_number(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _cf_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _cf_safe_text(value):
    if value is None:
        return ""
    return str(value).strip()


def _cf_unique_strings(values):
    result = []
    seen = set()
    for value in values or []:
        value = _cf_safe_text(value)
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _cf_clamp(value, low=0.0, high=1.0):
    try:
        value = float(value)
    except Exception:
        value = low
    return max(low, min(high, value))


def _cf_normalize_text(text):
    text = _cf_safe_text(text).lower()
    text = "".join(
        ch
        for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _cf_text_tokens(text):
    return {
        token
        for token in _cf_normalize_text(text).split()
        if len(token) >= 2
    }


def _cf_text_similarity(a, b):
    a = _cf_normalize_text(a)
    b = _cf_normalize_text(b)

    if not a or not b:
        return 0.0, 0.0

    if a == b:
        return 1.0, 1.0

    seq = difflib.SequenceMatcher(
        None,
        a,
        b,
    ).ratio()

    ta = _cf_text_tokens(a)
    tb = _cf_text_tokens(b)

    if not ta or not tb:
        jac = 0.0
    else:
        jac = len(ta & tb) / max(1, len(ta | tb))

    return seq, jac


def _cf_stable_hash(data):
    raw = json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha1(
        raw.encode("utf-8")
    ).hexdigest()


def _cf_source_domain(source):
    if source == "product":
        return "product"
    if source == "commercial":
        return "commercial"
    if source == "objections":
        return "objections"
    if source == "audience":
        return "audience"
    return None


def _cf_domain_from_category(category, fallback=None):
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

    return fallback


# ============================================================
# 6. DEDUPE DE INPUT
# ============================================================

def _cf_seen_input(output):
    output_id = (
        output.get("output_id")
        or output.get("id")
    )

    if not output_id:
        return False

    if output_id in context_fusion_seen_input_ids:
        context_fusion_stats["duplicates_ignored"] += 1
        return True

    if (
        len(context_fusion_seen_input_order)
        >= CONTEXT_FUSION_MAX_SEEN
    ):
        old = context_fusion_seen_input_order.popleft()
        context_fusion_seen_input_ids.discard(old)

    context_fusion_seen_input_order.append(output_id)
    context_fusion_seen_input_ids.add(output_id)
    return False


# ============================================================
# 7. TOPICO CANONICO
# ============================================================

def _cf_canonical_topic(raw, source=None):
    if not isinstance(raw, dict):
        return None

    category = raw.get("category")
    subtype = raw.get("subtype") or raw.get("topic")

    if not category and not subtype:
        return None

    domain = (
        raw.get("domain")
        or _cf_domain_from_category(
            category,
            _cf_source_domain(source),
        )
    )

    target = (
        raw.get("answer_target")
        or raw.get("action_target")
        or raw.get("resolution_target")
        or raw.get("information_need")
        or raw.get("need")
    )

    interpretation = (
        raw.get("interpretation_target")
        or raw.get("meaning")
    )

    result = {
        "domain": domain,
        "category": category,
        "subtype": subtype,
        "family": raw.get("family"),
        "label": (
            raw.get("label")
            or raw.get("topic_label")
            or subtype
            or category
        ),
        # Campo generico lido pelo Decision Coach.
        "information_need": target,
        "interpretation_target": interpretation,
        "requires_response": bool(
            raw.get("requires_response")
            or raw.get("response_needed")
            or raw.get("needs_resolution")
        ),
        "count": max(1, _cf_int(raw.get("count"), 1)),
        "unique_users": max(
            0,
            _cf_int(raw.get("unique_users"), 0),
        ),
        "strength": _cf_number(raw.get("strength"), None),
    }

    # Preservar nomes especificos para compatibilidade futura.
    for key in (
        "answer_target",
        "action_target",
        "resolution_target",
    ):
        if raw.get(key):
            result[key] = raw.get(key)

    return result


def _cf_topic_key(topic):
    if not isinstance(topic, dict):
        return "unknown"
    return ":".join([
        _cf_safe_text(topic.get("domain") or "unknown"),
        _cf_safe_text(topic.get("category") or "unknown"),
        _cf_safe_text(topic.get("subtype") or "unknown"),
    ])


def _cf_extract_topics(output, source=None):
    result = []

    raw_topics = output.get("topics")
    if isinstance(raw_topics, list):
        for raw in raw_topics:
            topic = _cf_canonical_topic(
                raw,
                source=source,
            )
            if topic:
                result.append(topic)

    raw_topic = output.get("topic")
    if isinstance(raw_topic, dict):
        topic = _cf_canonical_topic(
            raw_topic,
            source=source,
        )
        if topic:
            result.append(topic)

    # Dedupe local por chave.
    merged = {}
    for topic in result:
        key = _cf_topic_key(topic)
        if key not in merged:
            merged[key] = topic
            continue
        current = merged[key]
        current["requires_response"] = bool(
            current.get("requires_response")
            or topic.get("requires_response")
        )
        if not current.get("information_need"):
            current["information_need"] = topic.get(
                "information_need"
            )
        if not current.get("interpretation_target"):
            current["interpretation_target"] = topic.get(
                "interpretation_target"
            )

    return list(merged.values())


# ============================================================
# 8. COMENTARIO CANONICO
# ============================================================

def _cf_extract_comment(output):
    comment = output.get("comment")
    if not isinstance(comment, dict):
        return None

    text = _cf_safe_text(comment.get("text"))
    user = _cf_safe_text(comment.get("user"))
    event_id = comment.get("event_id")

    if not text and not user and not event_id:
        return None

    return {
        "event_id": event_id,
        "user": user or None,
        "text": text or None,
        "display_time": comment.get("display_time"),
    }


def _cf_root_id(output):
    comment = _cf_extract_comment(output) or {}

    # V1.1: o event_id do comentario representa melhor a evidencia real
    # do que IDs tecnicos criados em cada etapa do pipeline. Isso evita
    # contar o mesmo comentario duas vezes quando ele passa por mais de
    # um Coach ou quando um envelope tecnico e recriado.
    return (
        comment.get("event_id")
        or output.get("source_output_id")
        or output.get("source_dispatch_id")
        or output.get("output_id")
        or uuid.uuid4().hex
    )


def _cf_comment_fingerprint(comment):
    if not isinstance(comment, dict):
        return None

    user = _cf_normalize_text(comment.get("user"))
    text = _cf_normalize_text(comment.get("text"))

    if not text:
        return None

    return _cf_stable_hash({
        "user": user,
        "text": text,
    })


def _cf_canonical_root_id(output, comment, ingest_now):
    root_id = _cf_root_id(output)

    # Com event_id real, ele e a identidade canonica e nao precisamos
    # inferir nada por texto.
    event_id = (comment or {}).get("event_id") if isinstance(comment, dict) else None
    if event_id:
        return root_id

    fingerprint = _cf_comment_fingerprint(comment)
    if not fingerprint:
        return root_id

    previous = context_fusion_recent_comment_fingerprints.get(
        fingerprint
    )

    if previous:
        age = ingest_now - _cf_number(
            previous.get("timestamp"),
            0,
        )
        if age <= CONTEXT_FUSION_COMMENT_DEDUPE_SECONDS:
            context_fusion_stats[
                "comment_capture_duplicates_collapsed"
            ] += 1
            previous["timestamp"] = ingest_now
            return previous.get("root_id") or root_id

    context_fusion_recent_comment_fingerprints[fingerprint] = {
        "root_id": root_id,
        "timestamp": ingest_now,
    }
    return root_id


def _cf_prune_comment_fingerprints(now=None):
    if now is None:
        now = time.time()

    limit = now - max(
        10.0,
        CONTEXT_FUSION_COMMENT_DEDUPE_SECONDS * 3.0,
    )

    for key in list(context_fusion_recent_comment_fingerprints.keys()):
        item = context_fusion_recent_comment_fingerprints.get(key) or {}
        if _cf_number(item.get("timestamp"), 0) < limit:
            context_fusion_recent_comment_fingerprints.pop(key, None)


# ============================================================
# 9. EVIDENCIA COMPACTA
# ============================================================

def _cf_compact_evidence(source, output):
    evidence = output.get("evidence")
    if not isinstance(evidence, dict):
        evidence = {}

    return {
        "output_id": output.get("output_id"),
        "message_type": output.get("message_type"),
        "analysis_type": output.get("analysis_type"),
        "source_coach": output.get("source_coach"),
        "source_worker": output.get("source_worker"),
        "source": source,
        "source_output_id": output.get("source_output_id"),
        "source_dispatch_id": output.get("source_dispatch_id"),
        "timestamp": output.get("timestamp"),
        "response_needed": bool(
            output.get("response_needed")
            or output.get("needs_resolution")
        ),
        "topics": copy.deepcopy(
            output.get("topics") or []
        ),
        "topic": copy.deepcopy(
            output.get("topic")
            if isinstance(output.get("topic"), dict)
            else None
        ),
        "comment": copy.deepcopy(
            _cf_extract_comment(output)
        ),
        "signal_type": output.get("signal_type"),
        "data": copy.deepcopy(
            output.get("data")
            if isinstance(output.get("data"), dict)
            else None
        ),
        "evidence": {
            "count": evidence.get("count"),
            "unique_users": evidence.get("unique_users"),
            "max_repetitions_by_one_user": evidence.get(
                "max_repetitions_by_one_user"
            ),
            "multiple_users": evidence.get("multiple_users"),
            "repeated_by_same_user": evidence.get(
                "repeated_by_same_user"
            ),
            "examples": copy.deepcopy(
                evidence.get("examples") or []
            ),
            "strength": evidence.get("strength"),
        },
    }


# ============================================================
# 10. FILA DE SAIDA
# ============================================================

def _cf_queue_output(output):
    global context_fusion_output_queue

    if context_fusion_output_queue is None:
        return False

    try:
        context_fusion_output_queue.put_nowait(
            copy.deepcopy(output)
        )

    except asyncio.QueueFull:
        try:
            context_fusion_output_queue.get_nowait()
        except Exception:
            pass

        context_fusion_stats[
            "dropped_output_queue_full"
        ] += 1

        try:
            context_fusion_output_queue.put_nowait(
                copy.deepcopy(output)
            )
        except Exception:
            return False

    context_fusion_output_history.append(
        copy.deepcopy(output)
    )

    message_type = output.get("message_type")

    if message_type == "fusion_event":
        context_fusion_event_history.append(
            copy.deepcopy(output)
        )
        context_fusion_stats["fusion_events"] += 1

    elif message_type == "fusion_signal":
        context_fusion_signal_history.append(
            copy.deepcopy(output)
        )
        context_fusion_stats["fusion_signals"] += 1

    elif message_type == "context_snapshot":
        context_fusion_snapshot_history.append(
            copy.deepcopy(output)
        )
        context_fusion_stats["context_snapshots"] += 1

    context_fusion_stats["outputs_sent"] += 1
    return True


# ============================================================
# 11. JANELAS DE TOPICO
# ============================================================

def _cf_prune_topic_windows(now=None):
    if now is None:
        now = time.time()

    limit = now - CONTEXT_FUSION_STATE_WINDOW_SECONDS

    for key in list(context_fusion_topic_windows.keys()):
        queue = context_fusion_topic_windows[key]

        while queue and _cf_number(
            queue[0].get("timestamp"),
            0,
        ) < limit:
            queue.popleft()

        if not queue:
            context_fusion_topic_windows.pop(
                key,
                None,
            )

    for key in list(
        context_fusion_latest_topic_signal.keys()
    ):
        item = context_fusion_latest_topic_signal[key]
        if _cf_number(
            item.get("timestamp"),
            0,
        ) < limit:
            context_fusion_latest_topic_signal.pop(
                key,
                None,
            )


def _cf_register_topic_occurrence(
    topic,
    root_id,
    comment,
    timestamp,
):
    key = _cf_topic_key(topic)

    record = {
        "timestamp": timestamp,
        "root_id": root_id,
        "user": (comment or {}).get("user"),
        "text": (comment or {}).get("text"),
        "topic": copy.deepcopy(topic),
    }

    queue = context_fusion_topic_windows[key]

    # O mesmo comentario pode ter passado por mais de um Coach,
    # mas o mesmo topico nao deve contar duas vezes.
    if any(
        item.get("root_id") == root_id
        for item in queue
    ):
        return

    queue.append(record)
    _cf_prune_topic_windows(timestamp)


def _cf_update_topic_signal_state(
    source,
    output,
    topic,
    now=None,
):
    if now is None:
        now = time.time()

    evidence = output.get("evidence") or {}
    key = _cf_topic_key(topic)

    context_fusion_latest_topic_signal[key] = {
        "timestamp": now,
        "source": source,
        "topic": copy.deepcopy(topic),
        "count": max(
            1,
            _cf_int(evidence.get("count"), 1),
        ),
        "unique_users": max(
            0,
            _cf_int(evidence.get("unique_users"), 0),
        ),
        "max_repetitions_by_one_user": max(
            0,
            _cf_int(
                evidence.get("max_repetitions_by_one_user"),
                0,
            ),
        ),
        "multiple_users": bool(
            evidence.get("multiple_users")
        ),
        "repeated_by_same_user": bool(
            evidence.get("repeated_by_same_user")
        ),
        "examples": copy.deepcopy(
            evidence.get("examples") or []
        ),
        "source_output_ids": copy.deepcopy(
            evidence.get("source_output_ids") or []
        ),
        "output_id": output.get("output_id"),
    }


# ============================================================
# 12. RESUMO ATUAL DE UM TOPICO
# ============================================================

def _cf_topic_summary(topic_key, now=None):
    if now is None:
        now = time.time()

    _cf_prune_topic_windows(now)

    records = list(
        context_fusion_topic_windows.get(
            topic_key,
            [],
        )
    )

    signal = context_fusion_latest_topic_signal.get(
        topic_key
    ) or {}

    topic = None
    if records:
        topic = copy.deepcopy(
            records[-1].get("topic")
        )
    elif signal:
        topic = copy.deepcopy(signal.get("topic"))

    if not topic:
        return None

    root_ids = {
        item.get("root_id")
        for item in records
        if item.get("root_id")
    }

    users = {
        _cf_safe_text(item.get("user"))
        for item in records
        if _cf_safe_text(item.get("user"))
    }

    texts = []
    seen_texts = set()
    for item in records:
        text = _cf_safe_text(item.get("text"))
        norm = _cf_normalize_text(text)
        if not text or norm in seen_texts:
            continue
        seen_texts.add(norm)
        texts.append(text)

    for text in signal.get("examples") or []:
        text = _cf_safe_text(text)
        norm = _cf_normalize_text(text)
        if not text or norm in seen_texts:
            continue
        seen_texts.add(norm)
        texts.append(text)

    count = max(
        len(root_ids),
        _cf_int(signal.get("count"), 0),
        1,
    )

    unique_users = max(
        len(users),
        _cf_int(signal.get("unique_users"), 0),
    )

    result = copy.deepcopy(topic)
    result["count"] = count
    result["unique_users"] = unique_users
    result["examples"] = texts[-5:]

    return result


# ============================================================
# 13. EQUIVALENCIA SEMANTICA DE PERGUNTAS
#
# O modo futuro "Todos" NAO significa despejar cada linha do chat.
# Aqui o Fusion identifica quando duas frases diferentes representam
# essencialmente a mesma pergunta e as agrupa.
#
# Ao mesmo tempo, um subtipo amplo NAO e suficiente para fundir tudo.
# Exemplo: "tem GPS?" e "faz ligacao?" sao ambos connectivity,
# mas sao perguntas diferentes e devem continuar separadas.
# ============================================================

_CF_COLORS = {
    "preto", "preta", "branco", "branca", "rosa", "pink",
    "lilas", "roxo", "roxa", "azul", "verde", "vermelho",
    "vermelha", "amarelo", "amarela", "bege", "marrom",
    "cinza", "dourado", "dourada", "prata", "laranja",
}

_CF_NARROW_SEMANTIC_SUBTYPES = {
    # Subtipos em que, na ausencia de uma faceta mais especifica,
    # a propria categoria ja representa praticamente a mesma duvida.
    "price",
    "promotion_problem",
    "delivery_time",
    "purchase_path",
    "installments",
    "invoice_fiscal_document",
    "warranty",
    "authenticity",
    "product_condition",
    "certification_compliance",
    "brand_manufacturer",
    "expiration_validity",
    "show_demonstrate",
    "refund",
    "cancellation",
    "return",
    "exchange",
    "not_received",
    "delivered_not_received",
    "shipping_delay",
    "order_status",
    "wrong_item",
    "missing_item",
    "damaged_item",
    "defective_item",
    "not_as_described",
    "payment_charge_problem",
    "invoice_missing",
    "warranty_claim",
}


def _cf_first_match(normalized, patterns):
    for label, pattern in patterns:
        if re.search(pattern, normalized):
            return label
    return None


def _cf_variant_facet(normalized):
    words = set(normalized.split())

    colors = sorted(words & _CF_COLORS)
    if colors:
        return "color:" + ",".join(colors)

    size = re.search(
        r"\b(?:pp|p|m|g|gg|xg|xgg|\d{2})\b",
        normalized,
    )
    if size:
        return "size:" + size.group(0)

    return None


def _cf_semantic_facet(text, topic):
    normalized = _cf_normalize_text(text)
    subtype = _cf_safe_text((topic or {}).get("subtype"))

    if not subtype:
        return None

    # Comercial
    if subtype == "price":
        return "price"

    if subtype == "promotion_coupon":
        return "promotion_coupon"

    if subtype == "promotion_problem":
        return "promotion_problem"

    if subtype == "shipping":
        facet = _cf_first_match(normalized, [
            ("free", r"\bgratis\b|\bgratuito\b|\bfrete free\b"),
            ("cost", r"\bquanto\b.*\bfrete\b|\bvalor\b.*\bfrete\b|\bfrete\b.*\bquanto\b|\bfrete caro\b"),
            ("coverage", r"\bcep\b|\bentrega (?:em|para|no|na)\b|\benvia (?:para|pro|pra)\b"),
        ])
        return "shipping:" + (facet or "generic")

    if subtype == "delivery_time":
        return "delivery_time"

    if subtype in {"availability", "variant_selection"}:
        variant = _cf_variant_facet(normalized)
        return subtype + ":" + (variant or "generic")

    if subtype == "purchase_path":
        return "purchase_path"

    if subtype == "payment_method":
        method = _cf_first_match(normalized, [
            ("pix", r"\bpix\b"),
            ("credit_card", r"\bcartao\b|\bcredito\b"),
            ("debit", r"\bdebito\b"),
            ("boleto", r"\bboleto\b"),
        ])
        return "payment_method:" + (method or "generic")

    if subtype == "installments":
        return "installments"

    if subtype == "invoice_fiscal_document":
        return "invoice_fiscal_document"

    # Produto
    if subtype == "connectivity":
        facet = _cf_first_match(normalized, [
            ("gps", r"\bgps\b"),
            ("bluetooth", r"\bbluetooth\b|\bparelh\w*\b"),
            ("nfc", r"\bnfc\b"),
            ("wifi", r"\bwi ?fi\b|\bwifi\b"),
            ("5g", r"\b5g\b"),
            ("4g", r"\b4g\b"),
            ("calls", r"\bligacao\b|\bligacoes\b|\bliga\b|\btelefon\w*\b"),
            ("app", r"\baplicativo\b|\bapp\b"),
        ])
        return "connectivity:" + (facet or "generic")

    if subtype == "battery_duration":
        facet = _cf_first_match(normalized, [
            ("duration", r"\bdura\b|\bduracao\b|\bautonomia\b|\bquantas horas\b"),
            ("charging_time", r"\btempo de carga\b|\bquanto tempo.*carreg\b"),
            ("usb_c", r"\busb ?c\b"),
            ("charging", r"\bcarreg\w*\b|\brecarreg\w*\b"),
            ("battery_type", r"\bpilha\b|\bbateria removivel\b"),
        ])
        return "battery_duration:" + (facet or "generic")

    if subtype == "technical_specification":
        facet = _cf_first_match(normalized, [
            ("ram", r"\bram\b"),
            ("storage", r"\bgb\b|\bmemoria interna\b|\barmazenamento\b"),
            ("processor", r"\bprocessador\b|\bchip\b"),
            ("camera", r"\bcamera\b|\bmegapixel\w*\b|\bmp\b"),
            ("screen", r"\btela\b|\bresolucao\b|\bhz\b"),
        ])
        return "technical_specification:" + (facet or "generic")

    if subtype == "dimensions":
        facet = _cf_first_match(normalized, [
            ("length", r"\bcomprimento\b|\bcomprido\b"),
            ("width", r"\blargura\b|\blargo\b"),
            ("height", r"\baltura\b|\balto\b"),
            ("thickness", r"\bespessura\b|\bgrosso\b|\bfino\b"),
        ])
        return "dimensions:" + (facet or "generic")

    if subtype in {"color_variant", "size_fit"}:
        variant = _cf_variant_facet(normalized)
        return subtype + ":" + (variant or "generic")

    if subtype == "included_items":
        facet = _cf_first_match(normalized, [
            ("charger", r"\bcarregador\b"),
            ("cable", r"\bcabo\b"),
            ("case", r"\bcapa\b|\bcase\b"),
            ("box", r"\bcaixa\b|\bvem com\b|\bacompanha\b"),
        ])
        return "included_items:" + (facet or "generic")

    if subtype == "apparel_characteristic":
        facet = _cf_first_match(normalized, [
            ("pocket", r"\bbolso\b"),
            ("zipper", r"\bziper\b"),
            ("lining", r"\bforro\b"),
            ("stretch", r"\bestica\b|\belast\w*\b"),
            ("transparent", r"\btransparen\w*\b"),
            ("wash", r"\blavar\b|\bmaquina\b"),
            ("shrink", r"\bencolhe\b"),
            ("fade", r"\bdesbota\b"),
        ])
        return "apparel_characteristic:" + (facet or "generic")

    if subtype == "food_characteristic":
        facet = _cf_first_match(normalized, [
            ("calories", r"\bcaloria\w*\b"),
            ("servings", r"\bporcao\w*\b"),
            ("preparation", r"\bprepara\w*\b"),
            ("storage", r"\bgelar\b|\bconservar\b"),
            ("allergens", r"\balergen\w*\b"),
        ])
        return "food_characteristic:" + (facet or "generic")

    if subtype == "brand_manufacturer":
        return "brand_manufacturer"

    if subtype in _CF_NARROW_SEMANTIC_SUBTYPES:
        return subtype

    return None


def _cf_semantically_equivalent(text, topics, cluster):
    cluster_topics = {
        key: holder.get("topic") or {}
        for key, holder in (cluster.get("topics") or {}).items()
    }

    incoming_topics = {
        _cf_topic_key(topic): topic
        for topic in topics or []
    }

    overlap = set(incoming_topics) & set(cluster_topics)
    if not overlap:
        return False

    cluster_text = cluster.get("representative_text") or cluster.get(
        "normalized_text",
        "",
    )

    # Se alguma faceta compartilhada for claramente igual, tratamos
    # as frases como a mesma pergunta mesmo que as palavras mudem.
    for key in overlap:
        incoming_topic = incoming_topics[key]
        cluster_topic = cluster_topics[key]

        incoming_facet = _cf_semantic_facet(
            text,
            incoming_topic,
        )
        cluster_facet = _cf_semantic_facet(
            cluster_text,
            cluster_topic,
        )

        if incoming_facet and cluster_facet:
            if incoming_facet == cluster_facet:
                return True
            # Facetas diferentes dentro do mesmo subtipo amplo
            # (GPS x ligacao, RAM x armazenamento etc.) nao se fundem.
            continue

        subtype = incoming_topic.get("subtype")
        if subtype in _CF_NARROW_SEMANTIC_SUBTYPES:
            return True

    return False


# ============================================================
# 14. LOCALIZAR/CRIAR CLUSTER DE PERGUNTA
# ============================================================

def _cf_cluster_topic_keys(cluster):
    return set(
        cluster.get("topics", {}).keys()
    )


def _cf_topics_overlap(cluster, topics):
    incoming = {
        _cf_topic_key(topic)
        for topic in topics
    }

    if not incoming:
        return False

    return bool(
        incoming & _cf_cluster_topic_keys(cluster)
    )


def _cf_find_similar_cluster(
    text,
    topics,
    ingest_now,
):
    normalized = _cf_normalize_text(text)
    if not normalized:
        return None

    best = None
    best_score = 0.0

    for cluster in context_fusion_pending_clusters.values():
        if (
            ingest_now
            - cluster.get("ingest_last", ingest_now)
            > CONTEXT_FUSION_MAX_HOLD_SECONDS
        ):
            continue

        if not _cf_topics_overlap(cluster, topics):
            continue

        semantic_match = _cf_semantically_equivalent(
            text,
            topics,
            cluster,
        )

        seq, jac = _cf_text_similarity(
            normalized,
            cluster.get("normalized_text"),
        )

        textual_match = (
            seq >= CONTEXT_FUSION_TEXT_SIMILARITY
            or jac >= CONTEXT_FUSION_TOKEN_SIMILARITY
        )

        if not semantic_match and not textual_match:
            continue

        score = max(
            1.05 if semantic_match else 0.0,
            seq,
            jac,
        )
        if score > best_score:
            best = cluster
            best_score = score

    return best


def _cf_new_cluster(
    root_id,
    comment,
    ingest_now,
):
    cluster_id = uuid.uuid4().hex
    text = _cf_safe_text(
        (comment or {}).get("text")
    )

    cluster = {
        "cluster_id": cluster_id,
        "created_at": ingest_now,
        "ingest_last": ingest_now,
        "normalized_text": _cf_normalize_text(text),
        "representative_text": text,
        "roots": set(),
        "comments": {},
        "topics": {},
        "domains": set(),
        "evidence": [],
        "requires_response": False,
        "min_source_timestamp": None,
        "max_source_timestamp": None,
        "attached_topic_signal_ids": set(),
    }

    context_fusion_pending_clusters[
        cluster_id
    ] = cluster

    if root_id:
        context_fusion_root_to_cluster[
            root_id
        ] = cluster_id

    return cluster


def _cf_get_or_create_cluster(
    root_id,
    comment,
    topics,
    ingest_now,
):
    if root_id in context_fusion_root_to_cluster:
        cluster_id = context_fusion_root_to_cluster[
            root_id
        ]
        cluster = context_fusion_pending_clusters.get(
            cluster_id
        )
        if cluster is not None:
            return cluster

    text = _cf_safe_text(
        (comment or {}).get("text")
    )

    similar = _cf_find_similar_cluster(
        text,
        topics,
        ingest_now,
    )

    if similar is not None:
        if root_id:
            context_fusion_root_to_cluster[
                root_id
            ] = similar.get("cluster_id")
        return similar

    return _cf_new_cluster(
        root_id,
        comment,
        ingest_now,
    )


# ============================================================
# 14. ADICIONAR TOPICO AO CLUSTER
# ============================================================

def _cf_cluster_add_topic(
    cluster,
    topic,
    root_id=None,
    user=None,
    count_override=None,
    unique_override=None,
):
    key = _cf_topic_key(topic)

    holder = cluster["topics"].get(key)

    if holder is None:
        holder = {
            "topic": copy.deepcopy(topic),
            "roots": set(),
            "users": set(),
            "count_override": 0,
            "unique_override": 0,
        }
        cluster["topics"][key] = holder

    if root_id:
        holder["roots"].add(root_id)

    user = _cf_safe_text(user)
    if user:
        holder["users"].add(user)

    if count_override is not None:
        holder["count_override"] = max(
            holder.get("count_override", 0),
            _cf_int(count_override, 0),
        )

    if unique_override is not None:
        holder["unique_override"] = max(
            holder.get("unique_override", 0),
            _cf_int(unique_override, 0),
        )

    domain = topic.get("domain")
    if domain:
        cluster["domains"].add(domain)

    if topic.get("requires_response"):
        cluster["requires_response"] = True


# ============================================================
# 15. INCORPORAR ANALISE INDIVIDUAL
# ============================================================

def _cf_ingest_individual(
    source,
    output,
    ingest_now,
):
    comment = _cf_extract_comment(output)
    topics = _cf_extract_topics(
        output,
        source=source,
    )

    if not topics:
        context_fusion_stats[
            "individual_without_topics"
        ] += 1
        return None

    _cf_prune_comment_fingerprints(ingest_now)
    root_id = _cf_canonical_root_id(
        output,
        comment,
        ingest_now,
    )

    cluster = _cf_get_or_create_cluster(
        root_id,
        comment,
        topics,
        ingest_now,
    )

    cluster["ingest_last"] = ingest_now
    cluster["roots"].add(root_id)

    if comment:
        cluster["comments"][root_id] = copy.deepcopy(
            comment
        )

    source_timestamp = _cf_number(
        output.get("timestamp"),
        ingest_now,
    )

    minimum = cluster.get("min_source_timestamp")
    maximum = cluster.get("max_source_timestamp")

    cluster["min_source_timestamp"] = (
        source_timestamp
        if minimum is None
        else min(minimum, source_timestamp)
    )
    cluster["max_source_timestamp"] = (
        source_timestamp
        if maximum is None
        else max(maximum, source_timestamp)
    )

    evidence_id = output.get("output_id")
    existing_evidence_ids = {
        item.get("output_id")
        for item in cluster.get("evidence", [])
        if isinstance(item, dict)
    }

    if (
        evidence_id not in existing_evidence_ids
        and len(cluster["evidence"]) < CONTEXT_FUSION_MAX_EVIDENCE_PER_OUTPUT
    ):
        cluster["evidence"].append(
            _cf_compact_evidence(
                source,
                output,
            )
        )

    response_needed = bool(
        output.get("response_needed")
        or output.get("needs_resolution")
    )

    cluster["requires_response"] = bool(
        cluster.get("requires_response")
        or response_needed
    )

    user = (comment or {}).get("user")

    for topic in topics:
        if response_needed:
            topic["requires_response"] = True

        _cf_cluster_add_topic(
            cluster,
            topic,
            root_id=root_id,
            user=user,
        )

        _cf_register_topic_occurrence(
            topic,
            root_id,
            comment,
            source_timestamp,
        )

    context_fusion_stats["individual_received"] += 1
    return cluster


# ============================================================
# 16. ANEXAR SINAL DE TOPICO A CLUSTER PENDENTE
# ============================================================

def _cf_attach_topic_signal_to_pending(
    source,
    output,
    topic,
    ingest_now,
):
    key = _cf_topic_key(topic)
    evidence = output.get("evidence") or {}
    source_output_ids = set(
        evidence.get("source_output_ids") or []
    )

    candidates = []

    for cluster in context_fusion_pending_clusters.values():
        if key not in cluster.get("topics", {}):
            continue

        age = ingest_now - cluster.get(
            "ingest_last",
            ingest_now,
        )

        if age > CONTEXT_FUSION_MAX_HOLD_SECONDS:
            continue

        overlap = bool(
            source_output_ids
            & set(cluster.get("roots") or [])
        )

        candidates.append((
            1 if overlap else 0,
            cluster.get("ingest_last", 0),
            cluster,
        ))

    if not candidates:
        return False

    candidates.sort(
        key=lambda item: (
            item[0],
            item[1],
        ),
        reverse=True,
    )

    cluster = candidates[0][2]

    _cf_cluster_add_topic(
        cluster,
        topic,
        count_override=evidence.get("count"),
        unique_override=evidence.get("unique_users"),
    )

    signal_id = output.get("output_id")
    if signal_id:
        cluster["attached_topic_signal_ids"].add(
            signal_id
        )

    if len(cluster["evidence"]) < CONTEXT_FUSION_MAX_EVIDENCE_PER_OUTPUT:
        cluster["evidence"].append(
            _cf_compact_evidence(
                source,
                output,
            )
        )

    cluster["ingest_last"] = max(
        cluster.get("ingest_last", ingest_now),
        ingest_now,
    )

    context_fusion_stats[
        "topic_signals_attached_to_questions"
    ] += 1

    return True


# ============================================================
# 17. INCORPORAR SINAL LOCAL/TREND DE COACH
# ============================================================

def _cf_ingest_topic_signal(
    source,
    output,
    ingest_now,
):
    topics = _cf_extract_topics(
        output,
        source=source,
    )

    if not topics:
        context_fusion_stats[
            "topic_signal_without_topic"
        ] += 1
        return None

    topic = topics[0]

    _cf_update_topic_signal_state(
        source,
        output,
        topic,
        now=ingest_now,
    )

    if _cf_attach_topic_signal_to_pending(
        source,
        output,
        topic,
        ingest_now,
    ):
        return "attached"

    signal_id = output.get("output_id") or uuid.uuid4().hex

    context_fusion_pending_topic_signals[
        signal_id
    ] = {
        "source": source,
        "output": copy.deepcopy(output),
        "topic": copy.deepcopy(topic),
        "ingest_time": ingest_now,
    }

    context_fusion_stats["topic_signals_received"] += 1
    return "pending"


# ============================================================
# 18. AUDIENCIA
# ============================================================

def _cf_audience_strength(output):
    data = output.get("data") or {}
    strength = _cf_number(
        data.get("strength"),
        None,
    )

    if strength is not None:
        return _cf_clamp(strength)

    signal_type = output.get("signal_type")

    if signal_type in {
        "viewer_growth",
        "viewer_drop",
        "like_acceleration",
        "like_slowdown",
    }:
        return 0.60

    if signal_type in {
        "gift_received",
        "gift_activity",
        "follow_activity",
        "share_activity",
    }:
        return 0.45

    return 0.30


def _cf_emit_audience_event(
    source,
    output,
    ingest_now,
):
    signal_type = output.get("signal_type")
    if not signal_type:
        return None

    # V1.1: sinais de estado/atividade leve continuam disponiveis no
    # contexto, mas nao geram um candidato individual para o Decision.
    # Isso evita mensagens do tipo "houve um compartilhamento" a cada
    # pequena variacao do contador.
    if signal_type in CONTEXT_FUSION_AUDIENCE_STATE_ONLY_TYPES:
        context_fusion_stats[
            "audience_state_only_suppressed"
        ] += 1
        return None

    strength = _cf_audience_strength(output)

    # Presentes individuais podem ser eventos distintos e nao entram no
    # cooldown generico. Para os demais sinais, evitamos repetir o mesmo
    # tipo em intervalos muito curtos sem mudanca relevante de forca.
    if signal_type != "gift_received":
        previous = context_fusion_last_audience_event_emitted.get(
            signal_type
        )
        if previous:
            age = ingest_now - _cf_number(
                previous.get("timestamp"),
                0,
            )
            previous_strength = _cf_number(
                previous.get("strength"),
                0,
            ) or 0
            if (
                age < CONTEXT_FUSION_AUDIENCE_DIRECT_COOLDOWN_SECONDS
                and strength <= previous_strength + 0.20
            ):
                context_fusion_stats[
                    "audience_repeats_suppressed"
                ] += 1
                return None

    evidence = _cf_compact_evidence(
        source,
        output,
    )

    result = {
        "output_id": uuid.uuid4().hex,
        "message_type": "fusion_event",
        "fusion_version": CONTEXT_FUSION_VERSION,
        "context_kind": "audience_signal",
        "platform": output.get("platform") or context_fusion_platform,
        "live_id": output.get("live_id") or context_fusion_live_id,
        "subject": output.get("subject") or context_fusion_subject,
        "timestamp": ingest_now,
        "iso_time": _cf_now_iso(),
        "domains": ["audience"],
        "primary": None,
        "topics": [],
        "audience": {
            "signal_type": signal_type,
            "signals": [
                {
                    "signal_type": signal_type,
                    "strength": strength,
                    "data": copy.deepcopy(
                        output.get("data") or {}
                    ),
                }
            ],
            "active_signals": [signal_type],
        },
        "evidence": [evidence],
        "strength": strength,
        "requires_response": False,
        "correlation_only": False,
        "decision_candidate": True,
    }

    if _cf_queue_output(result):
        context_fusion_last_audience_event_emitted[
            signal_type
        ] = {
            "timestamp": ingest_now,
            "strength": strength,
            "output_id": result.get("output_id"),
        }
        return result

    return None


def _cf_ingest_audience(
    source,
    output,
    ingest_now,
):
    record = {
        "timestamp": ingest_now,
        "signal_type": output.get("signal_type"),
        "strength": _cf_audience_strength(output),
        "data": copy.deepcopy(output.get("data") or {}),
        "output_id": output.get("output_id"),
        "raw": _cf_compact_evidence(source, output),
    }

    context_fusion_audience_window.append(record)
    _cf_prune_audience(ingest_now)
    context_fusion_stats["audience_received"] += 1

    return _cf_emit_audience_event(
        source,
        output,
        ingest_now,
    )


def _cf_prune_audience(now=None):
    if now is None:
        now = time.time()

    limit = now - CONTEXT_FUSION_STATE_WINDOW_SECONDS

    while (
        context_fusion_audience_window
        and _cf_number(
            context_fusion_audience_window[0].get("timestamp"),
            0,
        ) < limit
    ):
        context_fusion_audience_window.popleft()


def _cf_active_audience(now=None, seconds=None):
    if now is None:
        now = time.time()
    if seconds is None:
        seconds = CONTEXT_FUSION_STATE_WINDOW_SECONDS

    _cf_prune_audience(now)
    limit = now - seconds

    recent = [
        item
        for item in context_fusion_audience_window
        if _cf_number(item.get("timestamp"), 0) >= limit
    ]

    # Mantemos apenas o sinal mais recente de cada tipo.
    latest = {}
    for item in recent:
        signal_type = item.get("signal_type")
        if signal_type:
            latest[signal_type] = item

    return list(latest.values())


# ============================================================
# 19. INGESTAO PUBLICA
# ============================================================

def context_fusion_ingest(
    source,
    output,
    now=None,
):
    global context_fusion_platform
    global context_fusion_live_id
    global context_fusion_subject
    global context_fusion_last_error

    if now is None:
        now = time.time()

    if source not in CONTEXT_FUSION_SOURCES:
        context_fusion_stats["invalid_source"] += 1
        return None

    if not isinstance(output, dict):
        context_fusion_stats["invalid_input"] += 1
        return None

    if _cf_seen_input(output):
        return None

    context_fusion_stats["inputs_received"] += 1
    context_fusion_source_counts[source] += 1

    message_type = output.get("message_type")
    context_fusion_message_type_counts[
        message_type or "unknown"
    ] += 1

    if output.get("platform"):
        context_fusion_platform = output.get("platform")
    if output.get("live_id"):
        context_fusion_live_id = output.get("live_id")
    if output.get("subject"):
        context_fusion_subject = output.get("subject")

    context_fusion_input_history.append({
        "source": source,
        "output": copy.deepcopy(output),
        "ingest_time": now,
    })

    try:
        if message_type in _CONTEXT_FUSION_INDIVIDUAL_TYPES:
            result = _cf_ingest_individual(
                source,
                output,
                now,
            )

        elif message_type in _CONTEXT_FUSION_TOPIC_SIGNAL_TYPES:
            result = _cf_ingest_topic_signal(
                source,
                output,
                now,
            )

        elif message_type == "audience_signal":
            result = _cf_ingest_audience(
                source,
                output,
                now,
            )

        else:
            context_fusion_stats[
                "unsupported_message_type"
            ] += 1
            result = None

        # Toda ingestao pode alterar o quadro atual.
        _cf_maybe_emit_correlations(now)
        return result

    except Exception as e:
        context_fusion_last_error = (
            f"{type(e).__name__}: {e}"
        )
        context_fusion_stats["processing_errors"] += 1
        return None


# ============================================================
# 20. FINALIZAR TOPICOS DO CLUSTER
# ============================================================

def _cf_cluster_topics(cluster):
    result = []

    for holder in cluster.get("topics", {}).values():
        topic = copy.deepcopy(holder.get("topic") or {})

        count = max(
            len(holder.get("roots") or []),
            _cf_int(holder.get("count_override"), 0),
            1,
        )

        unique_users = max(
            len(holder.get("users") or []),
            _cf_int(holder.get("unique_override"), 0),
        )

        topic["count"] = count
        topic["unique_users"] = unique_users

        # Forca descritiva, NAO prioridade.
        topic["strength"] = _cf_clamp(
            0.35
            + 0.12 * max(0, count - 1)
            + 0.08 * max(0, unique_users - 1)
        )

        result.append(topic)

    result.sort(
        key=lambda item: (
            bool(item.get("requires_response")),
            item.get("count", 1),
            item.get("unique_users", 0),
        ),
        reverse=True,
    )

    return result


def _cf_cluster_comments(cluster):
    comments = []
    seen = set()

    for root_id, comment in cluster.get(
        "comments",
        {},
    ).items():
        text = _cf_safe_text(comment.get("text"))
        user = _cf_safe_text(comment.get("user"))

        key = (
            root_id,
            _cf_normalize_text(text),
        )

        if key in seen:
            continue
        seen.add(key)

        comments.append({
            "root_id": root_id,
            "event_id": comment.get("event_id"),
            "user": user or None,
            "text": text or None,
        })

    return comments[
        -CONTEXT_FUSION_MAX_COMMENTS_PER_OUTPUT:
    ]


def _cf_group_strength(
    question_count,
    unique_users,
    domains,
    topics,
):
    strength = 0.34
    strength += 0.12 * max(0, question_count - 1)
    strength += 0.08 * max(0, unique_users - 1)
    strength += 0.06 * max(0, len(domains) - 1)

    if any(
        topic.get("requires_response")
        for topic in topics
    ):
        strength += 0.08

    return _cf_clamp(strength)


def _cf_choose_primary(topics):
    if not topics:
        return None

    # Isto NAO e prioridade operacional.
    # So escolhe o item mais representativo estruturalmente:
    # resposta necessaria -> maior repeticao -> mais usuarios.
    ordered = sorted(
        topics,
        key=lambda item: (
            bool(item.get("requires_response")),
            _cf_int(item.get("count"), 1),
            _cf_int(item.get("unique_users"), 0),
        ),
        reverse=True,
    )

    return copy.deepcopy(ordered[0])


# ============================================================
# 21. MEMORIA SEMANTICA ENTRE JANELAS
#
# A coalescencia curta junta eventos que chegam quase juntos. A memoria
# abaixo resolve outro caso: a MESMA situacao reaparecendo alguns
# segundos depois. Ela continua sendo contabilizada, mas nao vira um
# novo aviso a cada ocorrencia. Um novo output so sai quando ha
# crescimento significativo ou quando a situacao persiste por tempo
# suficiente com nova evidencia.
# ============================================================

def _cf_event_semantic_signature(topics, representative_text):
    parts = []

    for topic in topics or []:
        key = _cf_topic_key(topic)
        facet = _cf_semantic_facet(
            representative_text,
            topic,
        )

        # Para subtipos largos sem faceta conhecida, o texto normalizado
        # ajuda a nao fundir assuntos diferentes por engano.
        if facet is None and topic.get("subtype") not in _CF_NARROW_SEMANTIC_SUBTYPES:
            facet = _cf_normalize_text(representative_text)

        parts.append((key, facet or "generic"))

    return _cf_stable_hash({
        "parts": sorted(parts),
    })


def _cf_prune_semantic_memory(now=None):
    if now is None:
        now = time.time()

    limit = now - CONTEXT_FUSION_REPEAT_MEMORY_SECONDS

    for key in list(context_fusion_recent_semantic_events.keys()):
        item = context_fusion_recent_semantic_events.get(key) or {}
        if _cf_number(item.get("last_seen"), 0) < limit:
            context_fusion_recent_semantic_events.pop(key, None)


def _cf_merge_semantic_event_memory(output, cluster, now=None):
    if now is None:
        now = time.time()

    _cf_prune_semantic_memory(now)

    topics = output.get("topics") or []
    representative_text = cluster.get("representative_text") or ""
    signature = _cf_event_semantic_signature(
        topics,
        representative_text,
    )

    roots = set(
        (output.get("question_group") or {}).get(
            "source_comment_ids"
        ) or []
    )
    comments = list(
        (output.get("question_group") or {}).get("examples")
        or []
    )
    users = {
        _cf_safe_text(item.get("user"))
        for item in comments
        if isinstance(item, dict) and _cf_safe_text(item.get("user"))
    }

    memory = context_fusion_recent_semantic_events.get(signature)

    if memory is None:
        memory = {
            "first_seen": now,
            "last_seen": now,
            "last_emitted": now,
            "roots": set(roots),
            "users": set(users),
            "examples": copy.deepcopy(comments),
            "emitted_count": max(1, len(roots)),
            "emitted_unique_users": len(users),
            "domains": set(output.get("domains") or []),
            "last_output_id": output.get("output_id"),
        }
        context_fusion_recent_semantic_events[signature] = memory
        output["semantic_signature"] = signature
        output["repeat_aggregation"] = {
            "cumulative_count": max(1, len(roots)),
            "cumulative_unique_users": len(users),
            "new_since_last_display_candidate": max(1, len(roots)),
            "suppressed_between_emissions": 0,
        }
        return output

    previous_roots = set(memory.get("roots") or set())
    previous_users = set(memory.get("users") or set())

    memory["roots"] = previous_roots | roots
    memory["users"] = previous_users | users
    memory["last_seen"] = now
    memory["domains"] = set(memory.get("domains") or set()) | set(
        output.get("domains") or []
    )

    # Exemplos unicos, preservando ordem e sem inchar indefinidamente.
    example_keys = {
        (
            _cf_normalize_text(item.get("user")),
            _cf_normalize_text(item.get("text")),
        )
        for item in memory.get("examples") or []
        if isinstance(item, dict)
    }
    for item in comments:
        if not isinstance(item, dict):
            continue
        key = (
            _cf_normalize_text(item.get("user")),
            _cf_normalize_text(item.get("text")),
        )
        if key in example_keys:
            continue
        example_keys.add(key)
        memory.setdefault("examples", []).append(
            copy.deepcopy(item)
        )

    memory["examples"] = memory.get("examples", [])[
        -CONTEXT_FUSION_MAX_COMMENTS_PER_OUTPUT:
    ]

    total_count = max(1, len(memory.get("roots") or []))
    total_users = len(memory.get("users") or [])
    emitted_count = _cf_int(memory.get("emitted_count"), 0)
    emitted_users = _cf_int(memory.get("emitted_unique_users"), 0)

    new_occurrences = max(0, total_count - emitted_count)
    new_users = max(0, total_users - emitted_users)
    since_last_emit = now - _cf_number(
        memory.get("last_emitted"),
        0,
    )

    meaningful_growth = (
        new_occurrences >= CONTEXT_FUSION_REPEAT_REEMIT_MIN_NEW_OCCURRENCES
        or new_users >= CONTEXT_FUSION_REPEAT_REEMIT_MIN_NEW_USERS
    )

    persistent_refresh = (
        since_last_emit >= CONTEXT_FUSION_REPEAT_REFRESH_SECONDS
        and new_occurrences >= 1
    )

    if not meaningful_growth and not persistent_refresh:
        context_fusion_stats[
            "cross_window_repeats_suppressed"
        ] += max(1, len(roots))
        return None

    # Reemissao por escalada: agora o output representa o acumulado da
    # situacao, nao apenas a ultima pequena janela.
    qg = output.get("question_group") or {}
    qg["count"] = total_count
    qg["unique_users"] = total_users
    qg["same_question"] = total_count >= 2
    qg["examples"] = copy.deepcopy(memory.get("examples") or [])
    qg["source_comment_ids"] = list(memory.get("roots") or [])
    output["question_group"] = qg

    if total_count >= 2:
        output["context_kind"] = (
            "repeated_multi_domain_question"
            if len(output.get("domains") or []) >= 2
            else "repeated_question"
        )

    for topic in output.get("topics") or []:
        topic["count"] = max(
            _cf_int(topic.get("count"), 1),
            total_count,
        )
        topic["unique_users"] = max(
            _cf_int(topic.get("unique_users"), 0),
            total_users,
        )

    output["primary"] = _cf_choose_primary(
        output.get("topics") or []
    )
    output["strength"] = _cf_group_strength(
        total_count,
        total_users,
        output.get("domains") or [],
        output.get("topics") or [],
    )
    output["semantic_signature"] = signature
    output["repeat_aggregation"] = {
        "cumulative_count": total_count,
        "cumulative_unique_users": total_users,
        "new_since_last_display_candidate": new_occurrences,
        "suppressed_between_emissions": max(
            0,
            new_occurrences - 1,
        ),
    }

    memory["last_emitted"] = now
    memory["emitted_count"] = total_count
    memory["emitted_unique_users"] = total_users
    memory["last_output_id"] = output.get("output_id")

    context_fusion_stats[
        "cross_window_groups_reemitted_on_growth"
    ] += 1
    return output


# ============================================================
# 22. EMITIR CLUSTER DE PERGUNTA/COMENTARIO
# ============================================================

def _cf_emit_cluster(cluster, now=None):
    if now is None:
        now = time.time()

    topics = _cf_cluster_topics(cluster)
    comments = _cf_cluster_comments(cluster)

    if not topics:
        return None

    roots = set(cluster.get("roots") or [])
    question_count = max(1, len(roots))

    users = {
        _cf_safe_text(item.get("user"))
        for item in comments
        if _cf_safe_text(item.get("user"))
    }

    unique_users = len(users)
    domains = sorted(
        domain
        for domain in cluster.get("domains", set())
        if domain
    )

    same_question = question_count >= 2

    if same_question and len(domains) >= 2:
        context_kind = "repeated_multi_domain_question"
    elif same_question:
        context_kind = "repeated_question"
    elif len(domains) >= 2:
        context_kind = "multi_domain_comment"
    else:
        context_kind = "comment_context"

    examples = [
        {
            "user": item.get("user"),
            "text": item.get("text"),
        }
        for item in comments
        if item.get("text")
    ]

    output = {
        "output_id": uuid.uuid4().hex,
        "message_type": "fusion_event",
        "fusion_version": CONTEXT_FUSION_VERSION,
        "context_kind": context_kind,
        "platform": context_fusion_platform,
        "live_id": context_fusion_live_id,
        "subject": context_fusion_subject,
        "timestamp": now,
        "iso_time": _cf_now_iso(),
        "domains": domains,
        "primary": _cf_choose_primary(topics),
        "topics": topics,
        "audience": _cf_current_audience_block(
            now=now,
            seconds=CONTEXT_FUSION_CORRELATION_WINDOW_SECONDS,
        ),
        "evidence": copy.deepcopy(
            cluster.get("evidence", [])[
                -CONTEXT_FUSION_MAX_EVIDENCE_PER_OUTPUT:
            ]
        ),
        "comments": examples,
        "question_group": {
            "count": question_count,
            "unique_users": unique_users,
            "same_question": same_question,
            "normalized_question": cluster.get(
                "normalized_text"
            ),
            "examples": examples,
            "source_comment_ids": list(roots),
        },
        "time_window": {
            "first_source_timestamp": cluster.get(
                "min_source_timestamp"
            ),
            "last_source_timestamp": cluster.get(
                "max_source_timestamp"
            ),
            "coalesce_seconds": CONTEXT_FUSION_COALESCE_SECONDS,
        },
        "strength": _cf_group_strength(
            question_count,
            unique_users,
            domains,
            topics,
        ),
        "requires_response": bool(
            cluster.get("requires_response")
            or any(
                topic.get("requires_response")
                for topic in topics
            )
        ),
        "correlation_only": False,
    }

    # V1.1: consolidar repeticoes que atravessam janelas diferentes.
    candidate = _cf_merge_semantic_event_memory(
        output,
        cluster,
        now=now,
    )

    # Mesmo quando uma repeticao e suprimida, atualizamos a memoria do
    # topico para impedir que um topic_signal redundante a recoloque na
    # fila logo em seguida.
    reference = candidate or output
    reference_topics = reference.get("topics") or topics
    qg_reference = reference.get("question_group") or {}

    for topic in reference_topics:
        key = _cf_topic_key(topic)
        context_fusion_recent_question_topic_emissions[
            key
        ] = {
            "timestamp": now,
            "count": max(
                _cf_int(topic.get("count"), 1),
                _cf_int(qg_reference.get("count"), 1),
            ),
            "unique_users": max(
                _cf_int(topic.get("unique_users"), 0),
                _cf_int(qg_reference.get("unique_users"), 0),
            ),
            "output_id": reference.get("output_id"),
        }

    if candidate is None:
        return None

    output = candidate
    topics = output.get("topics") or topics
    question_count = _cf_int(
        (output.get("question_group") or {}).get("count"),
        question_count,
    )
    unique_users = _cf_int(
        (output.get("question_group") or {}).get("unique_users"),
        unique_users,
    )
    same_question = question_count >= 2

    if _cf_queue_output(output):
        if same_question:
            context_fusion_stats[
                "repeated_questions_grouped"
            ] += question_count
            context_fusion_stats[
                "question_groups_emitted"
            ] += 1
        else:
            context_fusion_stats[
                "unique_questions_emitted"
            ] += 1

        if len(domains) >= 2:
            context_fusion_stats[
                "multi_domain_comments_fused"
            ] += 1

    return output


# ============================================================
# 22. REMOVER CLUSTER PENDENTE
# ============================================================

def _cf_remove_cluster(cluster_id):
    cluster = context_fusion_pending_clusters.pop(
        cluster_id,
        None,
    )

    if cluster is None:
        return

    for root_id in list(cluster.get("roots") or []):
        if context_fusion_root_to_cluster.get(
            root_id
        ) == cluster_id:
            context_fusion_root_to_cluster.pop(
                root_id,
                None,
            )


# ============================================================
# 23. SINAL DE TOPICO REDUNDANTE?
# ============================================================

def _cf_topic_signal_is_redundant(
    topic,
    output,
    now=None,
):
    if now is None:
        now = time.time()

    key = _cf_topic_key(topic)
    recent = context_fusion_recent_question_topic_emissions.get(
        key
    )

    if not recent:
        return False

    if (
        now
        - _cf_number(recent.get("timestamp"), 0)
        > CONTEXT_FUSION_REDUNDANT_TOPIC_SECONDS
    ):
        return False

    evidence = output.get("evidence") or {}
    count = _cf_int(evidence.get("count"), 1)
    users = _cf_int(evidence.get("unique_users"), 0)

    recent_count = _cf_int(recent.get("count"), 1)
    recent_users = _cf_int(
        recent.get("unique_users"),
        0,
    )

    # Se nao houve crescimento relevante, o grupo ja representou o sinal.
    return (
        count <= recent_count + 1
        and users <= recent_users + 1
    )


# ============================================================
# 24. EMITIR SINAL DE TOPICO AGREGADO
# ============================================================

def _cf_emit_topic_signal(
    source,
    output,
    topic,
    now=None,
):
    if now is None:
        now = time.time()

    if _cf_topic_signal_is_redundant(
        topic,
        output,
        now,
    ):
        context_fusion_stats[
            "redundant_topic_signals_suppressed"
        ] += 1
        return None

    evidence = output.get("evidence") or {}

    count = max(
        1,
        _cf_int(evidence.get("count"), 1),
    )
    unique_users = max(
        0,
        _cf_int(evidence.get("unique_users"), 0),
    )

    topic = copy.deepcopy(topic)
    topic["count"] = count
    topic["unique_users"] = unique_users
    topic["strength"] = _cf_clamp(
        0.35
        + 0.12 * max(0, count - 1)
        + 0.08 * max(0, unique_users - 1)
    )

    domain = topic.get("domain") or _cf_source_domain(source)

    output_fused = {
        "output_id": uuid.uuid4().hex,
        "message_type": "fusion_signal",
        "fusion_version": CONTEXT_FUSION_VERSION,
        "context_kind": "topic_activity",
        "platform": output.get("platform") or context_fusion_platform,
        "live_id": output.get("live_id") or context_fusion_live_id,
        "subject": output.get("subject") or context_fusion_subject,
        "timestamp": now,
        "iso_time": _cf_now_iso(),
        "domains": [domain] if domain else [],
        "primary": copy.deepcopy(topic),
        "topics": [copy.deepcopy(topic)],
        "audience": _cf_current_audience_block(
            now=now,
            seconds=CONTEXT_FUSION_CORRELATION_WINDOW_SECONDS,
        ),
        "evidence": [
            _cf_compact_evidence(
                source,
                output,
            )
        ],
        "topic_activity": {
            "count": count,
            "unique_users": unique_users,
            "multiple_users": bool(
                evidence.get("multiple_users")
                or unique_users >= 2
            ),
            "repeated_by_same_user": bool(
                evidence.get("repeated_by_same_user")
            ),
            "examples": copy.deepcopy(
                evidence.get("examples") or []
            ),
            "aggregation_scope": "topic",
        },
        "strength": topic.get("strength"),
        "requires_response": bool(
            output.get("response_needed")
            or topic.get("requires_response")
        ),
        "correlation_only": False,
    }

    _cf_queue_output(output_fused)
    return output_fused


# ============================================================
# 25. FLUSH DOS PENDENTES
# ============================================================

def context_fusion_flush_pending(
    now=None,
    force=False,
):
    if now is None:
        now = time.time()

    emitted = []

    # Primeiro perguntas/comentarios.
    for cluster_id in list(
        context_fusion_pending_clusters.keys()
    ):
        cluster = context_fusion_pending_clusters.get(
            cluster_id
        )
        if cluster is None:
            continue

        quiet_for = now - cluster.get(
            "ingest_last",
            now,
        )
        total_age = now - cluster.get(
            "created_at",
            now,
        )

        ready = (
            force
            or quiet_for >= CONTEXT_FUSION_COALESCE_SECONDS
            or total_age >= CONTEXT_FUSION_MAX_HOLD_SECONDS
        )

        if not ready:
            continue

        output = _cf_emit_cluster(
            cluster,
            now=now,
        )

        if output:
            emitted.append(output)

        _cf_remove_cluster(cluster_id)

    # Depois sinais de topico que nao conseguiram ser incorporados.
    for signal_id in list(
        context_fusion_pending_topic_signals.keys()
    ):
        item = context_fusion_pending_topic_signals.get(
            signal_id
        )
        if item is None:
            continue

        age = now - item.get("ingest_time", now)

        if (
            not force
            and age < CONTEXT_FUSION_TOPIC_SIGNAL_HOLD_SECONDS
        ):
            continue

        output = _cf_emit_topic_signal(
            item.get("source"),
            item.get("output") or {},
            item.get("topic") or {},
            now=now,
        )

        if output:
            emitted.append(output)

        context_fusion_pending_topic_signals.pop(
            signal_id,
            None,
        )

    return emitted


# ============================================================
# 26. TOPICOS ATIVOS
# ============================================================

def context_fusion_active_topics(
    now=None,
    seconds=None,
):
    if now is None:
        now = time.time()
    if seconds is None:
        seconds = CONTEXT_FUSION_STATE_WINDOW_SECONDS

    _cf_prune_topic_windows(now)

    limit = now - seconds
    keys = set(context_fusion_topic_windows.keys())

    for key, signal in context_fusion_latest_topic_signal.items():
        if _cf_number(signal.get("timestamp"), 0) >= limit:
            keys.add(key)

    result = []

    for key in keys:
        summary = _cf_topic_summary(
            key,
            now=now,
        )

        if summary is None:
            continue

        # Verificar se ha alguma evidencia realmente recente.
        records = list(
            context_fusion_topic_windows.get(key, [])
        )
        signal = context_fusion_latest_topic_signal.get(key) or {}

        latest_timestamp = max(
            [
                _cf_number(item.get("timestamp"), 0)
                for item in records
            ]
            + [
                _cf_number(signal.get("timestamp"), 0)
            ]
        )

        if latest_timestamp < limit:
            continue

        result.append(summary)

    result.sort(
        key=lambda item: (
            item.get("count", 1),
            item.get("unique_users", 0),
        ),
        reverse=True,
    )

    return result


# ============================================================
# 27. BLOCO ATUAL DE AUDIENCIA
# ============================================================

def _cf_current_audience_block(
    now=None,
    seconds=None,
):
    active = _cf_active_audience(
        now=now,
        seconds=seconds,
    )

    if not active:
        return {
            "signals": [],
            "active_signals": [],
        }

    return {
        "signals": [
            {
                "signal_type": item.get("signal_type"),
                "strength": item.get("strength"),
                "data": copy.deepcopy(item.get("data") or {}),
            }
            for item in active
        ],
        "active_signals": _cf_unique_strings([
            item.get("signal_type")
            for item in active
        ]),
    }


# ============================================================
# 28. CORRELACOES ENTRE DOMINIOS
#
# Correlacao temporal/contextual NAO significa causalidade.
# ============================================================

def _cf_correlation_signature(
    context_kind,
    topics,
    audience_signals,
):
    data = {
        "context_kind": context_kind,
        "topics": sorted(
            _cf_topic_key(topic)
            for topic in topics
        ),
        "audience": sorted(audience_signals),
    }
    return _cf_stable_hash(data)


def _cf_correlation_recently_emitted(
    fingerprint,
    now=None,
):
    if now is None:
        now = time.time()

    previous = context_fusion_last_correlation_emitted.get(
        fingerprint
    )

    if previous is None:
        return False

    return (
        now - previous
        < CONTEXT_FUSION_CORRELATION_COOLDOWN_SECONDS
    )


def _cf_emit_correlation(
    context_kind,
    topics,
    audience_signals,
    now=None,
    note=None,
):
    if now is None:
        now = time.time()

    domains = sorted({
        topic.get("domain")
        for topic in topics
        if topic.get("domain")
    })

    if audience_signals:
        domains = sorted(set(domains) | {"audience"})

    fingerprint = _cf_correlation_signature(
        context_kind,
        topics,
        audience_signals,
    )

    if _cf_correlation_recently_emitted(
        fingerprint,
        now,
    ):
        return None

    strength = _cf_clamp(
        0.55
        + 0.05 * max(0, len(domains) - 1)
        + 0.03 * max(0, len(topics) - 1)
    )

    audience = _cf_current_audience_block(
        now=now,
        seconds=CONTEXT_FUSION_CORRELATION_WINDOW_SECONDS,
    )

    output = {
        "output_id": uuid.uuid4().hex,
        "message_type": "fusion_signal",
        "fusion_version": CONTEXT_FUSION_VERSION,
        "context_kind": context_kind,
        "platform": context_fusion_platform,
        "live_id": context_fusion_live_id,
        "subject": context_fusion_subject,
        "timestamp": now,
        "iso_time": _cf_now_iso(),
        "domains": domains,
        "primary": _cf_choose_primary(topics),
        "topics": copy.deepcopy(topics),
        "audience": audience,
        "evidence": [],
        "correlation": {
            "window_seconds": CONTEXT_FUSION_CORRELATION_WINDOW_SECONDS,
            "correlation_only": True,
            "causality_claimed": False,
            "note": note,
        },
        "strength": strength,
        "requires_response": any(
            topic.get("requires_response")
            for topic in topics
        ),
        "correlation_only": True,
    }

    context_fusion_last_correlation_emitted[
        fingerprint
    ] = now

    _cf_queue_output(output)
    return output


def _cf_maybe_emit_correlations(now=None):
    if now is None:
        now = time.time()

    topics = context_fusion_active_topics(
        now=now,
        seconds=CONTEXT_FUSION_CORRELATION_WINDOW_SECONDS,
    )

    if not topics:
        return []

    categories = {
        topic.get("category")
        for topic in topics
    }

    domains = {
        topic.get("domain")
        for topic in topics
        if topic.get("domain")
    }

    audience = _cf_current_audience_block(
        now=now,
        seconds=CONTEXT_FUSION_CORRELATION_WINDOW_SECONDS,
    )

    audience_signals = set(
        audience.get("active_signals") or []
    )

    outputs = []

    # Intencao de compra + barreira.
    if (
        "buying_intent" in categories
        and (
            "objection" in categories
            or "purchase_barrier" in categories
        )
    ):
        out = _cf_emit_correlation(
            "buying_intent_with_barrier",
            topics,
            list(audience_signals),
            now=now,
            note=(
                "Intencao de compra e barreira ocorreram na mesma janela; "
                "isso e correlacao temporal, nao causalidade."
            ),
        )
        if out:
            outputs.append(out)

    # Produto + comercial ativos na mesma janela.
    if (
        "product" in domains
        and "commercial" in domains
    ):
        out = _cf_emit_correlation(
            "product_interest_with_commercial_engagement",
            topics,
            list(audience_signals),
            now=now,
            note=(
                "Ha interesse de produto e atividade comercial proximos no tempo."
            ),
        )
        if out:
            outputs.append(out)

    positive_audience = bool(
        audience_signals
        & {
            "viewer_growth",
            "like_acceleration",
            "share_activity",
            "follow_activity",
        }
    )

    if topics and positive_audience:
        out = _cf_emit_correlation(
            "active_interest_with_positive_audience",
            topics,
            list(audience_signals),
            now=now,
            note=(
                "Sinais de interesse e sinais positivos de audiencia coexistem; "
                "nao e feita atribuicao causal."
            ),
        )
        if out:
            outputs.append(out)

    negative_audience = bool(
        audience_signals
        & {
            "viewer_drop",
            "like_slowdown",
        }
    )

    if (
        negative_audience
        and (
            "objection" in categories
            or "purchase_barrier" in categories
        )
    ):
        out = _cf_emit_correlation(
            "barrier_with_audience_slowdown",
            topics,
            list(audience_signals),
            now=now,
            note=(
                "Barreira e desaceleracao/queda coexistem na janela; "
                "nao e afirmado que uma causou a outra."
            ),
        )
        if out:
            outputs.append(out)

    return outputs


# ============================================================
# 29. SNAPSHOT ATUAL
# ============================================================

def _cf_snapshot_blocks(topics):
    blocks = {
        "product": [],
        "commercial": [],
        "objections": [],
        "post_purchase": [],
    }

    for topic in topics:
        domain = topic.get("domain")
        if domain in blocks:
            blocks[domain].append(
                copy.deepcopy(topic)
            )

    return blocks


def context_fusion_current_snapshot(
    now=None,
):
    if now is None:
        now = time.time()

    topics = context_fusion_active_topics(
        now=now,
        seconds=CONTEXT_FUSION_STATE_WINDOW_SECONDS,
    )

    audience = _cf_current_audience_block(
        now=now,
        seconds=CONTEXT_FUSION_STATE_WINDOW_SECONDS,
    )

    if not topics and not audience.get("active_signals"):
        return None

    blocks = _cf_snapshot_blocks(topics)

    domains = {
        topic.get("domain")
        for topic in topics
        if topic.get("domain")
    }

    if audience.get("active_signals"):
        domains.add("audience")

    strongest = 0.0
    for topic in topics:
        strongest = max(
            strongest,
            _cf_number(topic.get("strength"), 0) or 0,
        )

    for signal in audience.get("signals") or []:
        strongest = max(
            strongest,
            _cf_number(signal.get("strength"), 0) or 0,
        )

    return {
        "output_id": uuid.uuid4().hex,
        "message_type": "context_snapshot",
        "fusion_version": CONTEXT_FUSION_VERSION,
        "context_kind": "current_live_context",
        "platform": context_fusion_platform,
        "live_id": context_fusion_live_id,
        "subject": context_fusion_subject,
        "timestamp": now,
        "iso_time": _cf_now_iso(),
        "domains": sorted(domains),
        "primary": _cf_choose_primary(topics),
        "topics": copy.deepcopy(topics),
        "product": {
            "topics": blocks["product"],
            "active_topics": [
                item.get("subtype")
                for item in blocks["product"]
            ],
            "count": len(blocks["product"]),
        },
        "commercial": {
            "topics": blocks["commercial"],
            "active_topics": [
                item.get("subtype")
                for item in blocks["commercial"]
            ],
            "count": len(blocks["commercial"]),
        },
        "objections": {
            "topics": blocks["objections"],
            "active_topics": [
                item.get("subtype")
                for item in blocks["objections"]
            ],
            "count": len(blocks["objections"]),
        },
        "post_purchase": {
            "topics": blocks["post_purchase"],
            "active_topics": [
                item.get("subtype")
                for item in blocks["post_purchase"]
            ],
            "count": len(blocks["post_purchase"]),
        },
        "audience": audience,
        "evidence": [],
        "strength": _cf_clamp(strongest),
        "requires_response": any(
            topic.get("requires_response")
            for topic in topics
        ),
        "correlation_only": True,
        "decision_candidate": False,
    }


def _cf_snapshot_fingerprint(snapshot):
    if not snapshot:
        return None

    compact = {
        "topics": [
            (
                _cf_topic_key(topic),
                min(9, _cf_int(topic.get("count"), 1)),
                min(9, _cf_int(topic.get("unique_users"), 0)),
            )
            for topic in snapshot.get("topics") or []
        ],
        "audience": sorted(
            (snapshot.get("audience") or {}).get(
                "active_signals"
            ) or []
        ),
    }

    return _cf_stable_hash(compact)


def _cf_maybe_emit_snapshot(
    now=None,
    force=False,
):
    global context_fusion_last_snapshot_time
    global context_fusion_last_snapshot_fingerprint

    if now is None:
        now = time.time()

    if (
        not force
        and (
            now - context_fusion_last_snapshot_time
            < CONTEXT_FUSION_SNAPSHOT_INTERVAL_SECONDS
        )
    ):
        return None

    snapshot = context_fusion_current_snapshot(now=now)

    context_fusion_last_snapshot_time = now

    if snapshot is None:
        return None

    fingerprint = _cf_snapshot_fingerprint(snapshot)

    if (
        not force
        and fingerprint
        and fingerprint == context_fusion_last_snapshot_fingerprint
    ):
        context_fusion_stats[
            "unchanged_snapshots_suppressed"
        ] += 1
        return None

    context_fusion_last_snapshot_fingerprint = fingerprint

    # V1.1: snapshot continua disponivel para diagnostico/historico,
    # mas nao disputa a fila operacional com fusion_event/fusion_signal.
    context_fusion_snapshot_history.append(
        copy.deepcopy(snapshot)
    )
    context_fusion_stats["context_snapshots"] += 1
    context_fusion_stats["snapshots_history_only"] += 1

    if CONTEXT_FUSION_QUEUE_SNAPSHOTS_TO_DECISION:
        _cf_queue_output(snapshot)

    return snapshot


# ============================================================
# 30. TICK PERIODICO
# ============================================================

def context_fusion_tick(
    now=None,
    force=False,
):
    if now is None:
        now = time.time()

    emitted = context_fusion_flush_pending(
        now=now,
        force=force,
    )

    emitted.extend(
        _cf_maybe_emit_correlations(now)
    )

    snapshot = _cf_maybe_emit_snapshot(
        now=now,
        force=force,
    )

    if snapshot and CONTEXT_FUSION_QUEUE_SNAPSHOTS_TO_DECISION:
        emitted.append(snapshot)

    return emitted


# ============================================================
# 31. RESET
# ============================================================

def context_fusion_reset(platform=None):
    global context_fusion_output_queue
    global context_fusion_running
    global context_fusion_platform
    global context_fusion_live_id
    global context_fusion_subject
    global context_fusion_started_at
    global context_fusion_last_error
    global context_fusion_last_snapshot_time
    global context_fusion_last_snapshot_fingerprint

    context_fusion_output_queue = asyncio.Queue(
        maxsize=CONTEXT_FUSION_OUTPUT_QUEUE_LIMIT
    )

    context_fusion_output_history.clear()
    context_fusion_event_history.clear()
    context_fusion_signal_history.clear()
    context_fusion_snapshot_history.clear()
    context_fusion_input_history.clear()

    context_fusion_pending_clusters.clear()
    context_fusion_root_to_cluster.clear()
    context_fusion_pending_topic_signals.clear()

    context_fusion_topic_windows.clear()
    context_fusion_latest_topic_signal.clear()
    context_fusion_recent_question_topic_emissions.clear()
    context_fusion_audience_window.clear()

    context_fusion_seen_input_ids.clear()
    context_fusion_seen_input_order.clear()
    context_fusion_last_correlation_emitted.clear()
    context_fusion_recent_semantic_events.clear()
    context_fusion_recent_comment_fingerprints.clear()
    context_fusion_last_audience_event_emitted.clear()

    context_fusion_stats.clear()
    context_fusion_source_counts.clear()
    context_fusion_message_type_counts.clear()

    context_fusion_running = False
    context_fusion_platform = platform
    context_fusion_live_id = None
    context_fusion_subject = None
    context_fusion_started_at = time.time()
    context_fusion_last_error = None

    context_fusion_last_snapshot_time = 0.0
    context_fusion_last_snapshot_fingerprint = None


# ============================================================
# 32. API DE SAIDA PARA DECISION COACH
# ============================================================

async def context_fusion_next_output(timeout=None):
    queue = context_fusion_output_queue

    if queue is None:
        raise RuntimeError(
            "Context Fusion ainda nao iniciou uma LIVE."
        )

    if timeout is None:
        return await queue.get()

    return await asyncio.wait_for(
        queue.get(),
        timeout=timeout,
    )


async def context_fusion_next_context(timeout=None):
    return await context_fusion_next_output(
        timeout=timeout
    )


# ============================================================
# 33. HISTORICOS PUBLICOS
# ============================================================

def context_fusion_recent_outputs(limit=20):
    if limit <= 0:
        return []
    return list(context_fusion_output_history)[-limit:]


def context_fusion_recent_events(limit=20):
    if limit <= 0:
        return []
    return list(context_fusion_event_history)[-limit:]


def context_fusion_recent_signals(limit=20):
    if limit <= 0:
        return []
    return list(context_fusion_signal_history)[-limit:]


def context_fusion_recent_snapshots(limit=10):
    if limit <= 0:
        return []
    return list(context_fusion_snapshot_history)[-limit:]


# ============================================================
# 34. ESTADO DAS FILAS-FONTE
# ============================================================

def _cf_source_queue_size(source):
    spec = CONTEXT_FUSION_SOURCES.get(source) or {}
    queue = globals().get(spec.get("queue"))

    if queue is None:
        return None

    try:
        return queue.qsize()
    except Exception:
        return None


def _cf_source_running(source):
    spec = CONTEXT_FUSION_SOURCES.get(source) or {}
    return bool(
        globals().get(spec.get("running"), False)
    )


def _cf_source_ready(source):
    spec = CONTEXT_FUSION_SOURCES.get(source) or {}
    next_fn = globals().get(spec.get("next"))
    queue = globals().get(spec.get("queue"))

    return callable(next_fn) and queue is not None


def _cf_all_sources_ready():
    return all(
        _cf_source_ready(source)
        for source in CONTEXT_FUSION_SOURCES
    )


def _cf_all_sources_finished():
    for source in CONTEXT_FUSION_SOURCES:
        if _cf_source_running(source):
            return False

        size = _cf_source_queue_size(source)
        if size not in (None, 0):
            return False

    return True


# ============================================================
# 35. CONSUMIDOR DE UMA FONTE
# ============================================================

async def _cf_consume_source(source):
    global context_fusion_last_error

    spec = CONTEXT_FUSION_SOURCES[source]
    next_fn = globals().get(spec["next"])

    if not callable(next_fn):
        return

    while True:
        output = None

        try:
            output = await next_fn(timeout=0.45)

        except asyncio.TimeoutError:
            output = None

        except asyncio.CancelledError:
            raise

        except Exception as e:
            context_fusion_last_error = (
                f"{source}: {type(e).__name__}: {e}"
            )
            await asyncio.sleep(0.10)

        if output is not None:
            context_fusion_ingest(
                source,
                output,
                now=time.time(),
            )
            continue

        if (
            not live_engine_is_running()
            and not _cf_source_running(source)
            and (_cf_source_queue_size(source) in (None, 0))
        ):
            break


# ============================================================
# 36. LOOP PERIODICO
# ============================================================

async def _cf_periodic_loop():
    while True:
        context_fusion_tick(
            now=time.time(),
            force=False,
        )

        if (
            not live_engine_is_running()
            and _cf_all_sources_finished()
        ):
            break

        await asyncio.sleep(0.20)

    # Drenar tudo ao terminar a LIVE.
    context_fusion_tick(
        now=time.time(),
        force=True,
    )


# ============================================================
# 37. LOOP PRINCIPAL
# ============================================================

async def context_fusion_loop():
    global context_fusion_running
    global context_fusion_last_error

    context_fusion_running = True

    tasks = [
        asyncio.create_task(
            _cf_consume_source(source)
        )
        for source in CONTEXT_FUSION_SOURCES
    ]

    periodic = asyncio.create_task(
        _cf_periodic_loop()
    )

    try:
        await asyncio.gather(
            *tasks,
            periodic,
        )

    except asyncio.CancelledError:
        for task in tasks:
            if not task.done():
                task.cancel()
        if not periodic.done():
            periodic.cancel()
        raise

    except Exception as e:
        context_fusion_last_error = (
            f"{type(e).__name__}: {e}"
        )

    finally:
        context_fusion_running = False


# ============================================================
# 38. SUPERVISOR
# ============================================================

async def context_fusion_supervisor(platform):
    global context_fusion_last_error

    start = time.time()

    while True:
        if time.time() - start > 30:
            context_fusion_last_error = (
                "Fontes do Context Fusion nao ficaram prontas "
                "dentro de 30 segundos."
            )
            return

        try:
            ready = (
                live_engine_is_running()
                and live_engine_platform() == platform
                and _cf_all_sources_ready()
            )
        except Exception:
            ready = False

        if ready:
            break

        await asyncio.sleep(0.05)

    await context_fusion_loop()


# ============================================================
# 39. DIAGNOSTICO
# ============================================================

def context_fusion_diagnostic():
    output_queue_size = (
        context_fusion_output_queue.qsize()
        if context_fusion_output_queue is not None
        else None
    )

    return {
        "version": CONTEXT_FUSION_VERSION,
        "running": context_fusion_running,
        "platform": context_fusion_platform,
        "live_id": context_fusion_live_id,
        "subject": context_fusion_subject,
        "last_error": context_fusion_last_error,
        "stats": dict(context_fusion_stats),
        "source_counts": dict(context_fusion_source_counts),
        "message_type_counts": dict(
            context_fusion_message_type_counts
        ),
        "source_queue_sizes": {
            source: _cf_source_queue_size(source)
            for source in CONTEXT_FUSION_SOURCES
        },
        "pending_question_clusters": len(
            context_fusion_pending_clusters
        ),
        "pending_topic_signals": len(
            context_fusion_pending_topic_signals
        ),
        "semantic_memory_items": len(
            context_fusion_recent_semantic_events
        ),
        "output_queue_waiting": output_queue_size,
        "active_topics": context_fusion_active_topics(),
        "audience": _cf_current_audience_block(),
    }


def mostrar_context_fusion():
    d = context_fusion_diagnostic()
    s = d.get("stats", {})

    print("=" * 76)
    print("AGCN - CONTEXT FUSION V1.1")
    print("Versao:", CONTEXT_FUSION_VERSION)
    print("=" * 76)
    print("Rodando:", d.get("running"))
    print("Plataforma:", d.get("platform"))
    print("Live ID:", d.get("live_id"))
    print("Subject:", d.get("subject"))
    print()

    print("ENTRADA")
    print(
        " Outputs recebidos:",
        s.get("inputs_received", 0),
    )
    print(
        "  Produto:",
        d.get("source_counts", {}).get("product", 0),
    )
    print(
        "  Comercial:",
        d.get("source_counts", {}).get("commercial", 0),
    )
    print(
        "  Objecoes/Pos-compra:",
        d.get("source_counts", {}).get("objections", 0),
    )
    print(
        "  Audiencia:",
        d.get("source_counts", {}).get("audience", 0),
    )
    print(
        " Duplicados ignorados:",
        s.get("duplicates_ignored", 0),
    )
    print()

    print("FUSAO DE PERGUNTAS")
    print(
        " Perguntas unicas emitidas:",
        s.get("unique_questions_emitted", 0),
    )
    print(
        " Grupos de perguntas repetidas:",
        s.get("question_groups_emitted", 0),
    )
    print(
        " Ocorrencias repetidas agrupadas:",
        s.get("repeated_questions_grouped", 0),
    )
    print(
        " Comentarios multi-dominio fundidos:",
        s.get("multi_domain_comments_fused", 0),
    )
    print(
        " Sinais de topico anexados aos grupos:",
        s.get("topic_signals_attached_to_questions", 0),
    )
    print(
        " Sinais redundantes suprimidos:",
        s.get("redundant_topic_signals_suppressed", 0),
    )
    print(
        " Repeticoes entre janelas suprimidas:",
        s.get("cross_window_repeats_suppressed", 0),
    )
    print(
        " Grupos reemitidos por crescimento real:",
        s.get("cross_window_groups_reemitted_on_growth", 0),
    )
    print(
        " Capturas duplicadas colapsadas:",
        s.get("comment_capture_duplicates_collapsed", 0),
    )
    print()

    print("SAIDA PARA DECISION COACH")
    print(
        " Outputs enviados:",
        s.get("outputs_sent", 0),
    )
    print(
        " fusion_event:",
        s.get("fusion_events", 0),
    )
    print(
        " fusion_signal:",
        s.get("fusion_signals", 0),
    )
    print(
        " context_snapshot (historico):",
        s.get("context_snapshots", 0),
    )
    print(
        " Audiencia leve mantida so no contexto:",
        s.get("audience_state_only_suppressed", 0),
    )
    print(
        " Repeticoes de audiencia suprimidas:",
        s.get("audience_repeats_suppressed", 0),
    )
    print(
        " Aguardando Decision Coach:",
        d.get("output_queue_waiting"),
    )
    print()

    print("PENDENTES")
    print(
        " Clusters de perguntas:",
        d.get("pending_question_clusters"),
    )
    print(
        " Sinais de topico:",
        d.get("pending_topic_signals"),
    )
    print()

    print("TOPICOS ATIVOS")
    active = d.get("active_topics") or []

    if not active:
        print(" nenhum")
    else:
        for item in active[:10]:
            print(
                " ",
                item.get("domain"),
                "|",
                item.get("subtype"),
                "| count:",
                item.get("count"),
                "| usuarios:",
                item.get("unique_users"),
            )

    print()
    print("AUDIENCIA ATIVA")
    print(
        " ",
        (d.get("audience") or {}).get(
            "active_signals"
        ) or [],
    )
    print()
    print("Erro:", d.get("last_error"))
    print()
    print("ULTIMOS OUTPUTS:")

    recent = context_fusion_recent_outputs(8)

    if not recent:
        print(" nenhum ainda")
        return

    for item in recent:
        print()
        print(
            " ",
            item.get("message_type"),
            "|",
            item.get("context_kind"),
        )

        qg = item.get("question_group") or {}
        if qg:
            print(
                "   perguntas:",
                qg.get("count"),
                "| usuarios:",
                qg.get("unique_users"),
                "| repetida:",
                qg.get("same_question"),
            )
            for example in qg.get("examples") or []:
                print(
                    "    -",
                    example.get("user"),
                    ":",
                    example.get("text"),
                )

        topics = item.get("topics") or []
        if topics:
            print(
                "   topicos:",
                [
                    topic.get("subtype")
                    for topic in topics
                ],
            )

        audience = item.get("audience") or {}
        if audience.get("active_signals"):
            print(
                "   audiencia:",
                audience.get("active_signals"),
            )


# ============================================================
# 40. HELPERS DE TESTE
# ============================================================

def _cf_test_topic(
    domain,
    category,
    subtype,
    label=None,
    requires_response=True,
):
    return {
        "domain": domain,
        "category": category,
        "subtype": subtype,
        "family": domain,
        "label": label or subtype,
        "answer_target": f"esclarecer {label or subtype}",
        "requires_response": requires_response,
    }


def _cf_test_individual(
    source,
    output_id,
    root_id,
    user,
    text,
    topics,
    timestamp,
):
    message_type = {
        "product": "product_analysis",
        "commercial": "commercial_analysis",
        "objections": "objections_analysis",
    }[source]

    return {
        "output_id": output_id,
        "message_type": message_type,
        "analysis_type": "individual",
        "source_coach": source,
        "platform": "tiktok",
        "live_id": "test-live",
        "subject": "@teste",
        "timestamp": timestamp,
        "iso_time": _cf_now_iso(),
        "source_output_id": root_id,
        "source_dispatch_id": f"dispatch-{output_id}",
        "response_needed": any(
            item.get("requires_response")
            for item in topics
        ),
        "topics": copy.deepcopy(topics),
        "comment": {
            "event_id": root_id,
            "user": user,
            "text": text,
        },
    }


def _cf_test_topic_signal(
    source,
    output_id,
    topic,
    count,
    unique_users,
    timestamp,
):
    message_type = {
        "product": "product_topic_signal",
        "commercial": "commercial_topic_signal",
        "objections": "objections_topic_signal",
    }[source]

    return {
        "output_id": output_id,
        "message_type": message_type,
        "analysis_type": "local_activity",
        "source_coach": source,
        "platform": "tiktok",
        "live_id": "test-live",
        "subject": "@teste",
        "timestamp": timestamp,
        "topic": copy.deepcopy(topic),
        "response_needed": bool(
            topic.get("requires_response")
        ),
        "evidence": {
            "count": count,
            "unique_users": unique_users,
            "multiple_users": unique_users >= 2,
            "repeated_by_same_user": (
                count >= 2 and unique_users <= 1
            ),
            "examples": [],
            "source_output_ids": [],
        },
    }


def _cf_test_audience(
    output_id,
    signal_type,
    timestamp,
    strength=0.7,
):
    return {
        "output_id": output_id,
        "message_type": "audience_signal",
        "source_worker": "audience",
        "platform": "tiktok",
        "live_id": "test-live",
        "subject": "@teste",
        "signal_type": signal_type,
        "timestamp": timestamp,
        "data": {
            "strength": strength,
        },
    }


# ============================================================
# 41. TESTE INTERNO
# ============================================================

def testar_context_fusion_v11(verbose=False):
    print("=" * 76)
    print("TESTE INTERNO - CONTEXT FUSION V1.1")
    print("=" * 76)

    approved = 0
    failures = []

    def check(name, condition, detail=None):
        nonlocal approved
        if condition:
            approved += 1
            if verbose:
                print("OK  -", name)
        else:
            failures.append({
                "test": name,
                "detail": detail,
            })
            print("FALHA -", name, "|", detail)

    base = 1000.0

    # --------------------------------------------------------
    # A. UMA PERGUNTA INDIVIDUAL
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    connectivity = _cf_test_topic(
        "product",
        "product_question",
        "connectivity",
        "conectividade",
        True,
    )

    one = _cf_test_individual(
        "product",
        "p1",
        "root1",
        "Ana",
        "tem GPS?",
        [connectivity],
        base,
    )

    context_fusion_ingest(
        "product",
        one,
        now=base,
    )

    check(
        "pergunta fica pendente para coalescencia",
        len(context_fusion_pending_clusters) == 1,
        len(context_fusion_pending_clusters),
    )

    emitted = context_fusion_flush_pending(
        now=base + 2,
        force=False,
    )

    event = next(
        (
            item
            for item in emitted
            if item.get("message_type") == "fusion_event"
        ),
        None,
    )

    check(
        "pergunta individual vira fusion_event",
        event is not None,
        emitted,
    )
    check(
        "pergunta individual preserva usuario",
        bool(event)
        and (event.get("question_group") or {}).get(
            "examples",
            [{}],
        )[0].get("user") == "Ana",
        event,
    )
    check(
        "pergunta individual preserva texto",
        bool(event)
        and (event.get("question_group") or {}).get(
            "examples",
            [{}],
        )[0].get("text") == "tem GPS?",
        event,
    )
    check(
        "topico connectivity preservado",
        bool(event)
        and any(
            topic.get("subtype") == "connectivity"
            for topic in event.get("topics") or []
        ),
        event,
    )

    # --------------------------------------------------------
    # B. TRES PERGUNTAS IGUAIS = UM GRUPO
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    for idx, (user, text) in enumerate([
        ("Ana", "faz ligaÃ§Ã£o?"),
        ("Bia", "faz ligacao?"),
        ("Caio", "ele faz ligaÃ§Ã£o?"),
    ], start=1):
        context_fusion_ingest(
            "product",
            _cf_test_individual(
                "product",
                f"rp{idx}",
                f"rroot{idx}",
                user,
                text,
                [connectivity],
                base + idx * 0.1,
            ),
            now=base + idx * 0.1,
        )

    emitted = context_fusion_flush_pending(
        now=base + 2,
        force=True,
    )

    repeated = next(
        (
            item
            for item in emitted
            if item.get("context_kind") == "repeated_question"
        ),
        None,
    )

    check(
        "perguntas iguais agrupadas em um output",
        repeated is not None,
        emitted,
    )
    check(
        "grupo conta 3 perguntas",
        bool(repeated)
        and (repeated.get("question_group") or {}).get("count") == 3,
        repeated,
    )
    check(
        "grupo reconhece 3 usuarios",
        bool(repeated)
        and (repeated.get("question_group") or {}).get(
            "unique_users"
        ) == 3,
        repeated,
    )
    check(
        "grupo marca same_question",
        bool(repeated)
        and (repeated.get("question_group") or {}).get(
            "same_question"
        ) is True,
        repeated,
    )

    # --------------------------------------------------------
    # C. PERGUNTAS DIFERENTES DO MESMO SUBTIPO NAO SE FUNDAM
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    for idx, text in enumerate([
        "tem GPS?",
        "faz ligaÃ§Ã£o?",
    ], start=1):
        context_fusion_ingest(
            "product",
            _cf_test_individual(
                "product",
                f"dp{idx}",
                f"droot{idx}",
                f"U{idx}",
                text,
                [connectivity],
                base + idx * 0.1,
            ),
            now=base + idx * 0.1,
        )

    check(
        "perguntas diferentes de connectivity ficam em 2 clusters",
        len(context_fusion_pending_clusters) == 2,
        list(context_fusion_pending_clusters.values()),
    )

    emitted = context_fusion_flush_pending(
        now=base + 2,
        force=True,
    )

    unique_events = [
        item
        for item in emitted
        if item.get("message_type") == "fusion_event"
    ]

    check(
        "duas perguntas diferentes geram 2 eventos",
        len(unique_events) == 2,
        unique_events,
    )

    # --------------------------------------------------------
    # D. MESMO COMENTARIO MULTI-ROTA = UM CONTEXTO
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    color = _cf_test_topic(
        "product",
        "product_question",
        "color_variant",
        "cor ou variacao",
        True,
    )
    price = _cf_test_topic(
        "commercial",
        "commercial_question",
        "price",
        "preco",
        True,
    )

    product_part = _cf_test_individual(
        "product",
        "m1p",
        "same-root",
        "Dani",
        "tem rosa e quanto custa?",
        [color],
        base,
    )
    commercial_part = _cf_test_individual(
        "commercial",
        "m1c",
        "same-root",
        "Dani",
        "tem rosa e quanto custa?",
        [price],
        base,
    )

    context_fusion_ingest(
        "product",
        product_part,
        now=base,
    )
    context_fusion_ingest(
        "commercial",
        commercial_part,
        now=base + 0.1,
    )

    emitted = context_fusion_flush_pending(
        now=base + 2,
        force=True,
    )

    multi = next(
        (
            item
            for item in emitted
            if item.get("context_kind") == "multi_domain_comment"
        ),
        None,
    )

    check(
        "multi-rota do mesmo comentario vira um evento",
        multi is not None,
        emitted,
    )
    check(
        "multi-rota tem product e commercial",
        bool(multi)
        and set(multi.get("domains") or []) == {
            "product",
            "commercial",
        },
        multi,
    )
    check(
        "multi-rota preserva dois topicos",
        bool(multi)
        and {
            t.get("subtype")
            for t in multi.get("topics") or []
        } == {"color_variant", "price"},
        multi,
    )
    check(
        "multi-rota conta somente uma pergunta original",
        bool(multi)
        and (multi.get("question_group") or {}).get("count") == 1,
        multi,
    )

    # --------------------------------------------------------
    # E. TOPIC SIGNAL ANEXADO AO CLUSTER
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    context_fusion_ingest(
        "product",
        _cf_test_individual(
            "product",
            "ts1",
            "tsroot",
            "Eva",
            "qual o valor?",
            [price],
            base,
        ),
        now=base,
    )

    # price e commercial; criar via comercial.
    signal = _cf_test_topic_signal(
        "commercial",
        "ts-signal",
        price,
        2,
        2,
        base + 0.2,
    )

    # Corrigir o source do individual acima para commercial via segunda entrada.
    context_fusion_reset("tiktok")
    context_fusion_ingest(
        "commercial",
        _cf_test_individual(
            "commercial",
            "ts2",
            "tsroot2",
            "Eva",
            "qual o valor?",
            [price],
            base,
        ),
        now=base,
    )
    attach_result = context_fusion_ingest(
        "commercial",
        signal,
        now=base + 0.2,
    )

    check(
        "topic_signal e anexado a pergunta pendente",
        attach_result == "attached",
        attach_result,
    )
    check(
        "topic_signal anexado nao fica pendente separado",
        len(context_fusion_pending_topic_signals) == 0,
        context_fusion_pending_topic_signals,
    )

    emitted = context_fusion_flush_pending(
        now=base + 2,
        force=True,
    )

    attached_event = next(
        (
            item
            for item in emitted
            if item.get("message_type") == "fusion_event"
        ),
        None,
    )

    price_topic = next(
        (
            item
            for item in (attached_event or {}).get("topics", [])
            if item.get("subtype") == "price"
        ),
        None,
    )

    check(
        "topic_signal eleva count do topico",
        bool(price_topic)
        and price_topic.get("count") == 2,
        attached_event,
    )
    check(
        "topic_signal eleva unique_users",
        bool(price_topic)
        and price_topic.get("unique_users") == 2,
        attached_event,
    )

    # --------------------------------------------------------
    # F. TOPIC SIGNAL SOZINHO
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    solo_signal = _cf_test_topic_signal(
        "commercial",
        "solo-signal",
        price,
        3,
        3,
        base,
    )

    context_fusion_ingest(
        "commercial",
        solo_signal,
        now=base,
    )

    emitted = context_fusion_flush_pending(
        now=base + 2,
        force=True,
    )

    topic_activity = next(
        (
            item
            for item in emitted
            if item.get("context_kind") == "topic_activity"
        ),
        None,
    )

    check(
        "topic_signal sozinho vira fusion_signal",
        bool(topic_activity)
        and topic_activity.get("message_type") == "fusion_signal",
        emitted,
    )
    check(
        "topic_activity preserva count 3",
        bool(topic_activity)
        and (topic_activity.get("topic_activity") or {}).get("count") == 3,
        topic_activity,
    )

    # --------------------------------------------------------
    # G. AUDIENCIA
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    audience_output = context_fusion_ingest(
        "audience",
        _cf_test_audience(
            "a1",
            "viewer_growth",
            base,
            0.8,
        ),
        now=base,
    )

    check(
        "audience_signal vira fusion_event",
        bool(audience_output)
        and audience_output.get("message_type") == "fusion_event",
        audience_output,
    )
    check(
        "viewer_growth preservado",
        bool(audience_output)
        and "viewer_growth"
        in (
            audience_output.get("audience") or {}
        ).get("active_signals", []),
        audience_output,
    )

    # --------------------------------------------------------
    # H. CORRELACAO PRODUTO + COMERCIAL
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    context_fusion_ingest(
        "product",
        _cf_test_individual(
            "product",
            "cp1",
            "cpr1",
            "F1",
            "tem GPS?",
            [connectivity],
            base,
        ),
        now=base,
    )
    context_fusion_ingest(
        "commercial",
        _cf_test_individual(
            "commercial",
            "cp2",
            "cpr2",
            "F2",
            "qual o valor?",
            [price],
            base + 0.2,
        ),
        now=base + 0.2,
    )

    correlation = next(
        (
            item
            for item in context_fusion_signal_history
            if item.get("context_kind")
            == "product_interest_with_commercial_engagement"
        ),
        None,
    )

    check(
        "produto + comercial gera correlacao",
        correlation is not None,
        list(context_fusion_signal_history),
    )
    check(
        "correlacao nao afirma causalidade",
        bool(correlation)
        and (correlation.get("correlation") or {}).get(
            "causality_claimed"
        ) is False,
        correlation,
    )

    # --------------------------------------------------------
    # I. INTENCAO + BARREIRA
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    buying = _cf_test_topic(
        "commercial",
        "buying_intent",
        "conditional_purchase_intent",
        "intencao de compra condicionada",
        False,
    )
    shipping_objection = _cf_test_topic(
        "objections",
        "objection",
        "shipping_objection",
        "objecao de frete",
        False,
    )

    context_fusion_ingest(
        "commercial",
        _cf_test_individual(
            "commercial",
            "bi1",
            "bir1",
            "G1",
            "se o frete baixar eu compro",
            [buying],
            base,
        ),
        now=base,
    )
    context_fusion_ingest(
        "objections",
        _cf_test_individual(
            "objections",
            "bi2",
            "bir2",
            "G2",
            "frete ta caro",
            [shipping_objection],
            base + 0.2,
        ),
        now=base + 0.2,
    )

    barrier_correlation = next(
        (
            item
            for item in context_fusion_signal_history
            if item.get("context_kind")
            == "buying_intent_with_barrier"
        ),
        None,
    )

    check(
        "intencao + objecao gera contexto cruzado",
        barrier_correlation is not None,
        list(context_fusion_signal_history),
    )

    # --------------------------------------------------------
    # J. AUDIENCIA + INTERESSE
    # --------------------------------------------------------
    context_fusion_ingest(
        "audience",
        _cf_test_audience(
            "a2",
            "like_acceleration",
            base + 0.3,
            0.9,
        ),
        now=base + 0.3,
    )

    positive = next(
        (
            item
            for item in context_fusion_signal_history
            if item.get("context_kind")
            == "active_interest_with_positive_audience"
        ),
        None,
    )

    check(
        "interesse + audiencia positiva gera correlacao",
        positive is not None,
        list(context_fusion_signal_history),
    )

    # --------------------------------------------------------
    # K. SNAPSHOT
    # --------------------------------------------------------
    snapshot = context_fusion_current_snapshot(
        now=base + 0.4,
    )

    check(
        "snapshot existe com contexto ativo",
        snapshot is not None,
        snapshot,
    )
    check(
        "snapshot tem message_type correto",
        bool(snapshot)
        and snapshot.get("message_type") == "context_snapshot",
        snapshot,
    )
    check(
        "snapshot expoe audiencia",
        bool(snapshot)
        and "like_acceleration"
        in (snapshot.get("audience") or {}).get(
            "active_signals",
            [],
        ),
        snapshot,
    )

    # --------------------------------------------------------
    # L. DEDUPE INPUT
    # --------------------------------------------------------
    before = context_fusion_stats.get(
        "inputs_received",
        0,
    )

    duplicate = _cf_test_audience(
        "dup-audience",
        "share_activity",
        base + 1,
        0.4,
    )

    context_fusion_ingest(
        "audience",
        duplicate,
        now=base + 1,
    )
    context_fusion_ingest(
        "audience",
        duplicate,
        now=base + 1.1,
    )

    after = context_fusion_stats.get(
        "inputs_received",
        0,
    )

    check(
        "input duplicado conta uma vez",
        after == before + 1,
        {"before": before, "after": after},
    )
    check(
        "contador de duplicados incrementa",
        context_fusion_stats.get(
            "duplicates_ignored",
            0,
        ) >= 1,
        dict(context_fusion_stats),
    )

    # --------------------------------------------------------
    # M. CONTRATO PARA DECISION COACH
    # --------------------------------------------------------
    contract_sample = snapshot or {}

    check(
        "contrato possui domains",
        isinstance(contract_sample.get("domains"), list),
        contract_sample,
    )
    check(
        "contrato possui topics",
        isinstance(contract_sample.get("topics"), list),
        contract_sample,
    )
    check(
        "contrato possui audience",
        isinstance(contract_sample.get("audience"), dict),
        contract_sample,
    )
    check(
        "contrato possui strength",
        contract_sample.get("strength") is not None,
        contract_sample,
    )
    check(
        "contrato possui requires_response",
        "requires_response" in contract_sample,
        contract_sample,
    )

    # --------------------------------------------------------
    # N. REGRA CENTRAL DO MODO TODOS
    # --------------------------------------------------------
    # Verifica explicitamente a premissa pedida pelo usuario:
    # iguais agrupadas; diferentes preservadas.
    context_fusion_reset("tiktok")

    samples = [
        ("H1", "qual o valor?", "x1"),
        ("H2", "qual o valor", "x2"),
        ("H3", "aceita pix?", "x3"),
    ]

    payment = _cf_test_topic(
        "commercial",
        "commercial_question",
        "payment_method",
        "forma de pagamento",
        True,
    )

    for idx, (user, text, root) in enumerate(samples):
        topic = price if "valor" in text else payment
        context_fusion_ingest(
            "commercial",
            _cf_test_individual(
                "commercial",
                f"all-{idx}",
                root,
                user,
                text,
                [topic],
                base + idx * 0.1,
            ),
            now=base + idx * 0.1,
        )

    all_mode_outputs = context_fusion_flush_pending(
        now=base + 2,
        force=True,
    )

    all_events = [
        item
        for item in all_mode_outputs
        if item.get("message_type") == "fusion_event"
    ]

    check(
        "regra Todos: 2 iguais + 1 diferente = 2 contextos",
        len(all_events) == 2,
        all_events,
    )

    price_group = next(
        (
            item
            for item in all_events
            if any(
                topic.get("subtype") == "price"
                for topic in item.get("topics") or []
            )
        ),
        None,
    )

    payment_event = next(
        (
            item
            for item in all_events
            if any(
                topic.get("subtype") == "payment_method"
                for topic in item.get("topics") or []
            )
        ),
        None,
    )

    check(
        "regra Todos: pergunta repetida vira grupo count 2",
        bool(price_group)
        and (price_group.get("question_group") or {}).get("count") == 2,
        price_group,
    )
    check(
        "regra Todos: pergunta diferente continua individual",
        bool(payment_event)
        and (payment_event.get("question_group") or {}).get("count") == 1,
        payment_event,
    )
    check(
        "regra Todos: usuario da pergunta diferente e preservado",
        bool(payment_event)
        and (payment_event.get("question_group") or {}).get(
            "examples",
            [{}],
        )[0].get("user") == "H3",
        payment_event,
    )

    # --------------------------------------------------------
    # O. MESMA DUVIDA COM PALAVRAS DIFERENTES
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    for idx, (user, text) in enumerate([
        ("I1", "qual o valor?"),
        ("I2", "quanto custa?"),
        ("I3", "preco?"),
    ]):
        context_fusion_ingest(
            "commercial",
            _cf_test_individual(
                "commercial",
                f"semantic-price-{idx}",
                f"semantic-price-root-{idx}",
                user,
                text,
                [price],
                base + idx * 0.1,
            ),
            now=base + idx * 0.1,
        )

    semantic_price_outputs = context_fusion_flush_pending(
        now=base + 2,
        force=True,
    )
    semantic_price_events = [
        item
        for item in semantic_price_outputs
        if item.get("message_type") == "fusion_event"
    ]

    check(
        "mesma pergunta semantica de preco vira um unico grupo",
        len(semantic_price_events) == 1,
        semantic_price_events,
    )
    check(
        "grupo semantico de preco preserva 3 ocorrencias",
        bool(semantic_price_events)
        and (semantic_price_events[0].get("question_group") or {}).get("count") == 3,
        semantic_price_events,
    )

    # --------------------------------------------------------
    # P. MESMO SUBTIPO AMPLO, DUVIDAS DIFERENTES
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    for idx, (user, text) in enumerate([
        ("J1", "tem GPS?"),
        ("J2", "faz ligacao?"),
    ]):
        context_fusion_ingest(
            "product",
            _cf_test_individual(
                "product",
                f"semantic-connect-{idx}",
                f"semantic-connect-root-{idx}",
                user,
                text,
                [connectivity],
                base + idx * 0.1,
            ),
            now=base + idx * 0.1,
        )

    check(
        "connectivity: GPS e ligacao permanecem perguntas separadas",
        len(context_fusion_pending_clusters) == 2,
        list(context_fusion_pending_clusters.values()),
    )

    # --------------------------------------------------------
    # Q. REPETICAO EM JANELAS DIFERENTES NAO VIRA SPAM
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    defect = _cf_test_topic(
        "post_purchase",
        "post_purchase_issue",
        "defective_item",
        "produto com defeito",
        True,
    )

    first_defect = _cf_test_individual(
        "objections",
        "defect-1",
        "defect-root-1",
        "U1",
        "o produto veio com defeito",
        [defect],
        base,
    )
    context_fusion_ingest(
        "objections", first_defect, now=base
    )
    first_outputs = context_fusion_flush_pending(
        now=base + 2, force=True
    )

    second_defect = _cf_test_individual(
        "objections",
        "defect-2",
        "defect-root-2",
        "U2",
        "meu produto esta com defeito",
        [defect],
        base + 5,
    )
    context_fusion_ingest(
        "objections", second_defect, now=base + 5
    )
    second_outputs = context_fusion_flush_pending(
        now=base + 7, force=True
    )

    check(
        "mesma situacao em janela seguinte e suprimida sem crescimento suficiente",
        len([x for x in second_outputs if x.get("message_type") == "fusion_event"]) == 0,
        second_outputs,
    )

    third_defect = _cf_test_individual(
        "objections",
        "defect-3",
        "defect-root-3",
        "U3",
        "produto defeituoso aqui tambem",
        [defect],
        base + 9,
    )
    context_fusion_ingest(
        "objections", third_defect, now=base + 9
    )
    third_outputs = context_fusion_flush_pending(
        now=base + 11, force=True
    )
    escalated = next(
        (x for x in third_outputs if x.get("message_type") == "fusion_event"),
        None,
    )
    check(
        "terceira evidencia faz reemitir grupo acumulado",
        bool(escalated)
        and (escalated.get("question_group") or {}).get("count") == 3,
        escalated,
    )

    # --------------------------------------------------------
    # R. CAPTURA DUPLICADA SEM EVENT_ID E COLAPSADA
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    duplicate_a = _cf_test_individual(
        "commercial",
        "dup-cap-a",
        "tech-root-a",
        "Ana",
        "qual o valor?",
        [price],
        base,
    )
    duplicate_b = _cf_test_individual(
        "commercial",
        "dup-cap-b",
        "tech-root-b",
        "Ana",
        "qual o valor?",
        [price],
        base + 0.2,
    )
    duplicate_a["comment"]["event_id"] = None
    duplicate_b["comment"]["event_id"] = None

    context_fusion_ingest("commercial", duplicate_a, now=base)
    context_fusion_ingest("commercial", duplicate_b, now=base + 0.2)

    dup_outputs = context_fusion_flush_pending(
        now=base + 2, force=True
    )
    dup_event = next(
        (x for x in dup_outputs if x.get("message_type") == "fusion_event"),
        None,
    )
    check(
        "captura tecnica duplicada conta como uma evidencia",
        bool(dup_event)
        and (dup_event.get("question_group") or {}).get("count") == 1,
        dup_event,
    )

    # --------------------------------------------------------
    # S. FOLLOW/SHARE NAO VIRAM AVISO INDIVIDUAL
    # --------------------------------------------------------
    context_fusion_reset("tiktok")

    follow_result = context_fusion_ingest(
        "audience",
        _cf_test_audience(
            "aud-follow",
            "follow_activity",
            base,
            0.5,
        ),
        now=base,
    )
    share_result = context_fusion_ingest(
        "audience",
        _cf_test_audience(
            "aud-share",
            "share_activity",
            base + 0.1,
            0.5,
        ),
        now=base + 0.1,
    )

    check(
        "follow_activity fica no contexto sem output individual",
        follow_result is None,
        follow_result,
    )
    check(
        "share_activity fica no contexto sem output individual",
        share_result is None,
        share_result,
    )
    check(
        "follow/share continuam ativos para correlacao",
        {"follow_activity", "share_activity"}.issubset(
            set(( _cf_current_audience_block(now=base + 0.2) or {}).get("active_signals") or [])
        ),
        _cf_current_audience_block(now=base + 0.2),
    )

    # --------------------------------------------------------
    # T. SNAPSHOT NAO ENTRA NA FILA OPERACIONAL
    # --------------------------------------------------------
    snapshot_before_queue = context_fusion_output_queue.qsize()
    snapshot_v11 = _cf_maybe_emit_snapshot(
        now=base + 12,
        force=True,
    )
    snapshot_after_queue = context_fusion_output_queue.qsize()

    check(
        "snapshot continua sendo gerado",
        snapshot_v11 is not None,
        snapshot_v11,
    )
    check(
        "snapshot V1.1 nao entra na fila do Decision",
        snapshot_after_queue == snapshot_before_queue,
        {
            "before": snapshot_before_queue,
            "after": snapshot_after_queue,
        },
    )
    check(
        "snapshot marcado como nao acionavel",
        bool(snapshot_v11)
        and snapshot_v11.get("decision_candidate") is False
        and snapshot_v11.get("correlation_only") is True,
        snapshot_v11,
    )

    # Limpar estado para nao contaminar LIVE futura.
    context_fusion_reset(None)

    total = approved + len(failures)

    print()
    print("=" * 76)
    print("RESUMO DO TESTE CONTEXT FUSION V1.1")
    print("=" * 76)
    print("Aprovados:", approved)
    print("Falhas:", len(failures))
    print("Total:", total)

    result = {
        "approved": approved,
        "failed": len(failures),
        "total": total,
        "ok": len(failures) == 0,
        "contract": {
            "version": CONTEXT_FUSION_VERSION,
            "outputs": [
                "fusion_event",
                "fusion_signal",
                "context_snapshot",
            ],
            "sources": list(CONTEXT_FUSION_SOURCES.keys()),
            "all_mode_rule": (
                "todas as situacoes uteis distintas; repeticoes consolidadas sem spam"
            ),
            "snapshot_delivery": "history_only",
            "audience_state_only": sorted(
                CONTEXT_FUSION_AUDIENCE_STATE_ONLY_TYPES
            ),
        },
        "failures": failures,
    }

    print(result)
    return result


# Alias de compatibilidade com notebooks que ainda chamam o nome V1.
def testar_context_fusion_v1(verbose=False):
    return testar_context_fusion_v11(verbose=verbose)


# ============================================================
# 42. FINALIZAR TASK
# ============================================================

async def _context_fusion_finish_task(task):
    if task is None:
        return

    try:
        await asyncio.wait_for(
            asyncio.shield(task),
            timeout=3.0,
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
# 43. DESCOBRIR FUNCOES BASE
#
# Ordem oficial neste ponto:
#
# Live Engine
# -> Storage
# -> Worker Comentarios
# -> Dispatcher
# -> Coach Produto
# -> Coach Comercial
# -> Coach Objecoes
# -> Worker Audiencia
# -> Context Fusion  (esta celula)
# -> Decision Coach  (depois)
# -> Interface
#
# Se esta propria celula for reexecutada antes dos wrappers posteriores,
# nao empilhamos dois Context Fusion.
# Se Decision Coach/Interface ja estiverem carregados e voce quiser trocar
# esta versao, reinicie o runtime e execute a ordem oficial novamente.
# ============================================================

_current_fusion_shopee_executor = (
    executar_shopee_com_live_engine
)

if getattr(
    _current_fusion_shopee_executor,
    "_agcn_context_fusion_wrapper",
    False,
):
    _context_fusion_base_shopee = (
        _current_fusion_shopee_executor
        ._agcn_context_fusion_base
    )
else:
    _context_fusion_base_shopee = (
        _current_fusion_shopee_executor
    )


_current_fusion_tiktok_executor = (
    executar_tiktok_com_live_engine
)

if getattr(
    _current_fusion_tiktok_executor,
    "_agcn_context_fusion_wrapper",
    False,
):
    _context_fusion_base_tiktok = (
        _current_fusion_tiktok_executor
        ._agcn_context_fusion_base
    )
else:
    _context_fusion_base_tiktok = (
        _current_fusion_tiktok_executor
    )


# ============================================================
# 44. SHOPEE
# ============================================================

async def executar_shopee_com_live_engine():
    context_fusion_reset("shopee")

    fusion_task = asyncio.create_task(
        context_fusion_supervisor(
            "shopee"
        )
    )

    try:
        return await _context_fusion_base_shopee()

    finally:
        await _context_fusion_finish_task(
            fusion_task
        )


executar_shopee_com_live_engine._agcn_context_fusion_wrapper = True
executar_shopee_com_live_engine._agcn_context_fusion_base = (
    _context_fusion_base_shopee
)
executar_shopee_com_live_engine._agcn_context_fusion_version = (
    CONTEXT_FUSION_VERSION
)


# ============================================================
# 45. TIKTOK
# ============================================================

async def executar_tiktok_com_live_engine(username):
    context_fusion_reset("tiktok")

    fusion_task = asyncio.create_task(
        context_fusion_supervisor(
            "tiktok"
        )
    )

    try:
        return await _context_fusion_base_tiktok(
            username
        )

    finally:
        await _context_fusion_finish_task(
            fusion_task
        )


executar_tiktok_com_live_engine._agcn_context_fusion_wrapper = True
executar_tiktok_com_live_engine._agcn_context_fusion_base = (
    _context_fusion_base_tiktok
)
executar_tiktok_com_live_engine._agcn_context_fusion_version = (
    CONTEXT_FUSION_VERSION
)


# ============================================================
# 46. PRONTO
# ============================================================

print("AGCN Context Fusion V1.1 carregado.")
print("Versao:", CONTEXT_FUSION_VERSION)
print("Entradas:")
print("1. Coach Produto V1.1")
print("2. Coach Comercial V1")
print("3. Coach Objecoes/Pos-compra V1")
print("4. Worker Coach Audiencia V1")
print("Saidas:")
print("1. fusion_event")
print("2. fusion_signal")
print("3. context_snapshot (historico/API; nao entra na fila operacional por padrao)")
print("Regra: situacoes repetidas sao consolidadas inclusive entre janelas; assuntos diferentes sao preservados.")
print("Destino operacional: Decision Coach V1.2 (proxima etapa).")
print("Sem prioridade final, sem Storage e sem Interface.")
