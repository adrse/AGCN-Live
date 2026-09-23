# ============================================================
# AGCN - COACH COMERCIAL V1
#
# ARQUITETURA:
#
# Worker Coach Comentarios V1.5
#               ↓
#      Comment Dispatcher V1
#               ↓
#        fila "commercial"
#               ↓
#       COACH COMERCIAL V1
#               ↓
#       futuro Context Fusion
#
#
# RESPONSABILIDADES:
#
# - receber somente eventos roteados para Comercial
# - interpretar a classificacao ja feita pelo Worker
# - organizar perguntas comerciais por assunto
# - preservar intencoes de compra
# - preservar compras/pagamentos relatados
# - preservar observacoes comerciais relevantes
# - identificar qual informacao comercial precisa ser esclarecida
# - acompanhar repeticao por assunto
# - diferenciar repeticao da mesma pessoa de varias pessoas
# - transformar comment_trend em sinal comercial especializado
# - produzir outputs estruturados para o futuro Context Fusion
#
#
# NAO FAZ:
#
# - nao decide prioridade
# - nao decide quando mostrar
# - nao gera aviso final da Interface
# - nao acessa Coach Armazenamento
# - nao acessa Worker Audiencia
# - nao acessa Coach Produto
# - nao responde o cliente
# - nao inventa preco, estoque, frete, cupom ou condicoes comerciais
# - nao confirma compra ou pagamento real; apenas preserva o que foi relatado
#
#
# SAIDAS:
#
# 1. commercial_analysis
#    Analise comercial de um comentario individual.
#
# 2. commercial_topic_signal
#    Movimento/repeticao de um assunto comercial.
#
# ============================================================

import asyncio
import time
import uuid
import copy

from collections import deque, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo


# ============================================================
# 1. DEPENDENCIAS
# ============================================================

_CC_REQUIRED = [
    "comment_dispatcher_next_commercial",
    "live_engine_is_running",
    "live_engine_platform",
    "executar_shopee_com_live_engine",
    "executar_tiktok_com_live_engine",
]

_CC_MISSING = [
    name
    for name in _CC_REQUIRED
    if name not in globals()
]

if _CC_MISSING:
    raise RuntimeError(
        "Execute primeiro o Comment Dispatcher V1. Faltando: "
        + ", ".join(_CC_MISSING)
    )


# ============================================================
# 2. CONFIGURACAO
# ============================================================

COACH_COMMERCIAL_VERSION = "1.0"

COACH_COMMERCIAL_TIMEZONE = ZoneInfo(
    "America/Araguaina"
)

COACH_COMMERCIAL_OUTPUT_QUEUE_LIMIT = 5000
COACH_COMMERCIAL_MAX_HISTORY = 5000
COACH_COMMERCIAL_MAX_SEEN = 20000

# Janela local independente das tendencias do Worker.
COACH_COMMERCIAL_TOPIC_WINDOW_SECONDS = 60

# Comeca a produzir sinal local com 2 ocorrencias.
COACH_COMMERCIAL_LOCAL_TOPIC_THRESHOLD = 2

# Controle de reemissao do mesmo assunto.
COACH_COMMERCIAL_TOPIC_REEMIT_SECONDS = 15
COACH_COMMERCIAL_TOPIC_FORCE_REEMIT_SECONDS = 40


# ============================================================
# 3. MAPA SEMANTICO COMERCIAL
#
# O Worker Comentarios V1.5 ja classificou categoria/subtipo.
# O Coach Comercial acrescenta:
#
# - family
# - label
# - commercial_role
# - sales_stage
# - answer_target (somente quando ha algo a esclarecer)
# - interpretation_target (significado comercial do sinal)
#
# Nenhum desses campos e prioridade.
# Nenhum deles e uma resposta pronta ao cliente.
# ============================================================

COACH_COMMERCIAL_TOPIC_MAP = {
    # --------------------------------------------------------
    # PERGUNTAS COMERCIAIS
    # --------------------------------------------------------
    ("commercial_question", "price"): {
        "family": "pricing",
        "label": "preco ou valor",
        "commercial_role": "question",
        "sales_stage": "commercial_information",
        "answer_target": (
            "esclarecer o preco, valor ou valor aplicavel ao item perguntado"
        ),
        "interpretation_target": (
            "o usuario esta buscando informacao de preco antes ou durante a compra"
        ),
    },
    ("commercial_question", "promotion_coupon"): {
        "family": "promotion",
        "label": "cupom, desconto ou promocao",
        "commercial_role": "question",
        "sales_stage": "commercial_information",
        "answer_target": (
            "esclarecer cupom, desconto, promocao ou condicao promocional disponivel"
        ),
        "interpretation_target": (
            "o usuario esta buscando uma condicao promocional para a compra"
        ),
    },
    ("commercial_question", "promotion_problem"): {
        "family": "promotion",
        "label": "problema com cupom ou promocao",
        "commercial_role": "question",
        "sales_stage": "checkout_support",
        "answer_target": (
            "esclarecer o problema relatado com cupom, desconto ou promocao sem inventar regras"
        ),
        "interpretation_target": (
            "o usuario relata dificuldade para obter ou aplicar uma condicao promocional"
        ),
    },
    ("commercial_question", "shipping"): {
        "family": "shipping_and_logistics",
        "label": "frete ou envio",
        "commercial_role": "question",
        "sales_stage": "commercial_information",
        "answer_target": (
            "esclarecer frete, envio, cobertura de entrega ou condicao de frete"
        ),
        "interpretation_target": (
            "o usuario esta avaliando informacoes de frete ou envio para comprar"
        ),
    },
    ("commercial_question", "delivery_time"): {
        "family": "shipping_and_logistics",
        "label": "prazo de entrega",
        "commercial_role": "question",
        "sales_stage": "commercial_information",
        "answer_target": (
            "esclarecer o prazo estimado de entrega quando essa informacao estiver disponivel"
        ),
        "interpretation_target": (
            "o usuario esta avaliando o prazo de entrega antes da compra"
        ),
    },
    ("commercial_question", "availability"): {
        "family": "availability",
        "label": "estoque ou disponibilidade",
        "commercial_role": "question",
        "sales_stage": "commercial_information",
        "answer_target": (
            "esclarecer disponibilidade, estoque ou existencia da variacao solicitada"
        ),
        "interpretation_target": (
            "o usuario quer confirmar se o item ou variacao esta disponivel para compra"
        ),
    },
    ("commercial_question", "purchase_path"): {
        "family": "purchase_flow",
        "label": "caminho de compra",
        "commercial_role": "question",
        "sales_stage": "checkout_support",
        "answer_target": (
            "orientar onde ou como realizar a compra dentro do fluxo disponivel"
        ),
        "interpretation_target": (
            "o usuario demonstra interesse em avancar no processo de compra"
        ),
    },
    ("commercial_question", "payment_method"): {
        "family": "payment",
        "label": "forma de pagamento",
        "commercial_role": "question",
        "sales_stage": "checkout_support",
        "answer_target": (
            "esclarecer as formas de pagamento disponiveis"
        ),
        "interpretation_target": (
            "o usuario esta verificando como pode pagar pela compra"
        ),
    },
    ("commercial_question", "installments"): {
        "family": "payment",
        "label": "parcelamento",
        "commercial_role": "question",
        "sales_stage": "checkout_support",
        "answer_target": (
            "esclarecer parcelamento, quantidade de parcelas e condicoes quando disponiveis"
        ),
        "interpretation_target": (
            "o usuario esta avaliando as condicoes de parcelamento"
        ),
    },
    ("commercial_question", "invoice_fiscal_document"): {
        "family": "fiscal_document",
        "label": "nota fiscal ou documento fiscal",
        "commercial_role": "question",
        "sales_stage": "commercial_information",
        "answer_target": (
            "esclarecer se ha emissao de nota fiscal ou documento fiscal aplicavel"
        ),
        "interpretation_target": (
            "o usuario quer confirmar informacao fiscal relacionada a compra"
        ),
    },
    ("commercial_question", "variant_selection"): {
        "family": "variant_selection",
        "label": "selecao de variacao",
        "commercial_role": "question",
        "sales_stage": "purchase_configuration",
        "answer_target": (
            "esclarecer como selecionar a variacao, quantidade, cor, tamanho ou opcao desejada"
        ),
        "interpretation_target": (
            "o usuario esta configurando ou escolhendo a variacao que pretende comprar"
        ),
    },

    # --------------------------------------------------------
    # INTENCAO DE COMPRA
    # --------------------------------------------------------
    ("buying_intent", "purchase_intent"): {
        "family": "buying_intent",
        "label": "intencao de compra",
        "commercial_role": "buying_intent",
        "sales_stage": "purchase_intent",
        "answer_target": None,
        "interpretation_target": (
            "o usuario expressou intencao direta de comprar"
        ),
    },
    ("buying_intent", "conditional_purchase_intent"): {
        "family": "buying_intent",
        "label": "intencao de compra condicionada",
        "commercial_role": "buying_intent",
        "sales_stage": "conditional_purchase_intent",
        "answer_target": None,
        "interpretation_target": (
            "o usuario expressou intencao de comprar caso uma condicao mencionada seja atendida"
        ),
    },

    # --------------------------------------------------------
    # COMPRA / PAGAMENTO RELATADOS
    # --------------------------------------------------------
    ("purchase_completed", "reported_purchase"): {
        "family": "conversion_report",
        "label": "compra relatada",
        "commercial_role": "purchase_completed",
        "sales_stage": "purchase_reported",
        "answer_target": None,
        "interpretation_target": (
            "o usuario relatou que realizou a compra; isso nao confirma a transacao no sistema"
        ),
    },
    ("purchase_completed", "reported_payment"): {
        "family": "conversion_report",
        "label": "pagamento relatado",
        "commercial_role": "purchase_completed",
        "sales_stage": "payment_reported",
        "answer_target": None,
        "interpretation_target": (
            "o usuario relatou que realizou o pagamento; isso nao confirma o pagamento no sistema"
        ),
    },

    # --------------------------------------------------------
    # OBSERVACOES COMERCIAIS
    # --------------------------------------------------------
    ("commercial_observation", "promotion_mention"): {
        "family": "promotion",
        "label": "mencao de promocao",
        "commercial_role": "observation",
        "sales_stage": "commercial_context",
        "answer_target": None,
        "interpretation_target": (
            "o comentario menciona uma promocao ou oferta como contexto comercial"
        ),
    },
    ("commercial_observation", "shipping_mention"): {
        "family": "shipping_and_logistics",
        "label": "mencao de frete",
        "commercial_role": "observation",
        "sales_stage": "commercial_context",
        "answer_target": None,
        "interpretation_target": (
            "o comentario menciona frete ou envio como contexto comercial"
        ),
    },
    ("commercial_observation", "availability_mention"): {
        "family": "availability",
        "label": "mencao de estoque ou disponibilidade",
        "commercial_role": "observation",
        "sales_stage": "commercial_context",
        "answer_target": None,
        "interpretation_target": (
            "o comentario menciona estoque ou disponibilidade como contexto comercial"
        ),
    },
    ("commercial_observation", "price_mention"): {
        "family": "pricing",
        "label": "mencao de preco",
        "commercial_role": "observation",
        "sales_stage": "commercial_context",
        "answer_target": None,
        "interpretation_target": (
            "o comentario menciona um preco ou alteracao de preco como contexto comercial"
        ),
    },
    ("commercial_observation", "contextual_product_reference"): {
        "family": "product_reference",
        "label": "referencia contextual de produto",
        "commercial_role": "observation",
        "sales_stage": "commercial_context",
        "answer_target": None,
        "interpretation_target": (
            "o comentario faz uma referencia curta a produto ou variacao que pode servir de contexto para outros sinais"
        ),
    },
}


# ============================================================
# 3.1. CONTRATO COM WORKER COMENTARIOS V1.5
#
# Esta lista permite verificar automaticamente se o Coach
# Comercial cobre toda a taxonomia comercial conhecida.
# ============================================================

COACH_COMMERCIAL_WORKER_V15_TOPICS = {
    ("commercial_question", "price"),
    ("commercial_question", "promotion_coupon"),
    ("commercial_question", "promotion_problem"),
    ("commercial_question", "shipping"),
    ("commercial_question", "delivery_time"),
    ("commercial_question", "availability"),
    ("commercial_question", "purchase_path"),
    ("commercial_question", "payment_method"),
    ("commercial_question", "installments"),
    ("commercial_question", "invoice_fiscal_document"),
    ("commercial_question", "variant_selection"),
    ("buying_intent", "purchase_intent"),
    ("buying_intent", "conditional_purchase_intent"),
    ("purchase_completed", "reported_purchase"),
    ("purchase_completed", "reported_payment"),
    ("commercial_observation", "promotion_mention"),
    ("commercial_observation", "shipping_mention"),
    ("commercial_observation", "availability_mention"),
    ("commercial_observation", "price_mention"),
    ("commercial_observation", "contextual_product_reference"),
}


# ============================================================
# 4. FILA PARA O FUTURO CONTEXT FUSION
# ============================================================

coach_commercial_output_queue = None


# ============================================================
# 5. HISTORICOS
# ============================================================

coach_commercial_output_history = deque(
    maxlen=COACH_COMMERCIAL_MAX_HISTORY
)

coach_commercial_individual_history = deque(
    maxlen=COACH_COMMERCIAL_MAX_HISTORY
)

coach_commercial_topic_signal_history = deque(
    maxlen=COACH_COMMERCIAL_MAX_HISTORY
)


# ============================================================
# 6. JANELAS LOCAIS POR TOPICO
# ============================================================

coach_commercial_topic_windows = defaultdict(deque)
coach_commercial_last_topic_emitted = {}


# ============================================================
# 7. DEDUPLICACAO
# ============================================================

coach_commercial_seen_dispatch_ids = set()
coach_commercial_seen_dispatch_order = deque(
    maxlen=COACH_COMMERCIAL_MAX_SEEN
)


# ============================================================
# 8. ESTADO
# ============================================================

coach_commercial_running = False
coach_commercial_platform = None
coach_commercial_live_id = None
coach_commercial_subject = None
coach_commercial_started_at = None
coach_commercial_last_error = None


# ============================================================
# 9. ESTATISTICAS
# ============================================================

coach_commercial_stats = defaultdict(int)
coach_commercial_topic_counts = defaultdict(int)
coach_commercial_family_counts = defaultdict(int)
coach_commercial_category_counts = defaultdict(int)


# ============================================================
# 10. HORARIO
# ============================================================

def _cc_now_iso():
    return (
        datetime
        .now(COACH_COMMERCIAL_TIMEZONE)
        .isoformat()
    )


# ============================================================
# 11. FALLBACK SEMANTICO
# ============================================================

def _cc_fallback_info(category, subtype):
    if category == "commercial_question":
        return {
            "family": "commercial_information",
            "label": str(subtype or "pergunta comercial"),
            "commercial_role": "question",
            "sales_stage": "commercial_information",
            "answer_target": (
                "esclarecer a informacao comercial solicitada pelo usuario"
            ),
            "interpretation_target": (
                "o usuario fez uma pergunta comercial ainda nao especializada neste mapa"
            ),
        }

    if category == "buying_intent":
        return {
            "family": "buying_intent",
            "label": str(subtype or "intencao de compra"),
            "commercial_role": "buying_intent",
            "sales_stage": "purchase_intent",
            "answer_target": None,
            "interpretation_target": (
                "o usuario expressou um sinal de intencao de compra"
            ),
        }

    if category == "purchase_completed":
        return {
            "family": "conversion_report",
            "label": str(subtype or "compra ou pagamento relatado"),
            "commercial_role": "purchase_completed",
            "sales_stage": "purchase_reported",
            "answer_target": None,
            "interpretation_target": (
                "o usuario relatou uma acao de compra ou pagamento sem confirmacao transacional"
            ),
        }

    return {
        "family": "commercial_context",
        "label": str(subtype or "observacao comercial"),
        "commercial_role": "observation",
        "sales_stage": "commercial_context",
        "answer_target": None,
        "interpretation_target": (
            "o comentario contem contexto comercial relevante"
        ),
    }


# ============================================================
# 12. INFORMACAO DE UM TOPICO
# ============================================================

def _cc_topic_info(category, subtype):
    info = COACH_COMMERCIAL_TOPIC_MAP.get(
        (category, subtype)
    )

    if info is None:
        info = _cc_fallback_info(
            category,
            subtype,
        )

    return {
        "category": category,
        "subtype": subtype,
        "family": info.get("family"),
        "label": info.get("label"),
        "commercial_role": info.get("commercial_role"),
        "sales_stage": info.get("sales_stage"),
        "answer_target": info.get("answer_target"),
        "interpretation_target": info.get("interpretation_target"),
    }


# ============================================================
# 13. CHAVE DO TOPICO
# ============================================================

def _cc_topic_key(topic):
    return (
        str(topic.get("category"))
        + ":"
        + str(topic.get("subtype"))
    )


# ============================================================
# 14. REMOVER TOPICOS DUPLICADOS
# ============================================================

def _cc_unique_topics(topics):
    result = []
    seen = set()

    for topic in topics:
        key = (
            topic.get("category"),
            topic.get("subtype"),
        )

        if key in seen:
            continue

        seen.add(key)
        result.append(topic)

    return result


# ============================================================
# 15. EXTRAIR TOPICOS DO ENVELOPE DO DISPATCHER
# ============================================================

def _cc_topics_from_envelope(envelope):
    intents = (
        envelope.get("matched_intents")
        or []
    )

    accepted = {
        "commercial_question",
        "buying_intent",
        "purchase_completed",
        "commercial_observation",
    }

    topics = []

    for intent in intents:
        if not isinstance(intent, dict):
            continue

        category = intent.get("category")
        subtype = intent.get("subtype")

        if category not in accepted:
            continue

        topics.append(
            _cc_topic_info(
                category,
                subtype,
            )
        )

    topics = _cc_unique_topics(topics)

    # category_volume do Worker pode produzir um topico generico
    # category/None junto dos subtipos especificos. Quando isso
    # acontece, preservamos apenas os especificos dessa categoria.
    categories_with_specific = {
        topic.get("category")
        for topic in topics
        if topic.get("subtype") is not None
    }

    if categories_with_specific:
        topics = [
            topic
            for topic in topics
            if not (
                topic.get("category") in categories_with_specific
                and topic.get("subtype") is None
            )
        ]

    return topics


# ============================================================
# 16. EXTRAIR COMENTARIO ORIGINAL
# ============================================================

def _cc_comment_from_envelope(envelope):
    payload = (
        envelope.get("payload")
        or {}
    )

    comment = (
        payload.get("comment")
        or {}
    )

    return {
        "event_id": comment.get("event_id"),
        "user": comment.get("user"),
        "text": comment.get("text"),
        "display_time": comment.get("display_time"),
    }


# ============================================================
# 17. CLASSIFICACAO ORIGINAL
# ============================================================

def _cc_classification_from_envelope(envelope):
    payload = (
        envelope.get("payload")
        or {}
    )

    return (
        payload.get("classification")
        or {}
    )


# ============================================================
# 18. DEDUPLICACAO
# ============================================================

def _cc_seen(envelope):
    dispatch_id = envelope.get("dispatch_id")

    if not dispatch_id:
        return False

    if dispatch_id in coach_commercial_seen_dispatch_ids:
        coach_commercial_stats["duplicates_ignored"] += 1
        return True

    if (
        len(coach_commercial_seen_dispatch_order)
        >=
        COACH_COMMERCIAL_MAX_SEEN
    ):
        old = coach_commercial_seen_dispatch_order.popleft()
        coach_commercial_seen_dispatch_ids.discard(old)

    coach_commercial_seen_dispatch_order.append(dispatch_id)
    coach_commercial_seen_dispatch_ids.add(dispatch_id)

    return False


# ============================================================
# 19. FILA DE SAIDA
# ============================================================

def _cc_queue_output(output):
    global coach_commercial_output_queue

    if coach_commercial_output_queue is None:
        return False

    try:
        coach_commercial_output_queue.put_nowait(
            copy.deepcopy(output)
        )

    except asyncio.QueueFull:
        try:
            coach_commercial_output_queue.get_nowait()
        except Exception:
            pass

        coach_commercial_stats["dropped_output_queue_full"] += 1

        try:
            coach_commercial_output_queue.put_nowait(
                copy.deepcopy(output)
            )
        except Exception:
            return False

    coach_commercial_output_history.append(
        copy.deepcopy(output)
    )

    coach_commercial_stats["outputs_sent"] += 1

    return True


# ============================================================
# 20. PODAR JANELA TEMPORAL
# ============================================================

def _cc_prune_topic_window(queue, now=None):
    if now is None:
        now = time.time()

    limit = (
        now
        -
        COACH_COMMERCIAL_TOPIC_WINDOW_SECONDS
    )

    while queue and queue[0].get("timestamp", 0) < limit:
        queue.popleft()


# ============================================================
# 21. USUARIOS UNICOS
# ============================================================

def _cc_unique_users(records):
    users = {
        str(record.get("user")).strip()
        for record in records
        if record.get("user")
    }

    return len(users)


# ============================================================
# 22. MAIOR REPETICAO DE UMA MESMA PESSOA
# ============================================================

def _cc_max_repeat_by_one_user(records):
    counts = defaultdict(int)

    for record in records:
        user = record.get("user")
        if user:
            counts[str(user)] += 1

    if not counts:
        return 0

    return max(counts.values())


# ============================================================
# 23. EXEMPLOS
# ============================================================

def _cc_examples(records, limit=3):
    result = []
    seen = set()

    for record in reversed(records):
        text = (
            str(record.get("text") or "")
            .strip()
        )

        if not text:
            continue

        key = text.lower()
        if key in seen:
            continue

        seen.add(key)
        result.append(text)

        if len(result) >= limit:
            break

    result.reverse()
    return result


# ============================================================
# 24. PODE REEMITIR ATIVIDADE DO TOPICO?
# ============================================================

def _cc_can_emit_topic(
    topic_key,
    count,
    unique_users,
    now=None,
):
    if now is None:
        now = time.time()

    previous = coach_commercial_last_topic_emitted.get(
        topic_key
    )

    if previous is None:
        return True

    elapsed = (
        now
        -
        previous.get("time", 0)
    )

    if elapsed >= COACH_COMMERCIAL_TOPIC_FORCE_REEMIT_SECONDS:
        return True

    if elapsed < COACH_COMMERCIAL_TOPIC_REEMIT_SECONDS:
        # Crescimento forte pode reemitir mesmo antes do tempo.
        if count >= previous.get("count", 0) + 2:
            return True
        return False

    # Depois do cooldown minimo, so reemite se surgiu evidencia nova.
    if (
        count > previous.get("count", 0)
        or
        unique_users > previous.get("unique_users", 0)
    ):
        return True

    return False


# ============================================================
# 25. FLAGS COMERCIAIS
# ============================================================

def _cc_flags(topics):
    categories = {
        topic.get("category")
        for topic in topics
    }

    subtypes = {
        topic.get("subtype")
        for topic in topics
    }

    return {
        "has_commercial_question": "commercial_question" in categories,
        "has_buying_intent": "buying_intent" in categories,
        "has_direct_purchase_intent": "purchase_intent" in subtypes,
        "has_conditional_purchase_intent": (
            "conditional_purchase_intent" in subtypes
        ),
        "has_purchase_completed_report": (
            "purchase_completed" in categories
        ),
        "has_reported_purchase": "reported_purchase" in subtypes,
        "has_reported_payment": "reported_payment" in subtypes,
        "has_commercial_observation": (
            "commercial_observation" in categories
        ),
    }


# ============================================================
# 26. EMITIR ANALISE INDIVIDUAL
# ============================================================

def _cc_emit_individual_analysis(
    envelope,
    topics,
):
    classification = _cc_classification_from_envelope(
        envelope
    )

    comment = _cc_comment_from_envelope(
        envelope
    )

    response_needed = bool(
        envelope.get("requires_response")
        or
        classification.get("requires_response")
    )

    flags = _cc_flags(topics)

    output = {
        "output_id": uuid.uuid4().hex,
        "message_type": "commercial_analysis",
        "analysis_type": "individual",
        "source_coach": "commercial",
        "coach_version": COACH_COMMERCIAL_VERSION,
        "platform": envelope.get("platform"),
        "live_id": envelope.get("live_id"),
        "subject": envelope.get("subject"),
        "timestamp": envelope.get("timestamp", time.time()),
        "iso_time": envelope.get("iso_time") or _cc_now_iso(),
        "source_dispatch_id": envelope.get("dispatch_id"),
        "source_output_id": envelope.get("source_output_id"),
        "source_message_type": envelope.get("source_message_type"),
        "response_needed": response_needed,
        "is_question": bool(classification.get("is_question")),
        "is_request": bool(classification.get("is_request")),
        **flags,
        "topics": copy.deepcopy(topics),
        "comment": copy.deepcopy(comment),
        # O que precisa ser esclarecido pelo vendedor/sistema.
        "information_needs": [
            topic.get("answer_target")
            for topic in topics
            if topic.get("answer_target")
        ],
        # Significado comercial, sem decidir prioridade.
        "commercial_meanings": [
            topic.get("interpretation_target")
            for topic in topics
            if topic.get("interpretation_target")
        ],
    }

    if _cc_queue_output(output):
        coach_commercial_individual_history.append(
            copy.deepcopy(output)
        )
        coach_commercial_stats["individual_outputs"] += 1

    return output


# ============================================================
# 27. REGISTRAR OCORRENCIA LOCAL DO TOPICO
# ============================================================

def _cc_register_local_topic(
    envelope,
    topic,
):
    comment = _cc_comment_from_envelope(
        envelope
    )

    timestamp = envelope.get("timestamp")

    try:
        timestamp = float(timestamp)
    except Exception:
        timestamp = time.time()

    record = {
        "timestamp": timestamp,
        "dispatch_id": envelope.get("dispatch_id"),
        "source_output_id": envelope.get("source_output_id"),
        "user": comment.get("user"),
        "text": comment.get("text"),
    }

    topic_key = _cc_topic_key(topic)
    queue = coach_commercial_topic_windows[topic_key]

    queue.append(record)
    _cc_prune_topic_window(queue)

    coach_commercial_topic_counts[
        topic.get("subtype")
    ] += 1

    coach_commercial_family_counts[
        topic.get("family")
    ] += 1

    coach_commercial_category_counts[
        topic.get("category")
    ] += 1

    return list(queue)


# ============================================================
# 28. EMITIR ATIVIDADE LOCAL
# ============================================================

def _cc_maybe_emit_local_topic_signal(
    envelope,
    topic,
    records,
):
    if not records:
        return None

    count = len(records)

    if count < COACH_COMMERCIAL_LOCAL_TOPIC_THRESHOLD:
        return None

    unique_users = _cc_unique_users(records)
    max_same_user = _cc_max_repeat_by_one_user(records)
    topic_key = _cc_topic_key(topic)
    now = time.time()

    if not _cc_can_emit_topic(
        topic_key,
        count,
        unique_users,
        now,
    ):
        return None

    output = {
        "output_id": uuid.uuid4().hex,
        "message_type": "commercial_topic_signal",
        "analysis_type": "local_activity",
        "source_coach": "commercial",
        "coach_version": COACH_COMMERCIAL_VERSION,
        "platform": envelope.get("platform"),
        "live_id": envelope.get("live_id"),
        "subject": envelope.get("subject"),
        "timestamp": now,
        "iso_time": _cc_now_iso(),
        "topic": copy.deepcopy(topic),
        "topic_key": topic_key,
        "response_needed": (
            topic.get("category") == "commercial_question"
        ),
        "evidence": {
            "source": "coach_commercial_local_window",
            "window_seconds": COACH_COMMERCIAL_TOPIC_WINDOW_SECONDS,
            "count": count,
            "unique_users": unique_users,
            "max_repetitions_by_one_user": max_same_user,
            "repeated_by_same_user": max_same_user >= 2,
            "multiple_users": unique_users >= 2,
            "examples": _cc_examples(records),
            "source_output_ids": [
                record.get("source_output_id")
                for record in records
                if record.get("source_output_id")
            ],
        },
    }

    if _cc_queue_output(output):
        coach_commercial_topic_signal_history.append(
            copy.deepcopy(output)
        )

        coach_commercial_stats["local_topic_signals"] += 1

        coach_commercial_last_topic_emitted[
            topic_key
        ] = {
            "time": now,
            "count": count,
            "unique_users": unique_users,
        }

    return output


# ============================================================
# 29. TRANSFORMAR TENDENCIA DO WORKER
#
# A tendencia ja foi calculada pelo Worker Comentarios.
# Ela nao e adicionada novamente a janela local.
# ============================================================

def _cc_emit_worker_trend(
    envelope,
    topics,
):
    payload = (
        envelope.get("payload")
        or {}
    )

    output = {
        "output_id": uuid.uuid4().hex,
        "message_type": "commercial_topic_signal",
        "analysis_type": "worker_trend",
        "source_coach": "commercial",
        "coach_version": COACH_COMMERCIAL_VERSION,
        "platform": envelope.get("platform"),
        "live_id": envelope.get("live_id"),
        "subject": envelope.get("subject"),
        "timestamp": envelope.get("timestamp", time.time()),
        "iso_time": envelope.get("iso_time") or _cc_now_iso(),
        "source_dispatch_id": envelope.get("dispatch_id"),
        "source_output_id": envelope.get("source_output_id"),
        "trend_kind": payload.get("trend_kind"),
        "topics": copy.deepcopy(topics),
        "response_needed": bool(
            envelope.get("requires_response")
        ),
        "evidence": {
            "source": "worker_coach_comments",
            "window_seconds": payload.get("window_seconds"),
            "count": payload.get("count"),
            "unique_users": payload.get("unique_users"),
            "strength": payload.get("strength"),
            "examples": copy.deepcopy(
                payload.get("examples") or []
            ),
        },
    }

    if _cc_queue_output(output):
        coach_commercial_topic_signal_history.append(
            copy.deepcopy(output)
        )
        coach_commercial_stats["worker_trend_signals"] += 1

    return output


# ============================================================
# 30. PREVIEW PURO
#
# Nao altera estado. Util para teste.
# ============================================================

def coach_commercial_preview_envelope(envelope):
    topics = _cc_topics_from_envelope(
        envelope
    )

    payload = (
        envelope.get("payload")
        or {}
    )

    source_message_type = envelope.get(
        "source_message_type"
    )

    result = {
        "destination": envelope.get("destination"),
        "source_message_type": source_message_type,
        "topics": topics,
        "requires_response": envelope.get("requires_response"),
        "flags": _cc_flags(topics),
    }

    if source_message_type == "classified_comment":
        classification = (
            payload.get("classification")
            or {}
        )

        result["comment"] = _cc_comment_from_envelope(
            envelope
        )
        result["is_question"] = classification.get(
            "is_question"
        )
        result["is_request"] = classification.get(
            "is_request"
        )

    elif source_message_type == "comment_trend":
        result["trend"] = {
            "trend_kind": payload.get("trend_kind"),
            "count": payload.get("count"),
            "unique_users": payload.get("unique_users"),
            "strength": payload.get("strength"),
        }

    return result


# ============================================================
# 31. PROCESSAR ENVELOPE
# ============================================================

def coach_commercial_process_envelope(envelope):
    global coach_commercial_platform
    global coach_commercial_live_id
    global coach_commercial_subject

    if not isinstance(envelope, dict):
        return []

    if envelope.get("destination") != "commercial":
        coach_commercial_stats["wrong_destination"] += 1
        return []

    if _cc_seen(envelope):
        return []

    coach_commercial_stats["received"] += 1

    coach_commercial_platform = (
        envelope.get("platform")
        or
        coach_commercial_platform
    )

    coach_commercial_live_id = (
        envelope.get("live_id")
        or
        coach_commercial_live_id
    )

    coach_commercial_subject = (
        envelope.get("subject")
        or
        coach_commercial_subject
    )

    topics = _cc_topics_from_envelope(
        envelope
    )

    if not topics:
        coach_commercial_stats["without_commercial_topic"] += 1
        return []

    source_message_type = envelope.get(
        "source_message_type"
    )

    generated = []

    if source_message_type == "classified_comment":
        coach_commercial_stats["individual_received"] += 1

        individual_output = _cc_emit_individual_analysis(
            envelope,
            topics,
        )

        if individual_output is not None:
            generated.append(individual_output)

        for topic in topics:
            records = _cc_register_local_topic(
                envelope,
                topic,
            )

            local_signal = _cc_maybe_emit_local_topic_signal(
                envelope,
                topic,
                records,
            )

            if local_signal is not None:
                generated.append(local_signal)

    elif source_message_type == "comment_trend":
        coach_commercial_stats["trends_received"] += 1

        trend_output = _cc_emit_worker_trend(
            envelope,
            topics,
        )

        if trend_output is not None:
            generated.append(trend_output)

    else:
        coach_commercial_stats["unknown_source_message_type"] += 1

    return generated


# ============================================================
# 32. RESET PARA NOVA LIVE
# ============================================================

def coach_commercial_reset(platform=None):
    global coach_commercial_output_queue
    global coach_commercial_running
    global coach_commercial_platform
    global coach_commercial_live_id
    global coach_commercial_subject
    global coach_commercial_started_at
    global coach_commercial_last_error

    coach_commercial_output_queue = asyncio.Queue(
        maxsize=COACH_COMMERCIAL_OUTPUT_QUEUE_LIMIT
    )

    coach_commercial_output_history.clear()
    coach_commercial_individual_history.clear()
    coach_commercial_topic_signal_history.clear()
    coach_commercial_topic_windows.clear()
    coach_commercial_last_topic_emitted.clear()
    coach_commercial_seen_dispatch_ids.clear()
    coach_commercial_seen_dispatch_order.clear()
    coach_commercial_stats.clear()
    coach_commercial_topic_counts.clear()
    coach_commercial_family_counts.clear()
    coach_commercial_category_counts.clear()

    coach_commercial_running = False
    coach_commercial_platform = platform
    coach_commercial_live_id = None
    coach_commercial_subject = None
    coach_commercial_started_at = time.time()
    coach_commercial_last_error = None


# ============================================================
# 33. LOOP PRINCIPAL
# ============================================================

async def coach_commercial_loop():
    global coach_commercial_running
    global coach_commercial_last_error

    coach_commercial_running = True

    try:
        while True:
            envelope = None

            try:
                envelope = await (
                    comment_dispatcher_next_commercial(
                        timeout=0.5
                    )
                )

            except asyncio.TimeoutError:
                envelope = None

            except asyncio.CancelledError:
                raise

            except Exception as e:
                coach_commercial_last_error = (
                    f"{type(e).__name__}: {e}"
                )
                await asyncio.sleep(0.1)

            if envelope is not None:
                try:
                    coach_commercial_process_envelope(
                        envelope
                    )
                except Exception as e:
                    coach_commercial_last_error = (
                        f"{type(e).__name__}: {e}"
                    )
                continue

            dispatcher_running = bool(
                globals().get(
                    "comment_dispatcher_running",
                    False,
                )
            )

            source_queue = globals().get(
                "comment_dispatcher_commercial_queue"
            )

            source_empty = (
                source_queue is None
                or
                source_queue.empty()
            )

            if (
                not live_engine_is_running()
                and
                not dispatcher_running
                and
                source_empty
            ):
                break

    finally:
        coach_commercial_running = False


# ============================================================
# 34. SUPERVISOR
# ============================================================

async def coach_commercial_supervisor(platform):
    global coach_commercial_last_error

    start = time.time()

    while True:
        if time.time() - start > 30:
            coach_commercial_last_error = (
                "Comment Dispatcher nao ficou pronto dentro de 30 segundos."
            )
            return

        try:
            ready = (
                live_engine_is_running()
                and
                live_engine_platform() == platform
                and
                globals().get(
                    "comment_dispatcher_commercial_queue"
                ) is not None
                and
                bool(
                    globals().get(
                        "comment_dispatcher_running",
                        False,
                    )
                )
            )
        except Exception:
            ready = False

        if ready:
            break

        await asyncio.sleep(0.05)

    await coach_commercial_loop()


# ============================================================
# 35. API PARA O FUTURO CONTEXT FUSION
# ============================================================

async def coach_commercial_next_output(timeout=None):
    queue = coach_commercial_output_queue

    if queue is None:
        return None

    if timeout is None:
        return await queue.get()

    return await asyncio.wait_for(
        queue.get(),
        timeout=timeout,
    )


# ============================================================
# 36. OUTPUTS RECENTES
# ============================================================

def coach_commercial_recent_outputs(limit=20):
    if limit <= 0:
        return []

    return list(
        coach_commercial_output_history
    )[-limit:]


def coach_commercial_recent_individual(limit=20):
    if limit <= 0:
        return []

    return list(
        coach_commercial_individual_history
    )[-limit:]


def coach_commercial_recent_topic_signals(limit=20):
    if limit <= 0:
        return []

    return list(
        coach_commercial_topic_signal_history
    )[-limit:]


# ============================================================
# 37. TOPICOS ATIVOS
# ============================================================

def coach_commercial_active_topics():
    now = time.time()
    result = []

    for topic_key, queue in coach_commercial_topic_windows.items():
        _cc_prune_topic_window(
            queue,
            now,
        )

        if not queue:
            continue

        category, _, subtype = topic_key.partition(":")
        topic = _cc_topic_info(
            category,
            subtype if subtype != "None" else None,
        )

        records = list(queue)

        result.append({
            "topic_key": topic_key,
            "topic": topic,
            "count": len(records),
            "unique_users": _cc_unique_users(records),
            "max_repetitions_by_one_user": (
                _cc_max_repeat_by_one_user(records)
            ),
            "examples": _cc_examples(records),
        })

    result.sort(
        key=lambda item: (
            item.get("count", 0),
            item.get("unique_users", 0),
        ),
        reverse=True,
    )

    return result


# ============================================================
# 38. DIAGNOSTICO EM DICIONARIO
# ============================================================

def coach_commercial_diagnostic():
    source_queue = globals().get(
        "comment_dispatcher_commercial_queue"
    )

    source_queue_size = (
        source_queue.qsize()
        if source_queue is not None
        else None
    )

    output_queue_size = (
        coach_commercial_output_queue.qsize()
        if coach_commercial_output_queue is not None
        else None
    )

    return {
        "version": COACH_COMMERCIAL_VERSION,
        "running": coach_commercial_running,
        "platform": coach_commercial_platform,
        "live_id": coach_commercial_live_id,
        "subject": coach_commercial_subject,
        "source_queue_waiting": source_queue_size,
        "output_queue_waiting": output_queue_size,
        "last_error": coach_commercial_last_error,
        "stats": dict(coach_commercial_stats),
        "category_counts": dict(coach_commercial_category_counts),
        "topic_counts": dict(coach_commercial_topic_counts),
        "family_counts": dict(coach_commercial_family_counts),
        "active_topics": coach_commercial_active_topics(),
    }


# ============================================================
# 39. DIAGNOSTICO PRINCIPAL
# ============================================================

def mostrar_coach_comercial():
    d = coach_commercial_diagnostic()
    s = d.get("stats", {})
    categories = d.get("category_counts", {})

    print("=" * 76)
    print("AGCN - COACH COMERCIAL V1")
    print("Versao:", COACH_COMMERCIAL_VERSION)
    print("=" * 76)
    print("Rodando:", d.get("running"))
    print("Plataforma:", d.get("platform"))
    print("Live ID:", d.get("live_id"))
    print("Subject:", d.get("subject"))
    print()

    print("ENTRADA")
    print(
        " Recebidos do Dispatcher:",
        s.get("received", 0),
    )
    print(
        " Comentarios individuais:",
        s.get("individual_received", 0),
    )
    print(
        " Tendencias recebidas:",
        s.get("trends_received", 0),
    )
    print(
        " Duplicados ignorados:",
        s.get("duplicates_ignored", 0),
    )
    print(
        " Sem topico comercial:",
        s.get("without_commercial_topic", 0),
    )
    print()

    print("TIPOS COMERCIAIS PROCESSADOS")
    print(
        " Perguntas comerciais:",
        categories.get("commercial_question", 0),
    )
    print(
        " Intencoes de compra:",
        categories.get("buying_intent", 0),
    )
    print(
        " Compras/pagamentos relatados:",
        categories.get("purchase_completed", 0),
    )
    print(
        " Observacoes comerciais:",
        categories.get("commercial_observation", 0),
    )
    print()

    print("SAIDA PARA FUTURO CONTEXT FUSION")
    print(
        " Outputs enviados:",
        s.get("outputs_sent", 0),
    )
    print(
        " Analises individuais:",
        s.get("individual_outputs", 0),
    )
    print(
        " Sinais locais de topico:",
        s.get("local_topic_signals", 0),
    )
    print(
        " Tendencias do Worker transformadas:",
        s.get("worker_trend_signals", 0),
    )
    print(
        " Outputs descartados por fila cheia:",
        s.get("dropped_output_queue_full", 0),
    )
    print(
        " Aguardando futuro Context Fusion:",
        d.get("output_queue_waiting"),
    )
    print()

    print("Erro:", d.get("last_error"))
    print()

    print(
        "TOPICOS ATIVOS (ultimos "
        f"{COACH_COMMERCIAL_TOPIC_WINDOW_SECONDS}s):"
    )

    active = d.get("active_topics", [])

    if not active:
        print("  nenhum")
    else:
        for item in active[:10]:
            topic = item.get("topic", {})
            print()
            print(
                " ",
                topic.get("label"),
                "|",
                topic.get("subtype"),
            )
            print(
                "      comentarios:",
                item.get("count"),
                "| usuarios:",
                item.get("unique_users"),
                "| max mesmo usuario:",
                item.get("max_repetitions_by_one_user"),
            )
            print(
                "      exemplos:",
                item.get("examples"),
            )

    print()
    print("ULTIMOS OUTPUTS:")

    outputs = coach_commercial_recent_outputs(8)

    if not outputs:
        print("  nenhum ainda")
        return

    for output in outputs:
        print()
        print(
            " ",
            output.get("message_type"),
            "|",
            output.get("analysis_type"),
        )

        if output.get("message_type") == "commercial_analysis":
            comment = output.get("comment", {})
            topics = output.get("topics", [])
            print(
                "      ",
                comment.get("user"),
                ":",
                comment.get("text"),
            )
            print(
                "      topicos:",
                [
                    topic.get("subtype")
                    for topic in topics
                ],
            )
            print(
                "      precisa resposta:",
                output.get("response_needed"),
            )
            if output.get("has_buying_intent"):
                print("      sinal: intencao de compra")
            if output.get("has_purchase_completed_report"):
                print("      sinal: compra/pagamento relatado")

        else:
            topic = output.get("topic")
            topics = output.get("topics") or []

            if topic:
                subtypes = [topic.get("subtype")]
            else:
                subtypes = [
                    t.get("subtype")
                    for t in topics
                ]

            print(
                "      topicos:",
                subtypes,
            )

            evidence = output.get("evidence", {})
            print(
                "      count:",
                evidence.get("count"),
                "| usuarios:",
                evidence.get("unique_users"),
            )


# ============================================================
# 40. DIAGNOSTICO DETALHADO
# ============================================================

def mostrar_coach_comercial_detalhado(limit=20):
    outputs = coach_commercial_recent_individual(limit)

    print("=" * 76)
    print("AGCN - COACH COMERCIAL V1 - ANALISES INDIVIDUAIS")
    print("=" * 76)

    if not outputs:
        print("Nenhuma analise individual ainda.")
        return

    for output in outputs:
        comment = output.get("comment", {})

        print()
        print(
            comment.get("user"),
            ":",
            comment.get("text"),
        )

        print(
            "resposta necessaria:",
            output.get("response_needed"),
        )

        print(
            "flags:",
            {
                "commercial_question": output.get("has_commercial_question"),
                "buying_intent": output.get("has_buying_intent"),
                "conditional_intent": output.get("has_conditional_purchase_intent"),
                "purchase_completed": output.get("has_purchase_completed_report"),
                "commercial_observation": output.get("has_commercial_observation"),
            },
        )

        for topic in output.get("topics", []):
            print(
                " -",
                topic.get("category"),
                "/",
                topic.get("subtype"),
                "|",
                topic.get("label"),
            )
            print(
                "   papel:",
                topic.get("commercial_role"),
                "| etapa:",
                topic.get("sales_stage"),
            )
            if topic.get("answer_target"):
                print(
                    "   necessidade:",
                    topic.get("answer_target"),
                )
            print(
                "   significado:",
                topic.get("interpretation_target"),
            )


# ============================================================
# 41. ESTADO SIMPLES
# ============================================================

def coach_commercial_is_running():
    return bool(coach_commercial_running)


# ============================================================
# 42. VALIDAR COBERTURA DA TAXONOMIA
# ============================================================

def coach_commercial_validate_taxonomy():
    mapped = set(COACH_COMMERCIAL_TOPIC_MAP.keys())
    expected = set(COACH_COMMERCIAL_WORKER_V15_TOPICS)

    missing = sorted(
        expected - mapped
    )

    extra = sorted(
        mapped - expected
    )

    return {
        "worker_version": "1.5",
        "coach_version": COACH_COMMERCIAL_VERSION,
        "expected_commercial_topics": len(expected),
        "mapped_commercial_topics": len(expected & mapped),
        "missing": missing,
        "extra": extra,
        "ok": not missing,
    }


# ============================================================
# 43. TESTE INTERNO
#
# Teste puro: nao consome filas, nao inicia LIVE e nao altera
# o estado operacional do Coach.
# ============================================================

def testar_coach_comercial_v1(verbose=False):
    print("=" * 76)
    print("TESTE INTERNO - COACH COMERCIAL V1")
    print("=" * 76)

    approved = 0
    failures = []

    taxonomy = coach_commercial_validate_taxonomy()

    def make_envelope(
        intents,
        text="teste",
        source_message_type="classified_comment",
        trend_payload=None,
    ):
        payload = {
            "message_type": source_message_type,
            "classification": {
                "is_question": any(
                    i.get("category") == "commercial_question"
                    for i in intents
                ),
                "is_request": False,
                "requires_response": any(
                    bool(i.get("requires_response"))
                    for i in intents
                ),
                "intents": copy.deepcopy(intents),
            },
            "comment": {
                "event_id": "test-event",
                "user": "teste",
                "text": text,
                "display_time": "00:00:00",
            },
        }

        if trend_payload:
            payload.update(trend_payload)

        return {
            "dispatch_id": uuid.uuid4().hex,
            "destination": "commercial",
            "source_output_id": uuid.uuid4().hex,
            "source_message_type": source_message_type,
            "platform": "tiktok",
            "live_id": "test-live",
            "subject": "@teste",
            "timestamp": time.time(),
            "iso_time": _cc_now_iso(),
            "matched_categories": list(dict.fromkeys(
                i.get("category")
                for i in intents
                if i.get("category")
            )),
            "matched_subtypes": list(dict.fromkeys(
                i.get("subtype")
                for i in intents
                if i.get("subtype")
            )),
            "matched_intents": copy.deepcopy(intents),
            "requires_response": any(
                bool(i.get("requires_response"))
                for i in intents
            ),
            "payload": payload,
        }

    def check(name, condition, details=None):
        nonlocal approved

        if condition:
            approved += 1
            if verbose:
                print("OK:", name)
        else:
            failures.append({
                "name": name,
                "details": details,
            })
            print("FALHA:", name)
            if details is not None:
                print("  ", details)

    # --------------------------------------------------------
    # 20 topicos oficiais Worker V1.5 -> Coach Comercial V1
    # --------------------------------------------------------
    for category, subtype in sorted(
        COACH_COMMERCIAL_WORKER_V15_TOPICS
    ):
        requires_response = (
            category == "commercial_question"
        )

        envelope = make_envelope(
            [{
                "category": category,
                "subtype": subtype,
                "requires_response": requires_response,
            }],
            text=f"teste {category}/{subtype}",
        )

        preview = coach_commercial_preview_envelope(
            envelope
        )

        topics = preview.get("topics", [])

        check(
            f"mapeia {category}/{subtype}",
            len(topics) == 1
            and topics[0].get("category") == category
            and topics[0].get("subtype") == subtype
            and bool(topics[0].get("family"))
            and bool(topics[0].get("label")),
            topics,
        )

    # 21. Multi-intencao comercial ampla.
    multi = make_envelope(
        [
            {"category": "commercial_question", "subtype": "price", "requires_response": True},
            {"category": "commercial_question", "subtype": "promotion_coupon", "requires_response": True},
            {"category": "commercial_question", "subtype": "shipping", "requires_response": True},
            {"category": "buying_intent", "subtype": "conditional_purchase_intent", "requires_response": False},
        ],
        text="se tiver cupom e frete gratis eu pego, quanto fica?",
    )
    multi_preview = coach_commercial_preview_envelope(multi)
    multi_pairs = {
        (t.get("category"), t.get("subtype"))
        for t in multi_preview.get("topics", [])
    }
    check(
        "multi-intencao comercial preserva 4 topicos",
        multi_pairs == {
            ("commercial_question", "price"),
            ("commercial_question", "promotion_coupon"),
            ("commercial_question", "shipping"),
            ("buying_intent", "conditional_purchase_intent"),
        },
        multi_pairs,
    )

    # 22. Flag de intencao condicionada.
    check(
        "flag conditional_purchase_intent",
        bool(
            multi_preview.get("flags", {}).get(
                "has_conditional_purchase_intent"
            )
        ),
        multi_preview.get("flags"),
    )

    # 23. Compra relatada nao vira confirmacao real; apenas flag.
    purchase = make_envelope(
        [{
            "category": "purchase_completed",
            "subtype": "reported_purchase",
            "requires_response": False,
        }],
        text="comprei",
    )
    purchase_preview = coach_commercial_preview_envelope(purchase)
    check(
        "reported_purchase gera flag de compra relatada",
        bool(
            purchase_preview.get("flags", {}).get(
                "has_reported_purchase"
            )
        ),
        purchase_preview.get("flags"),
    )

    # 24. Observacao contextual nao exige resposta.
    context = make_envelope(
        [{
            "category": "commercial_observation",
            "subtype": "contextual_product_reference",
            "requires_response": False,
        }],
        text="o ipad rosa 128gb",
    )
    context_preview = coach_commercial_preview_envelope(context)
    check(
        "contextual_product_reference preservado sem resposta",
        context_preview.get("requires_response") is False
        and context_preview.get("topics", [])[0].get("subtype")
        == "contextual_product_reference",
        context_preview,
    )

    # 25. Tendencia simples de preco.
    trend = make_envelope(
        [{
            "category": "commercial_question",
            "subtype": "price",
            "requires_response": True,
        }],
        source_message_type="comment_trend",
        trend_payload={
            "trend_kind": "topic_repeat",
            "category": "commercial_question",
            "subtype": "price",
            "count": 3,
            "unique_users": 3,
            "strength": 0.8,
            "window_seconds": 60,
            "examples": ["qual valor?", "preco?"],
        },
    )
    trend_preview = coach_commercial_preview_envelope(trend)
    check(
        "comment_trend de preco interpretado",
        trend_preview.get("source_message_type") == "comment_trend"
        and trend_preview.get("trend", {}).get("count") == 3
        and trend_preview.get("topics", [])[0].get("subtype") == "price",
        trend_preview,
    )

    # 26. category_volume com generico + especificos remove generico.
    category_volume = make_envelope(
        [
            {"category": "commercial_question", "subtype": None, "requires_response": True},
            {"category": "commercial_question", "subtype": "price", "requires_response": True},
            {"category": "commercial_question", "subtype": "shipping", "requires_response": True},
        ],
        source_message_type="comment_trend",
        trend_payload={
            "trend_kind": "category_volume",
            "category": "commercial_question",
            "subtype": None,
            "subtypes": ["price", "shipping"],
            "count": 5,
            "unique_users": 4,
            "strength": 0.9,
            "window_seconds": 60,
        },
    )
    cv_preview = coach_commercial_preview_envelope(category_volume)
    cv_subtypes = {
        t.get("subtype")
        for t in cv_preview.get("topics", [])
    }
    check(
        "category_volume remove topico generico quando ha especificos",
        cv_subtypes == {"price", "shipping"},
        cv_subtypes,
    )

    # 27. Fallback nao descarta subtipo comercial futuro desconhecido.
    unknown = make_envelope(
        [{
            "category": "commercial_question",
            "subtype": "future_commercial_subtype",
            "requires_response": True,
        }],
        text="teste futuro",
    )
    unknown_preview = coach_commercial_preview_envelope(unknown)
    unknown_topics = unknown_preview.get("topics", [])
    check(
        "fallback preserva subtipo comercial futuro",
        len(unknown_topics) == 1
        and unknown_topics[0].get("subtype") == "future_commercial_subtype"
        and bool(unknown_topics[0].get("answer_target")),
        unknown_topics,
    )

    total = approved + len(failures)

    print()
    print("=" * 76)
    print("RESUMO DO TESTE COACH COMERCIAL V1")
    print("=" * 76)
    print(
        "Topicos comerciais Worker V1.5 mapeados:",
        f"{taxonomy['mapped_commercial_topics']} / "
        f"{taxonomy['expected_commercial_topics']}",
    )
    print("Aprovados:", approved)
    print("Falhas:", len(failures))
    print("Total:", total)

    result = {
        "approved": approved,
        "failed": len(failures),
        "total": total,
        "ok": (
            len(failures) == 0
            and taxonomy.get("ok") is True
        ),
        "taxonomy": taxonomy,
        "failures": failures,
    }

    print(result)
    return result


# ============================================================
# 44. FINALIZAR TASK
# ============================================================

async def _coach_commercial_finish_task(task):
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
# 45. DESCOBRIR BASE
#
# A funcao atual deve conter, nesta altura do Colab:
#
# Live Engine
# + Coach Armazenamento
# + Worker Comentarios V1.5
# + Comment Dispatcher V1
# + Coach Produto V1.1
#
# Agora adicionamos:
#
# + Coach Comercial V1
#
# Se esta propria celula for reexecutada antes de carregar
# wrappers posteriores, nao empilhamos dois Coaches Comerciais.
# Se Worker Audiencia/Interface ja tiverem sido carregados e
# voce quiser substituir esta versao, reinicie o runtime e
# execute os modulos novamente na ordem oficial.
# ============================================================

_current_commercial_shopee_executor = (
    executar_shopee_com_live_engine
)

if getattr(
    _current_commercial_shopee_executor,
    "_agcn_commercial_coach_wrapper",
    False,
):
    _coach_commercial_base_shopee = (
        _current_commercial_shopee_executor
        ._agcn_commercial_coach_base
    )
else:
    _coach_commercial_base_shopee = (
        _current_commercial_shopee_executor
    )


_current_commercial_tiktok_executor = (
    executar_tiktok_com_live_engine
)

if getattr(
    _current_commercial_tiktok_executor,
    "_agcn_commercial_coach_wrapper",
    False,
):
    _coach_commercial_base_tiktok = (
        _current_commercial_tiktok_executor
        ._agcn_commercial_coach_base
    )
else:
    _coach_commercial_base_tiktok = (
        _current_commercial_tiktok_executor
    )


# ============================================================
# 46. SHOPEE
# ============================================================

async def executar_shopee_com_live_engine():
    coach_commercial_reset(
        "shopee"
    )

    commercial_task = asyncio.create_task(
        coach_commercial_supervisor(
            "shopee"
        )
    )

    try:
        return await _coach_commercial_base_shopee()

    finally:
        await _coach_commercial_finish_task(
            commercial_task
        )


executar_shopee_com_live_engine._agcn_commercial_coach_wrapper = True
executar_shopee_com_live_engine._agcn_commercial_coach_base = (
    _coach_commercial_base_shopee
)
executar_shopee_com_live_engine._agcn_commercial_coach_version = (
    COACH_COMMERCIAL_VERSION
)


# ============================================================
# 47. TIKTOK
# ============================================================

async def executar_tiktok_com_live_engine(username):
    coach_commercial_reset(
        "tiktok"
    )

    commercial_task = asyncio.create_task(
        coach_commercial_supervisor(
            "tiktok"
        )
    )

    try:
        return await _coach_commercial_base_tiktok(
            username
        )

    finally:
        await _coach_commercial_finish_task(
            commercial_task
        )


executar_tiktok_com_live_engine._agcn_commercial_coach_wrapper = True
executar_tiktok_com_live_engine._agcn_commercial_coach_base = (
    _coach_commercial_base_tiktok
)
executar_tiktok_com_live_engine._agcn_commercial_coach_version = (
    COACH_COMMERCIAL_VERSION
)


# ============================================================
# 48. PRONTO
# ============================================================

print("AGCN Coach Comercial V1 carregado.")
print("Entrada: Comment Dispatcher V1 -> fila commercial.")
print("Analisa taxonomia comercial completa do Worker Comentarios V1.5:")
print("1. perguntas comerciais")
print("2. intencoes de compra")
print("3. compras e pagamentos relatados")
print("4. observacoes comerciais")
print("5. repeticao do mesmo assunto")
print("6. repeticao da mesma pessoa x varias pessoas")
print("Saidas:")
print("1. commercial_analysis")
print("2. commercial_topic_signal")
print("Destino futuro: Context Fusion.")
print("Sem prioridade, sem Storage e sem Interface.")
