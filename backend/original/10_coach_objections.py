# ============================================================
# AGCN - COACH OBJECOES/POS-COMPRA V1
#
# ARQUITETURA:
#
# Worker Coach Comentarios V1.5
#               ↓
#      Comment Dispatcher V1
#               ↓
#       fila "objections"
#               ↓
#  COACH OBJECOES/POS-COMPRA V1
#               ↓
#       futuro Context Fusion
#
#
# RESPONSABILIDADES:
#
# - receber somente eventos roteados para objections
# - interpretar a classificacao ja feita pelo Worker
# - organizar objecoes de pre-compra por assunto
# - organizar problemas e solicitacoes de pos-compra
# - preservar barreiras de compra futuras sem quebrar compatibilidade
# - identificar qual ponto precisa ser resolvido/esclarecido
# - acompanhar repeticao por assunto
# - diferenciar repeticao da mesma pessoa de varias pessoas
# - transformar comment_trend em sinal especializado
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
# - nao acessa Coach Comercial
# - nao responde o cliente
# - nao inventa politica de troca, devolucao, reembolso ou garantia
# - nao confirma status de pedido, entrega, cobranca ou estorno
# - nao trata relato do usuario como comprovacao transacional
#
#
# SAIDAS:
#
# 1. objections_analysis
#    Analise de uma objecao/problema individual.
#
# 2. objections_topic_signal
#    Movimento/repeticao de um assunto de objecao/pos-compra.
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

_CO_REQUIRED = [
    "comment_dispatcher_next_objections",
    "live_engine_is_running",
    "live_engine_platform",
    "executar_shopee_com_live_engine",
    "executar_tiktok_com_live_engine",
]

_CO_MISSING = [
    name
    for name in _CO_REQUIRED
    if name not in globals()
]

if _CO_MISSING:
    raise RuntimeError(
        "Execute primeiro o Comment Dispatcher V1. Faltando: "
        + ", ".join(_CO_MISSING)
    )


# ============================================================
# 2. CONFIGURACAO
# ============================================================

COACH_OBJECTIONS_VERSION = "1.0"

COACH_OBJECTIONS_TIMEZONE = ZoneInfo(
    "America/Araguaina"
)

COACH_OBJECTIONS_OUTPUT_QUEUE_LIMIT = 5000
COACH_OBJECTIONS_MAX_HISTORY = 5000
COACH_OBJECTIONS_MAX_SEEN = 20000

# Janela local independente das tendencias do Worker.
COACH_OBJECTIONS_TOPIC_WINDOW_SECONDS = 60

# Comeca a produzir sinal local com 2 ocorrencias.
COACH_OBJECTIONS_LOCAL_TOPIC_THRESHOLD = 2

# Controle de reemissao do mesmo assunto.
COACH_OBJECTIONS_TOPIC_REEMIT_SECONDS = 15
COACH_OBJECTIONS_TOPIC_FORCE_REEMIT_SECONDS = 40


# ============================================================
# 3. MAPA SEMANTICO - OBJECOES E POS-COMPRA
#
# O Worker Comentarios V1.5 ja classificou categoria/subtipo.
# Este Coach acrescenta:
#
# - family
# - label
# - issue_role
# - journey_stage
# - resolution_target
# - interpretation_target
#
# Nenhum desses campos e prioridade.
# Nenhum deles e uma resposta pronta ao cliente.
# ============================================================

COACH_OBJECTIONS_TOPIC_MAP = {
    # --------------------------------------------------------
    # OBJECOES / RESISTENCIAS DE PRE-COMPRA
    # --------------------------------------------------------
    ("objection", "purchase_hesitation"): {
        "family": "purchase_hesitation",
        "label": "hesitacao de compra",
        "issue_role": "objection",
        "journey_stage": "pre_purchase",
        "resolution_target": (
            "entender o motivo da hesitacao e esclarecer o ponto que impede o usuario de avancar"
        ),
        "interpretation_target": (
            "o usuario demonstra hesitacao ou recuo no processo de compra"
        ),
    },
    ("objection", "price_objection"): {
        "family": "price_resistance",
        "label": "objecao de preco",
        "issue_role": "objection",
        "journey_stage": "pre_purchase",
        "resolution_target": (
            "tratar a resistencia relacionada a preco ou percepcao de valor sem inventar desconto"
        ),
        "interpretation_target": (
            "o preco ou a percepcao de valor esta dificultando a compra"
        ),
    },
    ("objection", "shipping_objection"): {
        "family": "shipping_resistance",
        "label": "objecao de frete",
        "issue_role": "objection",
        "journey_stage": "pre_purchase",
        "resolution_target": (
            "esclarecer a resistencia relacionada ao custo ou condicao de frete sem inventar beneficio"
        ),
        "interpretation_target": (
            "o frete esta sendo percebido como barreira para a compra"
        ),
    },
    ("objection", "trust_objection"): {
        "family": "trust_and_security",
        "label": "objecao de confianca ou seguranca",
        "issue_role": "objection",
        "journey_stage": "pre_purchase",
        "resolution_target": (
            "esclarecer duvidas de confianca e seguranca usando apenas informacoes verificaveis"
        ),
        "interpretation_target": (
            "o usuario demonstra receio de golpe, entrega ou confiabilidade da compra"
        ),
    },
    ("objection", "authenticity_objection"): {
        "family": "authenticity_trust",
        "label": "objecao de autenticidade",
        "issue_role": "objection",
        "journey_stage": "pre_purchase",
        "resolution_target": (
            "esclarecer a preocupacao com originalidade ou autenticidade sem afirmar o que nao estiver comprovado"
        ),
        "interpretation_target": (
            "o usuario demonstra receio de que o produto seja falso, replica ou nao original"
        ),
    },
    ("objection", "quality_objection"): {
        "family": "quality_resistance",
        "label": "objecao de qualidade",
        "issue_role": "objection",
        "journey_stage": "pre_purchase",
        "resolution_target": (
            "esclarecer a preocupacao com qualidade, material, acabamento ou durabilidade com base em dados reais"
        ),
        "interpretation_target": (
            "o usuario demonstra duvida ou resistencia relacionada a qualidade do produto"
        ),
    },
    ("objection", "size_objection"): {
        "family": "size_fit_resistance",
        "label": "objecao de tamanho ou caimento",
        "issue_role": "objection",
        "journey_stage": "pre_purchase",
        "resolution_target": (
            "esclarecer a preocupacao com tamanho, medida ou caimento sem garantir ajuste sem base"
        ),
        "interpretation_target": (
            "o usuario teme escolher um tamanho inadequado ou que o produto nao sirva"
        ),
    },
    ("objection", "compatibility_objection"): {
        "family": "compatibility_resistance",
        "label": "objecao de compatibilidade",
        "issue_role": "objection",
        "journey_stage": "pre_purchase",
        "resolution_target": (
            "esclarecer a preocupacao com compatibilidade usando as especificacoes disponiveis"
        ),
        "interpretation_target": (
            "o usuario teme que o produto nao funcione com seu aparelho, modelo ou situacao"
        ),
    },
    ("objection", "delivery_time_objection"): {
        "family": "delivery_time_resistance",
        "label": "objecao de prazo de entrega",
        "issue_role": "objection",
        "journey_stage": "pre_purchase",
        "resolution_target": (
            "esclarecer a preocupacao com prazo de entrega sem prometer prazo nao confirmado"
        ),
        "interpretation_target": (
            "o prazo de entrega pode impedir ou adiar a compra"
        ),
    },
    ("objection", "promotion_objection"): {
        "family": "promotion_trust",
        "label": "objecao sobre promocao",
        "issue_role": "objection",
        "journey_stage": "pre_purchase",
        "resolution_target": (
            "esclarecer a duvida sobre promocao, urgencia ou condicao anunciada sem criar escassez artificial"
        ),
        "interpretation_target": (
            "o usuario questiona a legitimidade ou a condicao da promocao"
        ),
    },

    # --------------------------------------------------------
    # POS-COMPRA - SOLICITACOES
    # --------------------------------------------------------
    ("post_purchase_issue", "exchange"): {
        "family": "after_sales_request",
        "label": "solicitacao de troca",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre o processo de troca conforme a politica e o canal realmente disponiveis"
        ),
        "interpretation_target": (
            "o usuario quer trocar produto, tamanho, cor ou variacao apos a compra"
        ),
    },
    ("post_purchase_issue", "return"): {
        "family": "after_sales_request",
        "label": "solicitacao de devolucao",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre o processo de devolucao conforme a politica e o canal realmente disponiveis"
        ),
        "interpretation_target": (
            "o usuario quer devolver o produto apos a compra"
        ),
    },
    ("post_purchase_issue", "refund"): {
        "family": "after_sales_request",
        "label": "reembolso ou estorno",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "esclarecer o fluxo ou status de reembolso/estorno sem afirmar conclusao nao verificada"
        ),
        "interpretation_target": (
            "o usuario solicita ou questiona um reembolso ou estorno"
        ),
    },
    ("post_purchase_issue", "cancellation"): {
        "family": "after_sales_request",
        "label": "cancelamento de pedido ou compra",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre cancelamento conforme o estado do pedido e as regras realmente disponiveis"
        ),
        "interpretation_target": (
            "o usuario quer cancelar um pedido ou compra ja realizada"
        ),
    },

    # --------------------------------------------------------
    # POS-COMPRA - ENTREGA / RASTREIO
    # --------------------------------------------------------
    ("post_purchase_issue", "delivered_not_received"): {
        "family": "post_purchase_logistics",
        "label": "consta entregue mas nao recebido",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "investigar e orientar sobre pedido marcado como entregue que o usuario relata nao ter recebido"
        ),
        "interpretation_target": (
            "ha divergencia entre o status de entrega e o recebimento relatado pelo usuario"
        ),
    },
    ("post_purchase_issue", "not_received"): {
        "family": "post_purchase_logistics",
        "label": "pedido nao recebido",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "verificar e orientar sobre pedido que o usuario relata nao ter recebido"
        ),
        "interpretation_target": (
            "o usuario relata que o pedido ainda nao foi recebido"
        ),
    },
    ("post_purchase_issue", "shipping_delay"): {
        "family": "post_purchase_logistics",
        "label": "atraso de envio ou entrega",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "esclarecer o atraso ou falta de atualizacao logistica sem inventar status"
        ),
        "interpretation_target": (
            "o usuario relata atraso, rastreio parado ou demora na postagem/entrega"
        ),
    },
    ("post_purchase_issue", "order_status"): {
        "family": "post_purchase_logistics",
        "label": "status ou rastreio do pedido",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "esclarecer como consultar ou interpretar o status/rastreio usando dados reais do pedido"
        ),
        "interpretation_target": (
            "o usuario quer saber onde esta o pedido ou obter atualizacao de envio"
        ),
    },

    # --------------------------------------------------------
    # POS-COMPRA - ITEM/VARIACAO/CONTEUDO DO PEDIDO
    # --------------------------------------------------------
    ("post_purchase_issue", "wrong_variant"): {
        "family": "fulfillment_accuracy",
        "label": "variacao errada recebida",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre divergencia de cor, tamanho, numero ou outra variacao recebida"
        ),
        "interpretation_target": (
            "o usuario relata ter recebido uma variacao diferente da solicitada"
        ),
    },
    ("post_purchase_issue", "wrong_item"): {
        "family": "fulfillment_accuracy",
        "label": "produto errado recebido",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre recebimento de produto ou modelo diferente do comprado"
        ),
        "interpretation_target": (
            "o usuario relata divergencia entre o item comprado e o item recebido"
        ),
    },
    ("post_purchase_issue", "missing_item"): {
        "family": "fulfillment_accuracy",
        "label": "item faltando ou pedido incompleto",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre item, acessorio ou quantidade faltante no pedido"
        ),
        "interpretation_target": (
            "o usuario relata que o pedido chegou incompleto"
        ),
    },

    # --------------------------------------------------------
    # POS-COMPRA - CONDICAO / QUALIDADE DO PRODUTO
    # --------------------------------------------------------
    ("post_purchase_issue", "damaged_item"): {
        "family": "received_product_issue",
        "label": "produto danificado",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre produto ou embalagem recebidos com dano fisico"
        ),
        "interpretation_target": (
            "o usuario relata dano fisico no produto ou embalagem recebidos"
        ),
    },
    ("post_purchase_issue", "defective_item"): {
        "family": "received_product_issue",
        "label": "produto com defeito",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre produto que nao funciona ou apresentou defeito, sem diagnosticar alem do relato"
        ),
        "interpretation_target": (
            "o usuario relata falha de funcionamento ou defeito apos receber o produto"
        ),
    },
    ("post_purchase_issue", "not_as_described"): {
        "family": "received_product_issue",
        "label": "produto diferente do anunciado",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre divergencia entre produto recebido e anuncio/descricao"
        ),
        "interpretation_target": (
            "o usuario relata que o produto recebido nao corresponde ao que foi anunciado"
        ),
    },

    # --------------------------------------------------------
    # POS-COMPRA - COBRANCA / DOCUMENTO / GARANTIA
    # --------------------------------------------------------
    ("post_purchase_issue", "payment_charge_problem"): {
        "family": "billing_after_sales",
        "label": "problema de pagamento ou cobranca",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre cobranca, pagamento ou confirmacao divergentes usando apenas dados verificaveis"
        ),
        "interpretation_target": (
            "o usuario relata uma divergencia financeira apos tentar ou concluir a compra"
        ),
    },
    ("post_purchase_issue", "invoice_missing"): {
        "family": "fiscal_document_after_sales",
        "label": "nota fiscal ausente",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre localizacao ou obtencao da nota fiscal do pedido"
        ),
        "interpretation_target": (
            "o usuario relata que a nota fiscal nao foi recebida ou nao foi localizada"
        ),
    },
    ("post_purchase_issue", "warranty_claim"): {
        "family": "warranty_after_sales",
        "label": "acionamento de garantia",
        "issue_role": "post_purchase_issue",
        "journey_stage": "post_purchase",
        "resolution_target": (
            "orientar sobre como acionar a garantia conforme as regras e canais realmente disponiveis"
        ),
        "interpretation_target": (
            "o usuario quer utilizar a garantia apos a compra"
        ),
    },
}


# ============================================================
# 3.1. CONTRATO COM WORKER COMENTARIOS V1.5
#
# 10 subtipos de objection + 17 de post_purchase_issue.
# Total oficial conhecido nesta versao: 27.
#
# purchase_barrier e aceito pelo Coach por compatibilidade futura,
# mas ainda nao faz parte da taxonomia emitida pelo Worker V1.5.
# ============================================================

COACH_OBJECTIONS_WORKER_V15_TOPICS = {
    ("objection", "purchase_hesitation"),
    ("objection", "price_objection"),
    ("objection", "shipping_objection"),
    ("objection", "trust_objection"),
    ("objection", "authenticity_objection"),
    ("objection", "quality_objection"),
    ("objection", "size_objection"),
    ("objection", "compatibility_objection"),
    ("objection", "delivery_time_objection"),
    ("objection", "promotion_objection"),
    ("post_purchase_issue", "exchange"),
    ("post_purchase_issue", "return"),
    ("post_purchase_issue", "refund"),
    ("post_purchase_issue", "cancellation"),
    ("post_purchase_issue", "delivered_not_received"),
    ("post_purchase_issue", "not_received"),
    ("post_purchase_issue", "shipping_delay"),
    ("post_purchase_issue", "order_status"),
    ("post_purchase_issue", "wrong_variant"),
    ("post_purchase_issue", "wrong_item"),
    ("post_purchase_issue", "missing_item"),
    ("post_purchase_issue", "damaged_item"),
    ("post_purchase_issue", "defective_item"),
    ("post_purchase_issue", "not_as_described"),
    ("post_purchase_issue", "payment_charge_problem"),
    ("post_purchase_issue", "invoice_missing"),
    ("post_purchase_issue", "warranty_claim"),
}


# ============================================================
# 4. FILA PARA O FUTURO CONTEXT FUSION
# ============================================================

coach_objections_output_queue = None


# ============================================================
# 5. HISTORICOS
# ============================================================

coach_objections_output_history = deque(
    maxlen=COACH_OBJECTIONS_MAX_HISTORY
)

coach_objections_individual_history = deque(
    maxlen=COACH_OBJECTIONS_MAX_HISTORY
)

coach_objections_topic_signal_history = deque(
    maxlen=COACH_OBJECTIONS_MAX_HISTORY
)


# ============================================================
# 6. JANELAS LOCAIS POR TOPICO
# ============================================================

coach_objections_topic_windows = defaultdict(deque)
coach_objections_last_topic_emitted = {}


# ============================================================
# 7. DEDUPLICACAO
# ============================================================

coach_objections_seen_dispatch_ids = set()
coach_objections_seen_dispatch_order = deque(
    maxlen=COACH_OBJECTIONS_MAX_SEEN
)


# ============================================================
# 8. ESTADO
# ============================================================

coach_objections_running = False
coach_objections_platform = None
coach_objections_live_id = None
coach_objections_subject = None
coach_objections_started_at = None
coach_objections_last_error = None


# ============================================================
# 9. ESTATISTICAS
# ============================================================

coach_objections_stats = defaultdict(int)
coach_objections_topic_counts = defaultdict(int)
coach_objections_family_counts = defaultdict(int)
coach_objections_category_counts = defaultdict(int)


# ============================================================
# 10. HORARIO
# ============================================================

def _co_now_iso():
    return (
        datetime
        .now(COACH_OBJECTIONS_TIMEZONE)
        .isoformat()
    )


# ============================================================
# 11. FALLBACK SEMANTICO
# ============================================================

def _co_fallback_info(category, subtype):
    if category == "objection":
        return {
            "family": "purchase_resistance",
            "label": str(subtype or "objecao de compra"),
            "issue_role": "objection",
            "journey_stage": "pre_purchase",
            "resolution_target": (
                "entender e esclarecer a objecao ou resistencia de compra relatada"
            ),
            "interpretation_target": (
                "o usuario apresentou uma objecao ainda nao especializada neste mapa"
            ),
        }

    if category == "post_purchase_issue":
        return {
            "family": "post_purchase_support",
            "label": str(subtype or "problema de pos-compra"),
            "issue_role": "post_purchase_issue",
            "journey_stage": "post_purchase",
            "resolution_target": (
                "entender e encaminhar o problema de pos-compra usando apenas informacoes verificaveis"
            ),
            "interpretation_target": (
                "o usuario relatou um problema ou solicitacao de pos-compra ainda nao especializada neste mapa"
            ),
        }

    # Categoria reservada pelo Dispatcher para evolucao futura.
    if category == "purchase_barrier":
        return {
            "family": "purchase_barrier",
            "label": str(subtype or "barreira de compra"),
            "issue_role": "purchase_barrier",
            "journey_stage": "pre_purchase",
            "resolution_target": (
                "entender a barreira que impede ou dificulta o avanco da compra"
            ),
            "interpretation_target": (
                "o usuario apresenta uma barreira de compra encaminhada pela taxonomia futura"
            ),
        }

    return {
        "family": "objections_context",
        "label": str(subtype or "sinal de objecao ou pos-compra"),
        "issue_role": str(category or "unknown"),
        "journey_stage": "unknown",
        "resolution_target": (
            "entender o contexto do sinal antes de qualquer acao"
        ),
        "interpretation_target": (
            "o evento contem contexto de objecao ou pos-compra ainda nao especializado"
        ),
    }


# ============================================================
# 12. INFORMACAO DE UM TOPICO
# ============================================================

def _co_topic_info(category, subtype):
    info = COACH_OBJECTIONS_TOPIC_MAP.get(
        (category, subtype)
    )

    if info is None:
        info = _co_fallback_info(
            category,
            subtype,
        )

    return {
        "category": category,
        "subtype": subtype,
        "family": info.get("family"),
        "label": info.get("label"),
        "issue_role": info.get("issue_role"),
        "journey_stage": info.get("journey_stage"),
        "resolution_target": info.get("resolution_target"),
        "interpretation_target": info.get("interpretation_target"),
    }


# ============================================================
# 13. CHAVE DO TOPICO
# ============================================================

def _co_topic_key(topic):
    return (
        str(topic.get("category"))
        + ":"
        + str(topic.get("subtype"))
    )


# ============================================================
# 14. REMOVER TOPICOS DUPLICADOS
# ============================================================

def _co_unique_topics(topics):
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

def _co_topics_from_envelope(envelope):
    intents = (
        envelope.get("matched_intents")
        or []
    )

    accepted = {
        "objection",
        "post_purchase_issue",
        "purchase_barrier",
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
            _co_topic_info(
                category,
                subtype,
            )
        )

    topics = _co_unique_topics(topics)

    # category_volume pode trazer categoria/None junto de subtipos.
    # Quando ha subtipo especifico, o generico nao agrega informacao.
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

def _co_comment_from_envelope(envelope):
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

def _co_classification_from_envelope(envelope):
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

def _co_seen(envelope):
    dispatch_id = envelope.get("dispatch_id")

    if not dispatch_id:
        return False

    if dispatch_id in coach_objections_seen_dispatch_ids:
        coach_objections_stats["duplicates_ignored"] += 1
        return True

    if (
        len(coach_objections_seen_dispatch_order)
        >=
        COACH_OBJECTIONS_MAX_SEEN
    ):
        old = coach_objections_seen_dispatch_order.popleft()
        coach_objections_seen_dispatch_ids.discard(old)

    coach_objections_seen_dispatch_ids.add(dispatch_id)
    coach_objections_seen_dispatch_order.append(dispatch_id)
    return False


# ============================================================
# 19. FILA DE SAIDA
# ============================================================

def _co_queue_output(output):
    global coach_objections_output_queue

    queue = coach_objections_output_queue

    if queue is None:
        queue = asyncio.Queue(
            maxsize=COACH_OBJECTIONS_OUTPUT_QUEUE_LIMIT
        )
        coach_objections_output_queue = queue

    if queue.full():
        coach_objections_stats["dropped_output_queue_full"] += 1
        return False

    queue.put_nowait(
        copy.deepcopy(output)
    )

    coach_objections_output_history.append(
        copy.deepcopy(output)
    )

    coach_objections_stats["outputs_sent"] += 1
    return True


# ============================================================
# 20. PODAR JANELA TEMPORAL
# ============================================================

def _co_prune_topic_window(queue, now=None):
    if now is None:
        now = time.time()

    limit = now - COACH_OBJECTIONS_TOPIC_WINDOW_SECONDS

    while queue:
        timestamp = queue[0].get("timestamp", 0)

        if timestamp >= limit:
            break

        queue.popleft()


# ============================================================
# 21. USUARIOS UNICOS
# ============================================================

def _co_unique_users(records):
    users = {
        str(record.get("user"))
        for record in records
        if record.get("user") not in (None, "")
    }

    return len(users)


# ============================================================
# 22. MAIOR REPETICAO DE UMA MESMA PESSOA
# ============================================================

def _co_max_repeat_by_one_user(records):
    counts = defaultdict(int)

    for record in records:
        user = record.get("user")

        if user in (None, ""):
            continue

        counts[str(user)] += 1

    if not counts:
        return 0

    return max(counts.values())


# ============================================================
# 23. EXEMPLOS
# ============================================================

def _co_examples(records, limit=3):
    examples = []
    seen = set()

    for record in reversed(records):
        text = str(record.get("text") or "").strip()

        if not text:
            continue

        key = text.lower()

        if key in seen:
            continue

        seen.add(key)
        examples.append(text)

        if len(examples) >= limit:
            break

    examples.reverse()
    return examples


# ============================================================
# 24. PODE REEMITIR ATIVIDADE DO TOPICO?
# ============================================================

def _co_can_emit_topic(
    topic_key,
    count,
    unique_users,
    now=None,
):
    if now is None:
        now = time.time()

    previous = coach_objections_last_topic_emitted.get(
        topic_key
    )

    if previous is None:
        return True

    elapsed = now - previous.get("time", 0)

    if elapsed >= COACH_OBJECTIONS_TOPIC_FORCE_REEMIT_SECONDS:
        return True

    if elapsed < COACH_OBJECTIONS_TOPIC_REEMIT_SECONDS:
        # Crescimento forte pode reemitir mesmo antes do cooldown.
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
# 25. FLAGS DE OBJECAO/POS-COMPRA
# ============================================================

def _co_flags(topics):
    categories = {
        topic.get("category")
        for topic in topics
    }

    subtypes = {
        topic.get("subtype")
        for topic in topics
    }

    return {
        "has_objection": "objection" in categories,
        "has_purchase_hesitation": "purchase_hesitation" in subtypes,
        "has_purchase_barrier": "purchase_barrier" in categories,
        "has_post_purchase_issue": "post_purchase_issue" in categories,
        "has_exchange_or_return_request": bool(
            subtypes & {"exchange", "return"}
        ),
        "has_refund_or_cancellation_request": bool(
            subtypes & {"refund", "cancellation"}
        ),
        "has_delivery_issue": bool(
            subtypes
            & {
                "delivered_not_received",
                "not_received",
                "shipping_delay",
                "order_status",
            }
        ),
        "has_fulfillment_issue": bool(
            subtypes
            & {
                "wrong_variant",
                "wrong_item",
                "missing_item",
            }
        ),
        "has_product_after_sales_issue": bool(
            subtypes
            & {
                "damaged_item",
                "defective_item",
                "not_as_described",
            }
        ),
        "has_payment_charge_problem": (
            "payment_charge_problem" in subtypes
        ),
        "has_invoice_missing": "invoice_missing" in subtypes,
        "has_warranty_claim": "warranty_claim" in subtypes,
    }


# ============================================================
# 26. EMITIR ANALISE INDIVIDUAL
# ============================================================

def _co_emit_individual_analysis(
    envelope,
    topics,
):
    classification = _co_classification_from_envelope(
        envelope
    )

    comment = _co_comment_from_envelope(
        envelope
    )

    response_needed = bool(
        envelope.get("requires_response")
        or
        classification.get("requires_response")
    )

    flags = _co_flags(topics)

    output = {
        "output_id": uuid.uuid4().hex,
        "message_type": "objections_analysis",
        "analysis_type": "individual",
        "source_coach": "objections",
        "coach_version": COACH_OBJECTIONS_VERSION,
        "platform": envelope.get("platform"),
        "live_id": envelope.get("live_id"),
        "subject": envelope.get("subject"),
        "timestamp": envelope.get("timestamp", time.time()),
        "iso_time": envelope.get("iso_time") or _co_now_iso(),
        "source_dispatch_id": envelope.get("dispatch_id"),
        "source_output_id": envelope.get("source_output_id"),
        "source_message_type": envelope.get("source_message_type"),
        "response_needed": response_needed,
        # Resolucao nao significa prioridade nem obrigacao de interromper.
        "needs_resolution": bool(topics),
        "is_question": bool(classification.get("is_question")),
        "is_request": bool(classification.get("is_request")),
        **flags,
        "topics": copy.deepcopy(topics),
        "comment": copy.deepcopy(comment),
        "resolution_needs": [
            topic.get("resolution_target")
            for topic in topics
            if topic.get("resolution_target")
        ],
        "issue_meanings": [
            topic.get("interpretation_target")
            for topic in topics
            if topic.get("interpretation_target")
        ],
    }

    if _co_queue_output(output):
        coach_objections_individual_history.append(
            copy.deepcopy(output)
        )
        coach_objections_stats["individual_outputs"] += 1

    return output


# ============================================================
# 27. REGISTRAR OCORRENCIA LOCAL DO TOPICO
# ============================================================

def _co_register_local_topic(
    envelope,
    topic,
):
    comment = _co_comment_from_envelope(
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

    topic_key = _co_topic_key(topic)
    queue = coach_objections_topic_windows[topic_key]

    queue.append(record)
    _co_prune_topic_window(queue)

    coach_objections_topic_counts[
        topic.get("subtype")
    ] += 1

    coach_objections_family_counts[
        topic.get("family")
    ] += 1

    coach_objections_category_counts[
        topic.get("category")
    ] += 1

    return list(queue)


# ============================================================
# 28. EMITIR ATIVIDADE LOCAL
# ============================================================

def _co_maybe_emit_local_topic_signal(
    envelope,
    topic,
    records,
):
    if not records:
        return None

    count = len(records)

    if count < COACH_OBJECTIONS_LOCAL_TOPIC_THRESHOLD:
        return None

    unique_users = _co_unique_users(records)
    max_same_user = _co_max_repeat_by_one_user(records)
    topic_key = _co_topic_key(topic)
    now = time.time()

    if not _co_can_emit_topic(
        topic_key,
        count,
        unique_users,
        now,
    ):
        return None

    output = {
        "output_id": uuid.uuid4().hex,
        "message_type": "objections_topic_signal",
        "analysis_type": "local_activity",
        "source_coach": "objections",
        "coach_version": COACH_OBJECTIONS_VERSION,
        "platform": envelope.get("platform"),
        "live_id": envelope.get("live_id"),
        "subject": envelope.get("subject"),
        "timestamp": now,
        "iso_time": _co_now_iso(),
        "topic": copy.deepcopy(topic),
        "topic_key": topic_key,
        "response_needed": bool(
            envelope.get("requires_response")
        ),
        "needs_resolution": True,
        "evidence": {
            "source": "coach_objections_local_window",
            "window_seconds": COACH_OBJECTIONS_TOPIC_WINDOW_SECONDS,
            "count": count,
            "unique_users": unique_users,
            "max_repetitions_by_one_user": max_same_user,
            "repeated_by_same_user": max_same_user >= 2,
            "multiple_users": unique_users >= 2,
            "examples": _co_examples(records),
            "source_output_ids": [
                record.get("source_output_id")
                for record in records
                if record.get("source_output_id")
            ],
        },
    }

    if _co_queue_output(output):
        coach_objections_topic_signal_history.append(
            copy.deepcopy(output)
        )

        coach_objections_stats["local_topic_signals"] += 1

        coach_objections_last_topic_emitted[
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

def _co_emit_worker_trend(
    envelope,
    topics,
):
    payload = (
        envelope.get("payload")
        or {}
    )

    output = {
        "output_id": uuid.uuid4().hex,
        "message_type": "objections_topic_signal",
        "analysis_type": "worker_trend",
        "source_coach": "objections",
        "coach_version": COACH_OBJECTIONS_VERSION,
        "platform": envelope.get("platform"),
        "live_id": envelope.get("live_id"),
        "subject": envelope.get("subject"),
        "timestamp": envelope.get("timestamp", time.time()),
        "iso_time": envelope.get("iso_time") or _co_now_iso(),
        "source_dispatch_id": envelope.get("dispatch_id"),
        "source_output_id": envelope.get("source_output_id"),
        "trend_kind": payload.get("trend_kind"),
        "topics": copy.deepcopy(topics),
        "response_needed": bool(
            envelope.get("requires_response")
        ),
        "needs_resolution": bool(topics),
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

    if _co_queue_output(output):
        coach_objections_topic_signal_history.append(
            copy.deepcopy(output)
        )
        coach_objections_stats["worker_trend_signals"] += 1

    return output


# ============================================================
# 30. PREVIEW PURO
#
# Nao altera estado. Util para teste.
# ============================================================

def coach_objections_preview_envelope(envelope):
    topics = _co_topics_from_envelope(
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
        "flags": _co_flags(topics),
    }

    if source_message_type == "classified_comment":
        classification = (
            payload.get("classification")
            or {}
        )

        result["comment"] = _co_comment_from_envelope(
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

def coach_objections_process_envelope(envelope):
    global coach_objections_platform
    global coach_objections_live_id
    global coach_objections_subject

    if not isinstance(envelope, dict):
        return []

    if envelope.get("destination") != "objections":
        coach_objections_stats["wrong_destination"] += 1
        return []

    if _co_seen(envelope):
        return []

    coach_objections_stats["received"] += 1

    coach_objections_platform = (
        envelope.get("platform")
        or
        coach_objections_platform
    )

    coach_objections_live_id = (
        envelope.get("live_id")
        or
        coach_objections_live_id
    )

    coach_objections_subject = (
        envelope.get("subject")
        or
        coach_objections_subject
    )

    topics = _co_topics_from_envelope(
        envelope
    )

    if not topics:
        coach_objections_stats["without_objections_topic"] += 1
        return []

    source_message_type = envelope.get(
        "source_message_type"
    )

    generated = []

    if source_message_type == "classified_comment":
        coach_objections_stats["individual_received"] += 1

        individual_output = _co_emit_individual_analysis(
            envelope,
            topics,
        )

        if individual_output is not None:
            generated.append(individual_output)

        for topic in topics:
            records = _co_register_local_topic(
                envelope,
                topic,
            )

            local_signal = _co_maybe_emit_local_topic_signal(
                envelope,
                topic,
                records,
            )

            if local_signal is not None:
                generated.append(local_signal)

    elif source_message_type == "comment_trend":
        coach_objections_stats["trends_received"] += 1

        trend_output = _co_emit_worker_trend(
            envelope,
            topics,
        )

        if trend_output is not None:
            generated.append(trend_output)

    else:
        coach_objections_stats["unknown_source_message_type"] += 1

    return generated


# ============================================================
# 32. RESET PARA NOVA LIVE
# ============================================================

def coach_objections_reset(platform=None):
    global coach_objections_output_queue
    global coach_objections_running
    global coach_objections_platform
    global coach_objections_live_id
    global coach_objections_subject
    global coach_objections_started_at
    global coach_objections_last_error

    coach_objections_output_queue = asyncio.Queue(
        maxsize=COACH_OBJECTIONS_OUTPUT_QUEUE_LIMIT
    )

    coach_objections_output_history.clear()
    coach_objections_individual_history.clear()
    coach_objections_topic_signal_history.clear()
    coach_objections_topic_windows.clear()
    coach_objections_last_topic_emitted.clear()
    coach_objections_seen_dispatch_ids.clear()
    coach_objections_seen_dispatch_order.clear()
    coach_objections_stats.clear()
    coach_objections_topic_counts.clear()
    coach_objections_family_counts.clear()
    coach_objections_category_counts.clear()

    coach_objections_running = False
    coach_objections_platform = platform
    coach_objections_live_id = None
    coach_objections_subject = None
    coach_objections_started_at = time.time()
    coach_objections_last_error = None


# ============================================================
# 33. LOOP PRINCIPAL
# ============================================================

async def coach_objections_loop():
    global coach_objections_running
    global coach_objections_last_error

    coach_objections_running = True

    try:
        while True:
            envelope = None

            try:
                envelope = await (
                    comment_dispatcher_next_objections(
                        timeout=0.5
                    )
                )

            except asyncio.TimeoutError:
                envelope = None

            except asyncio.CancelledError:
                raise

            except Exception as e:
                coach_objections_last_error = (
                    f"{type(e).__name__}: {e}"
                )
                await asyncio.sleep(0.1)

            if envelope is not None:
                try:
                    coach_objections_process_envelope(
                        envelope
                    )
                except Exception as e:
                    coach_objections_last_error = (
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
                "comment_dispatcher_objections_queue"
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
        coach_objections_running = False


# ============================================================
# 34. SUPERVISOR
# ============================================================

async def coach_objections_supervisor(platform):
    global coach_objections_last_error

    start = time.time()

    while True:
        if time.time() - start > 30:
            coach_objections_last_error = (
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
                    "comment_dispatcher_objections_queue"
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

    await coach_objections_loop()


# ============================================================
# 35. API PARA O FUTURO CONTEXT FUSION
# ============================================================

async def coach_objections_next_output(timeout=None):
    queue = coach_objections_output_queue

    if queue is None:
        return None

    if timeout is None:
        return await queue.get()

    return await asyncio.wait_for(
        queue.get(),
        timeout=timeout,
    )


# Alias explicito com o nome completo do modulo.
coach_objections_post_purchase_next_output = (
    coach_objections_next_output
)


# ============================================================
# 36. OUTPUTS RECENTES
# ============================================================

def coach_objections_recent_outputs(limit=20):
    if limit <= 0:
        return []

    return list(
        coach_objections_output_history
    )[-limit:]


def coach_objections_recent_individual(limit=20):
    if limit <= 0:
        return []

    return list(
        coach_objections_individual_history
    )[-limit:]


def coach_objections_recent_topic_signals(limit=20):
    if limit <= 0:
        return []

    return list(
        coach_objections_topic_signal_history
    )[-limit:]


# ============================================================
# 37. TOPICOS ATIVOS
# ============================================================

def coach_objections_active_topics():
    now = time.time()
    result = []

    for topic_key, queue in coach_objections_topic_windows.items():
        _co_prune_topic_window(
            queue,
            now,
        )

        if not queue:
            continue

        category, _, subtype = topic_key.partition(":")
        topic = _co_topic_info(
            category,
            subtype if subtype != "None" else None,
        )

        records = list(queue)

        result.append({
            "topic_key": topic_key,
            "topic": topic,
            "count": len(records),
            "unique_users": _co_unique_users(records),
            "max_repetitions_by_one_user": (
                _co_max_repeat_by_one_user(records)
            ),
            "examples": _co_examples(records),
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

def coach_objections_diagnostic():
    source_queue = globals().get(
        "comment_dispatcher_objections_queue"
    )

    source_queue_size = (
        source_queue.qsize()
        if source_queue is not None
        else None
    )

    output_queue_size = (
        coach_objections_output_queue.qsize()
        if coach_objections_output_queue is not None
        else None
    )

    return {
        "version": COACH_OBJECTIONS_VERSION,
        "running": coach_objections_running,
        "platform": coach_objections_platform,
        "live_id": coach_objections_live_id,
        "subject": coach_objections_subject,
        "source_queue_waiting": source_queue_size,
        "output_queue_waiting": output_queue_size,
        "last_error": coach_objections_last_error,
        "stats": dict(coach_objections_stats),
        "category_counts": dict(coach_objections_category_counts),
        "topic_counts": dict(coach_objections_topic_counts),
        "family_counts": dict(coach_objections_family_counts),
        "active_topics": coach_objections_active_topics(),
    }


# ============================================================
# 39. DIAGNOSTICO PRINCIPAL
# ============================================================

def mostrar_coach_objecoes():
    d = coach_objections_diagnostic()
    s = d.get("stats", {})
    categories = d.get("category_counts", {})

    print("=" * 76)
    print("AGCN - COACH OBJECOES/POS-COMPRA V1")
    print("Versao:", COACH_OBJECTIONS_VERSION)
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
        " Sem topico de objecao/pos-compra:",
        s.get("without_objections_topic", 0),
    )
    print()

    print("TIPOS PROCESSADOS")
    print(
        " Objecoes de pre-compra:",
        categories.get("objection", 0),
    )
    print(
        " Problemas/solicitacoes de pos-compra:",
        categories.get("post_purchase_issue", 0),
    )
    print(
        " Barreiras futuras de compra:",
        categories.get("purchase_barrier", 0),
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
        f"{COACH_OBJECTIONS_TOPIC_WINDOW_SECONDS}s):"
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

    outputs = coach_objections_recent_outputs(8)

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

        if output.get("message_type") == "objections_analysis":
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
            if output.get("has_objection"):
                print("      sinal: objecao de compra")
            if output.get("has_post_purchase_issue"):
                print("      sinal: problema/solicitacao de pos-compra")

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


# Alias com nome completo.
mostrar_coach_objecoes_pos_compra = mostrar_coach_objecoes


# ============================================================
# 40. DIAGNOSTICO DETALHADO
# ============================================================

def mostrar_coach_objecoes_detalhado(limit=20):
    outputs = coach_objections_recent_individual(limit)

    print("=" * 76)
    print("AGCN - COACH OBJECOES/POS-COMPRA V1 - ANALISES INDIVIDUAIS")
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
                "objection": output.get("has_objection"),
                "purchase_hesitation": output.get("has_purchase_hesitation"),
                "post_purchase_issue": output.get("has_post_purchase_issue"),
                "delivery_issue": output.get("has_delivery_issue"),
                "fulfillment_issue": output.get("has_fulfillment_issue"),
                "warranty_claim": output.get("has_warranty_claim"),
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
                topic.get("issue_role"),
                "| etapa:",
                topic.get("journey_stage"),
            )
            if topic.get("resolution_target"):
                print(
                    "   necessidade:",
                    topic.get("resolution_target"),
                )
            print(
                "   significado:",
                topic.get("interpretation_target"),
            )


mostrar_coach_objecoes_pos_compra_detalhado = (
    mostrar_coach_objecoes_detalhado
)


# ============================================================
# 41. ESTADO SIMPLES
# ============================================================

def coach_objections_status():
    return {
        "version": COACH_OBJECTIONS_VERSION,
        "running": coach_objections_running,
        "platform": coach_objections_platform,
        "live_id": coach_objections_live_id,
        "subject": coach_objections_subject,
        "last_error": coach_objections_last_error,
    }


# ============================================================
# 42. VALIDAR COBERTURA DA TAXONOMIA
# ============================================================

def coach_objections_validate_taxonomy():
    mapped = set(COACH_OBJECTIONS_TOPIC_MAP.keys())
    expected = set(COACH_OBJECTIONS_WORKER_V15_TOPICS)

    missing = sorted(
        expected - mapped
    )

    extra = sorted(
        mapped - expected
    )

    return {
        "worker_version": "1.5",
        "coach_version": COACH_OBJECTIONS_VERSION,
        "expected_objections_topics": len(expected),
        "mapped_objections_topics": len(expected & mapped),
        "missing": missing,
        "extra": extra,
        "ok": not missing,
    }


# ============================================================
# 43. TESTE INTERNO
#
# Teste puro: nao consome fila real e nao altera estado da LIVE.
# ============================================================

def testar_coach_objecoes_v1(verbose=False):
    print("=" * 76)
    print("TESTE INTERNO - COACH OBJECOES/POS-COMPRA V1")
    print("=" * 76)

    approved = 0
    failures = []

    taxonomy = coach_objections_validate_taxonomy()

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
                    bool(i.get("requires_response"))
                    for i in intents
                ),
                "is_request": any(
                    i.get("category") == "post_purchase_issue"
                    and i.get("subtype")
                    in {
                        "exchange",
                        "return",
                        "refund",
                        "cancellation",
                        "warranty_claim",
                    }
                    for i in intents
                ),
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
            "destination": "objections",
            "source_output_id": uuid.uuid4().hex,
            "source_message_type": source_message_type,
            "platform": "tiktok",
            "live_id": "test-live",
            "subject": "@teste",
            "timestamp": time.time(),
            "iso_time": _co_now_iso(),
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
    # 27 topicos oficiais Worker V1.5 -> Coach Objecoes V1
    # --------------------------------------------------------
    for category, subtype in sorted(
        COACH_OBJECTIONS_WORKER_V15_TOPICS
    ):
        requires_response = (
            category == "post_purchase_issue"
        )

        envelope = make_envelope(
            [{
                "category": category,
                "subtype": subtype,
                "requires_response": requires_response,
            }],
            text=f"teste {category}/{subtype}",
        )

        preview = coach_objections_preview_envelope(
            envelope
        )

        topics = preview.get("topics", [])

        check(
            f"mapeia {category}/{subtype}",
            len(topics) == 1
            and topics[0].get("category") == category
            and topics[0].get("subtype") == subtype
            and bool(topics[0].get("family"))
            and bool(topics[0].get("label"))
            and bool(topics[0].get("resolution_target")),
            topics,
        )

    # 28. Multi-intencao de objecoes.
    multi = make_envelope(
        [
            {
                "category": "objection",
                "subtype": "price_objection",
                "requires_response": False,
            },
            {
                "category": "objection",
                "subtype": "shipping_objection",
                "requires_response": False,
            },
        ],
        text="ta caro e o frete tambem",
    )

    multi_preview = coach_objections_preview_envelope(multi)
    multi_pairs = {
        (t.get("category"), t.get("subtype"))
        for t in multi_preview.get("topics", [])
    }

    check(
        "multi-intencao preserva duas objecoes",
        multi_pairs == {
            ("objection", "price_objection"),
            ("objection", "shipping_objection"),
        },
        multi_pairs,
    )

    # 29. Flags de objecao.
    check(
        "flags identificam objecao",
        bool(
            multi_preview.get("flags", {}).get(
                "has_objection"
            )
        ),
        multi_preview.get("flags"),
    )

    # 30. Flags de pos-compra e logistica.
    post = make_envelope(
        [{
            "category": "post_purchase_issue",
            "subtype": "not_received",
            "requires_response": True,
        }],
        text="meu pedido nao chegou",
    )

    post_preview = coach_objections_preview_envelope(post)

    check(
        "flags identificam pos-compra/logistica",
        bool(
            post_preview.get("flags", {}).get(
                "has_post_purchase_issue"
            )
        )
        and bool(
            post_preview.get("flags", {}).get(
                "has_delivery_issue"
            )
        ),
        post_preview.get("flags"),
    )

    # 31. Tendencia simples de objecao de preco.
    trend = make_envelope(
        [{
            "category": "objection",
            "subtype": "price_objection",
            "requires_response": False,
        }],
        source_message_type="comment_trend",
        trend_payload={
            "trend_kind": "topic_repeat",
            "category": "objection",
            "subtype": "price_objection",
            "count": 3,
            "unique_users": 3,
            "strength": 0.8,
            "window_seconds": 60,
            "examples": ["ta caro", "muito caro"],
        },
    )

    trend_preview = coach_objections_preview_envelope(trend)

    check(
        "comment_trend de objecao interpretado",
        trend_preview.get("source_message_type") == "comment_trend"
        and trend_preview.get("trend", {}).get("count") == 3
        and trend_preview.get("topics", [])[0].get("subtype")
        == "price_objection",
        trend_preview,
    )

    # 32. category_volume com generico + especificos remove generico.
    category_volume = make_envelope(
        [
            {
                "category": "post_purchase_issue",
                "subtype": None,
                "requires_response": True,
            },
            {
                "category": "post_purchase_issue",
                "subtype": "not_received",
                "requires_response": True,
            },
            {
                "category": "post_purchase_issue",
                "subtype": "shipping_delay",
                "requires_response": True,
            },
        ],
        source_message_type="comment_trend",
        trend_payload={
            "trend_kind": "category_volume",
            "category": "post_purchase_issue",
            "subtype": None,
            "subtypes": ["not_received", "shipping_delay"],
            "count": 5,
            "unique_users": 4,
            "strength": 0.9,
            "window_seconds": 60,
        },
    )

    cv_preview = coach_objections_preview_envelope(category_volume)
    cv_subtypes = {
        t.get("subtype")
        for t in cv_preview.get("topics", [])
    }

    check(
        "category_volume remove topico generico quando ha especificos",
        cv_subtypes == {"not_received", "shipping_delay"},
        cv_subtypes,
    )

    # 33. Fallback nao descarta subtipo futuro de objection.
    unknown = make_envelope(
        [{
            "category": "objection",
            "subtype": "future_objection_subtype",
            "requires_response": False,
        }],
        text="teste futuro",
    )

    unknown_preview = coach_objections_preview_envelope(unknown)
    unknown_topics = unknown_preview.get("topics", [])

    check(
        "fallback preserva subtipo futuro de objection",
        len(unknown_topics) == 1
        and unknown_topics[0].get("subtype")
        == "future_objection_subtype"
        and bool(unknown_topics[0].get("resolution_target")),
        unknown_topics,
    )

    # 34. Categoria purchase_barrier reservada pelo Dispatcher.
    future_barrier = make_envelope(
        [{
            "category": "purchase_barrier",
            "subtype": "future_barrier",
            "requires_response": False,
        }],
        text="barreira futura",
    )

    barrier_preview = coach_objections_preview_envelope(
        future_barrier
    )

    check(
        "purchase_barrier futuro e preservado",
        len(barrier_preview.get("topics", [])) == 1
        and barrier_preview.get("topics", [])[0].get("issue_role")
        == "purchase_barrier",
        barrier_preview,
    )

    total = approved + len(failures)

    print()
    print("=" * 76)
    print("RESUMO DO TESTE COACH OBJECOES/POS-COMPRA V1")
    print("=" * 76)
    print(
        "Topicos Worker V1.5 mapeados:",
        f"{taxonomy['mapped_objections_topics']} / "
        f"{taxonomy['expected_objections_topics']}",
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


# Alias extenso, caso queira usar o nome completo.
testar_coach_objecoes_pos_compra_v1 = testar_coach_objecoes_v1


# ============================================================
# 44. FINALIZAR TASK
# ============================================================

async def _coach_objections_finish_task(task):
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
# Na ordem oficial, a funcao atual deve conter:
#
# Live Engine
# + Coach Armazenamento
# + Worker Comentarios V1.5
# + Comment Dispatcher V1
# + Coach Produto V1.1
# + Coach Comercial V1
#
# Agora adicionamos:
#
# + Coach Objecoes/Pos-compra V1
#
# Se esta propria celula for reexecutada antes de carregar
# wrappers posteriores, nao empilhamos dois Coaches de Objecoes.
# Se Worker Audiencia/Interface ja tiverem sido carregados e
# voce quiser substituir esta versao, reinicie o runtime e
# execute os modulos novamente na ordem oficial.
# ============================================================

_current_objections_shopee_executor = (
    executar_shopee_com_live_engine
)

if getattr(
    _current_objections_shopee_executor,
    "_agcn_objections_coach_wrapper",
    False,
):
    _coach_objections_base_shopee = (
        _current_objections_shopee_executor
        ._agcn_objections_coach_base
    )
else:
    _coach_objections_base_shopee = (
        _current_objections_shopee_executor
    )


_current_objections_tiktok_executor = (
    executar_tiktok_com_live_engine
)

if getattr(
    _current_objections_tiktok_executor,
    "_agcn_objections_coach_wrapper",
    False,
):
    _coach_objections_base_tiktok = (
        _current_objections_tiktok_executor
        ._agcn_objections_coach_base
    )
else:
    _coach_objections_base_tiktok = (
        _current_objections_tiktok_executor
    )


# ============================================================
# 46. SHOPEE
# ============================================================

async def executar_shopee_com_live_engine():
    coach_objections_reset(
        "shopee"
    )

    objections_task = asyncio.create_task(
        coach_objections_supervisor(
            "shopee"
        )
    )

    try:
        return await _coach_objections_base_shopee()

    finally:
        await _coach_objections_finish_task(
            objections_task
        )


executar_shopee_com_live_engine._agcn_objections_coach_wrapper = True
executar_shopee_com_live_engine._agcn_objections_coach_base = (
    _coach_objections_base_shopee
)
executar_shopee_com_live_engine._agcn_objections_coach_version = (
    COACH_OBJECTIONS_VERSION
)


# ============================================================
# 47. TIKTOK
# ============================================================

async def executar_tiktok_com_live_engine(username):
    coach_objections_reset(
        "tiktok"
    )

    objections_task = asyncio.create_task(
        coach_objections_supervisor(
            "tiktok"
        )
    )

    try:
        return await _coach_objections_base_tiktok(
            username
        )

    finally:
        await _coach_objections_finish_task(
            objections_task
        )


executar_tiktok_com_live_engine._agcn_objections_coach_wrapper = True
executar_tiktok_com_live_engine._agcn_objections_coach_base = (
    _coach_objections_base_tiktok
)
executar_tiktok_com_live_engine._agcn_objections_coach_version = (
    COACH_OBJECTIONS_VERSION
)


# ============================================================
# 48. PRONTO
# ============================================================

print("AGCN Coach Objecoes/Pos-compra V1 carregado.")
print("Entrada: Comment Dispatcher V1 -> fila objections.")
print("Analisa taxonomia de objecoes/pos-compra do Worker Comentarios V1.5:")
print("1. objecoes e hesitacao de pre-compra")
print("2. troca, devolucao, reembolso e cancelamento")
print("3. entrega, atraso, rastreio e nao recebimento")
print("4. item/variacao errados ou faltantes")
print("5. produto danificado, defeituoso ou divergente")
print("6. cobranca, nota fiscal e garantia")
print("7. repeticao do mesmo assunto")
print("8. repeticao da mesma pessoa x varias pessoas")
print("Saidas:")
print("1. objections_analysis")
print("2. objections_topic_signal")
print("Destino futuro: Context Fusion.")
print("Sem prioridade, sem Storage e sem Interface.")
