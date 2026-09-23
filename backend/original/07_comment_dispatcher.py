# ============================================================
# AGCN - COMMENT DISPATCHER V1
#
# Worker Coach Comentarios V1.3
#              ↓
#       COMMENT DISPATCHER
#        ├─ Coach Produto
#        ├─ Coach Comercial
#        └─ Coach Objecoes/Pos-compra
#
# O Dispatcher NAO interpreta.
# O Dispatcher NAO prioriza.
# O Dispatcher NAO consulta Storage.
# O Dispatcher NAO publica na Interface.
#
# Ele apenas recebe as classificacoes prontas do
# Worker Coach Comentarios e distribui para os
# futuros Coaches especializados.
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

_CD_REQUIRED = [

    "worker_coach_comments_next_output",

    "live_engine_is_running",

    "live_engine_platform",

    "executar_shopee_com_live_engine",

    "executar_tiktok_com_live_engine",

]


_CD_MISSING = [

    name

    for name in _CD_REQUIRED

    if name not in globals()

]


if _CD_MISSING:

    raise RuntimeError(

        "Execute primeiro o Live Engine V2 "
        "e o Worker Coach Comentarios V1.3. "
        "Faltando: "

        + ", ".join(
            _CD_MISSING
        )

    )


# ============================================================
# 2. CONFIGURACAO
# ============================================================

COMMENT_DISPATCHER_VERSION = "1.0"


COMMENT_DISPATCHER_TIMEZONE = (
    ZoneInfo(
        "America/Araguaina"
    )
)


COMMENT_DISPATCHER_QUEUE_LIMIT = 5000

COMMENT_DISPATCHER_MAX_HISTORY = 5000

COMMENT_DISPATCHER_MAX_SEEN = 20000


# ============================================================
# 3. MAPA DE ROTEAMENTO
#
# Aqui definimos somente GRANDES responsabilidades.
#
# Um comentario pode seguir para mais de um Coach.
#
# Exemplo:
#
# "tem como pegar 3, um rosa, um pink e um lilas?"
#
# product_question / color_variant
# commercial_question / variant_selection
# buying_intent
#
# Vai para:
#
# Coach Produto
# +
# Coach Comercial
#
# ============================================================

COMMENT_DISPATCHER_ROUTE_MAP = {

    # --------------------------------------------------------
    # COACH PRODUTO
    # --------------------------------------------------------

    "product": {

        "product_question",

        "demo_request",

    },


    # --------------------------------------------------------
    # COACH COMERCIAL
    # --------------------------------------------------------

    "commercial": {

        "commercial_question",

        "buying_intent",

        "purchase_completed",

        "commercial_observation",

    },


    # --------------------------------------------------------
    # COACH OBJECOES / POS-COMPRA
    # --------------------------------------------------------

    "objections": {

        "objection",

        "post_purchase_issue",

        # Reservado para uma evolucao futura do
        # Worker Comentarios.
        "purchase_barrier",

    },

}


# ============================================================
# 4. FILAS DOS FUTUROS COACHES
# ============================================================

comment_dispatcher_product_queue = None

comment_dispatcher_commercial_queue = None

comment_dispatcher_objections_queue = None


# ============================================================
# 5. HISTORICOS
# ============================================================

comment_dispatcher_recent_routes = deque(

    maxlen=
        COMMENT_DISPATCHER_MAX_HISTORY

)


comment_dispatcher_unrouted_history = deque(

    maxlen=
        COMMENT_DISPATCHER_MAX_HISTORY

)


# ============================================================
# 6. DEDUPLICACAO
# ============================================================

comment_dispatcher_seen_ids = set()


comment_dispatcher_seen_order = deque(

    maxlen=
        COMMENT_DISPATCHER_MAX_SEEN

)


# ============================================================
# 7. ESTADO
# ============================================================

comment_dispatcher_running = False

comment_dispatcher_platform = None

comment_dispatcher_live_id = None

comment_dispatcher_subject = None

comment_dispatcher_started_at = None

comment_dispatcher_last_error = None


# Estatisticas gerais.

comment_dispatcher_stats = defaultdict(
    int
)


# Quantas copias foram enviadas
# para cada Coach.

comment_dispatcher_route_counts = defaultdict(
    int
)


# Quantos outputs antigos precisaram ser removidos
# caso alguma fila lotasse.

comment_dispatcher_dropped_by_channel = defaultdict(
    int
)


# ============================================================
# 8. HORARIO
# ============================================================

def _cd_now_iso():

    return (
        datetime
        .now(
            COMMENT_DISPATCHER_TIMEZONE
        )
        .isoformat()
    )


# ============================================================
# 9. RETORNAR FILA POR DESTINO
# ============================================================

def _cd_queue_for(
    destination
):

    if destination == "product":

        return (
            comment_dispatcher_product_queue
        )


    if destination == "commercial":

        return (
            comment_dispatcher_commercial_queue
        )


    if destination == "objections":

        return (
            comment_dispatcher_objections_queue
        )


    return None


# ============================================================
# 10. DEDUPLICACAO
# ============================================================

def _cd_mark_seen(
    output
):

    output_id = (
        output.get(
            "output_id"
        )
    )


    if not output_id:

        return False


    output_id = str(
        output_id
    )


    if (
        output_id
        in
        comment_dispatcher_seen_ids
    ):

        comment_dispatcher_stats[
            "duplicates"
        ] += 1


        return True


    if (

        len(
            comment_dispatcher_seen_order
        )

        >=

        comment_dispatcher_seen_order.maxlen

    ):

        old = (
            comment_dispatcher_seen_order[0]
        )


        comment_dispatcher_seen_ids.discard(
            old
        )


    comment_dispatcher_seen_order.append(
        output_id
    )


    comment_dispatcher_seen_ids.add(
        output_id
    )


    return False


# ============================================================
# 11. ADICIONAR INTENT SEM DUPLICAR
# ============================================================

def _cd_add_intent(
    result,
    category,
    subtype=None,
    requires_response=False
):

    if not category:

        return


    key = (
        category,
        subtype
    )


    for item in result:

        existing_key = (

            item.get(
                "category"
            ),

            item.get(
                "subtype"
            )

        )


        if existing_key == key:

            # Se qualquer versao da classificacao disser
            # que exige resposta, preservamos True.

            if requires_response:

                item[
                    "requires_response"
                ] = True


            return


    result.append({

        "category":
            category,

        "subtype":
            subtype,

        "requires_response":
            bool(
                requires_response
            ),

    })


# ============================================================
# 12. EXTRAIR CLASSIFICACOES DO OUTPUT
#
# O Worker pode enviar:
#
# 1. classified_comment
# 2. comment_trend
#
# Aqui transformamos ambos numa estrutura comum.
# ============================================================

def _cd_extract_intents(
    output
):

    result = []


    message_type = (
        output.get(
            "message_type"
        )
    )


    # ========================================================
    # COMENTARIO INDIVIDUAL
    # ========================================================

    if (
        message_type
        ==
        "classified_comment"
    ):

        classification = (

            output.get(
                "classification"
            )

            or

            {}

        )


        intents = (

            classification.get(
                "intents"
            )

            or

            []

        )


        for intent in intents:

            if not isinstance(
                intent,
                dict
            ):

                continue


            _cd_add_intent(

                result,

                intent.get(
                    "category"
                ),

                intent.get(
                    "subtype"
                ),

                intent.get(
                    "requires_response",
                    False
                ),

            )


        # Protecao:
        #
        # mesmo que main_classification por algum motivo
        # nao esteja dentro de intents, ela nao sera perdida.

        main = (

            classification.get(
                "main_classification"
            )

            or

            {}

        )


        _cd_add_intent(

            result,

            main.get(
                "category"
            ),

            main.get(
                "subtype"
            ),

            main.get(

                "requires_response",

                classification.get(
                    "requires_response",
                    False
                )

            ),

        )


    # ========================================================
    # TENDENCIA
    # ========================================================

    elif (
        message_type
        ==
        "comment_trend"
    ):

        category = (
            output.get(
                "category"
            )
        )


        requires_response = (

            category
            in {

                "product_question",

                "commercial_question",

                "post_purchase_issue",

            }

        )


        _cd_add_intent(

            result,

            category,

            output.get(
                "subtype"
            ),

            requires_response,

        )


        # category_volume pode conter varios subtipos.

        for subtype in (

            output.get(
                "subtypes"
            )

            or

            []

        ):

            _cd_add_intent(

                result,

                category,

                subtype,

                requires_response,

            )


    return result


# ============================================================
# 13. CALCULAR ROTAS
#
# Esta funcao NAO coloca nada nas filas.
#
# Pode ser usada em diagnosticos/testes.
# ============================================================

def comment_dispatcher_routes_for_output(
    output
):

    intents = (
        _cd_extract_intents(
            output
        )
    )


    routes = {}


    for destination, categories in (

        COMMENT_DISPATCHER_ROUTE_MAP.items()

    ):

        matched = [

            copy.deepcopy(
                intent
            )

            for intent in intents

            if (
                intent.get(
                    "category"
                )
                in
                categories
            )

        ]


        if matched:

            routes[
                destination
            ] = matched


    return routes


# ============================================================
# 14. CRIAR ENVELOPE PARA O COACH ESPECIALIZADO
#
# O payload original do Worker e preservado inteiro.
#
# O Coach especializado recebe:
#
# - para onde foi roteado
# - por que foi roteado
# - quais categorias corresponderam
# - quais subtipos corresponderam
# - payload original completo
# ============================================================

def _cd_build_envelope(
    output,
    destination,
    matched_intents
):

    categories = list(

        dict.fromkeys(

            intent.get(
                "category"
            )

            for intent in matched_intents

            if intent.get(
                "category"
            )

        )

    )


    subtypes = list(

        dict.fromkeys(

            intent.get(
                "subtype"
            )

            for intent in matched_intents

            if intent.get(
                "subtype"
            )

        )

    )


    return {

        "dispatch_id":
            uuid.uuid4().hex,

        "message_type":
            "dispatched_comment_output",

        "dispatcher_version":
            COMMENT_DISPATCHER_VERSION,

        "destination":
            destination,

        "source_worker":
            "comments",

        "source_output_id":
            output.get(
                "output_id"
            ),

        "source_message_type":
            output.get(
                "message_type"
            ),

        "platform":
            output.get(
                "platform"
            ),

        "live_id":
            output.get(
                "live_id"
            ),

        "subject":
            output.get(
                "subject"
            ),

        "timestamp":
            output.get(
                "timestamp",
                time.time()
            ),

        "iso_time":
            (
                output.get(
                    "iso_time"
                )
                or
                _cd_now_iso()
            ),

        "matched_categories":
            categories,

        "matched_subtypes":
            subtypes,

        "matched_intents":
            copy.deepcopy(
                matched_intents
            ),

        "requires_response":
            any(

                bool(
                    intent.get(
                        "requires_response"
                    )
                )

                for intent
                in matched_intents

            ),

        # Saida original do Worker Comentarios.
        "payload":
            copy.deepcopy(
                output
            ),

    }


# ============================================================
# 15. ENVIAR PARA UMA FILA
# ============================================================

def _cd_put(
    destination,
    envelope
):

    queue = (
        _cd_queue_for(
            destination
        )
    )


    if queue is None:

        return False


    try:

        queue.put_nowait(
            envelope
        )


    except asyncio.QueueFull:

        # Se algum futuro Coach parar de consumir por muito
        # tempo, preservamos os dados mais recentes.

        try:

            queue.get_nowait()

        except Exception:

            pass


        comment_dispatcher_dropped_by_channel[
            destination
        ] += 1


        try:

            queue.put_nowait(
                envelope
            )

        except Exception:

            return False


    comment_dispatcher_stats[
        "routed_copies"
    ] += 1


    comment_dispatcher_route_counts[
        destination
    ] += 1


    comment_dispatcher_recent_routes.append({

        "source_output_id":
            envelope.get(
                "source_output_id"
            ),

        "source_message_type":
            envelope.get(
                "source_message_type"
            ),

        "destination":
            destination,

        "matched_categories":
            list(
                envelope.get(
                    "matched_categories"
                )
                or
                []
            ),

        "matched_subtypes":
            list(
                envelope.get(
                    "matched_subtypes"
                )
                or
                []
            ),

        "timestamp":
            envelope.get(
                "timestamp"
            ),

    })


    return True


# ============================================================
# 16. PROCESSAR OUTPUT DO WORKER COMENTARIOS
# ============================================================

def comment_dispatcher_process_output(
    output
):

    global comment_dispatcher_platform

    global comment_dispatcher_live_id

    global comment_dispatcher_subject


    if not isinstance(
        output,
        dict
    ):

        return []


    # ========================================================
    # DEDUP
    # ========================================================

    if _cd_mark_seen(
        output
    ):

        return []


    comment_dispatcher_stats[
        "received"
    ] += 1


    message_type = (
        output.get(
            "message_type"
        )
    )


    # ========================================================
    # TIPO DE OUTPUT
    # ========================================================

    if (
        message_type
        ==
        "classified_comment"
    ):

        comment_dispatcher_stats[
            "classified_comment"
        ] += 1


    elif (
        message_type
        ==
        "comment_trend"
    ):

        comment_dispatcher_stats[
            "comment_trend"
        ] += 1


    else:

        comment_dispatcher_stats[
            "unrouted"
        ] += 1


        comment_dispatcher_unrouted_history.append({

            "reason":
                "unsupported_message_type",

            "output":
                copy.deepcopy(
                    output
                ),

        })


        return []


    # ========================================================
    # IDENTIDADE DA LIVE
    # ========================================================

    comment_dispatcher_platform = (

        output.get(
            "platform"
        )

        or

        comment_dispatcher_platform

    )


    comment_dispatcher_live_id = (

        output.get(
            "live_id"
        )

        or

        comment_dispatcher_live_id

    )


    comment_dispatcher_subject = (

        output.get(
            "subject"
        )

        or

        comment_dispatcher_subject

    )


    # ========================================================
    # CALCULAR ROTAS
    # ========================================================

    routes = (
        comment_dispatcher_routes_for_output(
            output
        )
    )


    if not routes:

        comment_dispatcher_stats[
            "unrouted"
        ] += 1


        comment_dispatcher_unrouted_history.append({

            "reason":
                "no_destination_for_classification",

            "output":
                copy.deepcopy(
                    output
                ),

        })


        return []


    # ========================================================
    # FAN-OUT
    # ========================================================

    destinations = []


    for destination, matched_intents in (

        routes.items()

    ):

        envelope = (
            _cd_build_envelope(

                output,

                destination,

                matched_intents,

            )
        )


        if _cd_put(
            destination,
            envelope
        ):

            destinations.append(
                destination
            )


    if destinations:

        comment_dispatcher_stats[
            "unique_routed"
        ] += 1


        if len(
            destinations
        ) > 1:

            comment_dispatcher_stats[
                "multi_routed"
            ] += 1


    return destinations


# ============================================================
# 17. RESET PARA NOVA LIVE
# ============================================================

def comment_dispatcher_reset(
    platform
):

    global comment_dispatcher_product_queue

    global comment_dispatcher_commercial_queue

    global comment_dispatcher_objections_queue

    global comment_dispatcher_running

    global comment_dispatcher_platform

    global comment_dispatcher_live_id

    global comment_dispatcher_subject

    global comment_dispatcher_started_at

    global comment_dispatcher_last_error


    comment_dispatcher_product_queue = (
        asyncio.Queue(

            maxsize=
                COMMENT_DISPATCHER_QUEUE_LIMIT

        )
    )


    comment_dispatcher_commercial_queue = (
        asyncio.Queue(

            maxsize=
                COMMENT_DISPATCHER_QUEUE_LIMIT

        )
    )


    comment_dispatcher_objections_queue = (
        asyncio.Queue(

            maxsize=
                COMMENT_DISPATCHER_QUEUE_LIMIT

        )
    )


    comment_dispatcher_recent_routes.clear()

    comment_dispatcher_unrouted_history.clear()

    comment_dispatcher_seen_ids.clear()

    comment_dispatcher_seen_order.clear()

    comment_dispatcher_stats.clear()

    comment_dispatcher_route_counts.clear()

    comment_dispatcher_dropped_by_channel.clear()


    comment_dispatcher_running = False

    comment_dispatcher_platform = platform

    comment_dispatcher_live_id = None

    comment_dispatcher_subject = None

    comment_dispatcher_started_at = (
        time.time()
    )

    comment_dispatcher_last_error = None


# ============================================================
# 18. LOOP PRINCIPAL
# ============================================================

async def comment_dispatcher_loop():

    global comment_dispatcher_running

    global comment_dispatcher_last_error


    comment_dispatcher_running = True


    try:

        while True:

            item = None


            try:

                item = await (

                    worker_coach_comments_next_output(
                        timeout=0.5
                    )

                )


            except asyncio.TimeoutError:

                item = None


            except asyncio.CancelledError:

                raise


            except Exception as e:

                comment_dispatcher_last_error = (

                    f"{type(e).__name__}: "
                    f"{e}"

                )


                await asyncio.sleep(
                    0.1
                )


            # =================================================
            # OUTPUT RECEBIDO
            # =================================================

            if item is not None:

                try:

                    comment_dispatcher_process_output(
                        item
                    )


                except Exception as e:

                    comment_dispatcher_last_error = (

                        f"{type(e).__name__}: "
                        f"{e}"

                    )


                continue


            # =================================================
            # ENCERRAMENTO
            #
            # So sai depois que:
            #
            # Live Engine parou
            # Worker Comentarios parou
            # fila do Worker ficou vazia
            # =================================================

            comments_running = bool(

                globals().get(

                    "worker_coach_comments_running",

                    False

                )

            )


            source_queue = globals().get(

                "worker_coach_comments_output_queue"

            )


            source_empty = (

                source_queue is None

                or

                source_queue.empty()

            )


            if (

                not live_engine_is_running()

                and

                not comments_running

                and

                source_empty

            ):

                break


    finally:

        comment_dispatcher_running = False


# ============================================================
# 19. SUPERVISOR
# ============================================================

async def comment_dispatcher_supervisor(
    platform
):

    global comment_dispatcher_last_error


    start = time.time()


    while True:

        if (

            time.time()
            -
            start

            >
            30

        ):

            comment_dispatcher_last_error = (

                "Worker Coach Comentarios "
                "nao ficou pronto dentro de 30 segundos."

            )


            return


        try:

            ready = (

                live_engine_is_running()

                and

                live_engine_platform()
                ==
                platform

                and

                globals().get(
                    "worker_coach_comments_output_queue"
                )
                is not None

                and

                bool(

                    globals().get(

                        "worker_coach_comments_running",

                        False

                    )

                )

            )


        except Exception:

            ready = False


        if ready:

            break


        await asyncio.sleep(
            0.05
        )


    await comment_dispatcher_loop()


# ============================================================
# 20. API GENERICA DE LEITURA
# ============================================================

async def _cd_next(
    destination,
    timeout=None
):

    queue = (
        _cd_queue_for(
            destination
        )
    )


    if queue is None:

        raise RuntimeError(

            "Comment Dispatcher ainda nao "
            "iniciou uma LIVE."

        )


    if timeout is None:

        return await queue.get()


    return await asyncio.wait_for(

        queue.get(),

        timeout=
            timeout

    )


# ============================================================
# 21. API DO FUTURO COACH PRODUTO
# ============================================================

async def comment_dispatcher_next_product(
    timeout=None
):

    return await (

        _cd_next(
            "product",
            timeout
        )

    )


# ============================================================
# 22. API DO FUTURO COACH COMERCIAL
# ============================================================

async def comment_dispatcher_next_commercial(
    timeout=None
):

    return await (

        _cd_next(
            "commercial",
            timeout
        )

    )


# ============================================================
# 23. API DO FUTURO COACH OBJECOES
# ============================================================

async def comment_dispatcher_next_objections(
    timeout=None
):

    return await (

        _cd_next(
            "objections",
            timeout
        )

    )


# ============================================================
# 24. TAMANHOS DAS FILAS
# ============================================================

def comment_dispatcher_queue_sizes():

    result = {}


    for destination in (

        COMMENT_DISPATCHER_ROUTE_MAP

    ):

        queue = (
            _cd_queue_for(
                destination
            )
        )


        result[
            destination
        ] = (

            None

            if queue is None

            else

            queue.qsize()

        )


    return result


# ============================================================
# 25. MOSTRAR MAPA DE ROTAS
# ============================================================

def mostrar_rotas_comment_dispatcher():

    print(
        "=" * 76
    )

    print(
        "ROTAS - COMMENT DISPATCHER V1"
    )

    print(
        "=" * 76
    )


    for destination, categories in (

        COMMENT_DISPATCHER_ROUTE_MAP.items()

    ):

        print()

        print(
            destination
        )


        for category in sorted(
            categories
        ):

            print(
                "  -",
                category
            )


# ============================================================
# 26. DIAGNOSTICO PRINCIPAL
# ============================================================

def mostrar_comment_dispatcher():

    sizes = (
        comment_dispatcher_queue_sizes()
    )


    print(
        "=" * 76
    )

    print(
        "AGCN - COMMENT DISPATCHER V1"
    )

    print(
        "=" * 76
    )


    print(
        "Rodando:",
        comment_dispatcher_running
    )


    print(
        "Plataforma:",
        comment_dispatcher_platform
    )


    print(
        "Live ID:",
        comment_dispatcher_live_id
    )


    print(
        "Subject:",
        comment_dispatcher_subject
    )


    print()


    print(
        "ENTRADA"
    )


    print(
        "  Outputs recebidos:",
        comment_dispatcher_stats.get(
            "received",
            0
        )
    )


    print(
        "  classified_comment:",
        comment_dispatcher_stats.get(
            "classified_comment",
            0
        )
    )


    print(
        "  comment_trend:",
        comment_dispatcher_stats.get(
            "comment_trend",
            0
        )
    )


    print(
        "  Duplicados ignorados:",
        comment_dispatcher_stats.get(
            "duplicates",
            0
        )
    )


    print()


    print(
        "ROTEAMENTO"
    )


    print(
        "  Outputs unicos roteados:",
        comment_dispatcher_stats.get(
            "unique_routed",
            0
        )
    )


    print(
        "  Multi-rota:",
        comment_dispatcher_stats.get(
            "multi_routed",
            0
        )
    )


    print(
        "  Copias roteadas:",
        comment_dispatcher_stats.get(
            "routed_copies",
            0
        )
    )


    print(
        "  Sem destino:",
        comment_dispatcher_stats.get(
            "unrouted",
            0
        )
    )


    print()


    print(
        "FILAS DOS FUTUROS COACHES"
    )


    print(
        "  Coach Produto:",
        sizes.get(
            "product"
        )
    )


    print(
        "  Coach Comercial:",
        sizes.get(
            "commercial"
        )
    )


    print(
        "  Coach Objecoes/Pos-compra:",
        sizes.get(
            "objections"
        )
    )


    print()


    print(
        "TOTAL ENVIADO"
    )


    print(
        "  product:",
        comment_dispatcher_route_counts.get(
            "product",
            0
        )
    )


    print(
        "  commercial:",
        comment_dispatcher_route_counts.get(
            "commercial",
            0
        )
    )


    print(
        "  objections:",
        comment_dispatcher_route_counts.get(
            "objections",
            0
        )
    )


    print()


    print(
        "DESCARTES POR FILA CHEIA"
    )


    print(
        "  product:",
        comment_dispatcher_dropped_by_channel.get(
            "product",
            0
        )
    )


    print(
        "  commercial:",
        comment_dispatcher_dropped_by_channel.get(
            "commercial",
            0
        )
    )


    print(
        "  objections:",
        comment_dispatcher_dropped_by_channel.get(
            "objections",
            0
        )
    )


    print()


    print(
        "Erro:",
        comment_dispatcher_last_error
    )


    print()


    print(
        "ULTIMOS ROTEAMENTOS:"
    )


    recent = list(
        comment_dispatcher_recent_routes
    )[-12:]


    if not recent:

        print(
            "  nenhum ainda"
        )


    else:

        for item in recent:

            print()


            print(

                " ",

                item.get(
                    "source_message_type"
                ),

                "->",

                item.get(
                    "destination"
                ),

            )


            print(

                "    categorias:",

                item.get(
                    "matched_categories"
                )

            )


            print(

                "    subtipos:",

                item.get(
                    "matched_subtypes"
                )

            )


# ============================================================
# 27. TESTE INTERNO
#
# Nao precisa abrir LIVE.
#
# Serve apenas para verificar a logica de roteamento.
# ============================================================

def testar_comment_dispatcher_v1():

    testes = [

        # ----------------------------------------------------
        # Produto
        # ----------------------------------------------------

        (

            "pergunta de produto",

            {

                "message_type":
                    "classified_comment",

                "classification": {

                    "main_classification": {

                        "category":
                            "product_question",

                        "subtype":
                            "battery_duration",

                        "requires_response":
                            True,

                    },

                    "intents": [

                        {

                            "category":
                                "product_question",

                            "subtype":
                                "battery_duration",

                            "requires_response":
                                True,

                        }

                    ],

                },

            },

        ),


        # ----------------------------------------------------
        # Produto + Comercial
        # ----------------------------------------------------

        (

            "produto + comercial",

            {

                "message_type":
                    "classified_comment",

                "classification": {

                    "main_classification": {

                        "category":
                            "commercial_question",

                        "subtype":
                            "variant_selection",

                        "requires_response":
                            True,

                    },

                    "intents": [

                        {

                            "category":
                                "commercial_question",

                            "subtype":
                                "variant_selection",

                            "requires_response":
                                True,

                        },

                        {

                            "category":
                                "product_question",

                            "subtype":
                                "color_variant",

                            "requires_response":
                                True,

                        },

                        {

                            "category":
                                "buying_intent",

                            "subtype":
                                "purchase_intent",

                            "requires_response":
                                False,

                        },

                    ],

                },

            },

        ),


        # ----------------------------------------------------
        # Comercial + Objecao
        # ----------------------------------------------------

        (

            "comercial + objecao",

            {

                "message_type":
                    "classified_comment",

                "classification": {

                    "main_classification": {

                        "category":
                            "commercial_question",

                        "subtype":
                            "availability",

                        "requires_response":
                            True,

                    },

                    "intents": [

                        {

                            "category":
                                "commercial_question",

                            "subtype":
                                "availability",

                            "requires_response":
                                True,

                        },

                        {

                            "category":
                                "objection",

                            "subtype":
                                "purchase_hesitation",

                            "requires_response":
                                False,

                        },

                    ],

                },

            },

        ),


        # ----------------------------------------------------
        # Pos-compra
        # ----------------------------------------------------

        (

            "pos-compra",

            {

                "message_type":
                    "classified_comment",

                "classification": {

                    "main_classification": {

                        "category":
                            "post_purchase_issue",

                        "subtype":
                            "refund",

                        "requires_response":
                            True,

                    },

                    "intents": [

                        {

                            "category":
                                "post_purchase_issue",

                            "subtype":
                                "refund",

                            "requires_response":
                                True,

                        }

                    ],

                },

            },

        ),

    ]


    print(
        "=" * 76
    )

    print(
        "TESTE INTERNO - COMMENT DISPATCHER V1"
    )

    print(
        "=" * 76
    )


    for nome, output in testes:

        routes = (
            comment_dispatcher_routes_for_output(
                output
            )
        )


        print()

        print(
            "Caso:",
            nome
        )


        print(
            "Destinos:",
            list(
                routes.keys()
            )
        )


        for destination, intents in (

            routes.items()

        ):

            print(

                " ",

                destination,

                "->",

                intents

            )


# ============================================================
# 28. FINALIZAR TASK
# ============================================================

async def _comment_dispatcher_finish_task(
    task
):

    if task is None:

        return


    try:

        await asyncio.wait_for(

            asyncio.shield(
                task
            ),

            timeout=2.5

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
# 29. REMOVER CAMADAS OBSOLETAS
#
# IMPORTANTE:
#
# Se o antigo Coach Orchestrator monolitico tiver sido
# executado anteriormente neste runtime, ele NAO pode
# continuar consumindo a fila do Worker Comentarios.
#
# Esta funcao remove SOMENTE:
#
# - antigo Orchestrator monolitico
# - uma instancia anterior deste Dispatcher
#
# E PRESERVA:
#
# Live Engine
# Coach Armazenamento
# Worker Coach Comentarios
# Worker Coach Audiencia
# ============================================================

def _cd_unwrap_obsolete_layers(
    function
):

    current = function


    for _ in range(
        20
    ):

        # ====================================================
        # INSTANCIA ANTERIOR DO DISPATCHER
        # ====================================================

        if getattr(

            current,

            "_agcn_comment_dispatcher_wrapper",

            False

        ):

            current = getattr(

                current,

                "_agcn_comment_dispatcher_base"

            )


            continue


        # ====================================================
        # ANTIGO ORCHESTRATOR MONOLITICO
        # ====================================================

        if getattr(

            current,

            "_agcn_orchestrator_wrapper",

            False

        ):

            current = getattr(

                current,

                "_agcn_orchestrator_base"

            )


            continue


        break


    return current


# ============================================================
# 30. DESCOBRIR BASE SHOPEE
# ============================================================

_comment_dispatcher_base_shopee = (

    _cd_unwrap_obsolete_layers(

        executar_shopee_com_live_engine

    )

)


# ============================================================
# 31. DESCOBRIR BASE TIKTOK
# ============================================================

_comment_dispatcher_base_tiktok = (

    _cd_unwrap_obsolete_layers(

        executar_tiktok_com_live_engine

    )

)


# ============================================================
# 32. SHOPEE
#
# Continua executando:
#
# Live Engine
# + Coach Storage
# + Worker Comentarios
# + Worker Audiencia
#
# E agora adiciona:
#
# + Comment Dispatcher
# ============================================================

async def executar_shopee_com_live_engine():

    comment_dispatcher_reset(
        "shopee"
    )


    dispatcher_task = (
        asyncio.create_task(

            comment_dispatcher_supervisor(
                "shopee"
            )

        )
    )


    try:

        return await (
            _comment_dispatcher_base_shopee()
        )


    finally:

        await (
            _comment_dispatcher_finish_task(
                dispatcher_task
            )
        )


executar_shopee_com_live_engine._agcn_comment_dispatcher_wrapper = True


executar_shopee_com_live_engine._agcn_comment_dispatcher_base = (
    _comment_dispatcher_base_shopee
)


# ============================================================
# 33. TIKTOK
# ============================================================

async def executar_tiktok_com_live_engine(
    username
):

    comment_dispatcher_reset(
        "tiktok"
    )


    dispatcher_task = (
        asyncio.create_task(

            comment_dispatcher_supervisor(
                "tiktok"
            )

        )
    )


    try:

        return await (

            _comment_dispatcher_base_tiktok(
                username
            )

        )


    finally:

        await (
            _comment_dispatcher_finish_task(
                dispatcher_task
            )
        )


executar_tiktok_com_live_engine._agcn_comment_dispatcher_wrapper = True


executar_tiktok_com_live_engine._agcn_comment_dispatcher_base = (
    _comment_dispatcher_base_tiktok
)


# ============================================================
# 34. PRONTO
# ============================================================

print(
    "AGCN Comment Dispatcher V1 carregado."
)

print(
    "Entrada: Worker Coach Comentarios V1.3."
)

print(
    "Saidas:"
)

print(
    "1. Coach Produto"
)

print(
    "2. Coach Comercial"
)

print(
    "3. Coach Objecoes/Pos-compra"
)

print(
    "Multi-classificacao pode gerar multi-rota "
    "sem duplicar dentro do mesmo destino."
)

print(
    "Sem Storage, sem prioridade e sem Interface."
)

print(
    "O antigo Coach Orchestrator monolitico "
    "e removido da cadeia de execucao."
)
