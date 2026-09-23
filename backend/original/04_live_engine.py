# ============================================================
# AGCN - LIVE ENGINE V2
#
# FUNCAO:
#
# Receber dados dos Workers:
# - Shopee Worker
# - TikTok Worker
#
# NORMALIZAR os dados para o formato interno do AGCN
#
# E distribuir para QUATRO destinos logicos:
#
# 1. INTERFACE
#    -> snapshot + history
#
# 2. WORKER COACH COMENTARIOS
#    -> somente eventos "comment"
#
# 3. WORKER COACH AUDIENCIA
#    -> viewer_count
#    -> like_count
#    -> share_count
#    -> follow_count
#    -> gift_count
#    -> total_user
#    -> member_count
#    -> gift
#
# 4. ARMAZENAMENTO COACH
#    -> TODOS os eventos
#
#
# IMPORTANTE:
#
# Cada destino possui sua PROPRIA fila.
#
# Assim:
#
# evento
#   ├── copia para comentarios
#   ├── copia para audiencia
#   └── copia para armazenamento
#
# Um consumidor NAO rouba eventos do outro.
#
#
# NORMALIZACAO DE COMENTARIOS:
#
# Workers podem entregar:
#
# {
#     "hora": "...",
#     "usuario": "...",
#     "texto": "..."
# }
#
# Live Engine converte para:
#
# {
#     "display_time": "...",
#     "user": "...",
#     "text": "..."
# }
#
# A partir daqui todos os outros modulos
# trabalham com o MESMO formato.
# ============================================================


import asyncio
import time
import uuid

from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo


# ============================================================
# 1. CONFIGURACAO
# ============================================================

LIVE_ENGINE_TIMEZONE = (
    ZoneInfo(
        "America/Araguaina"
    )
)


LIVE_ENGINE_MAX_HISTORY = 20000


LIVE_ENGINE_POLL = 0.25


# Limites das filas.
#
# Sao grandes o suficiente para o prototipo,
# mas nao deixam a memoria crescer infinitamente.

LIVE_ENGINE_QUEUE_LIMITS = {

    "coach_comments":
        5000,

    "coach_audience":
        10000,

    "coach_storage":
        20000,

}


# ============================================================
# 2. TIPOS QUE IRAO PARA CADA WORKER COACH
# ============================================================

LIVE_ENGINE_COMMENT_TYPES = {

    "comment",

}


LIVE_ENGINE_AUDIENCE_TYPES = {

    "viewer_count",

    "like_count",

    "share_count",

    "follow_count",

    "gift_count",

    "total_user",

    "member_count",

    # Evento individual de presente TikTok
    "gift",

}


# ============================================================
# 3. MEMORIA PRINCIPAL DO LIVE ENGINE
#
# A Interface continua lendo estes dois objetos.
# ============================================================

live_engine_history = deque(
    maxlen=
        LIVE_ENGINE_MAX_HISTORY
)


live_engine_snapshot = {}


# ============================================================
# 4. FILAS PUB/SUB
#
# Cada modulo tera sua propria fila.
# ============================================================

live_engine_channels = {

    "coach_comments":
        None,

    "coach_audience":
        None,

    "coach_storage":
        None,

}


# ============================================================
# 5. ESTATISTICAS DE ROTEAMENTO
# ============================================================

live_engine_routing_stats = {

    "interface":
        0,

    "coach_comments":
        0,

    "coach_audience":
        0,

    "coach_storage":
        0,

    "dropped_comments":
        0,

    "dropped_audience":
        0,

    "dropped_storage":
        0,

}


# ============================================================
# 6. ESTADO INTERNO
# ============================================================

_live_engine_seq = 0


_live_engine_running = False


_live_engine_platform = None


_live_engine_started_at = None


# Ultimos valores das metricas.
#
# Evita emitir centenas de eventos identicos.

_live_engine_last_values = {}


# Comentarios ja enviados pelo Engine.

_live_engine_seen_comments = set()


_live_engine_seen_comment_order = deque(
    maxlen=20000
)


# Presentes ja enviados.

_live_engine_seen_gifts = set()


_live_engine_seen_gift_order = deque(
    maxlen=10000
)


# ============================================================
# 7. HORARIO
# ============================================================

def _engine_now_iso():

    return (
        datetime
        .now(
            LIVE_ENGINE_TIMEZONE
        )
        .isoformat()
    )


# ============================================================
# 8. LIVE ID ATUAL
# ============================================================

def _engine_live_id(
    platform
):

    try:

        if platform == "shopee":

            return (
                estado.get(
                    "sessionId"
                )
            )


        if platform == "tiktok":

            return (
                estado_tiktok.get(
                    "roomId"
                )
            )


    except Exception:

        pass


    return None


# ============================================================
# 9. SUBJECT
#
# Nome principal da LIVE / loja / usuario.
# ============================================================

def _engine_subject(
    platform
):

    try:

        if platform == "shopee":

            return (

                estado.get(
                    "loja"
                )

                or

                estado.get(
                    "username"
                )

                or

                estado.get(
                    "titulo"
                )

            )


        if platform == "tiktok":

            return (
                estado_tiktok.get(
                    "username"
                )
            )


    except Exception:

        pass


    return None


# ============================================================
# 10. COLOCAR EVENTO EM UMA FILA
#
# NUNCA bloqueia o Live Engine.
#
# Se uma fila ficar cheia:
# - remove o evento mais antigo
# - coloca o novo
#
# Isso evita que um Coach travado derrube a coleta.
# ============================================================

def _engine_queue_put(
    channel_name,
    event
):

    queue = (
        live_engine_channels.get(
            channel_name
        )
    )


    if queue is None:

        return


    try:

        queue.put_nowait(
            event.copy()
        )


    except asyncio.QueueFull:

        # ====================================================
        # REMOVER MAIS ANTIGO
        # ====================================================

        try:

            queue.get_nowait()

        except Exception:

            pass


        # ====================================================
        # CONTABILIZAR DROP
        # ====================================================

        if (
            channel_name
            ==
            "coach_comments"
        ):

            live_engine_routing_stats[
                "dropped_comments"
            ] += 1


        elif (
            channel_name
            ==
            "coach_audience"
        ):

            live_engine_routing_stats[
                "dropped_audience"
            ] += 1


        elif (
            channel_name
            ==
            "coach_storage"
        ):

            live_engine_routing_stats[
                "dropped_storage"
            ] += 1


        # ====================================================
        # TENTAR INSERIR NOVAMENTE
        # ====================================================

        try:

            queue.put_nowait(
                event.copy()
            )

        except Exception:

            pass


# ============================================================
# 11. ROTEADOR CENTRAL
# ============================================================

def _engine_route_event(
    event
):

    event_type = (
        event.get(
            "type"
        )
    )


    # ========================================================
    # DESTINO 1:
    # INTERFACE
    #
    # A Interface consulta snapshot/history.
    # ========================================================

    live_engine_routing_stats[
        "interface"
    ] += 1


    # ========================================================
    # DESTINO 2:
    # WORKER COACH COMENTARIOS
    # ========================================================

    if (
        event_type
        in
        LIVE_ENGINE_COMMENT_TYPES
    ):

        _engine_queue_put(

            "coach_comments",

            event

        )


        live_engine_routing_stats[
            "coach_comments"
        ] += 1


    # ========================================================
    # DESTINO 3:
    # WORKER COACH AUDIENCIA
    # ========================================================

    if (
        event_type
        in
        LIVE_ENGINE_AUDIENCE_TYPES
    ):

        _engine_queue_put(

            "coach_audience",

            event

        )


        live_engine_routing_stats[
            "coach_audience"
        ] += 1


    # ========================================================
    # DESTINO 4:
    # ARMAZENAMENTO COACH
    #
    # Recebe absolutamente tudo.
    # ========================================================

    _engine_queue_put(

        "coach_storage",

        event

    )


    live_engine_routing_stats[
        "coach_storage"
    ] += 1


# ============================================================
# 12. EMITIR EVENTO NORMALIZADO
# ============================================================

def live_engine_emit(
    platform,
    event_type,
    payload=None
):

    global _live_engine_seq


    if payload is None:

        payload = {}


    _live_engine_seq += 1


    timestamp = time.time()


    event = {

        # Ordem interna
        "seq":
            _live_engine_seq,


        # ID unico
        "event_id":
            uuid.uuid4().hex,


        # Plataforma
        "platform":
            platform,


        # ID da LIVE
        "live_id":
            _engine_live_id(
                platform
            ),


        # Loja / usuario
        "subject":
            _engine_subject(
                platform
            ),


        # Tipo normalizado
        "type":
            event_type,


        # Tempo
        "timestamp":
            timestamp,


        "iso_time":
            _engine_now_iso(),


        # Dados
        "payload":
            payload,

    }


    # ========================================================
    # INTERFACE:
    # HISTORICO
    # ========================================================

    live_engine_history.append(
        event
    )


    # ========================================================
    # INTERFACE:
    # SNAPSHOT
    #
    # Guarda o ultimo evento de cada tipo.
    # ========================================================

    live_engine_snapshot[
        event_type
    ] = event


    # ========================================================
    # PUB/SUB
    # ========================================================

    _engine_route_event(
        event
    )


    return event


# ============================================================
# 13. EMITIR APENAS SE VALOR MUDOU
# ============================================================

def _emit_if_changed(
    platform,
    event_type,
    value,
    extra_payload=None
):

    key = (
        platform,
        event_type
    )


    antigo = (
        _live_engine_last_values.get(
            key,
            object()
        )
    )


    if antigo == value:

        return None


    _live_engine_last_values[
        key
    ] = value


    payload = {

        "value":
            value

    }


    if extra_payload:

        payload.update(
            extra_payload
        )


    return live_engine_emit(

        platform,

        event_type,

        payload

    )


# ============================================================
# 14. HELPER DE DEDUPLICACAO
# ============================================================

def _engine_seen_add(
    seen_set,
    order_deque,
    key
):

    key = str(
        key
    )


    if key in seen_set:

        return False


    if (
        len(
            order_deque
        )
        >=
        order_deque.maxlen
    ):

        antigo = (
            order_deque[0]
        )


        seen_set.discard(
            antigo
        )


    order_deque.append(
        key
    )


    seen_set.add(
        key
    )


    return True


# ============================================================
# 15. EXTRAIR E NORMALIZAR COMENTARIO
#
# ESTA E A CORRECAO PRINCIPAL.
#
#
# FORMATO QUE OS WORKERS PODEM ENTREGAR:
#
# {
#     "hora": "15:45:32",
#     "usuario": "Maria",
#     "texto": "quanto custa?"
# }
#
#
# FORMATO INTERNO PADRAO DO AGCN:
#
# {
#     "id": ...,
#     "user": "Maria",
#     "text": "quanto custa?",
#     "display_time": "15:45:32"
# }
#
#
# Depois desta funcao:
#
# Interface
# Coach Comentarios
# Armazenamento
# Orchestrator
#
# nunca precisam saber o formato original.
# ============================================================

def _engine_extract_comment(
    item
):

    # ========================================================
    # PROTECAO:
    # CASO NAO SEJA DICIONARIO
    # ========================================================

    if not isinstance(
        item,
        dict
    ):

        texto = str(
            item
            or
            ""
        ).strip()


        return {

            "id":
                None,

            "user":
                "Usuario",

            "text":
                texto,

            "display_time":
                "",

        }


    # ========================================================
    # ID DO COMENTARIO
    # ========================================================

    comment_id = (

        item.get(
            "id"
        )

        or

        item.get(
            "message_id"
        )

        or

        item.get(
            "msg_id"
        )

        or

        item.get(
            "comment_id"
        )

        or

        item.get(
            "event_id"
        )

        or

        item.get(
            "uuid"
        )

    )


    # ========================================================
    # USUARIO
    #
    # "usuario" e o formato principal atual dos Workers.
    #
    # Os outros campos ficam como compatibilidade.
    # ========================================================

    user_raw = (

        item.get(
            "usuario"
        )

        or

        item.get(
            "user"
        )

        or

        item.get(
            "display_name"
        )

        or

        item.get(
            "nickname"
        )

        or

        item.get(
            "username"
        )

        or

        item.get(
            "unique_id"
        )

        or

        item.get(
            "author"
        )

        or

        item.get(
            "autor"
        )

    )


    # ========================================================
    # SE O USUARIO VIER COMO OBJETO / DICT
    # ========================================================

    if isinstance(
        user_raw,
        dict
    ):

        user = (

            user_raw.get(
                "display_name"
            )

            or

            user_raw.get(
                "nickname"
            )

            or

            user_raw.get(
                "username"
            )

            or

            user_raw.get(
                "unique_id"
            )

            or

            user_raw.get(
                "usuario"
            )

            or

            user_raw.get(
                "name"
            )

            or

            user_raw.get(
                "nome"
            )

            or

            "Usuario"

        )


    else:

        user = (

            user_raw

            or

            "Usuario"

        )


    # ========================================================
    # TEXTO
    #
    # "texto" e o formato principal atual dos Workers.
    # ========================================================

    text_raw = (

        item.get(
            "texto"
        )

        or

        item.get(
            "text"
        )

        or

        item.get(
            "comentario"
        )

        or

        item.get(
            "comment"
        )

        or

        item.get(
            "content"
        )

        or

        item.get(
            "message"
        )

        or

        item.get(
            "msg"
        )

        or

        ""

    )


    # ========================================================
    # HORARIO
    #
    # "hora" e o formato principal atual dos Workers.
    # ========================================================

    display_time_raw = (

        item.get(
            "hora"
        )

        or

        item.get(
            "display_time"
        )

        or

        item.get(
            "time"
        )

        or

        item.get(
            "horario"
        )

        or

        ""

    )


    # ========================================================
    # LIMPEZA
    # ========================================================

    user = str(
        user
    ).strip()


    if not user:

        user = "Usuario"


    text = str(
        text_raw
    ).strip()


    display_time = str(
        display_time_raw
    ).strip()


    # ========================================================
    # FORMATO NORMALIZADO FINAL
    # ========================================================

    return {

        "id":
            comment_id,

        "user":
            user,

        "text":
            text,

        "display_time":
            display_time,

    }


# ============================================================
# 16. GERAR ASSINATURA DE COMENTARIO
#
# Usada para evitar duplicados.
#
# Se existir ID:
# usa ID.
#
# Senao:
# usa plataforma + usuario + texto + horario.
# ============================================================

def _engine_comment_signature(
    platform,
    comment
):

    if comment.get(
        "id"
    ):

        return (

            platform

            +

            ":id:"

            +

            str(
                comment[
                    "id"
                ]
            )

        )


    return (

        platform

        +

        ":txt:"

        +

        str(
            comment.get(
                "user"
            )
        )

        +

        "|"

        +

        str(
            comment.get(
                "text"
            )
        )

        +

        "|"

        +

        str(
            comment.get(
                "display_time"
            )
        )

    )


# ============================================================
# 17. EXTRAIR PRESENTE TIKTOK
# ============================================================

def _engine_extract_gift(
    item
):

    if not isinstance(
        item,
        dict
    ):

        return {

            "id":
                None,

            "user":
                "Usuario",

            "gift":
                str(item),

            "count":
                1,

        }


    user_raw = (

        item.get(
            "usuario"
        )

        or

        item.get(
            "user"
        )

        or

        item.get(
            "username"
        )

        or

        item.get(
            "nickname"
        )

        or

        "Usuario"

    )


    if isinstance(
        user_raw,
        dict
    ):

        user_raw = (

            user_raw.get(
                "nickname"
            )

            or

            user_raw.get(
                "username"
            )

            or

            user_raw.get(
                "unique_id"
            )

            or

            user_raw.get(
                "name"
            )

            or

            "Usuario"

        )


    return {

        "id":

            item.get(
                "id"
            )

            or

            item.get(
                "event_id"
            )

            or

            item.get(
                "msg_id"
            ),


        "user":

            str(
                user_raw
            ),


        "gift":

            str(

                item.get(
                    "gift"
                )

                or

                item.get(
                    "gift_name"
                )

                or

                item.get(
                    "presente"
                )

                or

                item.get(
                    "name"
                )

                or

                "Presente"

            ),


        "count":

            item.get(
                "count"
            )

            or

            item.get(
                "repeat_count"
            )

            or

            item.get(
                "quantidade"
            )

            or

            1,

    }


# ============================================================
# 18. WATCHER SHOPEE
# ============================================================

async def live_engine_watch_shopee():

    while (
        _live_engine_running

        and

        _live_engine_platform
        ==
        "shopee"
    ):

        try:

            # =================================================
            # IDENTIDADE / STATUS
            # =================================================

            _emit_if_changed(

                "shopee",

                "session_id",

                estado.get(
                    "sessionId"
                )

            )


            _emit_if_changed(

                "shopee",

                "chatroom_id",

                estado.get(
                    "chatroomId"
                )

            )


            _emit_if_changed(

                "shopee",

                "live_status",

                estado.get(
                    "status"
                )

            )


            # =================================================
            # AUDIENCIA
            # =================================================

            _emit_if_changed(

                "shopee",

                "viewer_count",

                estado.get(
                    "viewers"
                ),

                {

                    "semantics":
                        "current"

                }

            )


            _emit_if_changed(

                "shopee",

                "like_count",

                estado.get(
                    "likes"
                ),

                {

                    "semantics":
                        "live_total"

                }

            )


            _emit_if_changed(

                "shopee",

                "share_count",

                estado.get(
                    "shares"
                ),

                {

                    "semantics":
                        "live_total"

                }

            )


            _emit_if_changed(

                "shopee",

                "product_count",

                estado.get(
                    "products"
                ),

                {

                    "semantics":
                        "snapshot"

                }

            )


            _emit_if_changed(

                "shopee",

                "member_count",

                estado.get(
                    "memberCnt"
                ),

                {

                    "semantics":
                        "unknown_semantics"

                }

            )


            # =================================================
            # COMENTARIOS
            # =================================================

            for raw_comment in list(
                comentarios
            ):

                comment = (
                    _engine_extract_comment(
                        raw_comment
                    )
                )


                # Comentarios sem texto nao sao uteis
                # para Interface nem para Coach.

                if not comment.get(
                    "text"
                ):

                    continue


                signature = (
                    _engine_comment_signature(

                        "shopee",

                        comment

                    )
                )


                if not _engine_seen_add(

                    _live_engine_seen_comments,

                    _live_engine_seen_comment_order,

                    signature

                ):

                    continue


                live_engine_emit(

                    "shopee",

                    "comment",

                    {

                        "id":
                            comment[
                                "id"
                            ],

                        "user":
                            comment[
                                "user"
                            ],

                        "text":
                            comment[
                                "text"
                            ],

                        "display_time":
                            comment[
                                "display_time"
                            ],

                    }

                )


        except Exception:

            pass


        await asyncio.sleep(
            LIVE_ENGINE_POLL
        )


# ============================================================
# 19. WATCHER TIKTOK
# ============================================================

async def live_engine_watch_tiktok():

    while (
        _live_engine_running

        and

        _live_engine_platform
        ==
        "tiktok"
    ):

        try:

            # =================================================
            # IDENTIDADE / CONEXAO
            # =================================================

            _emit_if_changed(

                "tiktok",

                "room_id",

                estado_tiktok.get(
                    "roomId"
                )

            )


            _emit_if_changed(

                "tiktok",

                "connection_status",

                estado_tiktok.get(
                    "conectado"
                )

            )


            _emit_if_changed(

                "tiktok",

                "live_ended",

                estado_tiktok.get(
                    "encerrada"
                )

            )


            # =================================================
            # AUDIENCIA
            # =================================================

            _emit_if_changed(

                "tiktok",

                "viewer_count",

                estado_tiktok.get(
                    "viewers"
                ),

                {

                    "semantics":
                        "current"

                }

            )


            _emit_if_changed(

                "tiktok",

                "like_count",

                estado_tiktok.get(
                    "likes"
                ),

                {

                    "semantics":
                        "live_total"

                }

            )


            _emit_if_changed(

                "tiktok",

                "total_user",

                estado_tiktok.get(
                    "totalUser"
                ),

                {

                    "semantics":
                        "separate_metric"

                }

            )


            _emit_if_changed(

                "tiktok",

                "follow_count",

                estado_tiktok.get(
                    "followsObservados"
                ),

                {

                    "semantics":
                        "observed_since_monitoring"

                }

            )


            _emit_if_changed(

                "tiktok",

                "share_count",

                estado_tiktok.get(
                    "sharesObservados"
                ),

                {

                    "semantics":
                        "observed_since_monitoring"

                }

            )


            _emit_if_changed(

                "tiktok",

                "gift_count",

                estado_tiktok.get(
                    "presentesObservados"
                ),

                {

                    "semantics":
                        "observed_since_monitoring"

                }

            )


            # =================================================
            # COMENTARIOS
            # =================================================

            for raw_comment in list(
                comentarios_tiktok
            ):

                comment = (
                    _engine_extract_comment(
                        raw_comment
                    )
                )


                # Nao emitir comentario vazio.

                if not comment.get(
                    "text"
                ):

                    continue


                signature = (
                    _engine_comment_signature(

                        "tiktok",

                        comment

                    )
                )


                if not _engine_seen_add(

                    _live_engine_seen_comments,

                    _live_engine_seen_comment_order,

                    signature

                ):

                    continue


                live_engine_emit(

                    "tiktok",

                    "comment",

                    {

                        "id":
                            comment[
                                "id"
                            ],

                        "user":
                            comment[
                                "user"
                            ],

                        "text":
                            comment[
                                "text"
                            ],

                        "display_time":
                            comment[
                                "display_time"
                            ],

                    }

                )


            # =================================================
            # PRESENTES INDIVIDUAIS
            # =================================================

            if (
                "presentes_tiktok"
                in globals()
            ):

                for raw_gift in list(
                    presentes_tiktok
                ):

                    gift = (
                        _engine_extract_gift(
                            raw_gift
                        )
                    )


                    signature = (

                        str(
                            gift.get(
                                "id"
                            )
                        )

                        if gift.get(
                            "id"
                        )

                        else

                        (
                            str(
                                gift.get(
                                    "user"
                                )
                            )

                            +

                            "|"

                            +

                            str(
                                gift.get(
                                    "gift"
                                )
                            )

                            +

                            "|"

                            +

                            str(
                                gift.get(
                                    "count"
                                )
                            )
                        )

                    )


                    if not _engine_seen_add(

                        _live_engine_seen_gifts,

                        _live_engine_seen_gift_order,

                        signature

                    ):

                        continue


                    live_engine_emit(

                        "tiktok",

                        "gift",

                        {

                            "id":
                                gift.get(
                                    "id"
                                ),

                            "user":
                                gift.get(
                                    "user"
                                ),

                            "gift":
                                gift.get(
                                    "gift"
                                ),

                            "count":
                                gift.get(
                                    "count"
                                ),

                        }

                    )


        except Exception:

            pass


        await asyncio.sleep(
            LIVE_ENGINE_POLL
        )


# ============================================================
# 20. RESET DO LIVE ENGINE
#
# Chamado antes de cada nova LIVE.
# ============================================================

def reset_live_engine(
    platform
):

    global live_engine_channels

    global _live_engine_seq
    global _live_engine_running
    global _live_engine_platform
    global _live_engine_started_at


    # ========================================================
    # ESTADO
    # ========================================================

    _live_engine_seq = 0


    _live_engine_running = True


    _live_engine_platform = (
        platform
    )


    _live_engine_started_at = (
        time.time()
    )


    # ========================================================
    # MEMORIA DA LIVE ANTERIOR
    # ========================================================

    live_engine_history.clear()


    live_engine_snapshot.clear()


    _live_engine_last_values.clear()


    _live_engine_seen_comments.clear()


    _live_engine_seen_comment_order.clear()


    _live_engine_seen_gifts.clear()


    _live_engine_seen_gift_order.clear()


    # ========================================================
    # NOVAS FILAS
    #
    # Criadas dentro do mesmo Event Loop que executa
    # a LIVE.
    # ========================================================

    live_engine_channels = {

        "coach_comments":

            asyncio.Queue(

                maxsize=
                    LIVE_ENGINE_QUEUE_LIMITS[
                        "coach_comments"
                    ]

            ),


        "coach_audience":

            asyncio.Queue(

                maxsize=
                    LIVE_ENGINE_QUEUE_LIMITS[
                        "coach_audience"
                    ]

            ),


        "coach_storage":

            asyncio.Queue(

                maxsize=
                    LIVE_ENGINE_QUEUE_LIMITS[
                        "coach_storage"
                    ]

            ),

    }


    # ========================================================
    # RESET DAS ESTATISTICAS
    # ========================================================

    for key in list(
        live_engine_routing_stats.keys()
    ):

        live_engine_routing_stats[
            key
        ] = 0


# ============================================================
# 21. PARAR LIVE ENGINE
# ============================================================

def stop_live_engine():

    global _live_engine_running


    _live_engine_running = False


# ============================================================
# 22. STATUS
# ============================================================

def live_engine_is_running():

    return bool(
        _live_engine_running
    )


def live_engine_platform():

    return (
        _live_engine_platform
    )


# ============================================================
# 23. INTERFACE
#
# A Interface V6 continua usando estas funcoes.
# ============================================================

def live_engine_recent_events(
    limit=20
):

    if limit <= 0:

        return []


    return list(
        live_engine_history
    )[-limit:]


def live_engine_current_snapshot():

    return dict(
        live_engine_snapshot
    )


# ============================================================
# 24. FUNCAO GENERICA PARA CONSUMIR UM CANAL
# ============================================================

async def live_engine_next_channel_event(
    channel_name,
    timeout=None
):

    queue = (
        live_engine_channels.get(
            channel_name
        )
    )


    if queue is None:

        raise RuntimeError(

            f"Canal '{channel_name}' "
            "ainda nao foi inicializado. "
            "Inicie uma LIVE primeiro."

        )


    if timeout is None:

        return await queue.get()


    return await asyncio.wait_for(

        queue.get(),

        timeout=timeout

    )


# ============================================================
# 25. SAIDA:
# WORKER COACH COMENTARIOS
# ============================================================

async def live_engine_next_comment_event(
    timeout=None
):

    return await (
        live_engine_next_channel_event(

            "coach_comments",

            timeout

        )
    )


# ============================================================
# 26. SAIDA:
# WORKER COACH AUDIENCIA
# ============================================================

async def live_engine_next_audience_event(
    timeout=None
):

    return await (
        live_engine_next_channel_event(

            "coach_audience",

            timeout

        )
    )


# ============================================================
# 27. SAIDA:
# ARMAZENAMENTO COACH
# ============================================================

async def live_engine_next_storage_event(
    timeout=None
):

    return await (
        live_engine_next_channel_event(

            "coach_storage",

            timeout

        )
    )


# ============================================================
# 28. TAMANHO ATUAL DAS FILAS
# ============================================================

def live_engine_channel_sizes():

    resultado = {}


    for nome, queue in (
        live_engine_channels.items()
    ):

        if queue is None:

            resultado[
                nome
            ] = None

        else:

            resultado[
                nome
            ] = queue.qsize()


    return resultado


# ============================================================
# 29. INICIAR OBSERVADOR DO ENGINE
# ============================================================

def start_live_engine(
    platform
):

    reset_live_engine(
        platform
    )


    if platform == "shopee":

        return asyncio.create_task(

            live_engine_watch_shopee()

        )


    if platform == "tiktok":

        return asyncio.create_task(

            live_engine_watch_tiktok()

        )


    raise ValueError(

        "Plataforma invalida: "
        +
        str(
            platform
        )

    )


# ============================================================
# 30. PARAR TASK DO OBSERVADOR
# ============================================================

async def stop_live_engine_task(
    task
):

    stop_live_engine()


    if task is None:

        return


    if not task.done():

        task.cancel()


    try:

        await task

    except BaseException:

        pass


# ============================================================
# 31. SHOPEE + LIVE ENGINE
#
# NOME MANTIDO PARA A INTERFACE V6.
# ============================================================

async def executar_shopee_com_live_engine():

    engine_task = (
        start_live_engine(
            "shopee"
        )
    )


    try:

        await main()


    finally:

        await stop_live_engine_task(
            engine_task
        )


# ============================================================
# 32. TIKTOK + LIVE ENGINE
#
# NOME MANTIDO PARA A INTERFACE V6.
# ============================================================

async def executar_tiktok_com_live_engine(
    username
):

    engine_task = (
        start_live_engine(
            "tiktok"
        )
    )


    try:

        await monitorar_tiktok(
            username
        )


    finally:

        await stop_live_engine_task(
            engine_task
        )


# ============================================================
# 33. DIAGNOSTICO COMPLETO
# ============================================================

def mostrar_live_engine(
    ultimos=12
):

    print(
        "=" * 65
    )

    print(
        "AGCN LIVE ENGINE V2"
    )

    print(
        "=" * 65
    )


    print(
        "Rodando:",
        live_engine_is_running()
    )


    print(
        "Plataforma:",
        live_engine_platform()
    )


    print(
        "Eventos no historico:",
        len(
            live_engine_history
        )
    )


    print()


    print(
        "FILAS:"
    )


    sizes = (
        live_engine_channel_sizes()
    )


    print(

        "  Worker Coach Comentarios:",

        sizes.get(
            "coach_comments"
        )

    )


    print(

        "  Worker Coach Audiencia:",

        sizes.get(
            "coach_audience"
        )

    )


    print(

        "  Armazenamento Coach:",

        sizes.get(
            "coach_storage"
        )

    )


    print()


    print(
        "ROTEAMENTO:"
    )


    print(

        "  Interface:",

        live_engine_routing_stats[
            "interface"
        ]

    )


    print(

        "  Coach Comentarios:",

        live_engine_routing_stats[
            "coach_comments"
        ]

    )


    print(

        "  Coach Audiencia:",

        live_engine_routing_stats[
            "coach_audience"
        ]

    )


    print(

        "  Coach Armazenamento:",

        live_engine_routing_stats[
            "coach_storage"
        ]

    )


    print()


    print(
        "EVENTOS DESCARTADOS POR FILA CHEIA:"
    )


    print(

        "  Comentarios:",

        live_engine_routing_stats[
            "dropped_comments"
        ]

    )


    print(

        "  Audiencia:",

        live_engine_routing_stats[
            "dropped_audience"
        ]

    )


    print(

        "  Armazenamento:",

        live_engine_routing_stats[
            "dropped_storage"
        ]

    )


    print()


    print(
        f"ULTIMOS {ultimos} EVENTOS:"
    )


    for evento in (
        live_engine_recent_events(
            ultimos
        )
    ):

        print(

            evento.get(
                "seq"
            ),

            "|",

            evento.get(
                "platform"
            ),

            "|",

            evento.get(
                "type"
            ),

            "|",

            evento.get(
                "payload"
            )

        )


# ============================================================
# 34. DIAGNOSTICO SOMENTE DAS ROTAS
# ============================================================

def mostrar_rotas_live_engine():

    sizes = (
        live_engine_channel_sizes()
    )


    print(
        "=" * 65
    )

    print(
        "LIVE ENGINE V2 - ROTAS"
    )

    print(
        "=" * 65
    )


    print()


    print(
        "INTERFACE"
    )

    print(
        "  Eventos disponibilizados:",
        live_engine_routing_stats[
            "interface"
        ]
    )


    print()


    print(
        "WORKER COACH COMENTARIOS"
    )

    print(
        "  Eventos enviados:",
        live_engine_routing_stats[
            "coach_comments"
        ]
    )

    print(
        "  Aguardando consumo:",
        sizes.get(
            "coach_comments"
        )
    )


    print()


    print(
        "WORKER COACH AUDIENCIA"
    )

    print(
        "  Eventos enviados:",
        live_engine_routing_stats[
            "coach_audience"
        ]
    )

    print(
        "  Aguardando consumo:",
        sizes.get(
            "coach_audience"
        )
    )


    print()


    print(
        "ARMAZENAMENTO COACH"
    )

    print(
        "  Eventos enviados:",
        live_engine_routing_stats[
            "coach_storage"
        ]
    )

    print(
        "  Aguardando consumo:",
        sizes.get(
            "coach_storage"
        )
    )


# ============================================================
# 35. PRONTO
# ============================================================

print(
    "AGCN Live Engine V2 carregado."
)

print(
    "Normalizacao de comentarios ativa."
)

print(
    "Saidas:"
)

print(
    "1. Interface"
)

print(
    "2. Worker Coach Comentarios"
)

print(
    "3. Worker Coach Audiencia"
)

print(
    "4. Armazenamento Coach"
)
