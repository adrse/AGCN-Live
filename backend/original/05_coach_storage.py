# ============================================================
# AGCN - COACH ARMAZENAMENTO V1
#
# RESPONSABILIDADE:
#
# LIVE ENGINE
#      ↓
# COACH ARMAZENAMENTO
#      ↕
# FUTURO COACH ORCHESTRATOR
#
#
# NAO existe comunicacao direta:
#
# Worker Coach Comentarios X Armazenamento
# Worker Coach Audiencia   X Armazenamento
#
#
# O Armazenamento recebe:
#
# 1. TODOS os eventos brutos do Live Engine
#
# Futuramente recebe do Orchestrator:
#
# 2. sinais que o Orchestrator decidiu registrar
# 3. decisoes tomadas
# 4. dicas efetivamente mostradas
# 5. dicas descartadas/suprimidas
#
#
# O Orchestrator podera consultar:
#
# - historico de audiencia
# - historico de likes
# - comentarios recentes
# - metricas recentes
# - dicas ja dadas
# - quando uma dica foi dada
# - contexto dos ultimos segundos/minutos
#
#
# V1:
# armazenamento somente em RAM.
#
# Quando o Colab reiniciar, os dados desaparecem.
#
# Futuramente:
# banco de dados persistente.
# ============================================================


import asyncio
import time
import uuid
import copy
import threading

from collections import deque, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo


# ============================================================
# 1. VERIFICAR LIVE ENGINE V2
# ============================================================

necessarios_storage = [

    "live_engine_next_storage_event",

    "live_engine_is_running",

    "live_engine_platform",

    "executar_shopee_com_live_engine",

    "executar_tiktok_com_live_engine",

]


faltando_storage = [

    nome

    for nome in necessarios_storage

    if nome not in globals()

]


if faltando_storage:

    raise RuntimeError(

        "Execute primeiro o Live Engine V2. "
        "Faltando: "

        + ", ".join(
            faltando_storage
        )

    )


# ============================================================
# 2. CONFIGURACAO
# ============================================================

COACH_STORAGE_TIMEZONE = ZoneInfo(
    "America/Araguaina"
)


# Quantas lives manter em memoria.

COACH_STORAGE_MAX_SESSIONS = 50


# Limites POR LIVE.

COACH_STORAGE_MAX_RAW_EVENTS = 30000

COACH_STORAGE_MAX_COMMENTS = 10000

COACH_STORAGE_MAX_METRIC_POINTS = 20000

COACH_STORAGE_MAX_ORCHESTRATOR_SIGNALS = 5000

COACH_STORAGE_MAX_DECISIONS = 5000

COACH_STORAGE_MAX_OUTPUTS = 5000


# ============================================================
# 3. TIPOS CONSIDERADOS METRICAS
# ============================================================

COACH_STORAGE_METRIC_TYPES = {

    "viewer_count",

    "like_count",

    "share_count",

    "follow_count",

    "gift_count",

    "total_user",

    "member_count",

    "product_count",

}


# ============================================================
# 4. ESTADO GLOBAL
# ============================================================

# run_id -> dados da LIVE

coach_storage_sessions = {}


# Ordem de criacao das lives.

coach_storage_session_order = deque()


# LIVE atual.

coach_storage_current_run_id = None


# Estado do consumidor.

coach_storage_running = False

coach_storage_last_error = None

coach_storage_events_received = 0


# Lock para permitir consultas do notebook
# enquanto a thread da Interface V6 grava dados.

coach_storage_lock = threading.RLock()


# ============================================================
# 5. HORARIOS
# ============================================================

def coach_storage_now_iso():

    return (
        datetime
        .now(
            COACH_STORAGE_TIMEZONE
        )
        .isoformat()
    )


# ============================================================
# 6. CRIAR UMA NOVA SESSAO DE ARMAZENAMENTO
#
# Uma sessao = uma execucao de monitoramento de LIVE.
#
# Mesmo que o live_id ainda nao tenha chegado,
# usamos um run_id interno.
# ============================================================

def coach_storage_create_session(
    platform
):

    global coach_storage_current_run_id


    run_id = uuid.uuid4().hex


    session = {

        # ====================================================
        # IDENTIDADE INTERNA
        # ====================================================

        "run_id":
            run_id,

        "platform":
            platform,

        "live_id":
            None,

        "subject":
            None,


        # ====================================================
        # TEMPO
        # ====================================================

        "started_at":
            time.time(),

        "started_iso":
            coach_storage_now_iso(),

        "ended_at":
            None,

        "ended_iso":
            None,

        "status":
            "running",


        # ====================================================
        # EVENTOS ORIGINAIS DO LIVE ENGINE
        # ====================================================

        "raw_events":
            deque(
                maxlen=
                    COACH_STORAGE_MAX_RAW_EVENTS
            ),


        # ====================================================
        # COMENTARIOS
        # ====================================================

        "comments":
            deque(
                maxlen=
                    COACH_STORAGE_MAX_COMMENTS
            ),


        # ====================================================
        # SERIES TEMPORAIS
        #
        # Exemplo:
        #
        # metrics["viewer_count"]
        # metrics["like_count"]
        # ====================================================

        "metrics":
            defaultdict(

                lambda:
                    deque(
                        maxlen=
                            COACH_STORAGE_MAX_METRIC_POINTS
                    )

            ),


        # ====================================================
        # CONTAGEM DOS EVENTOS
        # ====================================================

        "event_counts":
            defaultdict(
                int
            ),


        # ====================================================
        # FUTURO:
        # sinais recebidos pelos Workers e aceitos/registrados
        # pelo Orchestrator.
        #
        # IMPORTANTE:
        # Workers nao escrevem aqui diretamente.
        # ====================================================

        "orchestrator_signals":
            deque(
                maxlen=
                    COACH_STORAGE_MAX_ORCHESTRATOR_SIGNALS
            ),


        # ====================================================
        # DECISOES DO ORCHESTRATOR
        # ====================================================

        "orchestrator_decisions":
            deque(
                maxlen=
                    COACH_STORAGE_MAX_DECISIONS
            ),


        # ====================================================
        # DICAS / MENSAGENS FINAIS DO COACH
        # ====================================================

        "coach_outputs":
            deque(
                maxlen=
                    COACH_STORAGE_MAX_OUTPUTS
            ),

    }


    with coach_storage_lock:

        # ====================================================
        # LIMITE DE SESSOES EM RAM
        # ====================================================

        while (

            len(
                coach_storage_session_order
            )

            >=

            COACH_STORAGE_MAX_SESSIONS

        ):

            old_run_id = (
                coach_storage_session_order
                .popleft()
            )


            coach_storage_sessions.pop(
                old_run_id,
                None
            )


        coach_storage_sessions[
            run_id
        ] = session


        coach_storage_session_order.append(
            run_id
        )


        coach_storage_current_run_id = (
            run_id
        )


    return run_id


# ============================================================
# 7. PEGAR SESSAO ATUAL
# ============================================================

def coach_storage_current_session():

    with coach_storage_lock:

        if (
            coach_storage_current_run_id
            is None
        ):

            return None


        return (
            coach_storage_sessions.get(
                coach_storage_current_run_id
            )
        )


# ============================================================
# 8. PEGAR SESSAO POR RUN_ID
# ============================================================

def coach_storage_get_session(
    run_id=None
):

    with coach_storage_lock:

        if run_id is None:

            run_id = (
                coach_storage_current_run_id
            )


        if run_id is None:

            return None


        return (
            coach_storage_sessions.get(
                run_id
            )
        )


# ============================================================
# 9. FINALIZAR SESSAO
# ============================================================

def coach_storage_finalize_session(
    run_id=None
):

    session = (
        coach_storage_get_session(
            run_id
        )
    )


    if session is None:

        return


    with coach_storage_lock:

        if (
            session[
                "ended_at"
            ]
            is None
        ):

            session[
                "ended_at"
            ] = time.time()


            session[
                "ended_iso"
            ] = coach_storage_now_iso()


        session[
            "status"
        ] = "finished"


# ============================================================
# 10. COPIA SEGURA DO EVENTO
# ============================================================

def coach_storage_copy(
    value
):

    try:

        return copy.deepcopy(
            value
        )

    except Exception:

        try:

            return dict(
                value
            )

        except Exception:

            return value


# ============================================================
# 11. ARMAZENAR EVENTO DO LIVE ENGINE
# ============================================================

def coach_storage_ingest_event(
    event,
    run_id=None
):

    global coach_storage_events_received


    if not isinstance(
        event,
        dict
    ):

        return


    session = (
        coach_storage_get_session(
            run_id
        )
    )


    if session is None:

        return


    stored_event = (
        coach_storage_copy(
            event
        )
    )


    event_type = (
        stored_event.get(
            "type"
        )
    )


    payload = (

        stored_event.get(
            "payload"
        )

        or

        {}

    )


    with coach_storage_lock:

        # ====================================================
        # IDENTIDADE
        # ====================================================

        if (
            stored_event.get(
                "platform"
            )
        ):

            session[
                "platform"
            ] = stored_event.get(
                "platform"
            )


        if (
            stored_event.get(
                "live_id"
            )
        ):

            session[
                "live_id"
            ] = stored_event.get(
                "live_id"
            )


        if (
            stored_event.get(
                "subject"
            )
        ):

            session[
                "subject"
            ] = stored_event.get(
                "subject"
            )


        # ====================================================
        # RAW EVENT
        # ====================================================

        session[
            "raw_events"
        ].append(
            stored_event
        )


        # ====================================================
        # CONTADOR
        # ====================================================

        if event_type:

            session[
                "event_counts"
            ][
                event_type
            ] += 1


        coach_storage_events_received += 1


        # ====================================================
        # COMENTARIOS
        # ====================================================

        if (
            event_type
            ==
            "comment"
        ):

            session[
                "comments"
            ].append({

                "event_id":
                    stored_event.get(
                        "event_id"
                    ),

                "timestamp":
                    stored_event.get(
                        "timestamp"
                    ),

                "iso_time":
                    stored_event.get(
                        "iso_time"
                    ),

                "platform":
                    stored_event.get(
                        "platform"
                    ),

                "live_id":
                    stored_event.get(
                        "live_id"
                    ),

                "user":
                    payload.get(
                        "user"
                    ),

                "text":
                    payload.get(
                        "text"
                    ),

                "display_time":
                    payload.get(
                        "display_time"
                    ),

            })


        # ====================================================
        # METRICAS
        # ====================================================

        if (
            event_type
            in
            COACH_STORAGE_METRIC_TYPES
        ):

            session[
                "metrics"
            ][
                event_type
            ].append({

                "timestamp":
                    stored_event.get(
                        "timestamp"
                    ),

                "iso_time":
                    stored_event.get(
                        "iso_time"
                    ),

                "value":
                    payload.get(
                        "value"
                    ),

                "semantics":
                    payload.get(
                        "semantics"
                    ),

                "platform":
                    stored_event.get(
                        "platform"
                    ),

                "live_id":
                    stored_event.get(
                        "live_id"
                    ),

            })


# ============================================================
# 12. LOOP CONSUMIDOR
#
# Consome EXCLUSIVAMENTE:
#
# Live Engine
#      ↓
# canal coach_storage
#
# Nenhum Worker Coach participa aqui.
# ============================================================

async def coach_storage_consumer(
    run_id
):

    global coach_storage_running
    global coach_storage_last_error


    coach_storage_running = True

    coach_storage_last_error = None


    try:

        while True:

            event = None


            try:

                event = (
                    await
                    live_engine_next_storage_event(
                        timeout=0.5
                    )
                )


            except asyncio.TimeoutError:

                event = None


            except asyncio.CancelledError:

                raise


            except Exception as e:

                coach_storage_last_error = (

                    f"{type(e).__name__}: "
                    f"{e}"

                )


                await asyncio.sleep(
                    0.1
                )


            # =================================================
            # ARMAZENAR
            # =================================================

            if event is not None:

                coach_storage_ingest_event(

                    event,

                    run_id=
                        run_id

                )


                continue


            # =================================================
            # ENGINE PAROU
            #
            # Se nao ha evento esperando,
            # o armazenamento terminou esta LIVE.
            # =================================================

            if not live_engine_is_running():

                break


    except asyncio.CancelledError:

        raise


    finally:

        coach_storage_running = False


        coach_storage_finalize_session(
            run_id
        )


# ============================================================
# 13. SUPERVISOR
#
# O wrapper e iniciado ANTES de o Live Engine
# criar suas filas.
#
# Portanto esperamos a fila existir.
# ============================================================

async def coach_storage_supervisor(
    platform
):

    run_id = (
        coach_storage_create_session(
            platform
        )
    )


    inicio = time.time()


    try:

        while True:

            # =================================================
            # SEGURANCA:
            # nao ficar esperando eternamente.
            # =================================================

            if (
                time.time()
                -
                inicio
                >
                30
            ):

                coach_storage_last_error = (
                    "Live Engine nao iniciou "
                    "dentro de 30 segundos."
                )

                return


            try:

                engine_running = (
                    live_engine_is_running()
                )


                engine_platform = (
                    live_engine_platform()
                )


                storage_queue = (

                    live_engine_channels.get(
                        "coach_storage"
                    )

                    if (
                        "live_engine_channels"
                        in globals()
                    )

                    else

                    None

                )


            except Exception:

                engine_running = False

                engine_platform = None

                storage_queue = None


            if (

                engine_running

                and

                engine_platform
                ==
                platform

                and

                storage_queue
                is not None

            ):

                break


            await asyncio.sleep(
                0.05
            )


        # ====================================================
        # COMECA A CONSUMIR
        # ====================================================

        await coach_storage_consumer(
            run_id
        )


    finally:

        coach_storage_finalize_session(
            run_id
        )


# ============================================================
# 14. REGISTRAR SINAL PELO FUTURO ORCHESTRATOR
#
# IMPORTANTE:
#
# Essa funcao sera chamada pelo ORCHESTRATOR.
# Nao pelos Worker Coach.
# ============================================================

def coach_storage_save_orchestrator_signal(
    signal,
    run_id=None
):

    session = (
        coach_storage_get_session(
            run_id
        )
    )


    if session is None:

        return None


    registro = {

        "id":
            uuid.uuid4().hex,

        "timestamp":
            time.time(),

        "iso_time":
            coach_storage_now_iso(),

        "data":
            coach_storage_copy(
                signal
            ),

    }


    with coach_storage_lock:

        session[
            "orchestrator_signals"
        ].append(
            registro
        )


    return registro


# ============================================================
# 15. REGISTRAR DECISAO DO FUTURO ORCHESTRATOR
#
# Exemplo futuro:
#
# {
#   "action": "merge",
#   "signals": [...],
#   "priority": 90,
#   "reason": "preco + intencao de compra"
# }
# ============================================================

def coach_storage_save_decision(
    decision,
    run_id=None
):

    session = (
        coach_storage_get_session(
            run_id
        )
    )


    if session is None:

        return None


    registro = {

        "id":
            uuid.uuid4().hex,

        "timestamp":
            time.time(),

        "iso_time":
            coach_storage_now_iso(),

        "data":
            coach_storage_copy(
                decision
            ),

    }


    with coach_storage_lock:

        session[
            "orchestrator_decisions"
        ].append(
            registro
        )


    return registro


# ============================================================
# 16. REGISTRAR DICA FINAL
#
# Isso sera chamado pelo Coach Orchestrator
# quando decidir enviar algo para a Interface.
# ============================================================

def coach_storage_save_output(

    text,

    category=None,

    priority=None,

    source_signals=None,

    displayed=True,

    run_id=None

):

    session = (
        coach_storage_get_session(
            run_id
        )
    )


    if session is None:

        return None


    registro = {

        "id":
            uuid.uuid4().hex,

        "timestamp":
            time.time(),

        "iso_time":
            coach_storage_now_iso(),

        "text":
            str(
                text
            ),

        "category":
            category,

        "priority":
            priority,

        "displayed":
            bool(
                displayed
            ),

        "source_signals":
            coach_storage_copy(

                source_signals
                or
                []

            ),

    }


    with coach_storage_lock:

        session[
            "coach_outputs"
        ].append(
            registro
        )


    return registro


# ============================================================
# 17. CONSULTAR COMENTARIOS RECENTES
#
# Futuro Orchestrator podera usar.
# ============================================================

def coach_storage_recent_comments(

    seconds=60,

    limit=100,

    run_id=None

):

    session = (
        coach_storage_get_session(
            run_id
        )
    )


    if session is None:

        return []


    limite_tempo = (
        time.time()
        -
        seconds
    )


    with coach_storage_lock:

        resultado = [

            coach_storage_copy(
                item
            )

            for item
            in session[
                "comments"
            ]

            if (

                item.get(
                    "timestamp"
                )

                is not None

                and

                item.get(
                    "timestamp"
                )
                >=
                limite_tempo

            )

        ]


    if limit is not None:

        resultado = (
            resultado[
                -limit:
            ]
        )


    return resultado


# ============================================================
# 18. CONSULTAR SERIE DE UMA METRICA
#
# Exemplo:
#
# coach_storage_metric_window(
#     "viewer_count",
#     seconds=60
# )
# ============================================================

def coach_storage_metric_window(

    metric_type,

    seconds=60,

    run_id=None

):

    session = (
        coach_storage_get_session(
            run_id
        )
    )


    if session is None:

        return []


    limite_tempo = (
        time.time()
        -
        seconds
    )


    with coach_storage_lock:

        serie = list(

            session[
                "metrics"
            ].get(
                metric_type,
                []
            )

        )


    return [

        coach_storage_copy(
            item
        )

        for item
        in serie

        if (

            item.get(
                "timestamp"
            )

            is not None

            and

            item.get(
                "timestamp"
            )
            >=
            limite_tempo

        )

    ]


# ============================================================
# 19. ULTIMO VALOR DE UMA METRICA
# ============================================================

def coach_storage_latest_metric(

    metric_type,

    run_id=None

):

    session = (
        coach_storage_get_session(
            run_id
        )
    )


    if session is None:

        return None


    with coach_storage_lock:

        serie = (

            session[
                "metrics"
            ].get(
                metric_type
            )

        )


        if not serie:

            return None


        return (
            coach_storage_copy(
                serie[-1]
            )
        )


# ============================================================
# 20. CONSULTAR DICAS RECENTES
#
# Essencial para cooldown do Orchestrator.
# ============================================================

def coach_storage_recent_outputs(

    seconds=120,

    category=None,

    only_displayed=True,

    run_id=None

):

    session = (
        coach_storage_get_session(
            run_id
        )
    )


    if session is None:

        return []


    limite_tempo = (
        time.time()
        -
        seconds
    )


    with coach_storage_lock:

        saidas = list(
            session[
                "coach_outputs"
            ]
        )


    resultado = []


    for item in saidas:

        if (

            item.get(
                "timestamp"
            )

            is None

            or

            item.get(
                "timestamp"
            )
            <
            limite_tempo

        ):

            continue


        if (

            category is not None

            and

            item.get(
                "category"
            )
            !=
            category

        ):

            continue


        if (

            only_displayed

            and

            not item.get(
                "displayed",
                False
            )

        ):

            continue


        resultado.append(

            coach_storage_copy(
                item
            )

        )


    return resultado


# ============================================================
# 21. VERIFICAR SE UMA CATEGORIA JA FOI MOSTRADA
#
# Futuro uso:
#
# if coach_storage_was_recently_shown(
#     "price",
#     seconds=60
# ):
#     nao repetir
# ============================================================

def coach_storage_was_recently_shown(

    category,

    seconds=60,

    run_id=None

):

    saidas = (
        coach_storage_recent_outputs(

            seconds=seconds,

            category=category,

            only_displayed=True,

            run_id=run_id

        )
    )


    return bool(
        saidas
    )


# ============================================================
# 22. EVENTOS RECENTES
# ============================================================

def coach_storage_recent_events(

    seconds=60,

    event_type=None,

    limit=500,

    run_id=None

):

    session = (
        coach_storage_get_session(
            run_id
        )
    )


    if session is None:

        return []


    limite_tempo = (
        time.time()
        -
        seconds
    )


    with coach_storage_lock:

        eventos = list(
            session[
                "raw_events"
            ]
        )


    resultado = []


    for event in eventos:

        timestamp = (
            event.get(
                "timestamp"
            )
        )


        if (

            timestamp is None

            or

            timestamp
            <
            limite_tempo

        ):

            continue


        if (

            event_type is not None

            and

            event.get(
                "type"
            )
            !=
            event_type

        ):

            continue


        resultado.append(

            coach_storage_copy(
                event
            )

        )


    if limit is not None:

        resultado = (
            resultado[
                -limit:
            ]
        )


    return resultado


# ============================================================
# 23. CONTEXTO RESUMIDO PARA O FUTURO ORCHESTRATOR
#
# Esta funcao vai ficar muito util depois.
#
# Retorna uma fotografia dos ultimos X segundos.
# ============================================================

def coach_storage_context(

    seconds=60,

    run_id=None

):

    session = (
        coach_storage_get_session(
            run_id
        )
    )


    if session is None:

        return None


    comments = (
        coach_storage_recent_comments(

            seconds=seconds,

            limit=200,

            run_id=run_id

        )
    )


    metricas = {}


    for metric_type in (
        COACH_STORAGE_METRIC_TYPES
    ):

        serie = (
            coach_storage_metric_window(

                metric_type,

                seconds=seconds,

                run_id=run_id

            )
        )


        if serie:

            metricas[
                metric_type
            ] = {

                "points":
                    len(
                        serie
                    ),

                "first":
                    serie[0].get(
                        "value"
                    ),

                "last":
                    serie[-1].get(
                        "value"
                    ),

            }


    return {

        "run_id":
            session.get(
                "run_id"
            ),

        "platform":
            session.get(
                "platform"
            ),

        "live_id":
            session.get(
                "live_id"
            ),

        "subject":
            session.get(
                "subject"
            ),

        "seconds":
            seconds,

        "comments":
            len(
                comments
            ),

        "metrics":
            metricas,

        "recent_outputs":
            coach_storage_recent_outputs(

                seconds=seconds,

                run_id=run_id

            ),

    }


# ============================================================
# 24. LISTAR LIVES ARMAZENADAS
# ============================================================

def coach_storage_list_sessions():

    resultado = []


    with coach_storage_lock:

        for run_id in (
            coach_storage_session_order
        ):

            session = (
                coach_storage_sessions.get(
                    run_id
                )
            )


            if session is None:

                continue


            resultado.append({

                "run_id":
                    run_id,

                "platform":
                    session.get(
                        "platform"
                    ),

                "live_id":
                    session.get(
                        "live_id"
                    ),

                "subject":
                    session.get(
                        "subject"
                    ),

                "status":
                    session.get(
                        "status"
                    ),

                "started_iso":
                    session.get(
                        "started_iso"
                    ),

                "ended_iso":
                    session.get(
                        "ended_iso"
                    ),

                "events":
                    len(
                        session[
                            "raw_events"
                        ]
                    ),

                "comments":
                    len(
                        session[
                            "comments"
                        ]
                    ),

                "outputs":
                    len(
                        session[
                            "coach_outputs"
                        ]
                    ),

            })


    return resultado


# ============================================================
# 25. ENCONTRAR LIVE POR PLATAFORMA + LIVE_ID
# ============================================================

def coach_storage_find_session(

    platform,

    live_id

):

    live_id = str(
        live_id
    )


    with coach_storage_lock:

        for run_id in reversed(
            coach_storage_session_order
        ):

            session = (
                coach_storage_sessions.get(
                    run_id
                )
            )


            if session is None:

                continue


            if (

                session.get(
                    "platform"
                )
                ==
                platform

                and

                str(
                    session.get(
                        "live_id"
                    )
                )
                ==
                live_id

            ):

                return session


    return None


# ============================================================
# 26. DIAGNOSTICO
# ============================================================

def mostrar_coach_armazenamento():

    session = (
        coach_storage_current_session()
    )


    print(
        "=" * 68
    )

    print(
        "AGCN - COACH ARMAZENAMENTO V1"
    )

    print(
        "=" * 68
    )


    print(
        "Consumidor ativo:",
        coach_storage_running
    )


    print(
        "Eventos recebidos nesta sessao do Colab:",
        coach_storage_events_received
    )


    print(
        "Erro:",
        coach_storage_last_error
    )


    print(
        "Lives armazenadas:",
        len(
            coach_storage_sessions
        )
    )


    print()


    if session is None:

        print(
            "Nenhuma LIVE armazenada ainda."
        )

        return


    print(
        "LIVE ATUAL"
    )

    print(
        "Plataforma:",
        session.get(
            "platform"
        )
    )

    print(
        "Live ID:",
        session.get(
            "live_id"
        )
    )

    print(
        "Subject:",
        session.get(
            "subject"
        )
    )

    print(
        "Status:",
        session.get(
            "status"
        )
    )

    print(
        "Inicio:",
        session.get(
            "started_iso"
        )
    )


    print()


    print(
        "DADOS"
    )

    print(
        "Eventos brutos:",
        len(
            session[
                "raw_events"
            ]
        )
    )

    print(
        "Comentarios:",
        len(
            session[
                "comments"
            ]
        )
    )


    print()


    print(
        "METRICAS"
    )


    for metric_type, serie in (
        session[
            "metrics"
        ].items()
    ):

        if not serie:

            continue


        print(

            f"  {metric_type}: "
            f"{len(serie)} pontos | "
            f"ultimo = "
            f"{serie[-1].get('value')}"

        )


    print()


    print(
        "EVENTOS POR TIPO"
    )


    for event_type, count in sorted(

        session[
            "event_counts"
        ].items(),

        key=lambda item:
            item[1],

        reverse=True

    ):

        print(

            f"  {event_type}: "
            f"{count}"

        )


    print()


    print(
        "ORCHESTRATOR"
    )

    print(

        "Sinais registrados:",

        len(
            session[
                "orchestrator_signals"
            ]
        )

    )


    print(

        "Decisoes:",

        len(
            session[
                "orchestrator_decisions"
            ]
        )

    )


    print(

        "Dicas armazenadas:",

        len(
            session[
                "coach_outputs"
            ]
        )

    )


# ============================================================
# 27. INTEGRACAO AUTOMATICA COM O CICLO DA LIVE
#
# A Interface V6 ja chama:
#
# executar_shopee_com_live_engine()
#
# executar_tiktok_com_live_engine()
#
# Vamos envolver essas funcoes.
#
# Assim o Armazenamento inicia e encerra sozinho.
# ============================================================


# ============================================================
# DESCOBRIR A FUNCAO BASE ATUAL
#
# Isto permite reexecutar esta celula sem criar
# wrappers infinitos.
# ============================================================

_current_shopee_executor = (
    executar_shopee_com_live_engine
)


if getattr(

    _current_shopee_executor,

    "_agcn_storage_wrapper",

    False

):

    _coach_storage_base_shopee = (
        _current_shopee_executor
        ._agcn_storage_base
    )

else:

    _coach_storage_base_shopee = (
        _current_shopee_executor
    )


_current_tiktok_executor = (
    executar_tiktok_com_live_engine
)


if getattr(

    _current_tiktok_executor,

    "_agcn_storage_wrapper",

    False

):

    _coach_storage_base_tiktok = (
        _current_tiktok_executor
        ._agcn_storage_base
    )

else:

    _coach_storage_base_tiktok = (
        _current_tiktok_executor
    )


# ============================================================
# 28. HELPER PARA FINALIZAR STORAGE TASK
# ============================================================

async def _coach_storage_finish_task(
    task
):

    if task is None:

        return


    # ========================================================
    # Dar um pequeno tempo para drenar eventos restantes
    # depois que o Live Engine encerrar.
    # ========================================================

    try:

        await asyncio.wait_for(

            asyncio.shield(
                task
            ),

            timeout=2.0

        )


        return


    except asyncio.TimeoutError:

        pass


    except asyncio.CancelledError:

        pass


    except Exception:

        return


    # ========================================================
    # Se ainda estiver ativo, cancelar.
    # ========================================================

    if not task.done():

        task.cancel()


    try:

        await task

    except BaseException:

        pass


# ============================================================
# 29. SHOPEE + LIVE ENGINE + ARMAZENAMENTO
# ============================================================

async def executar_shopee_com_live_engine():

    storage_task = (
        asyncio.create_task(

            coach_storage_supervisor(
                "shopee"
            )

        )
    )


    try:

        return await (
            _coach_storage_base_shopee()
        )


    finally:

        await _coach_storage_finish_task(
            storage_task
        )


# Marcar wrapper.

executar_shopee_com_live_engine._agcn_storage_wrapper = True

executar_shopee_com_live_engine._agcn_storage_base = (
    _coach_storage_base_shopee
)


# ============================================================
# 30. TIKTOK + LIVE ENGINE + ARMAZENAMENTO
# ============================================================

async def executar_tiktok_com_live_engine(
    username
):

    storage_task = (
        asyncio.create_task(

            coach_storage_supervisor(
                "tiktok"
            )

        )
    )


    try:

        return await (
            _coach_storage_base_tiktok(
                username
            )
        )


    finally:

        await _coach_storage_finish_task(
            storage_task
        )


# Marcar wrapper.

executar_tiktok_com_live_engine._agcn_storage_wrapper = True

executar_tiktok_com_live_engine._agcn_storage_base = (
    _coach_storage_base_tiktok
)


# ============================================================
# 31. PRONTO
# ============================================================

print(
    "AGCN Coach Armazenamento V1 carregado."
)

print(
    "Entrada: Live Engine V2."
)

print(
    "Troca futura: somente com Coach Orchestrator."
)

print(
    "Nenhum Worker Coach acessa diretamente o armazenamento."
)
