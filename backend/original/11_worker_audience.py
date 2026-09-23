# ============================================================
# AGCN - WORKER COACH AUDIENCIA V1
#
# RESPONSABILIDADE:
#
# Live Engine V2
#      ↓
# canal "coach_audience"
#      ↓
# WORKER COACH AUDIENCIA
#      │
#      ├── recebe metricas normalizadas
#      ├── guarda janelas temporais curtas
#      ├── analisa espectadores
#      ├── analisa velocidade dos likes
#      ├── detecta compartilhamentos
#      ├── detecta follows
#      ├── detecta presentes
#      ├── respeita a semantica de cada metrica
#      └── produz SINAIS ESTRUTURADOS
#                     ↓
#            FUTURO COACH ORCHESTRATOR
#
#
# O WORKER NAO:
#
# - decide prioridade final
# - decide o que mostrar
# - escreve a fala do Coach
# - envia diretamente para Interface
# - acessa o Armazenamento Coach
#
#
# PRINCIPAIS SINAIS:
#
# viewer_growth
# viewer_drop
# viewer_stable
#
# like_acceleration
# like_slowdown
#
# share_activity
# follow_activity
# gift_activity
# gift_received
#
#
# IMPORTANTE:
#
# viewer_count
# -> audiencia atual
#
# like_count
# -> acumulado da LIVE
#
# TikTok share/follow/gift_count
# -> observado desde o inicio do monitoramento
#
# Shopee share_count
# -> total acumulado informado pela LIVE
#
# TikTok total_user
# -> metrica separada
# -> NAO e viewer_count
#
# Shopee member_count
# -> semantica ainda incerta
# -> NAO e usado para tendencia de audiencia
#
# ============================================================


import asyncio
import time
import uuid
import math
import statistics

from collections import deque, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo


# ============================================================
# 1. VERIFICAR LIVE ENGINE
# ============================================================

_WCA_REQUIRED = [

    "live_engine_next_audience_event",

    "live_engine_is_running",

    "live_engine_platform",

    "executar_shopee_com_live_engine",

    "executar_tiktok_com_live_engine",

]


_WCA_MISSING = [

    name

    for name in _WCA_REQUIRED

    if name not in globals()

]


if _WCA_MISSING:

    raise RuntimeError(

        "Execute primeiro o Live Engine V2. "
        "Faltando: "

        + ", ".join(
            _WCA_MISSING
        )

    )


# ============================================================
# 2. CONFIGURACAO
# ============================================================

WORKER_COACH_AUDIENCE_VERSION = "1.0"


WORKER_COACH_AUDIENCE_TIMEZONE = (
    ZoneInfo(
        "America/Araguaina"
    )
)


# ============================================================
# JANELA DE AUDIENCIA
#
# Comparamos:
#
# agora
# versus
# aproximadamente 30 segundos atras.
#
# Isso evita depender da velocidade de atualizacao
# de cada plataforma.
# ============================================================

WCA_VIEWER_WINDOW_SECONDS = 30


# Minimo de tempo de historico antes de analisar.

WCA_VIEWER_MIN_HISTORY_SECONDS = 20


# Mudanca minima percentual.

WCA_VIEWER_CHANGE_PERCENT = 8.0


# Mudanca absoluta minima.

WCA_VIEWER_MIN_ABSOLUTE_CHANGE = 5


# Considerado praticamente estavel.

WCA_VIEWER_STABLE_PERCENT = 3.0


# ============================================================
# LIKES
#
# Para aceleracao/desaceleracao:
#
# janela recente = 20 segundos
# janela anterior = 20 segundos
# ============================================================

WCA_LIKE_WINDOW_SECONDS = 20


# Mudanca minima da velocidade,
# em likes por minuto.

WCA_LIKE_MIN_RATE_DIFFERENCE = 5.0


# Razao minima para aceleracao.

WCA_LIKE_ACCELERATION_RATIO = 1.50


# Razao maxima para desaceleracao.

WCA_LIKE_SLOWDOWN_RATIO = 0.60


# ============================================================
# COOLDOWNS DOS SINAIS
#
# Isso NAO e prioridade.
#
# So evita o Worker gerar o mesmo sinal
# dezenas de vezes por segundo.
# ============================================================

WCA_COOLDOWN_VIEWER_DIRECTION = 15

WCA_COOLDOWN_VIEWER_STABLE = 60

WCA_COOLDOWN_LIKE = 20


# ============================================================
# MEMORIA
# ============================================================

WCA_MAX_METRIC_POINTS = 10000

WCA_MAX_OUTPUTS = 5000

WCA_OUTPUT_QUEUE_LIMIT = 5000


# ============================================================
# FREQUENCIA DA ANALISE PERIODICA
# ============================================================

WCA_PERIODIC_EVALUATION_SECONDS = 3.0


# ============================================================
# 3. TIPOS DE METRICAS
# ============================================================

WCA_METRIC_TYPES = {

    "viewer_count",

    "like_count",

    "share_count",

    "follow_count",

    "gift_count",

    "total_user",

    "member_count",

}


# ============================================================
# 4. ESTADO GLOBAL
# ============================================================

worker_coach_audience_output_queue = None


# Historico por metrica.

worker_coach_audience_metrics = defaultdict(

    lambda:
        deque(
            maxlen=WCA_MAX_METRIC_POINTS
        )

)


# Outputs produzidos.

worker_coach_audience_output_history = deque(
    maxlen=WCA_MAX_OUTPUTS
)


# Ultima emissao de cada sinal.

worker_coach_audience_last_emitted = {}


# Ultimo valor conhecido dos contadores
# share/follow/gift.

worker_coach_audience_counter_values = {}


# Presentes individuais ja vistos.

worker_coach_audience_seen_gifts = set()

worker_coach_audience_seen_gift_order = deque(
    maxlen=10000
)


# Estado da LIVE.

worker_coach_audience_running = False

worker_coach_audience_platform = None

worker_coach_audience_live_id = None

worker_coach_audience_subject = None

worker_coach_audience_started_at = None


# Contadores de diagnostico.

worker_coach_audience_events_received = 0

worker_coach_audience_outputs_emitted = 0

worker_coach_audience_dropped_outputs = 0

worker_coach_audience_last_error = None


worker_coach_audience_event_counts = defaultdict(
    int
)


# Controle da avaliacao periodica.

worker_coach_audience_last_periodic_eval = 0.0


# ============================================================
# 5. HORARIO
# ============================================================

def _wca_now_iso():

    return (
        datetime
        .now(
            WORKER_COACH_AUDIENCE_TIMEZONE
        )
        .isoformat()
    )


# ============================================================
# 6. CONVERTER NUMERO
# ============================================================

def _wca_number(
    value
):

    if value is None:

        return None


    try:

        number = float(
            value
        )


        if not math.isfinite(
            number
        ):

            return None


        return number


    except Exception:

        return None


# ============================================================
# 7. FORMATAR NUMERO
# ============================================================

def _wca_clean_number(
    value
):

    if value is None:

        return None


    try:

        value = float(
            value
        )


        if value.is_integer():

            return int(
                value
            )


        return round(
            value,
            3
        )


    except Exception:

        return value


# ============================================================
# 8. PEGAR SERIE DE UMA METRICA
# ============================================================

def _wca_series(
    metric_type
):

    return (
        worker_coach_audience_metrics.get(
            metric_type
        )
    )


# ============================================================
# 9. ULTIMO PONTO
# ============================================================

def _wca_latest_point(
    metric_type
):

    series = (
        _wca_series(
            metric_type
        )
    )


    if not series:

        return None


    return series[-1]


# ============================================================
# 10. VALOR EM OU ANTES DE UM HORARIO
#
# Fundamental para trabalhar com tempo real
# sem depender do numero de amostras.
# ============================================================

def _wca_value_at_or_before(
    metric_type,
    target_timestamp
):

    series = (
        _wca_series(
            metric_type
        )
    )


    if not series:

        return None


    for point in reversed(
        series
    ):

        timestamp = (
            point.get(
                "timestamp"
            )
        )


        if timestamp is None:

            continue


        if timestamp <= target_timestamp:

            return point


    return None


# ============================================================
# 11. QUANTIDADE DE PONTOS NUM INTERVALO
# ============================================================

def _wca_points_in_window(
    metric_type,
    seconds,
    now=None
):

    if now is None:

        now = time.time()


    minimum_time = (
        now
        -
        seconds
    )


    series = (
        _wca_series(
            metric_type
        )
    )


    if not series:

        return []


    return [

        point

        for point in series

        if (
            point.get(
                "timestamp"
            )
            is not None

            and

            point.get(
                "timestamp"
            )
            >=
            minimum_time
        )

    ]


# ============================================================
# 12. COOLDOWN
# ============================================================

def _wca_can_emit(
    key,
    cooldown_seconds,
    now=None
):

    if now is None:

        now = time.time()


    previous = (
        worker_coach_audience_last_emitted.get(
            key
        )
    )


    if previous is None:

        return True


    return (

        now
        -
        previous

        >=

        cooldown_seconds

    )


# ============================================================
# 13. MARCAR EMISSAO
# ============================================================

def _wca_mark_emitted(
    key,
    now=None
):

    if now is None:

        now = time.time()


    worker_coach_audience_last_emitted[
        key
    ] = now


# ============================================================
# 14. FILA DE SAIDA
# ============================================================

def _wca_queue_output(
    output
):

    global worker_coach_audience_dropped_outputs


    queue = (
        worker_coach_audience_output_queue
    )


    if queue is None:

        return


    try:

        queue.put_nowait(
            output.copy()
        )


    except asyncio.QueueFull:

        try:

            queue.get_nowait()

        except Exception:

            pass


        worker_coach_audience_dropped_outputs += 1


        try:

            queue.put_nowait(
                output.copy()
            )

        except Exception:

            pass


# ============================================================
# 15. EMITIR SINAL
# ============================================================

def _wca_emit_signal(
    signal_type,
    data,
    timestamp=None
):

    global worker_coach_audience_outputs_emitted


    if timestamp is None:

        timestamp = time.time()


    output = {

        "output_id":
            uuid.uuid4().hex,

        "message_type":
            "audience_signal",

        "source_worker":
            "audience",

        "worker_version":
            WORKER_COACH_AUDIENCE_VERSION,

        "platform":
            worker_coach_audience_platform,

        "live_id":
            worker_coach_audience_live_id,

        "subject":
            worker_coach_audience_subject,

        "signal_type":
            signal_type,

        "timestamp":
            timestamp,

        "iso_time":
            _wca_now_iso(),

        "data":
            data,

    }


    worker_coach_audience_output_history.append(
        output
    )


    _wca_queue_output(
        output
    )


    worker_coach_audience_outputs_emitted += 1


    return output


# ============================================================
# 16. FORCA DA MUDANCA DE AUDIENCIA
#
# NAO e prioridade.
#
# Apenas descreve intensidade estatistica.
# ============================================================

def _wca_viewer_strength(
    baseline,
    absolute_change,
    percent_change
):

    if baseline is None:

        return 0.0


    baseline = abs(
        float(
            baseline
        )
    )


    absolute_change = abs(
        float(
            absolute_change
        )
    )


    percent_change = abs(
        float(
            percent_change
        )
    )


    pct_score = min(
        1.0,
        percent_change / 25.0
    )


    absolute_reference = max(
        10.0,
        baseline * 0.15
    )


    abs_score = min(
        1.0,
        absolute_change / absolute_reference
    )


    score = (
        0.70 * pct_score
        +
        0.30 * abs_score
    )


    return round(
        score,
        3
    )


# ============================================================
# 17. ANALISAR ESPECTADORES
#
# Usa valor atual versus valor conhecido
# aproximadamente 30 segundos atras.
#
# O valor atual e considerado valido ate "agora"
# enquanto o Live Engine estiver ativo.
#
# Isso permite identificar estabilidade mesmo quando
# nao chegam novos eventos porque o numero nao mudou.
# ============================================================

def _wca_evaluate_viewers(
    now=None
):

    if now is None:

        now = time.time()


    latest = (
        _wca_latest_point(
            "viewer_count"
        )
    )


    if latest is None:

        return None


    current_value = (
        _wca_number(
            latest.get(
                "value"
            )
        )
    )


    if current_value is None:

        return None


    target_time = (
        now
        -
        WCA_VIEWER_WINDOW_SECONDS
    )


    baseline_point = (
        _wca_value_at_or_before(

            "viewer_count",

            target_time

        )
    )


    if baseline_point is None:

        return None


    baseline_value = (
        _wca_number(
            baseline_point.get(
                "value"
            )
        )
    )


    if baseline_value is None:

        return None


    elapsed = (
        now
        -
        baseline_point.get(
            "timestamp",
            now
        )
    )


    if (
        elapsed
        <
        WCA_VIEWER_MIN_HISTORY_SECONDS
    ):

        return None


    absolute_change = (
        current_value
        -
        baseline_value
    )


    if baseline_value > 0:

        percent_change = (

            absolute_change

            /

            baseline_value

            *

            100.0

        )

    else:

        percent_change = None


    points = (
        _wca_points_in_window(

            "viewer_count",

            WCA_VIEWER_WINDOW_SECONDS,

            now

        )
    )


    # ========================================================
    # CRESCIMENTO
    # ========================================================

    if (

        percent_change is not None

        and

        absolute_change
        >=
        WCA_VIEWER_MIN_ABSOLUTE_CHANGE

        and

        percent_change
        >=
        WCA_VIEWER_CHANGE_PERCENT

    ):

        key = "viewer_growth"


        if not _wca_can_emit(

            key,

            WCA_COOLDOWN_VIEWER_DIRECTION,

            now

        ):

            return None


        _wca_mark_emitted(
            key,
            now
        )


        return _wca_emit_signal(

            "viewer_growth",

            {

                "metric":
                    "viewer_count",

                "window_seconds":
                    WCA_VIEWER_WINDOW_SECONDS,

                "baseline_value":
                    _wca_clean_number(
                        baseline_value
                    ),

                "current_value":
                    _wca_clean_number(
                        current_value
                    ),

                "absolute_change":
                    _wca_clean_number(
                        absolute_change
                    ),

                "change_percent":
                    round(
                        percent_change,
                        2
                    ),

                "strength":
                    _wca_viewer_strength(

                        baseline_value,

                        absolute_change,

                        percent_change

                    ),

                "points_observed":
                    len(
                        points
                    ),

                "semantics":
                    latest.get(
                        "semantics"
                    ),

            },

            timestamp=now

        )


    # ========================================================
    # QUEDA
    # ========================================================

    if (

        percent_change is not None

        and

        absolute_change
        <=
        -WCA_VIEWER_MIN_ABSOLUTE_CHANGE

        and

        percent_change
        <=
        -WCA_VIEWER_CHANGE_PERCENT

    ):

        key = "viewer_drop"


        if not _wca_can_emit(

            key,

            WCA_COOLDOWN_VIEWER_DIRECTION,

            now

        ):

            return None


        _wca_mark_emitted(
            key,
            now
        )


        return _wca_emit_signal(

            "viewer_drop",

            {

                "metric":
                    "viewer_count",

                "window_seconds":
                    WCA_VIEWER_WINDOW_SECONDS,

                "baseline_value":
                    _wca_clean_number(
                        baseline_value
                    ),

                "current_value":
                    _wca_clean_number(
                        current_value
                    ),

                "absolute_change":
                    _wca_clean_number(
                        absolute_change
                    ),

                "change_percent":
                    round(
                        percent_change,
                        2
                    ),

                "strength":
                    _wca_viewer_strength(

                        baseline_value,

                        absolute_change,

                        percent_change

                    ),

                "points_observed":
                    len(
                        points
                    ),

                "semantics":
                    latest.get(
                        "semantics"
                    ),

            },

            timestamp=now

        )


    # ========================================================
    # ESTABILIDADE
    # ========================================================

    if percent_change is not None:

        stable_absolute_limit = max(

            2.0,

            baseline_value * 0.03

        )


        if (

            abs(
                percent_change
            )
            <=
            WCA_VIEWER_STABLE_PERCENT

            and

            abs(
                absolute_change
            )
            <=
            stable_absolute_limit

        ):

            key = "viewer_stable"


            if not _wca_can_emit(

                key,

                WCA_COOLDOWN_VIEWER_STABLE,

                now

            ):

                return None


            _wca_mark_emitted(
                key,
                now
            )


            return _wca_emit_signal(

                "viewer_stable",

                {

                    "metric":
                        "viewer_count",

                    "window_seconds":
                        WCA_VIEWER_WINDOW_SECONDS,

                    "baseline_value":
                        _wca_clean_number(
                            baseline_value
                        ),

                    "current_value":
                        _wca_clean_number(
                            current_value
                        ),

                    "absolute_change":
                        _wca_clean_number(
                            absolute_change
                        ),

                    "change_percent":
                        round(
                            percent_change,
                            2
                        ),

                    "points_observed":
                        len(
                            points
                        ),

                    "semantics":
                        latest.get(
                            "semantics"
                        ),

                },

                timestamp=now

            )


    return None


# ============================================================
# 18. ANALISAR VELOCIDADE DOS LIKES
#
# Compara:
#
# 20 segundos anteriores
# versus
# ultimos 20 segundos.
#
# Como like_count e acumulativo,
# calculamos DELTA / TEMPO.
# ============================================================

def _wca_evaluate_likes(
    now=None
):

    if now is None:

        now = time.time()


    latest = (
        _wca_latest_point(
            "like_count"
        )
    )


    if latest is None:

        return None


    current_value = (
        _wca_number(
            latest.get(
                "value"
            )
        )
    )


    if current_value is None:

        return None


    recent_boundary = (
        now
        -
        WCA_LIKE_WINDOW_SECONDS
    )


    previous_boundary = (
        now
        -
        (
            WCA_LIKE_WINDOW_SECONDS
            *
            2
        )
    )


    recent_start = (
        _wca_value_at_or_before(

            "like_count",

            recent_boundary

        )
    )


    previous_start = (
        _wca_value_at_or_before(

            "like_count",

            previous_boundary

        )
    )


    if (
        recent_start is None
        or
        previous_start is None
    ):

        return None


    recent_start_value = (
        _wca_number(
            recent_start.get(
                "value"
            )
        )
    )


    previous_start_value = (
        _wca_number(
            previous_start.get(
                "value"
            )
        )
    )


    if (
        recent_start_value is None
        or
        previous_start_value is None
    ):

        return None


    recent_delta = (
        current_value
        -
        recent_start_value
    )


    previous_delta = (
        recent_start_value
        -
        previous_start_value
    )


    # Contador resetou ou comportamento invalido.

    if (
        recent_delta < 0
        or
        previous_delta < 0
    ):

        return None


    recent_rate = (

        recent_delta

        /

        WCA_LIKE_WINDOW_SECONDS

        *

        60.0

    )


    previous_rate = (

        previous_delta

        /

        WCA_LIKE_WINDOW_SECONDS

        *

        60.0

    )


    rate_difference = (
        recent_rate
        -
        previous_rate
    )


    # ========================================================
    # ACELERACAO
    # ========================================================

    acceleration = False


    if previous_rate >= 1.0:

        acceleration = (

            recent_rate
            >=
            previous_rate
            *
            WCA_LIKE_ACCELERATION_RATIO

            and

            rate_difference
            >=
            WCA_LIKE_MIN_RATE_DIFFERENCE

        )


    else:

        # Se praticamente nao havia likes e de repente
        # surgiu atividade consideravel.

        acceleration = (
            recent_rate
            >=
            10.0
        )


    if acceleration:

        key = "like_acceleration"


        if _wca_can_emit(

            key,

            WCA_COOLDOWN_LIKE,

            now

        ):

            _wca_mark_emitted(
                key,
                now
            )


            ratio = (

                recent_rate
                /
                previous_rate

                if previous_rate > 0

                else

                None

            )


            if ratio is None:

                strength = min(
                    1.0,
                    recent_rate / 60.0
                )

            else:

                strength = min(
                    1.0,
                    max(
                        0.0,
                        (
                            ratio
                            -
                            1.0
                        )
                        /
                        2.0
                    )
                )


            return _wca_emit_signal(

                "like_acceleration",

                {

                    "metric":
                        "like_count",

                    "window_seconds":
                        WCA_LIKE_WINDOW_SECONDS,

                    "previous_delta":
                        _wca_clean_number(
                            previous_delta
                        ),

                    "recent_delta":
                        _wca_clean_number(
                            recent_delta
                        ),

                    "previous_rate_per_minute":
                        round(
                            previous_rate,
                            2
                        ),

                    "recent_rate_per_minute":
                        round(
                            recent_rate,
                            2
                        ),

                    "rate_change_per_minute":
                        round(
                            rate_difference,
                            2
                        ),

                    "rate_ratio":
                        (
                            round(
                                ratio,
                                2
                            )
                            if ratio is not None
                            else None
                        ),

                    "strength":
                        round(
                            strength,
                            3
                        ),

                    "current_total":
                        _wca_clean_number(
                            current_value
                        ),

                    "semantics":
                        latest.get(
                            "semantics"
                        ),

                },

                timestamp=now

            )


    # ========================================================
    # DESACELERACAO
    # ========================================================

    slowdown = (

        previous_rate
        >=
        10.0

        and

        recent_rate
        <=
        previous_rate
        *
        WCA_LIKE_SLOWDOWN_RATIO

        and

        (
            previous_rate
            -
            recent_rate
        )
        >=
        WCA_LIKE_MIN_RATE_DIFFERENCE

    )


    if slowdown:

        key = "like_slowdown"


        if _wca_can_emit(

            key,

            WCA_COOLDOWN_LIKE,

            now

        ):

            _wca_mark_emitted(
                key,
                now
            )


            ratio = (

                recent_rate
                /
                previous_rate

                if previous_rate > 0

                else
                0.0

            )


            strength = min(

                1.0,

                max(
                    0.0,
                    1.0 - ratio
                )

            )


            return _wca_emit_signal(

                "like_slowdown",

                {

                    "metric":
                        "like_count",

                    "window_seconds":
                        WCA_LIKE_WINDOW_SECONDS,

                    "previous_delta":
                        _wca_clean_number(
                            previous_delta
                        ),

                    "recent_delta":
                        _wca_clean_number(
                            recent_delta
                        ),

                    "previous_rate_per_minute":
                        round(
                            previous_rate,
                            2
                        ),

                    "recent_rate_per_minute":
                        round(
                            recent_rate,
                            2
                        ),

                    "rate_change_per_minute":
                        round(
                            rate_difference,
                            2
                        ),

                    "rate_ratio":
                        round(
                            ratio,
                            2
                        ),

                    "strength":
                        round(
                            strength,
                            3
                        ),

                    "current_total":
                        _wca_clean_number(
                            current_value
                        ),

                    "semantics":
                        latest.get(
                            "semantics"
                        ),

                },

                timestamp=now

            )


    return None


# ============================================================
# 19. CONTADORES DE ATIVIDADE
#
# share_count
# follow_count
# gift_count
#
# Primeiro valor e usado apenas como BASELINE.
#
# Isso evita interpretar:
#
# Shopee share_count = 50
#
# como:
#
# "50 compartilhamentos acabaram de acontecer".
# ============================================================

def _wca_process_counter_activity(
    metric_type,
    record
):

    current_value = (
        _wca_number(
            record.get(
                "value"
            )
        )
    )


    if current_value is None:

        return None


    previous_value = (
        worker_coach_audience_counter_values.get(
            metric_type
        )
    )


    worker_coach_audience_counter_values[
        metric_type
    ] = current_value


    # Primeiro valor = baseline.

    if previous_value is None:

        return None


    delta = (
        current_value
        -
        previous_value
    )


    # Reset ou valor invalido.

    if delta < 0:

        return None


    if delta == 0:

        return None


    signal_map = {

        "share_count":
            "share_activity",

        "follow_count":
            "follow_activity",

        "gift_count":
            "gift_activity",

    }


    signal_type = (
        signal_map.get(
            metric_type
        )
    )


    if signal_type is None:

        return None


    return _wca_emit_signal(

        signal_type,

        {

            "metric":
                metric_type,

            "delta":
                _wca_clean_number(
                    delta
                ),

            "previous_value":
                _wca_clean_number(
                    previous_value
                ),

            "current_value":
                _wca_clean_number(
                    current_value
                ),

            "semantics":
                record.get(
                    "semantics"
                ),

        },

        timestamp=
            record.get(
                "timestamp"
            )

    )


# ============================================================
# 20. PRESENTE INDIVIDUAL
# ============================================================

def _wca_gift_signature(
    event
):

    payload = (
        event.get(
            "payload"
        )
        or
        {}
    )


    gift_id = (
        payload.get(
            "id"
        )
    )


    if gift_id:

        return (
            "id:"
            +
            str(
                gift_id
            )
        )


    return (

        str(
            payload.get(
                "user"
            )
        )

        +

        "|"

        +

        str(
            payload.get(
                "gift"
            )
        )

        +

        "|"

        +

        str(
            payload.get(
                "count"
            )
        )

        +

        "|"

        +

        str(
            event.get(
                "timestamp"
            )
        )

    )


def _wca_seen_gift(
    signature
):

    signature = str(
        signature
    )


    if (
        signature
        in
        worker_coach_audience_seen_gifts
    ):

        return True


    if (

        len(
            worker_coach_audience_seen_gift_order
        )

        >=

        worker_coach_audience_seen_gift_order.maxlen

    ):

        old = (
            worker_coach_audience_seen_gift_order[0]
        )


        worker_coach_audience_seen_gifts.discard(
            old
        )


    worker_coach_audience_seen_gift_order.append(
        signature
    )


    worker_coach_audience_seen_gifts.add(
        signature
    )


    return False


def _wca_process_gift_event(
    event
):

    signature = (
        _wca_gift_signature(
            event
        )
    )


    if _wca_seen_gift(
        signature
    ):

        return None


    payload = (
        event.get(
            "payload"
        )
        or
        {}
    )


    return _wca_emit_signal(

        "gift_received",

        {

            "user":
                payload.get(
                    "user"
                ),

            "gift":
                payload.get(
                    "gift"
                ),

            "count":
                payload.get(
                    "count"
                ),

            "gift_id":
                payload.get(
                    "id"
                ),

        },

        timestamp=
            event.get(
                "timestamp"
            )

    )


# ============================================================
# 21. ARMAZENAR PONTO DE METRICA
# ============================================================

def _wca_store_metric(
    event
):

    event_type = (
        event.get(
            "type"
        )
    )


    payload = (
        event.get(
            "payload"
        )
        or
        {}
    )


    value = (
        _wca_number(
            payload.get(
                "value"
            )
        )
    )


    if value is None:

        return None


    timestamp = (
        event.get(
            "timestamp"
        )
    )


    try:

        timestamp = float(
            timestamp
        )

    except Exception:

        timestamp = time.time()


    record = {

        "timestamp":
            timestamp,

        "iso_time":
            event.get(
                "iso_time"
            ),

        "platform":
            event.get(
                "platform"
            ),

        "live_id":
            event.get(
                "live_id"
            ),

        "subject":
            event.get(
                "subject"
            ),

        "metric":
            event_type,

        "value":
            value,

        "semantics":
            payload.get(
                "semantics"
            ),

    }


    worker_coach_audience_metrics[
        event_type
    ].append(
        record
    )


    return record


# ============================================================
# 22. PROCESSAR EVENTO DE AUDIENCIA
# ============================================================

def worker_coach_audience_process_event(
    event
):

    global worker_coach_audience_events_received

    global worker_coach_audience_platform

    global worker_coach_audience_live_id

    global worker_coach_audience_subject


    if not isinstance(
        event,
        dict
    ):

        return


    event_type = (
        event.get(
            "type"
        )
    )


    worker_coach_audience_platform = (

        event.get(
            "platform"
        )

        or

        worker_coach_audience_platform

    )


    worker_coach_audience_live_id = (

        event.get(
            "live_id"
        )

        or

        worker_coach_audience_live_id

    )


    worker_coach_audience_subject = (

        event.get(
            "subject"
        )

        or

        worker_coach_audience_subject

    )


    worker_coach_audience_events_received += 1


    worker_coach_audience_event_counts[
        event_type
    ] += 1


    # ========================================================
    # PRESENTE INDIVIDUAL
    # ========================================================

    if event_type == "gift":

        _wca_process_gift_event(
            event
        )

        return


    # ========================================================
    # METRICAS
    # ========================================================

    if event_type not in WCA_METRIC_TYPES:

        return


    record = (
        _wca_store_metric(
            event
        )
    )


    if record is None:

        return


    # ========================================================
    # VIEWERS
    # ========================================================

    if event_type == "viewer_count":

        _wca_evaluate_viewers(

            now=
                record.get(
                    "timestamp"
                )

        )


    # ========================================================
    # LIKES
    # ========================================================

    elif event_type == "like_count":

        _wca_evaluate_likes(

            now=
                record.get(
                    "timestamp"
                )

        )


    # ========================================================
    # ATIVIDADES
    # ========================================================

    elif event_type in {

        "share_count",

        "follow_count",

        "gift_count",

    }:

        _wca_process_counter_activity(

            event_type,

            record

        )


    # ========================================================
    # total_user
    #
    # Guardamos.
    # NAO interpretamos como viewers.
    # ========================================================

    elif event_type == "total_user":

        pass


    # ========================================================
    # member_count
    #
    # Guardamos.
    # Semantica ainda incerta.
    # ========================================================

    elif event_type == "member_count":

        pass


# ============================================================
# 23. ANALISE PERIODICA
#
# Importante porque:
#
# se viewers permanecem iguais,
# o Live Engine nao envia outro viewer_count.
#
# Mesmo assim podemos perceber que ficou estavel.
#
# Tambem permite perceber queda na velocidade dos likes
# quando novos likes deixam de chegar.
# ============================================================

def _wca_periodic_evaluate():

    global worker_coach_audience_last_periodic_eval


    now = time.time()


    if (

        now
        -
        worker_coach_audience_last_periodic_eval

        <

        WCA_PERIODIC_EVALUATION_SECONDS

    ):

        return


    worker_coach_audience_last_periodic_eval = (
        now
    )


    _wca_evaluate_viewers(
        now
    )


    _wca_evaluate_likes(
        now
    )


# ============================================================
# 24. RESET
# ============================================================

def worker_coach_audience_reset(
    platform
):

    global worker_coach_audience_output_queue

    global worker_coach_audience_running

    global worker_coach_audience_platform

    global worker_coach_audience_live_id

    global worker_coach_audience_subject

    global worker_coach_audience_started_at

    global worker_coach_audience_events_received

    global worker_coach_audience_outputs_emitted

    global worker_coach_audience_dropped_outputs

    global worker_coach_audience_last_error

    global worker_coach_audience_last_periodic_eval


    worker_coach_audience_output_queue = (
        asyncio.Queue(

            maxsize=
                WCA_OUTPUT_QUEUE_LIMIT

        )
    )


    worker_coach_audience_metrics.clear()


    worker_coach_audience_output_history.clear()


    worker_coach_audience_last_emitted.clear()


    worker_coach_audience_counter_values.clear()


    worker_coach_audience_seen_gifts.clear()


    worker_coach_audience_seen_gift_order.clear()


    worker_coach_audience_event_counts.clear()


    worker_coach_audience_running = False


    worker_coach_audience_platform = (
        platform
    )


    worker_coach_audience_live_id = None


    worker_coach_audience_subject = None


    worker_coach_audience_started_at = (
        time.time()
    )


    worker_coach_audience_events_received = 0


    worker_coach_audience_outputs_emitted = 0


    worker_coach_audience_dropped_outputs = 0


    worker_coach_audience_last_error = None


    worker_coach_audience_last_periodic_eval = 0.0


# ============================================================
# 25. LOOP PRINCIPAL
# ============================================================

async def worker_coach_audience_loop():

    global worker_coach_audience_running

    global worker_coach_audience_last_error


    worker_coach_audience_running = True


    try:

        while True:

            event = None


            try:

                event = await (

                    live_engine_next_audience_event(
                        timeout=0.5
                    )

                )


            except asyncio.TimeoutError:

                event = None


            except asyncio.CancelledError:

                raise


            except Exception as e:

                worker_coach_audience_last_error = (

                    f"{type(e).__name__}: "
                    f"{e}"

                )


                await asyncio.sleep(
                    0.1
                )


            # =================================================
            # PROCESSAR EVENTO
            # =================================================

            if event is not None:

                try:

                    worker_coach_audience_process_event(
                        event
                    )


                except Exception as e:

                    worker_coach_audience_last_error = (

                        f"{type(e).__name__}: "
                        f"{e}"

                    )


            # =================================================
            # ANALISE PERIODICA
            # =================================================

            try:

                _wca_periodic_evaluate()


            except Exception as e:

                worker_coach_audience_last_error = (

                    f"{type(e).__name__}: "
                    f"{e}"

                )


            # =================================================
            # ENGINE PAROU
            # =================================================

            if (

                event is None

                and

                not live_engine_is_running()

            ):

                break


    finally:

        worker_coach_audience_running = False


# ============================================================
# 26. SUPERVISOR
#
# Espera a fila ser criada dentro do Event Loop
# que executa a LIVE.
# ============================================================

async def worker_coach_audience_supervisor(
    platform
):

    global worker_coach_audience_last_error


    start = time.time()


    while True:

        if (

            time.time()
            -
            start

            >

            30

        ):

            worker_coach_audience_last_error = (

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


            audience_queue = (

                live_engine_channels.get(
                    "coach_audience"
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

            audience_queue = None


        if (

            engine_running

            and

            engine_platform
            ==
            platform

            and

            audience_queue
            is not None

        ):

            break


        await asyncio.sleep(
            0.05
        )


    await worker_coach_audience_loop()


# ============================================================
# 27. SAIDA PARA FUTURO COACH ORCHESTRATOR
# ============================================================

async def worker_coach_audience_next_output(
    timeout=None
):

    queue = (
        worker_coach_audience_output_queue
    )


    if queue is None:

        raise RuntimeError(

            "Worker Coach Audiencia "
            "ainda nao iniciou uma LIVE."

        )


    if timeout is None:

        return await queue.get()


    return await asyncio.wait_for(

        queue.get(),

        timeout=timeout

    )


# ============================================================
# 28. ALIAS:
# next_signal
# ============================================================

async def worker_coach_audience_next_signal(
    timeout=None
):

    return await (

        worker_coach_audience_next_output(
            timeout=timeout
        )

    )


# ============================================================
# 29. OUTPUTS RECENTES
# ============================================================

def worker_coach_audience_recent_outputs(
    limit=20
):

    if limit <= 0:

        return []


    return list(
        worker_coach_audience_output_history
    )[-limit:]


# ============================================================
# 30. ULTIMA METRICA
# ============================================================

def worker_coach_audience_latest_metric(
    metric_type
):

    point = (
        _wca_latest_point(
            metric_type
        )
    )


    if point is None:

        return None


    return dict(
        point
    )


# ============================================================
# 31. SNAPSHOT ATUAL DO WORKER
# ============================================================

def worker_coach_audience_current_metrics():

    result = {}


    for metric_type in WCA_METRIC_TYPES:

        point = (
            _wca_latest_point(
                metric_type
            )
        )


        if point is None:

            continue


        result[
            metric_type
        ] = {

            "value":
                _wca_clean_number(
                    point.get(
                        "value"
                    )
                ),

            "semantics":
                point.get(
                    "semantics"
                ),

            "timestamp":
                point.get(
                    "timestamp"
                ),

        }


    return result


# ============================================================
# 32. DIAGNOSTICO DE METRICAS
# ============================================================

def mostrar_metricas_worker_coach_audiencia():

    print(
        "=" * 72
    )

    print(
        "AGCN - METRICAS DO WORKER COACH AUDIENCIA V1"
    )

    print(
        "=" * 72
    )


    snapshot = (
        worker_coach_audience_current_metrics()
    )


    if not snapshot:

        print(
            "Nenhuma metrica recebida ainda."
        )

        return


    order = [

        "viewer_count",

        "like_count",

        "share_count",

        "follow_count",

        "gift_count",

        "total_user",

        "member_count",

    ]


    for metric_type in order:

        data = (
            snapshot.get(
                metric_type
            )
        )


        if data is None:

            continue


        print()


        print(
            metric_type
        )


        print(
            "  valor:",
            data.get(
                "value"
            )
        )


        print(
            "  semantica:",
            data.get(
                "semantics"
            )
        )


        series = (
            _wca_series(
                metric_type
            )
        )


        print(
            "  pontos armazenados:",
            len(
                series
            )
            if series
            else 0
        )


# ============================================================
# 33. DIAGNOSTICO PRINCIPAL
# ============================================================

def mostrar_worker_coach_audiencia():

    print(
        "=" * 72
    )

    print(
        "AGCN - WORKER COACH AUDIENCIA V1"
    )

    print(
        "=" * 72
    )


    print(
        "Rodando:",
        worker_coach_audience_running
    )


    print(
        "Plataforma:",
        worker_coach_audience_platform
    )


    print(
        "Live ID:",
        worker_coach_audience_live_id
    )


    print(
        "Subject:",
        worker_coach_audience_subject
    )


    print()


    print(
        "EVENTOS"
    )


    print(
        "  Recebidos:",
        worker_coach_audience_events_received
    )


    for event_type, count in sorted(

        worker_coach_audience_event_counts.items(),

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
        "SAIDA PARA ORCHESTRATOR"
    )


    print(
        "  Sinais emitidos:",
        worker_coach_audience_outputs_emitted
    )


    print(
        "  Outputs descartados por fila cheia:",
        worker_coach_audience_dropped_outputs
    )


    if worker_coach_audience_output_queue is None:

        queue_size = None

    else:

        queue_size = (
            worker_coach_audience_output_queue.qsize()
        )


    print(
        "  Aguardando futuro Orchestrator:",
        queue_size
    )


    print()


    print(
        "Erro:",
        worker_coach_audience_last_error
    )


    # ========================================================
    # ULTIMOS VALORES
    # ========================================================

    print()


    print(
        "ULTIMAS METRICAS:"
    )


    snapshot = (
        worker_coach_audience_current_metrics()
    )


    if not snapshot:

        print(
            "  nenhuma"
        )


    else:

        for metric_type in [

            "viewer_count",

            "like_count",

            "share_count",

            "follow_count",

            "gift_count",

            "total_user",

            "member_count",

        ]:

            data = (
                snapshot.get(
                    metric_type
                )
            )


            if data is None:

                continue


            print(

                f"  {metric_type}: "
                f"{data.get('value')} "
                f"[{data.get('semantics')}]"

            )


    # ========================================================
    # OUTPUTS
    # ========================================================

    print()


    print(
        "ULTIMOS SINAIS:"
    )


    outputs = list(
        worker_coach_audience_output_history
    )[-12:]


    if not outputs:

        print(
            "  nenhum sinal ainda"
        )


    else:

        for output in outputs:

            print()


            print(
                " ",
                output.get(
                    "signal_type"
                )
            )


            data = (
                output.get(
                    "data"
                )
                or
                {}
            )


            signal_type = (
                output.get(
                    "signal_type"
                )
            )


            if signal_type in {

                "viewer_growth",

                "viewer_drop",

                "viewer_stable",

            }:

                print(

                    "    viewers:",

                    data.get(
                        "baseline_value"
                    ),

                    "→",

                    data.get(
                        "current_value"
                    ),

                    "| mudança:",

                    data.get(
                        "change_percent"
                    ),

                    "%",

                    "| força:",

                    data.get(
                        "strength"
                    )

                )


            elif signal_type in {

                "like_acceleration",

                "like_slowdown",

            }:

                print(

                    "    likes/min:",

                    data.get(
                        "previous_rate_per_minute"
                    ),

                    "→",

                    data.get(
                        "recent_rate_per_minute"
                    ),

                    "| força:",

                    data.get(
                        "strength"
                    )

                )


            elif signal_type in {

                "share_activity",

                "follow_activity",

                "gift_activity",

            }:

                print(

                    "    delta:",

                    data.get(
                        "delta"
                    ),

                    "| atual:",

                    data.get(
                        "current_value"
                    ),

                    "| semantica:",

                    data.get(
                        "semantics"
                    )

                )


            elif signal_type == "gift_received":

                print(

                    "    usuario:",

                    data.get(
                        "user"
                    ),

                    "| presente:",

                    data.get(
                        "gift"
                    ),

                    "| quantidade:",

                    data.get(
                        "count"
                    )

                )


# ============================================================
# 34. FINALIZAR TASK DO WORKER
# ============================================================

async def _worker_audience_finish_task(
    task
):

    if task is None:

        return


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


    if not task.done():

        task.cancel()


    try:

        await task


    except BaseException:

        pass


# ============================================================
# 35. DESCOBRIR FUNCOES BASE
#
# IMPORTANTE:
#
# Esta celula vem DEPOIS de:
#
# Live Engine
# Armazenamento Coach
# Worker Coach Comentarios
#
#
# Portanto a funcao atual ja inclui:
#
# Live Engine
# + Armazenamento
# + Worker Comentarios
#
#
# Agora acrescentamos:
#
# + Worker Audiencia
#
#
# Tambem permite reexecutar esta celula
# sem empilhar wrappers de Audiencia.
# ============================================================

_current_audience_shopee_executor = (
    executar_shopee_com_live_engine
)


if getattr(

    _current_audience_shopee_executor,

    "_agcn_audience_worker_wrapper",

    False

):

    _worker_audience_base_shopee = (

        _current_audience_shopee_executor
        ._agcn_audience_worker_base

    )


else:

    _worker_audience_base_shopee = (
        _current_audience_shopee_executor
    )


_current_audience_tiktok_executor = (
    executar_tiktok_com_live_engine
)


if getattr(

    _current_audience_tiktok_executor,

    "_agcn_audience_worker_wrapper",

    False

):

    _worker_audience_base_tiktok = (

        _current_audience_tiktok_executor
        ._agcn_audience_worker_base

    )


else:

    _worker_audience_base_tiktok = (
        _current_audience_tiktok_executor
    )


# ============================================================
# 36. SHOPEE
#
# LIVE ENGINE
# + STORAGE
# + COMMENTS
# + AUDIENCE
# ============================================================

async def executar_shopee_com_live_engine():

    worker_coach_audience_reset(
        "shopee"
    )


    audience_task = (
        asyncio.create_task(

            worker_coach_audience_supervisor(
                "shopee"
            )

        )
    )


    try:

        return await (
            _worker_audience_base_shopee()
        )


    finally:

        await _worker_audience_finish_task(
            audience_task
        )


executar_shopee_com_live_engine._agcn_audience_worker_wrapper = True


executar_shopee_com_live_engine._agcn_audience_worker_base = (
    _worker_audience_base_shopee
)


# ============================================================
# 37. TIKTOK
#
# LIVE ENGINE
# + STORAGE
# + COMMENTS
# + AUDIENCE
# ============================================================

async def executar_tiktok_com_live_engine(
    username
):

    worker_coach_audience_reset(
        "tiktok"
    )


    audience_task = (
        asyncio.create_task(

            worker_coach_audience_supervisor(
                "tiktok"
            )

        )
    )


    try:

        return await (
            _worker_audience_base_tiktok(
                username
            )
        )


    finally:

        await _worker_audience_finish_task(
            audience_task
        )


executar_tiktok_com_live_engine._agcn_audience_worker_wrapper = True


executar_tiktok_com_live_engine._agcn_audience_worker_base = (
    _worker_audience_base_tiktok
)


# ============================================================
# 38. PRONTO
# ============================================================

print(
    "AGCN Worker Coach Audiencia V1 carregado."
)

print(
    "Entrada: metricas normalizadas do Live Engine V2."
)

print(
    "Analises:"
)

print(
    "1. crescimento / queda / estabilidade de viewers"
)

print(
    "2. aceleracao / desaceleracao de likes"
)

print(
    "3. atividade de shares / follows / presentes"
)

print(
    "4. presentes individuais quando disponiveis"
)

print(
    "total_user e member_count sao armazenados sem interpretacao indevida."
)

print(
    "Saida: sinais estruturados para o futuro Coach Orchestrator."
)

print(
    "Nenhuma comunicacao direta com Interface ou Armazenamento Coach."
)
