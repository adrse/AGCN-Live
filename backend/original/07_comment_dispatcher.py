# ============================================================
# AGCN - COMMENT DISPATCHER V1.1
#
# Worker Coach Comentarios V1.5
#              ↓
#       COMMENT DISPATCHER
#        ├─ Coach Produto
#        ├─ Coach Comercial
#        ├─ Coach Objecoes/Pos-compra
#        └─ canal sales -> Sales Coach V2
#
# V1.1 preserva as tres rotas existentes e cria uma copia
# independente para o futuro Sales Coach V2.
# ============================================================

import asyncio
import copy
import time
import uuid
from collections import defaultdict, deque
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

_CD_MISSING = [name for name in _CD_REQUIRED if name not in globals()]

if _CD_MISSING:
    raise RuntimeError(
        "Execute primeiro o Live Engine V2 e o Worker Coach Comentarios V1.5. "
        "Faltando: " + ", ".join(_CD_MISSING)
    )


# ============================================================
# 2. CONFIGURACAO
# ============================================================

COMMENT_DISPATCHER_VERSION = "1.1"
COMMENT_DISPATCHER_TIMEZONE = ZoneInfo("America/Araguaina")
COMMENT_DISPATCHER_QUEUE_LIMIT = 5000
COMMENT_DISPATCHER_MAX_HISTORY = 5000
COMMENT_DISPATCHER_MAX_SEEN = 20000


# ============================================================
# 3. MAPA DE ROTEAMENTO
#
# post_purchase_issue continua somente no Coach de Objecoes/
# Pos-compra. O canal sales recebe oportunidades de conversao.
# ============================================================

COMMENT_DISPATCHER_ROUTE_MAP = {
    "product": {
        "product_question",
        "demo_request",
    },
    "commercial": {
        "commercial_question",
        "buying_intent",
        "purchase_completed",
        "commercial_observation",
    },
    "objections": {
        "objection",
        "post_purchase_issue",
        "purchase_barrier",
    },
    "sales": {
        "product_question",
        "demo_request",
        "commercial_question",
        "buying_intent",
        "purchase_completed",
        "commercial_observation",
        "objection",
        "purchase_barrier",
    },
}


# ============================================================
# 4. FILAS
# ============================================================

comment_dispatcher_product_queue = None
comment_dispatcher_commercial_queue = None
comment_dispatcher_objections_queue = None
comment_dispatcher_sales_queue = None


# ============================================================
# 5. HISTORICOS / DEDUP / ESTADO
# ============================================================

comment_dispatcher_recent_routes = deque(maxlen=COMMENT_DISPATCHER_MAX_HISTORY)
comment_dispatcher_unrouted_history = deque(maxlen=COMMENT_DISPATCHER_MAX_HISTORY)

comment_dispatcher_seen_ids = set()
comment_dispatcher_seen_order = deque(maxlen=COMMENT_DISPATCHER_MAX_SEEN)

comment_dispatcher_running = False
comment_dispatcher_platform = None
comment_dispatcher_live_id = None
comment_dispatcher_subject = None
comment_dispatcher_started_at = None
comment_dispatcher_last_error = None

comment_dispatcher_stats = defaultdict(int)
comment_dispatcher_route_counts = defaultdict(int)
comment_dispatcher_dropped_by_channel = defaultdict(int)


def _cd_now_iso():
    return datetime.now(COMMENT_DISPATCHER_TIMEZONE).isoformat()


def _cd_queue_for(destination):
    if destination == "product":
        return comment_dispatcher_product_queue
    if destination == "commercial":
        return comment_dispatcher_commercial_queue
    if destination == "objections":
        return comment_dispatcher_objections_queue
    if destination == "sales":
        return comment_dispatcher_sales_queue
    return None


def _cd_mark_seen(output):
    output_id = output.get("output_id")
    if not output_id:
        return False

    output_id = str(output_id)

    if output_id in comment_dispatcher_seen_ids:
        comment_dispatcher_stats["duplicates"] += 1
        return True

    if len(comment_dispatcher_seen_order) >= comment_dispatcher_seen_order.maxlen:
        comment_dispatcher_seen_ids.discard(comment_dispatcher_seen_order[0])

    comment_dispatcher_seen_order.append(output_id)
    comment_dispatcher_seen_ids.add(output_id)
    return False


def _cd_add_intent(result, category, subtype=None, requires_response=False):
    if not category:
        return

    key = (category, subtype)

    for item in result:
        if (item.get("category"), item.get("subtype")) == key:
            if requires_response:
                item["requires_response"] = True
            return

    result.append({
        "category": category,
        "subtype": subtype,
        "requires_response": bool(requires_response),
    })


def _cd_extract_intents(output):
    result = []
    message_type = output.get("message_type")

    if message_type == "classified_comment":
        classification = output.get("classification") or {}

        for intent in classification.get("intents") or []:
            if not isinstance(intent, dict):
                continue
            _cd_add_intent(
                result,
                intent.get("category"),
                intent.get("subtype"),
                intent.get("requires_response", False),
            )

        main = classification.get("main_classification") or {}
        _cd_add_intent(
            result,
            main.get("category"),
            main.get("subtype"),
            main.get(
                "requires_response",
                classification.get("requires_response", False),
            ),
        )

    elif message_type == "comment_trend":
        category = output.get("category")
        requires_response = category in {
            "product_question",
            "commercial_question",
            "post_purchase_issue",
        }

        _cd_add_intent(
            result,
            category,
            output.get("subtype"),
            requires_response,
        )

        for subtype in output.get("subtypes") or []:
            _cd_add_intent(
                result,
                category,
                subtype,
                requires_response,
            )

    return result


def comment_dispatcher_routes_for_output(output):
    intents = _cd_extract_intents(output)
    routes = {}

    for destination, categories in COMMENT_DISPATCHER_ROUTE_MAP.items():
        matched = [
            copy.deepcopy(intent)
            for intent in intents
            if intent.get("category") in categories
        ]
        if matched:
            routes[destination] = matched

    return routes


def _cd_build_envelope(output, destination, matched_intents):
    categories = list(dict.fromkeys(
        intent.get("category")
        for intent in matched_intents
        if intent.get("category")
    ))

    subtypes = list(dict.fromkeys(
        intent.get("subtype")
        for intent in matched_intents
        if intent.get("subtype")
    ))

    return {
        "dispatch_id": uuid.uuid4().hex,
        "message_type": "dispatched_comment_output",
        "dispatcher_version": COMMENT_DISPATCHER_VERSION,
        "destination": destination,
        "source_worker": "comments",
        "source_output_id": output.get("output_id"),
        "source_message_type": output.get("message_type"),
        "platform": output.get("platform"),
        "live_id": output.get("live_id"),
        "subject": output.get("subject"),
        "timestamp": output.get("timestamp", time.time()),
        "iso_time": output.get("iso_time") or _cd_now_iso(),
        "matched_categories": categories,
        "matched_subtypes": subtypes,
        "matched_intents": copy.deepcopy(matched_intents),
        "requires_response": any(
            bool(intent.get("requires_response"))
            for intent in matched_intents
        ),
        "payload": copy.deepcopy(output),
    }


def _cd_put(destination, envelope):
    queue = _cd_queue_for(destination)
    if queue is None:
        return False

    try:
        queue.put_nowait(envelope)
    except asyncio.QueueFull:
        try:
            queue.get_nowait()
        except Exception:
            pass

        comment_dispatcher_dropped_by_channel[destination] += 1

        try:
            queue.put_nowait(envelope)
        except Exception:
            return False

    comment_dispatcher_stats["routed_copies"] += 1
    comment_dispatcher_route_counts[destination] += 1

    comment_dispatcher_recent_routes.append({
        "source_output_id": envelope.get("source_output_id"),
        "source_message_type": envelope.get("source_message_type"),
        "destination": destination,
        "matched_categories": list(envelope.get("matched_categories") or []),
        "matched_subtypes": list(envelope.get("matched_subtypes") or []),
        "timestamp": envelope.get("timestamp"),
    })

    return True


def comment_dispatcher_process_output(output):
    global comment_dispatcher_platform
    global comment_dispatcher_live_id
    global comment_dispatcher_subject

    if not isinstance(output, dict):
        return []

    if _cd_mark_seen(output):
        return []

    comment_dispatcher_stats["received"] += 1
    message_type = output.get("message_type")

    if message_type == "classified_comment":
        comment_dispatcher_stats["classified_comment"] += 1
    elif message_type == "comment_trend":
        comment_dispatcher_stats["comment_trend"] += 1
    else:
        comment_dispatcher_stats["unrouted"] += 1
        comment_dispatcher_unrouted_history.append({
            "reason": "unsupported_message_type",
            "output": copy.deepcopy(output),
        })
        return []

    comment_dispatcher_platform = output.get("platform") or comment_dispatcher_platform
    comment_dispatcher_live_id = output.get("live_id") or comment_dispatcher_live_id
    comment_dispatcher_subject = output.get("subject") or comment_dispatcher_subject

    routes = comment_dispatcher_routes_for_output(output)

    if not routes:
        comment_dispatcher_stats["unrouted"] += 1
        comment_dispatcher_unrouted_history.append({
            "reason": "no_destination_for_classification",
            "output": copy.deepcopy(output),
        })
        return []

    destinations = []

    for destination, matched_intents in routes.items():
        envelope = _cd_build_envelope(output, destination, matched_intents)
        if _cd_put(destination, envelope):
            destinations.append(destination)

    if destinations:
        comment_dispatcher_stats["unique_routed"] += 1
        if len(destinations) > 1:
            comment_dispatcher_stats["multi_routed"] += 1

    return destinations


def comment_dispatcher_reset(platform):
    global comment_dispatcher_product_queue
    global comment_dispatcher_commercial_queue
    global comment_dispatcher_objections_queue
    global comment_dispatcher_sales_queue
    global comment_dispatcher_running
    global comment_dispatcher_platform
    global comment_dispatcher_live_id
    global comment_dispatcher_subject
    global comment_dispatcher_started_at
    global comment_dispatcher_last_error

    comment_dispatcher_product_queue = asyncio.Queue(
        maxsize=COMMENT_DISPATCHER_QUEUE_LIMIT
    )
    comment_dispatcher_commercial_queue = asyncio.Queue(
        maxsize=COMMENT_DISPATCHER_QUEUE_LIMIT
    )
    comment_dispatcher_objections_queue = asyncio.Queue(
        maxsize=COMMENT_DISPATCHER_QUEUE_LIMIT
    )
    comment_dispatcher_sales_queue = asyncio.Queue(
        maxsize=COMMENT_DISPATCHER_QUEUE_LIMIT
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
    comment_dispatcher_started_at = time.time()
    comment_dispatcher_last_error = None


async def comment_dispatcher_loop():
    global comment_dispatcher_running
    global comment_dispatcher_last_error

    comment_dispatcher_running = True

    try:
        while True:
            item = None

            try:
                item = await worker_coach_comments_next_output(timeout=0.5)
            except asyncio.TimeoutError:
                item = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                comment_dispatcher_last_error = f"{type(exc).__name__}: {exc}"
                await asyncio.sleep(0.1)

            if item is not None:
                try:
                    comment_dispatcher_process_output(item)
                except Exception as exc:
                    comment_dispatcher_last_error = f"{type(exc).__name__}: {exc}"
                continue

            comments_running = bool(
                globals().get("worker_coach_comments_running", False)
            )
            source_queue = globals().get("worker_coach_comments_output_queue")
            source_empty = source_queue is None or source_queue.empty()

            if (
                not live_engine_is_running()
                and not comments_running
                and source_empty
            ):
                break
    finally:
        comment_dispatcher_running = False


async def comment_dispatcher_supervisor(platform):
    global comment_dispatcher_last_error

    start = time.time()

    while True:
        if time.time() - start > 30:
            comment_dispatcher_last_error = (
                "Worker Coach Comentarios nao ficou pronto dentro de 30 segundos."
            )
            return

        try:
            ready = (
                live_engine_is_running()
                and live_engine_platform() == platform
                and globals().get("worker_coach_comments_output_queue") is not None
                and bool(globals().get("worker_coach_comments_running", False))
            )
        except Exception:
            ready = False

        if ready:
            break

        await asyncio.sleep(0.05)

    await comment_dispatcher_loop()


async def _cd_next(destination, timeout=None):
    queue = _cd_queue_for(destination)

    if queue is None:
        raise RuntimeError("Comment Dispatcher ainda nao iniciou uma LIVE.")

    if timeout is None:
        return await queue.get()

    return await asyncio.wait_for(queue.get(), timeout=timeout)


async def comment_dispatcher_next_product(timeout=None):
    return await _cd_next("product", timeout)


async def comment_dispatcher_next_commercial(timeout=None):
    return await _cd_next("commercial", timeout)


async def comment_dispatcher_next_objections(timeout=None):
    return await _cd_next("objections", timeout)


async def comment_dispatcher_next_sales(timeout=None):
    return await _cd_next("sales", timeout)


async def comment_dispatcher_next_sales_output(timeout=None):
    return await comment_dispatcher_next_sales(timeout=timeout)


def comment_dispatcher_queue_sizes():
    result = {}

    for destination in COMMENT_DISPATCHER_ROUTE_MAP:
        queue = _cd_queue_for(destination)
        result[destination] = None if queue is None else queue.qsize()

    return result


def mostrar_rotas_comment_dispatcher():
    print("=" * 76)
    print("ROTAS - COMMENT DISPATCHER V1.1")
    print("=" * 76)

    for destination, categories in COMMENT_DISPATCHER_ROUTE_MAP.items():
        print()
        print(destination)
        for category in sorted(categories):
            print("  -", category)


def mostrar_comment_dispatcher():
    sizes = comment_dispatcher_queue_sizes()

    print("=" * 76)
    print("AGCN - COMMENT DISPATCHER V1.1")
    print("=" * 76)
    print("Rodando:", comment_dispatcher_running)
    print("Plataforma:", comment_dispatcher_platform)
    print("Live ID:", comment_dispatcher_live_id)
    print("Subject:", comment_dispatcher_subject)
    print()
    print("ENTRADA")
    print("  Outputs recebidos:", comment_dispatcher_stats.get("received", 0))
    print("  classified_comment:", comment_dispatcher_stats.get("classified_comment", 0))
    print("  comment_trend:", comment_dispatcher_stats.get("comment_trend", 0))
    print("  Duplicados ignorados:", comment_dispatcher_stats.get("duplicates", 0))
    print()
    print("FILAS")
    print("  Coach Produto:", sizes.get("product"))
    print("  Coach Comercial:", sizes.get("commercial"))
    print("  Coach Objecoes/Pos-compra:", sizes.get("objections"))
    print("  Sales Coach V2:", sizes.get("sales"))
    print()
    print("TOTAL ENVIADO")
    for destination in ("product", "commercial", "objections", "sales"):
        print(
            f"  {destination}:",
            comment_dispatcher_route_counts.get(destination, 0),
        )
    print()
    print("Erro:", comment_dispatcher_last_error)


def testar_comment_dispatcher_v11():
    tests = [
        (
            {
                "message_type": "classified_comment",
                "classification": {
                    "main_classification": {
                        "category": "product_question",
                        "subtype": "functionality",
                        "requires_response": True,
                    },
                    "intents": [{
                        "category": "product_question",
                        "subtype": "functionality",
                        "requires_response": True,
                    }],
                },
            },
            {"product", "sales"},
        ),
        (
            {
                "message_type": "classified_comment",
                "classification": {
                    "main_classification": {
                        "category": "commercial_question",
                        "subtype": "availability",
                        "requires_response": True,
                    },
                    "intents": [
                        {
                            "category": "commercial_question",
                            "subtype": "availability",
                            "requires_response": True,
                        },
                        {
                            "category": "objection",
                            "subtype": "purchase_hesitation",
                            "requires_response": False,
                        },
                    ],
                },
            },
            {"commercial", "objections", "sales"},
        ),
        (
            {
                "message_type": "classified_comment",
                "classification": {
                    "main_classification": {
                        "category": "post_purchase_issue",
                        "subtype": "refund",
                        "requires_response": True,
                    },
                    "intents": [{
                        "category": "post_purchase_issue",
                        "subtype": "refund",
                        "requires_response": True,
                    }],
                },
            },
            {"objections"},
        ),
    ]

    failures = []

    for output, expected in tests:
        actual = set(comment_dispatcher_routes_for_output(output).keys())
        if actual != expected:
            failures.append({
                "expected": sorted(expected),
                "actual": sorted(actual),
            })

    return {
        "ok": not failures,
        "version": COMMENT_DISPATCHER_VERSION,
        "cases": len(tests),
        "failures": failures,
    }


def testar_comment_dispatcher_v1():
    return testar_comment_dispatcher_v11()


async def _comment_dispatcher_finish_task(task):
    if task is None:
        return

    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=2.5)
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


def _cd_unwrap_obsolete_layers(function):
    current = function

    for _ in range(20):
        if getattr(current, "_agcn_comment_dispatcher_wrapper", False):
            current = getattr(current, "_agcn_comment_dispatcher_base")
            continue

        if getattr(current, "_agcn_orchestrator_wrapper", False):
            current = getattr(current, "_agcn_orchestrator_base")
            continue

        break

    return current


_comment_dispatcher_base_shopee = _cd_unwrap_obsolete_layers(
    executar_shopee_com_live_engine
)

_comment_dispatcher_base_tiktok = _cd_unwrap_obsolete_layers(
    executar_tiktok_com_live_engine
)


async def executar_shopee_com_live_engine():
    comment_dispatcher_reset("shopee")

    dispatcher_task = asyncio.create_task(
        comment_dispatcher_supervisor("shopee")
    )

    try:
        return await _comment_dispatcher_base_shopee()
    finally:
        await _comment_dispatcher_finish_task(dispatcher_task)


executar_shopee_com_live_engine._agcn_comment_dispatcher_wrapper = True
executar_shopee_com_live_engine._agcn_comment_dispatcher_base = (
    _comment_dispatcher_base_shopee
)


async def executar_tiktok_com_live_engine(username):
    comment_dispatcher_reset("tiktok")

    dispatcher_task = asyncio.create_task(
        comment_dispatcher_supervisor("tiktok")
    )

    try:
        return await _comment_dispatcher_base_tiktok(username)
    finally:
        await _comment_dispatcher_finish_task(dispatcher_task)


executar_tiktok_com_live_engine._agcn_comment_dispatcher_wrapper = True
executar_tiktok_com_live_engine._agcn_comment_dispatcher_base = (
    _comment_dispatcher_base_tiktok
)


print("AGCN Comment Dispatcher V1.1 carregado.")
print("Entrada: Worker Coach Comentarios V1.5.")
print("Saidas:")
print("1. Coach Produto")
print("2. Coach Comercial")
print("3. Coach Objecoes/Pos-compra")
print("4. Canal Sales -> Sales Coach V2")
print(
    "Fan-out independente: um mesmo output pode seguir para "
    "Live Coach e Sales Coach."
)
print("Sem Storage, sem prioridade, sem geracao de orientacao e sem Interface.")
