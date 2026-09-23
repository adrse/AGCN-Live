# ============================================================
# AGCN LIVE - INTERFACE UNICA V8.0
#
# ARQUITETURA ATUAL:
#
# Interface V8.0
#    ↓
# Callback Python
#    ↓
# Shopee Worker / TikTok Worker
#    ↓
# Live Engine V2
#    ↓
# Workers + Dispatcher + Coaches especializados
#    ↓
# Context Fusion V1.1
#    ↓
# Decision Coach V1.2
#    ↓
# publicar_dica_coach()
#    ↓
# Interface V7
#
# NOVIDADES V7.1:
# - mantem o seletor de nivel do Coach:
#     Todos
#     Prioridade Alta
#     Essenciais
# - troca de nivel em tempo real, inclusive durante a LIVE
# - Interface escolhe o modo; Decision Coach aplica a politica
# - LIVE COACH mostra UMA orientacao por vez
# - cada orientacao fica visivel por 5 segundos (ou display_seconds)
# - fade in + barra regressiva + fade out
# - fila visual local sem empilhar mensagens na tela
# - mensagens expiradas sao descartadas antes de aparecer
# - protecao contra texto mojibake (ex.: HÃ¡ -> Há)
# - horario do Coach usa America/Araguaina (UTC-3)
# - "Todos" continua mostrando situacoes uteis distintas; o
#   Context Fusion V1.1 consolida repeticoes equivalentes
#
# NOVIDADES V8.0:
# - preserva o fluxo atual do LIVE COACH
# - adiciona fluxo paralelo do SALES COACH para Shopee
# - o mesmo link Shopee inicia os dois fluxos
# - campos opcionais de preco e informacoes adicionais
# - Equilibrado: 8s exibicao + 3s intervalo
# - Pressao / Feira: 7s exibicao + 2s intervalo
# - TikTok permanece no fluxo atual nesta etapa
#
# IMPORTANTE:
# - HTML + JavaScript ficam no MESMO bloco no Google Colab
# - esta interface nao reimplementa a inteligencia do Coach
# - prioridade, TTL, cooldown e deduplicacao continuam no
#   Decision Coach V1.2
# ============================================================

import asyncio
import threading
import time
import concurrent.futures
import re
import uuid

from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo

from google.colab import output
from IPython.display import display, HTML, JSON


# ============================================================
# 1. VERIFICAR SE TUDO FOI CARREGADO
# ============================================================

necessarios = [
    # Shopee
    "main",

    # TikTok
    "monitorar_tiktok",
    "normalizar_username_tiktok",

    # Live Engine
    "executar_shopee_com_live_engine",
    "executar_tiktok_com_live_engine",
    "live_engine_current_snapshot",
    "live_engine_recent_events",
    "live_engine_platform",

    # Decision Coach V1.2
    "decision_coach_set_alert_mode",
    "decision_coach_get_alert_mode",
    "decision_coach_alert_modes_info",

    # Fluxo independente do Coach Vendas - Shopee
    "executar_worker_shopee_produto",
    "shopee_product_reset",
    "shopee_product_stop",
    "product_extractor_reset",
    "product_extractor_set_seller_info",
    "product_extractor_stop",
    "executar_product_extractor",
    "product_sales_builder_reset",
    "product_sales_builder_stop",
    "executar_product_sales_builder",
    "sales_decision_reset",
    "sales_decision_set_style",
    "sales_decision_status",
    "sales_decision_stop",
    "sales_decision_next_output",
    "executar_sales_decision_coach",
]

faltando = [
    nome
    for nome in necessarios
    if nome not in globals()
]

if faltando:
    raise RuntimeError(
        "Execute primeiro os modulos do AGCN LIVE, incluindo o fluxo "
        "do Coach Vendas Shopee. Faltando: "
        + ", ".join(faltando)
    )


# ============================================================
# 2. CANAL DE DICAS DO DECISION COACH PARA A INTERFACE
#
# V7.1:
# - recebe tambem o objeto completo da decisao quando disponivel
# - cada dica ganha ID, timestamp, expiracao e duracao visual
# - usa horario local de Palmas/TO (America/Araguaina)
# - aplica uma protecao adicional contra texto UTF-8 corrompido
# ============================================================

AGCN_LOCAL_TZ = ZoneInfo("America/Araguaina")
AGCN_DEFAULT_COACH_DISPLAY_SECONDS = 8


def _agcn_local_hms(timestamp=None):
    if timestamp is None:
        dt = datetime.now(AGCN_LOCAL_TZ)
    else:
        dt = datetime.fromtimestamp(
            float(timestamp),
            tz=AGCN_LOCAL_TZ,
        )
    return dt.strftime("%H:%M:%S")


def _agcn_bad_text_score(value):
    value = str(value or "")
    markers = (
        "Ã",
        "Â",
        "â€",
        "â€™",
        "â€œ",
        "â€",
        "â€“",
        "â€”",
        "ï¿½",
        "�",
    )
    return sum(value.count(marker) for marker in markers)


def agcn_repair_text(value):
    """
    Corrige somente quando a conversao latin1 -> UTF-8 reduz sinais
    tipicos de mojibake. Texto normal, emojis e nomes validos permanecem.
    """
    if value is None:
        return ""

    current = str(value)

    for _ in range(2):
        before = _agcn_bad_text_score(current)
        if before <= 0:
            break

        try:
            candidate = current.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            break

        after = _agcn_bad_text_score(candidate)
        if after >= before:
            break

        current = candidate

    return current


# Ao carregar uma nova versao da Interface, descartamos dicas antigas da
# versao anterior para nao reproduzir backlog de testes antigos.
if "coach_tips" not in globals():
    coach_tips = deque(maxlen=80)
else:
    try:
        coach_tips.clear()
    except Exception:
        coach_tips = deque(maxlen=80)


def publicar_dica_coach(
    texto,
    categoria="info",
    decision=None,
    metadata=None,
):
    meta = decision if isinstance(decision, dict) else metadata
    if not isinstance(meta, dict):
        meta = {}

    timestamp = meta.get("timestamp")
    try:
        timestamp = float(timestamp)
    except (TypeError, ValueError):
        timestamp = time.time()

    display_seconds = meta.get("display_seconds")
    try:
        display_seconds = int(display_seconds)
    except (TypeError, ValueError):
        display_seconds = AGCN_DEFAULT_COACH_DISPLAY_SECONDS

    display_seconds = max(3, min(display_seconds, 15))

    expires_at = meta.get("expires_at")
    try:
        expires_at = float(expires_at)
    except (TypeError, ValueError):
        # Sem metadata, ainda evitamos backlog visual eterno.
        expires_at = timestamp + 30

    hora = (
        agcn_repair_text(meta.get("display_time"))
        or _agcn_local_hms(timestamp)
    )

    coach_tips.append({
        "id": str(meta.get("output_id") or uuid.uuid4().hex),
        "hora": hora,
        "categoria": agcn_repair_text(categoria or "info"),
        "texto": agcn_repair_text(texto or ""),
        "timestamp": timestamp,
        "expires_at": expires_at,
        "display_seconds": display_seconds,
        "priority": meta.get("priority"),
        "priority_level": meta.get("priority_level"),
        "alert_mode": meta.get("alert_mode"),
        "decision_key": meta.get("decision_key"),
    })


# ============================================================
# 3. ESTADO DO MONITORAMENTO
# ============================================================

agcn_thread = None
agcn_loop = None
agcn_task = None

agcn_monitorando = False
agcn_plataforma = None
agcn_status = "parado"
agcn_erro = None
agcn_stop_requested = False

# Fluxo independente do Coach Vendas.
agcn_sales_tasks = []
agcn_sales_enabled = False
agcn_sales_error = None
agcn_sales_generation = 0

if "sales_coach_tips" not in globals():
    sales_coach_tips = deque(maxlen=120)
else:
    try:
        sales_coach_tips.clear()
    except Exception:
        sales_coach_tips = deque(maxlen=120)


# ============================================================
# 4. UTILITARIOS DO SALES COACH
# ============================================================

def _agcn_iso_to_timestamp(value, default=None):
    if value is None:
        return default

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return default

    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return default


def _agcn_sales_hms(value=None):
    timestamp = _agcn_iso_to_timestamp(
        value,
        default=time.time(),
    )
    return _agcn_local_hms(timestamp)


def _agcn_call_in_monitor_loop(
    func,
    *args,
    timeout=5.0,
    **kwargs,
):
    """
    Executa funcoes sincronas do fluxo Sales dentro da thread/event loop
    do monitoramento. Isso evita usar asyncio.Queue a partir da thread
    dos callbacks do Colab.
    """
    loop = agcn_loop

    if (
        loop is None
        or loop.is_closed()
        or not loop.is_running()
    ):
        return func(*args, **kwargs)

    future = concurrent.futures.Future()

    def invoke():
        try:
            result = func(*args, **kwargs)
            future.set_result(result)
        except BaseException as exc:
            future.set_exception(exc)

    loop.call_soon_threadsafe(invoke)
    return future.result(timeout=timeout)


async def agcn_sales_guard(coro, label):
    """
    Falha no Coach Vendas nao derruba o LIVE COACH.
    Os dois fluxos sao independentes.
    """
    global agcn_sales_error

    try:
        return await coro
    except asyncio.CancelledError:
        raise
    except BaseException as exc:
        agcn_sales_error = (
            f"{label}: {type(exc).__name__}: {exc}"
        )
        return None


async def agcn_sales_output_bridge():
    """
    UNICO consumidor da fila final do Sales Decision Coach.
    O callback de status consulta apenas o deque visual.
    """
    global agcn_sales_generation

    while True:
        event = await sales_decision_next_output()

        if not isinstance(event, dict):
            continue

        event_type = event.get("type")

        if event_type == "sales_coach_cleared":
            try:
                sales_coach_tips.clear()
            except Exception:
                pass

            agcn_sales_generation += 1
            continue

        if event_type != "sales_coach_message":
            continue

        started_at = event.get("started_at")
        expires_at = event.get("expires_at")

        started_ts = _agcn_iso_to_timestamp(
            started_at,
            default=time.time(),
        )

        expires_ts = _agcn_iso_to_timestamp(
            expires_at,
            default=(
                started_ts
                + float(event.get("display_seconds") or 8)
            ),
        )

        sales_coach_tips.append({
            "id": (
                "sales-"
                + str(event.get("seq") or uuid.uuid4().hex)
            ),
            "seq": event.get("seq"),
            "hora": _agcn_sales_hms(started_at),
            "categoria": agcn_repair_text(
                event.get("category") or "sales"
            ),
            "texto": agcn_repair_text(
                event.get("message") or ""
            ),
            "style": event.get("style") or "equilibrado",
            "importance": event.get("importance"),
            "risk_level": event.get("risk_level"),
            "candidate_id": event.get("candidate_id"),
            "product_key": event.get("product_key"),
            "timestamp": started_ts,
            "expires_at": expires_ts,
            "display_seconds": float(
                event.get("display_seconds") or 8
            ),
            "interval_seconds": float(
                event.get("interval_seconds") or 3
            ),
            "cycle_seconds": float(
                event.get("cycle_seconds") or 11
            ),
        })


def agcn_preparar_sales_pipeline(
    seller_price="",
    seller_info="",
    sales_style="equilibrado",
):
    """
    Prepara uma nova LIVE Shopee para o fluxo paralelo de produto.
    Deve rodar dentro da thread do monitoramento.
    """
    global agcn_sales_enabled
    global agcn_sales_error

    agcn_sales_enabled = True
    agcn_sales_error = None

    shopee_product_reset(
        clear_history=True,
        clear_queue=True,
    )

    product_extractor_reset(
        clear_history=True,
        clear_output_queue=True,
        clear_seller_info=True,
    )

    product_sales_builder_reset(
        clear_history=True,
        clear_output_queue=True,
        keep_style=False,
    )

    sales_decision_reset(
        clear_history=True,
        clear_output_queue=True,
        keep_style=False,
    )

    # O Sales Decision sincroniza o Builder.
    sales_decision_set_style(
        sales_style or "equilibrado",
        sync_builder=True,
    )

    seller_price = str(seller_price or "").strip()
    seller_info = str(seller_info or "").strip()

    if seller_price or seller_info:
        product_extractor_set_seller_info(
            price=(seller_price or None),
            additional_info=(seller_info or None),
            facts={},
            replace=True,
            emit_update=False,
        )


def agcn_iniciar_sales_tasks(link):
    global agcn_sales_tasks

    # Consumidores primeiro, produtor por ultimo.
    coroutines = [
        (
            executar_product_extractor(),
            "Product Extractor",
        ),
        (
            executar_product_sales_builder(),
            "Product Sales Builder",
        ),
        (
            executar_sales_decision_coach(),
            "Sales Decision Coach",
        ),
        (
            agcn_sales_output_bridge(),
            "Sales Output Bridge",
        ),
        (
            executar_worker_shopee_produto(link),
            "Worker Shopee Produto",
        ),
    ]

    agcn_sales_tasks = [
        asyncio.create_task(
            agcn_sales_guard(
                coro,
                label,
            )
        )
        for coro, label in coroutines
    ]


async def agcn_encerrar_sales_tasks():
    global agcn_sales_tasks
    global agcn_sales_enabled

    for function_name in [
        "shopee_product_stop",
        "product_extractor_stop",
        "product_sales_builder_stop",
        "sales_decision_stop",
    ]:
        function = globals().get(function_name)
        if callable(function):
            try:
                function()
            except Exception:
                pass

    tasks = [
        task
        for task in agcn_sales_tasks
        if task is not None
        and not task.done()
    ]

    for task in tasks:
        task.cancel()

    if tasks:
        try:
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )
        except Exception:
            pass

    agcn_sales_tasks = []
    agcn_sales_enabled = False


# ============================================================
# 5. DETECTAR AUTOMATICAMENTE A PLATAFORMA
# ============================================================

def agcn_detectar_entrada(texto):
    texto = (texto or "").strip()

    if not texto:
        return None

    # Shopee - link curto atual usado pelo Worker.
    match = re.search(
        r"https://br\.shp\.ee/[A-Za-z0-9_-]+",
        texto,
    )

    if match:
        return {
            "platform": "shopee",
            "value": match.group(0),
        }

    # Se for URL, precisa ser TikTok.
    if texto.startswith(("http://", "https://")):
        if "tiktok.com/" not in texto.lower():
            return None

    try:
        username = normalizar_username_tiktok(texto)
    except Exception:
        username = None

    if username:
        return {
            "platform": "tiktok",
            "value": username,
        }

    return None


# ============================================================
# 6. PREPARAR NOVA LIVE SHOPEE
# ============================================================

def agcn_preparar_shopee(link):
    global LINK_CURTO
    global encerrar
    global LIVE_URL
    global SESSION_ID
    global ENDPOINT_SESSION

    LINK_CURTO = link
    encerrar = False

    LIVE_URL = None
    SESSION_ID = None
    ENDPOINT_SESSION = None

    estado.update({
        "sessionId": None,
        "chatroomId": None,
        "loja": None,
        "username": None,
        "titulo": None,
        "viewers": None,
        "likes": None,
        "shares": None,
        "products": None,
        "memberCnt": None,
        "status": None,
        "ultimaMetrica": None,
        "ultimoChat": None,
        "erro": None,
        "urlExpandida": None,
    })

    comentarios.clear()
    comentarios_ids.clear()

    if "comentarios_assinaturas" in globals():
        comentarios_assinaturas.clear()


# ============================================================
# 7. PAINEIS SILENCIOSOS
# ============================================================

async def agcn_painel_shopee():
    global encerrar

    while not encerrar:
        await asyncio.sleep(0.5)


async def agcn_painel_tiktok(client):
    while not estado_tiktok.get("encerrada", False):
        await asyncio.sleep(0.5)


# ============================================================
# 8. THREAD DO MONITORAMENTO
# ============================================================

def agcn_runner(
    plataforma,
    valor,
    sales_config=None,
):
    global agcn_loop
    global agcn_task
    global agcn_monitorando
    global agcn_status
    global agcn_erro
    global agcn_sales_enabled
    global agcn_sales_error

    sales_config = (
        sales_config
        if isinstance(sales_config, dict)
        else {}
    )

    agcn_monitorando = True
    agcn_status = "iniciando"
    agcn_erro = None
    agcn_sales_error = None

    painel_shopee_original = globals().get("painel")
    painel_tiktok_original = globals().get("painel_tiktok")
    clear_original = globals().get("clear_output")

    def clear_silencioso(*args, **kwargs):
        return None

    globals()["clear_output"] = clear_silencioso
    globals()["painel"] = agcn_painel_shopee
    globals()["painel_tiktok"] = agcn_painel_tiktok

    loop = asyncio.new_event_loop()
    agcn_loop = loop
    asyncio.set_event_loop(loop)

    async def executar():
        global agcn_status
        global agcn_sales_enabled

        if plataforma == "shopee":
            agcn_preparar_shopee(valor)

            # Fluxo paralelo: nao modifica o Worker Shopee antigo.
            agcn_preparar_sales_pipeline(
                seller_price=sales_config.get(
                    "seller_price",
                    "",
                ),
                seller_info=sales_config.get(
                    "seller_info",
                    "",
                ),
                sales_style=sales_config.get(
                    "sales_style",
                    "equilibrado",
                ),
            )

            agcn_iniciar_sales_tasks(valor)

            agcn_status = "conectando"

            try:
                await executar_shopee_com_live_engine()
            finally:
                await agcn_encerrar_sales_tasks()

        else:
            agcn_sales_enabled = False
            agcn_status = "conectando"
            await executar_tiktok_com_live_engine(valor)

    task = None

    try:
        task = loop.create_task(executar())
        agcn_task = task

        agcn_status = "ativo"
        loop.run_until_complete(task)

        if agcn_stop_requested:
            agcn_status = "encerrado"
        else:
            agcn_status = "finalizado"

    except asyncio.CancelledError:
        agcn_status = "encerrado"

    except BaseException as e:
        agcn_erro = f"{type(e).__name__}: {str(e)}"
        agcn_status = "erro"

    finally:
        try:
            if plataforma == "shopee" and agcn_sales_tasks:
                loop.run_until_complete(
                    agcn_encerrar_sales_tasks()
                )
        except Exception:
            pass

        try:
            pendentes = [
                item
                for item in asyncio.all_tasks(loop)
                if not item.done()
            ]

            for item in pendentes:
                item.cancel()

            if pendentes:
                loop.run_until_complete(
                    asyncio.gather(
                        *pendentes,
                        return_exceptions=True,
                    )
                )
        except Exception:
            pass

        try:
            loop.close()
        except Exception:
            pass

        if painel_shopee_original is not None:
            globals()["painel"] = painel_shopee_original

        if painel_tiktok_original is not None:
            globals()["painel_tiktok"] = painel_tiktok_original

        if clear_original is not None:
            globals()["clear_output"] = clear_original

        agcn_monitorando = False


# ============================================================
# 9. CALLBACK - INICIAR
# ============================================================

def agcn_callback_start(
    texto,
    seller_price="",
    seller_info="",
    sales_style="equilibrado",
):
    global agcn_thread
    global agcn_monitorando
    global agcn_plataforma
    global agcn_status
    global agcn_erro
    global agcn_stop_requested
    global agcn_sales_generation
    global agcn_sales_error

    if agcn_monitorando:
        return JSON({
            "ok": False,
            "message": "Ja existe um monitoramento ativo.",
        })

    detectado = agcn_detectar_entrada(texto)

    if not detectado:
        return JSON({
            "ok": False,
            "message": (
                "Informe um link da Shopee "
                "ou username do TikTok."
            ),
        })

    try:
        coach_tips.clear()
    except Exception:
        pass

    try:
        sales_coach_tips.clear()
    except Exception:
        pass

    agcn_sales_generation += 1
    agcn_sales_error = None

    sales_style = str(
        sales_style
        or "equilibrado"
    ).strip()

    if sales_style not in {
        "equilibrado",
        "pressao_feira",
    }:
        sales_style = "equilibrado"

    sales_config = {
        "seller_price": str(
            seller_price
            or ""
        ).strip(),
        "seller_info": str(
            seller_info
            or ""
        ).strip(),
        "sales_style": sales_style,
    }

    agcn_plataforma = detectado["platform"]
    agcn_status = "iniciando"
    agcn_erro = None
    agcn_stop_requested = False
    agcn_monitorando = True

    thread = threading.Thread(
        target=agcn_runner,
        args=(
            detectado["platform"],
            detectado["value"],
            sales_config,
        ),
        daemon=True,
    )

    agcn_thread = thread
    thread.start()

    return JSON({
        "ok": True,
        "platform": detectado["platform"],
        "sales_enabled": (
            detectado["platform"] == "shopee"
        ),
        "sales_style": sales_style,
        "message": "Monitoramento iniciado.",
    })


# ============================================================
# 10. CALLBACK - ENCERRAR
# ============================================================

def agcn_callback_stop():
    global agcn_stop_requested
    global agcn_status
    global encerrar

    agcn_stop_requested = True
    agcn_status = "encerrando"

    try:
        encerrar = True
    except Exception:
        pass

    for function_name in [
        "shopee_product_stop",
        "product_extractor_stop",
        "product_sales_builder_stop",
        "sales_decision_stop",
    ]:
        function = globals().get(function_name)
        if callable(function):
            try:
                function()
            except Exception:
                pass

    if agcn_loop is not None and agcn_task is not None:
        try:
            agcn_loop.call_soon_threadsafe(agcn_task.cancel)
        except Exception:
            pass

    return JSON({
        "ok": True,
        "message": "Encerramento solicitado.",
    })


# ============================================================
# 11. CALLBACK - ALTERAR NIVEL DO LIVE COACH
# ============================================================

def agcn_callback_set_alert_mode(mode):
    try:
        settings = decision_coach_set_alert_mode(mode)
        canonical = settings.get("alert_mode") or "all"
        modes = decision_coach_alert_modes_info()
        info = modes.get(canonical, {})

        return JSON({
            "ok": True,
            "mode": canonical,
            "label": info.get("label", canonical),
            "description": info.get("description", ""),
            "min_priority": settings.get("min_priority"),
            "global_cooldown_seconds": settings.get(
                "global_cooldown_seconds"
            ),
            "max_tips_per_minute": settings.get(
                "max_tips_per_minute"
            ),
        })

    except Exception as e:
        return JSON({
            "ok": False,
            "message": f"{type(e).__name__}: {e}",
        })


# ============================================================
# 12. CALLBACK - ALTERAR ESTILO DO SALES COACH
# ============================================================

def agcn_callback_set_sales_style(style):
    if agcn_plataforma != "shopee" or not agcn_monitorando:
        normalized = (
            "pressao_feira"
            if str(style) == "pressao_feira"
            else "equilibrado"
        )

        if normalized == "pressao_feira":
            return JSON({
                "ok": True,
                "style": normalized,
                "display_seconds": 7,
                "interval_seconds": 2,
                "cycle_seconds": 9,
                "applied_to_runtime": False,
            })

        return JSON({
            "ok": True,
            "style": normalized,
            "display_seconds": 8,
            "interval_seconds": 3,
            "cycle_seconds": 11,
            "applied_to_runtime": False,
        })

    try:
        result = _agcn_call_in_monitor_loop(
            sales_decision_set_style,
            style,
            sync_builder=True,
        )

        return JSON({
            "ok": True,
            "style": result.get("style"),
            "display_seconds": result.get(
                "display_seconds"
            ),
            "interval_seconds": result.get(
                "interval_seconds"
            ),
            "cycle_seconds": result.get(
                "cycle_seconds"
            ),
            "applied_to_runtime": True,
        })

    except Exception as exc:
        return JSON({
            "ok": False,
            "message": f"{type(exc).__name__}: {exc}",
        })


# ============================================================
# 13. CALLBACK - INFORMACOES ADICIONAIS DO PRODUTO
# ============================================================

def agcn_callback_set_product_info(
    seller_price="",
    seller_info="",
):
    if (
        agcn_plataforma != "shopee"
        or not agcn_monitorando
    ):
        return JSON({
            "ok": False,
            "message": (
                "As informacoes do Coach Vendas podem ser "
                "atualizadas durante uma LIVE Shopee ativa."
            ),
        })

    try:
        seller_price = str(
            seller_price
            or ""
        ).strip()

        seller_info = str(
            seller_info
            or ""
        ).strip()

        result = _agcn_call_in_monitor_loop(
            product_extractor_set_seller_info,
            price=(seller_price or None),
            additional_info=(seller_info or None),
            facts={},
            replace=True,
            emit_update=True,
        )

        return JSON({
            "ok": True,
            "message": "Informacoes do produto atualizadas.",
            "target": (
                result.get("target")
                if isinstance(result, dict)
                else None
            ),
        })

    except Exception as exc:
        return JSON({
            "ok": False,
            "message": f"{type(exc).__name__}: {exc}",
        })


# ============================================================
# 14. EXTRAIR VALOR DO LIVE ENGINE
# ============================================================

def agcn_value(snapshot, tipo):
    evento = snapshot.get(tipo)

    if not evento:
        return None

    return (
        evento
        .get("payload", {})
        .get("value")
    )


# ============================================================
# 15. CALLBACK - STATUS ATUAL
# ============================================================

def agcn_callback_status():
    try:
        snapshot = live_engine_current_snapshot()
        eventos = live_engine_recent_events(300)
    except Exception:
        snapshot = {}
        eventos = []

    plataforma = live_engine_platform() or agcn_plataforma

    subject = None
    live_id = None

    for evento in reversed(eventos):
        if subject is None and evento.get("subject"):
            subject = evento.get("subject")

        if live_id is None and evento.get("live_id"):
            live_id = evento.get("live_id")

        if subject and live_id:
            break

    comentarios_saida = []

    for evento in eventos:
        if evento.get("type") != "comment":
            continue

        payload = evento.get("payload", {})

        comentarios_saida.append({
            "user": str(payload.get("user") or "Usuario"),
            "text": str(payload.get("text") or ""),
            "time": str(payload.get("display_time") or ""),
        })

    comentarios_saida = comentarios_saida[-10:]

    # LIVE COACH.
    coach_saida = []
    agora = time.time()

    for dica in list(coach_tips)[-60:]:
        expires_at = dica.get("expires_at")
        try:
            expires_at = float(expires_at)
        except (TypeError, ValueError):
            expires_at = agora + 30

        if expires_at <= agora:
            continue

        coach_saida.append({
            "id": str(dica.get("id") or ""),
            "hora": agcn_repair_text(dica.get("hora") or ""),
            "categoria": agcn_repair_text(dica.get("categoria") or ""),
            "texto": agcn_repair_text(dica.get("texto") or ""),
            "timestamp": dica.get("timestamp"),
            "expires_at": expires_at,
            "display_seconds": dica.get(
                "display_seconds",
                AGCN_DEFAULT_COACH_DISPLAY_SECONDS,
            ),
            "priority": dica.get("priority"),
            "priority_level": dica.get("priority_level"),
            "alert_mode": dica.get("alert_mode"),
            "decision_key": dica.get("decision_key"),
        })

    # SALES COACH.
    sales_saida = []

    for dica in list(sales_coach_tips)[-80:]:
        expires_at = dica.get("expires_at")

        try:
            expires_at = float(expires_at)
        except (TypeError, ValueError):
            expires_at = (
                agora
                + float(dica.get("display_seconds", 8))
            )

        if expires_at <= agora:
            continue

        sales_saida.append({
            "id": str(dica.get("id") or ""),
            "seq": dica.get("seq"),
            "hora": agcn_repair_text(dica.get("hora") or ""),
            "categoria": agcn_repair_text(
                dica.get("categoria") or ""
            ),
            "texto": agcn_repair_text(
                dica.get("texto") or ""
            ),
            "style": dica.get("style"),
            "importance": dica.get("importance"),
            "risk_level": dica.get("risk_level"),
            "candidate_id": dica.get("candidate_id"),
            "product_key": dica.get("product_key"),
            "timestamp": dica.get("timestamp"),
            "expires_at": expires_at,
            "display_seconds": dica.get("display_seconds", 8),
            "interval_seconds": dica.get("interval_seconds", 3),
            "cycle_seconds": dica.get("cycle_seconds", 11),
        })

    try:
        sales_status = sales_decision_status()
    except Exception:
        sales_status = {
            "style": "equilibrado",
            "display_seconds": 8,
            "interval_seconds": 3,
            "cycle_seconds": 11,
            "current_product_key": None,
            "candidate_count": 0,
            "display_active": False,
            "blank_interval": False,
            "last_error": None,
        }

    ultimo_evento = None
    if eventos:
        ultimo_evento = eventos[-1].get("iso_time")

    try:
        alert_mode = decision_coach_get_alert_mode() or "all"
        alert_modes = decision_coach_alert_modes_info()
        alert_info = alert_modes.get(alert_mode, {})
    except Exception:
        alert_mode = "all"
        alert_modes = {}
        alert_info = {}

    sales_runtime_enabled = bool(
        plataforma == "shopee"
        and agcn_monitorando
        and agcn_sales_enabled
    )

    return JSON({
        "monitorando": bool(agcn_monitorando),
        "status": agcn_status,
        "platform": plataforma,
        "subject": subject,
        "live_id": live_id,
        "error": agcn_erro,
        "events": len(eventos),
        "last_event": ultimo_evento,

        "metrics": {
            "viewers": agcn_value(snapshot, "viewer_count"),
            "likes": agcn_value(snapshot, "like_count"),
            "shares": agcn_value(snapshot, "share_count"),
            "products": agcn_value(snapshot, "product_count"),
            "follows": agcn_value(snapshot, "follow_count"),
            "gifts": agcn_value(snapshot, "gift_count"),
        },

        "comments": comentarios_saida,
        "coach": coach_saida,

        "coach_mode": {
            "value": alert_mode,
            "label": alert_info.get("label", alert_mode),
            "description": alert_info.get("description", ""),
            "modes": alert_modes,
        },

        "sales_coach": {
            "enabled": sales_runtime_enabled,
            "available_for_platform": (
                plataforma in (None, "shopee")
            ),
            "generation": agcn_sales_generation,
            "style": sales_status.get(
                "style",
                "equilibrado",
            ),
            "display_seconds": sales_status.get(
                "display_seconds",
                8,
            ),
            "interval_seconds": sales_status.get(
                "interval_seconds",
                3,
            ),
            "cycle_seconds": sales_status.get(
                "cycle_seconds",
                11,
            ),
            "current_product_key": sales_status.get(
                "current_product_key"
            ),
            "candidate_count": sales_status.get(
                "candidate_count",
                0,
            ),
            "display_active": sales_status.get(
                "display_active",
                False,
            ),
            "blank_interval": sales_status.get(
                "blank_interval",
                False,
            ),
            "messages": sales_saida,
            "error": (
                agcn_sales_error
                or sales_status.get("last_error")
            ),
        },
    })


# ============================================================
# 16. CALLBACKS UNICOS
# ============================================================

CALLBACK_BASE = "agcn.live." + uuid.uuid4().hex
CALLBACK_START = CALLBACK_BASE + ".start"
CALLBACK_STOP = CALLBACK_BASE + ".stop"
CALLBACK_STATUS = CALLBACK_BASE + ".status"
CALLBACK_ALERT_MODE = CALLBACK_BASE + ".alert_mode"
CALLBACK_SALES_STYLE = CALLBACK_BASE + ".sales_style"
CALLBACK_PRODUCT_INFO = CALLBACK_BASE + ".product_info"

output.register_callback(
    CALLBACK_START,
    agcn_callback_start,
)

output.register_callback(
    CALLBACK_STOP,
    agcn_callback_stop,
)

output.register_callback(
    CALLBACK_STATUS,
    agcn_callback_status,
)

output.register_callback(
    CALLBACK_ALERT_MODE,
    agcn_callback_set_alert_mode,
)

output.register_callback(
    CALLBACK_SALES_STYLE,
    agcn_callback_set_sales_style,
)

output.register_callback(
    CALLBACK_PRODUCT_INFO,
    agcn_callback_set_product_info,
)


# ============================================================
# 17. ID DA INTERFACE
# ============================================================

UI_ID = "agcn-live-" + uuid.uuid4().hex[:10]


# ============================================================
# 18. HTML + JAVASCRIPT
# ============================================================

pagina = r"""
<div id="__UI_ID__" class="agcn-v7-root">
    <style>
        #__UI_ID__ {
            max-width: 760px;
            box-sizing: border-box;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
            color: #111;
            padding: 4px;
        }

        #__UI_ID__ * {
            box-sizing: border-box;
        }

        #__UI_ID__ .agcn-title {
            font-size: 28px;
            font-weight: 750;
            letter-spacing: -0.6px;
            margin-top: 4px;
        }

        #__UI_ID__ .agcn-subtitle {
            color: #777;
            margin-top: 4px;
            margin-bottom: 18px;
        }

        #__UI_ID__ .agcn-input {
            width: 100%;
            height: 48px;
            border: 1px solid #ccc;
            border-radius: 12px;
            padding: 0 14px;
            font-size: 16px;
            outline: none;
            background: #fff;
            color: #111;
        }

        #__UI_ID__ .agcn-input:focus {
            border-color: #777;
        }

        #__UI_ID__ .agcn-actions {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-top: 10px;
        }

        #__UI_ID__ button {
            font-family: inherit;
        }

        #__UI_ID__ .agcn-btn {
            border-radius: 11px;
            padding: 12px 18px;
            font-size: 15px;
            font-weight: 650;
            cursor: pointer;
        }

        #__UI_ID__ .agcn-btn-primary {
            border: 0;
            background: #111;
            color: #fff;
        }

        #__UI_ID__ .agcn-btn-secondary {
            border: 1px solid #ccc;
            background: #fff;
            color: #111;
        }

        #__UI_ID__ button:disabled {
            opacity: .45;
            cursor: default;
        }

        #__UI_ID__ .agcn-status {
            margin-top: 14px;
            color: #777;
            min-height: 22px;
        }

        #__UI_ID__ .agcn-debug {
            font-size: 11px;
            color: #999;
            margin-top: 5px;
            overflow-wrap: anywhere;
        }

        #__UI_ID__ .agcn-identity {
            margin-top: 22px;
        }

        #__UI_ID__ .agcn-cards {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 10px;
            margin-top: 12px;
        }

        #__UI_ID__ .agcn-metric-card,
        #__UI_ID__ .agcn-panel {
            border: 1px solid #ddd;
            border-radius: 14px;
            background: #fff;
        }

        #__UI_ID__ .agcn-metric-card {
            padding: 14px;
        }

        #__UI_ID__ .agcn-metric-label {
            font-size: 11px;
            color: #777;
        }

        #__UI_ID__ .agcn-metric-value {
            font-size: 27px;
            font-weight: 750;
            margin-top: 4px;
        }

        #__UI_ID__ .agcn-panel {
            padding: 16px;
            margin-top: 14px;
        }

        #__UI_ID__ .agcn-panel-title {
            font-weight: 750;
            letter-spacing: -.1px;
        }

        #__UI_ID__ .agcn-panel-title-row {
            display: flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 8px;
        }

        #__UI_ID__ .agcn-shopee-exclusive-badge {
            display: inline-flex;
            align-items: center;
            min-height: 24px;
            padding: 4px 8px;
            border-radius: 999px;
            background: #111;
            color: #fff;
            font-size: 9px;
            font-weight: 800;
            letter-spacing: .45px;
            line-height: 1.2;
            white-space: nowrap;
        }

        #__UI_ID__ .agcn-mode-wrap {
            margin-top: 14px;
            padding: 12px;
            border-radius: 12px;
            background: #f7f7f8;
            border: 1px solid #ececee;
        }

        #__UI_ID__ .agcn-mode-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 10px;
            margin-bottom: 9px;
        }

        #__UI_ID__ .agcn-mode-label {
            font-size: 11px;
            font-weight: 750;
            color: #555;
            letter-spacing: .45px;
        }

        #__UI_ID__ .agcn-mode-current {
            font-size: 11px;
            color: #777;
        }

        #__UI_ID__ .agcn-mode-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 7px;
        }

        #__UI_ID__ .agcn-mode-btn {
            width: 100%;
            min-height: 40px;
            border: 1px solid #d7d7da;
            border-radius: 10px;
            background: #fff;
            color: #333;
            font-size: 12px;
            font-weight: 650;
            padding: 8px 7px;
            cursor: pointer;
            transition: background .15s ease, color .15s ease, border-color .15s ease;
        }

        #__UI_ID__ .agcn-mode-btn.active {
            background: #111;
            color: #fff;
            border-color: #111;
        }

        #__UI_ID__ .agcn-mode-description {
            min-height: 34px;
            margin-top: 9px;
            font-size: 12px;
            line-height: 1.4;
            color: #707070;
        }

        #__UI_ID__ .agcn-mode-feedback {
            min-height: 16px;
            margin-top: 5px;
            font-size: 11px;
            color: #777;
        }

        #__UI_ID__ .agcn-scroll {
            max-height: 310px;
            overflow-y: auto;
            overscroll-behavior: contain;
        }

        #__UI_ID__ .agcn-comments-list {
            color: #777;
            margin-top: 9px;
        }

        #__UI_ID__ .agcn-coach-stage {
            position: relative;
            min-height: 152px;
            margin-top: 12px;
            overflow: hidden;
        }

        #__UI_ID__ .agcn-coach-empty {
            min-height: 152px;
            display: flex;
            align-items: center;
            color: #777;
            font-size: 13px;
        }

        #__UI_ID__ .agcn-coach-message {
            min-height: 152px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            padding: 14px;
            border: 1px solid #e7e7e9;
            border-radius: 14px;
            background: #fff;
            color: #111;
            opacity: 0;
            transform: translateY(7px) scale(.995);
            transition: opacity .28s ease, transform .28s ease;
            will-change: opacity, transform;
        }

        #__UI_ID__ .agcn-coach-message.visible {
            opacity: 1;
            transform: translateY(0) scale(1);
        }

        #__UI_ID__ .agcn-coach-message.leaving {
            opacity: 0;
            transform: translateY(-5px) scale(.995);
        }

        #__UI_ID__ .agcn-coach-meta {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 10px;
            margin-bottom: 10px;
        }

        #__UI_ID__ .agcn-coach-meta-left {
            display: flex;
            align-items: center;
            gap: 7px;
            min-width: 0;
        }

        #__UI_ID__ .agcn-coach-time {
            color: #999;
            font-size: 11px;
            white-space: nowrap;
        }

        #__UI_ID__ .agcn-coach-countdown {
            min-width: 28px;
            text-align: right;
            color: #777;
            font-size: 11px;
            font-variant-numeric: tabular-nums;
        }

        #__UI_ID__ .agcn-coach-text {
            flex: 1;
            font-size: 17px;
            line-height: 1.38;
            letter-spacing: -.1px;
            overflow-wrap: anywhere;
        }

        #__UI_ID__ .agcn-coach-progress {
            height: 3px;
            margin-top: 14px;
            border-radius: 999px;
            background: #ececee;
            overflow: hidden;
        }

        #__UI_ID__ .agcn-coach-progress-bar {
            width: 100%;
            height: 100%;
            border-radius: inherit;
            background: #111;
            transform-origin: left center;
        }

        #__UI_ID__ .agcn-item {
            padding: 9px 0;
            border-bottom: 1px solid #eee;
            color: #111;
        }

        #__UI_ID__ .agcn-item:last-child {
            border-bottom: 0;
        }

        #__UI_ID__ .agcn-time {
            font-size: 11px;
            color: #999;
            margin-bottom: 2px;
        }

        #__UI_ID__ .agcn-category {
            display: inline-block;
            margin-bottom: 5px;
            padding: 3px 7px;
            border-radius: 999px;
            background: #f1f1f2;
            color: #666;
            font-size: 10px;
            font-weight: 650;
        }

        #__UI_ID__ .agcn-product-extra {
            margin-top: 12px;
            padding: 13px;
            border: 1px solid #e5e5e7;
            border-radius: 13px;
            background: #fafafa;
        }

        #__UI_ID__ .agcn-extra-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 10px;
            margin-bottom: 10px;
        }

        #__UI_ID__ .agcn-extra-title {
            font-size: 11px;
            font-weight: 750;
            letter-spacing: .4px;
            color: #555;
        }

        #__UI_ID__ .agcn-extra-optional {
            font-size: 10px;
            color: #999;
        }

        #__UI_ID__ .agcn-extra-grid {
            display: grid;
            grid-template-columns: minmax(0, 180px) minmax(0, 1fr);
            gap: 9px;
        }

        #__UI_ID__ .agcn-extra-input,
        #__UI_ID__ .agcn-extra-textarea {
            width: 100%;
            border: 1px solid #d8d8db;
            border-radius: 10px;
            background: #fff;
            color: #111;
            font-family: inherit;
            font-size: 13px;
            outline: none;
        }

        #__UI_ID__ .agcn-extra-input {
            min-height: 42px;
            padding: 0 11px;
        }

        #__UI_ID__ .agcn-extra-textarea {
            min-height: 74px;
            resize: vertical;
            padding: 10px 11px;
            line-height: 1.35;
        }

        #__UI_ID__ .agcn-extra-input:focus,
        #__UI_ID__ .agcn-extra-textarea:focus {
            border-color: #888;
        }

        #__UI_ID__ .agcn-extra-bottom {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 10px;
            margin-top: 9px;
        }

        #__UI_ID__ .agcn-extra-help {
            color: #888;
            font-size: 11px;
            line-height: 1.35;
        }

        #__UI_ID__ .agcn-btn-small {
            flex: 0 0 auto;
            border: 1px solid #d5d5d8;
            border-radius: 9px;
            background: #fff;
            color: #222;
            padding: 8px 11px;
            font-size: 11px;
            font-weight: 650;
            cursor: pointer;
        }

        #__UI_ID__ .agcn-sales-style-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 7px;
        }

        #__UI_ID__ .agcn-sales-style-btn {
            width: 100%;
            min-height: 40px;
            border: 1px solid #d7d7da;
            border-radius: 10px;
            background: #fff;
            color: #333;
            font-size: 12px;
            font-weight: 650;
            padding: 8px 7px;
            cursor: pointer;
            transition: background .15s ease, color .15s ease, border-color .15s ease;
        }

        #__UI_ID__ .agcn-sales-style-btn.active {
            background: #111;
            color: #fff;
            border-color: #111;
        }

        #__UI_ID__ .agcn-sales-runtime {
            min-height: 18px;
            margin-top: 8px;
            font-size: 11px;
            color: #777;
        }

        @media (max-width: 560px) {
            #__UI_ID__ .agcn-mode-grid {
                grid-template-columns: 1fr;
            }

            #__UI_ID__ .agcn-mode-btn {
                text-align: left;
                padding-left: 12px;
            }

            #__UI_ID__ .agcn-extra-grid {
                grid-template-columns: 1fr;
            }

            #__UI_ID__ .agcn-extra-bottom {
                align-items: stretch;
                flex-direction: column;
            }

            #__UI_ID__ .agcn-btn-small {
                width: 100%;
            }
        }
    </style>

    <div class="agcn-title">AGCN LIVE</div>
    <div class="agcn-subtitle">Monitoramento inteligente de LIVE</div>

    <input
        id="__UI_ID__-input"
        class="agcn-input"
        type="text"
        placeholder="Link da Shopee ou username do TikTok"
        autocomplete="off"
    >

    <div class="agcn-product-extra">
        <div class="agcn-extra-head">
            <div class="agcn-extra-title">INFORMACOES ADICIONAIS DO PRODUTO</div>
            <div class="agcn-extra-optional">OPCIONAL</div>
        </div>

        <div class="agcn-extra-grid">
            <input
                id="__UI_ID__-seller-price"
                class="agcn-extra-input"
                type="text"
                placeholder="Preco adicional. Ex.: R$ 19,90"
                autocomplete="off"
            >

            <textarea
                id="__UI_ID__-seller-info"
                class="agcn-extra-textarea"
                placeholder="Ex.: material, garantia, quantidade, como funciona, detalhe importante do produto..."
            ></textarea>
        </div>

        <div class="agcn-extra-bottom">
            <div class="agcn-extra-help">
                Se ficar vazio, o Coach Vendas usa somente os dados automaticos da Shopee.
            </div>

            <button
                id="__UI_ID__-apply-product-info"
                class="agcn-btn-small"
                type="button"
                disabled
            >
                Aplicar na LIVE
            </button>
        </div>

        <div
            id="__UI_ID__-product-info-feedback"
            class="agcn-mode-feedback"
        ></div>
    </div>

    <div class="agcn-actions">
        <button
            id="__UI_ID__-start"
            class="agcn-btn agcn-btn-primary"
        >
            Iniciar monitoramento
        </button>

        <button
            id="__UI_ID__-stop"
            class="agcn-btn agcn-btn-secondary"
            disabled
        >
            Encerrar
        </button>
    </div>

    <div id="__UI_ID__-status" class="agcn-status">
        Aguardando link ou username.
    </div>

    <div id="__UI_ID__-debug" class="agcn-debug">
        Engine aguardando.
    </div>

    <div id="__UI_ID__-identity" class="agcn-identity"></div>
    <div id="__UI_ID__-cards" class="agcn-cards"></div>

    <div class="agcn-panel">
        <div class="agcn-panel-title">LIVE COACH</div>

        <div class="agcn-mode-wrap">
            <div class="agcn-mode-header">
                <div class="agcn-mode-label">NIVEL DO COACH</div>
                <div id="__UI_ID__-mode-current" class="agcn-mode-current">
                    Todos
                </div>
            </div>

            <div class="agcn-mode-grid">
                <button
                    type="button"
                    class="agcn-mode-btn"
                    data-mode="all"
                    aria-pressed="false"
                >
                    Todos
                </button>

                <button
                    type="button"
                    class="agcn-mode-btn"
                    data-mode="high"
                    aria-pressed="false"
                >
                    Prioridade Alta
                </button>

                <button
                    type="button"
                    class="agcn-mode-btn"
                    data-mode="essential"
                    aria-pressed="false"
                >
                    Essenciais
                </button>
            </div>

            <div id="__UI_ID__-mode-description" class="agcn-mode-description">
                Todas as perguntas e situacoes uteis distintas; repeticoes equivalentes sao agrupadas.
            </div>

            <div id="__UI_ID__-mode-feedback" class="agcn-mode-feedback"></div>
        </div>

        <div
            id="__UI_ID__-coach"
            class="agcn-coach-stage"
            aria-live="polite"
            aria-atomic="true"
        >
            <div class="agcn-coach-empty">
                Aguardando orientacoes do Coach...
            </div>
        </div>
    </div>

    <div class="agcn-panel">
        <div class="agcn-panel-title agcn-panel-title-row">
            <span>SALES COACH</span>
            <span class="agcn-shopee-exclusive-badge">
                FUNCIONALIDADES EXCLUSIVAS PARA SHOPEE
            </span>
        </div>

        <div class="agcn-mode-wrap">
            <div class="agcn-mode-header">
                <div class="agcn-mode-label">ESTILO DE VENDA</div>
                <div id="__UI_ID__-sales-style-current" class="agcn-mode-current">
                    Equilibrado
                </div>
            </div>

            <div class="agcn-sales-style-grid">
                <button
                    type="button"
                    class="agcn-sales-style-btn active"
                    data-sales-style="equilibrado"
                    aria-pressed="true"
                >
                    Equilibrado
                </button>

                <button
                    type="button"
                    class="agcn-sales-style-btn"
                    data-sales-style="pressao_feira"
                    aria-pressed="false"
                >
                    Pressao / Feira
                </button>
            </div>

            <div id="__UI_ID__-sales-style-description" class="agcn-mode-description">
                Ritmo natural e continuo: 8s de orientacao + 3s de intervalo.
            </div>

            <div id="__UI_ID__-sales-runtime" class="agcn-sales-runtime">
                Aguardando uma LIVE Shopee.
            </div>
        </div>

        <div
            id="__UI_ID__-sales-coach"
            class="agcn-coach-stage"
            aria-live="polite"
            aria-atomic="true"
        >
            <div class="agcn-coach-empty">
                Aguardando produto destacado na Shopee...
            </div>
        </div>
    </div>

    <div class="agcn-panel">
        <div class="agcn-panel-title">COMENTARIOS</div>

        <div
            id="__UI_ID__-comments"
            class="agcn-comments-list agcn-scroll"
        >
            Aguardando comentarios...
        </div>
    </div>

    <script>
    (() => {
        const root = document.getElementById("__UI_ID__");
        if (!root) return;

        const input = document.getElementById("__UI_ID__-input");
        const start = document.getElementById("__UI_ID__-start");
        const stop = document.getElementById("__UI_ID__-stop");
        const status = document.getElementById("__UI_ID__-status");
        const debug = document.getElementById("__UI_ID__-debug");
        const identity = document.getElementById("__UI_ID__-identity");
        const cards = document.getElementById("__UI_ID__-cards");
        const coach = document.getElementById("__UI_ID__-coach");
        const comments = document.getElementById("__UI_ID__-comments");
        const modeCurrent = document.getElementById("__UI_ID__-mode-current");
        const modeDescription = document.getElementById("__UI_ID__-mode-description");
        const modeFeedback = document.getElementById("__UI_ID__-mode-feedback");
        const modeButtons = Array.from(
            root.querySelectorAll(".agcn-mode-btn")
        );

        const sellerPrice = document.getElementById("__UI_ID__-seller-price");
        const sellerInfo = document.getElementById("__UI_ID__-seller-info");
        const applyProductInfo = document.getElementById("__UI_ID__-apply-product-info");
        const productInfoFeedback = document.getElementById("__UI_ID__-product-info-feedback");

        const salesCoach = document.getElementById("__UI_ID__-sales-coach");
        const salesStyleCurrent = document.getElementById("__UI_ID__-sales-style-current");
        const salesStyleDescription = document.getElementById("__UI_ID__-sales-style-description");
        const salesRuntime = document.getElementById("__UI_ID__-sales-runtime");
        const salesStyleButtons = Array.from(
            root.querySelectorAll(".agcn-sales-style-btn")
        );

        let polling = false;
        let changingMode = false;
        let currentMode = null;

        let currentPlatform = null;
        let salesStyle = "equilibrado";
        let changingSalesStyle = false;
        let salesGeneration = null;

        // LIVE COACH V7.1: uma mensagem por vez.
        const coachPending = [];
        const coachSeen = new Set();
        let coachCurrent = null;
        let coachHideTimer = null;
        let coachCountdownTimer = null;
        let coachTransitionTimer = null;

        // SALES COACH: fila visual independente do LIVE COACH.
        const salesPending = [];
        const salesSeen = new Set();
        let salesCurrent = null;
        let salesHideTimer = null;
        let salesCountdownTimer = null;
        let salesTransitionTimer = null;
        let salesIntervalTimer = null;
        let salesBlockedUntil = 0;

        function esc(value) {
            if (value === null || value === undefined) return "";

            return String(value)
                .replaceAll("&", "&amp;")
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;")
                .replaceAll('"', "&quot;")
                .replaceAll("'", "&#039;");
        }

        function formatNumber(value) {
            if (value === null || value === undefined) return "-";

            const n = Number(value);
            if (Number.isFinite(n)) {
                return new Intl.NumberFormat("pt-BR").format(n);
            }

            return esc(value);
        }

        function card(label, value) {
            return `
                <div class="agcn-metric-card">
                    <div class="agcn-metric-label">${esc(label)}</div>
                    <div class="agcn-metric-value">${formatNumber(value)}</div>
                </div>
            `;
        }

        function clearCoachTimers() {
            if (coachHideTimer) {
                clearTimeout(coachHideTimer);
                coachHideTimer = null;
            }
            if (coachCountdownTimer) {
                clearInterval(coachCountdownTimer);
                coachCountdownTimer = null;
            }
            if (coachTransitionTimer) {
                clearTimeout(coachTransitionTimer);
                coachTransitionTimer = null;
            }
        }

        function renderCoachEmpty(text = "Aguardando orientacoes do Coach...") {
            coach.innerHTML = `
                <div class="agcn-coach-empty">
                    ${esc(text)}
                </div>
            `;
        }

        function resetCoachDisplay() {
            clearCoachTimers();
            coachPending.length = 0;
            coachCurrent = null;
            renderCoachEmpty();
        }

        function coachMessageExpired(item) {
            const expiresAt = Number(item?.expires_at);
            if (!Number.isFinite(expiresAt)) return false;
            return (Date.now() / 1000) >= expiresAt;
        }

        function enqueueCoachMessages(items) {
            if (!Array.isArray(items)) return;

            for (const item of items) {
                const id = String(
                    item?.id ||
                    [item?.hora, item?.categoria, item?.texto].join("|")
                );

                if (!id || coachSeen.has(id)) continue;
                coachSeen.add(id);

                if (coachMessageExpired(item)) continue;

                coachPending.push({
                    ...item,
                    id,
                });
            }

            showNextCoachMessage();
        }

        function showNextCoachMessage() {
            if (coachCurrent) return;

            while (coachPending.length) {
                const candidate = coachPending.shift();
                if (!coachMessageExpired(candidate)) {
                    coachCurrent = candidate;
                    break;
                }
            }

            if (!coachCurrent) {
                renderCoachEmpty();
                return;
            }

            const item = coachCurrent;
            const durationSecondsRaw = Number(item.display_seconds);
            const durationSeconds = Number.isFinite(durationSecondsRaw)
                ? Math.max(3, Math.min(durationSecondsRaw, 15))
                : 5;
            const durationMs = durationSeconds * 1000;
            const startedAt = Date.now();

            coach.innerHTML = `
                <div class="agcn-coach-message" id="__UI_ID__-coach-current">
                    <div>
                        <div class="agcn-coach-meta">
                            <div class="agcn-coach-meta-left">
                                <div class="agcn-coach-time">${esc(item.hora || "")}</div>
                                ${
                                    item.categoria
                                        ? `<div class="agcn-category">${esc(item.categoria)}</div>`
                                        : ""
                                }
                            </div>
                            <div class="agcn-coach-countdown" id="__UI_ID__-coach-countdown">
                                ${Math.ceil(durationSeconds)}s
                            </div>
                        </div>
                        <div class="agcn-coach-text">${esc(item.texto || "")}</div>
                    </div>
                    <div class="agcn-coach-progress">
                        <div
                            class="agcn-coach-progress-bar"
                            id="__UI_ID__-coach-progress-bar"
                        ></div>
                    </div>
                </div>
            `;

            const messageEl = document.getElementById("__UI_ID__-coach-current");
            const countdownEl = document.getElementById("__UI_ID__-coach-countdown");
            const progressEl = document.getElementById("__UI_ID__-coach-progress-bar");

            requestAnimationFrame(() => {
                requestAnimationFrame(() => {
                    messageEl?.classList.add("visible");
                    if (progressEl) {
                        progressEl.style.transition = `transform ${durationMs}ms linear`;
                        progressEl.style.transform = "scaleX(0)";
                    }
                });
            });

            coachCountdownTimer = setInterval(() => {
                const elapsed = Date.now() - startedAt;
                const remaining = Math.max(0, durationMs - elapsed);
                if (countdownEl) {
                    countdownEl.textContent = `${Math.ceil(remaining / 1000)}s`;
                }
            }, 100);

            coachHideTimer = setTimeout(() => {
                if (coachCountdownTimer) {
                    clearInterval(coachCountdownTimer);
                    coachCountdownTimer = null;
                }

                messageEl?.classList.remove("visible");
                messageEl?.classList.add("leaving");

                coachTransitionTimer = setTimeout(() => {
                    coachCurrent = null;
                    coachHideTimer = null;
                    coachTransitionTimer = null;
                    showNextCoachMessage();
                }, 320);
            }, durationMs);
        }

        function clearSalesTimers() {
            if (salesHideTimer) {
                clearTimeout(salesHideTimer);
                salesHideTimer = null;
            }

            if (salesCountdownTimer) {
                clearInterval(salesCountdownTimer);
                salesCountdownTimer = null;
            }

            if (salesTransitionTimer) {
                clearTimeout(salesTransitionTimer);
                salesTransitionTimer = null;
            }

            if (salesIntervalTimer) {
                clearTimeout(salesIntervalTimer);
                salesIntervalTimer = null;
            }
        }

        function renderSalesEmpty(
            text = "Aguardando produto destacado na Shopee..."
        ) {
            salesCoach.innerHTML = `
                <div class="agcn-coach-empty">
                    ${esc(text)}
                </div>
            `;
        }

        function resetSalesDisplay(
            text = "Aguardando produto destacado na Shopee...",
            clearSeen = false
        ) {
            clearSalesTimers();
            salesPending.length = 0;
            salesCurrent = null;
            salesBlockedUntil = 0;

            if (clearSeen) {
                salesSeen.clear();
            }

            renderSalesEmpty(text);
        }

        function salesMessageExpired(item) {
            const expiresAt = Number(item?.expires_at);
            if (!Number.isFinite(expiresAt)) return false;
            return (Date.now() / 1000) >= expiresAt;
        }

        function enqueueSalesMessages(items) {
            if (!Array.isArray(items)) return;

            for (const item of items) {
                const id = String(
                    item?.id ||
                    [item?.hora, item?.categoria, item?.texto].join("|")
                );

                if (!id || salesSeen.has(id)) continue;

                salesSeen.add(id);

                if (salesMessageExpired(item)) continue;

                salesPending.push({
                    ...item,
                    id,
                });
            }

            showNextSalesMessage();
        }

        function showNextSalesMessage() {
            if (salesCurrent) return;

            const now = Date.now();

            if (now < salesBlockedUntil) {
                if (!salesIntervalTimer) {
                    salesIntervalTimer = setTimeout(() => {
                        salesIntervalTimer = null;
                        showNextSalesMessage();
                    }, Math.max(20, salesBlockedUntil - now));
                }
                return;
            }

            while (salesPending.length) {
                const candidate = salesPending.shift();
                if (!salesMessageExpired(candidate)) {
                    salesCurrent = candidate;
                    break;
                }
            }

            if (!salesCurrent) {
                renderSalesEmpty(
                    currentPlatform === "shopee"
                        ? "Preparando a proxima orientacao de venda..."
                        : "Aguardando uma LIVE Shopee."
                );
                return;
            }

            const item = salesCurrent;
            const durationRaw = Number(item.display_seconds);
            const intervalRaw = Number(item.interval_seconds);

            const durationSeconds = Number.isFinite(durationRaw)
                ? Math.max(3, Math.min(durationRaw, 15))
                : (salesStyle === "pressao_feira" ? 7 : 8);

            const intervalSeconds = Number.isFinite(intervalRaw)
                ? Math.max(0, Math.min(intervalRaw, 10))
                : (salesStyle === "pressao_feira" ? 2 : 3);

            const durationMs = durationSeconds * 1000;
            const intervalMs = intervalSeconds * 1000;
            const startedAt = Date.now();

            salesCoach.innerHTML = `
                <div class="agcn-coach-message" id="__UI_ID__-sales-current">
                    <div>
                        <div class="agcn-coach-meta">
                            <div class="agcn-coach-meta-left">
                                <div class="agcn-coach-time">${esc(item.hora || "")}</div>
                                ${
                                    item.categoria
                                        ? `<div class="agcn-category">${esc(item.categoria)}</div>`
                                        : ""
                                }
                            </div>
                            <div class="agcn-coach-countdown" id="__UI_ID__-sales-countdown">
                                ${Math.ceil(durationSeconds)}s
                            </div>
                        </div>
                        <div class="agcn-coach-text">${esc(item.texto || "")}</div>
                    </div>
                    <div class="agcn-coach-progress">
                        <div
                            class="agcn-coach-progress-bar"
                            id="__UI_ID__-sales-progress-bar"
                        ></div>
                    </div>
                </div>
            `;

            const messageEl = document.getElementById("__UI_ID__-sales-current");
            const countdownEl = document.getElementById("__UI_ID__-sales-countdown");
            const progressEl = document.getElementById("__UI_ID__-sales-progress-bar");

            requestAnimationFrame(() => {
                requestAnimationFrame(() => {
                    messageEl?.classList.add("visible");
                    if (progressEl) {
                        progressEl.style.transition = `transform ${durationMs}ms linear`;
                        progressEl.style.transform = "scaleX(0)";
                    }
                });
            });

            salesCountdownTimer = setInterval(() => {
                const elapsed = Date.now() - startedAt;
                const remaining = Math.max(0, durationMs - elapsed);
                if (countdownEl) {
                    countdownEl.textContent = `${Math.ceil(remaining / 1000)}s`;
                }
            }, 100);

            salesHideTimer = setTimeout(() => {
                if (salesCountdownTimer) {
                    clearInterval(salesCountdownTimer);
                    salesCountdownTimer = null;
                }

                // O intervalo comeca exatamente quando termina a exibicao.
                // O fade-out acontece dentro do intervalo.
                salesBlockedUntil = Date.now() + intervalMs;

                messageEl?.classList.remove("visible");
                messageEl?.classList.add("leaving");

                salesTransitionTimer = setTimeout(() => {
                    salesCurrent = null;
                    salesHideTimer = null;
                    salesTransitionTimer = null;
                    renderSalesEmpty("Preparando a proxima orientacao...");
                }, 320);

                salesIntervalTimer = setTimeout(() => {
                    salesIntervalTimer = null;
                    salesCurrent = null;
                    showNextSalesMessage();
                }, intervalMs);
            }, durationMs);
        }

        function salesStyleLabel(style) {
            return style === "pressao_feira"
                ? "Pressao / Feira"
                : "Equilibrado";
        }

        function salesStyleDefaultDescription(style) {
            if (style === "pressao_feira") {
                return "Ritmo mais agil: 7s de orientacao + 2s de intervalo.";
            }

            return "Ritmo natural e continuo: 8s de orientacao + 3s de intervalo.";
        }

        function applySalesStyleUI(style, description = "") {
            const normalized = style === "pressao_feira"
                ? "pressao_feira"
                : "equilibrado";

            salesStyle = normalized;
            salesStyleCurrent.textContent = salesStyleLabel(normalized);
            salesStyleDescription.textContent =
                description || salesStyleDefaultDescription(normalized);

            salesStyleButtons.forEach(button => {
                const active = button.dataset.salesStyle === normalized;
                button.classList.toggle("active", active);
                button.setAttribute("aria-pressed", active ? "true" : "false");
            });
        }

        function setSalesStyleButtonsDisabled(value) {
            salesStyleButtons.forEach(button => {
                button.disabled = Boolean(value);
            });
        }

        async function changeSalesStyle(style) {
            const normalized = style === "pressao_feira"
                ? "pressao_feira"
                : "equilibrado";

            if (changingSalesStyle) return;

            // Antes da LIVE, apenas guarda a escolha local.
            if (!currentPlatform || !root.dataset.monitorando) {
                applySalesStyleUI(normalized);
                return;
            }

            if (currentPlatform !== "shopee") {
                applySalesStyleUI(normalized);
                return;
            }

            changingSalesStyle = true;
            setSalesStyleButtonsDisabled(true);

            try {
                const data = await callPython(
                    "__CALLBACK_SALES_STYLE__",
                    [normalized]
                );

                if (!data || !data.ok) {
                    salesRuntime.innerHTML =
                        "<span style='color:#b00020'>" +
                        esc(data?.message || "Nao foi possivel alterar o estilo.") +
                        "</span>";
                    return;
                }

                applySalesStyleUI(
                    data.style,
                    data.style === "pressao_feira"
                        ? `Ritmo mais agil: ${data.display_seconds}s de orientacao + ${data.interval_seconds}s de intervalo.`
                        : `Ritmo natural e continuo: ${data.display_seconds}s de orientacao + ${data.interval_seconds}s de intervalo.`
                );
            }
            catch (error) {
                salesRuntime.innerHTML =
                    "<span style='color:#b00020'>" +
                    esc(String(error)) +
                    "</span>";
            }
            finally {
                changingSalesStyle = false;
                setSalesStyleButtonsDisabled(false);
            }
        }

        async function applyProductInfoNow() {
            if (
                currentPlatform !== "shopee" ||
                !root.dataset.monitorando
            ) {
                productInfoFeedback.innerHTML =
                    "<span style='color:#777'>Inicie uma LIVE Shopee para aplicar durante o monitoramento.</span>";
                return;
            }

            applyProductInfo.disabled = true;
            productInfoFeedback.textContent =
                "Atualizando informacoes do produto...";

            try {
                const data = await callPython(
                    "__CALLBACK_PRODUCT_INFO__",
                    [
                        sellerPrice.value.trim(),
                        sellerInfo.value.trim(),
                    ]
                );

                if (!data || !data.ok) {
                    productInfoFeedback.innerHTML =
                        "<span style='color:#b00020'>" +
                        esc(data?.message || "Nao foi possivel atualizar.") +
                        "</span>";
                    return;
                }

                productInfoFeedback.innerHTML =
                    "<span style='color:#177245'>Informacoes atualizadas no Product Profile.</span>";
            }
            catch (error) {
                productInfoFeedback.innerHTML =
                    "<span style='color:#b00020'>" +
                    esc(String(error)) +
                    "</span>";
            }
            finally {
                applyProductInfo.disabled = !(
                    currentPlatform === "shopee" &&
                    root.dataset.monitorando
                );
            }
        }

        function getData(result) {
            if (!result || !result.data) return null;
            return result.data["application/json"] || null;
        }

        async function callPython(name, args = []) {
            const result = await google.colab.kernel.invokeFunction(
                name,
                args,
                {}
            );
            return getData(result);
        }

        function defaultModeDescription(mode) {
            if (mode === "high") {
                return "Foca em situacoes de maior impacto comercial, repeticao, intencao de compra, objecoes e pos-compra.";
            }

            if (mode === "essential") {
                return "Mostra apenas situacoes muito fortes ou urgentes. E o modo com menos interrupcoes.";
            }

            return "Mostra todas as perguntas e situacoes uteis distintas. Perguntas equivalentes continuam agrupadas.";
        }

        function modeLabel(mode) {
            if (mode === "high") return "Prioridade Alta";
            if (mode === "essential") return "Essenciais";
            return "Todos";
        }

        function applyModeUI(mode, description = "") {
            const normalized = ["all", "high", "essential"].includes(mode)
                ? mode
                : "all";

            currentMode = normalized;
            modeCurrent.textContent = modeLabel(normalized);
            modeDescription.textContent = description || defaultModeDescription(normalized);

            modeButtons.forEach(button => {
                const active = button.dataset.mode === normalized;
                button.classList.toggle("active", active);
                button.setAttribute("aria-pressed", active ? "true" : "false");
            });
        }

        function setModeButtonsDisabled(value) {
            modeButtons.forEach(button => {
                button.disabled = Boolean(value);
            });
        }

        async function changeMode(mode) {
            if (changingMode || mode === currentMode) return;

            changingMode = true;
            setModeButtonsDisabled(true);
            modeFeedback.textContent = "Alterando nivel do Coach...";

            try {
                const data = await callPython(
                    "__CALLBACK_ALERT_MODE__",
                    [mode]
                );

                if (!data || !data.ok) {
                    modeFeedback.innerHTML =
                        "<span style='color:#b00020'>" +
                        esc(data?.message || "Nao foi possivel alterar o nivel do Coach.") +
                        "</span>";
                    return;
                }

                // Descarta fila visual criada pelo modo anterior.
                resetCoachDisplay();

                applyModeUI(
                    data.mode,
                    data.description || ""
                );

                modeFeedback.innerHTML =
                    "<span style='color:#177245'>Nivel alterado para <b>" +
                    esc(data.label || modeLabel(data.mode)) +
                    "</b>.</span>";
            }
            catch (error) {
                modeFeedback.innerHTML =
                    "<span style='color:#b00020'>" +
                    esc(String(error)) +
                    "</span>";
            }
            finally {
                changingMode = false;
                setModeButtonsDisabled(false);
            }
        }

        async function iniciar() {
            const value = input.value.trim();

            if (!value) {
                status.innerHTML =
                    "<span style='color:#b00020'>Informe o link ou username.</span>";
                return;
            }

            start.disabled = true;
            input.disabled = true;
            status.textContent = "Preparando monitoramento...";
            resetCoachDisplay();
            resetSalesDisplay(
                "Preparando Coach Vendas...",
                true
            );
            productInfoFeedback.textContent = "";

            try {
                const data = await callPython(
                    "__CALLBACK_START__",
                    [
                        value,
                        sellerPrice.value.trim(),
                        sellerInfo.value.trim(),
                        salesStyle,
                    ]
                );

                if (!data || !data.ok) {
                    status.innerHTML =
                        "<span style='color:#b00020'>" +
                        esc(data?.message || "Nao foi possivel iniciar.") +
                        "</span>";

                    start.disabled = false;
                    input.disabled = false;
                    stop.disabled = true;
                    return;
                }

                currentPlatform = data.platform || null;
                root.dataset.monitorando = "1";

                applyProductInfo.disabled = (
                    currentPlatform !== "shopee"
                );

                if (currentPlatform === "shopee") {
                    renderSalesEmpty(
                        "Aguardando produto destacado na Shopee..."
                    );
                }
                else {
                    renderSalesEmpty(
                        "Coach Vendas automatico para TikTok ainda nao conectado nesta etapa."
                    );
                }

                stop.disabled = false;
                status.innerHTML =
                    "<span style='color:#177245;font-weight:650'>" +
                    "Monitoramento iniciado. Conectando..." +
                    "</span>";
            }
            catch (error) {
                status.innerHTML =
                    "<span style='color:#b00020'>" +
                    esc(String(error)) +
                    "</span>";

                start.disabled = false;
                input.disabled = false;
                stop.disabled = true;
            }
        }

        async function encerrarMonitoramento() {
            stop.disabled = true;
            status.textContent = "Encerrando monitoramento...";

            try {
                await callPython(
                    "__CALLBACK_STOP__",
                    []
                );
            }
            catch (error) {
                status.innerHTML =
                    "<span style='color:#b00020'>" +
                    esc(String(error)) +
                    "</span>";
            }
        }

        async function atualizar() {
            if (polling) return;
            polling = true;

            try {
                const data = await callPython(
                    "__CALLBACK_STATUS__",
                    []
                );

                if (!data) return;

                currentPlatform = data.platform || currentPlatform;
                root.dataset.monitorando = data.monitorando ? "1" : "";

                applyProductInfo.disabled = !(
                    data.monitorando &&
                    currentPlatform === "shopee"
                );

                const backendMode = data.coach_mode?.value || "all";
                const backendDescription = data.coach_mode?.description || "";

                // Nao sobrescrever visual no meio do clique, mas manter
                // Interface sempre sincronizada com o Decision Coach.
                if (!changingMode) {
                    applyModeUI(backendMode, backendDescription);
                }

                debug.innerHTML =
                    "Engine: " +
                    (data.monitorando ? "ativo" : "parado") +
                    " | Eventos: " +
                    String(data.events ?? 0) +
                    " | Coach: " +
                    esc(modeLabel(backendMode)) +
                    (
                        data.last_event
                            ? " | Ultimo evento: " + esc(data.last_event)
                            : ""
                    );

                if (data.error) {
                    status.innerHTML =
                        "<span style='color:#b00020;font-weight:650'>" +
                        esc(data.error) +
                        "</span>";
                }
                else if (data.status === "ativo" || data.status === "conectando") {
                    status.innerHTML =
                        "<span style='color:#177245;font-weight:650'>" +
                        (data.status === "ativo" ? "Monitoramento ativo" : "Conectando a LIVE") +
                        (data.platform ? " - " + esc(String(data.platform).toUpperCase()) : "") +
                        "</span>";
                }
                else if (data.status === "iniciando") {
                    status.textContent = "Iniciando monitoramento...";
                }
                else if (data.status === "encerrando") {
                    status.textContent = "Encerrando monitoramento...";
                }
                else if (data.status === "encerrado" || data.status === "finalizado") {
                    status.innerHTML = "<b>Monitoramento encerrado.</b>";
                }

                if (data.monitorando) {
                    input.disabled = true;
                    start.disabled = true;
                    stop.disabled = false;
                }
                else if (
                    data.status === "encerrado" ||
                    data.status === "finalizado" ||
                    data.status === "erro"
                ) {
                    input.disabled = false;
                    start.disabled = false;
                    stop.disabled = true;
                    root.dataset.monitorando = "";
                    applyProductInfo.disabled = true;
                }

                const platform = data.platform
                    ? String(data.platform).toUpperCase()
                    : "";

                const subject = data.subject || (data.monitorando ? "Conectando..." : "");

                identity.innerHTML = `
                    <div style="font-size:12px;color:#777;">${esc(platform)}</div>
                    <div style="font-size:21px;font-weight:750;margin-top:4px;">${esc(subject)}</div>
                `;

                const m = data.metrics || {};

                let fourthLabel = "PRODUTOS";
                let fourthValue = m.products;

                if (data.platform === "tiktok") {
                    fourthLabel = "FOLLOWS";
                    fourthValue = m.follows;
                }

                cards.innerHTML =
                    card("ESPECTADORES", m.viewers) +
                    card("LIKES", m.likes) +
                    card("COMPARTILHAMENTOS", m.shares) +
                    card(fourthLabel, fourthValue);

                if (data.comments && data.comments.length) {
                    comments.innerHTML = data.comments.map(c => `
                        <div class="agcn-item">
                            <div class="agcn-time">${esc(c.time)}</div>
                            <b>${esc(c.user)}</b>
                            <div>${esc(c.text)}</div>
                        </div>
                    `).join("");
                }
                else {
                    comments.innerHTML =
                        "<span style='color:#777'>Aguardando comentarios...</span>";
                }

                // LIVE COACH V7.1:
                // novas orientacoes entram na fila, mas somente uma fica
                // visivel por vez. O polling nunca redesenha a atual.
                enqueueCoachMessages(data.coach || []);

                // =================================================
                // SALES COACH - independente do LIVE COACH
                // =================================================
                const sales = data.sales_coach || {};

                if (
                    salesGeneration === null ||
                    Number(sales.generation) !== Number(salesGeneration)
                ) {
                    salesGeneration = Number(
                        sales.generation || 0
                    );

                    resetSalesDisplay(
                        currentPlatform === "shopee"
                            ? "Aguardando produto destacado na Shopee..."
                            : "Aguardando uma LIVE Shopee.",
                        true
                    );
                }

                if (
                    sales.enabled &&
                    !changingSalesStyle
                ) {
                    applySalesStyleUI(
                        sales.style || salesStyle,
                        (sales.style || salesStyle) === "pressao_feira"
                            ? `Ritmo mais agil: ${sales.display_seconds ?? 7}s de orientacao + ${sales.interval_seconds ?? 2}s de intervalo.`
                            : `Ritmo natural e continuo: ${sales.display_seconds ?? 8}s de orientacao + ${sales.interval_seconds ?? 3}s de intervalo.`
                    );
                }

                if (sales.error) {
                    salesRuntime.innerHTML =
                        "<span style='color:#b00020'>" +
                        esc(sales.error) +
                        "</span>";
                }
                else if (
                    currentPlatform === "tiktok" &&
                    data.monitorando
                ) {
                    salesRuntime.textContent =
                        "Coach Vendas automatico do TikTok sera conectado quando existir o Worker TikTok Produto.";

                    if (!salesCurrent) {
                        renderSalesEmpty(
                            "Coach Vendas automatico para TikTok ainda nao conectado nesta etapa."
                        );
                    }
                }
                else if (
                    currentPlatform === "shopee" &&
                    data.monitorando
                ) {
                    const productKey = Array.isArray(
                        sales.current_product_key
                    )
                        ? sales.current_product_key.join(" / ")
                        : (
                            sales.current_product_key
                                ? String(sales.current_product_key)
                                : "aguardando produto"
                        );

                    salesRuntime.textContent =
                        `Produto: ${productKey} | Candidatos: ${sales.candidate_count ?? 0} | Ciclo: ${sales.display_seconds ?? 8}s + ${sales.interval_seconds ?? 3}s`;

                    enqueueSalesMessages(
                        sales.messages || []
                    );
                }
                else if (!data.monitorando) {
                    salesRuntime.textContent =
                        "Aguardando uma LIVE Shopee.";
                }
            }
            catch (error) {
                debug.innerHTML =
                    "<span style='color:#b00020'>Falha ao atualizar: " +
                    esc(String(error)) +
                    "</span>";
            }
            finally {
                polling = false;
            }
        }

        start.onclick = iniciar;
        stop.onclick = encerrarMonitoramento;

        input.addEventListener("keydown", event => {
            if (event.key === "Enter") {
                iniciar();
            }
        });

        modeButtons.forEach(button => {
            button.addEventListener("click", () => {
                changeMode(button.dataset.mode);
            });
        });

        salesStyleButtons.forEach(button => {
            button.addEventListener("click", () => {
                changeSalesStyle(
                    button.dataset.salesStyle
                );
            });
        });

        applyProductInfo.onclick =
            applyProductInfoNow;

        applySalesStyleUI(
            salesStyle
        );

        // Leitura inicial + atualizacao continua.
        atualizar();

        const timer = setInterval(
            atualizar,
            700
        );

        root.dataset.agcnTimer = String(timer);
    })();
    </script>
</div>
"""

pagina = (
    pagina
    .replace("__UI_ID__", UI_ID)
    .replace("__CALLBACK_START__", CALLBACK_START)
    .replace("__CALLBACK_STOP__", CALLBACK_STOP)
    .replace("__CALLBACK_STATUS__", CALLBACK_STATUS)
    .replace("__CALLBACK_ALERT_MODE__", CALLBACK_ALERT_MODE)
    .replace("__CALLBACK_SALES_STYLE__", CALLBACK_SALES_STYLE)
    .replace("__CALLBACK_PRODUCT_INFO__", CALLBACK_PRODUCT_INFO)
)


# ============================================================
# 19. MOSTRAR INTERFACE
# ============================================================

display(HTML(pagina))

print("AGCN Interface V8.0 carregada.")
print("Decision Coach V1.2 conectado ao seletor de nivel.")
print("Modos: Todos | Prioridade Alta | Essenciais.")
print("LIVE COACH preservado e independente.")
print("SALES COACH Shopee: Equilibrado 8s+3s | Pressao/Feira 7s+2s.")
print("Agora use somente os botoes da interface.")
