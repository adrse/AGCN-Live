import asyncio
import json
import time
import uuid

from collections import deque
from datetime import datetime
from urllib.parse import (
    urlparse,
    parse_qs,
    unquote
)
from zoneinfo import ZoneInfo

import requests

from playwright.async_api import (
    async_playwright
)

from IPython.display import (
    clear_output
)


# ============================================================
# AGCN SHOPEE WORKER
#
# Esta celula apenas CARREGA o motor.
# Quem inicia a LIVE sera a interface unica.
# ============================================================


# ============================================================
# ENTRADA
# ============================================================

LINK_CURTO = None


# ============================================================
# CONFIGURACOES
# ============================================================

DURACAO = 300

MAX_COMENTARIOS = 10

TIMEZONE = ZoneInfo(
    "America/Araguaina"
)


# ============================================================
# ESTADO DA LIVE
# ============================================================

estado = {

    "sessionId":
        None,

    "chatroomId":
        None,

    "loja":
        None,

    "username":
        None,

    "titulo":
        None,

    "viewers":
        None,

    "likes":
        None,

    "shares":
        None,

    "products":
        None,

    "memberCnt":
        None,

    "status":
        None,

    "ultimaMetrica":
        None,

    "ultimoChat":
        None,

    "erro":
        None,

    "urlExpandida":
        None,
}


comentarios = deque(
    maxlen=MAX_COMENTARIOS
)

comentarios_ids = set()


encerrar = False


LIVE_URL = None

SESSION_ID = None

ENDPOINT_SESSION = None


# ============================================================
# RESOLVEDOR MOBILE DO LINK CURTO
# ============================================================

def host_permitido(
    host
):

    host = (
        host
        or ""
    ).lower().rstrip(".")


    return (

        host == "br.shp.ee"

        or

        host.endswith(
            ".shp.ee"
        )

        or

        host == "shopee.com.br"

        or

        host.endswith(
            ".shopee.com.br"
        )

    )


def validar_url_shopee(
    url
):

    p = urlparse(
        url
    )


    if p.scheme != "https":

        raise ValueError(

            f"Somente HTTPS e permitido: "
            f"{p.scheme}"

        )


    if not host_permitido(
        p.hostname
    ):

        raise ValueError(

            f"Dominio nao permitido: "
            f"{p.hostname}"

        )


def extrair_session_id(
    url
):

    try:

        query = parse_qs(
            urlparse(
                url
            ).query
        )


        valor = query.get(
            "session"
        )


        if not valor:

            return None


        return str(
            valor[0]
        )


    except Exception:

        return None


def detectar_live(
    url
):

    if not url:

        return None


    candidato = str(
        url
    )


    # ----------------------------------------
    # Decodifica URLs percent-encoded
    # ----------------------------------------

    for _ in range(3):

        novo = unquote(
            candidato
        )


        if novo == candidato:

            break


        candidato = novo


    try:

        p = urlparse(
            candidato
        )


    except Exception:

        return None


    if (
        p.hostname
        !=
        "live.shopee.com.br"
    ):

        return None


    session_id = (
        extrair_session_id(
            candidato
        )
    )


    if not session_id:

        return None


    return {

        "url":
            candidato,

        "sessionId":
            session_id,

    }


async def resolver_link_mobile(
    browser,
    link
):

    """
    Recebe APENAS um link curto br.shp.ee
    e resolve a LIVE em contexto mobile,
    observando a navegacao real da Shopee.
    """


    validar_url_shopee(
        link
    )


    host_inicial = (

        urlparse(
            link
        ).hostname

        or ""

    ).lower()


    if (
        host_inicial
        !=
        "br.shp.ee"
    ):

        raise ValueError(

            "Informe um link curto no formato "
            "https://br.shp.ee/..."

        )


    # ========================================================
    # CONTEXTO MOBILE
    # ========================================================

    context = (
        await browser.new_context(

            user_agent=(

                "Mozilla/5.0 "
                "(iPhone; CPU iPhone OS 18_0 like Mac OS X) "
                "AppleWebKit/605.1.15 "
                "(KHTML, like Gecko) "
                "Version/18.0 "
                "Mobile/15E148 "
                "Safari/604.1"

            ),

            viewport={
                "width": 390,
                "height": 844
            },

            screen={
                "width": 390,
                "height": 844
            },

            device_scale_factor=3,

            is_mobile=True,

            has_touch=True,

            locale="pt-BR",

            timezone_id=
                "America/Araguaina",

        )
    )


    page = await context.new_page()


    urls_vistas = set()


    def registrar_request(
        request
    ):

        try:

            urls_vistas.add(
                request.url
            )


        except Exception:

            pass


    page.on(
        "request",
        registrar_request
    )


    try:

        # ====================================================
        # ABRIR LINK CURTO
        # ====================================================

        try:

            await page.goto(

                link,

                wait_until=
                    "domcontentloaded",

                timeout=30000,

            )


        except Exception:

            # Mesmo que haja timeout parcial,
            # continuamos observando a navegacao.

            pass


        # ====================================================
        # OBSERVAR POR ATE 30 SEGUNDOS
        # ====================================================

        for _ in range(60):


            # ------------------------------------------------
            # 1. URL PRINCIPAL
            # ------------------------------------------------

            achou = detectar_live(
                page.url
            )


            if achou:

                validar_url_shopee(
                    achou["url"]
                )

                return achou


            # ------------------------------------------------
            # 2. REQUESTS VISTOS PELO NAVEGADOR
            # ------------------------------------------------

            for url in list(
                urls_vistas
            ):

                achou = detectar_live(
                    url
                )


                if achou:

                    validar_url_shopee(
                        achou["url"]
                    )

                    return achou


            # ------------------------------------------------
            # 3. LINKS EXISTENTES NO DOM
            # ------------------------------------------------

            try:

                links = (
                    await page.eval_on_selector_all(

                        "a[href]",

                        """
                        els => els
                            .map(e => e.href)
                            .filter(Boolean)
                        """

                    )
                )


                for url in links:

                    achou = detectar_live(
                        url
                    )


                    if achou:

                        validar_url_shopee(
                            achou["url"]
                        )

                        return achou


            except Exception:

                pass


            # ------------------------------------------------
            # 4. CANONICAL / OG:URL
            # ------------------------------------------------

            try:

                candidatos = (
                    await page.evaluate(

                        """
                        () => {

                            const r = [];

                            const canonical =
                                document.querySelector(
                                    'link[rel="canonical"]'
                                );

                            if (canonical?.href) {
                                r.push(
                                    canonical.href
                                );
                            }

                            const og =
                                document.querySelector(
                                    'meta[property="og:url"]'
                                );

                            if (og?.content) {
                                r.push(
                                    og.content
                                );
                            }

                            return r;
                        }
                        """

                    )
                )


                for url in candidatos:

                    achou = detectar_live(
                        url
                    )


                    if achou:

                        validar_url_shopee(
                            achou["url"]
                        )

                        return achou


            except Exception:

                pass


            await asyncio.sleep(
                0.5
            )


        raise RuntimeError(

            "Nao consegui resolver o link curto "
            "para uma LIVE com session=..."

        )


    finally:

        await context.close()


# ============================================================
# UTILITARIOS
# ============================================================

def agora():

    return datetime.now(
        TIMEZONE
    ).strftime(
        "%H:%M:%S"
    )


def tempo_desde(
    ts
):

    if ts is None:

        return "aguardando"


    segundos = (
        time.time()
        -
        ts
    )


    if segundos < 1:

        return "agora"


    return (
        f"{segundos:.1f}s atras"
    )


# ============================================================
# SHOPEE WORKER VALIDADO
# ============================================================

async def ler_sessao(
    browser
):

    context = None


    try:

        # ====================================================
        # CONTEXTO NOVO PARA CADA LEITURA
        # ====================================================

        context = (
            await browser.new_context(

                locale="pt-BR",

                viewport={
                    "width": 1024,
                    "height": 720
                },

            )
        )


        page = await context.new_page()


        # ====================================================
        # BLOQUEAR RECURSOS PESADOS
        # ====================================================

        async def filtrar(
            route
        ):

            tipo = (
                route
                .request
                .resource_type
            )


            if tipo in {

                "image",
                "media",
                "font",

            }:

                await route.abort()


            else:

                await route.continue_()


        await page.route(
            "**/*",
            filtrar
        )


        # ====================================================
        # ESPERAR A PROPRIA SHOPEE SOLICITAR A SESSAO
        # ====================================================

        async with page.expect_response(

            lambda r:

                urlparse(
                    r.url
                ).path

                ==

                ENDPOINT_SESSION,

            timeout=20000,

        ) as info:


            try:

                await page.goto(

                    LIVE_URL,

                    wait_until=
                        "commit",

                    timeout=20000,

                )


            except Exception:

                pass


        response = (
            await info.value
        )


        # ====================================================
        # HTTP
        # ====================================================

        if (
            response.status
            !=
            200
        ):

            return False


        # ====================================================
        # JSON
        # ====================================================

        try:

            payload = (
                await response.json()
            )


        except Exception:

            return False


        data = (
            payload.get(
                "data"
            )

            or {}

        )


        sessao = data.get(
            "session"
        )


        if not isinstance(
            sessao,
            dict
        ):

            sessao = (

                data

                if isinstance(
                    data,
                    dict
                )

                else None

            )


        if not isinstance(
            sessao,
            dict
        ):

            return False


        # ====================================================
        # CONFIRMAR SESSAO
        # ====================================================

        if not (

            "viewer_count"
            in
            sessao

            or

            "like_cnt"
            in
            sessao

            or

            "chatroom_id"
            in
            sessao

        ):

            return False


        # ====================================================
        # ATUALIZAR ESTADO
        # ====================================================

        estado[
            "chatroomId"
        ] = (

            sessao.get(
                "chatroom_id"
            )

            or

            estado[
                "chatroomId"
            ]

        )


        estado[
            "loja"
        ] = (

            sessao.get(
                "nickname"
            )

            or

            estado[
                "loja"
            ]

        )


        estado[
            "username"
        ] = (

            sessao.get(
                "username"
            )

            or

            estado[
                "username"
            ]

        )


        estado[
            "titulo"
        ] = (

            sessao.get(
                "title"
            )

            or

            estado[
                "titulo"
            ]

        )


        estado[
            "viewers"
        ] = sessao.get(

            "viewer_count",

            estado[
                "viewers"
            ]

        )


        estado[
            "likes"
        ] = sessao.get(

            "like_cnt",

            estado[
                "likes"
            ]

        )


        estado[
            "shares"
        ] = sessao.get(

            "share_cnt",

            estado[
                "shares"
            ]

        )


        estado[
            "products"
        ] = sessao.get(

            "items_cnt",

            estado[
                "products"
            ]

        )


        estado[
            "memberCnt"
        ] = sessao.get(

            "member_cnt",

            estado[
                "memberCnt"
            ]

        )


        estado[
            "status"
        ] = sessao.get(

            "status",

            estado[
                "status"
            ]

        )


        estado[
            "ultimaMetrica"
        ] = time.time()


        estado[
            "erro"
        ] = None


        return True


    except Exception as e:

        estado[
            "erro"
        ] = str(e)

        return False


    finally:

        if context:

            try:

                await context.close()


            except Exception:

                pass


# ============================================================
# WORKER CONTINUO DE METRICAS
# ============================================================

async def metricas_worker(
    browser
):

    global encerrar


    while not encerrar:

        sucesso = await ler_sessao(
            browser
        )


        await asyncio.sleep(

            0.4
            if sucesso
            else 1.2

        )


# ============================================================
# CHAT
# ============================================================

def requisicao_chat(
    chatroom_id,
    chat_uuid,
    cursor
):

    url = (

        "https://chatroom-live.shopee.com.br"

        f"/api/v1/fetch/chatroom/"

        f"{chatroom_id}"

        f"/message"

    )


    return requests.get(

        url,

        params={

            "uuid":
                chat_uuid,

            "timestamp":
                cursor,

            "version":
                "v2",

        },

        headers={

            "Accept":
                "application/json,text/plain,*/*",

            "User-Agent":
                "Mozilla/5.0",

            "X-Livestreaming-Source":
                "shopee",

        },

        timeout=15,

    )


async def chat_worker():

    global encerrar


    chat_uuid = str(
        uuid.uuid4()
    )


    cursor = (
        int(
            time.time()
        )
        -
        10
    )


    # ========================================================
    # ESPERAR CHATROOM ID
    # ========================================================

    while (

        not estado[
            "chatroomId"
        ]

        and

        not encerrar

    ):

        await asyncio.sleep(
            0.5
        )


    # ========================================================
    # LOOP DO CHAT
    # ========================================================

    while not encerrar:


        chatroom = estado[
            "chatroomId"
        ]


        if not chatroom:

            await asyncio.sleep(
                1
            )

            continue


        try:

            response = (
                await asyncio.to_thread(

                    requisicao_chat,

                    chatroom,

                    chat_uuid,

                    cursor,

                )
            )


            if (
                response.status_code
                !=
                200
            ):

                await asyncio.sleep(
                    2
                )

                continue


            resposta = (
                response.json()
            )


            data = (

                resposta.get(
                    "data"
                )

                or {}

            )


            # =================================================
            # CURSOR
            # =================================================

            if (
                data.get(
                    "timestamp"
                )
                is not None
            ):

                cursor = int(
                    data[
                        "timestamp"
                    ]
                )


            # =================================================
            # MENSAGENS
            # =================================================

            for grupo in (

                data.get(
                    "message"
                )

                or []

            ):


                for msg in grupo.get(
                    "msgs",
                    []
                ):


                    msg_id = str(

                        msg.get(
                            "id",
                            ""
                        )

                    )


                    # -----------------------------------------
                    # DEDUPE
                    # -----------------------------------------

                    if (

                        msg_id

                        and

                        msg_id
                        in
                        comentarios_ids

                    ):

                        continue


                    if msg_id:

                        comentarios_ids.add(
                            msg_id
                        )


                    # -----------------------------------------
                    # NOME
                    # -----------------------------------------

                    nome = (

                        msg.get(
                            "display_name"
                        )

                        or

                        msg.get(
                            "nickname"
                        )

                        or

                        "Usuario"

                    )


                    # -----------------------------------------
                    # CONTEUDO
                    # -----------------------------------------

                    bruto = msg.get(
                        "content"
                    )


                    texto = None


                    if isinstance(
                        bruto,
                        str
                    ):

                        try:

                            conteudo = (
                                json.loads(
                                    bruto
                                )
                            )


                            texto = (

                                conteudo.get(
                                    "content_v2"
                                )

                                or

                                conteudo.get(
                                    "content"
                                )

                            )


                        except Exception:

                            texto = bruto


                    if texto:

                        comentarios.append({

                            "hora":
                                agora(),

                            "usuario":
                                nome,

                            "texto":
                                texto,

                        })


            estado[
                "ultimoChat"
            ] = time.time()


            # =================================================
            # INTERVALO DO CHAT
            # =================================================

            intervalo = data.get(
                "poll_interval",
                3
            )


            try:

                intervalo = float(
                    intervalo
                )


            except Exception:

                intervalo = 3


            await asyncio.sleep(

                max(
                    2,
                    intervalo
                )

            )


        except Exception:

            await asyncio.sleep(
                2
            )


# ============================================================
# PAINEL ORIGINAL DO WORKER
#
# Nossa interface unica podera substituir esta funcao
# temporariamente por um painel silencioso.
# ============================================================

async def painel():

    global encerrar


    inicio = time.time()


    while (

        not encerrar

        and

        time.time()
        -
        inicio
        <
        DURACAO

    ):


        clear_output(
            wait=True
        )


        loja = (

            estado[
                "loja"
            ]

            or

            estado[
                "username"
            ]

            or

            "Carregando..."

        )


        print(
            "=" * 60
        )

        print(
            "AGCN SHOPEE LIVE"
        )

        print(
            "=" * 60
        )


        print(
            loja
        )


        if estado[
            "titulo"
        ]:

            print(
                estado[
                    "titulo"
                ]
            )


        print()


        print(

            "ESPECTADORES :",

            estado[
                "viewers"
            ]

            if

            estado[
                "viewers"
            ]
            is not None

            else "-"

        )


        print(

            "LIKES        :",

            estado[
                "likes"
            ]

            if

            estado[
                "likes"
            ]
            is not None

            else "-"

        )


        print(

            "COMPART.     :",

            estado[
                "shares"
            ]

            if

            estado[
                "shares"
            ]
            is not None

            else "-"

        )


        print(

            "PRODUTOS     :",

            estado[
                "products"
            ]

            if

            estado[
                "products"
            ]
            is not None

            else "-"

        )


        print(

            "MEMBER CNT   :",

            estado[
                "memberCnt"
            ]

            if

            estado[
                "memberCnt"
            ]
            is not None

            else "-"

        )


        print()


        print(

            "Metricas:",

            tempo_desde(

                estado[
                    "ultimaMetrica"
                ]

            )

        )


        print()

        print(
            "-" * 60
        )

        print(
            "COMENTARIOS"
        )

        print(
            "-" * 60
        )


        if not comentarios:

            print(
                "Aguardando comentarios..."
            )


        else:

            for comentario in comentarios:

                print(

                    f"[{comentario['hora']}] "

                    f"{comentario['usuario']}: "

                    f"{comentario['texto']}"

                )


        print()


        print(

            "Chat:",

            tempo_desde(

                estado[
                    "ultimoChat"
                ]

            )

        )


        print()


        print(

            "Monitoramento ativo. "
            "Interrompa a celula para parar."

        )


        await asyncio.sleep(
            0.5
        )


    encerrar = True


# ============================================================
# EXECUCAO PRINCIPAL DO WORKER
# ============================================================

async def main():

    global encerrar
    global LIVE_URL
    global SESSION_ID
    global ENDPOINT_SESSION


    encerrar = False


    # ========================================================
    # VALIDAR LINK
    # ========================================================

    if (

        not LINK_CURTO

        or

        "COLE_AQUI"
        in
        LINK_CURTO

    ):

        print(

            "Cole primeiro o link curto "
            "da LIVE em LINK_CURTO."

        )

        return


    # ========================================================
    # PLAYWRIGHT
    # ========================================================

    async with async_playwright() as p:


        browser = (
            await p.chromium.launch(

                headless=True,

                args=[

                    "--no-sandbox",

                    "--disable-dev-shm-usage",

                ],

            )
        )


        clear_output(
            wait=True
        )


        print(
            "=" * 60
        )

        print(
            "AGCN SHOPEE"
        )

        print(
            "=" * 60
        )

        print()

        print(
            "Link curto recebido:"
        )

        print(
            LINK_CURTO
        )

        print()

        print(
            "Resolvendo link automaticamente..."
        )


        # ====================================================
        # RESOLVER LINK
        # ====================================================

        try:

            resolvido = (
                await resolver_link_mobile(

                    browser,

                    LINK_CURTO.strip()

                )
            )


            LIVE_URL = (
                resolvido[
                    "url"
                ]
            )


            SESSION_ID = (
                resolvido[
                    "sessionId"
                ]
            )


            ENDPOINT_SESSION = (

                f"/api/v1/session/"
                f"{SESSION_ID}"

            )


            estado[
                "sessionId"
            ] = SESSION_ID


            estado[
                "urlExpandida"
            ] = LIVE_URL


        except Exception as e:


            await browser.close()


            print()

            print(
                "Nao consegui resolver "
                "o link curto."
            )

            print(
                str(e)
            )


            return


        # ====================================================
        # INICIAR
        # ====================================================

        print()

        print(
            "Link resolvido com sucesso."
        )

        print(
            "Session ID:",
            SESSION_ID
        )

        print(
            "Iniciando Shopee Worker..."
        )


        await asyncio.sleep(
            1
        )


        # ====================================================
        # TASKS
        # ====================================================

        tarefa_metricas = (
            asyncio.create_task(

                metricas_worker(
                    browser
                )

            )
        )


        tarefa_chat = (
            asyncio.create_task(

                chat_worker()

            )
        )


        tarefa_painel = (
            asyncio.create_task(

                painel()

            )
        )


        try:

            await tarefa_painel


        except asyncio.CancelledError:

            pass


        finally:

            encerrar = True


            tarefa_metricas.cancel()

            tarefa_chat.cancel()


            try:

                await tarefa_metricas


            except BaseException:

                pass


            try:

                await tarefa_chat


            except BaseException:

                pass


            await browser.close()


    clear_output(
        wait=True
    )


    print(
        "Monitoramento encerrado."
    )


# ============================================================
# IMPORTANTE
#
# NAO COLOCAR:
#
# await main()
#
# A INTERFACE UNICA E O LIVE ENGINE VAO INICIAR O WORKER.
# ============================================================

print(
    "AGCN Shopee Worker carregado."
)
