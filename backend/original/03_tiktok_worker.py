import asyncio
import time

from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo

from IPython.display import (
    clear_output
)

from TikTokLive import (
    TikTokLiveClient
)

from TikTokLive.events import (
    ConnectEvent,
    DisconnectEvent,
    LiveEndEvent,
    CommentEvent,
    LikeEvent,
    RoomUserSeqEvent,
    FollowEvent,
    ShareEvent,
    GiftEvent,
)


# ============================================================
# AGCN TIKTOK WORKER
#
# Esta celula apenas CARREGA o motor.
#
# Ela NAO pede username.
# Ela NAO inicia uma LIVE sozinha.
#
# Quem inicia sera:
#
# INTERFACE UNICA
#       ↓
# TIKTOK WORKER
#       ↓
# LIVE ENGINE
#       ↓
# INTERFACE / COACH
# ============================================================


# ============================================================
# CONFIGURACOES
# ============================================================

DURACAO_TIKTOK = 300

MAX_COMENTARIOS_TIKTOK = 12

MAX_PRESENTES_TIKTOK = 8

TIMEZONE_TIKTOK = ZoneInfo(
    "America/Araguaina"
)


# ============================================================
# ESTADO
# ============================================================

estado_tiktok = {}


comentarios_tiktok = deque(
    maxlen=MAX_COMENTARIOS_TIKTOK
)


presentes_tiktok = deque(
    maxlen=MAX_PRESENTES_TIKTOK
)


comentarios_ids_tiktok = set()


comentarios_assinaturas_tiktok = {}


cliente_tiktok = None


# ============================================================
# UTILITARIOS
# ============================================================

def agora_tiktok():

    return datetime.now(
        TIMEZONE_TIKTOK
    ).strftime(
        "%H:%M:%S"
    )


def tempo_desde_tiktok(
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
# NORMALIZAR USERNAME
#
# Aceita:
#
# katianesilvashop
# @katianesilvashop
# https://www.tiktok.com/@katianesilvashop
# https://www.tiktok.com/@katianesilvashop/live
#
# Retorna sempre:
#
# @katianesilvashop
# ============================================================

def normalizar_username_tiktok(
    valor
):

    valor = (
        valor
        or ""
    ).strip()


    if not valor:

        return None


    # ========================================================
    # URL COMPLETA
    # ========================================================

    if (
        "tiktok.com/@"
        in
        valor
    ):

        parte = valor.split(
            "tiktok.com/@",
            1
        )[1]


        usuario = parte.split(
            "/",
            1
        )[0]


        usuario = usuario.split(
            "?",
            1
        )[0]


        usuario = usuario.strip()


        if not usuario:

            return None


        return (
            "@"
            +
            usuario.lstrip("@")
        )


    # ========================================================
    # USERNAME SIMPLES
    # ========================================================

    valor = valor.split()[0]


    valor = (
        valor
        .lstrip("@")
        .strip()
    )


    if not valor:

        return None


    return (
        "@"
        +
        valor
    )


# ============================================================
# RESET DE UMA NOVA LIVE
# ============================================================

def reset_tiktok_state(
    username
):

    global estado_tiktok
    global cliente_tiktok


    cliente_tiktok = None


    comentarios_tiktok.clear()

    presentes_tiktok.clear()

    comentarios_ids_tiktok.clear()

    comentarios_assinaturas_tiktok.clear()


    estado_tiktok = {

        "username":
            username,

        "conectado":
            False,

        "encerrada":
            False,

        "roomId":
            None,


        # ====================================================
        # METRICAS PRINCIPAIS
        # ====================================================

        "viewers":
            None,

        "totalUser":
            None,

        "likes":
            None,

        "ultimoLikeDelta":
            None,


        # ====================================================
        # CONTADORES OBSERVADOS DESDE O INICIO
        # ====================================================

        "followsObservados":
            0,

        "sharesObservados":
            0,

        "presentesObservados":
            0,


        # ====================================================
        # TEMPOS
        # ====================================================

        "ultimaMetrica":
            None,

        "ultimoComentario":
            None,

        "ultimoEvento":
            None,


        # ====================================================
        # ERRO
        # ====================================================

        "erro":
            None,

    }


# ============================================================
# NOME DO USUARIO
# ============================================================

def _user_name(
    user
):

    if user is None:

        return "Usuario"


    return (

        getattr(
            user,
            "nickname",
            None
        )

        or

        getattr(
            user,
            "unique_id",
            None
        )

        or

        getattr(
            user,
            "display_id",
            None
        )

        or

        "Usuario"

    )


# ============================================================
# ID DE COMENTARIO
# ============================================================

def _comment_msg_id(
    event
):

    """
    Tenta usar um ID real da mensagem.

    Se a versao do TikTokLive nao expuser
    um ID, retorna None.
    """


    common = getattr(
        event,
        "common",
        None
    )


    if common is not None:

        for campo in (

            "msg_id",
            "message_id",
            "id",

        ):

            valor = getattr(
                common,
                campo,
                None
            )


            if valor:

                return str(
                    valor
                )


    for campo in (

        "msg_id",
        "message_id",
        "id",

    ):

        valor = getattr(
            event,
            campo,
            None
        )


        if valor:

            return str(
                valor
            )


    return None


# ============================================================
# DEDUPE COMPLEMENTAR DOS COMENTARIOS
# ============================================================

def _comentario_duplicado(
    usuario,
    texto,
    janela=5
):

    agora_ts = time.time()


    assinatura = (

        str(
            usuario
        )
        .strip()
        .lower(),

        str(
            texto
        )
        .strip()
        .lower(),

    )


    anterior = (
        comentarios_assinaturas_tiktok.get(
            assinatura
        )
    )


    comentarios_assinaturas_tiktok[
        assinatura
    ] = agora_ts


    # ========================================================
    # LIMPEZA DAS ASSINATURAS ANTIGAS
    # ========================================================

    limite = (
        agora_ts
        -
        60
    )


    antigas = [

        chave

        for chave, ts

        in comentarios_assinaturas_tiktok.items()

        if ts < limite

    ]


    for chave in antigas:

        comentarios_assinaturas_tiktok.pop(
            chave,
            None
        )


    if anterior is None:

        return False


    return (

        agora_ts
        -
        anterior

    ) <= janela


# ============================================================
# LISTENERS DO TIKTOK
# ============================================================

def instalar_listeners_tiktok(
    client
):


    # ========================================================
    # CONEXAO
    # ========================================================

    @client.on(
        ConnectEvent
    )

    async def on_connect(
        event: ConnectEvent
    ):

        estado_tiktok[
            "conectado"
        ] = True


        estado_tiktok[
            "roomId"
        ] = (

            str(
                client.room_id
            )

            if

            client.room_id
            is not None

            else None

        )


        estado_tiktok[
            "ultimoEvento"
        ] = time.time()


        estado_tiktok[
            "erro"
        ] = None


    # ========================================================
    # ESPECTADORES
    # ========================================================

    @client.on(
        RoomUserSeqEvent
    )

    async def on_viewers(
        event: RoomUserSeqEvent
    ):

        # Nos testes validados,
        # event.total acompanhou a audiencia atual.

        total = getattr(
            event,
            "total",
            None
        )


        if total is not None:

            estado_tiktok[
                "viewers"
            ] = int(
                total
            )


        # ----------------------------------------------------
        # TOTAL_USER
        #
        # Mantemos separado.
        # Nao tratar como audiencia atual.
        # ----------------------------------------------------

        total_user = getattr(
            event,
            "total_user",
            None
        )


        if total_user is not None:

            estado_tiktok[
                "totalUser"
            ] = int(
                total_user
            )


        estado_tiktok[
            "ultimaMetrica"
        ] = time.time()


        estado_tiktok[
            "ultimoEvento"
        ] = time.time()


    # ========================================================
    # LIKES
    # ========================================================

    @client.on(
        LikeEvent
    )

    async def on_like(
        event: LikeEvent
    ):

        total = getattr(
            event,
            "total",
            None
        )


        count = getattr(
            event,
            "count",
            None
        )


        if total is not None:

            estado_tiktok[
                "likes"
            ] = int(
                total
            )


        if count is not None:

            estado_tiktok[
                "ultimoLikeDelta"
            ] = int(
                count
            )


        estado_tiktok[
            "ultimaMetrica"
        ] = time.time()


        estado_tiktok[
            "ultimoEvento"
        ] = time.time()


    # ========================================================
    # COMENTARIOS
    # ========================================================

    @client.on(
        CommentEvent
    )

    async def on_comment(
        event: CommentEvent
    ):

        texto = getattr(
            event,
            "comment",
            None
        )


        if not texto:

            return


        usuario = _user_name(

            getattr(
                event,
                "user",
                None
            )

        )


        msg_id = (
            _comment_msg_id(
                event
            )
        )


        # ----------------------------------------------------
        # DEDUPE PELO ID
        # ----------------------------------------------------

        if (

            msg_id

            and

            msg_id
            in
            comentarios_ids_tiktok

        ):

            return


        if msg_id:

            comentarios_ids_tiktok.add(
                msg_id
            )


        # ----------------------------------------------------
        # DEDUPE POR USUARIO + TEXTO
        # ----------------------------------------------------

        if _comentario_duplicado(
            usuario,
            texto
        ):

            return


        comentarios_tiktok.append({

            "hora":
                agora_tiktok(),

            "usuario":
                usuario,

            "texto":
                str(
                    texto
                ),

        })


        estado_tiktok[
            "ultimoComentario"
        ] = time.time()


        estado_tiktok[
            "ultimoEvento"
        ] = time.time()


    # ========================================================
    # FOLLOW
    # ========================================================

    @client.on(
        FollowEvent
    )

    async def on_follow(
        event: FollowEvent
    ):

        estado_tiktok[
            "followsObservados"
        ] += 1


        estado_tiktok[
            "ultimoEvento"
        ] = time.time()


    # ========================================================
    # SHARE
    # ========================================================

    @client.on(
        ShareEvent
    )

    async def on_share(
        event: ShareEvent
    ):

        estado_tiktok[
            "sharesObservados"
        ] += 1


        estado_tiktok[
            "ultimoEvento"
        ] = time.time()


    # ========================================================
    # PRESENTES
    # ========================================================

    @client.on(
        GiftEvent
    )

    async def on_gift(
        event: GiftEvent
    ):

        gift = getattr(
            event,
            "gift",
            None
        )


        if gift is None:

            return


        # ----------------------------------------------------
        # GIFT EM SEQUENCIA
        #
        # Ignora eventos intermediarios.
        # ----------------------------------------------------

        if bool(

            getattr(
                event,
                "streaking",
                False
            )

        ):

            return


        quantidade = getattr(
            event,
            "repeat_count",
            None
        )


        try:

            quantidade = int(
                quantidade
            )


        except Exception:

            quantidade = 1


        if quantidade < 1:

            quantidade = 1


        nome_gift = (

            getattr(
                gift,
                "name",
                None
            )

            or

            "Presente"

        )


        usuario = _user_name(

            getattr(
                event,
                "user",
                None
            )

        )


        estado_tiktok[
            "presentesObservados"
        ] += quantidade


        estado_tiktok[
            "ultimoEvento"
        ] = time.time()


        presentes_tiktok.append({

            "hora":
                agora_tiktok(),

            "usuario":
                usuario,

            "presente":
                str(
                    nome_gift
                ),

            "quantidade":
                quantidade,

        })


    # ========================================================
    # FIM DA LIVE
    # ========================================================

    @client.on(
        LiveEndEvent
    )

    async def on_live_end(
        event: LiveEndEvent
    ):

        estado_tiktok[
            "encerrada"
        ] = True


        estado_tiktok[
            "conectado"
        ] = False


        estado_tiktok[
            "ultimoEvento"
        ] = time.time()


    # ========================================================
    # DESCONEXAO
    # ========================================================

    @client.on(
        DisconnectEvent
    )

    async def on_disconnect(
        event: DisconnectEvent
    ):

        estado_tiktok[
            "conectado"
        ] = False


        estado_tiktok[
            "ultimoEvento"
        ] = time.time()


# ============================================================
# PAINEL ORIGINAL DO TIKTOK WORKER
#
# Mantemos por compatibilidade.
#
# Quando utilizarmos a INTERFACE UNICA,
# ela substituira temporariamente esta funcao
# por um painel silencioso.
# ============================================================

async def painel_tiktok(
    client
):

    inicio = time.time()


    while (

        time.time()
        -
        inicio

        <
        DURACAO_TIKTOK

        and

        not estado_tiktok[
            "encerrada"
        ]

    ):


        clear_output(
            wait=True
        )


        print(
            "=" * 60
        )

        print(
            "AGCN TIKTOK LIVE"
        )

        print(
            "=" * 60
        )


        print(

            "Usuario :",

            estado_tiktok[
                "username"
            ].lstrip("@")

        )


        print(

            "Room ID :",

            estado_tiktok[
                "roomId"
            ]

            if

            estado_tiktok[
                "roomId"
            ]
            is not None

            else "-"

        )


        print(

            "Status  :",

            "CONECTADO"

            if

            estado_tiktok[
                "conectado"
            ]

            else

            "CONECTANDO..."

        )


        print()


        # ====================================================
        # METRICAS
        # ====================================================

        print(

            "ESPECTADORES :",

            estado_tiktok[
                "viewers"
            ]

            if

            estado_tiktok[
                "viewers"
            ]
            is not None

            else "-"

        )


        print(

            "LIKES        :",

            estado_tiktok[
                "likes"
            ]

            if

            estado_tiktok[
                "likes"
            ]
            is not None

            else "-"

        )


        print(

            "FOLLOWS      :",

            estado_tiktok[
                "followsObservados"
            ],

            "(desde o inicio)"

        )


        print(

            "SHARES       :",

            estado_tiktok[
                "sharesObservados"
            ],

            "(desde o inicio)"

        )


        print(

            "PRESENTES    :",

            estado_tiktok[
                "presentesObservados"
            ],

            "(desde o inicio)"

        )


        if (

            estado_tiktok[
                "totalUser"
            ]
            is not None

        ):

            print(

                "TOTAL_USER   :",

                estado_tiktok[
                    "totalUser"
                ],

                "(campo separado; "
                "nao e audiencia atual)"

            )


        print()


        print(

            "Ultima metrica:",

            tempo_desde_tiktok(

                estado_tiktok[
                    "ultimaMetrica"
                ]

            )

        )


        # ====================================================
        # COMENTARIOS
        # ====================================================

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


        if not comentarios_tiktok:

            print(
                "Aguardando comentarios..."
            )


        else:

            for c in comentarios_tiktok:

                print(

                    f"[{c['hora']}] "

                    f"{c['usuario']}: "

                    f"{c['texto']}"

                )


        # ====================================================
        # PRESENTES
        # ====================================================

        print()

        print(
            "-" * 60
        )

        print(
            "PRESENTES RECENTES"
        )

        print(
            "-" * 60
        )


        if not presentes_tiktok:

            print(
                "Nenhum presente "
                "observado ainda."
            )


        else:

            for g in presentes_tiktok:

                print(

                    f"[{g['hora']}] "

                    f"{g['usuario']}: "

                    f"{g['quantidade']}x "

                    f"{g['presente']}"

                )


        print()


        print(

            "Ultimo comentario:",

            tempo_desde_tiktok(

                estado_tiktok[
                    "ultimoComentario"
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


# ============================================================
# FUNCAO PRINCIPAL DO TIKTOK WORKER
# ============================================================

async def monitorar_tiktok(
    username
):

    """
    Recebe somente o username.

    Faz automaticamente:

    username
        ↓
    verifica LIVE
        ↓
    conecta
        ↓
    descobre Room ID
        ↓
    recebe eventos
    """


    global cliente_tiktok


    # ========================================================
    # NORMALIZAR USERNAME
    # ========================================================

    username = (
        normalizar_username_tiktok(
            username
        )
    )


    if not username:

        print(
            "Username invalido."
        )

        return


    # ========================================================
    # RESET DA SESSAO
    # ========================================================

    reset_tiktok_state(
        username
    )


    # ========================================================
    # CLIENTE TIKTOK
    # ========================================================

    client = TikTokLiveClient(
        unique_id=username
    )


    cliente_tiktok = client


    # ========================================================
    # LISTENERS
    # ========================================================

    instalar_listeners_tiktok(
        client
    )


    print(
        "=" * 60
    )

    print(
        "AGCN TIKTOK"
    )

    print(
        "=" * 60
    )

    print()


    print(

        "Username recebido:",

        username.lstrip("@")

    )


    print()


    print(
        "Verificando se a conta "
        "esta em LIVE..."
    )


    # ========================================================
    # VERIFICAR SE ESTA AO VIVO
    # ========================================================

    try:

        live = (
            await client.is_live()
        )


    except Exception as e:

        print()

        print(
            "Nao consegui verificar "
            "a LIVE."
        )

        print(

            type(e).__name__,

            str(e)

        )


        estado_tiktok[
            "erro"
        ] = str(e)


        return


    if not live:

        print()

        print(
            "Esta conta nao esta "
            "em LIVE agora."
        )


        return


    # ========================================================
    # LIVE ENCONTRADA
    # ========================================================

    print()

    print(
        "LIVE encontrada."
    )

    print(
        "Conectando aos eventos "
        "em tempo real..."
    )


    connection_task = None


    # ========================================================
    # CONEXAO
    # ========================================================

    try:

        # ----------------------------------------------------
        # start() retorna a Task da conexao
        # ----------------------------------------------------

        connection_task = (
            await client.start()
        )


        # ----------------------------------------------------
        # O painel roda enquanto a conexao
        # recebe os eventos.
        #
        # Na interface unica, esta funcao
        # sera temporariamente substituida
        # por uma versao silenciosa.
        # ----------------------------------------------------

        await painel_tiktok(
            client
        )


    except asyncio.CancelledError:

        pass


    except Exception as e:

        estado_tiktok[
            "erro"
        ] = str(e)


        clear_output(
            wait=True
        )


        print(
            "Erro durante "
            "o monitoramento:"
        )


        print(

            type(e).__name__,

            str(e)

        )


    # ========================================================
    # FINALIZAR
    # ========================================================

    finally:

        try:

            if client.connected:

                await client.disconnect()


        except Exception:

            pass


        if (
            connection_task
            is not None
        ):

            try:

                await connection_task


            except BaseException:

                pass


    clear_output(
        wait=True
    )


    if estado_tiktok[
        "encerrada"
    ]:

        print(
            "LIVE encerrada pelo TikTok."
        )


    else:

        print(
            "Monitoramento TikTok encerrado."
        )


# ============================================================
# IMPORTANTE
#
# NAO COLOCAR:
#
# await monitorar_tiktok(...)
#
# O USERNAME VIRÁ DA INTERFACE UNICA.
#
# A INTERFACE VAI CHAMAR:
#
# executar_tiktok_com_live_engine(username)
#
# QUE ESTA DEFINIDA NO LIVE ENGINE.
# ============================================================


print(
    "AGCN TikTok Worker carregado."
)
