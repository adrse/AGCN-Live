# AGCN - COACH PRODUTO V1.1
#
# ARQUITETURA:
#
# Worker Coach Comentarios V1.5
#               ↓
#      Comment Dispatcher V1
#               ↓
#          fila "product"
#               ↓
#        COACH PRODUTO V1.1
#               ↓
#       futuro Context Fusion
#
#
# RESPONSABILIDADES:
#
# - receber somente eventos roteados para Produto
# - interpretar a classificacao ja feita pelo Worker
# - organizar perguntas por tema de produto
# - preservar perguntas individuais
# - identificar qual informacao o vendedor precisa esclarecer
# - identificar pedidos de demonstracao
# - acompanhar repeticao por assunto
# - diferenciar repeticao da mesma pessoa de varias pessoas
# - transformar comment_trend em sinal especializado
# - produzir outputs estruturados para Context Fusion
#
#
# NAO FAZ:
#
# - nao decide prioridade
# - nao decide quando mostrar
# - nao gera aviso final da Interface
# - nao acessa Coach Armazenamento
# - nao acessa Worker Audiencia
# - nao tenta responder a pergunta
# - nao inventa dados do produto
#
#
# SAIDAS:
#
# 1. product_analysis
#    Analise de um comentario individual.
#
# 2. product_topic_signal

#    Movimento/repeticao de um assunto de produto.
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

_CP_REQUIRED = [

       "comment_dispatcher_next_product",

       "live_engine_is_running",

       "live_engine_platform",

       "executar_shopee_com_live_engine",

       "executar_tiktok_com_live_engine",

]


_CP_MISSING = [

       name

       for name in _CP_REQUIRED

       if name not in globals()

]


if _CP_MISSING:


       raise RuntimeError(

              "Execute primeiro o Comment Dispatcher V1. "
              "Faltando: "

              + ", ".join(
                  _CP_MISSING
              )

       )


# ============================================================
# 2. CONFIGURACAO
# ============================================================

COACH_PRODUCT_VERSION = "1.1"


COACH_PRODUCT_TIMEZONE = ZoneInfo(
    "America/Araguaina"
)


COACH_PRODUCT_OUTPUT_QUEUE_LIMIT = 5000

COACH_PRODUCT_MAX_HISTORY = 5000

COACH_PRODUCT_MAX_SEEN = 20000


# ============================================================
# JANELA LOCAL
#
# Nao substitui as tendencias do Worker Comentarios.
#
# Serve para o Coach Produto perceber rapidamente:
#
# - duas perguntas do mesmo assunto
# - mesma pessoa insistindo
# - varias pessoas perguntando
#
# antes mesmo de uma tendencia maior aparecer.
# ============================================================

COACH_PRODUCT_TOPIC_WINDOW_SECONDS = 60


# Comeca a produzir uma atividade agregada
# quando houver pelo menos 2 ocorrencias.

COACH_PRODUCT_LOCAL_TOPIC_THRESHOLD = 2


# Controle de reemissao da mesma atividade.

COACH_PRODUCT_TOPIC_REEMIT_SECONDS = 15

COACH_PRODUCT_TOPIC_FORCE_REEMIT_SECONDS = 40


# ============================================================
# 3. MAPA SEMANTICO DOS TEMAS
#
# O Worker ja classificou o subtipo.
#
# Aqui o Coach Produto acrescenta:
#
# family
# label
# answer_target
#
# answer_target NAO e uma resposta.
#
# Ele apenas descreve qual informacao precisa ser
# esclarecida pelo vendedor.
# ============================================================

COACH_PRODUCT_TOPIC_MAP = {
    # --------------------------------------------------------
    # FUNCIONAMENTO E USO
    # --------------------------------------------------------
    "functionality": {
        "family": "functionality",
        "label": "funcionamento",
        "answer_target": (
            "explicar como o produto funciona ou para que serve"
        ),
    },
    "usage_method": {
        "family": "usage_and_instructions",
        "label": "modo de uso",
        "answer_target": (
            "esclarecer como usar, aplicar, instalar, montar ou preparar "
            "o produto conforme as informacoes disponiveis"
        ),
    },

    # --------------------------------------------------------
    # MARCA, AUTENTICIDADE, CONDICAO E CONFORMIDADE
    # --------------------------------------------------------
    "brand_manufacturer": {
        "family": "brand_and_manufacturer",
        "label": "marca ou fabricante",
        "answer_target": (
            "esclarecer a marca, o fabricante ou quem produz o produto"
        ),
    },
    "authenticity": {
        "family": "authenticity",
        "label": "autenticidade ou originalidade",
        "answer_target": (
            "esclarecer se o produto e original, oficial ou autentico "
            "com base nas informacoes disponiveis"
        ),
    },
    "product_condition": {
        "family": "product_condition",
        "label": "condicao do produto",
        "answer_target": (
            "esclarecer se o produto e novo, usado, recondicionado, "
            "open box, lacrado ou de mostruario"
        ),
    },
    "certification_compliance": {
        "family": "certification_and_compliance",
        "label": "certificacao ou conformidade",
        "answer_target": (
            "esclarecer certificacoes, homologacoes, registros ou selos "
            "aplicaveis ao produto"
        ),
    },
    "warranty": {
        "family": "warranty",
        "label": "garantia",
        "answer_target": (
            "esclarecer se existe garantia e quais condicoes podem ser informadas"
        ),
    },

    # --------------------------------------------------------
    # TAMANHO, VARIACOES E CARACTERISTICAS FISICAS
    # --------------------------------------------------------
    "size_fit": {
        "family": "size_and_fit",
        "label": "tamanho ou encaixe",
        "answer_target": (
            "esclarecer tamanho, numeracao, ajuste ou encaixe do produto"
        ),
    },
    "dimensions": {
        "family": "size_and_dimensions",
        "label": "medidas ou dimensoes",
        "answer_target": (
            "informar medidas como altura, largura, comprimento ou tamanho"
        ),
    },
    "color_variant": {
        "family": "variant",
        "label": "cor ou variacao",
        "answer_target": (
            "esclarecer quais cores ou variacoes do produto estao disponiveis"
        ),
    },
    "material_composition": {
        "family": "material",
        "label": "material ou composicao",
        "answer_target": (
            "informar de qual material ou composicao fisica o produto e feito"
        ),
    },
    "capacity": {
        "family": "capacity",
        "label": "capacidade",
        "answer_target": (
            "esclarecer a capacidade ou quantidade que o produto comporta"
        ),
    },
    "weight": {
        "family": "physical_specification",
        "label": "peso",
        "answer_target": "informar o peso do produto",
    },
    "quantity_package": {
        "family": "package_contents",
        "label": "quantidade do kit",
        "answer_target": (
            "esclarecer quantas unidades ou itens acompanham o produto"
        ),
    },
    "included_items": {
        "family": "package_contents",
        "label": "itens inclusos",
        "answer_target": (
            "informar quais itens ou acessorios acompanham o produto"
        ),
    },

    # --------------------------------------------------------
    # ELETRONICOS E ESPECIFICACOES TECNICAS
    # --------------------------------------------------------
    "compatibility": {
        "family": "compatibility",
        "label": "compatibilidade",
        "answer_target": (
            "esclarecer com quais aparelhos, modelos ou situacoes o produto e compativel"
        ),
    },
    "power_voltage": {
        "family": "technical_specification",
        "label": "voltagem ou potencia",
        "answer_target": (
            "esclarecer a voltagem, se e bivolt e/ou a potencia correta do produto"
        ),
    },
    "battery_duration": {
        "family": "battery",
        "label": "bateria ou autonomia",
        "answer_target": (
            "esclarecer autonomia, duracao, carregamento ou alimentacao da bateria"
        ),
    },
    "connectivity": {
        "family": "technical_specification",
        "label": "conectividade",
        "answer_target": (
            "esclarecer recursos de conexao como Bluetooth, Wi-Fi, NFC, rede movel "
            "ou outras formas de conectividade"
        ),
    },
    "technical_specification": {
        "family": "technical_specification",
        "label": "especificacao tecnica",
        "answer_target": (
            "esclarecer especificacoes como memoria, armazenamento, processador, "
            "tela ou outros dados tecnicos do produto"
        ),
    },

    # --------------------------------------------------------
    # RESISTENCIA E CARACTERISTICAS DE CATEGORIAS
    # --------------------------------------------------------
    "usage_resistance": {
        "family": "usage_and_resistance",
        "label": "uso ou resistencia",
        "answer_target": (
            "esclarecer limites de uso, resistencia e cuidados com o produto"
        ),
    },
    "apparel_characteristic": {
        "family": "apparel_characteristic",
        "label": "caracteristica de roupa ou calcado",
        "answer_target": (
            "esclarecer caracteristicas como elasticidade, transparencia, forro, "
            "bolso, ziper, lavagem, encolhimento, forma ou salto"
        ),
    },
    "sensory_characteristic": {
        "family": "sensory_characteristic",
        "label": "caracteristica sensorial",
        "answer_target": (
            "esclarecer caracteristicas como cheiro, aroma, fragrancia, textura ou sabor"
        ),
    },
    "ingredients_composition": {
        "family": "ingredients_and_composition",
        "label": "ingredientes ou composicao",
        "answer_target": (
            "informar ingredientes ou composicao declarada do produto"
        ),
    },
    "expiration_validity": {
        "family": "validity",
        "label": "validade",
        "answer_target": (
            "esclarecer prazo de validade, vencimento ou informacoes de conservacao "
            "quando disponiveis"
        ),
    },
    "safety_suitability": {
        "family": "suitability_and_safety",
        "label": "adequacao ou seguranca de uso",
        "answer_target": (
            "esclarecer para qual uso, publico ou perfil o produto e indicado com base "
            "nas informacoes do fabricante, sem inferir orientacao medica"
        ),
    },
    "food_characteristic": {
        "family": "food_characteristic",
        "label": "caracteristica de alimento",
        "answer_target": (
            "esclarecer preparo, porcoes, conservacao, calorias, ingredientes ou "
            "alergenos conforme as informacoes disponiveis"
        ),
    },

    # --------------------------------------------------------
    # COMPARACAO
    # --------------------------------------------------------
    "comparison": {
        "family": "comparison",
        "label": "comparacao entre produtos",
        "answer_target": (
            "comparar de forma objetiva as caracteristicas dos produtos mencionados"
        ),
    },

    # --------------------------------------------------------
    # FALLBACK E DEMONSTRACAO
    # --------------------------------------------------------
    "generic_characteristic": {
        "family": "product_characteristic",
        "label": "caracteristica do produto",
        "answer_target": (
            "identificar e esclarecer a caracteristica do produto perguntada pelo cliente"
        ),
    },
    "show_demonstrate": {
        "family": "demonstration",
        "label": "demonstracao do produto",
        "answer_target": (
            "mostrar ou demonstrar visualmente o aspecto solicitado"
        ),
    },
}


# ============================================================
# 3.1. CONTRATO SEMANTICO COM WORKER COMENTARIOS V1.5
#
# Estes sao os subtipos de product_question atualmente emitidos
# pelo Worker Coach Comentarios V1.5. O teste interno verifica
# que nenhum deles caiu no fallback por falta de mapeamento.
# ============================================================
COACH_PRODUCT_WORKER_V15_SUBTYPES = {
    "functionality",
    "usage_method",
    "compatibility",
    "size_fit",
    "dimensions",
    "color_variant",
    "material_composition",
    "capacity",
    "weight",
    "quantity_package",
    "included_items",
    "warranty",
    "brand_manufacturer",
    "authenticity",
    "product_condition",
    "certification_compliance",
    "battery_duration",
    "connectivity",
    "technical_specification",
    "power_voltage",
    "usage_resistance",
    "sensory_characteristic",
    "ingredients_composition",
    "expiration_validity",
    "safety_suitability",
    "apparel_characteristic",
    "food_characteristic",
    "comparison",
}


def coach_product_validate_taxonomy():
    mapped = set(COACH_PRODUCT_TOPIC_MAP)
    missing = sorted(COACH_PRODUCT_WORKER_V15_SUBTYPES - mapped)
    return {
        "worker_version": "1.5",
        "coach_version": COACH_PRODUCT_VERSION,
        "expected_product_subtypes": len(COACH_PRODUCT_WORKER_V15_SUBTYPES),
        "mapped_product_subtypes": len(
            COACH_PRODUCT_WORKER_V15_SUBTYPES & mapped
        ),
        "missing": missing,
        "ok": not missing,
    }


# ============================================================
# 4. FILA PARA O FUTURO CONTEXT FUSION

# ============================================================

coach_product_output_queue = None


# ============================================================
# 5. HISTORICOS
# ============================================================

coach_product_output_history = deque(
    maxlen=
        COACH_PRODUCT_MAX_HISTORY
)


coach_product_individual_history = deque(
    maxlen=
        COACH_PRODUCT_MAX_HISTORY
)


coach_product_topic_signal_history = deque(
    maxlen=
        COACH_PRODUCT_MAX_HISTORY
)


# ============================================================
# 6. JANELAS LOCAIS POR TOPICO
# ============================================================

coach_product_topic_windows = defaultdict(
    deque
)


coach_product_last_topic_emitted = {}


# ============================================================
# 7. DEDUPLICACAO
# ============================================================

coach_product_seen_dispatch_ids = set()

coach_product_seen_dispatch_order = deque(
    maxlen=
        COACH_PRODUCT_MAX_SEEN
)


# ============================================================
# 8. ESTADO
# ============================================================

coach_product_running = False

coach_product_platform = None

coach_product_live_id = None

coach_product_subject = None

coach_product_started_at = None

coach_product_last_error = None


# ============================================================
# 9. ESTATISTICAS
# ============================================================

coach_product_stats = defaultdict(
    int
)


coach_product_topic_counts = defaultdict(
    int
)


coach_product_family_counts = defaultdict(
    int
)


# ============================================================
# 10. HORARIO
# ============================================================

def _cp_now_iso():


       return (
           datetime
           .now(
                COACH_PRODUCT_TIMEZONE
           )
           .isoformat()
       )


# ============================================================
# 11. INFORMACAO DE UM TOPICO
# ============================================================

def _cp_topic_info(
    category,
    subtype
):

       if (
              category
              ==
              "demo_request"
       ):

              subtype = (
                  subtype
                  or
                  "show_demonstrate"
              )


       if (
              category
              ==
              "product_question"

              and

              not subtype
       ):

              subtype = (
                  "generic_characteristic"
              )

       info = (
           COACH_PRODUCT_TOPIC_MAP.get(
                subtype
           )
       )


       if info is None:

              info = {

                     "family":
                         "product_characteristic",

                     "label":
                         (
                                  str(
                                         subtype
                                         or
                                         "caracteristica do produto"
                                  )
                            ),

                     "answer_target":
                         (
                             "esclarecer a informacao "
                             "solicitada sobre o produto"
                         ),

              }


       return {

              "category":
                  category,

              "subtype":
                  subtype,

              "family":
                  info.get(
                      "family"
                  ),

              "label":
                  info.get(
                       "label"
                  ),

              "answer_target":
                  info.get(
                      "answer_target"
                  ),

       }


# ============================================================
# 12. CHAVE DO TOPICO
# ============================================================

def _cp_topic_key(
    topic
):

       return (

              str(
                     topic.get(
                         "category"
                     )
              )

              +

              ":"

              +

              str(
                     topic.get(
                         "subtype"
                     )
              )

       )


# ============================================================
# 13. REMOVER TOPICOS DUPLICADOS
# ============================================================


def _cp_unique_topics(
    topics
):

       result = []

       seen = set()


       for topic in topics:

              key = (
                  topic.get(
                      "category"
                  ),
                  topic.get(
                      "subtype"
                  ),
              )


              if key in seen:

                     continue


              seen.add(
                  key
              )


              result.append(
                  topic
              )


       return result


# ============================================================
# 14. EXTRAIR TOPICOS DO ENVELOPE DO DISPATCHER
# ============================================================

def _cp_topics_from_envelope(
    envelope

):

       intents = (

              envelope.get(
                  "matched_intents"
              )

              or

              []

       )


       topics = []


       for intent in intents:

              if not isinstance(
                  intent,
                  dict
              ):

                     continue


              category = (
                  intent.get(
                      "category"
                  )
              )


              subtype = (
                  intent.get(
                      "subtype"
                  )
              )


              if (
                     category
                     not in {

                            "product_question",

                            "demo_request",

                     }
              ):

                     continue


              topics.append(

                     _cp_topic_info(
                         category,
                         subtype
                     )

              )


       topics = (
           _cp_unique_topics(
               topics
           )
       )


       # Se uma tendencia category_volume trouxe:
       #
       # product_question / None
       #
       # junto com subtipos especificos,
       # removemos o generico para nao poluir.

       has_specific_product_topic = any(

              topic.get(
                  "category"
              )
              ==
              "product_question"

              and

              topic.get(
                  "subtype"
              )

              !=
              "generic_characteristic"

              for topic in topics

       )


       if has_specific_product_topic:

              topics = [

                     topic

                     for topic in topics

                     if not (

                            topic.get(
                                "category"
                            )
                            ==
                            "product_question"

                            and

                            topic.get(
                                "subtype"
                            )
                            ==
                            "generic_characteristic"

                     )

              ]


       return topics


# ============================================================
# 15. EXTRAIR O COMENTARIO ORIGINAL
# ============================================================

def _cp_comment_from_envelope(
    envelope

):

       payload = (

              envelope.get(
                  "payload"
              )

              or

              {}

       )


       comment = (

              payload.get(
                  "comment"
              )

              or

              {}

       )


       return {

              "event_id":
                  comment.get(
                      "event_id"
                  ),

              "user":
                  comment.get(
                      "user"
                  ),

              "text":
                  comment.get(
                      "text"
                  ),

              "display_time":
                  comment.get(

                         "display_time"
                     ),

       }


# ============================================================
# 16. CLASSIFICACAO ORIGINAL
# ============================================================

def _cp_classification_from_envelope(
    envelope
):

       payload = (

              envelope.get(
                  "payload"
              )

              or

              {}

       )


       return (

              payload.get(
                  "classification"
              )

              or

              {}

       )


# ============================================================
# 17. DEDUPLICACAO
# ============================================================

def _cp_seen(
    envelope

):

       dispatch_id = (
           envelope.get(
               "dispatch_id"
           )
       )


       if not dispatch_id:

              return False


       dispatch_id = str(
           dispatch_id
       )


       if (
              dispatch_id
              in
              coach_product_seen_dispatch_ids
       ):

              coach_product_stats[
                  "duplicates"
              ] += 1


              return True


       if (

              len(
                     coach_product_seen_dispatch_order
              )

              >=

              coach_product_seen_dispatch_order.maxlen

       ):

              old = (

                  coach_product_seen_dispatch_order[0]
              )


              coach_product_seen_dispatch_ids.discard(
                  old
              )


       coach_product_seen_dispatch_order.append(
           dispatch_id
       )


       coach_product_seen_dispatch_ids.add(
           dispatch_id
       )


       return False


# ============================================================
# 18. FILA DE SAIDA
# ============================================================

def _cp_queue_output(
    output
):

       queue = (
           coach_product_output_queue
       )


       if queue is None:

              return False


       try:

              queue.put_nowait(
                  output
              )

       except asyncio.QueueFull:

              try:

                     queue.get_nowait()

              except Exception:

                     pass


              coach_product_stats[
                  "dropped_outputs"
              ] += 1


              try:

                     queue.put_nowait(
                         output
                     )


              except Exception:

                     return False


       coach_product_stats[
           "outputs_sent"
       ] += 1


       coach_product_output_history.append(
           copy.deepcopy(
               output
           )
       )


       return True


# ============================================================
# 19. PODAR JANELA TEMPORAL
# ============================================================


def _cp_prune_topic_window(
    queue,
    now=None
):

       if now is None:

              now = time.time()


       cutoff = (

              now

              -

              COACH_PRODUCT_TOPIC_WINDOW_SECONDS

       )


       while (

              queue

              and

              queue[0].get(
                  "timestamp",
                  0
              )
              <
              cutoff

       ):

              queue.popleft()


# ============================================================
# 20. USUARIOS UNICOS
# ============================================================

def _cp_unique_users(
    records

):

       users = set()


       for record in records:

              user = str(

                     record.get(
                         "user"
                     )

                     or

                     ""

              ).strip().lower()


              if user:

                     users.add(
                         user
                     )


       return len(
           users
       )


# ============================================================
# 21. MAIOR REPETICAO POR UMA MESMA PESSOA
# ============================================================

def _cp_max_repeat_by_one_user(
    records
):

       counters = defaultdict(
           int
       )


       for record in records:

              user = str(

                     record.get(
                         "user"
                     )

                     or

                     ""

              ).strip().lower()


              if not user:

                     continue


              counters[
                  user
              ] += 1


       if not counters:

              return 0


       return max(
           counters.values()
       )


# ============================================================
# 22. EXEMPLOS
# ============================================================

def _cp_examples(
    records,
    limit=5
):

       result = []

       seen = set()

       for record in reversed(
           records
       ):

              text = str(

                     record.get(
                         "text"
                     )

                     or

                     ""

              ).strip()


              if not text:

                     continue


              normalized = (
                  text.lower()
              )


              if normalized in seen:

                     continue


              seen.add(
                  normalized
              )


              result.append(
                  text
              )


              if len(result) >= limit:

                     break

       result.reverse()


       return result


# ============================================================
# 23. PODE REEMITIR ATIVIDADE DO TOPICO?
# ============================================================

def _cp_can_emit_topic(
    topic_key,
    count,
    unique_users,
    now=None
):

       if now is None:

              now = time.time()


       previous = (
           coach_product_last_topic_emitted.get(
               topic_key
           )
       )


       if previous is None:

              return True


       elapsed = (

              now

              -

              previous.get(
                  "time",
                  0
              )


       )
       if (

              elapsed

              >=

              COACH_PRODUCT_TOPIC_FORCE_REEMIT_SECONDS

       ):

              return True


       if (

              elapsed

              >=

              COACH_PRODUCT_TOPIC_REEMIT_SECONDS

              and

              (
                     count
                     >
                     previous.get(
                         "count",
                         0
                     )

                     or

                     unique_users
                     >
                     previous.get(
                         "unique_users",
                         0
                     )
              )

       ):

              return True


       # Crescimento forte pode reemitir antes
       # do tempo minimo.

       if (

              count

              >=

              previous.get(
                  "count",
                  0
              )
              +
              2

       ):

              return True


       return False


# ============================================================
# 24. EMITIR ANALISE INDIVIDUAL
# ============================================================

def _cp_emit_individual_analysis(
    envelope,
    topics
):

       payload = (

              envelope.get(
                  "payload"
              )

              or

              {}

       )


       classification = (
           _cp_classification_from_envelope(
               envelope
           )
       )


       comment = (
           _cp_comment_from_envelope(
               envelope
           )
       )


       response_needed = bool(

              envelope.get(
                  "requires_response"
              )

              or

              classification.get(
                  "requires_response"
              )

       )


       has_question = bool(
           classification.get(
               "is_question"
           )
       )


       has_request = bool(
           classification.get(
               "is_request"
           )
       )


       has_demo_request = any(


              topic.get(
                  "category"
              )
              ==
              "demo_request"

              for topic in topics

       )


       product_question_topics = [

              topic

              for topic in topics

              if (
                     topic.get(
                         "category"
                     )
                     ==
                     "product_question"
              )

       ]


       output = {

              "output_id":
                  uuid.uuid4().hex,

              "message_type":
                  "product_analysis",

              "analysis_type":
                  "individual",

              "source_coach":
                  "product",

              "coach_version":
                  COACH_PRODUCT_VERSION,

              "platform":
                  envelope.get(
                      "platform"
                  ),

              "live_id":
                  envelope.get(
                      "live_id"
                  ),

              "subject":
                  envelope.get(
                      "subject"
                  ),

              "timestamp":
                  envelope.get(
                      "timestamp",
                      time.time()
                  ),

              "iso_time":
                  envelope.get(
                      "iso_time"
                  )
                  or
                  _cp_now_iso(),

              "source_dispatch_id":
                  envelope.get(
                      "dispatch_id"
                  ),

              "source_output_id":
                  envelope.get(
                      "source_output_id"
                  ),

              "source_message_type":
                  envelope.get(
                      "source_message_type"
                  ),

              "response_needed":
                  response_needed,

              "is_question":

                  has_question,

              "is_request":
                  has_request,

              "has_product_question":
                  bool(
                      product_question_topics
                  ),

              "has_demo_request":
                  has_demo_request,

              "topics":
                  copy.deepcopy(
                      topics
                  ),

              "comment":
                  copy.deepcopy(
                      comment
                  ),

              # Descreve O QUE precisa ser esclarecido.
              # Nao fornece a resposta.

              "information_needs": [

                     topic.get(
                         "answer_target"
                     )

                     for topic in topics

                     if topic.get(
                         "answer_target"
                     )

              ],

       }


       if _cp_queue_output(
           output
       ):


              coach_product_individual_history.append(
                  copy.deepcopy(
                      output
                  )
              )


              coach_product_stats[
                  "individual_outputs"
              ] += 1


       return output


# ============================================================
# 25. REGISTRAR OCORRENCIA LOCAL DO TOPICO
# ============================================================

def _cp_register_local_topic(
    envelope,
    topic
):

       comment = (
           _cp_comment_from_envelope(
               envelope
           )
       )


       timestamp = (
           envelope.get(
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

              "dispatch_id":
                  envelope.get(
                      "dispatch_id"
                  ),

              "source_output_id":
                  envelope.get(
                      "source_output_id"
                  ),

              "user":
                  comment.get(
                      "user"
                  ),

              "text":
                  comment.get(
                      "text"
                  ),

       }


       topic_key = (
           _cp_topic_key(
               topic
           )
       )


       queue = (
           coach_product_topic_windows[
               topic_key
           ]
       )

       queue.append(
           record
       )


       _cp_prune_topic_window(
           queue
       )


       coach_product_topic_counts[
           topic.get(
               "subtype"
           )
       ] += 1


       coach_product_family_counts[
           topic.get(
               "family"
           )
       ] += 1


       return list(
           queue
       )


# ============================================================
# 26. EMITIR ATIVIDADE LOCAL
# ============================================================

def _cp_maybe_emit_local_topic_signal(
    envelope,
    topic,
    records
):

       if not records:

              return None


       count = len(
           records

       )


       if (

              count

              <

              COACH_PRODUCT_LOCAL_TOPIC_THRESHOLD

       ):

              return None


       unique_users = (
           _cp_unique_users(
               records
           )
       )


       max_same_user = (
           _cp_max_repeat_by_one_user(
               records
           )
       )


       topic_key = (
           _cp_topic_key(
               topic
           )
       )


       now = time.time()


       if not _cp_can_emit_topic(

              topic_key,

              count,

              unique_users,


              now,

       ):

              return None


       repeated_by_same_user = (

              max_same_user
              >=
              2

       )


       multiple_users = (

              unique_users
              >=
              2

       )


       output = {

              "output_id":
                  uuid.uuid4().hex,

              "message_type":
                  "product_topic_signal",

              "analysis_type":
                  "local_activity",

              "source_coach":
                  "product",

              "coach_version":
                  COACH_PRODUCT_VERSION,

              "platform":
                  envelope.get(
                      "platform"

                     ),

              "live_id":
                  envelope.get(
                      "live_id"
                  ),

              "subject":
                  envelope.get(
                      "subject"
                  ),

              "timestamp":
                  now,

              "iso_time":
                  _cp_now_iso(),

              "topic":
                  copy.deepcopy(
                       topic
                  ),

              "topic_key":
                  topic_key,

              "evidence": {

                     "source":
                         "coach_product_local_window",

                     "window_seconds":
                         COACH_PRODUCT_TOPIC_WINDOW_SECONDS,

                     "count":
                         count,

                     "unique_users":
                         unique_users,

                     "max_repetitions_by_one_user":
                         max_same_user,

                     "repeated_by_same_user":
                         repeated_by_same_user,

                     "multiple_users":
                         multiple_users,

                     "examples":
                         _cp_examples(
                             records
                         ),

                     "source_output_ids": [

                            record.get(
                                "source_output_id"
                            )

                            for record in records

                            if record.get(
                                "source_output_id"
                            )

                     ],

              },

              "response_needed":
                  (
                      topic.get(
                          "category"
                      )
                      ==
                      "product_question"

                            or

                            topic.get(
                                "category"
                            )
                            ==
                            "demo_request"
                     ),

       }


       if _cp_queue_output(
           output
       ):


              coach_product_topic_signal_history.append(
                  copy.deepcopy(
                      output
                  )
              )


              coach_product_stats[
                  "local_topic_signals"
              ] += 1


              coach_product_last_topic_emitted[
                  topic_key
              ] = {

                     "time":
                         now,

                     "count":
                         count,

                     "unique_users":
                         unique_users,

              }


       return output


# ============================================================
# 27. TRANSFORMAR TENDENCIA DO WORKER
#
# Importante:
#
# Essa tendencia ja foi calculada pelo
# Worker Coach Comentarios.
#
# Portanto NAO adicionamos esses numeros novamente
# na janela local.
#
# Apenas transformamos em um sinal especializado
# de Produto.
# ============================================================


def _cp_emit_worker_trend(
    envelope,
    topics
):

       payload = (

              envelope.get(
                  "payload"
              )

              or

              {}

       )


       count = (
           payload.get(
               "count"
           )
       )


       unique_users = (
           payload.get(
               "unique_users"
           )
       )


       output = {

              "output_id":
                  uuid.uuid4().hex,

              "message_type":
                  "product_topic_signal",

              "analysis_type":
                  "worker_trend",

              "source_coach":
                  "product",

              "coach_version":
                  COACH_PRODUCT_VERSION,

              "platform":
                  envelope.get(
                      "platform"
                  ),

              "live_id":
                  envelope.get(
                      "live_id"
                  ),

              "subject":
                  envelope.get(
                      "subject"
                  ),

              "timestamp":
                  envelope.get(
                      "timestamp",
                      time.time()
                  ),

              "iso_time":
                  envelope.get(
                      "iso_time"
                  )
                  or
                  _cp_now_iso(),

              "source_dispatch_id":
                  envelope.get(
                      "dispatch_id"
                  ),

              "source_output_id":
                  envelope.get(
                      "source_output_id"
                  ),

              "trend_kind":
                  payload.get(
                      "trend_kind"
                  ),

              "topics":
                  copy.deepcopy(
                      topics
                  ),

              "response_needed":
                  bool(
                      envelope.get(
                          "requires_response"
                      )
                  ),

              "evidence": {

                     "source":
                         "worker_coach_comments",

                     "window_seconds":
                         payload.get(
                             "window_seconds"
                         ),

                     "count":
                         count,

                     "unique_users":
                         unique_users,

                     "strength":
                         payload.get(
                             "strength"
                         ),

                     "examples":
                         copy.deepcopy(

                                  payload.get(
                                      "examples"
                                  )

                                  or

                                  []

                            ),

              },

       }


       if _cp_queue_output(
           output
       ):

              coach_product_topic_signal_history.append(
                  copy.deepcopy(
                      output
                  )
              )


              coach_product_stats[
                  "worker_trend_signals"
              ] += 1


       return output


# ============================================================
# 28. PREVIEW PURO
#
# Nao altera estado.
# Util para teste.
# ============================================================

def coach_product_preview_envelope(
    envelope
):

       topics = (
           _cp_topics_from_envelope(
               envelope
           )
       )


       payload = (

              envelope.get(
                  "payload"

              )

              or

              {}

       )


       source_message_type = (
           envelope.get(
               "source_message_type"
           )
       )


       result = {

              "destination":
                  envelope.get(
                      "destination"
                  ),

              "source_message_type":
                  source_message_type,

              "topics":
                  topics,

              "requires_response":
                  envelope.get(
                      "requires_response"
                  ),

       }


       if (
              source_message_type
              ==
              "classified_comment"
       ):

              classification = (

                     payload.get(
                         "classification"

                     )

                     or

                     {}

              )


              result[
                  "comment"
              ] = (
                  _cp_comment_from_envelope(
                      envelope
                  )
              )


              result[
                  "is_question"
              ] = (
                  classification.get(
                      "is_question"
                  )
              )


              result[
                  "is_request"
              ] = (
                  classification.get(
                      "is_request"
                  )
              )


       elif (
           source_message_type
           ==
           "comment_trend"
       ):

              result[
                  "trend"
              ] = {

                     "trend_kind":
                         payload.get(
                             "trend_kind"
                         ),

                     "count":
                         payload.get(
                              "count"
                         ),

                     "unique_users":
                         payload.get(
                             "unique_users"
                         ),

                     "strength":
                         payload.get(
                             "strength"
                         ),

              }


       return result


# ============================================================
# 29. PROCESSAR ENVELOPE
# ============================================================

def coach_product_process_envelope(
    envelope
):

       global coach_product_platform

       global coach_product_live_id

       global coach_product_subject


       if not isinstance(
           envelope,
           dict
       ):

              return []


       # ========================================================
       # PROTECAO DE DESTINO
       # ========================================================

       if (
              envelope.get(
                  "destination"
              )
              !=
              "product"
       ):

              coach_product_stats[
                  "wrong_destination"
              ] += 1


              return []


       # ========================================================
       # DEDUP
       # ========================================================

       if _cp_seen(
           envelope
       ):

              return []


       coach_product_stats[
           "received"
       ] += 1


       # ========================================================
       # IDENTIDADE DA LIVE
       # ========================================================

       coach_product_platform = (

              envelope.get(
                  "platform"

              )

              or

              coach_product_platform

       )


       coach_product_live_id = (

              envelope.get(
                  "live_id"
              )

              or

              coach_product_live_id

       )


       coach_product_subject = (

              envelope.get(
                  "subject"
              )

              or

              coach_product_subject

       )


       # ========================================================
       # TOPICOS
       # ========================================================

       topics = (
           _cp_topics_from_envelope(
               envelope
           )
       )

       if not topics:

              coach_product_stats[
                  "without_product_topic"
              ] += 1


              return []


       source_message_type = (
           envelope.get(
               "source_message_type"
           )
       )


       generated = []


       # ========================================================
       # COMENTARIO INDIVIDUAL
       # ========================================================

       if (
              source_message_type
              ==
              "classified_comment"
       ):

              coach_product_stats[
                  "individual_received"
              ] += 1


              individual_output = (
                  _cp_emit_individual_analysis(

                            envelope,

                            topics

                     )
              )


              if individual_output is not None:


                     generated.append(
                         individual_output
                     )


              # Atualizar janelas por topico.

              for topic in topics:

                     records = (
                         _cp_register_local_topic(

                                  envelope,

                                  topic

                            )
                     )


                     local_signal = (
                         _cp_maybe_emit_local_topic_signal(

                                  envelope,

                                  topic,

                                  records

                            )
                     )


                     if local_signal is not None:

                            generated.append(
                                local_signal
                            )


       # ========================================================
       # TENDENCIA VINDO DO WORKER
       # ========================================================

       elif (

           source_message_type
           ==
           "comment_trend"
       ):

              coach_product_stats[
                  "trend_received"
              ] += 1


              trend_output = (
                  _cp_emit_worker_trend(

                            envelope,

                            topics

                     )
              )


              if trend_output is not None:

                     generated.append(
                         trend_output
                     )


       else:

              coach_product_stats[
                  "unsupported_source_type"
              ] += 1


       return generated


# ============================================================
# 30. RESET PARA NOVA LIVE
# ============================================================

def coach_product_reset(
    platform
):

       global coach_product_output_queue

       global coach_product_running

       global coach_product_platform

       global coach_product_live_id

       global coach_product_subject

       global coach_product_started_at

       global coach_product_last_error


       coach_product_output_queue = (
           asyncio.Queue(

                     maxsize=
                         COACH_PRODUCT_OUTPUT_QUEUE_LIMIT

              )
       )


       coach_product_output_history.clear()

       coach_product_individual_history.clear()

       coach_product_topic_signal_history.clear()

       coach_product_topic_windows.clear()

       coach_product_last_topic_emitted.clear()

       coach_product_seen_dispatch_ids.clear()

       coach_product_seen_dispatch_order.clear()

       coach_product_stats.clear()

       coach_product_topic_counts.clear()

       coach_product_family_counts.clear()


       coach_product_running = False


       coach_product_platform = (
           platform
       )

       coach_product_live_id = None

       coach_product_subject = None

       coach_product_started_at = (
           time.time()
       )

       coach_product_last_error = None


# ============================================================
# 31. LOOP PRINCIPAL
# ============================================================

async def coach_product_loop():

       global coach_product_running

       global coach_product_last_error


       coach_product_running = True


       try:

              while True:

                     envelope = None


                     try:

                            envelope = await (

                                  comment_dispatcher_next_product(
                                      timeout=0.5
                                  )

                            )


                     except asyncio.TimeoutError:

                            envelope = None


                     except asyncio.CancelledError:

                            raise


                     except Exception as e:

                            coach_product_last_error = (

                                   f"{type(e).__name__}: "
                                   f"{e}"

                            )


                            await asyncio.sleep(
                                0.1
                            )


                     # =================================================
                     # PROCESSAR
                     # =================================================

                     if envelope is not None:

                            try:

                                   coach_product_process_envelope(
                                       envelope
                                   )


                            except Exception as e:

                                   coach_product_last_error = (

                                         f"{type(e).__name__}: "
                                         f"{e}"

                                  )


                            continue


                     # =================================================
                     # ENCERRAMENTO
                     #
                     # O Coach so para depois que:
                     #
                     # Live Engine parou
                     # Dispatcher parou
                     # fila Product ficou vazia
                     # =================================================

                     dispatcher_running = bool(

                            globals().get(

                                  "comment_dispatcher_running",

                                  False

                            )

                     )


                     source_queue = globals().get(

                            "comment_dispatcher_product_queue"

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

              coach_product_running = False


# ============================================================
# 32. SUPERVISOR
# ============================================================

async def coach_product_supervisor(
    platform
):

       global coach_product_last_error


       start = time.time()


       while True:

              if (

                     time.time()
                     -
                     start

                     >

                     30

              ):

                     coach_product_last_error = (

                            "Comment Dispatcher nao ficou "
                            "pronto dentro de 30 segundos."

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
                                "comment_dispatcher_product_queue"
                            )
                            is not None

                            and

                            bool(

                                  globals().get(

                                         "comment_dispatcher_running",

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


       await coach_product_loop()


# ============================================================
# 33. API PARA O FUTURO CONTEXT FUSION
# ============================================================

async def coach_product_next_output(
    timeout=None
):

       queue = (
           coach_product_output_queue
       )


       if queue is None:

              raise RuntimeError(

                     "Coach Produto ainda nao iniciou uma LIVE."

              )


       if timeout is None:

              return await queue.get()


       return await asyncio.wait_for(

              queue.get(),

              timeout=
                  timeout

       )


# Alias semanticamente conveniente.

async def coach_product_next_signal(
    timeout=None
):

       return await (

              coach_product_next_output(
                  timeout=timeout
              )

       )


# ============================================================
# 34. OUTPUTS RECENTES
# ============================================================

def coach_product_recent_outputs(
    limit=20
):

       if limit <= 0:

              return []


       return list(
           coach_product_output_history
       )[-limit:]

def coach_product_recent_individual(
    limit=20
):

       if limit <= 0:

              return []


       return list(
           coach_product_individual_history
       )[-limit:]


def coach_product_recent_topic_signals(
    limit=20
):

       if limit <= 0:

              return []


       return list(
           coach_product_topic_signal_history
       )[-limit:]


# ============================================================
# 35. TOPICOS ATIVOS
# ============================================================

def coach_product_active_topics():

       now = time.time()

       result = []


       for topic_key, queue in (

              coach_product_topic_windows.items()

       ):

              _cp_prune_topic_window(
                  queue,

                     now
              )


              if not queue:

                     continue


              category, subtype = (

                     topic_key.split(
                         ":",
                         1
                     )

              )


              topic = (
                  _cp_topic_info(
                      category,
                      subtype
                  )
              )


              records = list(
                  queue
              )


              result.append({

                     "topic_key":
                         topic_key,

                     "topic":
                         topic,

                     "count":
                         len(
                                  records
                            ),

                     "unique_users":

                         _cp_unique_users(
                             records
                         ),

                     "max_repetitions_by_one_user":
                         _cp_max_repeat_by_one_user(
                             records
                         ),

                     "examples":
                         _cp_examples(
                             records,
                             limit=3
                         ),

              })


       result.sort(

              key=lambda item: (

                     item.get(
                         "count",
                         0
                     ),

                     item.get(
                         "unique_users",
                         0
                     ),

              ),

              reverse=True

       )


       return result


# ============================================================
# 36. DIAGNOSTICO PRINCIPAL
# ============================================================

def mostrar_coach_produto():

       print(
           "=" * 76
       )

       print(
           "AGCN - COACH PRODUTO V1.1"
       )

       print(
           "Versao:",
           COACH_PRODUCT_VERSION
       )

       print(
           "=" * 76
       )


       print(
           "Rodando:",
           coach_product_running
       )


       print(
           "Plataforma:",
           coach_product_platform
       )


       print(
           "Live ID:",
           coach_product_live_id
       )


       print(
           "Subject:",
           coach_product_subject
       )


       print()


       print(
           "ENTRADA"
       )

       print(
           " Recebidos do Dispatcher:",
           coach_product_stats.get(
               "received",
               0
           )
       )


       print(
           " Comentarios individuais:",
           coach_product_stats.get(
               "individual_received",
               0
           )
       )


       print(
           " Tendencias recebidas:",
           coach_product_stats.get(
               "trend_received",
               0
           )
       )


       print(
           " Duplicados ignorados:",
           coach_product_stats.get(
               "duplicates",
               0
           )
       )


       print(
           " Sem topico de produto:",
           coach_product_stats.get(
               "without_product_topic",
               0
           )
       )


       print()


       print(
           "SAIDA PARA FUTURO CONTEXT FUSION"
       )


       print(
           " Outputs enviados:",
           coach_product_stats.get(
               "outputs_sent",
               0
           )
       )


       print(
           " Analises individuais:",
           coach_product_stats.get(
               "individual_outputs",
               0
           )
       )


       print(
           " Sinais locais de topico:",
           coach_product_stats.get(
               "local_topic_signals",
               0
           )
       )


       print(
           " Tendencias do Worker transformadas:",
           coach_product_stats.get(
               "worker_trend_signals",
               0
           )
       )


       print(
           " Outputs descartados por fila cheia:",
           coach_product_stats.get(
               "dropped_outputs",

                     0
              )
       )


       if coach_product_output_queue is None:

              queue_size = None

       else:

              queue_size = (
                  coach_product_output_queue.qsize()
              )


       print(
           " Aguardando futuro Context Fusion:",
           queue_size
       )


       print()


       print(
           "Erro:",
           coach_product_last_error
       )


       # ========================================================
       # TOPICOS
       # ========================================================

       print()


       print(
           "TOPICOS ATIVOS "
           f"(ultimos {COACH_PRODUCT_TOPIC_WINDOW_SECONDS}s):"
       )


       active = (
           coach_product_active_topics()

       )


       if not active:

              print(
                  " nenhum"
              )


       else:

              for item in active[:10]:

                     topic = (
                         item.get(
                             "topic"
                         )
                         or
                         {}
                     )


                     print()


                     print(

                            " ",

                            topic.get(
                                "label"
                            ),

                            "|",

                            topic.get(
                                "subtype"
                            ),

                     )


                     print(

                            "       comentarios:",


                            item.get(
                                "count"
                            ),

                            "| usuarios:",

                            item.get(
                                "unique_users"
                            ),

                            "| max mesmo usuario:",

                            item.get(
                                "max_repetitions_by_one_user"
                            ),

                     )


                     print(

                            "       exemplos:",

                            item.get(
                                "examples"
                            )

                     )


       # ========================================================
       # ULTIMOS OUTPUTS
       # ========================================================

       print()


       print(
           "ULTIMOS OUTPUTS:"
       )


       outputs = list(
           coach_product_output_history
       )[-10:]

       if not outputs:

              print(
                  " nenhum ainda"
              )


       else:

              for output in outputs:

                     print()


                     print(

                            " ",

                            output.get(
                                "message_type"
                            ),

                            "|",

                            output.get(
                                "analysis_type"
                            ),

                     )


                     if (
                            output.get(
                                "message_type"
                            )
                            ==
                            "product_analysis"
                     ):

                            comment = (
                                output.get(
                                    "comment"
                                )
                                or
                                {}

                            )


                            print(

                                  "        ",

                                  comment.get(
                                      "user"
                                  ),

                                  ":",

                                  comment.get(
                                      "text"
                                  )

                            )


                            print(

                                  "        topicos:",

                                  [

                                         topic.get(
                                             "subtype"
                                         )

                                         for topic
                                         in output.get(
                                             "topics",
                                             []
                                         )

                                  ]

                            )


                            print(

                                  "        precisa resposta:",

                                  output.get(

                                         "response_needed"
                                  )

                            )


                     else:

                            evidence = (

                                  output.get(
                                      "evidence"
                                  )

                                  or

                                  {}

                            )


                            if output.get(
                                "topic"
                            ):

                                  topic_names = [

                                         output.get(
                                             "topic",
                                             {}
                                         ).get(
                                             "subtype"
                                         )

                                  ]


                            else:

                                  topic_names = [

                                         topic.get(
                                             "subtype"
                                         )

                                         for topic
                                         in output.get(

                                             "topics",
                                             []
                                         )

                                  ]


                            print(
                                "    topicos:",
                                topic_names
                            )


                            print(

                                  "        count:",

                                  evidence.get(
                                      "count"
                                  ),

                                  "| usuarios:",

                                  evidence.get(
                                      "unique_users"
                                  ),

                            )


# ============================================================
# 37. DIAGNOSTICO DETALHADO DAS ANALISES
# ============================================================

def mostrar_analises_coach_produto(
    limit=20
):

       print(
           "=" * 76
       )

       print(
           "ANALISES RECENTES - COACH PRODUTO V1.1"
       )

       print(
           "=" * 76
       )


       outputs = list(
           coach_product_output_history
       )[-limit:]


       if not outputs:

              print(
                  "Nenhuma analise ainda."
              )


              return


       for output in outputs:

              print()


              print(
                  "Tipo:",
                  output.get(
                      "message_type"
                  )
              )


              print(
                  "Analise:",
                  output.get(
                      "analysis_type"
                  )
              )


              if (
                     output.get(
                         "message_type"
                     )
                     ==
                     "product_analysis"

              ):

                     comment = (

                            output.get(
                                "comment"
                            )

                            or

                            {}

                     )


                     print(
                         "Usuario:",
                         comment.get(
                             "user"
                         )
                     )


                     print(
                         "Comentario:",
                         comment.get(
                             "text"
                         )
                     )


                     print(
                         "Pergunta:",
                         output.get(
                             "is_question"
                         )
                     )


                     print(
                         "Pedido:",
                         output.get(
                             "is_request"
                         )
                     )

                     print(
                         "Precisa resposta:",
                         output.get(
                             "response_needed"
                         )
                     )


                     print(
                         "Topicos:"
                     )


                     for topic in (
                         output.get(
                             "topics"
                         )
                         or
                         []
                     ):

                            print(

                                  "    -",

                                  topic.get(
                                      "label"
                                  ),

                                  "|",

                                  topic.get(
                                      "subtype"
                                  )

                            )


                            print(

                                  "        necessidade:",

                                  topic.get(
                                      "answer_target"
                                  )


                            )


              else:

                     evidence = (

                            output.get(
                                "evidence"
                            )

                            or

                            {}

                     )


                     topics = (
                         output.get(
                             "topics"
                         )
                     )


                     if topics is None:

                            single_topic = (
                                output.get(
                                    "topic"
                                )
                            )


                            topics = (

                                  [single_topic]

                                  if single_topic

                                  else

                                  []

                            )

                     print(
                         "Topicos:"
                     )


                     for topic in topics:

                            print(

                                  "    -",

                                  topic.get(
                                      "label"
                                  ),

                                  "|",

                                  topic.get(
                                      "subtype"
                                  )

                            )


                     print(
                         "Fonte:",
                         evidence.get(
                             "source"
                         )
                     )


                     print(
                         "Comentarios:",
                         evidence.get(
                             "count"
                         )
                     )


                     print(
                         "Usuarios:",
                         evidence.get(
                             "unique_users"
                         )

                     )


                     if (

                            evidence.get(
                                "max_repetitions_by_one_user"
                            )

                            is not None

                     ):

                            print(

                                  "Max repeticoes do mesmo usuario:",

                                  evidence.get(
                                      "max_repetitions_by_one_user"
                                  )

                            )


                     print(
                         "Exemplos:",
                         evidence.get(
                             "examples"
                         )
                     )


# ============================================================
# 38. TESTE INTERNO
#
# Nao altera filas nem estado da LIVE.
# ============================================================

def testar_coach_produto_v11():
    """
    Testa o contrato semantico entre Worker Comentarios V1.5,
    Comment Dispatcher V1 e Coach Produto V1.1.

    Nao altera filas nem o estado da LIVE.
    """

    exemplos = {
        "functionality": "pra que serve isso?",
        "usage_method": "como usa esse produto?",
        "compatibility": "funciona no iphone?",
        "size_fit": "o tamanho M veste 40?",
        "dimensions": "qual a largura?",
        "color_variant": "tem rosa?",
        "material_composition": "qual o material?",
        "capacity": "quantos litros cabe?",
        "weight": "qual o peso?",
        "quantity_package": "kit vem com quantos?",
        "included_items": "vem carregador?",
        "warranty": "quanto tempo de garantia?",
        "brand_manufacturer": "esse colchao e da ortobom?",
        "authenticity": "e original?",
        "product_condition": "vem lacrado?",
        "certification_compliance": "tem selo do inmetro?",
        "battery_duration": "quanto dura a bateria?",
        "connectivity": "tem bluetooth?",
        "technical_specification": "quanto de ram?",
        "power_voltage": "e bivolt?",
        "usage_resistance": "pode ir na lava-loucas?",
        "sensory_characteristic": "qual o cheiro?",
        "ingredients_composition": "quais os ingredientes?",
        "expiration_validity": "qual a validade?",
        "safety_suitability": "serve para pele sensivel?",
        "apparel_characteristic": "esse tecido estica?",
        "food_characteristic": "como prepara?",
        "comparison": "qual a diferenca desse pro outro?",
    }

    def envelope_classified(intents, text="teste", user="Teste"):
        return {
            "dispatch_id": uuid.uuid4().hex,
            "destination": "product",
            "source_message_type": "classified_comment",
            "requires_response": True,
            "platform": "test",
            "live_id": "test-live",
            "subject": "@teste",
            "matched_intents": intents,
            "payload": {
                "comment": {
                    "user": user,
                    "text": text,
                },
                "classification": {
                    "is_question": True,
                    "is_request": any(
                        i.get("category") == "demo_request"
                        for i in intents
                    ),
                    "requires_response": True,
                },
            },
        }

    def product_intent(subtype):
        return {
            "category": "product_question",
            "subtype": subtype,
            "requires_response": True,
        }

    falhas = []
    aprovados = 0

    print("=" * 76)
    print("TESTE INTERNO - COACH PRODUTO V1.1")
    print("=" * 76)

    # 1) Contrato completo com os 28 subtipos do Worker V1.5.
    taxonomy = coach_product_validate_taxonomy()
    if taxonomy["ok"]:
        aprovados += 1
    else:
        falhas.append({
            "case": "contrato de taxonomia Worker V1.5",
            "missing": taxonomy["missing"],
        })

    # 2) Cada subtipo precisa ser preservado e ter mapeamento especializado.
    for subtype in sorted(COACH_PRODUCT_WORKER_V15_SUBTYPES):
        preview = coach_product_preview_envelope(
            envelope_classified(
                [product_intent(subtype)],
                exemplos[subtype],
            )
        )
        topics = preview.get("topics") or []
        topic = topics[0] if topics else None

        ok = bool(
            topic
            and topic.get("subtype") == subtype
            and topic.get("family")
            and topic.get("label")
            and topic.get("answer_target")
        )

        if ok:
            aprovados += 1
        else:
            falhas.append({
                "case": f"subtipo {subtype}",
                "preview": preview,
            })

    # 3) Demonstracao.
    demo = coach_product_preview_envelope(
        envelope_classified(
            [{
                "category": "demo_request",
                "subtype": "show_demonstrate",
                "requires_response": True,
            }],
            "mostra por dentro",
        )
    )
    if [t.get("subtype") for t in demo.get("topics", [])] == ["show_demonstrate"]:
        aprovados += 1
    else:
        falhas.append({"case": "demonstracao", "preview": demo})

    # 4) Multi-topico: produto + demonstracao.
    multi = coach_product_preview_envelope(
        envelope_classified(
            [
                product_intent("color_variant"),
                {
                    "category": "demo_request",
                    "subtype": "show_demonstrate",
                    "requires_response": True,
                },
            ],
            "tem rosa? mostra pra gente",
        )
    )
    multi_topics = {t.get("subtype") for t in multi.get("topics", [])}
    if multi_topics == {"color_variant", "show_demonstrate"}:
        aprovados += 1
    else:
        falhas.append({"case": "multi-topico", "preview": multi})

    # 5) Fallback generico continua preservado para perguntas futuras/desconhecidas.
    generic = coach_product_preview_envelope(
        envelope_classified(
            [{
                "category": "product_question",
                "subtype": None,
                "requires_response": True,
            }],
            "como e esse produto?",
        )
    )
    if [t.get("subtype") for t in generic.get("topics", [])] == ["generic_characteristic"]:
        aprovados += 1
    else:
        falhas.append({"case": "fallback generico", "preview": generic})

    # 6) Tendencia do Worker precisa manter o topico especializado.
    trend_envelope = {
        "dispatch_id": uuid.uuid4().hex,
        "destination": "product",
        "source_message_type": "comment_trend",
        "requires_response": True,
        "platform": "test",
        "live_id": "test-live",
        "subject": "@teste",
        "matched_intents": [product_intent("battery_duration")],
        "payload": {
            "trend_kind": "topic_repeat",
            "count": 3,
            "unique_users": 2,
            "strength": 0.85,
            "window_seconds": 60,
            "examples": [
                "quanto dura a bateria?",
                "a bateria dura quanto?",
            ],
        },
    }
    trend = coach_product_preview_envelope(trend_envelope)
    if (
        trend.get("source_message_type") == "comment_trend"
        and [t.get("subtype") for t in trend.get("topics", [])]
        == ["battery_duration"]
        and (trend.get("trend") or {}).get("count") == 3
    ):
        aprovados += 1
    else:
        falhas.append({"case": "tendencia", "preview": trend})

    total = aprovados + len(falhas)

    print()
    print("=" * 76)
    print("RESUMO DO TESTE COACH PRODUTO V1.1")
    print("=" * 76)
    print("Subtipos Worker V1.5 mapeados:", taxonomy["mapped_product_subtypes"], "/", taxonomy["expected_product_subtypes"])
    print("Aprovados:", aprovados)
    print("Falhas:", len(falhas))
    print("Total:", total)

    if falhas:
        print()
        print("FALHAS:")
        for falha in falhas:
            print(" -", falha)

    resultado = {
        "approved": aprovados,
        "failed": len(falhas),
        "total": total,
        "ok": not falhas,
        "taxonomy": taxonomy,
        "failures": falhas,
    }
    print(resultado)
    return resultado


# Compatibilidade com a celula de teste antiga do notebook.
def testar_coach_produto_v1():
    return testar_coach_produto_v11()


# ============================================================
# 39. FINALIZAR TASK
# ============================================================

async def _coach_product_finish_task(
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
# 40. DESCOBRIR BASE
#
# As funcoes atuais ja contem:
#
# Live Engine
# + Coach Armazenamento
# + Worker Comentarios
# + Comment Dispatcher
#
# Agora adicionamos:
#

# + Coach Produto
#
#
# Se esta propria celula for reexecutada,
# nao empilhamos dois Coaches Produto.
# ============================================================


# ------------------------------------------------------------
# SHOPEE
# ------------------------------------------------------------

_current_product_shopee_executor = (
    executar_shopee_com_live_engine
)


if getattr(

       _current_product_shopee_executor,

       "_agcn_product_coach_wrapper",

       False

):

       _coach_product_base_shopee = (

              _current_product_shopee_executor
              ._agcn_product_coach_base

       )


else:

       _coach_product_base_shopee = (
           _current_product_shopee_executor
       )


# ------------------------------------------------------------
# TIKTOK
# ------------------------------------------------------------

_current_product_tiktok_executor = (
    executar_tiktok_com_live_engine
)


if getattr(

       _current_product_tiktok_executor,

       "_agcn_product_coach_wrapper",

       False

):

       _coach_product_base_tiktok = (

              _current_product_tiktok_executor
              ._agcn_product_coach_base

       )


else:

       _coach_product_base_tiktok = (
           _current_product_tiktok_executor
       )


# ============================================================
# 41. SHOPEE
# ============================================================

async def executar_shopee_com_live_engine():

       coach_product_reset(
           "shopee"
       )


       product_task = (
           asyncio.create_task(

                     coach_product_supervisor(
                         "shopee"
                     )


              )
       )


       try:

              return await (
                  _coach_product_base_shopee()
              )


       finally:

              await _coach_product_finish_task(
                  product_task
              )


executar_shopee_com_live_engine._agcn_product_coach_wrapper = True


executar_shopee_com_live_engine._agcn_product_coach_base = (
    _coach_product_base_shopee
)
executar_shopee_com_live_engine._agcn_product_coach_version = (
    COACH_PRODUCT_VERSION
)


# ============================================================
# 42. TIKTOK
# ============================================================

async def executar_tiktok_com_live_engine(
    username
):

       coach_product_reset(
           "tiktok"
       )


       product_task = (
           asyncio.create_task(

                     coach_product_supervisor(
                         "tiktok"
                     )


              )
       )


       try:

              return await (

                     _coach_product_base_tiktok(
                         username
                     )

              )


       finally:

              await _coach_product_finish_task(
                  product_task
              )


executar_tiktok_com_live_engine._agcn_product_coach_wrapper = True


executar_tiktok_com_live_engine._agcn_product_coach_base = (
    _coach_product_base_tiktok
)
executar_tiktok_com_live_engine._agcn_product_coach_version = (
    COACH_PRODUCT_VERSION
)


# ============================================================
# 43. PRONTO
# ============================================================

print(
    "AGCN Coach Produto V1.1 carregado."
)

print(
    "Entrada: Comment Dispatcher V1 -> fila product."
)

print(
    "Analisa taxonomia completa de Produto do Worker Comentarios V1.5:"
)


print(
    "1. perguntas e caracteristicas especializadas do produto"
)

print(
    "2. pedidos de demonstracao"
)

print(
    "3. repeticao do mesmo assunto"
)

print(
    "4. repeticao da mesma pessoa x varias pessoas"
)

print(
    "Saidas:"
)

print(
    "1. product_analysis"
)

print(
    "2. product_topic_signal"
)

print(
    "Destino futuro: Context Fusion."
)

print(
    "Sem prioridade, sem Storage e sem Interface."
)
