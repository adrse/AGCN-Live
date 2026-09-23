# ============================================================
# AGCN - WORKER COACH COMENTARIOS V1.5
#
# FUNCAO:
#
# Live Engine V2
#       ↓
# WORKER COACH COMENTARIOS
#       │
#       ├── recebe comentarios normalizados
#       ├── identifica relevancia
#       ├── identifica pergunta / pedido / afirmacao
#       ├── classifica intencoes em multiplas familias
#       ├── classifica perguntas sobre produto
#       ├── classifica perguntas comerciais
#       ├── identifica demonstracao / comparacao
#       ├── identifica compra / intencao / hesitacao
#       ├── identifica objecoes especializadas
#       ├── identifica pos-compra detalhado
#       ├── identifica observacoes comerciais
#       ├── preserva referencias contextuais de produto
#       ├── detecta repeticao / tendencias
#       │
#       ↓
# COMMENT DISPATCHER V1
#
# O WORKER NAO:
#
# - decide prioridade
# - decide quando mostrar
# - decide qual comentario interrompe o vendedor
# - escreve a fala final do Coach
# - envia diretamente para Interface
# - conversa com Armazenamento Coach
# - inventa dados de produto, estoque, frete, garantia ou oferta
# - atribui causalidade entre comentarios e metricas
#
# SAIDAS:
#
# 1. classified_comment
# 2. comment_trend
#
# NOVIDADES V1.5:
#
# - amplia a taxonomia para moda, beleza, casa/cozinha,
#   eletronicos, alimentos, acessorios, infantil/pet e utilidades
# - adiciona nota fiscal / documento fiscal
# - adiciona pagamento / parcelamento
# - adiciona autenticidade / condicao / certificacao
# - adiciona marca / fabricante
# - adiciona uso, ingredientes, validade, seguranca/adequacao
# - adiciona conectividade e especificacoes tecnicas
# - adiciona objecoes especializadas
# - detalha problemas de pos-compra
# - suporta multi-intencao de produto e comercial no mesmo comentario
# - preserva referencias contextuais curtas de produto
# - mantem a correcao V1.4 de functionality x size_fit x compatibility
# - preserva contratos publicos e formato de saida das versoes anteriores
# ============================================================

import asyncio
import time
import re
import uuid
import unicodedata

from collections import deque, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

# ============================================================
# 1. VERIFICAR LIVE ENGINE
# ============================================================

_WCC_REQUIRED = [
    "live_engine_next_comment_event",
    "live_engine_is_running",
    "live_engine_platform",
    "executar_shopee_com_live_engine",
    "executar_tiktok_com_live_engine",
]

_WCC_MISSING = [
    name
    for name in _WCC_REQUIRED
    if name not in globals()
]

if _WCC_MISSING:
    raise RuntimeError(
        "Execute primeiro o Live Engine V2. Faltando: "
        + ", ".join(_WCC_MISSING)
    )

# ============================================================
# 2. CONFIGURACAO
# ============================================================

WORKER_COACH_COMMENTS_VERSION = "1.5"
WORKER_COACH_COMMENTS_TIMEZONE = ZoneInfo("America/Araguaina")
WORKER_COACH_COMMENTS_WINDOW = 60
WORKER_COACH_COMMENTS_MAX_RECENT = 5000
WORKER_COACH_COMMENTS_MAX_OUTPUTS = 5000
WORKER_COACH_COMMENTS_OUTPUT_QUEUE_LIMIT = 5000
WORKER_COACH_COMMENTS_MIN_REEMIT_SECONDS = 12
WORKER_COACH_COMMENTS_FORCE_REEMIT_SECONDS = 35

# ============================================================
# 3. LIMITES DE TENDENCIA
# ============================================================

WCC_TOPIC_TREND_THRESHOLDS = {
    "product_question": 3,
    "commercial_question": 3,
    "demo_request": 2,
    "buying_intent": 2,
    "purchase_completed": 2,
    "objection": 2,
    "post_purchase_issue": 2,
}

WCC_CATEGORY_VOLUME_THRESHOLDS = {
    "product_question": 4,
    "commercial_question": 4,
    "objection": 3,
    "post_purchase_issue": 2,
}

# ============================================================
# 4. ESTADO
# ============================================================

worker_coach_comments_output_queue = None
worker_coach_comments_recent = deque(maxlen=WORKER_COACH_COMMENTS_MAX_RECENT)
worker_coach_comments_output_history = deque(maxlen=WORKER_COACH_COMMENTS_MAX_OUTPUTS)
worker_coach_comments_classified_history = deque(maxlen=WORKER_COACH_COMMENTS_MAX_OUTPUTS)
worker_coach_comments_trend_history = deque(maxlen=WORKER_COACH_COMMENTS_MAX_OUTPUTS)
worker_coach_comments_topic_windows = defaultdict(deque)
worker_coach_comments_category_windows = defaultdict(deque)
worker_coach_comments_last_trend_emitted = {}
worker_coach_comments_seen_event_ids = set()
worker_coach_comments_seen_event_order = deque(maxlen=20000)

worker_coach_comments_running = False
worker_coach_comments_platform = None
worker_coach_comments_live_id = None
worker_coach_comments_subject = None
worker_coach_comments_started_at = None

worker_coach_comments_comments_received = 0
worker_coach_comments_relevant_comments = 0
worker_coach_comments_neutral_comments = 0
worker_coach_comments_classified_sent = 0
worker_coach_comments_trends_emitted = 0
worker_coach_comments_dropped_outputs = 0
worker_coach_comments_last_error = None

# ============================================================
# 5. HORARIO
# ============================================================

def _wcc_now_iso():
    return datetime.now(WORKER_COACH_COMMENTS_TIMEZONE).isoformat()

# ============================================================
# 6. NORMALIZAR TEXTO
# ============================================================

def _wcc_normalize(text):
    text = str(text or "").lower()

    text = "".join(
        ch
        for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )

    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[^a-z0-9\s$]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text

# ============================================================
# 7. HELPERS DE TEXTO
# ============================================================

def _wcc_match(patterns, text):
    return any(re.search(pattern, text) for pattern in patterns)

def _wcc_exact_phrase(normalized, phrase):
    words = phrase.split()

    pattern = (
        r"(?<!\w)"
        + r"\s+".join(re.escape(word) for word in words)
        + r"(?!\w)"
    )

    return bool(re.search(pattern, normalized))

def _wcc_has_word(normalized, word):
    return bool(
        re.search(
            r"(?<!\w)" + re.escape(word) + r"(?!\w)",
            normalized
        )
    )

def _wcc_has_any_word(normalized, words):
    return any(
        _wcc_has_word(normalized, word)
        for word in words
    )

# ============================================================
# 8. VOCABULARIOS E REGRAS COMERCIAIS
# ============================================================

WCC_PRICE_PATTERNS = [
    r"\bpreco\b", r"\bpre\b", r"\bprc\b", r"\bvalor\b", r"\bvlr\b",
    r"\bquanto custa\b", r"\bcusta quanto\b", r"\bquanto e\b",
    r"\bquanto ta\b", r"\bquanto esta\b", r"\bquanto fica\b",
    r"\bqual o valor\b", r"\bqual valor\b", r"\bqual o preco\b",
    r"\bqual preco\b", r"\bpor quanto\b", r"\bquanto sai\b",
    r"\bqto\s+(custa|e|ta|esta|fica)\b",
    r"\bqnt\s+(custa|e|ta|esta|fica)\b",
    r"\bqnto\s+(custa|e|ta|esta|fica)\b", r"\br\$",
]

WCC_PROMOTION_TERMS = [
    r"\bcupom\b", r"\bcupomzinho\b", r"\bdesconto\b",
    r"\bpromocao\b", r"\bpromo\b", r"\boferta\b",
    r"\boferta relampago\b", r"\bcodigo de desconto\b",
    r"\bcombo\b",
]

WCC_PROMOTION_REQUEST_PATTERNS = [
    r"\btem cupom\b", r"\btem desconto\b", r"\btem promocao\b",
    r"\bqual cupom\b", r"\bqual o cupom\b", r"\bcade o cupom\b",
    r"\bda (?:um )?cupom\b", r"\blibera(?:r)? (?:um )?cupom\b",
    r"\bsolta (?:um )?cupom\b", r"\bmanda (?:o )?cupom\b",
    r"\bcoloca (?:a )?promocao\b", r"\bativa (?:a )?promocao\b",
    r"\bvai baixar mais\b", r"\btem desconto levando\b",
    r"\bqual codigo do cupom\b", r"\bcupom ainda funciona\b",
]

WCC_PROMOTION_PROBLEM_PATTERNS = [
    r"\bcupom nao (?:pega|funciona|aplica)\b",
    r"\bnao (?:ta|esta) aplicando (?:o )?desconto\b",
    r"\bnao aplicou (?:o )?desconto\b",
    r"\bpromocao sumiu\b", r"\boferta sumiu\b",
    r"\bpreco (?:mudou|aumentou) no carrinho\b",
    r"\bno carrinho (?:fica|aparece) outro valor\b",
    r"\bnao aparece frete gratis\b",
    r"\bcupom expirou\b",
]

WCC_SHIPPING_TERMS = [
    r"\bfrete\b", r"\benvio\b", r"\bentrega\b", r"\bcep\b",
    r"\benvia (?:pra|para|pro|pelo|pela)\b", r"\bmanda (?:pra|para|pro|pelo|pela)\b",
]

WCC_DELIVERY_TIME_PATTERNS = [
    r"\bprazo\b", r"\bchega quando\b", r"\bquando chega\b",
    r"\bquanto tempo (?:pra|para) chegar\b", r"\bdemora (?:pra|para) chegar\b",
    r"\bquantos dias (?:pra|para) chegar\b", r"\bchega ate\b",
    r"\bquando (?:voces )?enviam\b", r"\bquando (?:voces )?postam\b",
    r"\bdemora (?:pra|para) postar\b", r"\bpronta entrega\b",
]

WCC_AVAILABILITY_TERMS = [
    r"\bdisponivel\b", r"\bestoque\b", r"\besgotad[oa]s?\b",
    r"\bacabou\b", r"\breposicao\b", r"\brepor estoque\b",
    r"\bainda tem\b", r"\bvai voltar\b", r"\bvai repor\b",
    r"\bquando volta\b", r"\bquando repoe\b", r"\btem meu numero\b",
    r"\btem meu tamanho\b", r"\bnao tem meu numero\b",
    r"\bnao tem meu tamanho\b", r"\btem o numero\s*\d+\b",
    r"\btem numero\s*\d+\b", r"\btem (?:o |a )?(?:pp|p|m|g|gg|xg|xgg)\b",
]

WCC_AVAILABILITY_DECLARATIVE_PATTERNS = [
    r"\btem muito\b", r"\btem muita\b", r"\btem muitos\b",
    r"\btem muitas\b", r"\btem bastante\b", r"\btem varios\b",
    r"\btem varias\b", r"\btem de sobra\b", r"\bmuito em estoque\b",
    r"\bbastante em estoque\b", r"\bvoltou estoque\b",
    r"\bja esgotou\b", r"\bso tem \d+\b",
]

WCC_PURCHASE_PATH_PATTERNS = [
    r"\bmanda (?:o )?link\b", r"\bcade o link\b", r"\blink\b",
    r"\bcarrinho\b", r"\bsacola\b", r"\bonde (?:eu )?compro\b",
    r"\bonde compra\b", r"\bcomo compro\b", r"\bcomo comprar\b",
    r"\bcomo faco (?:pra|para) comprar\b", r"\bcomo fazer (?:pra|para) compra(?:r)?\b",
    r"\bcomo faz (?:pra|para) comprar\b", r"\badiciona no carrinho\b",
    r"\bcoloca no carrinho\b", r"\bqual (?:numero|item) (?:na|da) sacola\b",
    r"\bqual produto (?:ta|esta) fixado\b", r"\bonde clico\b",
    r"\bnao acho no carrinho\b",
]

WCC_PAYMENT_METHOD_PATTERNS = [
    r"\baceita pix\b", r"\baceita cartao\b", r"\baceita debito\b",
    r"\bforma de pagamento\b", r"\bformas de pagamento\b",
    r"\bcomo paga\b", r"\bcomo pagamento\b", r"\bpaga no pix\b",
    r"\bpaga no cartao\b", r"\btem pix\b", r"\btem cartao\b",
]

WCC_INSTALLMENT_PATTERNS = [
    r"\bparcela\b", r"\bparcelado\b", r"\bparcelamento\b",
    r"\bquantas vezes\b", r"\bsem juros\b", r"\bcom juros\b",
    r"\bdividir no cartao\b", r"\bpode dividir\b", r"\bvezes sem juros\b",
]

WCC_INVOICE_PATTERNS = [
    r"\bnota fiscal\b", r"\bnota\s+fiscal\b", r"\bnf(?:\s*e)?\b",
    r"\bdanfe\b", r"\bemite nota\b", r"\bemitem nota\b",
    r"\bvem com nota\b", r"\bvem nota\b", r"\bmanda nota\b",
    r"\bnota no meu cpf\b", r"\bnota no cpf\b",
]

# ============================================================
# 9. COMPRA / INTENCAO / HESITACAO
# ============================================================

WCC_BUYING_INTENT_PATTERNS = [
    r"\bvou\s+comprar\b", r"\bvou\s+compra\b", r"\bquero\s+comprar\b",
    r"\bquero\s+compra\b", r"\bvou\s+levar\b", r"\bquero\s+levar\b",
    r"\bvou\s+pegar\b", r"\bquero\s+pegar\b", r"\bquero esse\b",
    r"\bquero essa\b", r"\bquero um\b", r"\bquero uma\b",
    r"\bto comprando\b", r"\bestou comprando\b", r"\bja vou comprar\b",
    r"\btem como eu pegar\b", r"\bposso pegar\b", r"\bme ve um\b",
    r"\bme ve uma\b", r"\bvou pedir agora\b", r"\bsepara um pra mim\b",
    r"\bsepara uma pra mim\b", r"\besse eu quero\b", r"\bessa eu quero\b",
]

WCC_CONDITIONAL_BUYING_PATTERNS = [
    r"\bse\b.{0,80}\b(?:compro|comprar|pego|pegar|levo|levar|vou comprar|vou pegar|vou levar)\b",
    r"\b(?:compro|pego|levo)\b.{0,40}\bse\b",
]

WCC_PURCHASE_HESITATION_PATTERNS = [
    r"\bnao vou comprar\b", r"\bnao vou compra\b", r"\bnao vou pegar\b",
    r"\bnao vou levar\b", r"\bnao quero comprar\b", r"\bnao quero compra\b",
    r"\bnao quero pegar\b", r"\bdesisti\b", r"\bvou esperar\b",
    r"\bmelhor esperar\b", r"\bcaso nao vou pegar\b", r"\bnao compensa\b",
    r"\bvou pensar\b", r"\bnao sei se compro\b", r"\bto na duvida\b",
    r"\bestou na duvida\b", r"\bqueria mas\b", r"\bia comprar mas\b",
    r"\bdepois eu volto\b", r"\btalvez eu pegue\b", r"\btalvez eu compre\b",
]

def _wcc_purchase_is_frustrated(normalized):
    if _wcc_match(WCC_PURCHASE_HESITATION_PATTERNS, normalized):
        return True

    if (
        re.search(r"\bia comprar\b", normalized)
        and re.search(r"\b(mas|mais)\s+nao\b", normalized)
    ):
        return True

    return False

WCC_PURCHASE_COMPLETED_PATTERNS = [
    r"\bcomprei+\b", r"\bja\s+comprei+\b", r"\bacabei de comprar\b",
    r"\bfinalizei+\b", r"\bfinalizei+\s+a compra\b", r"\bcompra finalizada\b",
    r"\bpeguei+\s+o meu\b", r"\bpeguei+\s+a minha\b", r"\bgaranti+\s+o meu\b",
    r"\bgaranti+\s+a minha\b", r"\bpedido feito\b", r"\bja garanti+\b",
    r"\bfechei\b", r"\bja fiz o pedido\b",
]

WCC_PAYMENT_COMPLETED_PATTERNS = [
    r"\bpaguei\b", r"\bpix feito\b", r"\bpagamento feito\b",
    r"\bpagamento concluido\b", r"\bcartao aprovado\b",
]

# ============================================================
# 10. PEDIDO DE DEMONSTRACAO
# ============================================================

WCC_DEMO_PATTERNS = [
    r"\bmostra\b", r"\bmostre\b", r"\bmostrar\b", r"\bme mostra\b",
    r"\bde perto\b", r"\bmais perto\b", r"\baproxima\b", r"\baproxime\b",
    r"\bzoom\b", r"\bmostra atras\b", r"\bmostra a frente\b",
    r"\bmostra o tecido\b", r"\bmostra o detalhe\b", r"\bmostra o produto\b",
    r"\bmostra por dentro\b", r"\bmostra a textura\b", r"\bmostra a embalagem\b",
    r"\bmostra funcionando\b", r"\bexperimenta\b", r"\bexperimentar\b",
    r"\bvestindo\b", r"\bveste (?:o|a|esse|essa)\b", r"\bvira\b", r"\bvire\b",
    r"\babre (?:a|o|esse|essa)\b", r"\bliga (?:ele|ela|o|a)\b",
    r"\btesta (?:ele|ela|o|a|o som|a camera)\b",
    r"\bcompara (?:o|a|esse|essa|os|as)\b",
]

# ============================================================
# 11. OBJECOES ESPECIALIZADAS
# ============================================================

WCC_PRICE_OBJECTION_PATTERNS = [
    r"\bmuito caro\b", r"\bta caro\b", r"\besta caro\b", r"\bcaro demais\b",
    r"\bpreco salgado\b", r"\bnao vale (?:isso|esse preco)?\b",
    r"\bpor esse preco nao\b", r"\bachei mais barato\b",
    r"\bna outra loja (?:ta|esta) (?:mais )?barato\b",
    r"\bse fosse mais barato\b",
]

WCC_SHIPPING_OBJECTION_PATTERNS = [
    r"\bfrete caro\b", r"\bfrete (?:ta|esta) caro\b", r"\bfrete absurdo\b",
    r"\bfrete matou a compra\b", r"\bfrete ficou mais caro que o produto\b",
    r"\bcom esse frete nao da\b", r"\bnao vou pegar por causa do frete\b",
]

WCC_TRUST_OBJECTION_PATTERNS = [
    r"\bnao confio\b", r"\btenho medo de ser golpe\b", r"\bparece golpe\b",
    r"\bessa loja e confiavel\b", r"\be seguro comprar\b",
    r"\bvoces entregam mesmo\b", r"\btem muita reclamacao\b",
]

WCC_AUTHENTICITY_OBJECTION_PATTERNS = [
    r"\bparece falso\b", r"\bparece falsificado\b", r"\bnao parece original\b",
    r"\btenho medo de ser falso\b", r"\btenho medo de ser replica\b",
]

WCC_QUALITY_OBJECTION_PATTERNS = [
    r"\bparece fragil\b", r"\bmaterial parece ruim\b", r"\bnao parece igual (?:a|ao) foto\b",
    r"\be muito fino\b", r"\bnao gostei do acabamento\b",
    r"\bvi gente falando que da defeito\b", r"\bquebra facil\b",
]

WCC_SIZE_OBJECTION_PATTERNS = [
    r"\btenho medo de nao servir\b", r"\bacho que vai ficar pequeno\b",
    r"\bacho que vai ficar grande\b", r"\bnao sei meu tamanho\b",
    r"\bnao compro roupa online por causa do tamanho\b",
]

WCC_COMPATIBILITY_OBJECTION_PATTERNS = [
    r"\btenho medo de nao funcionar\b", r"\bnao sei se serve no meu\b",
    r"\bse nao (?:pegar|funcionar) no\b", r"\bmeu aparelho e antigo\b",
]

WCC_DELIVERY_OBJECTION_PATTERNS = [
    r"\bvai demorar demais\b", r"\bse levar \d+ dias nao quero\b",
    r"\bnao da tempo de chegar\b", r"\bdemora muito pra enviar\b",
    r"\bpreciso antes de\b",
]

WCC_PROMOTION_OBJECTION_PATTERNS = [
    r"\bessa promocao e real\b", r"\be promocao mesmo\b",
    r"\bnao acredito que vai acabar\b", r"\bultimas unidades e verdade\b",
    r"\besse preco estava igual ontem\b",
]

# ============================================================
# 12. POS-COMPRA DETALHADO
# ============================================================

WCC_POST_PURCHASE_RULES = [
    ("exchange", [
        r"\bquero trocar\b", r"\bposso trocar\b", r"\bcomo trocar\b",
        r"\btrocar (?:a|o) (?:cor|tamanho)\b", r"\btroca de (?:tamanho|cor)\b",
        r"\bmudar (?:a|o) (?:cor|tamanho)\b",
    ]),
    ("return", [
        r"\bquero devolver\b", r"\bcomo devolver\b", r"\bdevolucao\b",
        r"\bdevolver o produto\b", r"\bposso devolver\b", r"\bonde levo pra devolver\b",
        r"\bcomo gera etiqueta\b",
    ]),
    ("refund", [
        r"\breembolso\b", r"\bquero reembolso\b", r"\bpedir reembolso\b",
        r"\bquando cai o reembolso\b", r"\bnao recebi meu dinheiro\b",
        r"\bnao estornou\b", r"\bestorno\b",
    ]),
    ("cancellation", [
        r"\bquero cancelar\b", r"\bcomo cancelo\b", r"\bcancelar pedido\b",
        r"\bcancelar a compra\b", r"\bcancela meu pedido\b", r"\bainda da pra cancelar\b",
        r"\bpedi duas vezes\b",
    ]),
    ("delivered_not_received", [
        r"\bconsta entregue mas nao recebi\b", r"\bmarcou entregue mas nao recebi\b",
        r"\bdiz entregue mas nao chegou\b",
    ]),
    ("not_received", [
        r"\bpedido nao chegou\b", r"\bmeu pedido nao chegou\b",
        r"\bnao recebi (?:o|meu) pedido\b", r"\bninguem entregou\b",
        r"\bsumiu meu pedido\b",
    ]),
    ("shipping_delay", [
        r"\bpassou do prazo\b", r"\besta atrasado\b", r"\bpedido atrasado\b",
        r"\brastreio parado\b", r"\bcontinua preparando\b", r"\bdemora pra postar\b",
    ]),
    ("order_status", [
        r"\bcade meu pedido\b", r"\bonde esta meu pedido\b", r"\bja enviou\b",
        r"\bquando vai postar\b", r"\bstatus do pedido\b", r"\bcodigo de rastreio\b",
        r"\brastreio nao atualiza\b", r"\bcodigo nao funciona\b",
    ]),
    ("wrong_variant", [
        r"\bmandaram a cor errada\b", r"\bpedi .* veio .*\b",
        r"\bveio tamanho errado\b", r"\bveio numero errado\b",
    ]),
    ("wrong_item", [
        r"\bveio outro produto\b", r"\bproduto errado\b", r"\bmandaram o modelo errado\b",
        r"\bnao foi isso que comprei\b", r"\bveio errado\b",
    ]),
    ("missing_item", [
        r"\bfaltou (?:uma|um|a|o)\b", r"\bkit veio incompleto\b",
        r"\bnao veio o carregador\b", r"\bnao veio a capa\b",
        r"\bvieram \d+ e comprei \d+\b", r"\bcaixa veio vazia\b",
    ]),
    ("damaged_item", [
        r"\bveio quebrad[oa]\b", r"\bchegou quebrad[oa]\b", r"\bchegou trincad[oa]\b",
        r"\bveio rasgad[oa]\b", r"\bveio vazando\b", r"\bembalagem danificada\b",
    ]),
    ("defective_item", [
        r"\bveio com defeito\b", r"\bnao liga\b", r"\bparou de funcionar\b",
        r"\bproduto nao funciona\b", r"\bdeu defeito\b",
    ]),
    ("not_as_described", [
        r"\bnao e igual ao anuncio\b", r"\bnao corresponde a descricao\b",
        r"\bcor e diferente\b", r"\be bem menor do que parecia\b",
        r"\bmaterial nao e o mesmo\b", r"\bnao veio como mostraram\b",
    ]),
    ("payment_charge_problem", [
        r"\bcobrou duas vezes\b", r"\bpix saiu mas pedido nao confirmou\b",
        r"\bcartao aprovou mas nao aparece\b", r"\bcancelei e ainda cobraram\b",
        r"\bcobranca duplicada\b",
    ]),
    ("invoice_missing", [
        r"\bnao veio a nota\b", r"\bnao veio nota fiscal\b", r"\bonde baixo a nota fiscal\b",
        r"\bminha nf nao veio\b", r"\bpreciso da nota do pedido\b",
    ]),
    ("warranty_claim", [
        r"\bcomo uso a garantia\b", r"\bacionar a garantia\b", r"\baciono a garantia\b",
        r"\bonde aciono a garantia\b", r"\bmandar pra garantia\b",
        r"\bquero usar a garantia\b",
    ]),
]

# ============================================================
# 13. VOCABULARIOS DE PRODUTO
# ============================================================

WCC_COLOR_WORDS = {
    "cor", "cores", "azul", "preto", "preta", "branco", "branca",
    "rosa", "verde", "vermelho", "vermelha", "amarelo", "amarela",
    "bege", "marrom", "cinza", "roxo", "roxa", "lilas", "laranja",
    "nude", "pink", "dourado", "dourada", "prata", "vinho", "caramelo",
    "offwhite", "off", "white",
}

WCC_MATERIAL_WORDS = {
    "tecido", "material", "algodao", "poliester", "malha", "couro",
    "jeans", "linho", "viscose", "elastano", "lycra", "cetim", "seda",
    "renda", "microfibra", "plastico", "metal", "aco", "inox", "aluminio",
    "madeira", "vidro", "silicone", "ceramica", "porcelana", "borracha",
}

WCC_DEVICE_WORDS = {
    "iphone", "ipad", "android", "samsung", "motorola", "xiaomi", "ios",
    "windows", "mac", "macbook", "notebook", "celular", "tablet", "tv",
    "televisao", "alexa", "carro", "veiculo", "inducao",
}

WCC_PRODUCT_REFERENCE_WORDS = {
    "produto", "item", "modelo", "peca", "kit", "iphone", "ipad", "tablet",
    "celular", "notebook", "smartwatch", "relogio", "fone", "case", "capa",
    "pelicula", "perfume", "body", "splash", "panela", "garrafa", "vestido",
    "blusa", "calca", "camisa", "tenis", "sapato", "sandalia", "bolsa",
    "mochila", "carregador", "cabo", "fone", "creme", "serum", "shampoo",
    "mascara", "batom", "base", "maquiagem",
}

# ============================================================
# 14. DETECTAR PERGUNTA / PEDIDO
# ============================================================

WCC_INTERROGATIVE_START = [
    r"^quanto\b", r"^qual\b", r"^quais\b", r"^como\b", r"^onde\b",
    r"^quando\b", r"^porque\b", r"^por que\b", r"^pra que\b", r"^para que\b",
    r"^e pra que\b", r"^e para que\b", r"^sera que\b", r"^sabe se\b",
    r"^alguem sabe\b", r"^tem como\b", r"^aceita\b", r"^parcela\b",
    r"^possui\b", r"^emite\b",
]

WCC_IMPLICIT_QUESTION_PATTERNS = [
    r"^e\s+(?:bivolt|original|novo|nova|usado|usada|recondicionado|recondicionada|compativel)\b",
    r"^serve\s+(?:no|na|com|em|pra|para)\b",
    r"^funciona\s+(?:no|na|com|sem)\b",
    r"^vem\s+(?:com|nota|carregador|cabo|controle|pilha|capa|manual|brinde)\b",
    r"^tem\s+(?:garantia|nota fiscal|nf|bluetooth|wifi|wi fi|gps|nfc|chip|esim)\b",
]

def _wcc_is_question(original, normalized):
    original = str(original or "")

    if "?" in original:
        return True

    if _wcc_match(WCC_INTERROGATIVE_START, normalized):
        return True

    if _wcc_match(WCC_IMPLICIT_QUESTION_PATTERNS, normalized):
        return True

    if normalized.startswith("tem "):
        declarative_after_tem = re.match(
            r"^tem\s+(que|muito|muita|muitos|muitas|bastante|varios|varias|de sobra)\b",
            normalized
        )
        if not declarative_after_tem:
            return True

    phrases = [
        "da certo", "da pra", "da para", "quero saber", "queria saber",
        "me diz", "me fala", "serve pra", "serve para", "pra que serve",
        "para que serve", "serve pra que", "serve para que", "qual a funcao",
        "qual funcao", "o que faz", "como funciona",
    ]

    return any(_wcc_exact_phrase(normalized, phrase) for phrase in phrases)


def _wcc_is_request(normalized):
    request_patterns = [
        r"^(mostra|mostre|manda|coloca|coloque|adiciona|libera|solta|abre|liga|testa|compara)\b",
        r"^da\s+(um\s+)?cupom\b", r"\bpor favor\b", r"\bquero saber\b",
        r"\bme mostra\b", r"\bme manda\b", r"\bme fala\b", r"\bme diz\b",
        r"\bsepara (?:um|uma) pra mim\b",
    ]
    return _wcc_match(request_patterns, normalized)

# ============================================================
# 15. MENCAO MONETARIA
# ============================================================

def _wcc_has_price_mention(original):
    text = str(original or "").lower()

    if re.search(r"r\$\s*\d+(?:[.,]\d{1,2})?", text):
        return True
    if re.search(r"\b\d{1,5}[.,]\d{2}\b", text):
        return True
    if re.search(r"\b\d+(?:[.,]\d{1,2})?\s+rea(?:l|is)\b", text):
        return True
    return False

# ============================================================
# 16. SELECAO DE VARIANTE
# ============================================================

def _wcc_is_variant_selection(normalized):
    color_count = sum(
        1 for color in WCC_COLOR_WORDS
        if _wcc_has_word(normalized, color)
    )

    size_count = len(re.findall(r"\b(?:pp|p|m|g|gg|xg|xgg|\d{2})\b", normalized))

    selection_verb = bool(re.search(
        r"\b(pegar|pego|quero|levar|comprar|compra|escolher|escolho|mandar|manda)\b",
        normalized
    ))

    has_variant_language = bool(re.search(
        r"\b(outro|outra|cada|um de|uma de|de cada|cores?|tamanhos?)\b",
        normalized
    ))

    return bool(
        (color_count >= 2 and selection_verb)
        or (color_count >= 1 and selection_verb and has_variant_language)
        or (size_count >= 2 and selection_verb and has_variant_language)
    )

# ============================================================
# 17. CONTEXTO DE POS-COMPRA
# ============================================================

def _wcc_has_post_purchase_context(normalized):
    return bool(re.search(
        r"\b(meu pedido|minha compra|comprei|paguei|recebi|chegou|veio|mandaram|cancelei|rastreio|entregue|estorno|reembolso)\b",
        normalized
    ))

# ============================================================
# 18. SUBTIPOS DE PRODUTO - MULTI-INTENCAO
# ============================================================

def _wcc_collect_product_subtypes(original, normalized):
    words = set(normalized.split())
    found = []

    def add(subtype):
        if subtype not in found:
            found.append(subtype)

    # FUNCAO / UTILIDADE
    if _wcc_match([
        r"\b(?:pra|para)\s+que(?:\s+que)?\s+serve\b",
        r"\bserve\s+(?:pra|para)\s+que\b",
        r"\bo\s+que\b.{0,40}\bfaz\b",
        r"\bqual\s+(?:a\s+)?funcao\b",
        r"\bcomo\s+(?:(?:isso|isto|esse|essa|este|esta|produto|item|coisa)\s+)?funciona\b",
        r"\b(?:pra|para)\s+que\s+e\b", r"\btem\s+funcao\b",
    ], normalized) or re.fullmatch(r"(?:e\s+)?(?:pra|para)\s+que", normalized):
        add("functionality")

    # USO / METODO DE USO
    if _wcc_match([
        r"\bcomo usa\b", r"\bcomo usar\b", r"\bcomo aplica\b", r"\bcomo aplicar\b",
        r"\bcomo coloca\b", r"\bcomo colocar\b", r"\bcomo monta\b", r"\bcomo montar\b",
        r"\bcomo liga\b", r"\bcomo configurar\b", r"\bcomo configura\b",
        r"\bprecisa instalar\b", r"\btem que diluir\b", r"\bquantas vezes usa\b",
        r"\bmodo de usar\b",
    ], normalized):
        add("usage_method")

    # COMPATIBILIDADE - antes de size_fit
    if (
        _wcc_match([r"\bcompativel\b", r"\bcompatibilidade\b"], normalized)
        or re.search(r"\b(?:serve|funciona)\s+(?:no|na|com)\s+[a-z0-9]+", normalized)
        and bool(words & WCC_DEVICE_WORDS)
        or bool(words & WCC_DEVICE_WORDS) and re.search(r"\b(?:serve|funciona|compativel)\b", normalized)
    ):
        add("compatibility")

    # TAMANHO / ENCAIXE
    size_context = bool(
        re.search(r"\b(calco|calca|veste|visto|manequim|caimento|encaixe|forma)\b", normalized)
        or _wcc_exact_phrase(normalized, "da certo")
        or re.search(r"\bnumero\s*\d+\b", normalized)
        or re.search(r"\btamanho\s*(?:pp|p|m|g|gg|xg|xgg|\d+)\b", normalized)
        or re.search(r"\b\d{1,3}\s+da certo\b", normalized)
        or re.search(r"\b(?:uso|calco|calca|visto|manequim)\s+(?:numero\s*)?(?:pp|p|m|g|gg|xg|xgg|\d{1,3})\b", normalized)
        or re.search(r"\bserve\s+(?:em|no|na)\s+(?:mim|meu|minha|numero|tamanho|manequim|quem)\b", normalized)
        or re.search(r"\bserve\s+(?:pra|para)\s+(?:mim|quem|pessoa|crianca|adulto|homem|mulher)\b", normalized)
        or re.search(r"\bserve\s+(?:o\s+)?(?:tamanho\s*)?(?:pp|p|m|g|gg|xg|xgg|\d{1,3})\b", normalized)
        or re.search(r"\b(?:pp|p|m|g|gg|xg|xgg)\s+serve\b", normalized)
    )
    if size_context:
        add("size_fit")

    # DIMENSOES
    if _wcc_match([
        r"\bmedida(?:s)?\b", r"\bcomprimento\b", r"\baltura\b", r"\blargura\b",
        r"\bespessura\b", r"\bdiametro\b", r"\bcentimetro(?:s)?\b", r"\bcm\b",
        r"\bmetro(?:s)?\b", r"\bquanto mede\b", r"\btamanho do cabo\b",
        r"\btamanho da tela\b", r"\btamanho da alca\b",
    ], normalized) or ("tamanho" in words and not size_context):
        add("dimensions")

    # COR / VARIACAO
    if words & WCC_COLOR_WORDS or re.search(r"\boutras cores\b|\bqual cor\b|\bvariacao\b", normalized):
        add("color_variant")

    # MATERIAL / COMPOSICAO
    if words & WCC_MATERIAL_WORDS or re.search(r"\bcomposicao do (?:tecido|material)\b", normalized):
        add("material_composition")

    # CAPACIDADE
    if _wcc_match([
        r"\bcapacidade\b", r"\blitro(?:s)?\b", r"\bml\b", r"\bquanto cabe\b",
        r"\bcabe quanto\b", r"\bcabe notebook\b", r"\bcabe garrafa\b",
    ], normalized):
        add("capacity")

    # PESO
    if _wcc_match([r"\bpeso\b", r"\bpesa quanto\b", r"\bquanto pesa\b", r"\bquantos gramas\b", r"\be pesado\b", r"\be leve\b"], normalized):
        add("weight")

    # QUANTIDADE / KIT
    if _wcc_match([
        r"\bquantos vem\b", r"\bquantas vem\b", r"\bvem quantos\b", r"\bvem quantas\b",
        r"\bquantas unidades\b", r"\bquantos itens\b", r"\bkit vem com quantos\b",
        r"\be uma unidade\b", r"\bvem o par\b", r"\bquantas pecas\b",
        r"\b(?:preco|valor) e do kit\b", r"\bkit ou de um\b", r"\bkit ou unidade\b",
    ], normalized):
        add("quantity_package")

    # ITENS INCLUSOS
    if _wcc_match([
        r"\bvem com\b", r"\bacompanha\b", r"\binclui\b", r"\bvem junto\b",
        r"\bvem incluso\b", r"\bvem inclusa\b", r"\bo que vem na caixa\b",
        r"\bvem carregador\b", r"\bvem cabo\b", r"\bvem controle\b", r"\bvem brinde\b",
    ], normalized):
        add("included_items")

    # GARANTIA (pre-compra; claim e tratado antes no pos-compra)
    if re.search(r"\bgarantia\b", normalized) and not _wcc_match([p for s, ps in WCC_POST_PURCHASE_RULES if s == "warranty_claim" for p in ps], normalized):
        add("warranty")

    # MARCA / FABRICANTE
    #
    # Distingue perguntas sobre quem fabrica / qual e a marca
    # de perguntas de autenticidade. Tambem cobre a forma comum
    # de LIVE "esse produto e da <marca>?" sem depender de uma
    # lista fechada de marcas.
    brand_manufacturer_explicit = _wcc_match([
        r"\bqual(?: e)?(?: a)? marca\b",
        r"\bde qual marca\b",
        r"\bmarca (?:desse|deste|dessa|desta|do|da)\b",
        r"\bqual(?: e)?(?: o)? fabricante\b",
        r"\bde qual fabricante\b",
        r"\bquem fabrica\b",
        r"\bquem fabricou\b",
        r"\bquem e o fabricante\b",
    ], normalized)

    brand_manufacturer_named = re.search(
        r"\be da\s+([a-z0-9][a-z0-9._-]{2,})\b",
        normalized,
    )

    brand_manufacturer_exclusions = {
        "cor", "marca", "live", "loja", "oferta", "promocao",
        "sacola", "caixa", "foto", "imagem", "tela", "parte",
        "frente", "linha", "colecao", "familia", "categoria",
        "sessao", "campanha", "garantia", "embalagem", "descricao",
    }

    if brand_manufacturer_explicit or (
        brand_manufacturer_named
        and brand_manufacturer_named.group(1) not in brand_manufacturer_exclusions
    ):
        add("brand_manufacturer")

    # AUTENTICIDADE
    if _wcc_match([
        r"\be original\b", r"\boriginal mesmo\b", r"\be verdadeiro\b", r"\be de verdade\b",
        r"\be replica\b", r"\bnao e replica\b", r"\be falsificado\b", r"\bproduto oficial\b",
        r"\btem selo de autenticidade\b", r"\be da marca mesmo\b",
    ], normalized):
        add("authenticity")

    # CONDICAO
    if _wcc_match([
        r"\be novo\b", r"\be nova\b", r"\be usado\b", r"\be usada\b",
        r"\brecondicionado\b", r"\bopen box\b", r"\bvem lacrado\b",
        r"\bcaixa vem fechada\b", r"\bproduto de mostruario\b",
    ], normalized):
        add("product_condition")

    # CERTIFICACAO / CONFORMIDADE
    if _wcc_match([
        r"\banatel\b", r"\bhomologado\b", r"\binmetro\b", r"\banvisa\b",
        r"\bcertificado\b", r"\bcertificacao\b", r"\bregistro\b",
    ], normalized):
        add("certification_compliance")

    # BATERIA / AUTONOMIA
    if _wcc_match([
        r"\bbateria\b", r"\bautonomia\b", r"\bcarga\b", r"\brecarrega\b",
        r"\bcarregar\b", r"\bdura quanto\b", r"\bquanto dura\b",
        r"\bquantas horas\b", r"\btempo de carga\b", r"\busa pilha\b",
        r"\busb c\b",
    ], normalized):
        add("battery_duration")

    # CONECTIVIDADE
    if _wcc_match([
        r"\bbluetooth\b", r"\bwi ?fi\b", r"\b5g\b", r"\bgps\b", r"\bnfc\b",
        r"\besim\b", r"\baceita chip\b", r"\btem chip\b", r"\bfaz ligacao\b",
        r"\bprecisa de aplicativo\b", r"\bqual app\b", r"\bfunciona sem internet\b",
    ], normalized):
        add("connectivity")

    # ESPECIFICACOES TECNICAS
    if _wcc_match([
        r"\b\d+\s*(?:gb|tb)\b", r"\bram\b", r"\bprocessador\b", r"\bresolucao\b",
        r"\bversao do android\b", r"\bcamera frontal\b", r"\bcamera traseira\b",
        r"\bentrada de cartao\b", r"\btela de \d+\b",
    ], normalized):
        add("technical_specification")

    # VOLTAGEM / POTENCIA
    if _wcc_match([
        r"\bbivolt\b", r"\bvoltagem\b", r"\bvolts?\b", r"\bwatt(?:s)?\b",
        r"\bpotencia\b", r"\b(?:110|127|220)\s*v?\b",
    ], normalized):
        add("power_voltage")

    # USO / RESISTENCIA
    if _wcc_match([
        r"\baguenta\b", r"\bresiste\b", r"\bresistente\b", r"\ba prova d agua\b",
        r"\bpode molhar\b", r"\bpode lavar\b", r"\bpode congelar\b", r"\bpode aquecer\b",
        r"\bmicro ?ondas\b", r"\bfreezer\b", r"\bcongelador\b", r"\blava loucas\b",
        r"\bpode ir no forno\b", r"\bquebra facil\b", r"\barranha\b", r"\bvaza\b",
    ], normalized):
        add("usage_resistance")

    # SENSORIAL
    if _wcc_match([
        r"\bcheiro\b", r"\bfragrancia\b", r"\baroma\b", r"\bperfume\b",
        r"\bsabor\b", r"\be doce\b", r"\bamadeirado\b", r"\bfixa bem\b",
        r"\btextura\b", r"\boleoso\b", r"\bpegajoso\b",
    ], normalized):
        add("sensory_characteristic")

    # INGREDIENTES / COMPOSICAO FORMULACAO
    if _wcc_match([
        r"\bingrediente(?:s)?\b", r"\btem acido\b", r"\btem alcool\b", r"\bparabeno\b",
        r"\bsulfato\b", r"\bvegano\b", r"\btem silicone\b", r"\bcomposicao\b",
        r"\bgluten\b", r"\blactose\b", r"\bamendoim\b", r"\bacucar\b", r"\badocante\b",
    ], normalized):
        add("ingredients_composition")

    # VALIDADE
    if _wcc_match([
        r"\bvalidade\b", r"\bvence quando\b", r"\bdata de fabricacao\b",
        r"\bperto de vencer\b", r"\bquanto tempo dura aberto\b", r"\bdepois de aberto\b",
    ], normalized):
        add("expiration_validity")

    # SEGURANCA / ADEQUACAO
    if _wcc_match([
        r"\bpele sensivel\b", r"\bpele oleosa\b", r"\bcabelo cacheado\b",
        r"\bcrianca pode usar\b", r"\bgravida pode usar\b", r"\bgestante pode usar\b",
        r"\bquem tem alergia\b", r"\bpode usar todo dia\b", r"\bidade indicada\b",
        r"\bpeso indicado\b",
    ], normalized):
        add("safety_suitability")

    # CARACTERISTICAS DE MODA
    if _wcc_match([
        r"\bestica\b", r"\btransparente\b", r"\bmarca muito\b", r"\btem forro\b",
        r"\btem bolso\b", r"\btem ziper\b", r"\bamarrota\b", r"\bencolhe\b",
        r"\bdesbota\b", r"\blavar na maquina\b", r"\bsalto tem quantos\b",
        r"\bforma grande\b", r"\bforma pequena\b",
    ], normalized):
        add("apparel_characteristic")

    # ALIMENTOS
    if _wcc_match([
        r"\bcalorias\b", r"\bporcoes\b", r"\bquantas porcoes\b", r"\bcomo prepara\b",
        r"\bprecisa gelar\b", r"\bconservar\b", r"\balergenico\b", r"\balergenos\b",
    ], normalized):
        add("food_characteristic")

    # COMPARACAO
    if _wcc_match([
        r"\bqual e melhor\b", r"\bqual melhor\b", r"\bqual a diferenca\b",
        r"\bdiferenca desse pro outro\b", r"\bdiferenca deste para o outro\b",
        r"\besse ou o outro\b", r"\bqual e maior\b", r"\bqual dura mais\b",
        r"\be igual ao outro\b", r"\bcompara\b",
    ], normalized):
        add("comparison")

    return found

# ============================================================
# 19. PERGUNTA GENERICA DE PRODUTO
# ============================================================

def _wcc_looks_like_product_question(original, normalized, subtypes):
    is_question = _wcc_is_question(original, normalized)
    is_request = _wcc_is_request(normalized)

    if subtypes:
        # Regras explicitas de produto podem identificar perguntas curtas
        # mesmo quando o usuario omite o ponto de interrogacao.
        if is_question or is_request:
            return True

        implicit_product_prompt = bool(re.search(
            r"^(?:e\s+)?(?:bivolt|original|novo|nova|usado|usada|recondicionado|recondicionada|garantia|compativel)\b",
            normalized
        )) or bool(re.search(
            r"^(?:serve|funciona|vem|possui|inclui|acompanha)\b",
            normalized
        ))

        return implicit_product_prompt

    if not is_question:
        return False

    exact_words = {"produto", "item", "modelo", "peca", "esse", "essa", "isso"}
    if _wcc_has_any_word(normalized, exact_words):
        return True

    exact_phrases = [
        "vem com", "como funciona", "o que faz", "pra que serve", "para que serve",
        "serve pra que", "serve para que", "qual a funcao", "qual funcao",
        "pode usar", "pode molhar", "tem garantia", "quanto mede", "quanto pesa",
    ]
    return any(_wcc_exact_phrase(normalized, phrase) for phrase in exact_phrases)

# ============================================================
# 20. REFERENCIA CONTEXTUAL DE PRODUTO
# ============================================================

def _wcc_is_contextual_product_reference(normalized):
    words = set(normalized.split())

    if len(normalized.split()) > 12:
        return False

    has_product_word = bool(words & WCC_PRODUCT_REFERENCE_WORDS)
    has_storage_spec = bool(re.search(r"\b\d+\s*(?:gb|tb)\b", normalized))
    has_color = bool(words & WCC_COLOR_WORDS)
    has_size = bool(re.search(r"\b(?:pp|p|m|g|gg|xg|xgg|\d{2})\b", normalized))
    has_variant_reference = bool(re.search(r"\b(?:esse|essa|aquele|aquela|o|a)\b", normalized))

    return bool(
        has_product_word and (has_storage_spec or has_color or has_size or has_variant_reference)
        or has_storage_spec and (has_color or has_product_word)
    )

# ============================================================
# 21. ADICIONAR INTENCAO SEM DUPLICAR
# ============================================================

def _wcc_add_intent(intents, category, subtype, requires_response):
    key = (category, subtype)
    for existing in intents:
        if (existing.get("category"), existing.get("subtype")) == key:
            return

    intents.append({
        "category": category,
        "subtype": subtype,
        "requires_response": bool(requires_response),
    })

# ============================================================
# 22. CLASSIFICADOR PRINCIPAL V1.5
# ============================================================

def _wcc_classify_comment(text):
    original = str(text or "").strip()
    normalized = _wcc_normalize(original)

    is_question = _wcc_is_question(original, normalized)
    is_request = _wcc_is_request(normalized)
    intents = []
    conditional_purchase = _wcc_match(WCC_CONDITIONAL_BUYING_PATTERNS, normalized)

    # --------------------------------------------------------
    # A. POS-COMPRA
    # --------------------------------------------------------
    matched_post_purchase = set()
    for subtype, patterns in WCC_POST_PURCHASE_RULES:
        if _wcc_match(patterns, normalized):
            matched_post_purchase.add(subtype)
            _wcc_add_intent(intents, "post_purchase_issue", subtype, True)

    post_purchase_context = bool(matched_post_purchase) or _wcc_has_post_purchase_context(normalized)

    # --------------------------------------------------------
    # B. PRECO
    # --------------------------------------------------------
    if _wcc_match(WCC_PRICE_PATTERNS, normalized):
        price_short = bool(re.fullmatch(r"(?:preco|pre|prc|valor|vlr)", normalized))
        if is_question or is_request or price_short:
            _wcc_add_intent(intents, "commercial_question", "price", True)

    # --------------------------------------------------------
    # C. PROMOCAO / CUPOM / PROBLEMA DE PROMOCAO
    # --------------------------------------------------------
    promotion_problem = _wcc_match(WCC_PROMOTION_PROBLEM_PATTERNS, normalized)
    promotion_request = _wcc_match(WCC_PROMOTION_REQUEST_PATTERNS, normalized)
    has_promotion_term = _wcc_match(WCC_PROMOTION_TERMS, normalized)

    if promotion_problem:
        _wcc_add_intent(intents, "commercial_question", "promotion_problem", True)
    elif has_promotion_term:
        if is_question or is_request or promotion_request or conditional_purchase:
            _wcc_add_intent(intents, "commercial_question", "promotion_coupon", True)
        else:
            _wcc_add_intent(intents, "commercial_observation", "promotion_mention", False)

    # --------------------------------------------------------
    # D. FRETE / PRAZO
    # --------------------------------------------------------
    has_shipping = _wcc_match(WCC_SHIPPING_TERMS, normalized)
    has_delivery_time = _wcc_match(WCC_DELIVERY_TIME_PATTERNS, normalized)

    if has_shipping:
        shipping_short = bool(re.fullmatch(r"(?:frete|entrega|envio|cep|frete gratis)", normalized))
        if is_question or is_request or shipping_short or conditional_purchase:
            _wcc_add_intent(intents, "commercial_question", "shipping", True)
        elif not post_purchase_context:
            _wcc_add_intent(intents, "commercial_observation", "shipping_mention", False)

    if has_delivery_time and not post_purchase_context:
        _wcc_add_intent(intents, "commercial_question", "delivery_time", True)

    # --------------------------------------------------------
    # E. DISPONIBILIDADE / ESTOQUE
    # --------------------------------------------------------
    has_availability = _wcc_match(WCC_AVAILABILITY_TERMS, normalized)

    # "tem preto?", "tem rosa?", "tem M?", etc.
    words = set(normalized.split())
    has_variant_availability = bool(
        re.search(r"\b(?:tem|tiver)\b", normalized)
        and (
            bool(words & WCC_COLOR_WORDS)
            or re.search(r"\b(?:pp|p|m|g|gg|xg|xgg|\d{2})\b", normalized)
        )
    )

    availability_declarative = _wcc_match(WCC_AVAILABILITY_DECLARATIVE_PATTERNS, normalized)

    if has_availability or has_variant_availability:
        if (is_question or has_variant_availability) and not availability_declarative:
            _wcc_add_intent(intents, "commercial_question", "availability", True)
        elif re.search(r"\bnao tem meu (?:numero|tamanho)\b", normalized):
            _wcc_add_intent(intents, "commercial_question", "availability", True)
        else:
            _wcc_add_intent(intents, "commercial_observation", "availability_mention", False)

    # --------------------------------------------------------
    # F. CAMINHO DE COMPRA
    # --------------------------------------------------------
    if _wcc_match(WCC_PURCHASE_PATH_PATTERNS, normalized):
        _wcc_add_intent(intents, "commercial_question", "purchase_path", True)

    # --------------------------------------------------------
    # G. PAGAMENTO / PARCELAMENTO
    # --------------------------------------------------------
    if _wcc_match(WCC_PAYMENT_METHOD_PATTERNS, normalized):
        if is_question or is_request or re.fullmatch(r"(?:pix|cartao|debito)", normalized):
            _wcc_add_intent(intents, "commercial_question", "payment_method", True)

    if _wcc_match(WCC_INSTALLMENT_PATTERNS, normalized):
        if is_question or is_request or re.fullmatch(r"(?:parcela|parcelado|parcelamento)", normalized):
            _wcc_add_intent(intents, "commercial_question", "installments", True)

    # --------------------------------------------------------
    # H. NOTA FISCAL / DOCUMENTO FISCAL
    # --------------------------------------------------------
    has_invoice = _wcc_match(WCC_INVOICE_PATTERNS, normalized)
    if has_invoice and "invoice_missing" not in matched_post_purchase:
        _wcc_add_intent(intents, "commercial_question", "invoice_fiscal_document", True)

    # --------------------------------------------------------
    # I. SELECAO DE VARIANTES
    # --------------------------------------------------------
    variant_selection = _wcc_is_variant_selection(normalized)
    if variant_selection:
        _wcc_add_intent(intents, "commercial_question", "variant_selection", True)

    # --------------------------------------------------------
    # J. PERGUNTAS SOBRE PRODUTO - MULTI-SUBTIPO
    # --------------------------------------------------------
    product_subtypes = _wcc_collect_product_subtypes(original, normalized)
    if _wcc_looks_like_product_question(original, normalized, product_subtypes) or (variant_selection and product_subtypes):
        if not product_subtypes:
            product_subtypes = ["generic_characteristic"]
        for subtype in product_subtypes:
            _wcc_add_intent(intents, "product_question", subtype, True)

    # --------------------------------------------------------
    # K. DEMONSTRACAO
    # --------------------------------------------------------
    if _wcc_match(WCC_DEMO_PATTERNS, normalized):
        _wcc_add_intent(intents, "demo_request", "show_demonstrate", True)

    # --------------------------------------------------------
    # L. COMPRA CONCLUIDA / PAGAMENTO REPORTADO
    # --------------------------------------------------------
    if _wcc_match(WCC_PURCHASE_COMPLETED_PATTERNS, normalized):
        _wcc_add_intent(intents, "purchase_completed", "reported_purchase", False)

    if _wcc_match(WCC_PAYMENT_COMPLETED_PATTERNS, normalized):
        _wcc_add_intent(intents, "purchase_completed", "reported_payment", False)

    # --------------------------------------------------------
    # M. HESITACAO / INTENCAO
    # --------------------------------------------------------
    frustrated_purchase = _wcc_purchase_is_frustrated(normalized)

    if frustrated_purchase:
        _wcc_add_intent(intents, "objection", "purchase_hesitation", False)
    else:
        if conditional_purchase:
            _wcc_add_intent(intents, "buying_intent", "conditional_purchase_intent", False)
        elif _wcc_match(WCC_BUYING_INTENT_PATTERNS, normalized) or (re.search(r"\bquero\b", normalized) and bool(words & WCC_COLOR_WORDS)):
            _wcc_add_intent(intents, "buying_intent", "purchase_intent", False)

    if variant_selection and not frustrated_purchase:
        _wcc_add_intent(intents, "buying_intent", "purchase_intent", False)

    # --------------------------------------------------------
    # N. OBJECOES ESPECIALIZADAS
    # --------------------------------------------------------
    objection_rules = [
        ("price_objection", WCC_PRICE_OBJECTION_PATTERNS),
        ("shipping_objection", WCC_SHIPPING_OBJECTION_PATTERNS),
        ("trust_objection", WCC_TRUST_OBJECTION_PATTERNS),
        ("authenticity_objection", WCC_AUTHENTICITY_OBJECTION_PATTERNS),
        ("quality_objection", WCC_QUALITY_OBJECTION_PATTERNS),
        ("size_objection", WCC_SIZE_OBJECTION_PATTERNS),
        ("compatibility_objection", WCC_COMPATIBILITY_OBJECTION_PATTERNS),
        ("delivery_time_objection", WCC_DELIVERY_OBJECTION_PATTERNS),
        ("promotion_objection", WCC_PROMOTION_OBJECTION_PATTERNS),
    ]

    for subtype, patterns in objection_rules:
        if _wcc_match(patterns, normalized):
            _wcc_add_intent(intents, "objection", subtype, bool(is_question))

    # --------------------------------------------------------
    # O. MENCAO MONETARIA
    # --------------------------------------------------------
    if _wcc_has_price_mention(original):
        has_price_question = any(
            i.get("category") == "commercial_question" and i.get("subtype") == "price"
            for i in intents
        )
        if not has_price_question:
            _wcc_add_intent(intents, "commercial_observation", "price_mention", False)

    # --------------------------------------------------------
    # P. REFERENCIA CONTEXTUAL CURTA DE PRODUTO
    # --------------------------------------------------------
    if not intents and _wcc_is_contextual_product_reference(normalized):
        _wcc_add_intent(intents, "commercial_observation", "contextual_product_reference", False)

    # --------------------------------------------------------
    # CLASSIFICACAO PRINCIPAL - NAO E PRIORIDADE
    # --------------------------------------------------------
    main_classification = None

    special_variant = next(
        (
            intent for intent in intents
            if intent.get("category") == "commercial_question"
            and intent.get("subtype") == "variant_selection"
        ),
        None,
    )

    if special_variant is not None:
        main_classification = dict(special_variant)
    else:
        classification_order = [
            "post_purchase_issue",
            "product_question",
            "commercial_question",
            "demo_request",
            "buying_intent",
            "purchase_completed",
            "objection",
            "commercial_observation",
        ]

        for wanted_category in classification_order:
            found = next(
                (intent for intent in intents if intent.get("category") == wanted_category),
                None,
            )
            if found is not None:
                main_classification = dict(found)
                break

    relevant = bool(intents)
    requires_response = any(i.get("requires_response", False) for i in intents)

    return {
        "relevant": relevant,
        "is_question": is_question,
        "is_request": is_request,
        "requires_response": requires_response,
        "main_classification": main_classification,
        "intents": intents,
        "normalized": normalized,
    }

# ============================================================
# 29. TEMA GENERICO
# ============================================================

WCC_GENERIC_STOPWORDS = {
    "a",
    "o",
    "as",
    "os",
    "um",
    "uma",
    "de",
    "da",
    "do",
    "das",
    "dos",
    "e",
    "em",
    "no",
    "na",
    "nos",
    "nas",
    "pra",
    "para",
    "por",
    "que",
    "qual",
    "quais",
    "como",
    "tem",
    "esse",
    "essa",
    "isso",
    "me",
    "eu",
    "voce",
    "voces",
    "pode",
    "ai",
}

def _wcc_generic_topic_signature(normalized):
    tokens = []

    for token in normalized.split():
        if len(token) < 3:
            continue

        if token in WCC_GENERIC_STOPWORDS:
            continue

        if token not in tokens:
            tokens.append(token)

    if not tokens:
        return "generic"

    return "_".join(tokens[:4])

# ============================================================
# 30. CHAVE DE TENDENCIA
# ============================================================

def _wcc_intent_topic_key(intent, record):
    category = intent.get("category")
    subtype = intent.get("subtype")

    if (
        category == "product_question"
        and subtype == "generic_characteristic"
    ):
        signature = _wcc_generic_topic_signature(
            record.get("normalized", "")
        )

        return (
            f"{category}:"
            f"{subtype}:"
            f"{signature}"
        )

    return f"{category}:{subtype}"

# ============================================================
# 31. LIMPAR JANELA
# ============================================================

def _wcc_prune(queue, now=None):
    if now is None:
        now = time.time()

    limit = now - WORKER_COACH_COMMENTS_WINDOW

    while (
        queue
        and
        queue[0].get("timestamp", 0) < limit
    ):
        queue.popleft()

# ============================================================
# 32. USUARIOS UNICOS
# ============================================================

def _wcc_unique_users(records):
    users = set()

    for record in records:
        user = str(
            record.get("user")
            or ""
        ).strip().lower()

        if user:
            users.add(user)

    return len(users)

# ============================================================
# 33. FORCA DO PADRAO
# ============================================================

def _wcc_strength(
    count,
    unique_users,
    threshold
):
    if threshold <= 0:
        return 0.0

    count_ratio = count / threshold
    user_ratio = unique_users / threshold

    score = (
        0.65 * count_ratio
        +
        0.35 * user_ratio
    )

    return round(
        min(
            1.0,
            max(
                0.0,
                score
            )
        ),
        3
    )

# ============================================================
# 34. FILA DE SAIDA
# ============================================================

def _wcc_queue_output(output):
    global worker_coach_comments_dropped_outputs

    queue = worker_coach_comments_output_queue

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

        worker_coach_comments_dropped_outputs += 1

        try:
            queue.put_nowait(
                output.copy()
            )
        except Exception:
            pass

# ============================================================
# 35. COMENTARIO INDIVIDUAL CLASSIFICADO
# ============================================================

def _wcc_emit_classified_comment(record):
    global worker_coach_comments_classified_sent

    classification = record.get(
        "classification",
        {}
    )

    if not classification.get("relevant"):
        return None

    output = {
        "output_id": uuid.uuid4().hex,
        "message_type": "classified_comment",
        "source_worker": "comments",
        "worker_version": WORKER_COACH_COMMENTS_VERSION,
        "platform": record.get("platform"),
        "live_id": record.get("live_id"),
        "subject": record.get("subject"),
        "timestamp": record.get("timestamp"),
        "iso_time": record.get("iso_time"),

        "comment": {
            "event_id": record.get("event_id"),
            "user": record.get("user"),
            "text": record.get("text"),
            "display_time": record.get("display_time"),
        },

        "classification": {
            "relevant": True,
            "is_question": classification.get(
                "is_question",
                False
            ),
            "is_request": classification.get(
                "is_request",
                False
            ),
            "requires_response": classification.get(
                "requires_response",
                False
            ),
            "main_classification": classification.get(
                "main_classification"
            ),
            "intents": classification.get(
                "intents",
                []
            ),
        },
    }

    worker_coach_comments_output_history.append(
        output
    )

    worker_coach_comments_classified_history.append(
        output
    )

    _wcc_queue_output(output)

    worker_coach_comments_classified_sent += 1

    return output

# ============================================================
# 36. EXEMPLOS
# ============================================================

def _wcc_examples(
    records,
    limit=5
):
    examples = []
    seen = set()

    for record in reversed(records):
        text = str(
            record.get("text")
            or ""
        ).strip()

        key = text.lower()

        if (
            text
            and key not in seen
        ):
            examples.append(text)
            seen.add(key)

        if len(examples) >= limit:
            break

    examples.reverse()

    return examples

# ============================================================
# 37. CONTROLE DE REEMISSAO
# ============================================================

def _wcc_can_emit_trend(
    key,
    count,
    now=None
):
    if now is None:
        now = time.time()

    previous = worker_coach_comments_last_trend_emitted.get(
        key
    )

    if previous is None:
        return True

    elapsed = (
        now
        -
        previous.get("time", 0)
    )

    if (
        elapsed
        >= WORKER_COACH_COMMENTS_FORCE_REEMIT_SECONDS
    ):
        return True

    if (
        elapsed
        >= WORKER_COACH_COMMENTS_MIN_REEMIT_SECONDS
        and
        count
        >= previous.get("count", 0) + 2
    ):
        return True

    return False

# ============================================================
# 38. EMITIR TENDENCIA
# ============================================================

def _wcc_emit_trend(
    trend_kind,
    category,
    subtype,
    topic_key,
    records,
    threshold
):
    global worker_coach_comments_trends_emitted

    if not records:
        return None

    count = len(records)

    if count < threshold:
        return None

    unique_users = _wcc_unique_users(
        records
    )

    emit_key = f"{trend_kind}:{topic_key}"
    now = time.time()

    if not _wcc_can_emit_trend(
        emit_key,
        count,
        now
    ):
        return None

    latest = records[-1]
    subtypes_seen = set()

    for record in records:
        classification = record.get(
            "classification",
            {}
        )

        for intent in classification.get(
            "intents",
            []
        ):
            if intent.get("category") == category:
                intent_subtype = intent.get("subtype")

                if intent_subtype:
                    subtypes_seen.add(
                        intent_subtype
                    )

    output = {
        "output_id": uuid.uuid4().hex,
        "message_type": "comment_trend",
        "source_worker": "comments",
        "worker_version": WORKER_COACH_COMMENTS_VERSION,
        "platform": (
            latest.get("platform")
            or worker_coach_comments_platform
        ),
        "live_id": (
            latest.get("live_id")
            or worker_coach_comments_live_id
        ),
        "subject": (
            latest.get("subject")
            or worker_coach_comments_subject
        ),
        "timestamp": now,
        "iso_time": _wcc_now_iso(),
        "trend_kind": trend_kind,
        "category": category,
        "subtype": subtype,
        "topic_key": topic_key,
        "subtypes": sorted(subtypes_seen),
        "window_seconds": WORKER_COACH_COMMENTS_WINDOW,
        "count": count,
        "unique_users": unique_users,
        "strength": _wcc_strength(
            count,
            unique_users,
            threshold
        ),
        "examples": _wcc_examples(records),
    }

    worker_coach_comments_output_history.append(
        output
    )

    worker_coach_comments_trend_history.append(
        output
    )

    _wcc_queue_output(output)

    worker_coach_comments_last_trend_emitted[
        emit_key
    ] = {
        "time": now,
        "count": count,
    }

    worker_coach_comments_trends_emitted += 1

    return output

# ============================================================
# 39. ATUALIZAR TENDENCIAS
# ============================================================

def _wcc_update_trends(record):
    classification = record.get(
        "classification",
        {}
    )

    intents = classification.get(
        "intents",
        []
    )

    if not intents:
        return

    categories_added = set()

    for intent in intents:
        category = intent.get("category")
        subtype = intent.get("subtype")

        if category == "commercial_observation":
            continue

        threshold = WCC_TOPIC_TREND_THRESHOLDS.get(
            category
        )

        if threshold is not None:
            topic_key = _wcc_intent_topic_key(
                intent,
                record
            )

            topic_queue = (
                worker_coach_comments_topic_windows[
                    topic_key
                ]
            )

            topic_queue.append(record)
            _wcc_prune(topic_queue)

            _wcc_emit_trend(
                trend_kind="topic_repeat",
                category=category,
                subtype=subtype,
                topic_key=topic_key,
                records=list(topic_queue),
                threshold=threshold,
            )

        if category in categories_added:
            continue

        categories_added.add(category)

        category_queue = (
            worker_coach_comments_category_windows[
                category
            ]
        )

        category_queue.append(record)
        _wcc_prune(category_queue)

    for category in categories_added:
        threshold = WCC_CATEGORY_VOLUME_THRESHOLDS.get(
            category
        )

        if threshold is None:
            continue

        queue = worker_coach_comments_category_windows[
            category
        ]

        _wcc_prune(queue)

        subtypes = set()

        for item in queue:
            item_classification = item.get(
                "classification",
                {}
            )

            for item_intent in item_classification.get(
                "intents",
                []
            ):
                if item_intent.get("category") == category:
                    item_subtype = item_intent.get("subtype")

                    if item_subtype:
                        subtypes.add(item_subtype)

        if (
            category
            in {
                "product_question",
                "commercial_question",
            }
            and len(subtypes) < 2
        ):
            continue

        _wcc_emit_trend(
            trend_kind="category_volume",
            category=category,
            subtype=None,
            topic_key=f"{category}:__volume__",
            records=list(queue),
            threshold=threshold,
        )

# ============================================================
# 40. DEDUPLICACAO
# ============================================================

def _wcc_event_seen(event):
    event_id = event.get("event_id")

    if not event_id:
        return False

    event_id = str(event_id)

    if event_id in worker_coach_comments_seen_event_ids:
        return True

    if (
        len(worker_coach_comments_seen_event_order)
        >=
        worker_coach_comments_seen_event_order.maxlen
    ):
        old = worker_coach_comments_seen_event_order[0]

        worker_coach_comments_seen_event_ids.discard(
            old
        )

    worker_coach_comments_seen_event_order.append(
        event_id
    )

    worker_coach_comments_seen_event_ids.add(
        event_id
    )

    return False

# ============================================================
# 41. PROCESSAR EVENTO
# ============================================================

def worker_coach_comments_process_event(event):
    global worker_coach_comments_comments_received
    global worker_coach_comments_relevant_comments
    global worker_coach_comments_neutral_comments
    global worker_coach_comments_platform
    global worker_coach_comments_live_id
    global worker_coach_comments_subject

    if not isinstance(event, dict):
        return

    if event.get("type") != "comment":
        return

    if _wcc_event_seen(event):
        return

    payload = event.get("payload") or {}

    user = str(
        payload.get("user")
        or "Usuario"
    ).strip()

    text = str(
        payload.get("text")
        or ""
    ).strip()

    display_time = str(
        payload.get("display_time")
        or ""
    ).strip()

    if not text:
        return

    worker_coach_comments_platform = (
        event.get("platform")
        or worker_coach_comments_platform
    )

    worker_coach_comments_live_id = (
        event.get("live_id")
        or worker_coach_comments_live_id
    )

    worker_coach_comments_subject = (
        event.get("subject")
        or worker_coach_comments_subject
    )

    timestamp = event.get("timestamp")

    try:
        timestamp = float(timestamp)
    except Exception:
        timestamp = time.time()

    classification = _wcc_classify_comment(
        text
    )

    record = {
        "event_id": event.get("event_id"),
        "timestamp": timestamp,
        "iso_time": event.get("iso_time"),
        "platform": event.get("platform"),
        "live_id": event.get("live_id"),
        "subject": event.get("subject"),
        "user": user,
        "text": text,
        "display_time": display_time,
        "normalized": classification.get("normalized"),
        "classification": classification,
    }

    worker_coach_comments_recent.append(
        record
    )

    worker_coach_comments_comments_received += 1

    if classification.get("relevant"):
        worker_coach_comments_relevant_comments += 1

        _wcc_emit_classified_comment(
            record
        )

        _wcc_update_trends(
            record
        )

    else:
        worker_coach_comments_neutral_comments += 1

# ============================================================
# 42. RESET PARA NOVA LIVE
# ============================================================

def worker_coach_comments_reset(platform):
    global worker_coach_comments_output_queue
    global worker_coach_comments_running
    global worker_coach_comments_platform
    global worker_coach_comments_live_id
    global worker_coach_comments_subject
    global worker_coach_comments_started_at
    global worker_coach_comments_comments_received
    global worker_coach_comments_relevant_comments
    global worker_coach_comments_neutral_comments
    global worker_coach_comments_classified_sent
    global worker_coach_comments_trends_emitted
    global worker_coach_comments_dropped_outputs
    global worker_coach_comments_last_error

    worker_coach_comments_output_queue = asyncio.Queue(
        maxsize=WORKER_COACH_COMMENTS_OUTPUT_QUEUE_LIMIT
    )

    worker_coach_comments_recent.clear()
    worker_coach_comments_output_history.clear()
    worker_coach_comments_classified_history.clear()
    worker_coach_comments_trend_history.clear()
    worker_coach_comments_topic_windows.clear()
    worker_coach_comments_category_windows.clear()
    worker_coach_comments_last_trend_emitted.clear()
    worker_coach_comments_seen_event_ids.clear()
    worker_coach_comments_seen_event_order.clear()

    worker_coach_comments_running = False
    worker_coach_comments_platform = platform
    worker_coach_comments_live_id = None
    worker_coach_comments_subject = None
    worker_coach_comments_started_at = time.time()

    worker_coach_comments_comments_received = 0
    worker_coach_comments_relevant_comments = 0
    worker_coach_comments_neutral_comments = 0
    worker_coach_comments_classified_sent = 0
    worker_coach_comments_trends_emitted = 0
    worker_coach_comments_dropped_outputs = 0
    worker_coach_comments_last_error = None

# ============================================================
# 43. LOOP PRINCIPAL
# ============================================================

async def worker_coach_comments_loop():
    global worker_coach_comments_running
    global worker_coach_comments_last_error

    worker_coach_comments_running = True

    try:
        while True:
            event = None

            try:
                event = await live_engine_next_comment_event(
                    timeout=0.5
                )

            except asyncio.TimeoutError:
                event = None

            except asyncio.CancelledError:
                raise

            except Exception as e:
                worker_coach_comments_last_error = (
                    f"{type(e).__name__}: {e}"
                )

                await asyncio.sleep(0.1)

            if event is not None:
                try:
                    worker_coach_comments_process_event(
                        event
                    )

                except Exception as e:
                    worker_coach_comments_last_error = (
                        f"{type(e).__name__}: {e}"
                    )

                continue

            if not live_engine_is_running():
                break

    finally:
        worker_coach_comments_running = False

# ============================================================
# 44. SUPERVISOR
# ============================================================

async def worker_coach_comments_supervisor(platform):
    global worker_coach_comments_last_error

    start = time.time()

    while True:
        if time.time() - start > 30:
            worker_coach_comments_last_error = (
                "Live Engine nao iniciou "
                "dentro de 30 segundos."
            )
            return

        try:
            engine_running = live_engine_is_running()
            engine_platform = live_engine_platform()

            comment_queue = (
                live_engine_channels.get(
                    "coach_comments"
                )
                if "live_engine_channels" in globals()
                else None
            )

        except Exception:
            engine_running = False
            engine_platform = None
            comment_queue = None

        if (
            engine_running
            and engine_platform == platform
            and comment_queue is not None
        ):
            break

        await asyncio.sleep(0.05)

    await worker_coach_comments_loop()

# ============================================================
# 45. SAIDA PARA COMMENT DISPATCHER V1
# ============================================================

async def worker_coach_comments_next_output(
    timeout=None
):
    queue = worker_coach_comments_output_queue

    if queue is None:
        raise RuntimeError(
            "Worker Coach Comentarios "
            "ainda nao iniciou uma LIVE."
        )

    if timeout is None:
        return await queue.get()

    return await asyncio.wait_for(
        queue.get(),
        timeout=timeout
    )

# ============================================================
# 46. COMPATIBILIDADE COM NOME ANTIGO
# ============================================================

async def worker_coach_comments_next_signal(
    timeout=None
):
    return await worker_coach_comments_next_output(
        timeout=timeout
    )

# ============================================================
# 47. CONSULTAS
# ============================================================

def worker_coach_comments_recent_outputs(limit=20):
    if limit <= 0:
        return []

    return list(
        worker_coach_comments_output_history
    )[-limit:]

def worker_coach_comments_recent_classified(limit=20):
    if limit <= 0:
        return []

    return list(
        worker_coach_comments_classified_history
    )[-limit:]

def worker_coach_comments_recent_trends(limit=20):
    if limit <= 0:
        return []

    return list(
        worker_coach_comments_trend_history
    )[-limit:]

# ============================================================
# 48. TESTAR CLASSIFICADOR SEM LIVE
# ============================================================

def worker_coach_comments_classify_text(text):
    result = _wcc_classify_comment(text)

    return {
        "text": str(text),
        "normalized": result.get("normalized"),
        "relevant": result.get("relevant"),
        "is_question": result.get("is_question"),
        "is_request": result.get("is_request"),
        "requires_response": result.get("requires_response"),
        "main_classification": result.get("main_classification"),
        "intents": result.get("intents"),
    }

# ============================================================
# 49. DIAGNOSTICO DE CLASSIFICACAO
# ============================================================

def mostrar_classificacao_worker_coach_comentarios(
    limit=20
):
    print("=" * 76)
    print("CLASSIFICACAO DOS ULTIMOS COMENTARIOS - V1.5")
    print("=" * 76)

    records = list(
        worker_coach_comments_recent
    )[-limit:]

    if not records:
        print(
            "Nenhum comentario recebido ainda."
        )
        return

    for record in records:
        classification = record.get(
            "classification",
            {}
        )

        print()
        print("Usuario:", record.get("user"))
        print("Comentario:", record.get("text"))
        print("Relevante:", classification.get("relevant"))
        print("Pergunta:", classification.get("is_question"))
        print("Pedido:", classification.get("is_request"))
        print(
            "Exige resposta:",
            classification.get("requires_response")
        )
        print(
            "Principal:",
            classification.get("main_classification")
        )
        print(
            "Classificacoes:",
            classification.get("intents")
        )

# ============================================================
# 50. DIAGNOSTICO PRINCIPAL
# ============================================================

def mostrar_worker_coach_comentarios():
    print("=" * 72)
    print("AGCN - WORKER COACH COMENTARIOS V1.5")
    print("=" * 72)

    print(
        "Rodando:",
        worker_coach_comments_running
    )
    print(
        "Plataforma:",
        worker_coach_comments_platform
    )
    print(
        "Live ID:",
        worker_coach_comments_live_id
    )
    print(
        "Subject:",
        worker_coach_comments_subject
    )

    print()
    print("COMENTARIOS")
    print(
        "  Recebidos:",
        worker_coach_comments_comments_received
    )
    print(
        "  Relevantes:",
        worker_coach_comments_relevant_comments
    )
    print(
        "  Neutros/nao classificados:",
        worker_coach_comments_neutral_comments
    )

    print()
    print("SAIDA PARA COMMENT DISPATCHER V1")
    print(
        "  Comentarios classificados enviados:",
        worker_coach_comments_classified_sent
    )
    print(
        "  Tendencias emitidas:",
        worker_coach_comments_trends_emitted
    )
    print(
        "  Outputs descartados por fila cheia:",
        worker_coach_comments_dropped_outputs
    )

    if worker_coach_comments_output_queue is None:
        queue_size = None
    else:
        queue_size = (
            worker_coach_comments_output_queue.qsize()
        )

    print(
        "  Aguardando Comment Dispatcher V1:",
        queue_size
    )

    print()
    print(
        "Erro:",
        worker_coach_comments_last_error
    )

    print()
    print(
        "JANELAS ATUAIS "
        f"(ultimos {WORKER_COACH_COMMENTS_WINDOW}s):"
    )

    now = time.time()

    categories = [
        "product_question",
        "commercial_question",
        "demo_request",
        "buying_intent",
        "purchase_completed",
        "objection",
        "post_purchase_issue",
    ]

    for category in categories:
        queue = worker_coach_comments_category_windows.get(
            category,
            deque()
        )

        _wcc_prune(
            queue,
            now
        )

        print(
            f"  {category}: "
            f"{len(queue)}"
        )

    print()
    print("ULTIMAS TENDENCIAS:")

    trends = list(
        worker_coach_comments_trend_history
    )[-10:]

    if not trends:
        print("  nenhuma tendencia ainda")

    else:
        for trend in trends:
            print()
            print(
                " ",
                trend.get("trend_kind"),
                "|",
                trend.get("category"),
                "|",
                trend.get("subtype"),
            )

            print(
                "    count:",
                trend.get("count"),
                "| users:",
                trend.get("unique_users"),
                "| strength:",
                trend.get("strength"),
            )

            print(
                "    exemplos:",
                trend.get("examples")
            )

# ============================================================
# 51. TESTE INTERNO DA V1.5
#
# Nao precisa de LIVE.
#
# Corpus amplo com:
# - regressao V1.3/V1.4
# - produto
# - comercial
# - demonstracao
# - compra
# - objecoes
# - pos-compra
# - observacoes
# - multi-intencao
# - casos neutros
# ============================================================

def testar_worker_coach_comentarios_v15(verbose=False):
    testes = [
        # REGRESSOES V1.3/V1.4
        {"t":"Mas, é oferta relâmpago. Tem que acelerar!", "e":[("commercial_observation","promotion_mention")]},
        {"t":"Dá cupom", "e":[("commercial_question","promotion_coupon")]},
        {"t":"pode vender Lucas a vontade que tem muito em estoque kkkk", "e":[("commercial_observation","availability_mention")]},
        {"t":"Compreiiii 2kits", "e":[("purchase_completed","reported_purchase")]},
        {"t":"caso nao vou pegar esse kit", "e":[("objection","purchase_hesitation")]},
        {"t":"tem como eu pegar 3 um rosa bebê outro pink e outro lilás", "e":[("commercial_question","variant_selection"),("product_question","color_variant"),("buying_intent","purchase_intent")]},
        {"t":"é bug?", "e":[], "q":True, "exact":True},
        {"t":"qual o tamanho do cabo?", "e":[("product_question","dimensions")]},
        {"t":"é bivolt?", "e":[("product_question","power_voltage")]},
        {"t":"33 dá certo, né?", "e":[("product_question","size_fit")]},
        {"t":"como fazer para compra", "e":[("commercial_question","purchase_path")]},
        {"t":"eu ia comprar mas não tem meu número??", "e":[("commercial_question","availability"),("objection","purchase_hesitation")]},
        {"t":"e pra que que serve essa coisa ai?", "e":[("product_question","functionality")], "f":[("product_question","size_fit")]},
        {"t":"pra que serve isso?", "e":[("product_question","functionality")], "f":[("product_question","size_fit")]},
        {"t":"isso serve pra quê?", "e":[("product_question","functionality")], "f":[("product_question","size_fit")]},
        {"t":"como isso funciona?", "e":[("product_question","functionality")]},
        {"t":"eu calço 37, serve?", "e":[("product_question","size_fit")], "f":[("product_question","functionality")]},
        {"t":"serve pra quem veste 42?", "e":[("product_question","size_fit")]},
        {"t":"serve no iphone?", "e":[("product_question","compatibility")], "f":[("product_question","size_fit")]},
        {"t":"funciona no android?", "e":[("product_question","compatibility")]},

        # COMERCIAL
        {"t":"qual o valor?", "e":[("commercial_question","price")]},
        {"t":"preço?", "e":[("commercial_question","price")]},
        {"t":"quanto fica no carrinho?", "e":[("commercial_question","price"),("commercial_question","purchase_path")]},
        {"t":"tem cupom?", "e":[("commercial_question","promotion_coupon")]},
        {"t":"qual código do cupom?", "e":[("commercial_question","promotion_coupon")]},
        {"t":"o cupom não pega", "e":[("commercial_question","promotion_problem")]},
        {"t":"não tá aplicando o desconto", "e":[("commercial_question","promotion_problem")]},
        {"t":"quanto é o frete?", "e":[("commercial_question","shipping")]},
        {"t":"envia pro Tocantins?", "e":[("commercial_question","shipping")]},
        {"t":"chega quando?", "e":[("commercial_question","delivery_time")]},
        {"t":"quantos dias pra chegar?", "e":[("commercial_question","delivery_time")]},
        {"t":"ainda tem?", "e":[("commercial_question","availability")]},
        {"t":"tem o preto?", "e":[("commercial_question","availability"),("product_question","color_variant")]},
        {"t":"tem M?", "e":[("commercial_question","availability")]},
        {"t":"quando volta estoque?", "e":[("commercial_question","availability")]},
        {"t":"onde compra?", "e":[("commercial_question","purchase_path")]},
        {"t":"qual item na sacola?", "e":[("commercial_question","purchase_path")]},
        {"t":"aceita pix?", "e":[("commercial_question","payment_method")]},
        {"t":"como paga?", "e":[("commercial_question","payment_method")]},
        {"t":"parcela?", "e":[("commercial_question","installments")]},
        {"t":"quantas vezes sem juros?", "e":[("commercial_question","installments")]},
        {"t":"tem nota fiscal? Bom dia", "e":[("commercial_question","invoice_fiscal_document")]},
        {"t":"vem com nota?", "e":[("commercial_question","invoice_fiscal_document")]},
        {"t":"emite NF-e?", "e":[("commercial_question","invoice_fiscal_document")]},
        {"t":"quero um rosa e um preto", "e":[("commercial_question","variant_selection"),("product_question","color_variant"),("buying_intent","purchase_intent")]},

        # PRODUTO - FUNCAO / USO / TAMANHO / MEDIDAS
        {"t":"qual a função disso?", "e":[("product_question","functionality")]},
        {"t":"o que esse produto faz?", "e":[("product_question","functionality")]},
        {"t":"como usa?", "e":[("product_question","usage_method")]},
        {"t":"como aplica esse sérum?", "e":[("product_question","usage_method")]},
        {"t":"como monta?", "e":[("product_question","usage_method")]},
        {"t":"veste M?", "e":[("product_question","size_fit")]},
        {"t":"uso 38, será que serve?", "e":[("product_question","size_fit")]},
        {"t":"qual manequim veste?", "e":[("product_question","size_fit")]},
        {"t":"quanto mede?", "e":[("product_question","dimensions")]},
        {"t":"qual o comprimento da alça?", "e":[("product_question","dimensions")]},
        {"t":"qual tamanho da tela?", "e":[("product_question","dimensions")]},

        # PRODUTO - COR / MATERIAL / CAPACIDADE / PESO / KIT / INCLUSOS
        {"t":"tem bege?", "e":[("product_question","color_variant"),("commercial_question","availability")]},
        {"t":"qual é esse rosa?", "e":[("product_question","color_variant")]},
        {"t":"qual tecido?", "e":[("product_question","material_composition")]},
        {"t":"é inox?", "e":[("product_question","material_composition")]},
        {"t":"quantos litros?", "e":[("product_question","capacity")]},
        {"t":"cabe notebook?", "e":[("product_question","capacity")]},
        {"t":"quanto pesa?", "e":[("product_question","weight")]},
        {"t":"é pesado?", "e":[("product_question","weight")]},
        {"t":"kit vem com quantos?", "e":[("product_question","quantity_package")]},
        {"t":"esse preço é do kit ou de um?", "e":[("commercial_question","price"),("product_question","quantity_package")]},
        {"t":"vem carregador?", "e":[("product_question","included_items")]},
        {"t":"o que vem na caixa?", "e":[("product_question","included_items")]},

        # PRODUTO - GARANTIA / AUTENTICIDADE / CONDICAO / CERTIFICACAO
        {"t":"quanto tempo de garantia no tablet??", "e":[("product_question","warranty")]},
        {"t":"tem garantia do fabricante?", "e":[("product_question","warranty")]},

        # PRODUTO - MARCA / FABRICANTE
        {"t":"esse colchão é da ortobom?", "e":[("product_question","brand_manufacturer")]},
        {"t":"qual a marca?", "e":[("product_question","brand_manufacturer")]},
        {"t":"de qual marca é?", "e":[("product_question","brand_manufacturer")]},
        {"t":"qual o fabricante?", "e":[("product_question","brand_manufacturer")]},
        {"t":"quem fabrica?", "e":[("product_question","brand_manufacturer")]},
        {"t":"essa é da Wap?", "e":[("product_question","brand_manufacturer")]},

        {"t":"é original?", "e":[("product_question","authenticity")]},
        {"t":"não é réplica?", "e":[("product_question","authenticity")]},
        {"t":"é novo?", "e":[("product_question","product_condition")]},
        {"t":"é recondicionado?", "e":[("product_question","product_condition")]},
        {"t":"vem lacrado?", "e":[("product_question","product_condition")]},
        {"t":"tem Anatel?", "e":[("product_question","certification_compliance")]},
        {"t":"tem selo do Inmetro?", "e":[("product_question","certification_compliance")]},

        # ELETRONICOS
        {"t":"é compatível com iPhone 15?", "e":[("product_question","compatibility")]},
        {"t":"funciona com Alexa?", "e":[("product_question","compatibility")]},
        {"t":"é bivolt?", "e":[("product_question","power_voltage")]},
        {"t":"quantos watts?", "e":[("product_question","power_voltage")]},
        {"t":"quanto dura a bateria?", "e":[("product_question","battery_duration")]},
        {"t":"carrega por USB-C?", "e":[("product_question","battery_duration")]},
        {"t":"tem bluetooth?", "e":[("product_question","connectivity")]},
        {"t":"pega 5G?", "e":[("product_question","connectivity")]},
        {"t":"tem NFC?", "e":[("product_question","connectivity")]},
        {"t":"é 128gb?", "e":[("product_question","technical_specification")]},
        {"t":"quanto de RAM?", "e":[("product_question","technical_specification")]},
        {"t":"qual processador?", "e":[("product_question","technical_specification")]},

        # RESISTENCIA / MODA / BELEZA / ALIMENTOS
        {"t":"é à prova d'água?", "e":[("product_question","usage_resistance")]},
        {"t":"pode ir no micro-ondas?", "e":[("product_question","usage_resistance")]},
        {"t":"qual cheiro?", "e":[("product_question","sensory_characteristic")]},
        {"t":"é amadeirado?", "e":[("product_question","sensory_characteristic")]},
        {"t":"qual textura?", "e":[("product_question","sensory_characteristic")]},
        {"t":"tem parabeno?", "e":[("product_question","ingredients_composition")]},
        {"t":"tem glúten?", "e":[("product_question","ingredients_composition")]},
        {"t":"qual validade?", "e":[("product_question","expiration_validity")]},
        {"t":"vence quando?", "e":[("product_question","expiration_validity")]},
        {"t":"grávida pode usar?", "e":[("product_question","safety_suitability")]},
        {"t":"serve pra pele oleosa?", "e":[("product_question","safety_suitability")], "f":[("product_question","size_fit")]},
        {"t":"estica?", "e":[("product_question","apparel_characteristic")]},
        {"t":"tem forro?", "e":[("product_question","apparel_characteristic")]},
        {"t":"quantas calorias?", "e":[("product_question","food_characteristic")]},
        {"t":"precisa gelar?", "e":[("product_question","food_characteristic")]},
        {"t":"qual a diferença desse pro outro?", "e":[("product_question","comparison")]},
        {"t":"qual é melhor?", "e":[("product_question","comparison")]},

        # DEMONSTRACAO
        {"t":"mostra de perto", "e":[("demo_request","show_demonstrate")]},
        {"t":"mostra o tecido", "e":[("demo_request","show_demonstrate")]},
        {"t":"abre a caixa", "e":[("demo_request","show_demonstrate")]},
        {"t":"testa o som", "e":[("demo_request","show_demonstrate")]},
        {"t":"compara o rosa e o pink", "e":[("demo_request","show_demonstrate"),("product_question","color_variant")]},

        # COMPRA / INTENCAO / CONDICIONAL
        {"t":"vou comprar", "e":[("buying_intent","purchase_intent")]},
        {"t":"quero o rosa", "e":[("buying_intent","purchase_intent")]},
        {"t":"se tiver M eu compro", "e":[("buying_intent","conditional_purchase_intent"),("commercial_question","availability")]},
        {"t":"se tiver frete grátis eu pego", "e":[("buying_intent","conditional_purchase_intent"),("commercial_question","shipping")]},
        {"t":"já comprei", "e":[("purchase_completed","reported_purchase")]},
        {"t":"pedido feito", "e":[("purchase_completed","reported_purchase")]},
        {"t":"pix feito", "e":[("purchase_completed","reported_payment")]},
        {"t":"paguei", "e":[("purchase_completed","reported_payment")]},

        # OBJECOES
        {"t":"tá muito caro", "e":[("objection","price_objection")]},
        {"t":"achei mais barato", "e":[("objection","price_objection")]},
        {"t":"frete tá caro", "e":[("objection","shipping_objection")]},
        {"t":"tenho medo de ser golpe", "e":[("objection","trust_objection")]},
        {"t":"essa loja é confiável?", "e":[("objection","trust_objection")]},
        {"t":"parece falso", "e":[("objection","authenticity_objection")]},
        {"t":"material parece ruim", "e":[("objection","quality_objection")]},
        {"t":"tenho medo de não servir", "e":[("objection","size_objection")]},
        {"t":"tenho medo de não funcionar no meu celular", "e":[("objection","compatibility_objection")]},
        {"t":"vai demorar demais", "e":[("objection","delivery_time_objection")]},
        {"t":"essa promoção é real?", "e":[("objection","promotion_objection")]},
        {"t":"vou pensar", "e":[("objection","purchase_hesitation")]},

        # POS-COMPRA
        {"t":"cadê meu pedido?", "e":[("post_purchase_issue","order_status")]},
        {"t":"rastreio não atualiza", "e":[("post_purchase_issue","order_status")]},
        {"t":"passou do prazo", "e":[("post_purchase_issue","shipping_delay")]},
        {"t":"meu pedido não chegou", "e":[("post_purchase_issue","not_received")]},
        {"t":"consta entregue mas não recebi", "e":[("post_purchase_issue","delivered_not_received")]},
        {"t":"veio outro produto", "e":[("post_purchase_issue","wrong_item")]},
        {"t":"mandaram a cor errada", "e":[("post_purchase_issue","wrong_variant")]},
        {"t":"kit veio incompleto", "e":[("post_purchase_issue","missing_item")]},
        {"t":"veio quebrado", "e":[("post_purchase_issue","damaged_item")]},
        {"t":"veio com defeito", "e":[("post_purchase_issue","defective_item")]},
        {"t":"não é igual ao anúncio", "e":[("post_purchase_issue","not_as_described")]},
        {"t":"quero trocar o tamanho", "e":[("post_purchase_issue","exchange")]},
        {"t":"quero devolver", "e":[("post_purchase_issue","return")]},
        {"t":"quero reembolso", "e":[("post_purchase_issue","refund")]},
        {"t":"quero cancelar", "e":[("post_purchase_issue","cancellation")]},
        {"t":"cobrou duas vezes", "e":[("post_purchase_issue","payment_charge_problem")]},
        {"t":"não veio a nota fiscal", "e":[("post_purchase_issue","invoice_missing")], "f":[("commercial_question","invoice_fiscal_document")]},
        {"t":"onde aciono a garantia?", "e":[("post_purchase_issue","warranty_claim")], "f":[("product_question","warranty")]},

        # OBSERVACOES / CONTEXTO
        {"t":"o preço caiu para R$ 39,90", "e":[("commercial_observation","price_mention")]},
        {"t":"voltou estoque", "e":[("commercial_observation","availability_mention")]},
        {"t":"o frete apareceu grátis", "e":[("commercial_observation","shipping_mention")]},
        {"t":"O iPad rosa 128gb", "e":[("commercial_observation","contextual_product_reference")]},

        # MULTI-INTENCAO
        {"t":"tem o rosa M e quanto fica com cupom e frete?", "e":[("product_question","color_variant"),("commercial_question","availability"),("commercial_question","price"),("commercial_question","promotion_coupon"),("commercial_question","shipping")]},
        {"t":"serve no iPhone e vem carregador?", "e":[("product_question","compatibility"),("product_question","included_items")]},
        {"t":"é original e vem com nota?", "e":[("product_question","authenticity"),("commercial_question","invoice_fiscal_document")]},
        {"t":"é bivolt e tem garantia?", "e":[("product_question","power_voltage"),("product_question","warranty")]},
        {"t":"tá caro mas se liberar cupom eu pego", "e":[("objection","price_objection"),("commercial_question","promotion_coupon"),("buying_intent","conditional_purchase_intent")]},

        # NEUTROS
        {"t":"bom dia", "e":[], "exact":True},
        {"t":"kkkk", "e":[], "exact":True},
        {"t":"obrigada", "e":[], "exact":True},
        {"t":"linda", "e":[], "exact":True},
    ]

    print("=" * 76)
    print("TESTE INTERNO - WORKER COACH COMENTARIOS V1.5")
    print("=" * 76)

    aprovados = 0
    falhas = []

    for caso in testes:
        texto = caso["t"]
        esperados = caso.get("e", [])
        proibidos = caso.get("f", [])
        exact = caso.get("exact", False)
        expected_question = caso.get("q", None)

        resultado = worker_coach_comments_classify_text(texto)
        obtidos = [
            (i.get("category"), i.get("subtype"))
            for i in resultado.get("intents", [])
        ]

        passou = all(item in obtidos for item in esperados)
        passou = passou and all(item not in obtidos for item in proibidos)

        if exact:
            passou = passou and set(obtidos) == set(esperados)

        if expected_question is not None:
            passou = passou and resultado.get("is_question") is expected_question

        if passou:
            aprovados += 1
            if verbose:
                print("OK |", texto, "=>", obtidos)
        else:
            falhas.append({
                "text": texto,
                "expected": esperados,
                "forbidden": proibidos,
                "obtained": obtidos,
                "question": resultado.get("is_question"),
                "request": resultado.get("is_request"),
                "main": resultado.get("main_classification"),
            })
            print()
            print("FALHOU:", texto)
            print("  Esperado:", esperados)
            print("  Proibido:", proibidos)
            print("  Obtido:", obtidos)
            print("  Pergunta:", resultado.get("is_question"))
            print("  Pedido:", resultado.get("is_request"))
            print("  Principal:", resultado.get("main_classification"))

    print()
    print("=" * 76)
    print("RESUMO DO TESTE V1.5")
    print("=" * 76)
    print("Aprovados:", aprovados)
    print("Falhas:", len(falhas))
    print("Total:", len(testes))

    return {
        "approved": aprovados,
        "failed": len(falhas),
        "total": len(testes),
        "ok": len(falhas) == 0,
        "failures": falhas,
    }

# ============================================================
# 52. FINALIZAR TASK
# ============================================================

async def _worker_comments_finish_task(task):
    if task is None:
        return

    try:
        await asyncio.wait_for(
            asyncio.shield(task),
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
# 53. DESCOBRIR FUNCOES BASE
#
# Mantem a estrategia validada da V1.3.
#
# Evita empilhar uma versao anterior do proprio Worker
# Comentarios quando esta celula e executada novamente na
# ordem correta do Colab.
#
# IMPORTANTE:
# se Dispatcher / Coach Produto / Worker Audiencia ja tiverem
# sido carregados, reinicie o runtime e execute novamente os
# modulos na ordem oficial. Nao fazemos cirurgia dinamica nos
# wrappers posteriores nesta V1.5.
# ============================================================

_current_comments_shopee_executor = (
    executar_shopee_com_live_engine
)

if getattr(
    _current_comments_shopee_executor,
    "_agcn_comments_worker_wrapper",
    False
):
    _worker_comments_base_shopee = (
        _current_comments_shopee_executor
        ._agcn_comments_worker_base
    )
else:
    _worker_comments_base_shopee = (
        _current_comments_shopee_executor
    )

_current_comments_tiktok_executor = (
    executar_tiktok_com_live_engine
)

if getattr(
    _current_comments_tiktok_executor,
    "_agcn_comments_worker_wrapper",
    False
):
    _worker_comments_base_tiktok = (
        _current_comments_tiktok_executor
        ._agcn_comments_worker_base
    )
else:
    _worker_comments_base_tiktok = (
        _current_comments_tiktok_executor
    )

# ============================================================
# 54. SHOPEE
# ============================================================

async def executar_shopee_com_live_engine():
    worker_coach_comments_reset(
        "shopee"
    )

    comments_task = (
        asyncio.create_task(
            worker_coach_comments_supervisor(
                "shopee"
            )
        )
    )

    try:
        return await (
            _worker_comments_base_shopee()
        )

    finally:
        await _worker_comments_finish_task(
            comments_task
        )

executar_shopee_com_live_engine._agcn_comments_worker_wrapper = True
executar_shopee_com_live_engine._agcn_comments_worker_base = (
    _worker_comments_base_shopee
)
executar_shopee_com_live_engine._agcn_comments_worker_version = (
    WORKER_COACH_COMMENTS_VERSION
)

# ============================================================
# 55. TIKTOK
# ============================================================

async def executar_tiktok_com_live_engine(
    username
):
    worker_coach_comments_reset(
        "tiktok"
    )

    comments_task = (
        asyncio.create_task(
            worker_coach_comments_supervisor(
                "tiktok"
            )
        )
    )

    try:
        return await (
            _worker_comments_base_tiktok(
                username
            )
        )

    finally:
        await _worker_comments_finish_task(
            comments_task
        )

executar_tiktok_com_live_engine._agcn_comments_worker_wrapper = True
executar_tiktok_com_live_engine._agcn_comments_worker_base = (
    _worker_comments_base_tiktok
)
executar_tiktok_com_live_engine._agcn_comments_worker_version = (
    WORKER_COACH_COMMENTS_VERSION
)

# ============================================================
# 56. PRONTO
# ============================================================

print(
    "AGCN Worker Coach Comentarios V1.5 carregado."
)

print(
    "Entrada: comentarios normalizados do Live Engine V2."
)

print("Saidas:")
print("1. classified_comment")
print("2. comment_trend")

print(
    "Perguntas, pedidos, observacoes e intencoes sao separados."
)

print(
    "V1.5: taxonomia ampliada de produto, comercial, compra, "
    "objecoes, pos-compra e contexto, preservando a correcao V1.4."
)

print(
    "Prioridade e fala final continuam sendo responsabilidade "
    "das camadas posteriores."
)

print(
    "Nenhuma comunicacao direta com Interface ou Armazenamento Coach."
)
