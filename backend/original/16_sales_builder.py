# ============================================================
# AGCN LIVE - PRODUCT SALES BUILDER V1.0
#
# FLUXO:
# Worker Shopee Produto -> Product Extractor -> Product Sales Builder
# -> futuro Sales Decision Coach -> SALES COACH
#
# RESPONSABILIDADE:
# - recebe Product Profile do Product Extractor;
# - identifica oportunidades comerciais seguras;
# - cria blocos de fala/acao para LIVE commerce;
# - cria versao Equilibrado e Pressao / Feira;
# - preserva evidencia e origem de cada orientacao;
# - bloqueia afirmacoes sem base.
#
# NAO FAZ:
# - tempo / cooldown / duracao / ordem final;
# - interface;
# - leitura de comentarios;
# - alteracao de Worker, Live Engine ou Coaches atuais;
# - falsa urgencia, falsa escassez ou promessas sem prova.
# ============================================================

import asyncio
import copy
import hashlib
import json
import re
import time
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo


# ============================================================
# 1. CONFIGURACOES
# ============================================================

PRODUCT_SALES_BUILDER_VERSION = "1.0"
PRODUCT_SALES_BUNDLE_SCHEMA_VERSION = "1.0"
PRODUCT_SALES_TIMEZONE = ZoneInfo("America/Araguaina")
PRODUCT_SALES_HISTORY_MAXLEN = 200

STYLE_BALANCED = "equilibrado"
STYLE_PRESSURE = "pressao_feira"
ALLOWED_STYLES = {STYLE_BALANCED, STYLE_PRESSURE}

IMPORTANCE = {
    "presentation": 78,
    "characteristic": 70,
    "benefit": 82,
    "demonstration": 88,
    "usage": 80,
    "differential": 72,
    "price_value": 90,
    "social_proof": 76,
    "promotion": 92,
    "availability": 74,
    "preventive_objection": 84,
    "reinforcement": 68,
    "cta": 86,
    "transition": 60,
    "warning": 96,
}


# ============================================================
# 2. ESTADO / FILA / HISTORICO
# ============================================================

product_sales_builder_output_queue = asyncio.Queue()
product_sales_builder_history = deque(maxlen=PRODUCT_SALES_HISTORY_MAXLEN)

product_sales_builder_state = {
    "version": PRODUCT_SALES_BUILDER_VERSION,
    "schema_version": PRODUCT_SALES_BUNDLE_SCHEMA_VERSION,
    "running": False,
    "style": STYLE_BALANCED,
    "processed_profiles": 0,
    "bundles_started": 0,
    "bundles_changed": 0,
    "bundles_updated": 0,
    "bundles_cleared": 0,
    "style_updates": 0,
    "current_key": None,
    "current_profile": None,
    "current_bundle": None,
    "previous_bundle": None,
    "last_input_at": None,
    "last_output_at": None,
    "last_error": None,
}

_stop_requested = False
_output_seq = 0


# ============================================================
# 3. UTILITARIOS
# ============================================================

def _now_iso():
    return datetime.now(PRODUCT_SALES_TIMEZONE).isoformat()


def _copy(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _known(fact):
    return (
        isinstance(fact, dict)
        and fact.get("known") is True
        and fact.get("value") is not None
    )


def _fact_value(fact):
    return fact.get("value") if _known(fact) else None


def _fact_source(fact):
    return fact.get("source") if _known(fact) else None


def _numeric(fact):
    value = _fact_value(fact)
    if isinstance(value, dict):
        value = value.get("normalized")
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _text(value, max_len=220):
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
    elif isinstance(value, bool):
        text = "sim" if value else "nao"
    elif isinstance(value, (int, float)):
        text = str(value)
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, sort_keys=True)
        except Exception:
            text = str(value)
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return text


def _norm(value):
    text = (_text(value, 1000) or "").lower()
    table = str.maketrans({
        "Ã¡": "a", "Ã ": "a", "Ã¢": "a", "Ã£": "a", "Ã¤": "a",
        "Ã©": "e", "Ã¨": "e", "Ãª": "e", "Ã«": "e",
        "Ã­": "i", "Ã¬": "i", "Ã®": "i", "Ã¯": "i",
        "Ã³": "o", "Ã²": "o", "Ã´": "o", "Ãµ": "o", "Ã¶": "o",
        "Ãº": "u", "Ã¹": "u", "Ã»": "u", "Ã¼": "u", "Ã§": "c",
    })
    return re.sub(r"\s+", " ", text.translate(table)).strip()


def _human_number(value, decimals=0):
    if value is None:
        return None
    number = float(value)
    if decimals <= 0:
        return f"{int(round(number)):,}".replace(",", ".")
    result = f"{number:,.{decimals}f}"
    return result.replace(",", "X").replace(".", ",").replace("X", ".")


def _currency(profile):
    fact = ((profile.get("price") or {}).get("currency") or {})
    value = _fact_value(fact)
    return str(value).upper() if value else "BRL"


def _money(value, currency="BRL"):
    if value is None:
        return None
    if str(currency).upper() == "BRL":
        return "R$ " + _human_number(value, 2)
    return f"{currency} {_human_number(value, 2)}"


def _product_name(profile):
    fact = ((profile.get("product") or {}).get("name") or {})
    return _text(_fact_value(fact), 180) or "produto"


def _product_key(profile):
    identity = profile.get("identity") or {}
    key = identity.get("key")
    if key:
        return tuple(str(x) for x in key)
    platform = identity.get("platform")
    shop_id = identity.get("shop_id")
    item_id = identity.get("item_id")
    if platform is None or shop_id is None or item_id is None:
        return None
    return (str(platform), str(shop_id), str(item_id))


def _evidence(path, fact=None, value=None, source=None, note=None):
    if fact is not None:
        value = _fact_value(fact)
        source = _fact_source(fact)
    return {
        "path": path,
        "value": _copy(value),
        "source": source,
        "note": note,
    }


def _candidate_id(profile, category, topic, evidence):
    paths = sorted(
        str(x.get("path"))
        for x in evidence
        if isinstance(x, dict) and x.get("path")
    )
    base = "|".join([
        "|".join(_product_key(profile) or ("unknown",)),
        category,
        topic,
        "|".join(paths),
    ])
    return f"sales_{category}_{hashlib.sha1(base.encode('utf-8')).hexdigest()[:14]}"


def _candidate(
    profile,
    category,
    topic,
    objective,
    action,
    balanced,
    pressure,
    evidence=None,
    importance=None,
    risk="low",
    can_repeat=False,
    repeat_group=None,
    restrictions=None,
    tags=None,
):
    evidence = list(evidence or [])
    style = product_sales_builder_state.get("style") or STYLE_BALANCED
    texts = {STYLE_BALANCED: balanced, STYLE_PRESSURE: pressure}
    return {
        "candidate_id": _candidate_id(profile, category, topic, evidence),
        "category": category,
        "topic_key": topic,
        "objective": objective,
        "action": action,
        "importance_hint": int(importance if importance is not None else IMPORTANCE.get(category, 60)),
        "risk_level": risk,
        "can_repeat": bool(can_repeat),
        "repeat_group": repeat_group or topic,
        "evidence": _copy(evidence),
        "evidence_paths": [e.get("path") for e in evidence if e.get("path")],
        "restrictions": list(restrictions or []),
        "tags": list(tags or []),
        "texts": texts,
        "selected_style": style,
        "selected_text": texts.get(style) or balanced,
    }


def _dedupe(candidates):
    out = []
    ids = set()
    signatures = set()
    for item in candidates:
        cid = item.get("candidate_id")
        sig = (item.get("category"), _norm((item.get("texts") or {}).get(STYLE_BALANCED)))
        if cid in ids or sig in signatures:
            continue
        ids.add(cid)
        signatures.add(sig)
        out.append(item)
    return out


# ============================================================
# 4. FATOS TEXTUAIS DISPONIVEIS
# ============================================================

def _collect_text_facts(profile):
    out = []
    product = profile.get("product") or {}
    for field in ("name", "title_text", "brand"):
        fact = product.get(field) or {}
        if _known(fact):
            out.append({
                "path": f"product.{field}",
                "value": _fact_value(fact),
                "source": _fact_source(fact),
                "field": field,
            })

    chars = profile.get("characteristics") or {}
    structured = chars.get("structured_shopee") or {}
    for key, fact in structured.items():
        if _known(fact):
            out.append({
                "path": f"characteristics.structured_shopee.{key}",
                "value": _fact_value(fact),
                "source": _fact_source(fact),
                "field": key,
            })

    seller_facts = chars.get("seller_facts") or {}
    for key, fact in seller_facts.items():
        if _known(fact):
            out.append({
                "path": f"characteristics.seller_facts.{key}",
                "value": _fact_value(fact),
                "source": _fact_source(fact),
                "field": key,
            })

    notes = chars.get("seller_notes") or {}
    if _known(notes):
        out.append({
            "path": "characteristics.seller_notes",
            "value": _fact_value(notes),
            "source": _fact_source(notes),
            "field": "seller_notes",
        })
    return out


def _find_keyword_evidence(profile, keywords):
    out = []
    normalized_keywords = [_norm(x) for x in keywords]
    for item in _collect_text_facts(profile):
        haystack = _norm(item.get("value"))
        if any(k and k in haystack for k in normalized_keywords):
            out.append(_evidence(
                item.get("path"),
                value=item.get("value"),
                source=item.get("source"),
                note="Fato textual explicito usado pelo Builder.",
            ))
    return out


# ============================================================
# 5. APRESENTACAO
# ============================================================

def _build_presentation(profile):
    name = _product_name(profile)
    name_fact = ((profile.get("product") or {}).get("name") or {})
    evidence = [_evidence("product.name", fact=name_fact)] if _known(name_fact) else []
    return [_candidate(
        profile,
        "presentation",
        "introduce_product",
        "Apresentar o produto com clareza e iniciar a fala.",
        "Apresentar o produto e situar rapidamente o publico.",
        f"Apresente o {name} de forma direta e explique o que ele e usando apenas informacoes confirmadas.",
        f"Ja entra falando do {name}: mostra o produto e explica rapido o que ele e, sem deixar a LIVE parar.",
        evidence=evidence,
        importance=78,
        can_repeat=False,
        restrictions=["Nao inventar funcao, beneficio ou qualidade nao confirmada."],
        tags=["opening", "continuous_selling"],
    )]


# ============================================================
# 6. CARACTERISTICAS
# ============================================================

def _build_characteristics(profile):
    out = []
    chars = profile.get("characteristics") or {}
    structured = chars.get("structured_shopee") or {}
    labels = {
        "material": "material",
        "color": "cor",
        "size": "tamanho",
        "model": "modelo",
        "variation": "variacoes",
        "attributes": "atributos",
        "specifications": "especificacoes",
        "description": "descricao",
    }

    for key, fact in structured.items():
        if not _known(fact):
            continue
        value = _text(_fact_value(fact))
        if not value:
            continue
        label = labels.get(key, key)
        out.append(_candidate(
            profile,
            "characteristic",
            f"shopee_{key}",
            "Explorar uma caracteristica objetiva do produto.",
            f"Explicar a caracteristica: {label}.",
            f"Fale do {label}: {value}. Mostre esse detalhe enquanto explica, se ele estiver visivel no produto.",
            f"Puxa agora o {label}: {value}. Mostra esse detalhe na camera e reforca isso na apresentacao.",
            evidence=[_evidence(f"characteristics.structured_shopee.{key}", fact=fact)],
            can_repeat=True,
            repeat_group=f"characteristic_{key}",
            restrictions=["Nao transformar a caracteristica em promessa nao comprovada."],
        ))

    seller_facts = chars.get("seller_facts") or {}
    for key, fact in seller_facts.items():
        if not _known(fact):
            continue
        value = _text(_fact_value(fact))
        if not value:
            continue
        out.append(_candidate(
            profile,
            "characteristic",
            f"seller_{key}",
            "Usar uma informacao adicional confirmada pelo vendedor.",
            f"Apresentar a informacao '{key}'.",
            f"Inclua esta informacao na apresentacao: {key}: {value}.",
            f"Reforca agora essa informacao: {key}: {value}. Nao deixa esse ponto passar.",
            evidence=[_evidence(f"characteristics.seller_facts.{key}", fact=fact)],
            can_repeat=True,
            repeat_group=f"seller_fact_{key}",
            restrictions=["Nao ampliar a afirmacao alem do texto informado pelo vendedor."],
            tags=["seller_input"],
        ))

    notes = chars.get("seller_notes") or {}
    if _known(notes):
        note = _text(_fact_value(notes), 280)
        if note:
            out.append(_candidate(
                profile,
                "characteristic",
                "seller_notes",
                "Usar informacoes adicionais fornecidas pelo vendedor.",
                "Trazer a observacao adicional para a fala.",
                f"Inclua na explicacao esta informacao adicional: {note}",
                f"Traz essa informacao para a fala agora: {note}",
                evidence=[_evidence("characteristics.seller_notes", fact=notes)],
                can_repeat=True,
                repeat_group="seller_notes",
                restrictions=["Repetir apenas o que foi fornecido; nao adicionar conclusoes."],
                tags=["seller_input"],
            ))
    return out


# ============================================================
# 7. BENEFICIOS CONSERVADORES
# ============================================================

SAFE_BENEFIT_RULES = [
    {
        "keywords": ["portatil", "compacto", "compacta", "leve"],
        "topic": "portability",
        "action": "Destacar praticidade para transportar e guardar.",
        "balanced": "Transforme essa caracteristica em beneficio: destaque a praticidade para transportar e guardar.",
        "pressure": "Puxa a praticidade agora: reforca como o formato facilita transportar e guardar, e mostra isso na camera.",
    },
    {
        "keywords": ["divisoria", "divisorias", "compartimento", "compartimentos", "organizador", "organizadora"],
        "topic": "organization",
        "action": "Mostrar como a estrutura ajuda a separar e organizar itens.",
        "balanced": "Mostre a organizacao na pratica e destaque como os compartimentos ajudam a separar os itens.",
        "pressure": "Abre e mostra os espacos agora. Reforca como isso ajuda a separar e organizar os itens.",
    },
    {
        "keywords": ["dobravel"],
        "topic": "foldable_storage",
        "action": "Demonstrar a praticidade de guardar quando dobrado.",
        "balanced": "Mostre como ele dobra e destaque a praticidade de guardar dessa forma.",
        "pressure": "Dobra ele na camera agora e mostra como fica mais pratico para guardar.",
    },
    {
        "keywords": ["ajustavel"],
        "topic": "adjustability",
        "action": "Mostrar o ajuste conforme a necessidade.",
        "balanced": "Demonstre o ajuste e explique como ele permite adaptar o produto conforme a necessidade.",
        "pressure": "Mostra o ajuste agora e reforca que da para adaptar conforme a necessidade.",
    },
    {
        "keywords": ["lavavel"],
        "topic": "washable",
        "action": "Destacar a praticidade de limpeza.",
        "balanced": "Se a informacao 'lavavel' estiver confirmada, destaque a praticidade de limpeza.",
        "pressure": "Reforca que e lavavel e puxa a praticidade de limpeza, sem prometer resistencia alem do que esta informado.",
    },
    {
        "keywords": ["removivel"],
        "topic": "removable",
        "action": "Mostrar a parte removivel e como ela se encaixa no uso.",
        "balanced": "Mostre a parte removivel e explique em que momento ela pode ser retirada.",
        "pressure": "Tira a parte removivel na camera e mostra como funciona.",
    },
    {
        "keywords": ["transparente"],
        "topic": "visibility",
        "action": "Mostrar a visualizacao do conteudo.",
        "balanced": "Mostre a parte transparente e destaque a facilidade de visualizar o conteudo.",
        "pressure": "Mostra a transparencia agora e reforca como da para ver o conteudo com facilidade.",
    },
    {
        "keywords": ["antiderrapante"],
        "topic": "grip",
        "action": "Mostrar a superficie antiderrapante.",
        "balanced": "Demonstre a superficie antiderrapante e explique que ela ajuda a reduzir o escorregamento.",
        "pressure": "Mostra a parte antiderrapante e reforca esse ponto na pratica.",
    },
    {
        "keywords": ["recarregavel"],
        "topic": "rechargeable",
        "action": "Explicar o carregamento e o fato de ser recarregavel.",
        "balanced": "Explique como e feito o carregamento e destaque que o produto e recarregavel.",
        "pressure": "Puxa agora que ele e recarregavel e mostra onde carrega, se isso estiver visivel.",
    },
    {
        "keywords": ["sem fio", "wireless"],
        "topic": "wireless",
        "action": "Demonstrar o uso sem cabo conectado durante a operacao.",
        "balanced": "Destaque o uso sem fio e mostre como o produto funciona sem cabo conectado durante a operacao.",
        "pressure": "Mostra o uso sem fio agora e reforca essa liberdade de uso na demonstracao.",
    },
]


def _build_benefits(profile):
    out = []
    for rule in SAFE_BENEFIT_RULES:
        evidence = _find_keyword_evidence(profile, rule["keywords"])
        if not evidence:
            continue
        out.append(_candidate(
            profile,
            "benefit",
            rule["topic"],
            "Transformar caracteristica explicita em beneficio pratico conservador.",
            rule["action"],
            rule["balanced"],
            rule["pressure"],
            evidence=evidence[:4],
            can_repeat=True,
            repeat_group=f"benefit_{rule['topic']}",
            restrictions=["Nao ampliar o beneficio alem da caracteristica explicitamente confirmada."],
            tags=["derived_benefit"],
        ))
    return out


# ============================================================
# 8. DEMONSTRACAO
# ============================================================

def _build_demonstration(profile):
    out = []
    name = _product_name(profile)
    name_fact = ((profile.get("product") or {}).get("name") or {})
    evidence = [_evidence("product.name", fact=name_fact)] if _known(name_fact) else []

    out.append(_candidate(
        profile,
        "demonstration",
        "visual_overview",
        "Manter a apresentacao visual ativa.",
        "Mostrar o produto de perto e passar pelos detalhes visiveis.",
        f"Mostre o {name} de perto e passe pelos detalhes visiveis enquanto explica.",
        f"Chega o {name} na camera e mostra os detalhes agora. Vai apontando e falando para nao deixar a apresentacao esfriar.",
        evidence=evidence,
        importance=88,
        can_repeat=True,
        repeat_group="demo_visual",
        restrictions=["Descrever apenas detalhes realmente visiveis ou confirmados."],
    ))

    chars = profile.get("characteristics") or {}
    structured = chars.get("structured_shopee") or {}
    for key in ("size", "material", "variation", "attributes", "specifications"):
        fact = structured.get(key) or {}
        if not _known(fact):
            continue
        value = _text(_fact_value(fact))
        if not value:
            continue
        if key == "size":
            balanced = f"Mostre o tamanho na camera e de uma referencia visual. O tamanho informado e: {value}."
            pressure = f"Mostra o tamanho agora e da referencia na mao. O tamanho informado e {value}."
        elif key == "material":
            balanced = f"Aproxime a camera do acabamento e fale do material informado: {value}."
            pressure = f"Chega na camera e mostra o acabamento. Reforca o material: {value}."
        else:
            balanced = f"Use a demonstracao para explicar {key}: {value}."
            pressure = f"Mostra isso agora na camera e puxa {key}: {value}."
        out.append(_candidate(
            profile,
            "demonstration",
            f"demo_{key}",
            "Transformar informacao objetiva em demonstracao visual.",
            f"Demonstrar {key}.",
            balanced,
            pressure,
            evidence=[_evidence(f"characteristics.structured_shopee.{key}", fact=fact)],
            can_repeat=True,
            repeat_group=f"demo_{key}",
            restrictions=["Se o detalhe nao estiver fisicamente disponivel, apenas explique o dado."],
        ))

    function_keywords = [
        "dobravel", "ajustavel", "removivel", "recarregavel",
        "sem fio", "wireless", "divisoria", "compartimento",
        "giratorio", "encaixe", "fechamento",
    ]
    evidence = _find_keyword_evidence(profile, function_keywords)
    if evidence:
        out.append(_candidate(
            profile,
            "demonstration",
            "show_how_it_works",
            "Comprovar visualmente como o produto funciona.",
            "Demonstrar mecanismo ou funcionamento confirmado.",
            "Mostre o funcionamento passo a passo na camera e explique o que esta acontecendo enquanto demonstra.",
            "Faz o funcionamento agora na camera, passo a passo, e vai narrando tudo enquanto mostra.",
            evidence=evidence[:5],
            importance=91,
            can_repeat=True,
            repeat_group="demo_function",
            restrictions=["Nao atribuir funcao que nao esteja confirmada pelas evidencias."],
        ))
    return out


# ============================================================
# 9. USO / FUNCIONAMENTO
# ============================================================

def _build_usage(profile):
    out = []
    chars = profile.get("characteristics") or {}
    seller_facts = chars.get("seller_facts") or {}
    usage_keys = ("uso", "funcao", "funcionalidade", "como_usar", "como_funciona", "indicado_para", "finalidade")

    for key in usage_keys:
        fact = seller_facts.get(key) or {}
        if not _known(fact):
            continue
        value = _text(_fact_value(fact), 260)
        out.append(_candidate(
            profile,
            "usage",
            f"seller_usage_{key}",
            "Explicar uso ou funcionamento confirmado pelo vendedor.",
            "Explicar uso/funcionamento.",
            f"Explique como usar ou para que serve: {value}",
            f"Explica o uso agora, sem parar a fala: {value}",
            evidence=[_evidence(f"characteristics.seller_facts.{key}", fact=fact)],
            can_repeat=True,
            repeat_group="usage",
            restrictions=["Nao acrescentar forma de uso que nao tenha sido confirmada."],
            tags=["seller_input"],
        ))

    description = (chars.get("structured_shopee") or {}).get("description") or {}
    if _known(description):
        value = _text(_fact_value(description), 260)
        out.append(_candidate(
            profile,
            "usage",
            "description_usage",
            "Usar descricao oficial para explicar o produto.",
            "Explicar funcao/uso com base na descricao.",
            f"Use a descricao do produto para explicar como ele funciona: {value}",
            f"Puxa a funcao do produto agora usando a descricao confirmada: {value}",
            evidence=[_evidence("characteristics.structured_shopee.description", fact=description)],
            can_repeat=True,
            repeat_group="usage_description",
            restrictions=["Nao extrapolar a descricao oficial."],
        ))
    return out


# ============================================================
# 10. DIFERENCIAIS OBJETIVOS
# ============================================================

def _build_differentials(profile):
    out = []
    product = profile.get("product") or {}
    brand = product.get("brand") or {}
    if _known(brand):
        value = _text(_fact_value(brand))
        out.append(_candidate(
            profile,
            "differential",
            "brand",
            "Usar marca como informacao objetiva.",
            "Mencionar a marca sem associar superioridade nao comprovada.",
            f"Mencione a marca informada: {value}. Use isso como identificacao do produto, sem prometer qualidade que nao esteja comprovada.",
            f"Reforca a marca {value} na fala, mas sem inventar superioridade ou garantia.",
            evidence=[_evidence("product.brand", fact=brand)],
            can_repeat=False,
        ))

    material = (((profile.get("characteristics") or {}).get("structured_shopee") or {}).get("material") or {})
    if _known(material):
        value = _text(_fact_value(material))
        out.append(_candidate(
            profile,
            "differential",
            "material_objective",
            "Destacar um diferencial objetivo sem criar promessa de qualidade.",
            "Mostrar e citar o material.",
            f"Destaque o material informado, {value}, e mostre o acabamento sem usar promessas que nao estejam confirmadas.",
            f"Mostra o acabamento e reforca o material: {value}. Fato objetivo, sem exagerar.",
            evidence=[_evidence("characteristics.structured_shopee.material", fact=material)],
            can_repeat=True,
            repeat_group="material",
        ))
    return out


# ============================================================
# 11. PRECO / VALOR
# ============================================================

def _build_price(profile):
    out = []
    price = profile.get("price") or {}
    effective = price.get("effective_price") or {}
    current = _numeric(effective)
    currency = _currency(profile)

    if current is not None:
        source_path = "price.seller_price" if price.get("effective_price_source") == "seller" else "price.shopee_current"
        formatted = _money(current, currency)
        out.append(_candidate(
            profile,
            "price_value",
            "current_price",
            "Deixar o preco atual claro durante a LIVE.",
            "Reforcar o preco atual confirmado.",
            f"Reforce o preco atual: {formatted}. Fale o valor de forma clara enquanto mostra o produto.",
            f"Puxa o preco agora: {formatted}. Repete o valor com clareza e mantem o produto na camera.",
            evidence=[_evidence(source_path, fact=effective)],
            importance=92,
            can_repeat=True,
            repeat_group="price",
            restrictions=["Usar apenas o preco efetivo registrado no Product Profile."],
        ))

    previous_fact = price.get("shopee_previous") or {}
    previous = _numeric(previous_fact)
    conflict = bool(price.get("seller_shopee_conflict"))
    if (
        current is not None
        and previous is not None
        and previous > current
        and not conflict
        and price.get("effective_price_source") != "seller"
    ):
        discount_fact = price.get("discount_percent") or {}
        discount = _numeric(discount_fact)
        evidence = [
            _evidence("price.shopee_current", fact=price.get("shopee_current") or {}),
            _evidence("price.shopee_previous", fact=previous_fact),
        ]
        discount_phrase = ""
        if discount is not None:
            discount_phrase = f" A diferenca informada equivale a {_human_number(discount, 0)}%."
            evidence.append(_evidence("price.discount_percent", fact=discount_fact))
        out.append(_candidate(
            profile,
            "price_value",
            "price_comparison",
            "Mostrar diferenca objetiva entre preco atual e anterior.",
            "Comparar preco atual e anterior.",
            f"Compare os valores: de {_money(previous, currency)} por {_money(current, currency)}.{discount_phrase}",
            f"Reforca a diferenca de preco: antes {_money(previous, currency)}, agora {_money(current, currency)}.{discount_phrase}",
            evidence=evidence,
            importance=94,
            can_repeat=True,
            repeat_group="price_comparison",
            restrictions=["Nao chamar de ultima chance, so hoje ou oferta relampago sem prova."],
        ))

    if conflict:
        out.append(_candidate(
            profile,
            "warning",
            "price_conflict",
            "Evitar que o vendedor anuncie dois precos diferentes sem perceber.",
            "Confirmar qual preco deve ser falado.",
            "Atencao ao preco: o valor informado pelo vendedor esta diferente do valor automatico da Shopee. Use o preco definido pelo vendedor e evite comparar com o preco anterior ate confirmar a condicao.",
            "Confere o preco antes de repetir: o valor manual esta diferente da Shopee. Fala o valor definido pelo vendedor e nao mistura os dois.",
            evidence=[
                _evidence("price.shopee_current", fact=price.get("shopee_current") or {}),
                _evidence("price.seller_price", fact=price.get("seller_price") or {}),
            ],
            importance=98,
            risk="high",
            can_repeat=False,
            tags=["conflict"],
            restrictions=["Nao comparar o preco manual com o antigo da Shopee enquanto o conflito existir."],
        ))
    return out


# ============================================================
# 12. PROVA SOCIAL
# ============================================================

def _build_social(profile):
    out = []
    social = profile.get("social_proof") or {}
    sold_fact = social.get("sold") or {}
    rating_fact = social.get("rating") or {}
    count_fact = social.get("rating_count") or {}
    sold = _numeric(sold_fact)
    rating = _numeric(rating_fact)
    count = _numeric(count_fact)

    if sold is not None and sold > 0:
        out.append(_candidate(
            profile,
            "social_proof",
            "sold_count",
            "Usar volume de vendas como prova social objetiva.",
            "Mencionar a quantidade de vendidos informada pela Shopee.",
            f"Use a prova social: a Shopee informa {_human_number(sold, 0)} vendidos. Mencione o numero sem transformar isso em garantia de qualidade.",
            f"Puxa a prova social agora: a Shopee mostra {_human_number(sold, 0)} vendidos. Reforca o numero, sem prometer resultado.",
            evidence=[_evidence("social_proof.sold", fact=sold_fact)],
            can_repeat=True,
            repeat_group="social_sold",
            restrictions=["Quantidade vendida nao e garantia de qualidade ou resultado."],
        ))

    if rating is not None:
        if count is not None and count > 0:
            balanced = f"Reforce a avaliacao informada: {_human_number(rating, 1)}, com {_human_number(count, 0)} avaliacoes. Use isso como prova social, sem prometer que todo comprador tera a mesma experiencia."
            pressure = f"Mostra a prova social: avaliacao {_human_number(rating, 1)} com {_human_number(count, 0)} avaliacoes. Reforca o dado e segue mostrando o produto."
            evidence = [
                _evidence("social_proof.rating", fact=rating_fact),
                _evidence("social_proof.rating_count", fact=count_fact),
            ]
        else:
            balanced = f"Mencione a avaliacao informada pela Shopee: {_human_number(rating, 1)}. Use o dado como prova social."
            pressure = f"Puxa a avaliacao agora: {_human_number(rating, 1)}. Reforca o dado e continua a demonstracao."
            evidence = [_evidence("social_proof.rating", fact=rating_fact)]
        out.append(_candidate(
            profile,
            "social_proof",
            "rating",
            "Usar avaliacao como prova social objetiva.",
            "Mencionar avaliacao confirmada.",
            balanced,
            pressure,
            evidence=evidence,
            can_repeat=True,
            repeat_group="social_rating",
            restrictions=["Avaliacao nao deve virar garantia de satisfacao ou resultado."],
        ))
    return out


# ============================================================
# 13. DISPONIBILIDADE
# ============================================================

def _build_availability(profile):
    out = []
    availability = profile.get("availability") or {}
    is_oos_fact = availability.get("is_oos") or {}
    stock_fact = availability.get("stock") or {}
    is_oos = _fact_value(is_oos_fact) if _known(is_oos_fact) else None

    if is_oos is True:
        return [_candidate(
            profile,
            "warning",
            "out_of_stock",
            "Evitar orientacao de compra para produto marcado como esgotado.",
            "Nao anunciar disponibilidade.",
            "A Shopee marcou este produto como esgotado. Evite dizer que esta disponivel ate o dado mudar.",
            "Esse item aparece como esgotado. Nao chama para comprar como se estivesse disponivel.",
            evidence=[_evidence("availability.is_oos", fact=is_oos_fact)],
            importance=99,
            risk="high",
            can_repeat=False,
        )]

    stock = _numeric(stock_fact)
    if stock is not None and stock >= 0:
        out.append(_candidate(
            profile,
            "availability",
            "stock",
            "Informar disponibilidade sem criar escassez.",
            "Mencionar o estoque exibido, se necessario.",
            f"Se for util falar de disponibilidade, a Shopee esta mostrando estoque de {_human_number(stock, 0)}. Informe o numero sem criar urgencia artificial.",
            f"Se entrar no assunto de estoque, o sistema mostra {_human_number(stock, 0)}. Fala o dado, mas nao crie escassez sem prova.",
            evidence=[_evidence("availability.stock", fact=stock_fact)],
            can_repeat=False,
            restrictions=["Nunca converter estoque em escassez sem prova explicita."],
        ))
    return out


# ============================================================
# 14. PROMOCOES
# ============================================================

ACTIVE_WORDS = {"active", "ativo", "valid", "valido", "available", "disponivel", "ongoing", "in_progress"}


def _explicit_active(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return _norm(value) in ACTIVE_WORDS
    if isinstance(value, dict):
        for key in ("is_active", "active", "is_valid", "valid", "is_available", "available"):
            if key in value:
                raw = value.get(key)
                if raw is True:
                    return True
                if isinstance(raw, str) and _norm(raw) in ACTIVE_WORDS:
                    return True
        status = value.get("status")
        if isinstance(status, str) and _norm(status) in ACTIVE_WORDS:
            return True
        return any(_explicit_active(v) for v in value.values() if isinstance(v, (dict, list)))
    if isinstance(value, list):
        return any(_explicit_active(v) for v in value)
    return False


def _build_promotions(profile):
    out = []
    blocked = []
    promotions = profile.get("promotions") or {}
    active_evidence = []

    for field in ("item_promotion", "applied_promotion_info", "vouchers", "promotion"):
        fact = promotions.get(field) or {}
        if not _known(fact):
            continue
        value = _fact_value(fact)
        if _explicit_active(value):
            active_evidence.append(_evidence(
                f"promotions.{field}",
                fact=fact,
                note="Estrutura contem indicador explicito de ativo/valido.",
            ))
        else:
            blocked.append({
                "category": "promotion",
                "path": f"promotions.{field}",
                "reason": "Existe dado promocional, mas nao ha indicador explicito suficiente de que a condicao esta ativa agora.",
            })

    if active_evidence:
        out.append(_candidate(
            profile,
            "promotion",
            "confirmed_active_promotion",
            "Explorar promocao com estado explicitamente ativo/valido.",
            "Mencionar a condicao promocional confirmada.",
            "Ha uma condicao promocional marcada como ativa/valida nos dados. Explique somente o que estiver confirmado na tela e nos dados.",
            "Tem condicao promocional marcada como ativa. Puxa isso na fala e mostra exatamente a regra confirmada, sem inventar prazo ou escassez.",
            evidence=active_evidence,
            importance=95,
            risk="medium",
            can_repeat=True,
            repeat_group="promotion",
            restrictions=[
                "Nao inventar prazo, quantidade, cupom, frete gratis ou urgencia.",
                "Se a condicao deixar de estar ativa, este candidato fica obsoleto.",
            ],
        ))
    return out, blocked


# ============================================================
# 15. OBJECAO PREVENTIVA
# ============================================================

def _build_preventive(profile):
    out = []
    structured = ((profile.get("characteristics") or {}).get("structured_shopee") or {})
    configs = {
        "size": (
            "Antecipar duvida de tamanho/dimensao.",
            "Antecipe a duvida de tamanho: mostre uma referencia visual e fale o tamanho informado.",
            "Ja mata a duvida de tamanho: mostra na mao e fala a medida ou tamanho informado.",
        ),
        "material": (
            "Antecipar duvida sobre material.",
            "Fale do material e aproxime a camera do acabamento para o publico entender melhor o produto.",
            "Mostra o material de perto e reforca esse ponto antes que a duvida apareca.",
        ),
        "variation": (
            "Antecipar duvida sobre opcoes/variacoes.",
            "Explique quais variacoes estao informadas para evitar confusao na hora de escolher.",
            "Puxa as variacoes agora e deixa claro quais opcoes aparecem para o cliente.",
        ),
        "model": (
            "Antecipar duvida sobre modelo.",
            "Diga claramente qual e o modelo informado antes de seguir para os beneficios.",
            "Reforca o modelo agora para nao deixar o publico confundir o produto.",
        ),
    }
    for key, (objective, balanced, pressure) in configs.items():
        fact = structured.get(key) or {}
        if not _known(fact):
            continue
        out.append(_candidate(
            profile,
            "preventive_objection",
            f"prevent_{key}",
            objective,
            f"Antecipar duvida sobre {key}.",
            balanced,
            pressure,
            evidence=[_evidence(f"characteristics.structured_shopee.{key}", fact=fact)],
            can_repeat=True,
            repeat_group=f"prevent_{key}",
            restrictions=["Explicar preventivamente; nao fingir que o publico perguntou isso."],
        ))

    for conflict in profile.get("conflicts") or []:
        field = conflict.get("field")
        if field == "price":
            continue
        out.append(_candidate(
            profile,
            "warning",
            "conflict_" + re.sub(r"[^a-zA-Z0-9_]+", "_", str(field)),
            "Evitar que informacoes conflitantes sejam tratadas como fato.",
            "Confirmar o dado antes de usa-lo.",
            f"Existe conflito de informacao em '{field}'. Confirme o dado antes de transformar isso em argumento de venda.",
            f"Tem conflito em '{field}'. Nao usa esse ponto na pressa: confirma primeiro.",
            evidence=[_evidence("conflicts", value=conflict, source="product_extractor")],
            importance=97,
            risk="high",
            can_repeat=False,
            tags=["conflict"],
        ))
    return out


# ============================================================
# 16. REFORCO
# ============================================================

def _build_reinforcement(profile, candidates):
    out = []
    benefits = [x for x in candidates if x.get("category") == "benefit"]
    prices = [x for x in candidates if x.get("category") == "price_value" and x.get("topic_key") == "current_price"]
    socials = [x for x in candidates if x.get("category") == "social_proof"]

    if benefits:
        src = benefits[0]
        out.append(_candidate(
            profile,
            "reinforcement",
            "reinforce_benefit",
            "Voltar ao principal beneficio por outro angulo.",
            "Reforcar beneficio com nova demonstracao.",
            "Volte ao principal beneficio por outro angulo: mostre de novo a caracteristica que sustenta esse beneficio e conecte com o uso pratico.",
            "Volta nesse beneficio sem repetir a mesma frase: mostra a caracteristica de novo e conecta com o uso pratico.",
            evidence=src.get("evidence") or [],
            importance=72,
            can_repeat=True,
            repeat_group="reinforcement_benefit",
            restrictions=["Mudar o angulo da fala, nao inventar novo beneficio."],
        ))

    if prices:
        src = prices[0]
        out.append(_candidate(
            profile,
            "reinforcement",
            "reinforce_price",
            "Reforcar preco sem falsa urgencia.",
            "Retomar o valor atual depois de outro ponto.",
            "Retome o preco de forma natural depois de mostrar outro detalhe do produto.",
            "Depois de mostrar outro ponto, puxa o preco de novo para manter a oferta viva.",
            evidence=src.get("evidence") or [],
            importance=74,
            can_repeat=True,
            repeat_group="reinforcement_price",
            restrictions=["Nao adicionar 'so hoje', 'acabando' ou escassez nao confirmada."],
        ))

    if socials:
        src = socials[0]
        out.append(_candidate(
            profile,
            "reinforcement",
            "reinforce_social",
            "Retomar prova social sem exagero.",
            "Voltar a um dado de prova social.",
            "Depois de explicar o produto, retome uma prova social confirmada para reforcar confianca.",
            "Volta na prova social depois da demonstracao e reforca o dado confirmado.",
            evidence=src.get("evidence") or [],
            importance=70,
            can_repeat=True,
            repeat_group="reinforcement_social",
        ))
    return out


# ============================================================
# 17. CTA
# ============================================================

def _build_cta(profile):
    name = _product_name(profile)
    name_fact = ((profile.get("product") or {}).get("name") or {})
    evidence = [_evidence("product.name", fact=name_fact)] if _known(name_fact) else []
    return [_candidate(
        profile,
        "cta",
        "open_product",
        "Converter interesse em acao dentro da LIVE.",
        "Convidar o publico a abrir o produto destacado.",
        f"Convide quem se interessou pelo {name} a tocar no produto destacado e conferir os detalhes.",
        f"Chama o pessoal para clicar no {name} da tela e conferir agora os detalhes do produto.",
        evidence=evidence,
        can_repeat=True,
        repeat_group="cta_product",
        restrictions=["Nao prometer desconto, estoque, frete ou prazo nao confirmado."],
    )]


# ============================================================
# 18. TRANSICAO
# ============================================================

def _build_transition(profile):
    return [_candidate(
        profile,
        "transition",
        "keep_flowing",
        "Evitar silencio e ajudar a mudar de assunto mantendo a apresentacao viva.",
        "Trocar o foco para outro aspecto do produto.",
        "Depois de fechar esse ponto, mude o foco para outro aspecto do produto: caracteristica, demonstracao, beneficio, preco ou prova social.",
        "Nao para a fala: fechou um ponto, ja puxa outro aspecto do produto e continua a demonstracao.",
        evidence=[],
        importance=60,
        can_repeat=True,
        repeat_group="transition",
        restrictions=["A transicao nao autoriza criar fatos novos."],
    )]


# ============================================================
# 19. TOPICOS BLOQUEADOS
# ============================================================

def _build_blocked_topics(profile, promotion_blocked):
    blocked = list(promotion_blocked)
    price = profile.get("price") or {}
    if not _known(price.get("effective_price") or {}):
        blocked.append({"category": "price_value", "reason": "Preco atual nao confirmado."})

    social = profile.get("social_proof") or {}
    if not (_known(social.get("sold") or {}) or _known(social.get("rating") or {})):
        blocked.append({"category": "social_proof", "reason": "Sem dados confiaveis de vendidos ou avaliacao."})

    seller_facts = ((profile.get("characteristics") or {}).get("seller_facts") or {})
    guarantee = any("garantia" in _norm(key) and _known(fact) for key, fact in seller_facts.items())
    if not guarantee:
        blocked.append({"category": "guarantee", "reason": "Garantia nao confirmada no Product Profile."})

    blocked.append({
        "category": "scarcity",
        "reason": "Escassez/ultimas unidades nao sao inferidas automaticamente a partir do estoque.",
    })
    return blocked


# ============================================================
# 20. MONTAR SALES BUNDLE
# ============================================================

def product_sales_builder_build(profile):
    if not isinstance(profile, dict):
        raise TypeError("Product Profile deve ser um dicionario.")
    key = _product_key(profile)
    if key is None:
        raise ValueError("Product Profile sem identidade valida.")

    candidates = []
    candidates += _build_presentation(profile)
    candidates += _build_characteristics(profile)
    candidates += _build_benefits(profile)
    candidates += _build_demonstration(profile)
    candidates += _build_usage(profile)
    candidates += _build_differentials(profile)
    candidates += _build_price(profile)
    candidates += _build_social(profile)
    candidates += _build_availability(profile)

    promo_candidates, promo_blocked = _build_promotions(profile)
    candidates += promo_candidates
    candidates += _build_preventive(profile)
    candidates += _build_reinforcement(profile, candidates)
    candidates += _build_cta(profile)
    candidates += _build_transition(profile)
    candidates = _dedupe(candidates)

    by_category = {}
    for item in candidates:
        category = item.get("category")
        by_category[category] = by_category.get(category, 0) + 1

    return {
        "schema_version": PRODUCT_SALES_BUNDLE_SCHEMA_VERSION,
        "product_key": key,
        "identity": _copy(profile.get("identity") or {}),
        "product_name": _product_name(profile),
        "selected_style": product_sales_builder_state.get("style") or STYLE_BALANCED,
        "candidate_count": len(candidates),
        "candidates_by_category": by_category,
        "candidates": candidates,
        "blocked_topics": _build_blocked_topics(profile, promo_blocked),
        "source_profile": {
            "schema_version": profile.get("schema_version"),
            "provenance": _copy(profile.get("provenance") or {}),
            "seller_input_used": bool((profile.get("seller_input") or {}).get("used")),
            "conflict_count": len(profile.get("conflicts") or []),
        },
        "built_at": _now_iso(),
    }


# ============================================================
# 21. EMISSAO
# ============================================================

def _emit(output_type, bundle=None, previous_bundle=None, reason=None):
    global _output_seq
    _output_seq += 1
    output = {
        "seq": _output_seq,
        "type": output_type,
        "platform": "shopee",
        "emitted_at": _now_iso(),
        "reason": reason,
        "bundle": _copy(bundle),
        "previous_bundle": _copy(previous_bundle),
    }
    product_sales_builder_history.append(_copy(output))
    try:
        product_sales_builder_output_queue.put_nowait(_copy(output))
    except Exception:
        pass
    product_sales_builder_state["last_output_at"] = time.time()
    return output


# ============================================================
# 22. PROCESSAR SAIDA DO PRODUCT EXTRACTOR
# ============================================================

def product_sales_builder_process_output(extractor_output):
    if not isinstance(extractor_output, dict):
        raise TypeError("Saida do Product Extractor deve ser um dicionario.")

    output_type = extractor_output.get("type")
    if output_type not in {
        "product_profile_started",
        "product_profile_changed",
        "product_profile_updated",
        "product_profile_cleared",
    }:
        return None

    product_sales_builder_state["processed_profiles"] += 1
    product_sales_builder_state["last_input_at"] = time.time()
    product_sales_builder_state["last_error"] = None

    if output_type == "product_profile_cleared":
        previous = _copy(product_sales_builder_state.get("current_bundle"))
        product_sales_builder_state["previous_bundle"] = previous
        product_sales_builder_state["current_bundle"] = None
        product_sales_builder_state["current_profile"] = None
        product_sales_builder_state["current_key"] = None
        product_sales_builder_state["bundles_cleared"] += 1
        return _emit(
            "sales_candidates_cleared",
            bundle=None,
            previous_bundle=previous,
            reason="product_profile_cleared",
        )

    profile = extractor_output.get("profile")
    if not isinstance(profile, dict):
        raise ValueError("Saida do Extractor sem Product Profile valido.")

    bundle = product_sales_builder_build(profile)
    previous = _copy(product_sales_builder_state.get("current_bundle"))
    product_sales_builder_state["previous_bundle"] = previous
    product_sales_builder_state["current_bundle"] = _copy(bundle)
    product_sales_builder_state["current_profile"] = _copy(profile)
    product_sales_builder_state["current_key"] = bundle.get("product_key")

    if output_type == "product_profile_started":
        product_sales_builder_state["bundles_started"] += 1
        return _emit("sales_candidates_started", bundle, previous, "product_profile_started")
    if output_type == "product_profile_changed":
        product_sales_builder_state["bundles_changed"] += 1
        return _emit("sales_candidates_changed", bundle, previous, "product_profile_changed")

    product_sales_builder_state["bundles_updated"] += 1
    return _emit("sales_candidates_updated", bundle, previous, "product_profile_updated")


# ============================================================
# 23. ESTILO
# ============================================================

def product_sales_builder_normalize_style(style):
    value = _norm(style).replace(" ", "_").replace("/", "_")
    value = re.sub(r"_+", "_", value)
    aliases = {
        "equilibrado": STYLE_BALANCED,
        "balanced": STYLE_BALANCED,
        "pressao": STYLE_PRESSURE,
        "pressao_feira": STYLE_PRESSURE,
        "feira": STYLE_PRESSURE,
        "pressure": STYLE_PRESSURE,
    }
    resolved = aliases.get(value, value)
    if resolved not in ALLOWED_STYLES:
        raise ValueError("Estilo invalido. Use 'equilibrado' ou 'pressao_feira'.")
    return resolved


def product_sales_builder_set_style(style, emit_update=True):
    normalized = product_sales_builder_normalize_style(style)
    if product_sales_builder_state.get("style") == normalized:
        return _copy(product_sales_builder_state.get("current_bundle"))

    product_sales_builder_state["style"] = normalized
    product_sales_builder_state["style_updates"] += 1
    profile = product_sales_builder_state.get("current_profile")
    if not isinstance(profile, dict):
        return None

    previous = _copy(product_sales_builder_state.get("current_bundle"))
    bundle = product_sales_builder_build(profile)
    product_sales_builder_state["previous_bundle"] = previous
    product_sales_builder_state["current_bundle"] = _copy(bundle)
    if emit_update:
        _emit("sales_candidates_style_updated", bundle, previous, "style_changed")
    return _copy(bundle)


# ============================================================
# 24. SUPERVISOR
# ============================================================

async def product_sales_builder_supervisor():
    global _stop_requested
    if "product_extractor_next_output" not in globals() or not callable(globals().get("product_extractor_next_output")):
        raise RuntimeError(
            "Product Extractor nao esta carregado. Execute primeiro a celula do Product Extractor V1."
        )

    _stop_requested = False
    product_sales_builder_state["running"] = True
    product_sales_builder_state["last_error"] = None

    try:
        while not _stop_requested:
            try:
                extractor_output = await product_extractor_next_output(timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                product_sales_builder_state["last_error"] = str(exc)
                await asyncio.sleep(0.5)
                continue

            try:
                product_sales_builder_process_output(extractor_output)
            except Exception as exc:
                product_sales_builder_state["last_error"] = str(exc)
    finally:
        product_sales_builder_state["running"] = False


async def executar_product_sales_builder():
    return await product_sales_builder_supervisor()


# ============================================================
# 25. PARAR / RESET
# ============================================================

def product_sales_builder_stop():
    global _stop_requested
    _stop_requested = True


def product_sales_builder_reset(clear_history=True, clear_output_queue=True, keep_style=True):
    global _stop_requested, _output_seq
    style = product_sales_builder_state.get("style") if keep_style else STYLE_BALANCED
    _stop_requested = False
    _output_seq = 0
    product_sales_builder_state.update({
        "version": PRODUCT_SALES_BUILDER_VERSION,
        "schema_version": PRODUCT_SALES_BUNDLE_SCHEMA_VERSION,
        "running": False,
        "style": style,
        "processed_profiles": 0,
        "bundles_started": 0,
        "bundles_changed": 0,
        "bundles_updated": 0,
        "bundles_cleared": 0,
        "style_updates": 0,
        "current_key": None,
        "current_profile": None,
        "current_bundle": None,
        "previous_bundle": None,
        "last_input_at": None,
        "last_output_at": None,
        "last_error": None,
    })
    if clear_history:
        product_sales_builder_history.clear()
    if clear_output_queue:
        try:
            while True:
                product_sales_builder_output_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass


# ============================================================
# 26. API PUBLICA PARA SALES DECISION COACH
# ============================================================

async def product_sales_builder_next_output(timeout=None):
    if timeout is None:
        return await product_sales_builder_output_queue.get()
    return await asyncio.wait_for(
        product_sales_builder_output_queue.get(),
        timeout=float(timeout),
    )


def product_sales_builder_current_bundle():
    return _copy(product_sales_builder_state.get("current_bundle"))


def product_sales_builder_current_candidates():
    bundle = product_sales_builder_state.get("current_bundle")
    return _copy((bundle or {}).get("candidates") or [])


def product_sales_builder_recent_outputs(limit=20):
    try:
        limit = max(1, int(limit))
    except Exception:
        limit = 20
    return _copy(list(product_sales_builder_history)[-limit:])


def product_sales_builder_status():
    bundle = product_sales_builder_state.get("current_bundle") or {}
    return {
        "version": product_sales_builder_state.get("version"),
        "schema_version": product_sales_builder_state.get("schema_version"),
        "running": product_sales_builder_state.get("running"),
        "style": product_sales_builder_state.get("style"),
        "processed_profiles": product_sales_builder_state.get("processed_profiles"),
        "bundles_started": product_sales_builder_state.get("bundles_started"),
        "bundles_changed": product_sales_builder_state.get("bundles_changed"),
        "bundles_updated": product_sales_builder_state.get("bundles_updated"),
        "bundles_cleared": product_sales_builder_state.get("bundles_cleared"),
        "style_updates": product_sales_builder_state.get("style_updates"),
        "current_key": _copy(product_sales_builder_state.get("current_key")),
        "candidate_count": bundle.get("candidate_count", 0),
        "candidates_by_category": _copy(bundle.get("candidates_by_category") or {}),
        "blocked_topics": len(bundle.get("blocked_topics") or []),
        "history_size": len(product_sales_builder_history),
        "output_queue_size": product_sales_builder_output_queue.qsize(),
        "last_error": product_sales_builder_state.get("last_error"),
    }


# ============================================================
# 27. DIAGNOSTICO
# ============================================================

def mostrar_product_sales_builder(show_candidates=8):
    status = product_sales_builder_status()
    bundle = product_sales_builder_current_bundle()

    print("=" * 76)
    print("AGCN LIVE - PRODUCT SALES BUILDER V1.0")
    print("=" * 76)
    print("Executando:", status["running"])
    print("Estilo:", status["style"])
    print("Perfis processados:", status["processed_profiles"])
    print("Candidatos:", status["candidate_count"])
    print("Categorias:", status["candidates_by_category"])
    print("Topicos bloqueados:", status["blocked_topics"])
    print("Erro:", status["last_error"])
    print()

    if not bundle:
        print("Nenhum banco de vendas ativo.")
        return

    print("Produto:", bundle.get("product_name"))
    print()
    candidates = bundle.get("candidates") or []
    try:
        show_candidates = max(1, int(show_candidates))
    except Exception:
        show_candidates = 8

    for index, item in enumerate(candidates[:show_candidates], 1):
        print(f"{index}. [{item.get('category')}] {item.get('selected_text')}")
        print("   importancia:", item.get("importance_hint"), "| risco:", item.get("risk_level"))
        print()


# ============================================================
# 28. AUTO-TESTE INTERNO
# ============================================================

def product_sales_builder_self_test():
    product_sales_builder_reset(clear_history=True, clear_output_queue=True, keep_style=False)

    def fact(value, source="shopee"):
        return {
            "known": True,
            "value": value,
            "source": source,
            "source_field": "test",
            "note": None,
        }

    def unknown():
        return {
            "known": False,
            "value": None,
            "source": None,
            "source_field": None,
            "note": None,
        }

    profile = {
        "schema_version": "1.0",
        "identity": {
            "platform": "shopee",
            "session_id": "TESTE",
            "item_id": "123",
            "shop_id": "456",
            "key": ("shopee", "456", "123"),
        },
        "product": {
            "name": fact("Mini Porta Joias Portatil"),
            "image": unknown(),
            "brand": unknown(),
            "item_type": unknown(),
            "category_ids": unknown(),
            "title_text": fact("Mini Porta Joias Portatil com Divisorias", "shopee_title"),
        },
        "price": {
            "currency": fact("BRL"),
            "shopee_current": fact({"raw": 11.99, "normalized": 11.99}),
            "shopee_max": unknown(),
            "shopee_previous": fact({"raw": 25.0, "normalized": 25.0}),
            "shopee_previous_max": unknown(),
            "out_of_live_price": unknown(),
            "discount_percent": fact({"raw": 52, "normalized": 52.0}),
            "seller_price": unknown(),
            "effective_price": fact({"raw": 11.99, "normalized": 11.99}),
            "effective_price_source": "shopee",
            "seller_shopee_conflict": False,
        },
        "social_proof": {
            "sold": fact({"raw": 20000, "normalized": 20000.0}),
            "rating": fact({"raw": 4.9, "normalized": 4.9}),
            "rating_count": fact({"raw": 3500, "normalized": 3500.0}),
            "popularity": unknown(),
        },
        "availability": {
            "stock": fact({"raw": 716, "normalized": 716.0}),
            "is_oos": fact(False),
            "status": unknown(),
            "location_stocks": unknown(),
        },
        "promotions": {
            "discount_percent": fact({"raw": 52, "normalized": 52.0}),
            "item_promotion": fact({"promo_id": 1}),
            "applied_promotion_info": unknown(),
            "vouchers": unknown(),
            "promotion": unknown(),
            "requires_active_validation": True,
        },
        "characteristics": {
            "structured_shopee": {
                "material": fact("couro sintetico"),
                "color": unknown(),
                "size": fact("compacto"),
                "model": unknown(),
                "variation": unknown(),
                "attributes": fact({"divisorias": True}),
                "specifications": unknown(),
                "description": unknown(),
            },
            "seller_facts": {
                "divisorias": fact("possui divisorias internas", "seller"),
            },
            "seller_notes": unknown(),
        },
        "seller_input": {
            "used": True,
            "price": None,
            "additional_info": None,
            "facts": {"divisorias": "possui divisorias internas"},
            "updated_at": None,
        },
        "provenance": {"automatic_source": "shopee_show_item"},
        "source_snapshot": {"available_raw_fields": []},
        "conflicts": [],
    }

    checks = 0
    result = product_sales_builder_process_output({
        "type": "product_profile_started",
        "profile": profile,
        "previous_profile": None,
    })
    assert result["type"] == "sales_candidates_started"; checks += 1
    bundle = result["bundle"]
    assert bundle["candidate_count"] > 8; checks += 1
    categories = {x["category"] for x in bundle["candidates"]}
    for required in ("presentation", "demonstration", "benefit", "price_value", "social_proof", "cta", "transition", "preventive_objection"):
        assert required in categories; checks += 1
    assert "promotion" not in categories; checks += 1
    assert any(x.get("category") == "promotion" for x in bundle["blocked_topics"]); checks += 1

    all_text = " ".join(
        ((x.get("texts") or {}).get(STYLE_BALANCED, "") + " " + (x.get("texts") or {}).get(STYLE_PRESSURE, ""))
        for x in bundle["candidates"]
    ).lower()
    assert "ultimas unidades" not in all_text; checks += 1
    assert "so hoje" not in all_text; checks += 1

    pressure_bundle = product_sales_builder_set_style("pressao_feira", emit_update=False)
    assert pressure_bundle["selected_style"] == STYLE_PRESSURE; checks += 1
    assert pressure_bundle["candidate_count"] == bundle["candidate_count"]; checks += 1

    conflict = _copy(profile)
    conflict["price"]["seller_price"] = fact({"raw": "R$ 10,90", "normalized": 10.90}, "seller")
    conflict["price"]["effective_price"] = _copy(conflict["price"]["seller_price"])
    conflict["price"]["effective_price_source"] = "seller"
    conflict["price"]["seller_shopee_conflict"] = True
    conflict_bundle = product_sales_builder_build(conflict)
    topics = {x["topic_key"] for x in conflict_bundle["candidates"]}
    assert "price_conflict" in topics; checks += 1
    assert "price_comparison" not in topics; checks += 1

    active = _copy(profile)
    active["promotions"]["item_promotion"] = fact({"is_active": True, "label": "LIVE"})
    active_bundle = product_sales_builder_build(active)
    assert "promotion" in {x["category"] for x in active_bundle["candidates"]}; checks += 1

    oos = _copy(profile)
    oos["availability"]["is_oos"] = fact(True)
    oos_bundle = product_sales_builder_build(oos)
    assert "out_of_stock" in {x["topic_key"] for x in oos_bundle["candidates"]}; checks += 1

    cleared = product_sales_builder_process_output({
        "type": "product_profile_cleared",
        "profile": None,
        "previous_profile": profile,
    })
    assert cleared["type"] == "sales_candidates_cleared"; checks += 1
    assert product_sales_builder_current_bundle() is None; checks += 1

    return {"ok": True, "checks": checks, "version": PRODUCT_SALES_BUILDER_VERSION}


# ============================================================
# 29. ORDEM DE USO NO COLAB
#
# 1. Worker Shopee Produto V1
# 2. Product Extractor V1
# 3. Product Sales Builder V1
#
# Futuro Sales Decision Coach consumira SOMENTE:
# product_sales_builder_next_output()
#
# O Builder nao inicia sozinho.
# ============================================================

print("AGCN Product Sales Builder V1.0 carregado.")
