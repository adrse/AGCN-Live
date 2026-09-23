# ============================================================
# AGCN LIVE - PRODUCT EXTRACTOR V1.0
#
# FUNCAO:
#
# Worker Shopee Produto
#        |
#        v
# PRODUCT EXTRACTOR
#        |
#        +-- recebe o produto bruto da Shopee
#        +-- recebe informacoes adicionais do vendedor
#        +-- normaliza os dados
#        +-- preserva a origem de cada informacao
#        +-- identifica conflitos sem apagar a fonte original
#        +-- monta um Product Profile padronizado
#        |
#        v
# Futuro Product Sales Builder
#
#
# O PRODUCT EXTRACTOR NAO:
#
# - cria argumentos de venda
# - cria CTA
# - decide o que o vendedor deve falar
# - decide quando mostrar uma mensagem
# - inventa caracteristicas ausentes
# - transforma "desconhecido" em fato
# - altera o Worker Shopee Produto
# - altera Live Engine / Coaches atuais
#
#
# REGRA DE PROVENIENCIA:
#
# Todo dado relevante deve manter sua origem:
#
# - shopee
# - shopee_title
# - seller
# - derived
#
# Quando vendedor e Shopee informarem valores diferentes,
# ambos sao preservados e o conflito fica explicito.
#
#
# Esta celula apenas CARREGA o modulo.
# ============================================================

import asyncio
import copy
import re
import time
from collections import deque
from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo


# ============================================================
# 1. VERSAO / CONFIGURACOES
# ============================================================

PRODUCT_EXTRACTOR_VERSION = "1.0"
PRODUCT_PROFILE_SCHEMA_VERSION = "1.0"

PRODUCT_EXTRACTOR_TIMEZONE = ZoneInfo(
    "America/Araguaina"
)

PRODUCT_EXTRACTOR_HISTORY_MAXLEN = 200


# ============================================================
# 2. FILA / HISTORICO / ESTADO
#
# A fila de saida tera um unico consumidor no futuro:
# Product Sales Builder.
#
# Interface e diagnosticos consultam estado/historico.
# ============================================================

product_extractor_output_queue = asyncio.Queue()

product_extractor_history = deque(
    maxlen=PRODUCT_EXTRACTOR_HISTORY_MAXLEN
)

product_extractor_state = {
    "version": PRODUCT_EXTRACTOR_VERSION,
    "schema_version": PRODUCT_PROFILE_SCHEMA_VERSION,

    "running": False,
    "processed_events": 0,
    "profiles_started": 0,
    "profiles_changed": 0,
    "profiles_updated": 0,
    "profiles_cleared": 0,

    "current_key": None,
    "current_profile": None,
    "previous_profile": None,

    # Mantemos apenas o evento bruto atual necessario
    # para reconstruir o perfil se o vendedor adicionar
    # informacoes depois que o produto ja estiver na tela.
    "current_input_event": None,

    "last_input_at": None,
    "last_output_at": None,
    "last_error": None,
}

_product_extractor_stop_requested = False
_product_extractor_seq = 0

# Informacoes manuais associadas a um produto especifico.
# Chave:
# ("shopee", shop_id, item_id)
_seller_info_by_product = {}

# Se a Interface receber informacoes antes de o primeiro
# produto ser identificado, elas ficam aqui e sao aplicadas
# ao primeiro produto detectado.
_pending_seller_info = {
    "price": None,
    "additional_info": None,
    "facts": {},
    "updated_at": None,
}


# ============================================================
# 3. UTILITARIOS GERAIS
# ============================================================

def _pe_now_iso():
    return datetime.now(
        PRODUCT_EXTRACTOR_TIMEZONE
    ).isoformat()


def _pe_copy(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _pe_clean_text(value):
    if value is None:
        return None

    text = str(
        value
    ).strip()

    return (
        text
        if text
        else None
    )


def _pe_known(value):
    return (
        value is not None
        and value != ""
        and value != []
        and value != {}
    )


def _pe_fact(
    value=None,
    source=None,
    source_field=None,
    note=None,
):
    known = _pe_known(
        value
    )

    return {
        "known": known,
        "value": (
            _pe_copy(value)
            if known
            else None
        ),
        "source": (
            source
            if known
            else None
        ),
        "source_field": (
            source_field
            if known
            else None
        ),
        "note": (
            note
            if known and note
            else None
        ),
    }


def _pe_first_field(
    raw,
    field_names,
):
    if not isinstance(
        raw,
        dict,
    ):
        return (
            None,
            None,
        )

    for field_name in field_names:
        if (
            field_name in raw
            and _pe_known(
                raw.get(
                    field_name
                )
            )
        ):
            return (
                raw.get(
                    field_name
                ),
                field_name,
            )

    return (
        None,
        None,
    )


def _pe_direct_fact(
    raw,
    field_names,
    source="shopee",
    note=None,
):
    value, field_name = (
        _pe_first_field(
            raw,
            field_names,
        )
    )

    return _pe_fact(
        value=value,
        source=source,
        source_field=field_name,
        note=note,
    )


def _pe_numeric(value):
    if value is None:
        return None

    if isinstance(
        value,
        bool,
    ):
        return None

    if isinstance(
        value,
        (int, float, Decimal),
    ):
        try:
            return float(
                value
            )
        except Exception:
            return None

    text = str(
        value
    ).strip()

    if not text:
        return None

    # Remove moeda, porcentagem e espacos.
    text = (
        text
        .replace("R$", "")
        .replace("%", "")
        .replace("\u00a0", " ")
        .strip()
    )

    # Mantem apenas digitos, sinais e separadores.
    text = re.sub(
        r"[^0-9,\.\-]",
        "",
        text,
    )

    if not text:
        return None

    # Formato brasileiro:
    # 1.234,56 -> 1234.56
    if (
        "," in text
        and "." in text
        and text.rfind(",")
        >
        text.rfind(".")
    ):
        text = (
            text
            .replace(".", "")
            .replace(",", ".")
        )

    # 17,90 -> 17.90
    elif (
        "," in text
        and "." not in text
    ):
        text = text.replace(
            ",",
            ".",
        )

    try:
        return float(
            Decimal(
                text
            )
        )
    except (
        InvalidOperation,
        ValueError,
    ):
        return None


def _pe_normalized_numeric_fact(
    raw,
    field_names,
    source="shopee",
):
    raw_value, field_name = (
        _pe_first_field(
            raw,
            field_names,
        )
    )

    if field_name is None:
        return _pe_fact()

    normalized = (
        _pe_numeric(
            raw_value
        )
    )

    # Preservamos o bruto junto com o valor numerico.
    # Nao aplicamos escala presumida de centavos.
    value = {
        "raw": _pe_copy(
            raw_value
        ),
        "normalized": normalized,
    }

    return _pe_fact(
        value=value,
        source=source,
        source_field=field_name,
    )


def _pe_fact_numeric_value(
    fact,
):
    if not isinstance(
        fact,
        dict,
    ):
        return None

    value = fact.get(
        "value"
    )

    if isinstance(
        value,
        dict,
    ):
        return value.get(
            "normalized"
        )

    return _pe_numeric(
        value
    )


def _pe_has_seller_info(
    info,
):
    if not isinstance(
        info,
        dict,
    ):
        return False

    return (
        _pe_known(
            info.get(
                "price"
            )
        )
        or
        _pe_known(
            info.get(
                "additional_info"
            )
        )
        or
        bool(
            info.get(
                "facts"
            )
        )
    )


# ============================================================
# 4. IDENTIDADE DO PRODUTO
# ============================================================

def _pe_identity_from_event(
    event,
):
    if not isinstance(
        event,
        dict,
    ):
        return None

    platform = (
        _pe_clean_text(
            event.get(
                "platform"
            )
        )
        or
        "shopee"
    )

    raw = event.get(
        "raw_product"
    )

    if not isinstance(
        raw,
        dict,
    ):
        raw = {}

    item_id = (
        event.get(
            "item_id"
        )
        if event.get(
            "item_id"
        )
        is not None
        else raw.get(
            "item_id"
        )
    )

    shop_id = (
        event.get(
            "shop_id"
        )
        if event.get(
            "shop_id"
        )
        is not None
        else raw.get(
            "shop_id"
        )
    )

    if (
        item_id is None
        or shop_id is None
    ):
        return None

    return {
        "platform": str(
            platform
        ),
        "item_id": str(
            item_id
        ),
        "shop_id": str(
            shop_id
        ),
        "key": (
            str(platform),
            str(shop_id),
            str(item_id),
        ),
    }


def _pe_identity_key(
    platform,
    shop_id,
    item_id,
):
    if (
        platform is None
        or shop_id is None
        or item_id is None
    ):
        return None

    return (
        str(platform),
        str(shop_id),
        str(item_id),
    )


# ============================================================
# 5. INFORMACOES MANUAIS DO VENDEDOR
#
# A Interface podera chamar esta funcao.
#
# Exemplos:
#
# product_extractor_set_seller_info(
#     price="R$ 17,90",
#     additional_info=(
#         "Material em aco inox. "
#         "Acompanha 2 unidades. "
#         "Garantia de 3 meses."
#     ),
# )
#
# Opcionalmente:
#
# facts={
#     "material": "aco inox",
#     "garantia": "3 meses",
#     "quantidade": "2 unidades",
# }
#
# O texto livre NAO e interpretado aqui.
# Ele apenas e marcado como fonte seller.
# ============================================================

def product_extractor_set_seller_info(
    price=None,
    additional_info=None,
    facts=None,
    platform=None,
    shop_id=None,
    item_id=None,
    replace=False,
    emit_update=True,
):
    global _pending_seller_info

    if facts is None:
        facts = {}

    if not isinstance(
        facts,
        dict,
    ):
        raise TypeError(
            "facts deve ser um dicionario."
        )

    clean_additional = (
        _pe_clean_text(
            additional_info
        )
    )

    current_profile = (
        product_extractor_state.get(
            "current_profile"
        )
    )

    # --------------------------------------------------------
    # Se IDs nao foram informados, usa o produto atual.
    # --------------------------------------------------------
    if (
        shop_id is None
        or item_id is None
    ):
        if isinstance(
            current_profile,
            dict,
        ):
            identity = (
                current_profile.get(
                    "identity"
                )
                or {}
            )

            platform = (
                platform
                or identity.get(
                    "platform"
                )
            )
            shop_id = (
                shop_id
                if shop_id is not None
                else identity.get(
                    "shop_id"
                )
            )
            item_id = (
                item_id
                if item_id is not None
                else identity.get(
                    "item_id"
                )
            )

    platform = (
        platform
        or "shopee"
    )

    key = _pe_identity_key(
        platform,
        shop_id,
        item_id,
    )

    incoming = {
        "price": price,
        "additional_info": clean_additional,
        "facts": _pe_copy(
            facts
        ),
        "updated_at": _pe_now_iso(),
    }

    # --------------------------------------------------------
    # Ainda nao existe produto atual:
    # guarda para o primeiro produto detectado.
    # --------------------------------------------------------
    if key is None:
        if replace:
            _pending_seller_info = {
                "price": price,
                "additional_info": clean_additional,
                "facts": _pe_copy(
                    facts
                ),
                "updated_at": _pe_now_iso(),
            }
        else:
            if price is not None:
                _pending_seller_info[
                    "price"
                ] = price

            if clean_additional is not None:
                _pending_seller_info[
                    "additional_info"
                ] = clean_additional

            if facts:
                _pending_seller_info[
                    "facts"
                ].update(
                    _pe_copy(
                        facts
                    )
                )

            _pending_seller_info[
                "updated_at"
            ] = _pe_now_iso()

        return {
            "target": "pending_next_product",
            "seller_info": _pe_copy(
                _pending_seller_info
            ),
        }

    # --------------------------------------------------------
    # Informacao para produto conhecido.
    # --------------------------------------------------------
    existing = (
        _seller_info_by_product.get(
            key,
            {
                "price": None,
                "additional_info": None,
                "facts": {},
                "updated_at": None,
            },
        )
    )

    if replace:
        new_info = incoming

    else:
        new_info = _pe_copy(
            existing
        )

        if price is not None:
            new_info[
                "price"
            ] = price

        if clean_additional is not None:
            new_info[
                "additional_info"
            ] = clean_additional

        if facts:
            new_info[
                "facts"
            ].update(
                _pe_copy(
                    facts
                )
            )

        new_info[
            "updated_at"
        ] = _pe_now_iso()

    _seller_info_by_product[
        key
    ] = new_info

    # --------------------------------------------------------
    # Se alterou o produto atualmente exibido,
    # reconstrui o perfil e emite profile_updated.
    # --------------------------------------------------------
    current_key = (
        product_extractor_state.get(
            "current_key"
        )
    )

    if (
        emit_update
        and current_key == key
        and isinstance(
            product_extractor_state.get(
                "current_input_event"
            ),
            dict,
        )
    ):
        _pe_rebuild_current_profile_after_seller_update()

    return {
        "target": "product",
        "product_key": key,
        "seller_info": _pe_copy(
            new_info
        ),
    }


def product_extractor_clear_seller_info(
    platform=None,
    shop_id=None,
    item_id=None,
    emit_update=True,
):
    global _pending_seller_info

    current_profile = (
        product_extractor_state.get(
            "current_profile"
        )
    )

    if (
        shop_id is None
        or item_id is None
    ):
        if isinstance(
            current_profile,
            dict,
        ):
            identity = (
                current_profile.get(
                    "identity"
                )
                or {}
            )

            platform = (
                platform
                or identity.get(
                    "platform"
                )
            )
            shop_id = (
                shop_id
                if shop_id is not None
                else identity.get(
                    "shop_id"
                )
            )
            item_id = (
                item_id
                if item_id is not None
                else identity.get(
                    "item_id"
                )
            )

    key = _pe_identity_key(
        platform or "shopee",
        shop_id,
        item_id,
    )

    if key is None:
        _pending_seller_info = {
            "price": None,
            "additional_info": None,
            "facts": {},
            "updated_at": None,
        }

        return {
            "cleared": "pending_next_product"
        }

    _seller_info_by_product.pop(
        key,
        None,
    )

    if (
        emit_update
        and
        product_extractor_state.get(
            "current_key"
        )
        ==
        key
        and isinstance(
            product_extractor_state.get(
                "current_input_event"
            ),
            dict,
        )
    ):
        _pe_rebuild_current_profile_after_seller_update()

    return {
        "cleared": key
    }


def product_extractor_get_seller_info(
    platform=None,
    shop_id=None,
    item_id=None,
):
    current_profile = (
        product_extractor_state.get(
            "current_profile"
        )
    )

    if (
        shop_id is None
        or item_id is None
    ):
        if isinstance(
            current_profile,
            dict,
        ):
            identity = (
                current_profile.get(
                    "identity"
                )
                or {}
            )

            platform = (
                platform
                or identity.get(
                    "platform"
                )
            )
            shop_id = (
                shop_id
                if shop_id is not None
                else identity.get(
                    "shop_id"
                )
            )
            item_id = (
                item_id
                if item_id is not None
                else identity.get(
                    "item_id"
                )
            )

    key = _pe_identity_key(
        platform or "shopee",
        shop_id,
        item_id,
    )

    if key is None:
        return _pe_copy(
            _pending_seller_info
        )

    return _pe_copy(
        _seller_info_by_product.get(
            key
        )
    )


# ============================================================
# 6. APLICAR INFORMACOES PENDENTES
# ============================================================

def _pe_attach_pending_if_needed(
    key,
):
    global _pending_seller_info

    if key is None:
        return

    if key in _seller_info_by_product:
        return

    if not _pe_has_seller_info(
        _pending_seller_info
    ):
        return

    _seller_info_by_product[
        key
    ] = _pe_copy(
        _pending_seller_info
    )

    # A informacao pendente pertence ao primeiro produto
    # detectado e nao deve vazar para o seguinte.
    _pending_seller_info = {
        "price": None,
        "additional_info": None,
        "facts": {},
        "updated_at": None,
    }


# ============================================================
# 7. PRODUTO / PRECO
# ============================================================

def _pe_build_product_section(
    raw,
):
    name = _pe_direct_fact(
        raw,
        [
            "name",
            "title",
            "item_name",
            "product_name",
        ],
        source="shopee",
    )

    image = _pe_direct_fact(
        raw,
        [
            "image",
            "image_url",
            "item_image",
            "cover",
            "cover_image",
        ],
        source="shopee",
    )

    brand = _pe_direct_fact(
        raw,
        [
            "brand",
            "brand_name",
        ],
        source="shopee",
    )

    item_type = _pe_direct_fact(
        raw,
        [
            "item_type",
            "type",
        ],
        source="shopee",
    )

    category_ids = _pe_direct_fact(
        raw,
        [
            "category_ids",
            "categories",
            "category_id",
        ],
        source="shopee",
    )

    return {
        "name": name,
        "image": image,
        "brand": brand,
        "item_type": item_type,
        "category_ids": category_ids,

        # O titulo pode conter caracteristicas, mas o Extractor
        # NAO interpreta palavras do titulo como fatos separados.
        "title_text": _pe_fact(
            value=(
                name.get(
                    "value"
                )
                if name.get(
                    "known"
                )
                else None
            ),
            source="shopee_title",
            source_field=(
                name.get(
                    "source_field"
                )
                if name.get(
                    "known"
                )
                else None
            ),
            note=(
                "Texto original do titulo; "
                "nao interpretado pelo Extractor."
            ),
        ),
    }


def _pe_build_price_section(
    raw,
    seller_info,
):
    currency = _pe_direct_fact(
        raw,
        [
            "currency",
            "currency_code",
        ],
        source="shopee",
    )

    current = (
        _pe_normalized_numeric_fact(
            raw,
            [
                "price",
                "current_price",
                "price_min",
                "min_price",
            ],
            source="shopee",
        )
    )

    previous = (
        _pe_normalized_numeric_fact(
            raw,
            [
                "price_before_discount",
                "original_price",
                "price_min_before_discount",
                "before_discount_price",
            ],
            source="shopee",
        )
    )

    price_max = (
        _pe_normalized_numeric_fact(
            raw,
            [
                "price_max",
                "max_price",
            ],
            source="shopee",
        )
    )

    previous_max = (
        _pe_normalized_numeric_fact(
            raw,
            [
                "price_max_before_discount",
                "max_price_before_discount",
            ],
            source="shopee",
        )
    )

    out_of_live = (
        _pe_normalized_numeric_fact(
            raw,
            [
                "out_of_live_price",
            ],
            source="shopee",
        )
    )

    discount = (
        _pe_normalized_numeric_fact(
            raw,
            [
                "discount",
                "discount_percent",
                "discount_percentage",
            ],
            source="shopee",
        )
    )

    # --------------------------------------------------------
    # Se a Shopee nao informou o percentual mas informou
    # preco atual e anterior, podemos calcular explicitamente
    # como dado derivado.
    # --------------------------------------------------------
    current_number = (
        _pe_fact_numeric_value(
            current
        )
    )
    previous_number = (
        _pe_fact_numeric_value(
            previous
        )
    )

    if (
        not discount.get(
            "known"
        )
        and current_number is not None
        and previous_number is not None
        and previous_number > 0
        and previous_number >= current_number
    ):
        derived_discount = round(
            (
                (
                    previous_number
                    -
                    current_number
                )
                /
                previous_number
            )
            *
            100,
            2,
        )

        discount = _pe_fact(
            value={
                "raw": None,
                "normalized": derived_discount,
            },
            source="derived",
            source_field=(
                "price + price_before_discount"
            ),
            note=(
                "Percentual calculado; "
                "nao veio pronto da Shopee."
            ),
        )

    # --------------------------------------------------------
    # Preco informado manualmente.
    # --------------------------------------------------------
    seller_price_raw = (
        seller_info.get(
            "price"
        )
        if isinstance(
            seller_info,
            dict,
        )
        else None
    )

    seller_price_number = (
        _pe_numeric(
            seller_price_raw
        )
    )

    seller_price = _pe_fact(
        value=(
            {
                "raw": _pe_copy(
                    seller_price_raw
                ),
                "normalized": seller_price_number,
            }
            if _pe_known(
                seller_price_raw
            )
            else None
        ),
        source="seller",
        source_field="seller.price",
    )

    # Regra definida para o Coach Vendas:
    # vendedor pode informar um preco operacional diferente,
    # mas o valor automatico permanece preservado.
    effective = (
        _pe_copy(
            seller_price
        )
        if seller_price.get(
            "known"
        )
        else _pe_copy(
            current
        )
    )

    shopee_price_number = (
        _pe_fact_numeric_value(
            current
        )
    )

    conflict = False

    if (
        seller_price_number is not None
        and shopee_price_number is not None
    ):
        conflict = (
            abs(
                seller_price_number
                -
                shopee_price_number
            )
            >
            0.005
        )

    return {
        "currency": currency,

        "shopee_current": current,
        "shopee_max": price_max,

        "shopee_previous": previous,
        "shopee_previous_max": previous_max,

        "out_of_live_price": out_of_live,
        "discount_percent": discount,

        "seller_price": seller_price,

        "effective_price": effective,
        "effective_price_source": (
            effective.get(
                "source"
            )
            if isinstance(
                effective,
                dict,
            )
            else None
        ),

        "seller_shopee_conflict": conflict,
    }


# ============================================================
# 8. PROVA SOCIAL
# ============================================================

def _pe_build_social_proof_section(
    raw,
):
    sold = (
        _pe_normalized_numeric_fact(
            raw,
            [
                "sold",
                "sold_count",
                "historical_sold",
                "sales",
            ],
            source="shopee",
        )
    )

    rating = (
        _pe_normalized_numeric_fact(
            raw,
            [
                "rating",
                "rating_star",
                "star",
                "item_rating",
            ],
            source="shopee",
        )
    )

    rating_count = (
        _pe_normalized_numeric_fact(
            raw,
            [
                "rating_count",
                "rating_total",
                "review_count",
                "ratings",
            ],
            source="shopee",
        )
    )

    popularity = _pe_direct_fact(
        raw,
        [
            "popularity",
            "label",
            "labels",
        ],
        source="shopee",
    )

    return {
        "sold": sold,
        "rating": rating,
        "rating_count": rating_count,
        "popularity": popularity,
    }


# ============================================================
# 9. DISPONIBILIDADE
# ============================================================

def _pe_build_availability_section(
    raw,
):
    stock = (
        _pe_normalized_numeric_fact(
            raw,
            [
                "display_total_stock",
                "stock",
                "stock_count",
                "total_stock",
            ],
            source="shopee",
        )
    )

    is_oos = _pe_direct_fact(
        raw,
        [
            "is_oos",
            "out_of_stock",
        ],
        source="shopee",
    )

    status = _pe_direct_fact(
        raw,
        [
            "status",
            "item_status",
        ],
        source="shopee",
    )

    location_stocks = _pe_direct_fact(
        raw,
        [
            "location_stocks",
        ],
        source="shopee",
    )

    return {
        "stock": stock,
        "is_oos": is_oos,
        "status": status,
        "location_stocks": location_stocks,
    }


# ============================================================
# 10. PROMOCOES
#
# O Extractor preserva estruturas e valores informados.
# Ele NAO afirma sozinho que uma promocao esta ativa agora,
# salvo quando a propria estrutura trouxer esse estado.
# ============================================================

def _pe_build_promotions_section(
    raw,
    price_section,
):
    item_promotion = _pe_direct_fact(
        raw,
        [
            "item_promotion",
        ],
        source="shopee",
        note=(
            "Estrutura bruta de promocao da Shopee."
        ),
    )

    applied_promotion = _pe_direct_fact(
        raw,
        [
            "applied_promotion_info",
            "applied_promotion",
        ],
        source="shopee",
        note=(
            "Estrutura informada pela Shopee; "
            "validacao temporal fica para modulo posterior."
        ),
    )

    vouchers = _pe_direct_fact(
        raw,
        [
            "vouchers",
            "voucher",
            "voucher_list",
        ],
        source="shopee",
    )

    promotion = _pe_direct_fact(
        raw,
        [
            "promotion",
            "promotions",
            "live_promotion",
        ],
        source="shopee",
    )

    return {
        "discount_percent": _pe_copy(
            price_section.get(
                "discount_percent"
            )
        ),
        "item_promotion": item_promotion,
        "applied_promotion_info": applied_promotion,
        "vouchers": vouchers,
        "promotion": promotion,

        # Sinal para impedir que o proximo modulo trate
        # qualquer objeto promocional como "ativo agora"
        # sem verificar a propria estrutura/timestamps.
        "requires_active_validation": True,
    }


# ============================================================
# 11. CARACTERISTICAS
#
# Somente campos estruturados e informacoes do vendedor.
# O titulo e preservado, mas nao e "interpretado".
# ============================================================

def _pe_build_characteristics_section(
    raw,
    seller_info,
):
    aliases = {
        "material": [
            "material",
        ],
        "color": [
            "color",
            "colour",
        ],
        "size": [
            "size",
        ],
        "model": [
            "model",
            "model_name",
        ],
        "variation": [
            "variation",
            "variations",
            "models",
        ],
        "attributes": [
            "attributes",
            "attribute_list",
        ],
        "specifications": [
            "specifications",
            "specs",
        ],
        "description": [
            "description",
            "short_description",
        ],
    }

    structured = {}

    for normalized_name, fields in aliases.items():
        structured[
            normalized_name
        ] = _pe_direct_fact(
            raw,
            fields,
            source="shopee",
        )

    seller_facts = {}

    if isinstance(
        seller_info,
        dict,
    ):
        for key, value in (
            seller_info.get(
                "facts"
            )
            or {}
        ).items():
            clean_key = _pe_clean_text(
                key
            )

            if not clean_key:
                continue

            seller_facts[
                clean_key
            ] = _pe_fact(
                value=value,
                source="seller",
                source_field=(
                    f"seller.facts.{clean_key}"
                ),
            )

    seller_notes = _pe_fact(
        value=(
            seller_info.get(
                "additional_info"
            )
            if isinstance(
                seller_info,
                dict,
            )
            else None
        ),
        source="seller",
        source_field="seller.additional_info",
        note=(
            "Texto livre fornecido pelo vendedor; "
            "nao interpretado pelo Extractor."
        ),
    )

    return {
        "structured_shopee": structured,
        "seller_facts": seller_facts,
        "seller_notes": seller_notes,
    }


# ============================================================
# 12. CONFLITOS DE INFORMACAO
# ============================================================

def _pe_build_conflicts(
    profile,
):
    conflicts = []

    price = (
        profile.get(
            "price"
        )
        or {}
    )

    if price.get(
        "seller_shopee_conflict"
    ):
        conflicts.append({
            "field": "price",
            "shopee": _pe_copy(
                price.get(
                    "shopee_current"
                )
            ),
            "seller": _pe_copy(
                price.get(
                    "seller_price"
                )
            ),
            "resolution_for_guidance": (
                "seller"
            ),
            "note": (
                "Os dois valores foram preservados. "
                "O preco informado pelo vendedor e o "
                "valor efetivo para orientacao comercial."
            ),
        })

    # --------------------------------------------------------
    # Caracteristicas estruturadas:
    # se vendedor usar a mesma chave de um campo Shopee
    # e os valores forem diferentes, registramos o conflito.
    # Nao escolhemos automaticamente um "verdadeiro".
    # --------------------------------------------------------
    characteristics = (
        profile.get(
            "characteristics"
        )
        or {}
    )

    shopee_structured = (
        characteristics.get(
            "structured_shopee"
        )
        or {}
    )

    seller_facts = (
        characteristics.get(
            "seller_facts"
        )
        or {}
    )

    for key, seller_fact in seller_facts.items():
        shopee_fact = (
            shopee_structured.get(
                key
            )
        )

        if (
            not isinstance(
                shopee_fact,
                dict,
            )
            or
            not shopee_fact.get(
                "known"
            )
            or
            not seller_fact.get(
                "known"
            )
        ):
            continue

        shopee_value = shopee_fact.get(
            "value"
        )
        seller_value = seller_fact.get(
            "value"
        )

        if str(
            shopee_value
        ).strip().lower() != str(
            seller_value
        ).strip().lower():
            conflicts.append({
                "field": (
                    f"characteristics.{key}"
                ),
                "shopee": _pe_copy(
                    shopee_fact
                ),
                "seller": _pe_copy(
                    seller_fact
                ),
                "resolution_for_guidance": (
                    "unresolved"
                ),
                "note": (
                    "Conflito preservado; "
                    "modulos posteriores nao devem "
                    "afirmar um dos valores como fato "
                    "sem regra explicita."
                ),
            })

    return conflicts


# ============================================================
# 13. MONTAR PRODUCT PROFILE
# ============================================================

def _pe_build_profile(
    event,
):
    if not isinstance(
        event,
        dict,
    ):
        raise TypeError(
            "Evento de entrada deve ser um dicionario."
        )

    raw = event.get(
        "raw_product"
    )

    if not isinstance(
        raw,
        dict,
    ):
        raise ValueError(
            "Evento sem raw_product valido."
        )

    identity = (
        _pe_identity_from_event(
            event
        )
    )

    if identity is None:
        raise ValueError(
            "Nao foi possivel identificar item_id + shop_id."
        )

    key = identity[
        "key"
    ]

    _pe_attach_pending_if_needed(
        key
    )

    seller_info = (
        _seller_info_by_product.get(
            key,
            {
                "price": None,
                "additional_info": None,
                "facts": {},
                "updated_at": None,
            },
        )
    )

    product_section = (
        _pe_build_product_section(
            raw
        )
    )

    price_section = (
        _pe_build_price_section(
            raw,
            seller_info,
        )
    )

    social_section = (
        _pe_build_social_proof_section(
            raw
        )
    )

    availability_section = (
        _pe_build_availability_section(
            raw
        )
    )

    promotions_section = (
        _pe_build_promotions_section(
            raw,
            price_section,
        )
    )

    characteristics_section = (
        _pe_build_characteristics_section(
            raw,
            seller_info,
        )
    )

    profile = {
        "schema_version": (
            PRODUCT_PROFILE_SCHEMA_VERSION
        ),

        "identity": {
            "platform": identity[
                "platform"
            ],
            "session_id": (
                str(
                    event.get(
                        "session_id"
                    )
                )
                if event.get(
                    "session_id"
                )
                is not None
                else None
            ),
            "item_id": identity[
                "item_id"
            ],
            "shop_id": identity[
                "shop_id"
            ],
            "key": identity[
                "key"
            ],
        },

        "product": product_section,
        "price": price_section,
        "social_proof": social_section,
        "availability": availability_section,
        "promotions": promotions_section,
        "characteristics": characteristics_section,

        "seller_input": {
            "used": _pe_has_seller_info(
                seller_info
            ),
            "price": _pe_copy(
                seller_info.get(
                    "price"
                )
            ),
            "additional_info": _pe_copy(
                seller_info.get(
                    "additional_info"
                )
            ),
            "facts": _pe_copy(
                seller_info.get(
                    "facts"
                )
                or {}
            ),
            "updated_at": seller_info.get(
                "updated_at"
            ),
        },

        "provenance": {
            "automatic_source": (
                "shopee_show_item"
            ),
            "worker_event_type": (
                event.get(
                    "type"
                )
            ),
            "worker_observed_at": (
                event.get(
                    "observed_at"
                )
            ),
            "profile_built_at": (
                _pe_now_iso()
            ),
            "seller_input_used": (
                _pe_has_seller_info(
                    seller_info
                )
            ),
        },

        # O raw nao segue para a parte principal do perfil.
        # Mantemos somente um mapa dos campos existentes,
        # util para diagnostico sem duplicar o JSON inteiro.
        "source_snapshot": {
            "available_raw_fields": sorted(
                str(
                    key
                )
                for key in raw.keys()
            ),
        },

        "conflicts": [],
    }

    profile[
        "conflicts"
    ] = _pe_build_conflicts(
        profile
    )

    return profile


# ============================================================
# 14. EMITIR SAIDA
# ============================================================

def _pe_emit(
    output_type,
    profile=None,
    previous_profile=None,
    reason=None,
):
    global _product_extractor_seq

    _product_extractor_seq += 1

    output = {
        "seq": _product_extractor_seq,
        "type": output_type,
        "platform": "shopee",
        "emitted_at": _pe_now_iso(),
        "reason": reason,

        "profile": _pe_copy(
            profile
        ),

        "previous_profile": _pe_copy(
            previous_profile
        ),
    }

    product_extractor_history.append(
        _pe_copy(
            output
        )
    )

    try:
        product_extractor_output_queue.put_nowait(
            _pe_copy(
                output
            )
        )
    except Exception:
        pass

    product_extractor_state[
        "last_output_at"
    ] = time.time()

    return output


# ============================================================
# 15. PROCESSAR EVENTO DO WORKER SHOPEE PRODUTO
# ============================================================

def product_extractor_process_event(
    event,
):
    if not isinstance(
        event,
        dict,
    ):
        raise TypeError(
            "Evento deve ser um dicionario."
        )

    event_type = event.get(
        "type"
    )

    if event_type not in {
        "product_started",
        "product_changed",
        "product_cleared",
    }:
        # Eventos desconhecidos nao viram Product Profile.
        return None

    product_extractor_state[
        "processed_events"
    ] += 1
    product_extractor_state[
        "last_input_at"
    ] = time.time()
    product_extractor_state[
        "last_error"
    ] = None

    # --------------------------------------------------------
    # Produto saiu da tela.
    # --------------------------------------------------------
    if event_type == "product_cleared":
        previous = _pe_copy(
            product_extractor_state.get(
                "current_profile"
            )
        )

        product_extractor_state[
            "previous_profile"
        ] = previous
        product_extractor_state[
            "current_profile"
        ] = None
        product_extractor_state[
            "current_key"
        ] = None
        product_extractor_state[
            "current_input_event"
        ] = None
        product_extractor_state[
            "profiles_cleared"
        ] += 1

        return _pe_emit(
            "product_profile_cleared",
            profile=None,
            previous_profile=previous,
            reason=(
                "worker_product_cleared"
            ),
        )

    # --------------------------------------------------------
    # Produto presente.
    # --------------------------------------------------------
    profile = _pe_build_profile(
        event
    )

    previous = _pe_copy(
        product_extractor_state.get(
            "current_profile"
        )
    )

    identity = (
        profile.get(
            "identity"
        )
        or {}
    )

    key = identity.get(
        "key"
    )

    product_extractor_state[
        "previous_profile"
    ] = previous
    product_extractor_state[
        "current_profile"
    ] = _pe_copy(
        profile
    )
    product_extractor_state[
        "current_key"
    ] = key
    product_extractor_state[
        "current_input_event"
    ] = _pe_copy(
        event
    )

    if event_type == "product_started":
        product_extractor_state[
            "profiles_started"
        ] += 1

        return _pe_emit(
            "product_profile_started",
            profile=profile,
            previous_profile=previous,
            reason=(
                "worker_product_started"
            ),
        )

    product_extractor_state[
        "profiles_changed"
    ] += 1

    return _pe_emit(
        "product_profile_changed",
        profile=profile,
        previous_profile=previous,
        reason=(
            "worker_product_changed"
        ),
    )


# ============================================================
# 16. RECONSTRUIR PERFIL QUANDO O VENDEDOR EDITA DADOS
# ============================================================

def _pe_rebuild_current_profile_after_seller_update():
    event = (
        product_extractor_state.get(
            "current_input_event"
        )
    )

    if not isinstance(
        event,
        dict,
    ):
        return None

    previous = _pe_copy(
        product_extractor_state.get(
            "current_profile"
        )
    )

    profile = _pe_build_profile(
        event
    )

    product_extractor_state[
        "previous_profile"
    ] = previous
    product_extractor_state[
        "current_profile"
    ] = _pe_copy(
        profile
    )
    product_extractor_state[
        "profiles_updated"
    ] += 1

    return _pe_emit(
        "product_profile_updated",
        profile=profile,
        previous_profile=previous,
        reason="seller_info_updated",
    )


# ============================================================
# 17. SUPERVISOR
#
# Conecta SOMENTE ao Worker Shopee Produto.
#
# Requer que a celula do Worker Shopee Produto V1 tenha sido
# executada antes, pois utiliza:
#
# shopee_product_next_event()
#
# Nao existe conexao com Live Engine nem Coaches antigos.
# ============================================================

async def product_extractor_supervisor():
    global _product_extractor_stop_requested

    if (
        "shopee_product_next_event"
        not in globals()
        or
        not callable(
            globals().get(
                "shopee_product_next_event"
            )
        )
    ):
        raise RuntimeError(
            "Worker Shopee Produto nao esta carregado. "
            "Execute primeiro a celula do Worker Shopee Produto V1."
        )

    _product_extractor_stop_requested = False

    product_extractor_state[
        "running"
    ] = True
    product_extractor_state[
        "last_error"
    ] = None

    try:
        while not _product_extractor_stop_requested:
            try:
                event = await (
                    shopee_product_next_event(
                        timeout=1.0
                    )
                )

            except asyncio.TimeoutError:
                continue

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                product_extractor_state[
                    "last_error"
                ] = str(exc)

                await asyncio.sleep(
                    0.5
                )
                continue

            try:
                product_extractor_process_event(
                    event
                )

            except Exception as exc:
                product_extractor_state[
                    "last_error"
                ] = str(exc)

    finally:
        product_extractor_state[
            "running"
        ] = False


# Alias claro para futura Interface.
async def executar_product_extractor():
    return await product_extractor_supervisor()


# ============================================================
# 18. PARAR / RESET
# ============================================================

def product_extractor_stop():
    global _product_extractor_stop_requested

    _product_extractor_stop_requested = True


def product_extractor_reset(
    clear_history=True,
    clear_output_queue=True,
    clear_seller_info=False,
):
    global _product_extractor_stop_requested
    global _product_extractor_seq
    global _pending_seller_info

    _product_extractor_stop_requested = False
    _product_extractor_seq = 0

    product_extractor_state.update({
        "version": PRODUCT_EXTRACTOR_VERSION,
        "schema_version": PRODUCT_PROFILE_SCHEMA_VERSION,

        "running": False,
        "processed_events": 0,
        "profiles_started": 0,
        "profiles_changed": 0,
        "profiles_updated": 0,
        "profiles_cleared": 0,

        "current_key": None,
        "current_profile": None,
        "previous_profile": None,
        "current_input_event": None,

        "last_input_at": None,
        "last_output_at": None,
        "last_error": None,
    })

    if clear_history:
        product_extractor_history.clear()

    if clear_output_queue:
        try:
            while True:
                product_extractor_output_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass

    if clear_seller_info:
        _seller_info_by_product.clear()

        _pending_seller_info = {
            "price": None,
            "additional_info": None,
            "facts": {},
            "updated_at": None,
        }


# ============================================================
# 19. API PUBLICA PARA PRODUCT SALES BUILDER / DIAGNOSTICO
# ============================================================

async def product_extractor_next_output(
    timeout=None,
):
    if timeout is None:
        return await (
            product_extractor_output_queue.get()
        )

    return await asyncio.wait_for(
        product_extractor_output_queue.get(),
        timeout=float(
            timeout
        ),
    )


def product_extractor_current_profile():
    return _pe_copy(
        product_extractor_state.get(
            "current_profile"
        )
    )


def product_extractor_previous_profile():
    return _pe_copy(
        product_extractor_state.get(
            "previous_profile"
        )
    )


def product_extractor_recent_outputs(
    limit=20,
):
    try:
        limit = max(
            1,
            int(
                limit
            ),
        )
    except Exception:
        limit = 20

    return _pe_copy(
        list(
            product_extractor_history
        )[-limit:]
    )


def product_extractor_status():
    return {
        "version": (
            product_extractor_state.get(
                "version"
            )
        ),
        "schema_version": (
            product_extractor_state.get(
                "schema_version"
            )
        ),
        "running": (
            product_extractor_state.get(
                "running"
            )
        ),
        "processed_events": (
            product_extractor_state.get(
                "processed_events"
            )
        ),
        "profiles_started": (
            product_extractor_state.get(
                "profiles_started"
            )
        ),
        "profiles_changed": (
            product_extractor_state.get(
                "profiles_changed"
            )
        ),
        "profiles_updated": (
            product_extractor_state.get(
                "profiles_updated"
            )
        ),
        "profiles_cleared": (
            product_extractor_state.get(
                "profiles_cleared"
            )
        ),
        "current_key": _pe_copy(
            product_extractor_state.get(
                "current_key"
            )
        ),
        "history_size": len(
            product_extractor_history
        ),
        "output_queue_size": (
            product_extractor_output_queue.qsize()
        ),
        "seller_profiles_count": len(
            _seller_info_by_product
        ),
        "pending_seller_info": (
            _pe_copy(
                _pending_seller_info
            )
        ),
        "last_error": (
            product_extractor_state.get(
                "last_error"
            )
        ),
    }


# ============================================================
# 20. PAINEL DE DIAGNOSTICO
#
# Nao consome fila.
# ============================================================

def mostrar_product_extractor():
    status = (
        product_extractor_status()
    )

    profile = (
        product_extractor_current_profile()
    )

    print("=" * 72)
    print(
        "AGCN LIVE - PRODUCT EXTRACTOR V1.0"
    )
    print("=" * 72)

    print(
        "Executando:",
        status[
            "running"
        ],
    )
    print(
        "Eventos processados:",
        status[
            "processed_events"
        ],
    )
    print(
        "Perfis iniciados:",
        status[
            "profiles_started"
        ],
    )
    print(
        "Trocas:",
        status[
            "profiles_changed"
        ],
    )
    print(
        "Atualizacoes do vendedor:",
        status[
            "profiles_updated"
        ],
    )
    print(
        "Perfis limpos:",
        status[
            "profiles_cleared"
        ],
    )
    print(
        "Erro:",
        status[
            "last_error"
        ],
    )
    print()

    if not profile:
        print(
            "PRODUCT PROFILE: nenhum produto atual."
        )
        return

    identity = (
        profile.get(
            "identity"
        )
        or {}
    )

    product = (
        profile.get(
            "product"
        )
        or {}
    )

    price = (
        profile.get(
            "price"
        )
        or {}
    )

    social = (
        profile.get(
            "social_proof"
        )
        or {}
    )

    availability = (
        profile.get(
            "availability"
        )
        or {}
    )

    print("PRODUCT PROFILE")
    print(
        "item_id:",
        identity.get(
            "item_id"
        ),
    )
    print(
        "shop_id:",
        identity.get(
            "shop_id"
        ),
    )

    name_fact = (
        product.get(
            "name"
        )
        or {}
    )

    print(
        "nome:",
        name_fact.get(
            "value"
        ),
    )

    effective_price = (
        price.get(
            "effective_price"
        )
        or {}
    )

    print(
        "preco efetivo:",
        effective_price.get(
            "value"
        ),
    )
    print(
        "origem do preco:",
        price.get(
            "effective_price_source"
        ),
    )
    print(
        "conflito de preco:",
        price.get(
            "seller_shopee_conflict"
        ),
    )

    sold = (
        social.get(
            "sold"
        )
        or {}
    )

    rating = (
        social.get(
            "rating"
        )
        or {}
    )

    stock = (
        availability.get(
            "stock"
        )
        or {}
    )

    print(
        "vendidos:",
        sold.get(
            "value"
        ),
    )
    print(
        "avaliacao:",
        rating.get(
            "value"
        ),
    )
    print(
        "estoque:",
        stock.get(
            "value"
        ),
    )

    seller_input = (
        profile.get(
            "seller_input"
        )
        or {}
    )

    print(
        "informacao do vendedor:",
        seller_input.get(
            "used"
        ),
    )
    print(
        "conflitos registrados:",
        len(
            profile.get(
                "conflicts"
            )
            or []
        ),
    )
    print()
    print(
        "Saidas aguardando Product Sales Builder:",
        status[
            "output_queue_size"
        ],
    )


# ============================================================
# 21. AUTO-TESTE LOCAL
#
# Nao acessa a Shopee.
# Nao consome fila do Worker.
# Serve apenas para validar a logica interna.
# ============================================================

def product_extractor_self_test():
    product_extractor_reset(
        clear_history=True,
        clear_output_queue=True,
        clear_seller_info=True,
    )

    event = {
        "type": "product_started",
        "platform": "shopee",
        "session_id": "TESTE",
        "item_id": 123,
        "shop_id": 456,
        "observed_at": _pe_now_iso(),
        "raw_product": {
            "item_id": 123,
            "shop_id": 456,
            "name": "Produto Teste",
            "currency": "BRL",
            "price": 11.99,
            "price_before_discount": 25.00,
            "discount": 52,
            "sold": 20000,
            "rating": 4.9,
            "rating_count": 3500,
            "display_total_stock": 716,
            "is_oos": False,
            "category_ids": [1, 2],
            "item_promotion": {
                "example": True
            },
        },
    }

    first = (
        product_extractor_process_event(
            event
        )
    )

    assert (
        first["type"]
        ==
        "product_profile_started"
    )

    profile = first[
        "profile"
    ]

    assert (
        profile[
            "identity"
        ][
            "item_id"
        ]
        ==
        "123"
    )

    assert (
        profile[
            "price"
        ][
            "effective_price_source"
        ]
        ==
        "shopee"
    )

    product_extractor_set_seller_info(
        price="R$ 10,90",
        additional_info=(
            "Informacao manual de teste."
        ),
        facts={
            "material": "aco inox"
        },
    )

    updated = (
        product_extractor_current_profile()
    )

    assert (
        updated[
            "price"
        ][
            "effective_price_source"
        ]
        ==
        "seller"
    )

    assert (
        updated[
            "price"
        ][
            "seller_shopee_conflict"
        ]
        is True
    )

    assert (
        updated[
            "seller_input"
        ][
            "used"
        ]
        is True
    )

    clear_event = {
        "type": "product_cleared",
        "platform": "shopee",
        "session_id": "TESTE",
        "item_id": None,
        "shop_id": None,
        "observed_at": _pe_now_iso(),
        "raw_product": None,
    }

    cleared = (
        product_extractor_process_event(
            clear_event
        )
    )

    assert (
        cleared["type"]
        ==
        "product_profile_cleared"
    )

    assert (
        product_extractor_current_profile()
        is None
    )

    return {
        "ok": True,
        "tests": 10,
        "version": (
            PRODUCT_EXTRACTOR_VERSION
        ),
    }


# ============================================================
# 22. ORDEM DE USO NO COLAB
#
# 1. Worker Shopee Produto V1
# 2. Product Extractor V1
#
# Durante o teste:
#
# tarefa_extractor = asyncio.create_task(
#     executar_product_extractor()
# )
#
# O Worker Shopee Produto emite:
# product_started / product_changed / product_cleared
#
# O Product Extractor emite:
# product_profile_started
# product_profile_changed
# product_profile_updated
# product_profile_cleared
#
# Futuramente:
#
# Product Sales Builder consumira exclusivamente:
# product_extractor_next_output()
#
# NAO existe await automatico neste arquivo.
# ============================================================

print(
    "AGCN Product Extractor V1.0 carregado."
)
