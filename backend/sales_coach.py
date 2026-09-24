"""
AGCN LIVE / ALIVE - SALES COACH V2

Motor deterministico de orientacao comercial em tempo real.

Entrada:
- envelopes do canal "sales" do Comment Dispatcher V1.1;
- Product Context V1;
- passagem do tempo para orientacoes proativas.

Saida:
- sales_coach_message para a interface;
- orientacao ao VENDEDOR, nunca resposta automatica ao cliente.

Principios:
- Leve e Maximo possuem as mesmas capacidades;
- Maximo aumenta frequencia, intensidade e conducao para fechamento;
- nunca inventa frete, estoque, garantia, beneficio, urgencia ou oferta;
- usa somente fatos presentes no Product Context;
- preserva o comentario original quando a orientacao for reativa;
- evita repeticao recente;
- funciona igualmente para Shopee e TikTok.
"""

from __future__ import annotations

import asyncio
import copy
import re
import time
import unicodedata
import uuid
from collections import deque
from typing import Awaitable, Callable, Optional

from .product_context import ProductContext


SALES_COACH_VERSION = "2.0"

SALES_COACH_HISTORY_LIMIT = 300
SALES_COACH_RECENT_SIGNATURES = 80
SALES_COACH_OUTPUT_QUEUE_LIMIT = 500

SALES_COACH_MODE_CONFIG = {
    "leve": {
        "reactive_cooldown": 5.0,
        "proactive_interval": 45.0,
        "display_seconds": 11,
        "repeat_guard_seconds": 55.0,
    },
    "maximo": {
        "reactive_cooldown": 2.0,
        "proactive_interval": 22.0,
        "display_seconds": 13,
        "repeat_guard_seconds": 32.0,
    },
}


def _norm(value):
    text = str(value or "").strip().lower()
    text = "".join(
        ch
        for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _money(value):
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return (
        "R$ "
        + f"{number:,.2f}"
        .replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )


def _percent(value):
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    if number.is_integer():
        return f"{int(number)}%"

    return (
        f"{number:.2f}"
        .rstrip("0")
        .rstrip(".")
        .replace(".", ",")
        + "%"
    )


def _trim(value, limit=220):
    text = re.sub(
        r"\s+",
        " ",
        str(value or "").strip(),
    )
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _contains_any(text, terms):
    normalized = _norm(text)
    return any(
        _norm(term) in normalized
        for term in terms
    )


def _sentence_candidates(text):
    raw = str(text or "").strip()
    if not raw:
        return []

    parts = re.split(
        r"(?<=[.!?;])\s+|\n+",
        raw,
    )

    return [
        part.strip(" -•\t")
        for part in parts
        if part.strip(" -•\t")
    ]


FACT_TERMS = {
    "price": {
        "preco",
        "preço",
        "valor",
        "custa",
        "quanto",
    },
    "promotion": {
        "promocao",
        "promoção",
        "desconto",
        "cupom",
        "oferta",
    },
    "shipping": {
        "frete",
        "envio",
        "entrega gratis",
        "entrega grátis",
    },
    "delivery": {
        "prazo",
        "chega",
        "entrega",
        "dias",
    },
    "stock": {
        "estoque",
        "disponivel",
        "disponível",
        "tem ainda",
    },
    "size": {
        "tamanho",
        "medida",
        "dimensao",
        "dimensão",
        "ml",
        "litro",
        "grama",
        "kg",
        "cm",
    },
    "color": {
        "cor",
        "cores",
        "rosa",
        "preto",
        "branco",
        "azul",
        "vermelho",
        "verde",
    },
    "composition": {
        "material",
        "composicao",
        "composição",
        "ingrediente",
        "ingredientes",
        "oleo",
        "óleo",
        "algodao",
        "algodão",
    },
    "use": {
        "serve",
        "usar",
        "uso",
        "aplicar",
        "funciona",
        "indicado",
        "indicada",
    },
    "warranty": {
        "garantia",
        "troca",
    },
    "payment": {
        "pagamento",
        "parcela",
        "parcelado",
        "pix",
        "cartao",
        "cartão",
    },
    "brand": {
        "marca",
        "fabricante",
    },
    "authenticity": {
        "original",
        "autentico",
        "autêntico",
        "falso",
    },
}


SUBTYPE_FACT_KEY = {
    "price": "price",
    "promotion_coupon": "promotion",
    "promotion_problem": "promotion",
    "shipping": "shipping",
    "delivery_time": "delivery",
    "availability": "stock",
    "payment_method": "payment",
    "installment": "payment",
    "warranty": "warranty",
    "size_fit": "size",
    "dimensions": "size",
    "capacity_volume": "size",
    "color_variant": "color",
    "variant_selection": "color",
    "material_composition": "composition",
    "ingredients": "composition",
    "brand_manufacturer": "brand",
    "authenticity": "authenticity",
    "usage_method": "use",
    "functionality": "use",
}


CATEGORY_LABEL = {
    "product_question": "DÚVIDA DE PRODUTO",
    "demo_request": "DEMONSTRAÇÃO",
    "commercial_question": "DÚVIDA COMERCIAL",
    "buying_intent": "INTENÇÃO DE COMPRA",
    "purchase_completed": "COMPRA RELATADA",
    "commercial_observation": "SINAL COMERCIAL",
    "objection": "OBJEÇÃO",
    "purchase_barrier": "BARREIRA DE COMPRA",
    "comment_trend": "TENDÊNCIA",
    "proactive": "AÇÃO PROATIVA",
}


class SalesCoach:
    """
    Uma instancia por sessao do usuario.

    O runtime pode:
    - chamar process_dispatch(envelope) manualmente;
    - iniciar run(next_sales) para consumir a fila do Dispatcher;
    - chamar maybe_proactive() periodicamente;
    - ler recent_outputs() para publicar na Interface.
    """

    def __init__(
        self,
        product_context: ProductContext,
    ):
        if not isinstance(
            product_context,
            ProductContext,
        ):
            raise TypeError(
                "SalesCoach requer ProductContext."
            )

        self.product_context = product_context

        self.output_queue = asyncio.Queue(
            maxsize=SALES_COACH_OUTPUT_QUEUE_LIMIT
        )

        self.history = deque(
            maxlen=SALES_COACH_HISTORY_LIMIT
        )

        self.recent_comments = deque(
            maxlen=100
        )

        self.recent_signatures = deque(
            maxlen=SALES_COACH_RECENT_SIGNATURES
        )

        self.running = False
        self.last_error = None

        self.last_output_at = 0.0
        self.last_reactive_at = 0.0
        self.last_proactive_at = 0.0

        self._proactive_index = 0
        self._seen_dispatch_ids = set()
        self._seen_dispatch_order = deque(
            maxlen=2000
        )

    # ========================================================
    # ESTADO
    # ========================================================

    def reset_live_state(self):
        self.history.clear()
        self.recent_comments.clear()
        self.recent_signatures.clear()
        self._seen_dispatch_ids.clear()
        self._seen_dispatch_order.clear()

        self.last_error = None
        self.last_output_at = 0.0
        self.last_reactive_at = 0.0
        self.last_proactive_at = 0.0
        self._proactive_index = 0

    def snapshot(self):
        product = self.product_context.snapshot()

        return {
            "version": SALES_COACH_VERSION,
            "enabled": bool(
                product.get("enabled")
            ),
            "mode": product.get(
                "mode",
                "leve",
            ),
            "running": bool(self.running),
            "last_error": self.last_error,
            "messages": self.recent_outputs(60),
            "recent_comment_count": len(
                self.recent_comments
            ),
            "last_output_at": (
                self.last_output_at
                or None
            ),
            "last_reactive_at": (
                self.last_reactive_at
                or None
            ),
            "last_proactive_at": (
                self.last_proactive_at
                or None
            ),
        }

    # ========================================================
    # HELPERS DE CONTEXTO
    # ========================================================

    def _mode(self):
        mode = self.product_context.snapshot().get(
            "mode",
            "leve",
        )
        return (
            mode
            if mode in SALES_COACH_MODE_CONFIG
            else "leve"
        )

    def _config(self):
        return SALES_COACH_MODE_CONFIG[
            self._mode()
        ]

    def _facts(self):
        return self.product_context.known_facts()

    def _product_name(self):
        return (
            self._facts().get("name")
            or "o produto"
        )

    def _registered_text(self):
        facts = self._facts()

        return "\n".join(
            value
            for value in [
                facts.get("description"),
                facts.get("additional_info"),
            ]
            if value
        )

    def _fact_snippet(
        self,
        fact_key,
        comment_text="",
    ):
        """
        Busca um trecho cadastrado relacionado ao assunto.

        O trecho e devolvido como texto do proprio Product Context.
        Nenhum dado novo e inferido.
        """
        registered = self._registered_text()

        if not registered:
            return None

        terms = set(
            FACT_TERMS.get(
                fact_key,
                set(),
            )
        )

        normalized_comment = _norm(
            comment_text
        )

        # Palavras relevantes do comentario tambem ajudam
        # a localizar a frase cadastrada.
        for token in re.findall(
            r"[a-zA-ZÀ-ÿ0-9]+",
            normalized_comment,
        ):
            if len(token) >= 4:
                terms.add(token)

        best = None
        best_score = 0

        for sentence in _sentence_candidates(
            registered
        ):
            normalized = _norm(sentence)
            score = sum(
                1
                for term in terms
                if _norm(term) in normalized
            )

            if score > best_score:
                best = sentence
                best_score = score

        if best_score <= 0:
            return None

        return _trim(best, 260)

    def _price_context(self):
        facts = self._facts()

        regular = facts.get(
            "regular_price"
        )
        current = facts.get(
            "current_price"
        )
        discount = facts.get(
            "discount_percent"
        )

        if current is not None:
            pieces = [
                f"preço atual {_money(current)}"
            ]

            if (
                regular is not None
                and regular != current
            ):
                pieces.append(
                    f"preço normal {_money(regular)}"
                )

            if discount is not None:
                pieces.append(
                    f"{_percent(discount)} de desconto"
                )

            return ", ".join(pieces)

        if regular is not None:
            return (
                f"preço cadastrado "
                f"{_money(regular)}"
            )

        return None

    def _registered_or_unknown(
        self,
        fact_key,
        comment_text,
        friendly_label,
    ):
        if fact_key == "price":
            price = self._price_context()
            if price:
                return {
                    "known": True,
                    "text": price,
                }

        if fact_key == "promotion":
            facts = self._facts()

            if facts.get(
                "discount_percent"
            ) is not None:
                return {
                    "known": True,
                    "text": (
                        f"desconto cadastrado "
                        f"{_percent(facts['discount_percent'])}"
                    ),
                }

        snippet = self._fact_snippet(
            fact_key,
            comment_text,
        )

        if snippet:
            return {
                "known": True,
                "text": snippet,
            }

        return {
            "known": False,
            "text": (
                f"A informação sobre {friendly_label} "
                "não está cadastrada no Product Context."
            ),
        }

    # ========================================================
    # LEITURA DO ENVELOPE
    # ========================================================

    def _payload(self, envelope):
        payload = (
            envelope.get("payload")
            if isinstance(envelope, dict)
            else None
        )
        return (
            payload
            if isinstance(payload, dict)
            else {}
        )

    def _source_comment(self, envelope):
        payload = self._payload(envelope)

        if payload.get(
            "message_type"
        ) != "classified_comment":
            return None

        comment = (
            payload.get("comment")
            or {}
        )

        if not isinstance(comment, dict):
            return None

        text = str(
            comment.get("text")
            or ""
        ).strip()

        if not text:
            return None

        return {
            "event_id":
                comment.get("event_id"),
            "user":
                str(
                    comment.get("user")
                    or "Usuário"
                ).strip(),
            "text":
                text,
            "display_time":
                comment.get(
                    "display_time"
                ),
        }

    def _intent_list(self, envelope):
        intents = envelope.get(
            "matched_intents"
        )

        if not isinstance(intents, list):
            intents = []

        return [
            item
            for item in intents
            if isinstance(item, dict)
        ]

    def _primary_intent(self, envelope):
        intents = self._intent_list(
            envelope
        )

        priority = [
            "buying_intent",
            "commercial_question",
            "product_question",
            "demo_request",
            "objection",
            "purchase_barrier",
            "purchase_completed",
            "commercial_observation",
        ]

        for category in priority:
            found = next(
                (
                    intent
                    for intent in intents
                    if intent.get("category")
                    == category
                ),
                None,
            )

            if found:
                return found

        if intents:
            return intents[0]

        payload = self._payload(
            envelope
        )

        if payload.get(
            "message_type"
        ) == "comment_trend":
            return {
                "category":
                    payload.get("category"),
                "subtype":
                    payload.get("subtype"),
                "requires_response":
                    False,
            }

        return {
            "category": None,
            "subtype": None,
            "requires_response": False,
        }

    # ========================================================
    # GERACAO REATIVA
    # ========================================================

    def _reactive_price(
        self,
        comment,
    ):
        price = self._price_context()

        if price:
            if self._mode() == "maximo":
                return (
                    f"Responda o preço e conduza para a compra: "
                    f"destaque {price}. Em seguida, faça um CTA claro "
                    f"para o público avançar no produto."
                )

            return (
                f"Responda objetivamente e aproveite a oportunidade: "
                f"informe {price} e convide a pessoa a conferir o produto."
            )

        return (
            "A pessoa perguntou preço, mas nenhum preço está cadastrado. "
            "Confirme o valor antes de responder; não improvise."
        )

    def _reactive_promotion(
        self,
        comment,
    ):
        known = self._registered_or_unknown(
            "promotion",
            comment["text"],
            "promoção, desconto ou cupom",
        )

        if not known["known"]:
            return (
                "A pergunta é sobre promoção/desconto, mas essa condição "
                "não está cadastrada. Confirme a oferta antes de anunciar."
            )

        if self._mode() == "maximo":
            return (
                f"Use a promoção para acelerar a decisão: "
                f"{known['text']}. Reforce a vantagem e termine com CTA."
            )

        return (
            f"Explique a condição promocional cadastrada: "
            f"{known['text']}. Depois convide a pessoa a conferir a oferta."
        )

    def _reactive_unknown_fact(
        self,
        fact_key,
        comment,
        label,
    ):
        known = self._registered_or_unknown(
            fact_key,
            comment["text"],
            label,
        )

        if known["known"]:
            if self._mode() == "maximo":
                return (
                    f"Responda usando apenas o dado cadastrado: "
                    f"“{known['text']}”. Depois conecte essa informação "
                    f"ao benefício para a pessoa e conduza para o CTA."
                )

            return (
                f"Responda usando o que está cadastrado: "
                f"“{known['text']}”. Se fizer sentido, demonstre o ponto "
                f"na câmera e convide a pessoa a conhecer o produto."
            )

        return (
            f"A pergunta é sobre {label}, mas essa informação não está "
            "cadastrada. Avise o vendedor para confirmar antes de responder; "
            "não invente esse dado."
        )

    def _reactive_product_question(
        self,
        subtype,
        comment,
    ):
        fact_key = SUBTYPE_FACT_KEY.get(
            subtype,
            "use",
        )

        labels = {
            "size":
                "tamanho, medida ou capacidade",
            "color":
                "cor ou variação",
            "composition":
                "material, composição ou ingrediente",
            "use":
                "uso, indicação ou funcionamento",
            "brand":
                "marca ou fabricante",
            "authenticity":
                "autenticidade",
            "warranty":
                "garantia",
        }

        label = labels.get(
            fact_key,
            "essa característica do produto",
        )

        return self._reactive_unknown_fact(
            fact_key,
            comment,
            label,
        )

    def _reactive_commercial_question(
        self,
        subtype,
        comment,
    ):
        if subtype == "price":
            return self._reactive_price(
                comment
            )

        if subtype in {
            "promotion_coupon",
            "promotion_problem",
        }:
            return self._reactive_promotion(
                comment
            )

        mapping = {
            "shipping":
                ("shipping", "frete ou envio"),
            "delivery_time":
                ("delivery", "prazo de entrega"),
            "availability":
                ("stock", "estoque ou disponibilidade"),
            "payment_method":
                ("payment", "forma de pagamento"),
            "installment":
                ("payment", "parcelamento"),
            "variant_selection":
                ("color", "variação solicitada"),
        }

        fact_key, label = mapping.get(
            subtype,
            ("use", "essa condição comercial"),
        )

        return self._reactive_unknown_fact(
            fact_key,
            comment,
            label,
        )

    def _reactive_demo(
        self,
        comment,
    ):
        facts = self._facts()
        description = facts.get(
            "description"
        )
        additional = facts.get(
            "additional_info"
        )

        detail = (
            _trim(description, 180)
            if description
            else _trim(additional, 180)
            if additional
            else None
        )

        if detail:
            if self._mode() == "maximo":
                return (
                    f"Atenda ao pedido de demonstração agora. "
                    f"Mostre o produto de perto e use este dado cadastrado: "
                    f"“{detail}”. Enquanto demonstra, explique o benefício "
                    f"e conduza para a compra."
                )

            return (
                f"Mostre o produto na câmera e explique este ponto cadastrado: "
                f"“{detail}”. Depois pergunte se a pessoa quer ver algum "
                f"detalhe específico."
            )

        return (
            "Há pedido de demonstração. Mostre fisicamente o produto e descreva "
            "apenas o que você consegue comprovar na câmera; não acrescente "
            "características não cadastradas."
        )

    def _reactive_buying_intent(
        self,
        comment,
    ):
        price = self._price_context()

        if self._mode() == "maximo":
            base = (
                "Há intenção clara de compra. Não deixe o interesse esfriar: "
                "responda diretamente, elimine a última dúvida e faça um CTA "
                "de fechamento agora."
            )
        else:
            base = (
                "Há intenção de compra. Responda diretamente e facilite o "
                "próximo passo com um CTA simples."
            )

        if price:
            base += (
                f" Você pode reforçar {price}."
            )

        return base

    def _reactive_objection(
        self,
        subtype,
        comment,
    ):
        if subtype == "price_objection":
            price = self._price_context()
            description = self._facts().get(
                "description"
            )

            if description:
                value = _trim(
                    description,
                    170,
                )

                if self._mode() == "maximo":
                    return (
                        f"Trate a objeção de preço pela percepção de valor. "
                        f"Reforce o benefício cadastrado “{value}”"
                        + (
                            f" e depois relembre {price}."
                            if price
                            else "."
                        )
                        + " Feche com CTA sem inventar desconto."
                    )

                return (
                    f"Reconheça a dúvida sobre preço e explique o valor do "
                    f"produto usando o benefício cadastrado “{value}”."
                    + (
                        f" Informe também {price}."
                        if price
                        else ""
                    )
                )

            return (
                "Existe objeção de preço, mas faltam benefícios cadastrados "
                "para sustentar valor. Não invente desconto; explique apenas "
                "o que estiver confirmado e pergunte qual é a dúvida principal."
            )

        mapping = {
            "shipping_objection":
                ("shipping", "frete"),
            "trust_objection":
                ("authenticity", "confiança ou segurança"),
            "authenticity_objection":
                ("authenticity", "autenticidade"),
            "quality_objection":
                ("composition", "qualidade ou material"),
            "size_objection":
                ("size", "tamanho ou medida"),
            "compatibility_objection":
                ("use", "compatibilidade"),
            "delivery_time_objection":
                ("delivery", "prazo de entrega"),
            "promotion_objection":
                ("promotion", "promoção"),
        }

        fact_key, label = mapping.get(
            subtype,
            ("use", "essa objeção"),
        )

        known = self._registered_or_unknown(
            fact_key,
            comment["text"],
            label,
        )

        if known["known"]:
            return (
                f"Trate a objeção com informação verificável: "
                f"“{known['text']}”. Depois confirme se esse ponto resolve "
                f"a dúvida e conduza para o próximo passo."
            )

        return (
            f"A objeção envolve {label}, mas não há informação cadastrada "
            "suficiente para responder com segurança. Confirme o dado antes "
            "de argumentar; não prometa o que não está registrado."
        )

    def _reactive_purchase_completed(
        self,
        comment,
    ):
        return (
            "A pessoa relata que comprou. Reconheça a compra de forma breve "
            "e use o sinal como prova de movimento da LIVE sem afirmar "
            "pagamento aprovado ou status do pedido."
        )

    def _reactive_observation(
        self,
        comment,
    ):
        if self._mode() == "maximo":
            return (
                "Há um sinal comercial relevante no comentário. Puxe esse "
                "assunto para a apresentação, conecte ao produto cadastrado "
                "e mantenha a conversa orientada para compra."
            )

        return (
            "Há um sinal comercial útil. Aproveite o assunto para explicar "
            "o produto e manter o público engajado."
        )

    def _trend_guidance(
        self,
        envelope,
    ):
        payload = self._payload(
            envelope
        )

        category = payload.get(
            "category"
        )
        subtype = payload.get(
            "subtype"
        )
        count = payload.get(
            "count"
        )
        unique_users = payload.get(
            "unique_users"
        )
        examples = payload.get(
            "examples"
        ) or []

        subject = (
            subtype
            or category
            or "assunto comercial"
        ).replace("_", " ")

        volume = []
        if count:
            volume.append(
                f"{count} ocorrências"
            )
        if unique_users:
            volume.append(
                f"{unique_users} pessoas"
            )

        volume_text = (
            " (" + ", ".join(volume) + ")"
            if volume
            else ""
        )

        if self._mode() == "maximo":
            guidance = (
                f"Esse assunto está se repetindo{volume_text}: {subject}. "
                "Pare por alguns segundos e trate o tema para o grupo inteiro, "
                "usando somente fatos cadastrados. Depois retome a condução "
                "para o CTA."
            )
        else:
            guidance = (
                f"Há repetição de {subject}{volume_text}. Vale responder "
                "esse ponto para todos de uma vez e depois seguir a apresentação."
            )

        if examples:
            guidance += (
                f" Exemplo ouvido: “{_trim(examples[-1], 120)}”."
            )

        return guidance

    def _guidance_for(
        self,
        envelope,
    ):
        payload = self._payload(
            envelope
        )

        if payload.get(
            "message_type"
        ) == "comment_trend":
            return (
                self._trend_guidance(
                    envelope
                ),
                "comment_trend",
            )

        comment = self._source_comment(
            envelope
        )

        if not comment:
            return (
                None,
                None,
            )

        intent = self._primary_intent(
            envelope
        )

        category = intent.get(
            "category"
        )
        subtype = intent.get(
            "subtype"
        )

        if category == "product_question":
            guidance = (
                self._reactive_product_question(
                    subtype,
                    comment,
                )
            )

        elif category == "commercial_question":
            guidance = (
                self._reactive_commercial_question(
                    subtype,
                    comment,
                )
            )

        elif category == "demo_request":
            guidance = (
                self._reactive_demo(
                    comment
                )
            )

        elif category == "buying_intent":
            guidance = (
                self._reactive_buying_intent(
                    comment
                )
            )

        elif category in {
            "objection",
            "purchase_barrier",
        }:
            guidance = (
                self._reactive_objection(
                    subtype,
                    comment,
                )
            )

        elif category == "purchase_completed":
            guidance = (
                self._reactive_purchase_completed(
                    comment
                )
            )

        else:
            guidance = (
                self._reactive_observation(
                    comment
                )
            )

        return (
            guidance,
            category
            or "commercial_observation",
        )

    # ========================================================
    # PROATIVO
    # ========================================================

    def _proactive_candidates(self):
        facts = self._facts()
        mode = self._mode()
        product = (
            facts.get("name")
            or "o produto"
        )

        candidates = []

        description = facts.get(
            "description"
        )

        additional = facts.get(
            "additional_info"
        )

        price = self._price_context()

        if description:
            detail = _trim(
                description,
                180,
            )

            candidates.append({
                "key": "benefit",
                "category":
                    "BENEFÍCIO / VALOR",
                "text": (
                    (
                        f"Traga o benefício de volta para o centro da LIVE: "
                        f"“{detail}”. Explique por que isso importa para o "
                        f"cliente e conecte diretamente ao próximo passo de compra."
                    )
                    if mode == "maximo"
                    else
                    (
                        f"Reforce um benefício cadastrado de {product}: "
                        f"“{detail}”. Explique de forma simples como isso "
                        f"pode ser útil para quem está assistindo."
                    )
                ),
            })

        if additional:
            detail = _trim(
                additional,
                170,
            )

            candidates.append({
                "key": "detail",
                "category":
                    "DETALHE DO PRODUTO",
                "text": (
                    (
                        f"Use um detalhe concreto para renovar o interesse: "
                        f"“{detail}”. Mostre esse ponto na câmera se for possível "
                        f"e termine puxando o público para a decisão."
                    )
                    if mode == "maximo"
                    else
                    (
                        f"Apresente mais um detalhe cadastrado: “{detail}”. "
                        f"Se for visível, mostre na câmera enquanto explica."
                    )
                ),
            })

        if price:
            candidates.append({
                "key": "price",
                "category":
                    "PREÇO / OFERTA",
                "text": (
                    (
                        f"Faça uma rodada de preço agora: destaque {price}. "
                        f"Depois diga claramente qual é o próximo passo para comprar. "
                        f"Não crie urgência ou estoque que não estejam cadastrados."
                    )
                    if mode == "maximo"
                    else
                    (
                        f"Relembre o preço para quem entrou depois: {price}. "
                        f"Em seguida, convide o público a conferir o produto."
                    )
                ),
            })

        candidates.append({
            "key": "demo",
            "category":
                "DEMONSTRAÇÃO",
            "text": (
                (
                    f"Volte a mostrar {product} de perto. Demonstre um ponto "
                    f"que esteja cadastrado e, enquanto mostra, conduza a fala "
                    f"do benefício para o CTA."
                )
                if mode == "maximo"
                else
                (
                    f"Mostre {product} novamente na câmera e convide o público "
                    f"a mandar dúvidas sobre o que quer ver com mais detalhe."
                )
            ),
        })

        candidates.append({
            "key": "engagement",
            "category":
                "ENGAJAMENTO COMERCIAL",
            "text": (
                (
                    f"Faça uma pergunta de compra para movimentar a conversa: "
                    f"pergunte qual dúvida ainda impede o público de avançar "
                    f"em {product}. Use as respostas para tratar objeções."
                )
                if mode == "maximo"
                else
                (
                    f"Convide o público a dizer qual dúvida ainda tem sobre "
                    f"{product}. Isso cria novas oportunidades para explicar "
                    f"o produto sem repetir a mesma fala."
                )
            ),
        })

        return candidates

    def maybe_proactive(
        self,
        *,
        now=None,
        force=False,
    ):
        if now is None:
            now = time.time()

        snapshot = self.product_context.snapshot()

        if not snapshot.get(
            "enabled"
        ):
            return None

        config = self._config()

        if (
            not force
            and self.last_proactive_at
            and now - self.last_proactive_at
            < config["proactive_interval"]
        ):
            return None

        # Comentario reativo recente tem preferencia.
        if (
            not force
            and self.last_reactive_at
            and now - self.last_reactive_at
            < config["reactive_cooldown"]
        ):
            return None

        candidates = (
            self._proactive_candidates()
        )

        if not candidates:
            return None

        for offset in range(
            len(candidates)
        ):
            index = (
                self._proactive_index
                + offset
            ) % len(candidates)

            candidate = candidates[
                index
            ]

            signature = (
                "proactive:"
                + candidate["key"]
            )

            if self._signature_blocked(
                signature,
                now,
            ):
                continue

            self._proactive_index = (
                index + 1
            ) % len(candidates)

            output = self._emit(
                text=candidate["text"],
                category=(
                    candidate["category"]
                ),
                source="proactive",
                signature=signature,
                now=now,
            )

            self.last_proactive_at = now

            return output

        return None

    # ========================================================
    # CONTROLE DE REPETICAO / SAIDA
    # ========================================================

    def _signature_blocked(
        self,
        signature,
        now,
    ):
        guard = self._config()[
            "repeat_guard_seconds"
        ]

        for item in reversed(
            self.recent_signatures
        ):
            if (
                item["signature"]
                == signature
            ):
                return (
                    now - item["time"]
                    < guard
                )

        return False

    def _remember_signature(
        self,
        signature,
        now,
    ):
        self.recent_signatures.append({
            "signature": signature,
            "time": now,
        })

    def _emit(
        self,
        *,
        text,
        category,
        source,
        signature,
        now=None,
        source_comment=None,
        source_dispatch_id=None,
        matched_categories=None,
        matched_subtypes=None,
    ):
        if now is None:
            now = time.time()

        mode = self._mode()
        config = self._config()

        output = {
            "id": uuid.uuid4().hex,
            "type": "sales_coach_message",
            "version": SALES_COACH_VERSION,
            "source": source,
            "mode": mode,
            "timestamp": now,
            "display_seconds":
                config["display_seconds"],
            "expires_at":
                now
                + config["display_seconds"],
            "texto": str(text).strip(),
            "categoria": category,
            "product_name":
                self._product_name(),
            "source_comment":
                copy.deepcopy(
                    source_comment
                ),
            "source_dispatch_id":
                source_dispatch_id,
            "matched_categories":
                list(
                    matched_categories
                    or []
                ),
            "matched_subtypes":
                list(
                    matched_subtypes
                    or []
                ),
        }

        self.history.append(
            copy.deepcopy(output)
        )

        self._remember_signature(
            signature,
            now,
        )

        self.last_output_at = now

        try:
            self.output_queue.put_nowait(
                copy.deepcopy(output)
            )
        except asyncio.QueueFull:
            try:
                self.output_queue.get_nowait()
            except Exception:
                pass

            try:
                self.output_queue.put_nowait(
                    copy.deepcopy(output)
                )
            except Exception:
                pass

        return output

    # ========================================================
    # PROCESSAMENTO DO DISPATCHER
    # ========================================================

    def _seen_dispatch(
        self,
        dispatch_id,
    ):
        if not dispatch_id:
            return False

        dispatch_id = str(
            dispatch_id
        )

        if dispatch_id in self._seen_dispatch_ids:
            return True

        if (
            len(self._seen_dispatch_order)
            >= self._seen_dispatch_order.maxlen
        ):
            old = self._seen_dispatch_order[0]
            self._seen_dispatch_ids.discard(
                old
            )

        self._seen_dispatch_order.append(
            dispatch_id
        )
        self._seen_dispatch_ids.add(
            dispatch_id
        )

        return False

    def process_dispatch(
        self,
        envelope,
        *,
        now=None,
    ):
        if now is None:
            now = time.time()

        if not isinstance(
            envelope,
            dict,
        ):
            return None

        if envelope.get(
            "destination"
        ) != "sales":
            return None

        if self._seen_dispatch(
            envelope.get(
                "dispatch_id"
            )
        ):
            return None

        product_state = (
            self.product_context.snapshot()
        )

        # Monitoramento da LIVE continua normalmente,
        # mas Sales Coach nao produz orientacao enquanto OFF.
        if not product_state.get(
            "enabled"
        ):
            return None

        guidance, category = (
            self._guidance_for(
                envelope
            )
        )

        if not guidance:
            return None

        comment = self._source_comment(
            envelope
        )

        if comment:
            self.recent_comments.append(
                copy.deepcopy(comment)
            )

        subtype = None
        intent = self._primary_intent(
            envelope
        )

        if isinstance(intent, dict):
            subtype = intent.get(
                "subtype"
            )

        signature = (
            "reactive:"
            + str(category or "")
            + ":"
            + str(subtype or "")
            + ":"
            + _norm(
                comment["text"]
                if comment
                else self._payload(
                    envelope
                ).get(
                    "topic_key",
                    "",
                )
            )[:80]
        )

        # Nao suprime perguntas individuais diferentes.
        # O guard vale somente para a mesma assinatura.
        if self._signature_blocked(
            signature,
            now,
        ):
            return None

        output = self._emit(
            text=guidance,
            category=CATEGORY_LABEL.get(
                category,
                CATEGORY_LABEL.get(
                    "comment_trend"
                )
                if category
                == "comment_trend"
                else "OPORTUNIDADE DE VENDA",
            ),
            source=(
                "trend"
                if category
                == "comment_trend"
                else "comment"
            ),
            signature=signature,
            now=now,
            source_comment=comment,
            source_dispatch_id=envelope.get(
                "dispatch_id"
            ),
            matched_categories=envelope.get(
                "matched_categories"
            ),
            matched_subtypes=envelope.get(
                "matched_subtypes"
            ),
        )

        self.last_reactive_at = now

        return output

    # ========================================================
    # LOOP ASSINCRONO
    # ========================================================

    async def run(
        self,
        next_sales: Callable[
            ...,
            Awaitable[dict]
        ],
        *,
        live_running: Optional[
            Callable[[], bool]
        ] = None,
    ):
        """
        Consome a fila sales sem interferir nas demais filas.

        next_sales deve ser normalmente:
        ns["comment_dispatcher_next_sales"]
        """
        self.running = True
        self.last_error = None

        try:
            while True:
                envelope = None

                try:
                    envelope = await next_sales(
                        timeout=0.5
                    )
                except asyncio.TimeoutError:
                    envelope = None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.last_error = (
                        f"{type(exc).__name__}: {exc}"
                    )
                    await asyncio.sleep(
                        0.1
                    )

                if envelope is not None:
                    try:
                        self.process_dispatch(
                            envelope
                        )
                    except Exception as exc:
                        self.last_error = (
                            f"{type(exc).__name__}: {exc}"
                        )

                try:
                    self.maybe_proactive()
                except Exception as exc:
                    self.last_error = (
                        f"{type(exc).__name__}: {exc}"
                    )

                if (
                    live_running is not None
                    and not live_running()
                ):
                    # Se o Dispatcher ainda tiver eventos,
                    # next_sales os entrega antes de chegar aqui.
                    break

        finally:
            self.running = False

    # ========================================================
    # APIs DE LEITURA
    # ========================================================

    async def next_output(
        self,
        timeout=None,
    ):
        if timeout is None:
            return await self.output_queue.get()

        return await asyncio.wait_for(
            self.output_queue.get(),
            timeout=timeout,
        )

    def recent_outputs(
        self,
        limit=30,
    ):
        try:
            limit = max(
                0,
                min(
                    int(limit),
                    SALES_COACH_HISTORY_LIMIT,
                ),
            )
        except Exception:
            limit = 30

        if limit <= 0:
            return []

        return copy.deepcopy(
            list(self.history)[-limit:]
        )


# ============================================================
# SELF TEST
# ============================================================

def _test_envelope(
    *,
    text,
    category,
    subtype,
    user="Cliente",
):
    return {
        "dispatch_id": uuid.uuid4().hex,
        "message_type":
            "dispatched_comment_output",
        "dispatcher_version":
            "1.1",
        "destination":
            "sales",
        "matched_categories": [
            category
        ],
        "matched_subtypes": [
            subtype
        ] if subtype else [],
        "matched_intents": [
            {
                "category": category,
                "subtype": subtype,
                "requires_response": True,
            }
        ],
        "payload": {
            "output_id":
                uuid.uuid4().hex,
            "message_type":
                "classified_comment",
            "comment": {
                "event_id":
                    uuid.uuid4().hex,
                "user":
                    user,
                "text":
                    text,
                "display_time":
                    "20:00:00",
            },
            "classification": {
                "relevant": True,
                "is_question": True,
                "is_request": False,
                "requires_response": True,
                "main_classification": {
                    "category": category,
                    "subtype": subtype,
                    "requires_response": True,
                },
                "intents": [
                    {
                        "category": category,
                        "subtype": subtype,
                        "requires_response": True,
                    }
                ],
            },
        },
    }


def sales_coach_self_test():
    ctx = ProductContext()

    ctx.update(
        name="NIVEA Locao Hidratante Milk",
        regular_price="22,90",
        current_price="14,99",
        discount="35",
        description=(
            "Indicada para pele seca e extrasseca. "
            "Com oleo de amendoas e hidratacao profunda."
        ),
        additional_info="Conteudo: 200 ml.",
        mode="leve",
        replace=True,
    )

    ctx.activate()

    coach = SalesCoach(ctx)

    price = coach.process_dispatch(
        _test_envelope(
            text="Quanto custa?",
            category="commercial_question",
            subtype="price",
        ),
        now=1000,
    )

    if (
        not price
        or "R$ 14,99"
        not in price["texto"]
    ):
        return {
            "ok": False,
            "step": "price",
            "output": price,
        }

    dry_skin = coach.process_dispatch(
        _test_envelope(
            text="Serve pra pele seca?",
            category="product_question",
            subtype="functionality",
        ),
        now=1010,
    )

    if (
        not dry_skin
        or "pele seca" not in _norm(
            dry_skin["texto"]
        )
    ):
        return {
            "ok": False,
            "step": "registered_benefit",
            "output": dry_skin,
        }

    volume = coach.process_dispatch(
        _test_envelope(
            text="Quantos ml?",
            category="product_question",
            subtype="capacity_volume",
        ),
        now=1020,
    )

    if (
        not volume
        or "200 ml" not in _norm(
            volume["texto"]
        )
    ):
        return {
            "ok": False,
            "step": "volume",
            "output": volume,
        }

    almond = coach.process_dispatch(
        _test_envelope(
            text="Tem oleo de amendoas?",
            category="product_question",
            subtype="ingredients",
        ),
        now=1030,
    )

    if (
        not almond
        or "amendoas" not in _norm(
            almond["texto"]
        )
    ):
        return {
            "ok": False,
            "step": "ingredient",
            "output": almond,
        }

    shipping = coach.process_dispatch(
        _test_envelope(
            text="Tem frete gratis?",
            category="commercial_question",
            subtype="shipping",
        ),
        now=1040,
    )

    if (
        not shipping
        or "nao esta cadastrada"
        not in _norm(
            shipping["texto"]
        )
    ):
        return {
            "ok": False,
            "step": "unknown_shipping",
            "output": shipping,
        }

    if (
        "tem frete gratis"
        in _norm(
            shipping["texto"]
        )
        and "nao esta cadastrada"
        not in _norm(
            shipping["texto"]
        )
    ):
        return {
            "ok": False,
            "step": "invented_shipping",
            "output": shipping,
        }

    proactive_light = (
        coach.maybe_proactive(
            now=1100,
            force=True,
        )
    )

    if not proactive_light:
        return {
            "ok": False,
            "step": "proactive_light",
        }

    ctx.set_mode(
        "maximo"
    )

    maximum = coach.process_dispatch(
        _test_envelope(
            text="Quanto custa agora?",
            category="commercial_question",
            subtype="price",
        ),
        now=1200,
    )

    if (
        not maximum
        or maximum["mode"]
        != "maximo"
        or "R$ 14,99"
        not in maximum["texto"]
    ):
        return {
            "ok": False,
            "step": "maximum_capability",
            "output": maximum,
        }

    proactive_max = (
        coach.maybe_proactive(
            now=1300,
            force=True,
        )
    )

    if (
        not proactive_max
        or proactive_max["mode"]
        != "maximo"
    ):
        return {
            "ok": False,
            "step": "proactive_maximum",
            "output": proactive_max,
        }

    ctx.deactivate()

    disabled = coach.process_dispatch(
        _test_envelope(
            text="Quanto custa?",
            category="commercial_question",
            subtype="price",
        ),
        now=1400,
    )

    if disabled is not None:
        return {
            "ok": False,
            "step": "disabled",
            "output": disabled,
        }

    return {
        "ok": True,
        "version":
            SALES_COACH_VERSION,
        "messages":
            len(coach.history),
    }


if __name__ == "__main__":
    print(
        sales_coach_self_test()
    )
