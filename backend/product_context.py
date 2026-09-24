"""
AGCN LIVE / ALIVE - PRODUCT CONTEXT V1

Fonte oficial de contexto do produto para o Sales Coach V2.

Responsabilidades:
- manter um único produto ativo por sessão;
- ativar/desativar o Sales Coach;
- armazenar modo Leve ou Máximo;
- armazenar somente informações fornecidas pelo vendedor;
- normalizar preços sem inventar valores;
- calcular desconto automaticamente quando possível;
- aceitar desconto manual;
- expor snapshot seguro para API/interface/Sales Coach;
- preservar estado durante refresh enquanto a sessão do backend existir.

Não faz:
- scraping de produto;
- leitura de Shopee/TikTok Shop;
- consulta a API externa;
- descoberta automática de produto;
- geração de orientação de venda;
- inferência de fatos não cadastrados.
"""

from __future__ import annotations

import copy
import re
import threading
import time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


PRODUCT_CONTEXT_VERSION = "1.0"

PRODUCT_CONTEXT_MODES = {
    "leve",
    "maximo",
}

PRODUCT_CONTEXT_MAX_NAME = 180
PRODUCT_CONTEXT_MAX_DESCRIPTION = 6000
PRODUCT_CONTEXT_MAX_ADDITIONAL_INFO = 12000

_MONEY_QUANT = Decimal("0.01")
_PERCENT_QUANT = Decimal("0.01")


class ProductContextError(ValueError):
    """Erro de validacao do Product Context."""


def _clean_text(value, limit):
    text = str(value or "").strip()
    text = re.sub(r"\r\n?", "\n", text)
    if len(text) > limit:
        raise ProductContextError(
            f"Texto excede o limite de {limit} caracteres."
        )
    return text


def _normalize_mode(value):
    mode = str(value or "leve").strip().lower()

    aliases = {
        "light": "leve",
        "leve": "leve",
        "max": "maximo",
        "maximum": "maximo",
        "máximo": "maximo",
        "maximo": "maximo",
    }

    mode = aliases.get(mode, mode)

    if mode not in PRODUCT_CONTEXT_MODES:
        raise ProductContextError(
            "Modo invalido. Use 'leve' ou 'maximo'."
        )

    return mode


def _parse_decimal(value, field_name):
    if value is None:
        return None

    if isinstance(value, bool):
        raise ProductContextError(
            f"{field_name} invalido."
        )

    if isinstance(value, Decimal):
        number = value

    elif isinstance(value, (int, float)):
        number = Decimal(str(value))

    else:
        raw = str(value).strip()

        if not raw:
            return None

        raw = raw.replace("R$", "").replace("%", "").strip()
        raw = re.sub(r"\s+", "", raw)

        if "," in raw and "." in raw:
            # 1.234,56 -> 1234.56
            if raw.rfind(",") > raw.rfind("."):
                raw = raw.replace(".", "").replace(",", ".")
            # 1,234.56 -> 1234.56
            else:
                raw = raw.replace(",", "")
        elif "," in raw:
            raw = raw.replace(".", "").replace(",", ".")

        raw = re.sub(r"[^0-9.\-]", "", raw)

        try:
            number = Decimal(raw)
        except (InvalidOperation, ValueError):
            raise ProductContextError(
                f"{field_name} invalido."
            ) from None

    if not number.is_finite():
        raise ProductContextError(
            f"{field_name} invalido."
        )

    return number


def _normalize_money(value, field_name):
    number = _parse_decimal(
        value,
        field_name,
    )

    if number is None:
        return None

    if number < 0:
        raise ProductContextError(
            f"{field_name} nao pode ser negativo."
        )

    return number.quantize(
        _MONEY_QUANT,
        rounding=ROUND_HALF_UP,
    )


def _normalize_discount(value):
    number = _parse_decimal(
        value,
        "Desconto",
    )

    if number is None:
        return None

    if number < 0 or number > 100:
        raise ProductContextError(
            "Desconto deve estar entre 0 e 100."
        )

    return number.quantize(
        _PERCENT_QUANT,
        rounding=ROUND_HALF_UP,
    )


def _decimal_to_float(value):
    if value is None:
        return None
    return float(value)


def _money_label(value):
    if value is None:
        return None

    formatted = f"{value:.2f}".replace(".", ",")
    return f"R$ {formatted}"


def _percent_label(value):
    if value is None:
        return None

    if value == value.to_integral():
        return f"{int(value)}%"

    formatted = (
        f"{value:.2f}"
        .rstrip("0")
        .rstrip(".")
        .replace(".", ",")
    )

    return f"{formatted}%"


def _automatic_discount(
    regular_price,
    current_price,
):
    if (
        regular_price is None
        or current_price is None
        or regular_price <= 0
        or current_price > regular_price
    ):
        return None

    percent = (
        (regular_price - current_price)
        / regular_price
        * Decimal("100")
    )

    return percent.quantize(
        _PERCENT_QUANT,
        rounding=ROUND_HALF_UP,
    )


class ProductContext:
    """
    Estado de um unico produto ativo por sessao.

    A classe e deliberadamente independente de plataforma:
    o mesmo contexto serve para LIVE Shopee ou TikTok.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self.reset()

    def reset(self):
        with getattr(
            self,
            "_lock",
            threading.RLock(),
        ):
            now = time.time()

            self.enabled = False
            self.mode = "leve"

            self.name = ""
            self.regular_price = None
            self.current_price = None

            self.manual_discount = None
            self.description = ""
            self.additional_info = ""

            self.created_at = now
            self.updated_at = now
            self.activated_at = None
            self.deactivated_at = None

            self.revision = 0

        return self.snapshot()

    def _touch(self):
        self.updated_at = time.time()
        self.revision += 1

    def _effective_discount(self):
        if self.manual_discount is not None:
            return (
                self.manual_discount,
                "manual",
            )

        automatic = _automatic_discount(
            self.regular_price,
            self.current_price,
        )

        if automatic is not None:
            return (
                automatic,
                "automatic",
            )

        return (
            None,
            None,
        )

    def update(
        self,
        *,
        name=None,
        regular_price=None,
        current_price=None,
        discount=None,
        description=None,
        additional_info=None,
        mode=None,
        replace=False,
    ):
        """
        Atualiza dados cadastrados pelo vendedor.

        replace=False:
            somente campos explicitamente enviados mudam.

        replace=True:
            campos ausentes sao limpos, exceto mode.
        """
        with self._lock:
            if mode is not None:
                self.mode = _normalize_mode(
                    mode
                )

            if replace or name is not None:
                self.name = _clean_text(
                    name,
                    PRODUCT_CONTEXT_MAX_NAME,
                )

            if replace or regular_price is not None:
                self.regular_price = (
                    _normalize_money(
                        regular_price,
                        "Preco normal",
                    )
                )

            if replace or current_price is not None:
                self.current_price = (
                    _normalize_money(
                        current_price,
                        "Preco atual",
                    )
                )

            if replace or discount is not None:
                self.manual_discount = (
                    _normalize_discount(
                        discount
                    )
                )

            if replace or description is not None:
                self.description = (
                    _clean_text(
                        description,
                        PRODUCT_CONTEXT_MAX_DESCRIPTION,
                    )
                )

            if replace or additional_info is not None:
                self.additional_info = (
                    _clean_text(
                        additional_info,
                        PRODUCT_CONTEXT_MAX_ADDITIONAL_INFO,
                    )
                )

            self._touch()

            return self.snapshot()

    def configure(
        self,
        data,
        *,
        replace=True,
    ):
        if not isinstance(data, dict):
            raise ProductContextError(
                "O Product Context deve ser um objeto."
            )

        return self.update(
            name=data.get("name"),
            regular_price=data.get(
                "regular_price"
            ),
            current_price=data.get(
                "current_price"
            ),
            discount=data.get("discount"),
            description=data.get(
                "description"
            ),
            additional_info=data.get(
                "additional_info"
            ),
            mode=data.get("mode"),
            replace=replace,
        )

    def set_mode(self, mode):
        with self._lock:
            self.mode = _normalize_mode(
                mode
            )
            self._touch()
            return self.snapshot()

    def can_activate(self):
        with self._lock:
            return bool(
                self.name.strip()
            )

    def activation_errors(self):
        errors = []

        with self._lock:
            if not self.name.strip():
                errors.append(
                    "Informe o nome do produto."
                )

        return errors

    def activate(self, mode=None):
        with self._lock:
            if mode is not None:
                self.mode = _normalize_mode(
                    mode
                )

            errors = (
                self.activation_errors()
            )

            if errors:
                raise ProductContextError(
                    " ".join(errors)
                )

            self.enabled = True
            self.activated_at = time.time()
            self.deactivated_at = None
            self._touch()

            return self.snapshot()

    def deactivate(self):
        with self._lock:
            self.enabled = False
            self.deactivated_at = time.time()
            self._touch()

            return self.snapshot()

    def clear_product(
        self,
        *,
        keep_mode=True,
    ):
        with self._lock:
            mode = (
                self.mode
                if keep_mode
                else "leve"
            )

            now = time.time()

            self.enabled = False
            self.mode = mode

            self.name = ""
            self.regular_price = None
            self.current_price = None
            self.manual_discount = None
            self.description = ""
            self.additional_info = ""

            self.updated_at = now
            self.deactivated_at = now
            self.activated_at = None
            self.revision += 1

            return self.snapshot()

    def known_facts(self):
        """
        Retorna apenas fatos realmente cadastrados ou calculados
        deterministicamente a partir deles.

        O Sales Coach V2 deve usar este metodo como fonte de verdade
        e nunca preencher lacunas com suposicoes.
        """
        with self._lock:
            discount, discount_source = (
                self._effective_discount()
            )

            facts = {}

            if self.name:
                facts["name"] = self.name

            if self.regular_price is not None:
                facts["regular_price"] = (
                    _decimal_to_float(
                        self.regular_price
                    )
                )

            if self.current_price is not None:
                facts["current_price"] = (
                    _decimal_to_float(
                        self.current_price
                    )
                )

            if discount is not None:
                facts["discount_percent"] = (
                    _decimal_to_float(
                        discount
                    )
                )
                facts["discount_source"] = (
                    discount_source
                )

            if self.description:
                facts["description"] = (
                    self.description
                )

            if self.additional_info:
                facts["additional_info"] = (
                    self.additional_info
                )

            return copy.deepcopy(facts)

    def snapshot(self):
        with self._lock:
            discount, discount_source = (
                self._effective_discount()
            )

            return {
                "version":
                    PRODUCT_CONTEXT_VERSION,

                "enabled":
                    bool(self.enabled),

                "mode":
                    self.mode,

                "product": {
                    "name":
                        self.name or None,

                    "regular_price":
                        _decimal_to_float(
                            self.regular_price
                        ),

                    "regular_price_label":
                        _money_label(
                            self.regular_price
                        ),

                    "current_price":
                        _decimal_to_float(
                            self.current_price
                        ),

                    "current_price_label":
                        _money_label(
                            self.current_price
                        ),

                    "discount_percent":
                        _decimal_to_float(
                            discount
                        ),

                    "discount_label":
                        _percent_label(
                            discount
                        ),

                    "discount_source":
                        discount_source,

                    "description":
                        self.description or None,

                    "additional_info":
                        (
                            self.additional_info
                            or None
                        ),
                },

                "ready":
                    self.can_activate(),

                "activation_errors":
                    self.activation_errors(),

                "revision":
                    self.revision,

                "created_at":
                    self.created_at,

                "updated_at":
                    self.updated_at,

                "activated_at":
                    self.activated_at,

                "deactivated_at":
                    self.deactivated_at,
            }

    def export_state(self):
        """
        Estado serializavel para API/frontend/persistencia futura.
        """
        return self.snapshot()

    def import_state(self, state):
        """
        Reidrata contexto previamente salvo sem confiar em campos
        derivados enviados pelo cliente.
        """
        if not isinstance(state, dict):
            raise ProductContextError(
                "Estado do Product Context invalido."
            )

        product = state.get("product") or {}

        if not isinstance(product, dict):
            raise ProductContextError(
                "Produto do Product Context invalido."
            )

        snapshot = self.update(
            name=product.get("name"),
            regular_price=product.get(
                "regular_price"
            ),
            current_price=product.get(
                "current_price"
            ),
            discount=(
                product.get(
                    "discount_percent"
                )
                if product.get(
                    "discount_source"
                )
                == "manual"
                else None
            ),
            description=product.get(
                "description"
            ),
            additional_info=product.get(
                "additional_info"
            ),
            mode=state.get(
                "mode",
                "leve",
            ),
            replace=True,
        )

        if bool(state.get("enabled")):
            return self.activate()

        self.deactivate()
        return snapshot


def product_context_self_test():
    """
    Teste interno sem rede e sem LIVE.
    """
    ctx = ProductContext()

    initial = ctx.snapshot()

    if initial["enabled"]:
        return {
            "ok": False,
            "step": "initial_disabled",
        }

    if initial["mode"] != "leve":
        return {
            "ok": False,
            "step": "initial_mode",
        }

    try:
        ctx.activate()
        return {
            "ok": False,
            "step": "activation_without_name",
        }
    except ProductContextError:
        pass

    ctx.update(
        name="NIVEA Locao Hidratante Milk",
        regular_price="22,90",
        current_price="14,99",
        description=(
            "Para pele seca e extrasseca, "
            "com oleo de amendoas e hidratacao profunda."
        ),
        additional_info="200 ml",
        mode="maximo",
        replace=True,
    )

    snapshot = ctx.activate()

    if not snapshot["enabled"]:
        return {
            "ok": False,
            "step": "activation",
        }

    if snapshot["mode"] != "maximo":
        return {
            "ok": False,
            "step": "mode",
        }

    discount = snapshot[
        "product"
    ][
        "discount_percent"
    ]

    if discount != 34.54:
        return {
            "ok": False,
            "step": "automatic_discount",
            "value": discount,
        }

    facts = ctx.known_facts()

    if "shipping" in facts:
        return {
            "ok": False,
            "step": "invented_fact",
        }

    ctx.update(
        discount="35",
        replace=False,
    )

    snapshot = ctx.snapshot()

    if (
        snapshot["product"][
            "discount_percent"
        ]
        != 35.0
    ):
        return {
            "ok": False,
            "step": "manual_discount",
        }

    if (
        snapshot["product"][
            "discount_source"
        ]
        != "manual"
    ):
        return {
            "ok": False,
            "step": "manual_source",
        }

    ctx.deactivate()

    if ctx.snapshot()["enabled"]:
        return {
            "ok": False,
            "step": "deactivate",
        }

    return {
        "ok": True,
        "version":
            PRODUCT_CONTEXT_VERSION,
    }


if __name__ == "__main__":
    print(
        product_context_self_test()
    )
