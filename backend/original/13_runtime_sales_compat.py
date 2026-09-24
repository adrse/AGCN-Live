"""
AGCN LIVE - LEGACY SALES COMPAT BRIDGE V1.0

Purpose
-------
This module exists only to let the historical Interface V8 bridge load
inside the standalone web runtime after legacy Sales modules 14-17 were
retired from execution.

The active commercial architecture is:
- Product Context V1
- Sales Coach V2
- Comment Dispatcher sales channel

This file DOES NOT restore the old product scraping / sales pipeline.
It only exposes the legacy symbol names that Interface V8 checks during
startup.

The standalone runtime later replaces agcn_runner and supplies the real
Sales Coach V2 state independently.

Keep this module loaded AFTER Decision Coach (13) and BEFORE Interface
V8 (18).
"""

from __future__ import annotations


LEGACY_SALES_COMPAT_VERSION = "1.0"


class LegacySalesPipelineDisabled(RuntimeError):
    """Raised if an obsolete Sales pipeline execution path is invoked."""


def _disabled(name):
    raise LegacySalesPipelineDisabled(
        "Legacy Sales pipeline disabled: "
        + str(name)
        + ". Use Product Context V1 + Sales Coach V2."
    )


# ============================================================
# OLD SHOPEE PRODUCT WORKER API
# ============================================================

async def executar_worker_shopee_produto(*args, **kwargs):
    _disabled("executar_worker_shopee_produto")


def shopee_product_reset(*args, **kwargs):
    return {
        "ok": True,
        "legacy_compat": True,
    }


def shopee_product_stop(*args, **kwargs):
    return {
        "ok": True,
        "legacy_compat": True,
    }


# ============================================================
# OLD PRODUCT EXTRACTOR API
# ============================================================

def product_extractor_reset(*args, **kwargs):
    return {
        "ok": True,
        "legacy_compat": True,
    }


def product_extractor_set_seller_info(
    *args,
    **kwargs,
):
    return {
        "ok": True,
        "legacy_compat": True,
        "target": None,
    }


def product_extractor_stop(*args, **kwargs):
    return {
        "ok": True,
        "legacy_compat": True,
    }


async def executar_product_extractor(*args, **kwargs):
    _disabled("executar_product_extractor")


# ============================================================
# OLD PRODUCT SALES BUILDER API
# ============================================================

def product_sales_builder_reset(*args, **kwargs):
    return {
        "ok": True,
        "legacy_compat": True,
    }


def product_sales_builder_stop(*args, **kwargs):
    return {
        "ok": True,
        "legacy_compat": True,
    }


async def executar_product_sales_builder(*args, **kwargs):
    _disabled("executar_product_sales_builder")


# ============================================================
# OLD SALES DECISION API
# ============================================================

def sales_decision_reset(*args, **kwargs):
    return {
        "ok": True,
        "legacy_compat": True,
    }


def sales_decision_set_style(
    style="equilibrado",
    *args,
    **kwargs,
):
    normalized = (
        "pressao_feira"
        if str(style) == "pressao_feira"
        else "equilibrado"
    )

    if normalized == "pressao_feira":
        return {
            "ok": True,
            "style": normalized,
            "display_seconds": 7,
            "interval_seconds": 2,
            "cycle_seconds": 9,
            "legacy_compat": True,
        }

    return {
        "ok": True,
        "style": normalized,
        "display_seconds": 8,
        "interval_seconds": 3,
        "cycle_seconds": 11,
        "legacy_compat": True,
    }


def sales_decision_status(*args, **kwargs):
    """
    Neutral status used only by the historical Interface V8 status
    callback. NotebookRuntime.status() replaces the commercial state
    with the real Sales Coach V2 snapshot afterwards.
    """
    return {
        "style": "equilibrado",
        "display_seconds": 8,
        "interval_seconds": 3,
        "cycle_seconds": 11,
        "current_product_key": None,
        "candidate_count": 0,
        "display_active": False,
        "blank_interval": False,
        "last_error": None,
        "legacy_compat": True,
    }


def sales_decision_stop(*args, **kwargs):
    return {
        "ok": True,
        "legacy_compat": True,
    }


async def sales_decision_next_output(*args, **kwargs):
    _disabled("sales_decision_next_output")


async def executar_sales_decision_coach(*args, **kwargs):
    _disabled("executar_sales_decision_coach")
