"""
AGCN LIVE - LIVE HISTORY V1

Historico persistente das LIVEs monitoradas.

Objetivos:
- registrar inicio e fim de cada monitoramento;
- guardar somente metricas reais recebidas do backend;
- calcular pico de audiencia sem chamar isso de "visualizacoes";
- guardar totais finais quando disponiveis;
- guardar produto/Sales Coach usados naquela LIVE;
- fornecer resumo para a futura tela Inicio;
- separar historico por owner_key anonimo do navegador.

Importante:
- nao existe conta/login nesta versao;
- owner_key deve ser gerado no frontend e persistido no navegador;
- nenhum numero e inventado;
- campos indisponiveis permanecem NULL;
- SQLite usa somente biblioteca padrao do Python.

Persistencia:
- o caminho pode ser definido por AGCN_HISTORY_DB;
- padrao: <repo>/data/agcn_live_history.sqlite3;
- em Railway, para persistir entre deploys, o diretorio do banco
  devera ficar em um volume persistente.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


LIVE_HISTORY_VERSION = "1.0"

DEFAULT_TIMEZONE = os.environ.get(
    "AGCN_HISTORY_TIMEZONE",
    "America/Araguaina",
)

try:
    LIVE_HISTORY_TIMEZONE = ZoneInfo(DEFAULT_TIMEZONE)
except Exception:
    LIVE_HISTORY_TIMEZONE = timezone.utc


_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_DB = _ROOT / "data" / "agcn_live_history.sqlite3"

LIVE_HISTORY_DB = Path(
    os.environ.get(
        "AGCN_HISTORY_DB",
        str(_DEFAULT_DB),
    )
)

_OWNER_RE = re.compile(r"^[A-Za-z0-9_-]{16,160}$")


class LiveHistoryError(ValueError):
    """Erro de validacao do historico."""


def _now():
    return time.time()


def _as_float(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def _as_int(value):
    number = _as_float(value)
    if number is None:
        return None
    try:
        return int(number)
    except Exception:
        return None


def _max_nullable(old, new):
    if new is None:
        return old
    if old is None:
        return new
    return max(old, new)


def _clean_text(value, limit):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:limit]


def _normalize_owner_key(value):
    key = str(value or "").strip()
    if not _OWNER_RE.fullmatch(key):
        raise LiveHistoryError("owner_key invalido.")
    return key


def _json_dump(value):
    if value is None:
        return None
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def _json_load(value, default=None):
    if not value:
        return {} if default is None else default
    try:
        return json.loads(value)
    except Exception:
        return {} if default is None else default


def _local_date_from_ts(timestamp):
    return datetime.fromtimestamp(
        float(timestamp),
        tz=LIVE_HISTORY_TIMEZONE,
    ).date()


def _day_start_ts(day):
    local = datetime(
        day.year,
        day.month,
        day.day,
        tzinfo=LIVE_HISTORY_TIMEZONE,
    )
    return local.timestamp()


class LiveHistory:
    """Camada SQLite thread-safe para o historico de LIVEs."""

    def __init__(self, db_path=None):
        self.db_path = Path(db_path or LIVE_HISTORY_DB)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self):
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def _initialize(self):
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS live_history (
                        id TEXT PRIMARY KEY,
                        owner_key TEXT NOT NULL,
                        runtime_session TEXT,
                        platform TEXT NOT NULL,
                        subject TEXT,
                        live_id TEXT,
                        started_at REAL NOT NULL,
                        ended_at REAL,
                        duration_seconds REAL,
                        status TEXT NOT NULL,
                        audience_peak INTEGER,
                        audience_last INTEGER,
                        total_users INTEGER,
                        total_users_semantics TEXT,
                        likes_final INTEGER,
                        shares_final INTEGER,
                        follows_final INTEGER,
                        gifts_final INTEGER,
                        products_final INTEGER,
                        comments_count INTEGER NOT NULL DEFAULT 0,
                        product_name TEXT,
                        sales_coach_enabled INTEGER NOT NULL DEFAULT 0,
                        sales_mode TEXT,
                        extra_json TEXT,
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_live_history_owner_started
                    ON live_history (owner_key, started_at DESC)
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_live_history_owner_status
                    ON live_history (owner_key, status)
                    """
                )
            finally:
                conn.close()

    def _row_to_dict(self, row):
        if row is None:
            return None
        result = dict(row)
        result["sales_coach_enabled"] = bool(
            result.get("sales_coach_enabled")
        )
        result["extra"] = _json_load(
            result.pop("extra_json", None),
            default={},
        )
        return result

    def start_live(
        self,
        *,
        owner_key,
        platform,
        runtime_session=None,
        subject=None,
        live_id=None,
        product_name=None,
        sales_coach_enabled=False,
        sales_mode=None,
        started_at=None,
        extra=None,
    ):
        owner_key = _normalize_owner_key(owner_key)
        platform = str(platform or "").strip().lower()

        if platform not in {"shopee", "tiktok"}:
            raise LiveHistoryError("Plataforma invalida.")

        if started_at is None:
            started_at = _now()

        started_at = _as_float(started_at)
        if started_at is None:
            raise LiveHistoryError("started_at invalido.")

        live_record_id = uuid.uuid4().hex
        now = _now()

        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    INSERT INTO live_history (
                        id, owner_key, runtime_session, platform,
                        subject, live_id, started_at, status,
                        product_name, sales_coach_enabled, sales_mode,
                        extra_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        live_record_id,
                        owner_key,
                        _clean_text(runtime_session, 240),
                        platform,
                        _clean_text(subject, 500),
                        _clean_text(live_id, 300),
                        started_at,
                        "active",
                        _clean_text(product_name, 300),
                        1 if sales_coach_enabled else 0,
                        _clean_text(sales_mode, 40),
                        _json_dump(extra if isinstance(extra, dict) else {}),
                        now,
                        now,
                    ),
                )
            finally:
                conn.close()

        return self.get_live(
            owner_key=owner_key,
            live_record_id=live_record_id,
        )

    def update_live(
        self,
        *,
        owner_key,
        live_record_id,
        metrics=None,
        comments_count=None,
        subject=None,
        live_id=None,
        product_name=None,
        sales_coach_enabled=None,
        sales_mode=None,
        total_users=None,
        total_users_semantics=None,
        extra=None,
        observed_at=None,
    ):
        owner_key = _normalize_owner_key(owner_key)

        record = self.get_live(
            owner_key=owner_key,
            live_record_id=live_record_id,
        )
        if record is None:
            raise LiveHistoryError("Registro de LIVE nao encontrado.")
        if record.get("status") != "active":
            return record

        metrics = metrics if isinstance(metrics, dict) else {}

        viewers = _as_int(metrics.get("viewers"))
        likes = _as_int(metrics.get("likes"))
        shares = _as_int(metrics.get("shares"))
        follows = _as_int(metrics.get("follows"))
        gifts = _as_int(metrics.get("gifts"))
        products = _as_int(metrics.get("products"))
        comments_count = _as_int(comments_count)
        total_users = _as_int(total_users)

        audience_peak = _max_nullable(
            record.get("audience_peak"),
            viewers,
        )
        comment_max = _max_nullable(
            record.get("comments_count"),
            comments_count,
        )

        previous_extra = record.get("extra") or {}
        if isinstance(extra, dict):
            previous_extra.update(extra)
        if observed_at is not None:
            previous_extra["last_observed_at"] = observed_at

        now = _now()

        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    UPDATE live_history
                    SET
                        subject = COALESCE(?, subject),
                        live_id = COALESCE(?, live_id),
                        audience_peak = ?,
                        audience_last = COALESCE(?, audience_last),
                        total_users = COALESCE(?, total_users),
                        total_users_semantics = COALESCE(?, total_users_semantics),
                        likes_final = COALESCE(?, likes_final),
                        shares_final = COALESCE(?, shares_final),
                        follows_final = COALESCE(?, follows_final),
                        gifts_final = COALESCE(?, gifts_final),
                        products_final = COALESCE(?, products_final),
                        comments_count = ?,
                        product_name = COALESCE(?, product_name),
                        sales_coach_enabled = COALESCE(?, sales_coach_enabled),
                        sales_mode = COALESCE(?, sales_mode),
                        extra_json = ?,
                        updated_at = ?
                    WHERE id = ?
                      AND owner_key = ?
                    """,
                    (
                        _clean_text(subject, 500),
                        _clean_text(live_id, 300),
                        audience_peak,
                        viewers,
                        total_users,
                        _clean_text(total_users_semantics, 80),
                        likes,
                        shares,
                        follows,
                        gifts,
                        products,
                        int(comment_max or 0),
                        _clean_text(product_name, 300),
                        (
                            1 if sales_coach_enabled else 0
                        ) if sales_coach_enabled is not None else None,
                        _clean_text(sales_mode, 40),
                        _json_dump(previous_extra),
                        now,
                        live_record_id,
                        owner_key,
                    ),
                )
            finally:
                conn.close()

        return self.get_live(
            owner_key=owner_key,
            live_record_id=live_record_id,
        )

    def finish_live(
        self,
        *,
        owner_key,
        live_record_id,
        status="finished",
        ended_at=None,
        metrics=None,
        comments_count=None,
        subject=None,
        live_id=None,
        product_name=None,
        sales_coach_enabled=None,
        sales_mode=None,
        total_users=None,
        total_users_semantics=None,
        extra=None,
    ):
        owner_key = _normalize_owner_key(owner_key)

        if status not in {"finished", "stopped", "error"}:
            raise LiveHistoryError("Status final invalido.")

        self.update_live(
            owner_key=owner_key,
            live_record_id=live_record_id,
            metrics=metrics,
            comments_count=comments_count,
            subject=subject,
            live_id=live_id,
            product_name=product_name,
            sales_coach_enabled=sales_coach_enabled,
            sales_mode=sales_mode,
            total_users=total_users,
            total_users_semantics=total_users_semantics,
            extra=extra,
        )

        record = self.get_live(
            owner_key=owner_key,
            live_record_id=live_record_id,
        )
        if record is None:
            raise LiveHistoryError("Registro de LIVE nao encontrado.")
        if record.get("status") != "active":
            return record

        if ended_at is None:
            ended_at = _now()

        ended_at = _as_float(ended_at)
        if ended_at is None:
            raise LiveHistoryError("ended_at invalido.")

        duration = max(
            0.0,
            ended_at - float(record["started_at"]),
        )
        now = _now()

        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    UPDATE live_history
                    SET ended_at = ?,
                        duration_seconds = ?,
                        status = ?,
                        updated_at = ?
                    WHERE id = ?
                      AND owner_key = ?
                    """,
                    (
                        ended_at,
                        duration,
                        status,
                        now,
                        live_record_id,
                        owner_key,
                    ),
                )
            finally:
                conn.close()

        return self.get_live(
            owner_key=owner_key,
            live_record_id=live_record_id,
        )

    def get_live(
        self,
        *,
        owner_key,
        live_record_id,
    ):
        owner_key = _normalize_owner_key(owner_key)

        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    """
                    SELECT *
                    FROM live_history
                    WHERE id = ?
                      AND owner_key = ?
                    LIMIT 1
                    """,
                    (
                        live_record_id,
                        owner_key,
                    ),
                ).fetchone()
            finally:
                conn.close()

        return self._row_to_dict(row)

    def recent_lives(
        self,
        *,
        owner_key,
        limit=20,
        include_active=True,
    ):
        owner_key = _normalize_owner_key(owner_key)

        try:
            limit = max(1, min(int(limit), 200))
        except Exception:
            limit = 20

        where = "" if include_active else "AND status <> 'active'"

        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    f"""
                    SELECT *
                    FROM live_history
                    WHERE owner_key = ?
                    {where}
                    ORDER BY started_at DESC
                    LIMIT ?
                    """,
                    (
                        owner_key,
                        limit,
                    ),
                ).fetchall()
            finally:
                conn.close()

        return [self._row_to_dict(row) for row in rows]

    def last_live(
        self,
        *,
        owner_key,
        include_active=False,
    ):
        items = self.recent_lives(
            owner_key=owner_key,
            limit=1,
            include_active=include_active,
        )
        return items[0] if items else None

    def home_summary(
        self,
        *,
        owner_key,
        days=7,
    ):
        owner_key = _normalize_owner_key(owner_key)

        try:
            days = int(days)
        except Exception:
            days = 7

        if days not in {7, 30}:
            raise LiveHistoryError(
                "Periodo invalido. Use 7 ou 30 dias."
            )

        now_local = datetime.now(LIVE_HISTORY_TIMEZONE)
        today = now_local.date()
        first_day = today - timedelta(days=days - 1)
        start_ts = _day_start_ts(first_day)

        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM live_history
                    WHERE owner_key = ?
                      AND started_at >= ?
                      AND status <> 'active'
                    ORDER BY started_at ASC
                    """,
                    (
                        owner_key,
                        start_ts,
                    ),
                ).fetchall()
            finally:
                conn.close()

        records = [self._row_to_dict(row) for row in rows]

        by_day = {
            (first_day + timedelta(days=offset)): {
                "date": (
                    first_day + timedelta(days=offset)
                ).isoformat(),
                "lives": 0,
                "audience_peak": 0,
            }
            for offset in range(days)
        }

        for record in records:
            day = _local_date_from_ts(record["started_at"])
            bucket = by_day.get(day)
            if bucket is None:
                continue

            bucket["lives"] += 1
            peak = _as_int(record.get("audience_peak"))
            if peak is not None:
                bucket["audience_peak"] = max(
                    bucket["audience_peak"],
                    peak,
                )

        series = [
            by_day[day]
            for day in sorted(by_day)
        ]

        completed = [
            record
            for record in records
            if record.get("status")
            in {"finished", "stopped", "error"}
        ]

        peaks = [
            _as_int(item.get("audience_peak"))
            for item in completed
        ]
        peaks = [
            value
            for value in peaks
            if value is not None
        ]

        best_peak = max(peaks) if peaks else None
        average_peak = (
            round(sum(peaks) / len(peaks), 1)
            if peaks
            else None
        )

        return {
            "version": LIVE_HISTORY_VERSION,
            "period_days": days,
            "metric": {
                "key": "audience_peak",
                "label": "Pico de audiencia",
                "semantics": (
                    "Maior audiencia simultanea observada em cada LIVE."
                ),
            },
            "total_lives": len(completed),
            "best_audience_peak": best_peak,
            "average_audience_peak": average_peak,
            "series": series,
            "last_live": self.last_live(
                owner_key=owner_key,
                include_active=False,
            ),
        }

    def active_live(
        self,
        *,
        owner_key,
    ):
        owner_key = _normalize_owner_key(owner_key)

        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    """
                    SELECT *
                    FROM live_history
                    WHERE owner_key = ?
                      AND status = 'active'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    (owner_key,),
                ).fetchone()
            finally:
                conn.close()

        return self._row_to_dict(row)

    def mark_abandoned_active(
        self,
        *,
        owner_key,
        status="stopped",
    ):
        active = self.active_live(
            owner_key=owner_key
        )
        if active is None:
            return None

        return self.finish_live(
            owner_key=owner_key,
            live_record_id=active["id"],
            status=status,
            ended_at=_now(),
            extra={"ended_by": "recovery"},
        )


_default_history = None
_default_history_lock = threading.Lock()


def get_live_history():
    global _default_history

    if _default_history is not None:
        return _default_history

    with _default_history_lock:
        if _default_history is None:
            _default_history = LiveHistory()

    return _default_history


def live_history_self_test():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "history.sqlite3"
        history = LiveHistory(db)

        owner = "test_owner_" + "a" * 24
        started = 1_700_000_000.0

        record = history.start_live(
            owner_key=owner,
            platform="tiktok",
            runtime_session="session-test",
            product_name="Produto Teste",
            sales_coach_enabled=True,
            sales_mode="leve",
            started_at=started,
        )

        history.update_live(
            owner_key=owner,
            live_record_id=record["id"],
            metrics={
                "viewers": 120,
                "likes": 300,
                "shares": 12,
            },
            comments_count=8,
            total_users=500,
            total_users_semantics="platform_total_user",
        )

        history.update_live(
            owner_key=owner,
            live_record_id=record["id"],
            metrics={
                "viewers": 180,
                "likes": 450,
                "shares": 20,
            },
            comments_count=14,
        )

        finished = history.finish_live(
            owner_key=owner,
            live_record_id=record["id"],
            status="finished",
            ended_at=started + 600,
            metrics={
                "viewers": 90,
                "likes": 500,
                "shares": 25,
            },
            comments_count=16,
        )

        assert finished["audience_peak"] == 180
        assert finished["duration_seconds"] == 600
        assert finished["comments_count"] == 16
        assert history.active_live(owner_key=owner) is None

        return {
            "ok": True,
            "version": LIVE_HISTORY_VERSION,
            "record": finished,
        }


if __name__ == "__main__":
    print(live_history_self_test())
