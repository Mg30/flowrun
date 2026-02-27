"""SQLite-backed state store with write-through cache, optional TTL, and opt-in crash recovery."""

from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

from flowrun.serialization import JsonSerializer, ResultSerializer
from flowrun.state import RunRecord, TaskRunRecord, TaskStatus

_log = logging.getLogger("flowrun.sqlite_store")

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    dag_name    TEXT NOT NULL,
    created_at  REAL NOT NULL,
    finished_at REAL
);

CREATE TABLE IF NOT EXISTS task_runs (
    run_id      TEXT    NOT NULL,
    task_name   TEXT    NOT NULL,
    attempt     INTEGER NOT NULL DEFAULT 0,
    status      TEXT    NOT NULL DEFAULT 'PENDING',
    started_at  REAL,
    finished_at REAL,
    error       TEXT,
    result      BLOB,
    PRIMARY KEY (run_id, task_name),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
"""


class SqliteStateStore:
    """Persistent state store backed by SQLite.

    Reads go through an in-memory write-through cache so the scheduler's
    tight loop stays fast.  Every mutation is written to both the cache
    *and* SQLite in the same call.

    Parameters
    ----------
    db_path : str | Path
        Path to the SQLite database file, or ``":memory:"`` for a
        transient in-memory database useful in tests.
    serializer : ResultSerializer | None
        Strategy for serializing / deserializing task results stored in
        the ``result BLOB`` column. Defaults to :class:`JsonSerializer`.
    cache_ttl_s : float | None
        When set, cached ``RunRecord`` objects that have not been accessed
        for longer than *cache_ttl_s* seconds may be evicted from memory.
        They remain in SQLite and will be reloaded on demand.  ``None``
        (the default) keeps entries cached indefinitely.
    recover : bool
        When ``True`` the constructor scans for orphaned ``RUNNING`` tasks
        (left behind by a crashed process) and resets them to ``FAILED``
        with ``error='PROCESS_CRASH'``.  Defaults to ``False``.
    """

    # Re-use the same valid-transition map as the in-memory store.
    _VALID_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
        "PENDING": {"RUNNING", "SKIPPED"},
        "RUNNING": {"SUCCESS", "FAILED"},
        "SUCCESS": set(),
        "FAILED": {"PENDING"},
        "SKIPPED": set(),
    }

    def __init__(
        self,
        db_path: str | Path,
        *,
        serializer: ResultSerializer | None = None,
        cache_ttl_s: float | None = None,
        recover: bool = False,
    ) -> None:
        """Open (or create) the SQLite database at *db_path* and prepare the schema."""
        self._serializer: ResultSerializer = serializer or JsonSerializer()
        self._cache_ttl_s = cache_ttl_s

        self._conn = sqlite3.connect(
            str(db_path),
            check_same_thread=False,
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)

        # Cache: run_id -> (RunRecord, last_access_time)
        self._cache: dict[str, tuple[RunRecord, float]] = {}

        if recover:
            self._recover_crashed()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._conn.close()

    # ------------------------------------------------------------------
    # Public API (satisfies StateStoreProtocol)
    # ------------------------------------------------------------------

    def create_run(self, run_id: str, dag_name: str, task_names: list[str]) -> RunRecord:
        """Create and persist a brand-new run record."""
        rec = RunRecord(
            run_id=run_id,
            dag_name=dag_name,
            tasks={t: TaskRunRecord(task_name=t) for t in task_names},
        )
        self._insert_run(rec)
        self._cache_put(rec)
        return rec

    def create_resumed_run(
        self,
        run_id: str,
        prev_run_id: str,
        dag_name: str,
        task_names: list[str],
        reset_tasks: set[str] | None = None,
    ) -> RunRecord:
        """Create a new run that inherits successful task results from *prev_run_id*."""
        prev = self.get_run(prev_run_id)
        reset_tasks = reset_tasks or set()
        rec = RunRecord(run_id=run_id, dag_name=dag_name, tasks={})
        for t in task_names:
            prev_task = prev.tasks.get(t)
            if prev_task and prev_task.status == "SUCCESS" and t not in reset_tasks:
                rec.tasks[t] = TaskRunRecord(
                    task_name=t,
                    attempt=prev_task.attempt,
                    status="SUCCESS",
                    started_at=prev_task.started_at,
                    finished_at=prev_task.finished_at,
                    result=prev_task.result,
                )
            else:
                rec.tasks[t] = TaskRunRecord(task_name=t)
        self._insert_run(rec)
        self._cache_put(rec)
        return rec

    def get_run(self, run_id: str) -> RunRecord:
        """Return the RunRecord for *run_id*, loading from SQLite on a cache miss."""
        hit = self._cache.get(run_id)
        if hit is not None:
            rec, _ = hit
            self._cache[run_id] = (rec, time.time())
            return rec
        # Cache miss — load from SQLite
        rec = self._load_run(run_id)
        self._cache_put(rec)
        return rec

    def mark_running(self, run_id: str, task_name: str) -> None:
        """Transition *task_name* to RUNNING and record its start time."""
        tr = self._transition(run_id, task_name, "RUNNING")
        tr.started_at = time.time()
        tr.attempt += 1
        self._persist_task(run_id, tr)

    def mark_success(self, run_id: str, task_name: str, result: object) -> None:
        """Transition *task_name* to SUCCESS and store its *result*."""
        tr = self._transition(run_id, task_name, "SUCCESS")
        tr.result = result
        tr.finished_at = time.time()
        self._persist_task(run_id, tr)

    def mark_failed(self, run_id: str, task_name: str, err: str) -> None:
        """Transition *task_name* to FAILED and record the error message."""
        tr = self._transition(run_id, task_name, "FAILED")
        tr.error = err
        tr.finished_at = time.time()
        self._persist_task(run_id, tr)

    def mark_skipped(self, run_id: str, task_name: str, reason: str) -> None:
        """Transition *task_name* to SKIPPED and record the skip *reason*."""
        tr = self._transition(run_id, task_name, "SKIPPED")
        tr.error = reason
        tr.finished_at = time.time()
        self._persist_task(run_id, tr)

    def mark_retry(self, run_id: str, task_name: str) -> None:
        """Reset *task_name* back to PENDING so it can be retried."""
        tr = self._transition(run_id, task_name, "PENDING")
        tr.error = None
        tr.finished_at = None
        self._persist_task(run_id, tr)

    def clear_result(self, run_id: str, task_name: str) -> None:
        """Remove the stored result for *task_name* in *run_id*."""
        rec = self.get_run(run_id)
        rec.tasks[task_name].result = None
        self._conn.execute(
            "UPDATE task_runs SET result = NULL WHERE run_id = ? AND task_name = ?",
            (run_id, task_name),
        )
        self._conn.commit()

    def finalize_run_if_done(self, run_id: str) -> None:
        """Set the run's finished_at timestamp once all tasks have reached a terminal state."""
        rec = self.get_run(run_id)
        if rec.finished_at is not None:
            return
        if all(t.status in ("SUCCESS", "FAILED", "SKIPPED") for t in rec.tasks.values()):
            rec.finished_at = time.time()
            self._conn.execute(
                "UPDATE runs SET finished_at = ? WHERE run_id = ?",
                (rec.finished_at, run_id),
            )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Extra helpers (not in Protocol — SQLite-specific)
    # ------------------------------------------------------------------

    def list_runs(self, dag_name: str | None = None) -> list[RunRecord]:
        """Return stored runs, optionally filtered by DAG name.

        Loads runs from SQLite (does **not** require them to be cached).
        """
        if dag_name is not None:
            rows = self._conn.execute(
                "SELECT run_id FROM runs WHERE dag_name = ? ORDER BY created_at",
                (dag_name,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT run_id FROM runs ORDER BY created_at",
            ).fetchall()
        return [self.get_run(row[0]) for row in rows]

    # ------------------------------------------------------------------
    # Internal — state machine
    # ------------------------------------------------------------------

    def _transition(self, run_id: str, task_name: str, target: TaskStatus) -> TaskRunRecord:
        rec = self.get_run(run_id)
        tr = rec.tasks[task_name]
        allowed = self._VALID_TRANSITIONS.get(tr.status, set())
        if target not in allowed:
            raise RuntimeError(f"Invalid state transition for task {task_name!r}: {tr.status} -> {target}")
        tr.status = target
        return tr

    # ------------------------------------------------------------------
    # Internal — SQLite persistence
    # ------------------------------------------------------------------

    def _insert_run(self, rec: RunRecord) -> None:
        """INSERT a full RunRecord (run + all task rows) inside a transaction."""
        cur = self._conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.execute(
                "INSERT INTO runs (run_id, dag_name, created_at, finished_at) VALUES (?, ?, ?, ?)",
                (rec.run_id, rec.dag_name, rec.created_at, rec.finished_at),
            )
            for tr in rec.tasks.values():
                result_blob = self._serialize_result(tr.result)
                cur.execute(
                    "INSERT INTO task_runs (run_id, task_name, attempt, status, started_at, finished_at, error, result) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        rec.run_id,
                        tr.task_name,
                        tr.attempt,
                        tr.status,
                        tr.started_at,
                        tr.finished_at,
                        tr.error,
                        result_blob,
                    ),
                )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

    def _persist_task(self, run_id: str, tr: TaskRunRecord) -> None:
        """Persist a single task record mutation to SQLite."""
        result_blob = self._serialize_result(tr.result)
        self._conn.execute(
            "UPDATE task_runs SET attempt=?, status=?, started_at=?, finished_at=?, error=?, result=? "
            "WHERE run_id=? AND task_name=?",
            (tr.attempt, tr.status, tr.started_at, tr.finished_at, tr.error, result_blob, run_id, tr.task_name),
        )
        self._conn.commit()

    def _load_run(self, run_id: str) -> RunRecord:
        """Load a RunRecord from SQLite.  Raises ``KeyError`` if missing."""
        row = self._conn.execute(
            "SELECT run_id, dag_name, created_at, finished_at FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"Run {run_id!r} not found")
        rec = RunRecord(run_id=row[0], dag_name=row[1], created_at=row[2], finished_at=row[3], tasks={})
        task_rows = self._conn.execute(
            "SELECT task_name, attempt, status, started_at, finished_at, error, result FROM task_runs WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        for tr_row in task_rows:
            result = self._deserialize_result(tr_row[6])
            rec.tasks[tr_row[0]] = TaskRunRecord(
                task_name=tr_row[0],
                attempt=tr_row[1],
                status=tr_row[2],
                started_at=tr_row[3],
                finished_at=tr_row[4],
                error=tr_row[5],
                result=result,
            )
        return rec

    # ------------------------------------------------------------------
    # Internal — serialization helpers
    # ------------------------------------------------------------------

    def _serialize_result(self, obj: Any) -> bytes | None:
        if obj is None:
            return None
        return self._serializer.serialize(obj)

    def _deserialize_result(self, data: bytes | None) -> Any:
        if data is None:
            return None
        return self._serializer.deserialize(data)

    # ------------------------------------------------------------------
    # Internal — cache management
    # ------------------------------------------------------------------

    def _cache_put(self, rec: RunRecord) -> None:
        self._cache[rec.run_id] = (rec, time.time())
        self._maybe_evict()

    def _maybe_evict(self) -> None:
        """Evict cache entries older than *cache_ttl_s* (if configured)."""
        if self._cache_ttl_s is None:
            return
        now = time.time()
        stale = [rid for rid, (_, ts) in self._cache.items() if (now - ts) > self._cache_ttl_s]
        for rid in stale:
            del self._cache[rid]

    # ------------------------------------------------------------------
    # Internal — crash recovery
    # ------------------------------------------------------------------

    def _recover_crashed(self) -> None:
        """Reset orphaned RUNNING tasks to FAILED with error 'PROCESS_CRASH'."""
        now = time.time()
        cur = self._conn.execute("SELECT run_id, task_name FROM task_runs WHERE status = 'RUNNING'")
        rows = cur.fetchall()
        if not rows:
            return
        _log.info("Recovering %d orphaned RUNNING task(s)", len(rows))
        for run_id, task_name in rows:
            self._conn.execute(
                "UPDATE task_runs SET status='FAILED', error='PROCESS_CRASH', finished_at=? "
                "WHERE run_id=? AND task_name=?",
                (now, run_id, task_name),
            )
        self._conn.commit()
        # Invalidate any cached entries for affected runs so they are reloaded.
        affected_run_ids = {r[0] for r in rows}
        for rid in affected_run_ids:
            self._cache.pop(rid, None)
