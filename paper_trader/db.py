"""SQLite persistence for paper-trading bots, trades, and daily P&L."""

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

SCHEMA = """
CREATE TABLE IF NOT EXISTS bots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT    NOT NULL,
    symbol          TEXT    NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'initialized',
    stage           TEXT    NOT NULL DEFAULT 'initialized',
    cancel_requested INTEGER DEFAULT 0,
    pm_open         REAL,
    pm_close        REAL,
    mkt_open        REAL,
    stock_jump      REAL,
    dji_bps         REAL,
    signal_detected INTEGER DEFAULT 0,
    notes           TEXT,
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL,
    UNIQUE(date, symbol)
);

CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_id          INTEGER NOT NULL,
    date            TEXT    NOT NULL,
    symbol          TEXT    NOT NULL,
    entry_time      TEXT,
    exit_time       TEXT,
    spot_entry      REAL,
    spot_exit       REAL,
    strike          REAL,
    sigma           REAL,
    delta_entry     REAL,
    entry_premium   REAL,
    exit_premium    REAL,
    contracts       INTEGER DEFAULT 1,
    entry_cost      REAL,
    pnl             REAL,
    pnl_pct         REAL,
    exit_reason     TEXT,
    status          TEXT    NOT NULL DEFAULT 'open',
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL,
    FOREIGN KEY (bot_id) REFERENCES bots(id)
);
"""


class PaperTradingDB:
    def __init__(self, db_path: str = "data/paper_trading.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    def _now(self) -> str:
        return datetime.now(ET).isoformat()

    # ── bots ──────────────────────────────────────────────────────────

    def create_bot(self, date: str, symbol: str) -> int:
        now = self._now()
        with self._conn() as conn:
            cur = conn.execute(
                """INSERT OR IGNORE INTO bots
                   (date, symbol, status, stage, created_at, updated_at)
                   VALUES (?, ?, 'initialized', 'initialized', ?, ?)""",
                (date, symbol, now, now),
            )
            if cur.rowcount == 0:
                row = conn.execute(
                    "SELECT id FROM bots WHERE date=? AND symbol=?",
                    (date, symbol),
                ).fetchone()
                return row["id"]
            return cur.lastrowid

    def update_bot(self, bot_id: int, **fields):
        fields["updated_at"] = self._now()
        sets = ", ".join(f"{k}=?" for k in fields)
        vals = list(fields.values()) + [bot_id]
        with self._conn() as conn:
            conn.execute(f"UPDATE bots SET {sets} WHERE id=?", vals)

    def get_bot(self, bot_id: int) -> Optional[Dict]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM bots WHERE id=?", (bot_id,)).fetchone()
            return dict(row) if row else None

    def get_bots_for_date(self, date: str) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM bots WHERE date=? ORDER BY symbol", (date,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_active_bots(self) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM bots
                   WHERE status NOT IN ('completed','no_signal','cancelled')
                   ORDER BY date DESC, symbol"""
            ).fetchall()
            return [dict(r) for r in rows]

    def get_all_bot_dates(self) -> List[str]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT date FROM bots ORDER BY date"
            ).fetchall()
            return [r["date"] for r in rows]

    def request_cancel(self, bot_id: int):
        self.update_bot(bot_id, cancel_requested=1)

    def is_cancel_requested(self, bot_id: int) -> bool:
        bot = self.get_bot(bot_id)
        return bool(bot and bot["cancel_requested"])

    # ── trades ────────────────────────────────────────────────────────

    def create_trade(
        self,
        bot_id: int,
        date: str,
        symbol: str,
        spot_entry: float,
        strike: float,
        sigma: float,
        delta_entry: float,
        entry_premium: float,
        contracts: int,
        entry_time: Optional[str] = None,
    ) -> int:
        now = self._now()
        entry_cost = entry_premium * 100 * contracts
        with self._conn() as conn:
            cur = conn.execute(
                """INSERT INTO trades
                   (bot_id, date, symbol, entry_time, spot_entry, strike,
                    sigma, delta_entry, entry_premium, contracts, entry_cost,
                    status, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,'open',?,?)""",
                (
                    bot_id, date, symbol, entry_time or now,
                    spot_entry, strike, sigma, delta_entry,
                    entry_premium, contracts, entry_cost, now, now,
                ),
            )
            return cur.lastrowid

    def close_trade(
        self,
        trade_id: int,
        spot_exit: float,
        exit_premium: float,
        exit_reason: str,
        exit_time: Optional[str] = None,
    ):
        with self._conn() as conn:
            row = conn.execute(
                "SELECT entry_premium, contracts FROM trades WHERE id=?", (trade_id,)
            ).fetchone()
            if not row:
                return
            pnl = (exit_premium - row["entry_premium"]) * 100 * row["contracts"]
            pnl_pct = (
                (exit_premium - row["entry_premium"]) / row["entry_premium"] * 100
                if row["entry_premium"] > 0 else 0
            )
            now = self._now()
            conn.execute(
                """UPDATE trades SET
                   spot_exit=?, exit_premium=?, exit_reason=?, exit_time=?,
                   pnl=?, pnl_pct=?, status='closed', updated_at=?
                   WHERE id=?""",
                (spot_exit, exit_premium, exit_reason, exit_time or now,
                 round(pnl, 2), round(pnl_pct, 1), now, trade_id),
            )

    def get_open_trades(self) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM trades WHERE status='open' ORDER BY date, symbol"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_trades_for_date(self, date: str) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM trades WHERE date=? ORDER BY symbol", (date,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_all_trades(self, limit: int = 500) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY date DESC, symbol LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_trade_by_bot(self, bot_id: int) -> Optional[Dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM trades WHERE bot_id=? ORDER BY id DESC LIMIT 1",
                (bot_id,),
            ).fetchone()
            return dict(row) if row else None

    # ── aggregated queries ────────────────────────────────────────────

    def get_daily_pnl(self) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT date,
                          SUM(pnl)        AS total_pnl,
                          SUM(entry_cost) AS total_cost,
                          COUNT(*)        AS trade_count,
                          SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins
                   FROM trades WHERE status='closed'
                   GROUP BY date ORDER BY date"""
            ).fetchall()
            return [dict(r) for r in rows]

    def get_daily_pnl_by_symbol(self) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT date, symbol,
                          SUM(pnl)        AS total_pnl,
                          SUM(entry_cost) AS total_cost,
                          COUNT(*)        AS trade_count,
                          SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins
                   FROM trades WHERE status='closed'
                   GROUP BY date, symbol ORDER BY date, symbol"""
            ).fetchall()
            return [dict(r) for r in rows]

    def get_summary(self, date: Optional[str] = None) -> Dict:
        clause = "WHERE status='closed'" + (f" AND date='{date}'" if date else "")
        with self._conn() as conn:
            row = conn.execute(
                f"""SELECT COUNT(*)        AS trades,
                           SUM(pnl)        AS total_pnl,
                           SUM(entry_cost) AS total_invested,
                           SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) AS wins
                    FROM trades {clause}"""
            ).fetchone()
            d = dict(row)
            for k in ("total_pnl", "total_invested"):
                if d[k] is None:
                    d[k] = 0.0
            if d["trades"] is None:
                d["trades"] = 0
            if d["wins"] is None:
                d["wins"] = 0
            d["win_rate"] = (
                round(d["wins"] / d["trades"] * 100, 1) if d["trades"] else 0.0
            )
            d["roi"] = (
                round(d["total_pnl"] / d["total_invested"] * 100, 1)
                if d["total_invested"] else 0.0
            )
            return d
