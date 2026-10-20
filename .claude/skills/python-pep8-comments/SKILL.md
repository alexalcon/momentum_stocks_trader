---
name: "python-pep8-comments"
description: "Apply PEP 8-compliant Python commenting conventions to all generated code, tailored for algorithmic trading systems. Use this skill EVERY TIME code is generated, written, or modified in this project. Trigger on any Python code output — new modules, functions, classes, configuration blocks, database models, API routes, trading pipeline stages, risk engine logic, signal generators, order managers, and test files. Also trigger when the user says 'add comments', 'document this', 'annotate', 'write code for X', 'implement X', or any variation requesting Python code. This skill is ALWAYS active for Python code in this project — never skip it even for short snippets or utility functions."
---

# Python PEP 8 Comments Skill

This skill enforces consistent, PEP 8-compliant Python commenting for all code in the `risk_gated_llm_trader` project. Every piece of Python code generated must follow these rules exactly.

## Core Formatting Rules (apply to ALL code)

1. Line length for comments: 79 characters maximum (matching PEP 8 code limit).
2. Inline comments: two spaces before the `#`, one space after.
3. Block comments: `#` followed by one space, aligned to the code block's indentation level.
4. No redundant comments. Never restate what the code obviously does.
5. Comment the WHY and the WHAT (architectural intent, trading logic, risk invariants), not the HOW (Python syntax).
6. Section separators use the Unicode box-drawing character `─` (U+2500), NOT hyphens `-`. The full line must be exactly 79 characters: `# ` (2 chars) followed by 77 `─` characters.
7. No trailing whitespace on comment lines.
8. All docstrings use triple double-quotes `"""`.

---

## Comment Type Reference

### Module-Level Docstring

Every module starts with a docstring immediately after any `from __future__` imports. It states the module's role in the system pipeline, its key invariants, and which component it belongs to.

```python
"""
thesis_engine.py -- LLM Thesis Engine (Component: thesis_engine)

Generates a structured directional thesis (long/short/flat) for a single
crypto asset per daily session. Inputs: OHLCV price data, Hugging Face
sentiment scores, and the previous thesis from the database. Output: a
validated ThesisOutput schema object.

Invariants:
    - The LLM never sizes or places orders. Direction only.
    - Malformed or out-of-band outputs are rejected before returning.
    - Re-evaluation always loads the prior thesis; cold-start is allowed
      only if no prior thesis exists for this symbol.
"""
```

### Class Docstring

States the class responsibility, which pipeline stage it belongs to, and any thread-safety or lifecycle notes.

```python
class RiskEngine:
    """
    Deterministic risk gate. The sole component authorized to approve,
    resize, or reject a proposed order.

    Pipeline stage: post-confluence, pre-execution.
    Thread safety: single-threaded; one instance per backend session.

    Raises:
        RiskRejectionError: when an order breaches any hard limit.
    """
```

### Method / Function Docstring

One-line summary, then Args, Returns, Raises. Skip sections that do not apply. Use the NumPy docstring style because it reads cleanly in terminal output and is standard in quantitative/scientific Python.

```python
def evaluate_confluence(
    thesis_direction: str,
    signal_direction: str,
    symbol: str,
) -> bool:
    """
    Return True only when LLM thesis and technical signal agree.

    Neither side alone is sufficient to open a position. Disagreement
    is logged and the session skips this symbol for the day.

    Parameters
    ----------
    thesis_direction : str
        Direction from the validated LLM thesis. One of {'long', 'short',
        'flat'}.
    signal_direction : str
        Direction from the deterministic strategy module. One of {'long',
        'short', 'flat'}.
    symbol : str
        Binance USDT-M Futures symbol, e.g. 'BTCUSDT'. Used only for
        audit logging on disagreement.

    Returns
    -------
    bool
        True if both directions match and neither is 'flat'.

    Raises
    ------
    ValueError
        If either direction is not in the allowed set.
    """
```

### Inline Comment

Two spaces before `#`, one space after. Explain WHY, not WHAT.

```python
leverage = min(requested_leverage, self.MAX_LEVERAGE)  # hard cap, never negotiate
stop_pct = 0.02  # 2 % stop; must sit inside liquidation distance
```

### Block Comment

One `#` per line, one space after, aligned to the surrounding code. Use for multi-line explanations before a logical unit.

```python
# Re-evaluation path: load the prior thesis so the LLM can reconcile
# its previous directional view with today's price and sentiment data.
# Cold-start (no prior thesis) is allowed on the first session only.
prior_thesis = self.db.load_latest_thesis(symbol)
```

### Section Separator

Use inside long modules or classes to separate pipeline stages. Use the Unicode box-drawing character `─` (U+2500), never hyphens. The line must be exactly 79 characters: `# ` followed by 77 `─` characters.

```python
# ─────────────────────────────────────────────────────────────────────────────
# Application lifespan -- ordered startup and shutdown
# ─────────────────────────────────────────────────────────────────────────────
```

### TODO / FIXME / NOTE

Always include a reference tag so items are traceable.

```python
# TODO(phase3): replace placeholder thresholds with backtested values
# FIXME(DEV-3): orphaned-order handling not yet implemented
# NOTE: drawdown kill switch fires here; downstream must treat this as terminal
```

---

## Component-Specific Comment Patterns

Apply the matching pattern based on which component the code belongs to.

### thesis_engine

- Document the prompt construction logic: what data is injected and why.
- Comment the output validation schema and any sanity-check bounds.
- Mark the re-evaluation branch clearly (prior thesis present vs. cold-start).

```python
# Inject prior thesis so the model reconciles its previous view rather
# than producing a stateless, potentially contradictory decision.
prompt = build_thesis_prompt(
    price_data=ohlcv,
    sentiment=sentiment_scores,
    prior_thesis=prior_thesis,  # None triggers cold-start branch
)
```

### risk_manager

- Every limit must carry the category it enforces (from the V2 spec).
- Rejection reasons must be explicit strings, not codes.
- Liquidation distance check must comment the formula used.

```python
# Category: stop-inside-liquidation check.
# Liquidation price for a long at leverage L:
#   liq_price = entry * (1 - 1/L + maintenance_margin_rate)
# Stop must be strictly above liq_price.
if proposed_stop <= liquidation_price:
    raise RiskRejectionError(
        "stop-loss sits at or below liquidation price"
    )
```

### order_router / order manager

- Every order dict must document which Binance endpoint it targets.
- Bracket structure (entry + stop-loss + take-profit) must be explicit.
- Idempotency key comment required on retry logic.

```python
# Bracketed order: entry market + stop-market + take-profit limit.
# All three legs submitted atomically; orphan handler runs on partial fill.
order_plan = {
    "entry": build_market_order(symbol, side, qty),
    "stop": build_stop_market(symbol, side, stop_price),
    "tp": build_limit_order(symbol, opposite_side, tp_price, qty),
}
```

### data_ingestion

- Comment the source (Binance REST vs. websocket) and the timeframe.
- Note any fallback behavior when the primary feed is unavailable.

```python
# Fetch 1-hour OHLCV from Binance Futures REST (python-binance).
# Fallback: websocket kline stream if REST call fails twice in a row.
klines = self.client.futures_klines(
    symbol=symbol,
    interval=Client.KLINE_INTERVAL_1HOUR,
    limit=100,  # enough history for technical indicators
)
```

### feature_pipeline / strategy module (technical signal)

- Comment which indicator is computed and the parameter rationale.
- Mark the output contract: direction must be one of {'long', 'short', 'flat'}.

```python
# EMA crossover signal: fast=9, slow=21 (standard intraday configuration).
# Output contract: direction in {'long', 'short', 'flat'}.
# 'flat' is emitted when fast and slow EMAs are within the noise band.
```

### portfolio_store / database models (SQLAlchemy)

- Each column comment states its role and any constraint rationale.
- The daily_ledger UNIQUE constraint must carry a comment explaining why it is a DB-level rule, not a runtime check.

```python
class DailyLedger(Base):
    """
    Enforces one trade per crypto per day at the database level.

    The UNIQUE constraint on (symbol, date) is the hard guarantee.
    Runtime checks alone are insufficient because a race or crash
    could allow a duplicate insert.
    """
    __tablename__ = "daily_ledger"

    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False)   # Binance USDT-M symbol
    date = Column(Date, nullable=False)       # UTC session date
    trade_id = Column(Integer, ForeignKey("trades.id"), nullable=False)

    __table_args__ = (
        # DB-level guard: one row per (symbol, date) pair.
        # This is not redundant with runtime checks -- it is the
        # authoritative enforcement mechanism.
        UniqueConstraint("symbol", "date", name="uq_ledger_symbol_date"),
    )
```

### dashboard (FastAPI routes)

- Comment the HTTP method, route purpose, and which backend component it delegates to.
- Mark any state mutations that affect the session state machine.

```python
@router.post("/session/start")
async def start_session(background_tasks: BackgroundTasks) -> dict:
    """
    Trigger the daily session pipeline (14:30 UTC nominal start).

    Mutates session state: idle -> active. Spawns the per-crypto
    analysis loop as a background task so the HTTP response returns
    immediately.
    """
```

### config_secrets

- Never comment the value of a secret, only its purpose and source.
- Always note that the variable must come from the environment.

```python
# Binance testnet API key. Source: environment variable BINANCE_TESTNET_KEY.
# Permissions required: futures read + futures trade. No withdrawal.
BINANCE_API_KEY: str = os.environ["BINANCE_TESTNET_KEY"]
```

---

## What NOT to Comment

These are noise. Never generate them.

```python
# BAD: restates the code
i += 1  # increment i by 1

# BAD: obvious type annotation restatement
symbol: str = "BTCUSDT"  # symbol is a string

# BAD: commented-out dead code left in
# result = old_function(x)

# BAD: vague placeholder
# TODO: fix this later
```

---

## Quick Checklist (verify before finalizing any code block)

- [ ] Module docstring present and states pipeline stage + invariants.
- [ ] Every class has a docstring with responsibility and stage.
- [ ] Every public method has a NumPy-style docstring.
- [ ] Inline comments: 2 spaces before `#`, 1 space after, max 79 chars.
- [ ] Block comments explain WHY, not WHAT.
- [ ] Section separators: `─` (U+2500), exactly 79 chars total (`# ` + 77 `─`).
- [ ] Risk limits and confluence rules carry category labels.
- [ ] UNIQUE DB constraint carries the "why DB-level, not runtime" note.
- [ ] No secrets values in comments.
- [ ] No commented-out dead code.
- [ ] TODO/FIXME/NOTE items carry a phase or ticket reference.