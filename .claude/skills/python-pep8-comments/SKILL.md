---
name: python-pep8-comments
description: Apply PEP 8-compliant Python commenting conventions to all generated code, tailored for algorithmic trading systems. Use this skill EVERY TIME code is generated, written, or modified in this project. Trigger on any Python code output — new modules, functions, classes, configuration blocks, database models, API routes, trading pipeline stages, risk engine logic, signal generators, order managers, and test files. Also trigger when the user says "add comments", "document this", "annotate", "write code for X", "implement X", or any variation requesting Python code. This skill is ALWAYS active for Python code in this project — never skip it even for short snippets or utility functions.
---

# Python PEP 8 Comments Skill

This skill enforces consistent, PEP 8-compliant Python commenting for all code in the `risk_gated_llm_trader` project. Every piece of Python code generated must follow these rules exactly.

## Core Formatting Rules (apply to ALL code)

1. Line length for comments: 79 characters maximum (matching PEP 8 code limit).
2. Inline comments: two spaces before the `#`, one space after.
3. Block comments: `#` followed by one space, aligned to the code block's indentation level.
4. No redundant comments. Never restate what the code obviously does.
5. Comment the WHY and the WHAT (architectural intent, trading logic, risk invariants), not the HOW (Python syntax).
6. Section separators use `# ---` with a label, 79 chars total.
7. No trailing whitespace on comment lines.
8. All docstrings use triple double-quotes `"""`.

---

## Comment Type Reference

### Module-Level Docstring

Every module starts with a docstring immediately after any `from __future__` imports. It states the module's role in the system pipeline, its key invariants, and which component it belongs to.

```python
"""
thesis_engine.py -- LLM Thesis Engine (Component: thesis_engine)

Generates a structured directional swing thesis (long/short/flat) for a
single crypto asset. Inputs: multi-timeframe OHLCV (1D regime, 4H trend,
1H signal), raw news headlines, and the symbol's active or prior thesis.
Output: a validated ThesisOutput schema object.

Invariants:
    - The LLM never sizes or places orders. Direction only.
    - Malformed or out-of-band outputs are rejected before returning.
    - Re-evaluation always loads the prior thesis; cold-start is allowed
      only if no prior thesis exists for this symbol.
    - One active thesis per symbol, enforced at the database level.
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
    Thread safety: one instance per backend process; the fast loop and
    the event path may call it concurrently.

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
    is logged and the symbol is skipped for this pass; the next
    completed candle re-evaluates it.

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
# Re-evaluation path: load the ACTIVE thesis so the LLM reconciles the
# view currently justifying an open position, not merely the last one
# that finished. Cold-start (no prior thesis) is the first pass only.
prior_thesis = self.db.load_latest_thesis(symbol)
```

### Section Separator

Use inside long modules or classes to separate pipeline stages. Total line length 79 chars.

```python
# ---------------------------------------------------------------------------
# Phase 2 -- News ingestion and multi-timeframe thesis generation
# ---------------------------------------------------------------------------
```

### TODO / FIXME / NOTE

Always include a reference tag so items are traceable.

```python
# TODO(phase3): replace placeholder thresholds with backtested values
# TODO(phase5): re-derive holding horizons from the backtest
# FIXME: orphaned-order handling not yet implemented
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
payload = build_skill_payload(
    ohlcv_1d=regime_frame,      # what kind of market this is
    ohlcv_4h=trend_frame,       # the structure the position rides
    ohlcv_1h=signal_frame,      # where entry and stop belong
    headlines=headlines,        # raw text; the skill reads it directly
    prior_thesis=prior_thesis,  # None triggers cold-start branch
)
```

### risk_manager

- Every limit must carry the category it enforces (from the V3 spec).
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
# EMA crossover on the 4H trend frame: fast=9, slow=21. Direction comes
# from the higher timeframes; the 1H frame only times the entry, because
# an hourly reversal against a 4H trend is noise on a multi-day hold.
# Output contract: direction in {'long', 'short', 'flat'}.
# 'flat' is emitted when fast and slow EMAs are within the noise band.
```

### portfolio_store / database models (SQLAlchemy)

- Each column comment states its role and any constraint rationale.
- A constraint that enforces a system invariant must carry a comment explaining why it is a DB-level rule, not a runtime check.

```python
class Thesis(Base):
    """
    Per-symbol thesis, governing that symbol until it closes.

    A thesis opens, lives for its horizon -- which may span several UTC
    dates and several service restarts -- and closes for a stated
    reason. The partial unique index below is the hard guarantee that
    only one governs a symbol at a time. Runtime checks alone are
    insufficient because the fast loop and an event-driven
    re-evaluation could race and open two competing theses.
    """
    __tablename__ = "theses"

    id = Column(Integer, primary_key=True)
    thesis_uid = Column(String(36), nullable=False, unique=True)
    symbol = Column(String, nullable=False)   # Binance USDT-M symbol
    status = Column(String, nullable=False)   # active|closed|superseded
    expires_at = Column(DateTime)             # the time stop

    __table_args__ = (
        # DB-level guard: at most one active row per symbol. Partial
        # rather than a plain UNIQUE because closed and superseded rows
        # must remain -- they are the thesis history re-evaluation
        # reads.
        Index(
            "uq_active_thesis_per_symbol",
            "symbol",
            unique=True,
            sqlite_where=text("status = 'active'"),
        ),
    )
```

### dashboard (FastAPI routes)

- Comment the HTTP method, route purpose, and which backend component it delegates to.
- Mark any state mutations that affect the service state machine.
- Where a control is easy to confuse with a more destructive one, say what it does NOT do.

```python
@router.post("/strategy/stop")
def stop_strategy(request: Request) -> ServiceStatus:
    """
    Stop strategy execution, leaving positions open and protected.

    Mutates service state: running_* -> stopping -> stopped, and
    disables new entries. Does NOT close positions: stop-loss and
    take-profit legs are deliberately left resting at the exchange so
    open positions stay defended after this process exits. Closing
    them is /emergency-flat's job.
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
- [ ] Risk limits and confluence rules carry category labels.
- [ ] UNIQUE DB constraint carries the "why DB-level, not runtime" note.
- [ ] No secrets values in comments.
- [ ] No commented-out dead code.
- [ ] TODO/FIXME/NOTE items carry a phase or ticket reference.