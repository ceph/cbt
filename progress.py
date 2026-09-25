"""
progress.py — CLI progress bars for CBT runs.

All output is written to *stderr* via tqdm.  While bars are active, logging is
redirected through ``tqdm.write()`` so log lines appear cleanly above the bars
with no blank-line or mid-line glitches.

Bars are automatically suppressed when:
  - stdout is not a TTY  (non-interactive / piped)
  - settings.cluster contains ``is_teuthology: true``
  - ``--no-progress`` was passed on the command line

Call ``setup()`` once after ``settings.initialize()`` and after arg parsing
so the disabled flag is resolved correctly.

Design notes
------------
* No background ticker thread — tqdm's own ``{elapsed}`` token in the format
  string provides live elapsed time without any threading.
* ``dynamic_ncols=True`` on every bar so they reflow correctly on terminal resize.
* ``_logging_redirect`` is active for the entire run (entered in
  ``overall_bar``) — it replaces the console StreamHandler with a level-
  preserving tqdm-aware variant so log lines and bar redraws are serialised.
  Unlike ``tqdm.contrib.logging.logging_redirect_tqdm``, the replacement
  handler copies the original handler's *level* so DEBUG messages never leak
  to the screen.
* The overall bar is advanced explicitly when each phase exits, so the filled
  portion reflects work actually completed.
"""

import contextlib
import logging
import sys
import threading
from collections.abc import Generator
from typing import Any, Optional

import tqdm

import settings

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

# Sensible estimate (seconds) for the cluster initialisation phase when no
# explicit duration is available.  Users can override via
# ``cluster.init_estimate_secs`` in their YAML config.
DEFAULT_CLUSTER_INIT_SECS: int = 180

# How often the ticker thread advances the phase bar (seconds).
# Coarse enough to avoid busy-looping; fine enough to feel responsive.
TICK_INTERVAL: float = 15.0

# tqdm bar_format strings — defined once here so they are not rebuilt on every
# context-manager entry and so all bars share a consistent layout.
_BAR_FORMAT_TIMED = "{l_bar}{bar}| {n_fmt}/{total_fmt}s  [elapsed {elapsed} | eta {remaining}]"
_BAR_FORMAT_SPIN = "{desc}: {elapsed} elapsed"

# ──────────────────────────────────────────────────────────────────────────────
# Module-level state (set once by setup())
# ──────────────────────────────────────────────────────────────────────────────

_disabled: bool = True  # safe default — overridden by setup()
_overall: Optional["OverallBar"] = None  # set by overall_bar() context manager


def setup(no_progress: bool = False) -> None:
    """Resolve and store the global disabled flag.

    Call once after ``settings.initialize()`` and after CLI arg parsing.

    Args:
        no_progress: True when ``--no-progress`` was supplied on the CLI.
    """
    global _disabled  # pylint: disable=global-statement
    is_tty = sys.stdout.isatty()
    is_teuthology = bool(settings.cluster.get("is_teuthology", False))
    _disabled = not is_tty or is_teuthology or no_progress


# ──────────────────────────────────────────────────────────────────────────────
# Public helpers
# ──────────────────────────────────────────────────────────────────────────────


def get_overall_bar() -> Optional["OverallBar"]:
    """Return the active :class:`OverallBar`, or ``None`` when not running.

    Allows code deep in the call stack (e.g. :class:`~workloads.workloads.Workloads`)
    to advance the overall bar without needing the handle passed through every
    intermediate function signature.
    """
    return _overall


def cluster_init_estimate() -> int:
    """Return the estimated cluster-initialisation duration in seconds.

    Reads ``cluster.init_estimate_secs`` from the loaded settings, falling
    back to :data:`DEFAULT_CLUSTER_INIT_SECS`.
    """
    return int(settings.cluster.get("init_estimate_secs", DEFAULT_CLUSTER_INIT_SECS))


# ──────────────────────────────────────────────────────────────────────────────
# Internal: level-preserving logging redirect
# ──────────────────────────────────────────────────────────────────────────────


class _TqdmHandler(logging.StreamHandler):  # type: ignore[type-arg]
    """StreamHandler that writes via ``tqdm.write()`` instead of directly.

    This ensures that log output and tqdm bar redraws are serialised, preventing
    the blank-line / mid-line glitch seen when both write to the same terminal.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg, file=self.stream)
            self.flush()
        except Exception:  # pylint: disable=broad-except
            self.handleError(record)


@contextlib.contextmanager
def _logging_redirect(logger: logging.Logger) -> Generator[None, None, None]:
    """Temporarily replace *logger*'s console handler with a tqdm-aware one.

    Unlike ``tqdm.contrib.logging.logging_redirect_tqdm``, this helper copies
    the original handler's **level** and **formatter** so that the level
    filtering configured in :func:`logging_configuration.setup_loggers` is
    fully preserved (INFO+ to screen, DEBUG only to the file).
    """
    # Find the existing console StreamHandler (not a FileHandler).
    original: Optional[logging.StreamHandler] = next(  # type: ignore[type-arg]
        (h for h in logger.handlers if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)),
        None,
    )

    if original is None or _disabled:
        # Nothing to redirect (bars are off, or no console handler found).
        yield
        return

    replacement = _TqdmHandler(stream=original.stream)
    replacement.setLevel(original.level)  # ← preserve INFO-only filtering
    replacement.setFormatter(original.formatter)

    logger.removeHandler(original)
    logger.addHandler(replacement)
    try:
        yield
    finally:
        logger.removeHandler(replacement)
        logger.addHandler(original)


# ──────────────────────────────────────────────────────────────────────────────
# Progress bar context managers
# ──────────────────────────────────────────────────────────────────────────────


class OverallBar:  # pylint: disable=too-few-public-methods
    """Wrapper around the outer tqdm bar that exposes ``advance(seconds)``."""

    def __init__(self, pbar: "tqdm.tqdm[Any]") -> None:
        self._pbar = pbar
        self._lock = threading.Lock()

    def advance(self, seconds: float) -> None:
        """Advance the outer bar by *seconds* work-units."""
        with self._lock:
            self._pbar.update(seconds)


@contextlib.contextmanager
def overall_bar(total_seconds: int) -> Generator[OverallBar, None, None]:
    """Context manager for the outer overall-run progress bar.

    Activates the level-preserving logging redirect for its duration so that
    all log messages are routed through ``tqdm.write()``.  This ensures log
    lines appear cleanly above both bars with no visual glitches, while the
    INFO-only screen filter configured at startup is fully preserved.

    The bar tracks elapsed work in seconds.  Callers advance it explicitly
    via :meth:`OverallBar.advance` after each phase completes.

    Args:
        total_seconds: Estimated total wall-clock seconds for the whole run.

    Yields:
        An :class:`OverallBar` handle that callers use to advance the bar.
    """
    pbar = tqdm.tqdm(
        total=total_seconds,
        desc="Overall run",
        unit="s",
        bar_format=_BAR_FORMAT_TIMED,
        position=0,
        leave=True,
        file=sys.stderr,
        dynamic_ncols=True,
        disable=_disabled,
    )
    handle = OverallBar(pbar)
    global _overall  # pylint: disable=global-statement
    cbt_logger = logging.getLogger("cbt")
    with _logging_redirect(cbt_logger):
        _overall = handle
        try:
            yield handle
        finally:
            _overall = None
            # Snap to 100% so the bar always closes complete, regardless of any
            # estimation error or time spent outside phase bars (e.g. report generation).
            if not _disabled and pbar.total is not None and pbar.n < pbar.total:
                pbar.update(pbar.total - pbar.n)
            pbar.close()


@contextlib.contextmanager
def phase_bar(
    description: str,
    duration_seconds: Optional[int] = None,
    overall: Optional[OverallBar] = None,
) -> Generator[None, None, None]:
    """Context manager for a single CBT phase (init, prefill, run, …).

    When *duration_seconds* is given a background ticker thread advances the
    bar by :data:`TICK_INTERVAL` seconds every :data:`TICK_INTERVAL` wall-clock
    seconds, so the filled portion moves in real time while CBT is blocked on
    remote commands.  The ticker only calls ``pbar.update()`` — it never
    touches ``tqdm.write()`` or the logging system, so there is no deadlock
    risk with the ``_TqdmHandler`` logging redirect.

    When ``duration_seconds`` is ``None`` the bar shows elapsed time only
    (no fill, no ticker needed).

    On exit the outer *overall* bar is advanced by the estimated duration so
    the overall progress reflects the completed work.

    Args:
        description:      Human-readable label shown on the bar.
        duration_seconds: Expected duration of this phase in seconds.
                          ``None`` for indeterminate phases.
        overall:          The :class:`OverallBar` handle returned by
                          :func:`overall_bar`.  When provided it is advanced
                          by *duration_seconds* on exit.
    """
    pbar = tqdm.tqdm(
        total=duration_seconds,
        desc=description,
        unit="s",
        bar_format=_BAR_FORMAT_TIMED if duration_seconds is not None else _BAR_FORMAT_SPIN,
        position=1,
        leave=False,
        file=sys.stderr,
        dynamic_ncols=True,
        disable=_disabled,
        mininterval=1.0,
    )

    stop_event = threading.Event()
    ticker: Optional[threading.Thread] = None
    if duration_seconds is not None and not _disabled:
        ticker = threading.Thread(
            target=_tick,
            args=(pbar, stop_event, overall),
            daemon=True,
            name=f"cbt-ticker-{description}",
        )
        ticker.start()

    try:
        yield
    finally:
        stop_event.set()
        if ticker is not None:
            ticker.join(timeout=TICK_INTERVAL + 1.0)
        # Capture pbar.n before close() so we read a stable value.
        already_ticked = pbar.n  # how much the ticker already credited
        pbar.close()
        # Advance overall by any remaining seconds not yet ticked (e.g. phase
        # finished early, or bars were disabled so no ticker ran).
        if overall is not None and duration_seconds is not None:
            remaining_credit = duration_seconds - already_ticked
            if remaining_credit > 0:
                overall.advance(remaining_credit)


def _tick(
    pbar: "tqdm.tqdm[Any]",
    stop_event: threading.Event,
    overall: Optional[OverallBar] = None,
) -> None:
    """Advance *pbar* and *overall* by :data:`TICK_INTERVAL` every :data:`TICK_INTERVAL` seconds.

    Clamps phase-bar updates so the bar never exceeds its total.  Only calls
    ``pbar.update()`` and ``overall.advance()`` — never ``tqdm.write()`` or
    any logging function — so it is safe to run concurrently with
    ``_TqdmHandler``.
    """
    while not stop_event.wait(timeout=TICK_INTERVAL):
        remaining = (pbar.total or 0) - pbar.n
        if remaining > 0:
            increment = min(TICK_INTERVAL, remaining)
            pbar.update(increment)
            if overall is not None:
                overall.advance(increment)
