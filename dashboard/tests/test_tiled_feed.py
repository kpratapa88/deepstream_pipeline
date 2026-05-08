"""
Property-based test for the tiled live feed grid layout formula.

Feature: streamlit-dashboard
  Property 6: Tiled grid column formula — Validates: Requirements 3.2
"""
import math

from hypothesis import given, settings as hyp_settings
from hypothesis import strategies as st

from dashboard.components.tiled_feed import _compute_grid


# ---------------------------------------------------------------------------
# Property 6: Tiled grid column formula
# Feature: streamlit-dashboard, Property 6: Tiled grid column formula
# Validates: Requirements 3.2
# ---------------------------------------------------------------------------

@given(st.integers(min_value=1, max_value=100))
@hyp_settings(max_examples=100)
def test_tiled_grid_column_formula(n: int):
    """
    For any integer N >= 1 representing the number of active streams,
    the computed column count SHALL equal ceil(sqrt(N)) and the row count
    SHALL equal ceil(N / cols), ensuring all N tiles fit in the grid.
    Validates: Requirements 3.2
    """
    cols, rows = _compute_grid(n)

    expected_cols = math.ceil(math.sqrt(n))
    expected_rows = math.ceil(n / expected_cols)

    assert cols == expected_cols, (
        f"N={n}: expected cols={expected_cols}, got {cols}"
    )
    assert rows == expected_rows, (
        f"N={n}: expected rows={expected_rows}, got {rows}"
    )
    # All N tiles must fit in the grid
    assert cols * rows >= n, (
        f"N={n}: grid {cols}x{rows}={cols * rows} cannot fit {n} tiles"
    )
