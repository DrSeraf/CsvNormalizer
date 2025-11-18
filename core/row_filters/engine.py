# core/row_filters/engine.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import pandas as pd


@dataclass
class OneFilledRuleStats:
    removed: int = 0


class OneFilledRowFilter:
    """
    Удаляет строки, в которых среди выбранных столбцов заполнена не более одной ячейки.
    Непустая ячейка = значение не NaN и не пустая строка "".
    """

    def __init__(self, subset: List[str]):
        self.subset = [c for c in subset or []]
        self.stats = OneFilledRuleStats()

    @staticmethod
    def _non_empty_mask(series: pd.Series) -> pd.Series:
        # считаем непустыми те, что не NaN и не ""
        return series.fillna("").astype(str) != ""

    def apply(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, OneFilledRuleStats]:
        if not self.subset:
            return df, self.stats

        present_cols = [c for c in self.subset if c in df.columns]
        if not present_cols:
            return df, self.stats

        # посчитаем число непустых среди выбранных колонок
        non_empty_counts = None
        for i, col in enumerate(present_cols):
            mask = self._non_empty_mask(df[col])
            non_empty_counts = mask.astype("int32") if i == 0 else (non_empty_counts + mask.astype("int32"))

        # оставляем строки, где непустых столбцов больше 1
        keep_mask = non_empty_counts > 1
        removed_now = int((~keep_mask).sum())
        if removed_now:
            self.stats.removed += removed_now

        return df[keep_mask], self.stats
