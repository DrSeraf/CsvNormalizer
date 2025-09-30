# core/dedup/engine.py
from __future__ import annotations

from typing import List, Set

import pandas as pd


class DedupEngine:
    """
    Потоковая дедупликация по выбранному подмножеству столбцов.
    Хранит множество уже встреченных ключей между чанками.
    """

    def __init__(self, subset: List[str], ignore_empty_in_subset: bool = True):
        self.subset = subset or []
        self.ignore_empty_in_subset = ignore_empty_in_subset
        self._seen: Set[str] = set()
        self.removed = 0

    def _make_keys(self, df: pd.DataFrame) -> pd.Series:
        # Превращаем ключевые столбцы в строку с разделителем, NaN -> ""
        keys = df[self.subset].astype(str).agg("||".join, axis=1)
        return keys

    def filter_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.subset or not all(col in df.columns for col in self.subset):
            return df

        keys = self._make_keys(df)

        # Пустые ключи — пропускаем и не учитываем в seen, если ignore_empty_in_subset=True
        empty_mask = pd.Series(False, index=df.index)
        if self.ignore_empty_in_subset:
            # пустой ключ — это когда все столбцы пустые строки
            empty_mask = df[self.subset].replace("", pd.NA).isna().all(axis=1)

        # Уже встречавшиеся ключи
        already_seen = keys.isin(self._seen)

        # К оставлению: те, у кого ключ пустой (если игнорим пустые), либо новые ключи
        keep_mask = empty_mask | (~already_seen)

        # Обновим счётчик удалённых
        removed_now = int((~keep_mask).sum())
        if removed_now:
            self.removed += removed_now

        # Добавим в seen только непустые и новые ключи
        to_add = keys[~empty_mask & ~already_seen]
        if not to_add.empty:
            self._seen.update(to_add.tolist())

        return df[keep_mask]
