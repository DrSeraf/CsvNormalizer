# core/dedup/engine.py
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# Простой дедуп (как раньше): удаляет дубликаты строк по ключу, сохраняет первое
# ──────────────────────────────────────────────────────────────────────────────
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
            empty_mask = df[self.subset].replace("", pd.NA).isna().all(axis=1)

        already_seen = keys.isin(self._seen)
        keep_mask = empty_mask | (~already_seen)

        removed_now = int((~keep_mask).sum())
        if removed_now:
            self.removed += removed_now

        to_add = keys[~empty_mask & ~already_seen]
        if not to_add.empty:
            self._seen.update(to_add.tolist())

        return df[keep_mask]


# ──────────────────────────────────────────────────────────────────────────────
# Дедуп с объединением значений выбранных колонок через ';' по ключу
# ──────────────────────────────────────────────────────────────────────────────
def _merge_semicolon(existing: Optional[str], new: Optional[str]) -> str:
    """Склеивает значения через ';' без дублей, сохраняя порядок.
    Пустые и одинаковые не добавляет."""
    ex = (existing or "").strip()
    nv = (new or "").strip()
    if nv == "" and ex != "":
        return ex
    if ex == "":
        return nv
    parts = ex.split(";")
    if nv and nv not in parts:
        parts.append(nv)
    parts = [p for p in parts if p != ""]
    return ";".join(parts)


class DedupMergeEngine:
    """Дедупликация с объединением выбранных колонок через ';' по ключу.

    - Сохраняем первую строку; дубликаты обновляют ТОЛЬКО выбранные merge-колонки.
    - Остальные колонки остаются как у первой строки (оригинала).
    - Пустые ключи (если ignore_empty=True) не объединяются: каждая пустая строка уникальна.
    - Для стабильности и больших объёмов агрегация хранится во временной SQLite-базе.
    """

    def __init__(
        self,
        *,
        key_column: str,
        all_columns: List[str],
        merge_columns: List[str],
        ignore_empty: bool = True,
        db_path: Optional[str] = None,
    ):
        if not key_column:
            raise ValueError("key_column is required")

        self.key_column = key_column
        self.all_columns = list(all_columns)
        # исключаем ключ из объединяемых
        self.merge_columns = [c for c in merge_columns if c in self.all_columns and c != key_column]
        self.ignore_empty = ignore_empty

        self._order_counter = 0
        self.removed = 0

        # SQLite
        self.db_path = db_path or os.path.join(Path.cwd(), ".dedup_merge_tmp.db")
        try:
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
        except Exception:
            pass

        self.conn = sqlite3.connect(self.db_path)
        # немного ускорим работу
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=OFF;")

        # создаём таблицу агрегатов
        cols_sql = ", ".join([f'"{c}" TEXT' for c in self.all_columns])
        self.conn.execute(f'CREATE TABLE agg (__key__ TEXT PRIMARY KEY, __order__ INTEGER, {cols_sql})')
        self.conn.commit()

    def close(self):
        try:
            self.conn.close()
        finally:
            # по желанию можно удалить файл
            # try: os.remove(self.db_path) except: pass
            pass

    # ── внутренние утилиты для чтения/записи одной строки в SQLite ───────────
    def _insert_row(self, key: str, row: Dict[str, str]):
        self._order_counter += 1
        placeholders_cols = ["__key__", "__order__"] + self.all_columns
        col_list = ",".join([f'"{c}"' for c in placeholders_cols])
        placeholders_q = ",".join(["?"] * len(placeholders_cols))
        values = [key, self._order_counter] + [
            ("" if row.get(c, "") is None else str(row.get(c, ""))) for c in self.all_columns
        ]
        self.conn.execute(f"INSERT INTO agg ({col_list}) VALUES ({placeholders_q})", values)

    def _fetch_row(self, key: str) -> Optional[Dict[str, str]]:
        col_list = ",".join([f'"{c}"' for c in self.all_columns])
        cur = self.conn.execute(f"SELECT {col_list} FROM agg WHERE __key__=?", (key,))
        row = cur.fetchone()
        if row is None:
            return None
        return {col: (row[i] if row[i] is not None else "") for i, col in enumerate(self.all_columns)}

    def _update_row(self, key: str, updated: Dict[str, str]):
        if not updated:
            return
        set_exprs = [f'"{col}"=?' for col in updated.keys()]
        values: List[str] = list(updated.values()) + [key]
        self.conn.execute(f"UPDATE agg SET {', '.join(set_exprs)} WHERE __key__=?", values)

    # ── основной поток обработки ──────────────────────────────────────────────
    def process_chunk(self, df: pd.DataFrame):
        """Обработать чанк: добавить новые ключи или объединить значения по дубликатам."""
        with self.conn:  # транзакция на чанк
            for _, row in df.iterrows():
                key_val = "" if (self.key_column not in row or pd.isna(row[self.key_column])) else str(row[self.key_column])
                if self.ignore_empty and key_val == "":
                    # каждая пустая — уникальна, подставим синтетический ключ
                    key = f"__EMPTY__#{self._order_counter + 1}"
                else:
                    key = key_val

                existing = self._fetch_row(key)
                if existing is None:
                    # вставка первой строки как оригинал (все колонки)
                    payload = {c: (None if (c not in row or pd.isna(row[c])) else str(row[c])) for c in self.all_columns}
                    self._insert_row(key, payload)
                else:
                    # дубликат — объединяем ТОЛЬКО merge_columns
                    updates: Dict[str, str] = {}
                    for col in self.merge_columns:
                        new_val = "" if (col not in row or pd.isna(row[col])) else str(row[col])
                        merged = _merge_semicolon(existing.get(col, ""), new_val)
                        if merged != existing.get(col, ""):
                            updates[col] = merged
                    if updates:
                        self._update_row(key, updates)

                    # считаем удалённой строкой (дубликат с непустым ключом)
                    if not (self.ignore_empty and key_val == ""):
                        self.removed += 1

    def export_batches(self, batch_size: int = 100_000) -> Iterable[pd.DataFrame]:
        """Итеративно отдаёт агрегированные данные чанками по порядку появления (первое вхождение сначала)."""
        col_list = ",".join([f'"{c}"' for c in self.all_columns])
        cur = self.conn.execute(f"SELECT {col_list} FROM agg ORDER BY __order__ ASC")

        rows: List[List[str]] = []
        for rec in cur:
            rows.append(list(rec))
            if len(rows) >= batch_size:
                yield pd.DataFrame(rows, columns=self.all_columns)
                rows.clear()
        if rows:
            yield pd.DataFrame(rows, columns=self.all_columns)
