# core/pipeline/runner.py
from __future__ import annotations

import os
import time
from typing import Callable, Dict, Any, List, Optional

import yaml
import pandas as pd

from core.pipeline import registry
from core.io.reader import read_csv_in_chunks
from core.io.writer import CsvIncrementalWriter
from core.logging.sink import LogSink
from core.logging.formatter import (
    format_header,
    format_column_section,
    format_row_filters_section,   # NEW
    format_dedup_section,
    format_footer,
)
from core.dedup.engine import DedupMergeEngine
from core.row_filters.engine import OneFilledRowFilter  # NEW

TITLES = {
    "email": "ПОЧТА",
    "phone": "ТЕЛЕФОН",
    "birthdate": "ДАТА РОЖДЕНИЯ",
    "ip_address": "IP-АДРЕС",
    "lastname": "ФАМИЛИЯ",
    "firstname": "ИМЯ",
    "middlename": "ОТЧЕСТВО",
    "fullname": "ФИО",
}

def run_pipeline(
    input_csv: str,
    output_csv: str,
    config_yaml: str,
    log_txt: str,
    *,
    chunksize: int = 100_000,
    delimiter_override: Optional[str] = None,
    encoding_override: Optional[str] = None,
    dedup_enabled: bool = False,
    dedup_subset: Optional[List[str]] = None,
    ignore_empty_in_subset: bool = True,
    dedup_merge_columns: Optional[List[str]] = None,
    progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
    rows_total_estimate: Optional[int] = None,

    # NEW — очистка коротких значений
    min_length_enabled: bool = False,
    min_length_value: int = 3,
    min_length_columns: Optional[List[str]] = None,

    # NEW — фильтр строк "только 1 заполненная"
    row_filter_one_filled_enabled: bool = False,
    row_filter_subset: Optional[List[str]] = None,
) -> None:
    with open(config_yaml, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    cfg_global = cfg.get("global", {}) if isinstance(cfg, dict) else {}

    delimiter: str = delimiter_override or cfg_global.get("delimiter", ",")
    encoding: str = encoding_override or cfg_global.get("encoding", "auto")
    columns_cfg: Dict[str, Any] = cfg.get("columns", {})

    os.makedirs(os.path.dirname(log_txt) or ".", exist_ok=True)
    log = LogSink(log_txt)

    key_col = (dedup_subset or [None])[0]
    merge_cols = dedup_merge_columns or []

    summary: Dict[str, Any] = {
        "rows_total": 0,
        "columns": list(columns_cfg.keys()),
        "delimiter": delimiter,
        "encoding": encoding,
        "dedup": {
            "enabled": bool(dedup_enabled and key_col),
            "subset": [key_col] if key_col else [],
            "removed": 0,
            "merge_columns": merge_cols,
        },
        "row_filters": {
            "one_filled": {
                "enabled": bool(row_filter_one_filled_enabled and (row_filter_subset or [])),
                "subset": row_filter_subset or [],
                "removed": 0,
            }
        },
        "cols_stats": {},   # col -> {changed, cleared, examples}
        "duration_sec": 0.0,
        "rows_per_sec": None,
    }

    writer = CsvIncrementalWriter(output_csv)
    deduper: Optional[DedupMergeEngine] = None

    start_ts = time.perf_counter()
    rows_done = 0
    examples_limit = 25
    first_chunk_written = False

    # подготовим фильтр строк (если нужен)
    row_filter = OneFilledRowFilter(subset=row_filter_subset or []) if summary["row_filters"]["one_filled"]["enabled"] else None

    # подготовим функцию "короткие значения"
    ml_cols = set(min_length_columns or [])
    min_len = int(min_length_value)
    apply_min_length = bool(min_length_enabled and ml_cols)

    if progress_cb:
        progress_cb({"phase": "start", "rows_done": 0, "rows_est": rows_total_estimate, "elapsed_sec": 0.0, "rps": 0.0, "percent": 0})

    for chunk in read_csv_in_chunks(input_csv, delimiter=delimiter, encoding=encoding, chunksize=chunksize):
        chunk_len = len(chunk)
        summary["rows_total"] += chunk_len
        rows_done += chunk_len

        # 1) Правила из профиля по колонкам
        for col_name, spec in columns_cfg.items():
            if col_name not in chunk.columns:
                continue

            rules: List[Dict[str, Any]] = spec.get("rules", [])
            series = chunk[col_name]
            local_changed = 0
            local_cleared = 0
            local_examples: List[Dict[str, Any]] = []

            for step in rules:
                if not isinstance(step, dict) or len(step) != 1:
                    raise ValueError(f"Bad rule format for column '{col_name}': {step}")
                name, params = list(step.items())[0]
                params = params or {}

                fn = registry.get(name)
                if registry.is_advanced(name):
                    new_series, stats = fn(series, **params)
                    series = new_series
                    local_changed += int(stats.get("changed", 0))
                    local_cleared += int(stats.get("cleared", stats.get("cleared_invalid", 0)))
                    for ex in stats.get("examples", []):
                        if len(local_examples) < examples_limit:
                            local_examples.append(ex)
                else:
                    series = fn(series, **params)

            chunk[col_name] = series

            if col_name not in summary["cols_stats"]:
                summary["cols_stats"][col_name] = {"changed": 0, "cleared": 0, "examples": []}
            summary["cols_stats"][col_name]["changed"] += local_changed
            summary["cols_stats"][col_name]["cleared"] += local_cleared
            space_left = examples_limit - len(summary["cols_stats"][col_name]["examples"])
            if space_left > 0 and local_examples:
                summary["cols_stats"][col_name]["examples"].extend(local_examples[:space_left])

        # 2) Очистка коротких значений (доп. правило из UI)
        if apply_min_length:
            fn = registry.get("min_length_clear")
            for col in [c for c in chunk.columns if c in ml_cols]:
                new_series, stats = fn(chunk[col], min_len=min_len)
                chunk[col] = new_series

                if col not in summary["cols_stats"]:
                    summary["cols_stats"][col] = {"changed": 0, "cleared": 0, "examples": []}
                summary["cols_stats"][col]["changed"] += int(stats.get("changed", 0))
                summary["cols_stats"][col]["cleared"] += int(stats.get("cleared", 0))
                space_left = examples_limit - len(summary["cols_stats"][col]["examples"])
                if space_left > 0:
                    summary["cols_stats"][col]["examples"].extend(stats.get("examples", [])[:space_left])

        # 3) Фильтр строк "только 1 заполненная ячейка"
        if row_filter is not None:
            before = len(chunk)
            chunk, rstats = row_filter.apply(chunk)
            removed_now = before - len(chunk)
            if removed_now:
                summary["row_filters"]["one_filled"]["removed"] += removed_now

        # 4) Дедуп/мердж или запись
        if summary["dedup"]["enabled"]:
            if deduper is None:
                deduper = DedupMergeEngine(
                    key_column=key_col,
                    all_columns=list(chunk.columns),
                    merge_columns=merge_cols,
                    ignore_empty=True,
                )
            deduper.process_chunk(chunk)
        else:
            writer.write_chunk(chunk, header=not first_chunk_written)
            first_chunk_written = True

        # прогресс
        if progress_cb:
            elapsed = time.perf_counter() - start_ts
            rps = (rows_done / elapsed) if elapsed > 0 else 0.0
            percent = None
            if rows_total_estimate and rows_total_estimate > 0:
                percent = max(0, min(100, int(rows_done * 100 / rows_total_estimate)))
            progress_cb({"phase": "processing", "rows_done": rows_done, "rows_est": rows_total_estimate, "elapsed_sec": elapsed, "rps": rps, "percent": percent})

    # выгрузка после дедупа
    if summary["dedup"]["enabled"] and deduper is not None:
        for out_df in deduper.export_batches(batch_size=100_000):
            writer.write_chunk(out_df, header=not first_chunk_written)
            first_chunk_written = True
        summary["dedup"]["removed"] = deduper.removed
        deduper.close()

    summary["duration_sec"] = time.perf_counter() - start_ts
    summary["rows_per_sec"] = (summary["rows_total"] / summary["duration_sec"]) if summary["duration_sec"] > 0 else None

    if progress_cb:
        progress_cb({"phase": "done", "rows_done": rows_done, "rows_est": rows_total_estimate, "elapsed_sec": summary["duration_sec"], "rps": summary["rows_per_sec"] or 0.0, "percent": 100})

    # лог
    log.write(format_header(
        input_csv=input_csv,
        output_csv=output_csv,
        rows_total=summary["rows_total"],
        columns=summary["columns"],
        delimiter=summary["delimiter"],
        encoding=summary["encoding"],
        dedup_enabled=summary["dedup"]["enabled"],
        dedup_subset=summary["dedup"]["subset"],
        duration_sec=summary["duration_sec"],
        rows_per_sec=summary["rows_per_sec"],
    ))

    # секции по колонкам
    for col, stats in summary["cols_stats"].items():
        title = TITLES.get(col, col.upper())
        log.write(format_column_section(
            title=title,
            column=col,
            changed=int(stats["changed"]),
            cleared=int(stats["cleared"]),
            examples=stats["examples"],
        ))

    # фильтр строк
    rf = summary["row_filters"]["one_filled"]
    log.write(format_row_filters_section(
        one_filled_enabled=rf["enabled"],
        subset=rf["subset"],
        removed=rf["removed"],
    ))

    # дедуп
    log.write(format_dedup_section(
        enabled=summary["dedup"]["enabled"],
        subset=summary["dedup"]["subset"],
        removed=summary["dedup"]["removed"],
        merge_columns=summary["dedup"]["merge_columns"],
    ))
    log.write(format_footer())
    log.close()
