# core/pipeline/runner.py
from __future__ import annotations

import os
from typing import Dict, Any, List, Optional

import yaml

from core.pipeline import registry
from core.io.reader import read_csv_in_chunks
from core.io.writer import CsvIncrementalWriter
from core.logging.sink import LogSink
from core.logging.formatter import (
    format_header,
    format_column_section,
    format_dedup_section,
    format_footer,
)
from core.dedup.engine import DedupMergeEngine  # NEW


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
    dedup_subset: Optional[List[str]] = None,        # список, но ожидаем 1 поле
    ignore_empty_in_subset: bool = True,
    dedup_merge_columns: Optional[List[str]] = None, # какие колонки объединять через ';'
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

    summary = {
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
        "cols_stats": {},
    }

    writer = CsvIncrementalWriter(output_csv)

    deduper: Optional[DedupMergeEngine] = None
    examples_limit = 25

    # обработка чанками
    first_chunk_written = False
    for chunk in read_csv_in_chunks(input_csv, delimiter=delimiter, encoding=encoding, chunksize=chunksize):
        summary["rows_total"] += len(chunk)

        # применяем правила профиля к колонкам
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

        # дедуп с объединением
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
            # если дедуп выключен — пишем как раньше
            writer.write_chunk(chunk, header=not first_chunk_written)
            first_chunk_written = True

    # если дедуп включён, выгружаем агрегат в CSV
    if summary["dedup"]["enabled"] and deduper is not None:
        for out_df in deduper.export_batches(batch_size=100_000):
            writer.write_chunk(out_df, header=not first_chunk_written)
            first_chunk_written = True
        summary["dedup"]["removed"] = deduper.removed
        deduper.close()

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
    ))

    for col, stats in summary["cols_stats"].items():
        title = TITLES.get(col, col.upper())
        log.write(format_column_section(
            title=title,
            column=col,
            changed=int(stats["changed"]),
            cleared=int(stats["cleared"]),
            examples=stats["examples"],
        ))

    log.write(format_dedup_section(
        enabled=summary["dedup"]["enabled"],
        subset=summary["dedup"]["subset"],
        removed=summary["dedup"]["removed"],
        merge_columns=summary["dedup"]["merge_columns"],
    ))
    log.write(format_footer())
    log.close()
