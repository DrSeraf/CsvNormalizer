# core/pipeline/runner.py
from __future__ import annotations

import os
from typing import Dict, Any, List, Optional

import pandas as pd
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
from core.dedup.engine import DedupEngine


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
) -> None:
    # ── загрузим конфиг
    with open(config_yaml, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    cfg_global = cfg.get("global", {}) if isinstance(cfg, dict) else {}

    delimiter: str = delimiter_override or cfg_global.get("delimiter", ",")
    encoding: str = encoding_override or cfg_global.get("encoding", "auto")
    columns_cfg: Dict[str, Any] = cfg.get("columns", {})

    # Если в конфиге есть секция дедупа — берём по умолчанию оттуда, но UI приоритетнее
    dedup_cfg = cfg_global.get("deduplicate", {}) if isinstance(cfg_global, dict) else {}
    dedup_enabled = bool(dedup_enabled or dedup_cfg.get("enabled", False))
    dedup_subset = dedup_subset or dedup_cfg.get("subset", [])
    ignore_empty_in_subset = bool(dedup_cfg.get("ignore_empty_in_subset", True) if ignore_empty_in_subset is None else ignore_empty_in_subset)

    # ── подготовим лог
    os.makedirs(os.path.dirname(log_txt) or ".", exist_ok=True)
    log = LogSink(log_txt)

    summary = {
        "rows_total": 0,
        "columns": list(columns_cfg.keys()),
        "email": {
            "changed": 0,
            "cleared_invalid": 0,
            "examples": [],
        },
        "dedup": {
            "enabled": dedup_enabled,
            "subset": dedup_subset or [],
            "removed": 0,
        },
        "delimiter": delimiter,
        "encoding": encoding,
    }

    # ── писатель результата
    writer = CsvIncrementalWriter(output_csv)

    # ── дедуп-движок
    deduper = None
    if dedup_enabled and dedup_subset:
        deduper = DedupEngine(subset=dedup_subset, ignore_empty_in_subset=True)

    # ── обработка чанками
    examples_limit = 25
    first_chunk = True
    for chunk in read_csv_in_chunks(
        input_csv,
        delimiter=delimiter,
        encoding=encoding,
        chunksize=chunksize,
    ):
        summary["rows_total"] += len(chunk)

        # По каждому столбцу из конфига применяем последовательность правил
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

                    if col_name == "email":
                        local_changed += int(stats.get("changed", 0))
                        local_cleared += int(stats.get("cleared_invalid", 0))
                        for ex in stats.get("examples", []):
                            if len(summary["email"]["examples"]) + len(local_examples) < examples_limit:
                                local_examples.append(ex)
                else:
                    series = fn(series, **params)

            # записываем изменённую колонку обратно
            chunk[col_name] = series

            if col_name == "email":
                summary["email"]["changed"] += local_changed
                summary["email"]["cleared_invalid"] += local_cleared
                space_left = examples_limit - len(summary["email"]["examples"])
                if space_left > 0 and local_examples:
                    summary["email"]["examples"].extend(local_examples[:space_left])

        # ── дедуп (если включен)
        if deduper is not None:
            before = len(chunk)
            chunk = deduper.filter_chunk(chunk)
            removed_now = before - len(chunk)
            if removed_now:
                summary["dedup"]["removed"] += removed_now

        # ── пишем чанк в выходной CSV
        writer.write_chunk(chunk, header=first_chunk)
        first_chunk = False

    # ── лог
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

    if "email" in columns_cfg:
        log.write(format_column_section(
            title="ПОЧТА",
            column="email",
            changed=summary["email"]["changed"],
            cleared=summary["email"]["cleared_invalid"],
            examples=summary["email"]["examples"],
        ))

    # раздел про дедуп
    log.write(format_dedup_section(
        enabled=summary["dedup"]["enabled"],
        subset=summary["dedup"]["subset"],
        removed=summary["dedup"]["removed"],
    ))

    log.write(format_footer())
    log.close()
