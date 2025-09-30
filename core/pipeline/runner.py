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
from core.dedup.engine import DedupEngine

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
) -> None:
    with open(config_yaml, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    cfg_global = cfg.get("global", {}) if isinstance(cfg, dict) else {}

    delimiter: str = delimiter_override or cfg_global.get("delimiter", ",")
    encoding: str = encoding_override or cfg_global.get("encoding", "auto")
    columns_cfg: Dict[str, Any] = cfg.get("columns", {})

    dedup_cfg = cfg_global.get("deduplicate", {}) if isinstance(cfg_global, dict) else {}
    dedup_enabled = bool(dedup_enabled or dedup_cfg.get("enabled", False))
    dedup_subset = dedup_subset or dedup_cfg.get("subset", [])
    ignore_empty_in_subset = bool(dedup_cfg.get("ignore_empty_in_subset", True) if ignore_empty_in_subset is None else ignore_empty_in_subset)

    os.makedirs(os.path.dirname(log_txt) or ".", exist_ok=True)
    log = LogSink(log_txt)

    summary = {
        "rows_total": 0,
        "columns": list(columns_cfg.keys()),
        "delimiter": delimiter,
        "encoding": encoding,
        "dedup": {"enabled": dedup_enabled, "subset": dedup_subset or [], "removed": 0},
        "cols_stats": {},   # col -> {changed, cleared, examples}
    }

    writer = CsvIncrementalWriter(output_csv)
    deduper = DedupEngine(subset=dedup_subset, ignore_empty_in_subset=True) if (dedup_enabled and dedup_subset) else None

    examples_limit = 25
    first_chunk = True
    for chunk in read_csv_in_chunks(input_csv, delimiter=delimiter, encoding=encoding, chunksize=chunksize):
        summary["rows_total"] += len(chunk)

        for col_name, spec in columns_cfg.items():
            if col_name not in chunk.columns:
                continue

            rules: List[Dict[str, Any]] = spec.get("rules", [])
            series = chunk[col_name]

            # локальная статистика по колонке
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
                    # поддерживаем и ключ cleared, и cleared_invalid (для обратной совместимости)
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

            # дополним примеры, но не больше 25 на колонку
            space_left = examples_limit - len(summary["cols_stats"][col_name]["examples"])
            if space_left > 0 and local_examples:
                summary["cols_stats"][col_name]["examples"].extend(local_examples[:space_left])

        if deduper is not None:
            before = len(chunk)
            chunk = deduper.filter_chunk(chunk)
            removed_now = before - len(chunk)
            if removed_now:
                summary["dedup"]["removed"] += removed_now

        writer.write_chunk(chunk, header=first_chunk)
        first_chunk = False

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

    log.write(format_dedup_section(
        enabled=summary["dedup"]["enabled"],
        subset=summary["dedup"]["subset"],
        removed=summary["dedup"]["removed"],
    ))
    log.write(format_footer())
    log.close()
