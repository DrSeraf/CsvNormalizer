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
from core.logging.formatter import format_header, format_column_section, format_footer


def run_pipeline(
    input_csv: str,
    output_csv: str,
    config_yaml: str,
    log_txt: str,
    *,
    chunksize: int = 100_000,
) -> None:
    # ── загрузим конфиг
    with open(config_yaml, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    delimiter: str = cfg.get("global", {}).get("delimiter", ",")
    encoding: str = cfg.get("global", {}).get("encoding", "auto")
    columns_cfg: Dict[str, Any] = cfg.get("columns", {})

    # ── подготовим лог
    os.makedirs(os.path.dirname(log_txt) or ".", exist_ok=True)
    log = LogSink(log_txt)
    # заголовок добавим после первой статистики (но шапку файла можно заранее)
    summary = {
        "rows_total": 0,
        "columns": list(columns_cfg.keys()),
        "email": {
            "changed": 0,
            "cleared_invalid": 0,
            "examples": [],  # соберём топ-25
        },
    }

    # ── писатель результата
    writer = CsvIncrementalWriter(output_csv)

    # ── обработка чанками
    examples_limit = 25
    first_chunk = True
    for chunk in read_csv_in_chunks(input_csv, delimiter=delimiter, encoding=encoding, chunksize=chunksize):
        summary["rows_total"] += len(chunk)

        # По каждому столбцу из конфига применяем последовательность правил
        for col_name, spec in columns_cfg.items():
            if col_name not in chunk.columns:
                continue  # столбец отсутствует — пропускаем

            rules: List[Dict[str, Any]] = spec.get("rules", [])
            series = chunk[col_name]

            local_changed = 0
            local_cleared = 0
            local_examples: List[Dict[str, Any]] = []

            for step in rules:
                # каждый step — это {name: params}
                if not isinstance(step, dict) or len(step) != 1:
                    raise ValueError(f"Bad rule format for column '{col_name}': {step}")
                name, params = list(step.items())[0]
                params = params or {}

                fn = registry.get(name)

                if registry.is_advanced(name):
                    # advanced должен вернуть (Series, stats)
                    new_series, stats = fn(series, **params)
                    series = new_series

                    # собираем статистику только один раз — для validate_regex (email_basic)
                    if col_name == "email":
                        local_changed += int(stats.get("changed", 0))
                        local_cleared += int(stats.get("cleared_invalid", 0))
                        # примеры копим, но ограничиваем общий пул
                        for ex in stats.get("examples", []):
                            if len(summary["email"]["examples"]) + len(local_examples) < examples_limit:
                                local_examples.append(ex)
                else:
                    # примитивы возвращают Series
                    series = fn(series, **params)

            # записываем изменённую колонку обратно в чанк
            chunk[col_name] = series

            # обновим сводку для email
            if col_name == "email":
                summary["email"]["changed"] += local_changed
                summary["email"]["cleared_invalid"] += local_cleared
                # добавим примеры из этого чанка
                space_left = examples_limit - len(summary["email"]["examples"])
                if space_left > 0 and local_examples:
                    summary["email"]["examples"].extend(local_examples[:space_left])

        # ── пишем чанк в выходной CSV
        writer.write_chunk(chunk, header=first_chunk)
        first_chunk = False

    # ── сформируем лог
    log.write(format_header(
        input_csv=input_csv,
        output_csv=output_csv,
        rows_total=summary["rows_total"],
        columns=summary["columns"],
    ))

    # раздел по email (если колонка была в конфиге)
    if "email" in columns_cfg:
        log.write(format_column_section(
            title="ПОЧТА",
            column="email",
            changed=summary["email"]["changed"],
            cleared=summary["email"]["cleared_invalid"],
            examples=summary["email"]["examples"],
        ))

    log.write(format_footer())
    log.close()
