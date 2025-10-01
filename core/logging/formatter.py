# core/logging/formatter.py
from __future__ import annotations

from datetime import datetime
from typing import List, Dict, Any


def format_header(
    *,
    input_csv: str,
    output_csv: str,
    rows_total: int,
    columns: List[str],
    delimiter: str = ",",
    encoding: str = "auto",
    dedup_enabled: bool = False,
    dedup_subset: List[str] | None = None,
    duration_sec: float | None = None,
    rows_per_sec: float | None = None,
) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cols = ", ".join(columns) if columns else "-"
    dedup_subset = dedup_subset or []
    lines = [
        "======== ОТЧЁТ НОРМАЛИЗАЦИИ ========",
        f"Дата/время: {ts}",
        f"Файл входной: {input_csv}",
        f"Файл выходной: {output_csv}",
        f"Строк обработано: {rows_total}",
        f"Столбцы: {cols}",
        f"Разделитель: {delimiter}",
        f"Кодировка: {encoding}",
        f"Дедупликация: {'ВКЛ' if dedup_enabled else 'ВЫКЛ'}"
        + (f" (по полю: {', '.join(dedup_subset)})" if dedup_enabled and dedup_subset else ""),
    ]
    if duration_sec is not None:
        lines.append(f"Длительность: {duration_sec:.2f} сек")
    if rows_per_sec is not None:
        lines.append(f"Скорость: {rows_per_sec:.2f} строк/сек")
    lines += [
        "====================================",
        "",
    ]
    return "\n".join(lines)


def format_column_section(
    *,
    title: str,
    column: str,
    changed: int,
    cleared: int,
    examples: List[Dict[str, Any]],
    initial_empty: int | None = None,  # <- новый параметр
    **_: Any,  # <- на будущее: игнорируем лишние ключи, чтобы не падать
) -> str:
    lines: List[str] = []
    lines.append(f"================ {title} ==============")
    if initial_empty is not None:
        lines.append(f"[{column}] изначально пустых ячеек: {initial_empty}")
    lines.append(f"[{column}] ячеек изменено: {changed}")
    lines.append(f"[{column}] ячеек очищено по валидации: {cleared}")
    lines.append("")
    if examples:
        for ex in examples:
            row = ex.get("row", "")
            before = ex.get("before", "")
            after = ex.get("after", "")
            note = ex.get("note", "")
            lines.append(f"[{column}] строка {row}: \"{before}\" → \"{after}\"{f' ({note})' if note else ''}")
    else:
        lines.append("(примеров изменений нет)")
    lines.append("")
    return "\n".join(lines)


def format_row_filters_section(*, one_filled_enabled: bool, subset: List[str], removed: int) -> str:
    lines = []
    lines.append("================ ФИЛЬТР СТРОК ==============")
    lines.append("Правило: удалить строку, если заполнена ровно одна ячейка среди выбранных столбцов")
    lines.append(f"Статус: {'ВКЛЮЧЕНО' if one_filled_enabled else 'ВЫКЛЮЧЕНО'}")
    if one_filled_enabled:
        lines.append(f"Столбцы для проверки: {', '.join(subset) if subset else '-'}")
        lines.append(f"Удалено строк: {removed}")
    lines.append("")
    return "\n".join(lines)


def format_dedup_section(*, enabled: bool, subset: List[str], removed: int, merge_columns: List[str] | None = None) -> str:
    lines = []
    lines.append("================ ДЕДУПЛИКАЦИЯ ==============")
    lines.append(f"Статус: {'ВКЛЮЧЕНА' if enabled else 'ВЫКЛЮЧЕНА'}")
    if enabled:
        lines.append(f"Поле(я): {', '.join(subset) if subset else '-'}")
        if merge_columns is not None:
            lines.append(f"Объединяем столбцы: {', '.join(merge_columns) if merge_columns else '(нет)'}")
        lines.append(f"Удалено дубликатов: {removed}")
    lines.append("")
    return "\n".join(lines)


def format_footer() -> str:
    return "=============== КОНЕЦ ОТЧЁТА ===============\n"
