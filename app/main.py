# app/main.py
from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

# PYTHONPATH → корень проекта
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.pipeline.runner import run_pipeline  # noqa: E402

APP_TITLE = "CSV Normalizer — MVP"
SETTINGS_PATH = ROOT / "configs" / "app_settings.yaml"


# ───────────────────────── settings ─────────────────────────
DEFAULT_SETTINGS = {
    "save_dir": str((ROOT / "out").resolve()),
    "out_name": "normalized.csv",
    "log_name": "normalize_log.txt",
    "profile": "configs/profiles/uni.yaml",
    "delimiter": ",",
    "encoding": "auto",
    "chunksize": 100_000,
    "dedup_enabled": False,
    "dedup_field": "",
    "dedup_merge_columns": [],
    "estimate_total_rows": True,
}

def load_settings() -> dict:
    try:
        if SETTINGS_PATH.exists():
            with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return {**DEFAULT_SETTINGS, **data}
    except Exception:
        pass
    return dict(DEFAULT_SETTINGS)

def save_settings(data: dict) -> None:
    try:
        SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, allow_unicode=True, sort_keys=True)
    except Exception:
        pass


# ───────────────────────── helpers ─────────────────────────
def read_columns_head(path: str, delimiter: str, encoding: str) -> list[str]:
    if not path or not Path(path).exists():
        return []
    try_order = [encoding] if encoding != "auto" else ["utf-8", "cp1251", "latin1"]
    for enc in try_order:
        try:
            df = pd.read_csv(path, sep=delimiter, nrows=0, dtype=str, keep_default_na=False, encoding=enc)
            return list(df.columns)
        except Exception:
            continue
    return []

def folder_picker(label: str, start_path: str | Path | None = None, key: str = "folder_picker") -> str:
    """Простой проводник папок в сайдбаре. Возвращает выбранную папку."""
    st.sidebar.subheader(label)

    if f"{key}__cwd" not in st.session_state:
        base = Path(start_path) if start_path else Path.cwd()
        st.session_state[f"{key}__cwd"] = str(base.resolve())

    cwd = Path(st.session_state[f"{key}__cwd"])
    manual_path = st.sidebar.text_input("Текущая папка", value=str(cwd), key=f"{key}__manual_path")

    cols = st.sidebar.columns([1, 1, 2])
    with cols[0]:
        if st.button("⬆️ Вверх", key=f"{key}__up"):
            st.session_state[f"{key}__cwd"] = str(cwd.parent.resolve())
            cwd = Path(st.session_state[f"{key}__cwd"])
    with cols[1]:
        if st.button("Перейти", key=f"{key}__go"):
            p = Path(manual_path).expanduser()
            if p.exists() and p.is_dir():
                st.session_state[f"{key}__cwd"] = str(p.resolve())
                cwd = Path(st.session_state[f"{key}__cwd"])

    try:
        subdirs = sorted([p for p in cwd.iterdir() if p.is_dir()], key=lambda p: p.name.lower())
    except Exception:
        subdirs = []

    choice = st.sidebar.selectbox(
        "Подпапки", options=["— оставить текущую —"] + [d.name for d in subdirs], key=f"{key}__select"
    )
    if choice != "— оставить текущую —":
        new_cwd = cwd / choice
        if new_cwd.exists():
            st.session_state[f"{key}__cwd"] = str(new_cwd.resolve())
            cwd = new_cwd

    st.sidebar.caption(f"Текущая папка: {cwd}")
    return str(cwd)

def quick_count_rows(path: str) -> int:
    """Быстрый подсчёт строк (минус заголовок). Делает отдельный проход по файлу."""
    try:
        total = 0
        with open(path, "rb") as f:
            for _ in f:
                total += 1
        return max(0, total - 1)
    except Exception:
        return 0


# ───────────────────────── UI ─────────────────────────
def page_header():
    st.title(APP_TITLE)
    st.caption("Выбор входного CSV → конфиг правил → запуск нормализации → лог + прогресс и метрики")

def sidebar_inputs():
    settings = load_settings()

    st.sidebar.header("Файлы")
    mode = st.sidebar.radio("Источник входного файла", ["Локальный путь", "Загрузка файла"], index=0)

    input_path = None
    uploaded_tmp = None

    if mode == "Локальный путь":
        input_path = st.sidebar.text_input("Путь к входному CSV", value="")
    else:
        up = st.sidebar.file_uploader("Загрузите CSV", type=["csv"])
        if up is not None:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            tmp.write(up.getbuffer())
            tmp.flush()
            tmp.close()
            uploaded_tmp = tmp.name
            input_path = uploaded_tmp

    st.sidebar.divider()
    st.sidebar.header("Куда сохранять результат")
    save_dir = folder_picker(
        "Выбор папки сохранения",
        start_path=Path(settings["save_dir"]),
        key="save_dir",
    )
    out_name = st.sidebar.text_input("Имя выходного CSV", value=settings["out_name"])
    log_name = st.sidebar.text_input("Имя лога (.txt)", value=settings["log_name"])
    output_path = str(Path(save_dir) / out_name) if out_name else ""
    log_path = str(Path(save_dir) / log_name) if log_name else ""

    st.sidebar.divider()
    st.sidebar.header("Профиль правил")
    profile = st.sidebar.selectbox(
        "Выберите YAML-профиль",
        options=[
            "configs/profiles/uni.yaml",
            "configs/profiles/minimal_email.yaml",
        ],
        index=0 if settings["profile"] == "configs/profiles/uni.yaml" else 1,
    )

    st.sidebar.divider()
    delimiter = st.sidebar.text_input("Разделитель", value=settings["delimiter"])
    encoding = st.sidebar.selectbox("Кодировка", options=["auto", "utf-8", "cp1251", "latin1"],
                                    index=["auto", "utf-8", "cp1251", "latin1"].index(settings["encoding"]))
    chunksize = st.sidebar.number_input(
        "Размер чанка", min_value=10_000, max_value=2_000_000, step=50_000, value=int(settings["chunksize"])
    )

    # Дедупликация с объединением
    st.sidebar.divider()
    st.sidebar.header("Дедупликация")
    columns = read_columns_head(input_path, delimiter, encoding)
    dedup_enabled = st.sidebar.checkbox("Включить дедупликацию", value=bool(settings["dedup_enabled"]))
    dedup_field_default = settings.get("dedup_field") if settings.get("dedup_field") in columns else (columns[0] if columns else "")
    dedup_field = st.sidebar.selectbox(
        "Поле для дедупа (ключ)",
        options=columns if columns else ["(колонки не обнаружены)"],
        disabled=not (dedup_enabled and columns),
        index=(columns.index(dedup_field_default) if (columns and dedup_field_default in columns) else 0),
    ) if columns else "(колонки не обнаружены)"
    dedup_subset = [dedup_field] if (dedup_enabled and columns) else []

    merge_candidates = [c for c in columns if c != dedup_field] if columns else []
    # восстановим сохранённые merge-колонки
    saved_merge = [c for c in settings.get("dedup_merge_columns", []) if c in merge_candidates]
    dedup_merge_columns = st.sidebar.multiselect(
        "Столбцы для объединения значений через ';'",
        options=merge_candidates,
        default=saved_merge,
        disabled=not (dedup_enabled and columns),
    )

    st.sidebar.divider()
    estimate_total_rows = st.sidebar.checkbox(
        "Оценить общее число строк для прогресса (требует дополнительного прохода по файлу)",
        value=bool(settings.get("estimate_total_rows", True)),
    )

    return (
        input_path,
        output_path,
        log_path,
        profile,
        delimiter,
        encoding,
        chunksize,
        uploaded_tmp,
        dedup_enabled,
        dedup_subset,
        dedup_merge_columns,
        save_dir,
        out_name,
        log_name,
        estimate_total_rows,
        dedup_field if isinstance(dedup_field, str) else "",
    )

def show_preview(input_path: str, delimiter: str, encoding: str):
    st.subheader("Предпросмотр входных данных")
    if not input_path or not Path(input_path).exists():
        st.info("Укажите существующий входной CSV, чтобы увидеть предпросмотр.")
        return

    try_order = [encoding] if encoding != "auto" else ["utf-8", "cp1251", "latin1"]
    df = None
    err = None
    for enc in try_order:
        try:
            df = pd.read_csv(
                input_path,
                sep=delimiter,
                nrows=200,
                dtype=str,
                keep_default_na=False,
                encoding=enc,
            )
            break
        except Exception as e:
            err = e
            continue

    if df is None:
        st.error(f"Не удалось прочитать CSV: {err}")
        return

    st.write(f"Строк в предпросмотре: {len(df)}  •  Колонок: {len(df.columns)}")
    st.dataframe(df, use_container_width=True)

def run_button(
    input_path,
    output_path,
    log_path,
    profile,
    delimiter,
    encoding,
    chunksize,
    dedup_enabled,
    dedup_subset,
    dedup_merge_columns,
    save_dir,
    out_name,
    log_name,
    estimate_total_rows,
    dedup_field,
):
    st.subheader("Запуск")
    can_run = all([input_path, output_path, log_path, profile])
    run = st.button("🚀 Запустить нормализацию", disabled=not can_run, type="primary")
    if not can_run:
        st.caption("Заполните входной файл, папку сохранения и имена файлов, а также профиль правил.")

    if run:
        # гарантируем наличие папок
        try:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            st.error(f"Не удалось создать папку сохранения: {e}")
            return

        # считаем строки для прогресса (опционально)
        rows_est = None
        if estimate_total_rows and Path(input_path).exists():
            with st.spinner("Подсчёт строк для прогресса…"):
                rows_est = quick_count_rows(input_path)

        # прогресс-UI
        prog = st.progress(0)
        status = st.empty()
        metrics_box = st.empty()

        def _progress_cb(info: dict):
            percent = info.get("percent")
            if percent is not None:
                prog.progress(int(percent))
            rows_done = info.get("rows_done", 0)
            rows_est_ = info.get("rows_est", None)
            elapsed = info.get("elapsed_sec", 0.0)
            rps = info.get("rps", 0.0)
            eta_txt = ""
            if rows_est_ and rows_est_ > 0 and rps and rps > 0:
                remain = max(0, rows_est_ - rows_done)
                eta = remain / rps
                eta_txt = f" • ETA ~ {int(eta)} сек"
            status.text(f"Обработано строк: {rows_done}" + (f" / ~{rows_est_}" if rows_est_ else "") + eta_txt)
            metrics_box.markdown(f"**Скорость:** {rps:.2f} строк/сек • **Прошло:** {elapsed:.1f} сек")

        # сохраняем текущие настройки
        save_settings({
            "save_dir": save_dir,
            "out_name": out_name,
            "log_name": log_name,
            "profile": profile,
            "delimiter": delimiter,
            "encoding": encoding,
            "chunksize": int(chunksize),
            "dedup_enabled": bool(dedup_enabled),
            "dedup_field": dedup_field,
            "dedup_merge_columns": list(dedup_merge_columns or []),
            "estimate_total_rows": bool(estimate_total_rows),
        })

        with st.spinner("Обработка… это может занять время на больших файлах"):
            run_pipeline(
                input_csv=input_path,
                output_csv=output_path,
                config_yaml=profile,
                log_txt=log_path,
                chunksize=int(chunksize),
                delimiter_override=delimiter,
                encoding_override=encoding,
                dedup_enabled=dedup_enabled,
                dedup_subset=dedup_subset,
                dedup_merge_columns=dedup_merge_columns,
                progress_cb=_progress_cb,
                rows_total_estimate=rows_est,
            )

        prog.progress(100)
        st.success("Готово! Результат и лог записаны.")

        # Скачать файлы
        st.subheader("Скачать результаты")
        try:
            if Path(output_path).exists():
                with open(output_path, "rb") as fh:
                    st.download_button(
                        "⬇️ Скачать CSV", data=fh.read(), file_name=Path(output_path).name, mime="text/csv"
                    )
        except Exception as e:
            st.warning(f"Не удалось подготовить CSV для скачивания: {e}")

        try:
            if Path(log_path).exists():
                with open(log_path, "rb") as fh:
                    st.download_button(
                        "⬇️ Скачать лог (.txt)", data=fh.read(), file_name=Path(log_path).name, mime="text/plain"
                    )
        except Exception as e:
            st.warning(f"Не удалось подготовить лог для скачивания: {e}")

        # Показать хвост лога
        try:
            with open(log_path, "r", encoding="utf-8") as fh:
                text = fh.read()
            st.subheader("Лог (фрагмент)")
            st.text(text[-8000:])
        except Exception as e:
            st.warning(f"Не удалось прочитать лог: {e}")


def main():
    page_header()
    (
        input_path,
        output_path,
        log_path,
        profile,
        delimiter,
        encoding,
        chunksize,
        uploaded_tmp,
        dedup_enabled,
        dedup_subset,
        dedup_merge_columns,
        save_dir,
        out_name,
        log_name,
        estimate_total_rows,
        dedup_field,
    ) = sidebar_inputs()
    show_preview(input_path, delimiter, encoding)
    run_button(
        input_path,
        output_path,
        log_path,
        profile,
        delimiter,
        encoding,
        chunksize,
        dedup_enabled,
        dedup_subset,
        dedup_merge_columns,
        save_dir,
        out_name,
        log_name,
        estimate_total_rows,
        dedup_field,
    )

    # очистка временного файла
    if uploaded_tmp and Path(uploaded_tmp).exists():
        try:
            os.remove(uploaded_tmp)
        except Exception:
            pass


if __name__ == "__main__":
    main()
