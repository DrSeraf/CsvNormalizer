# app/main.py
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

# PYTHONPATH → корень проекта
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.pipeline.runner import run_pipeline  # noqa: E402

APP_TITLE = "CSV Normalizer — MVP"


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


# ───────────────────────── UI ─────────────────────────
def page_header():
    st.title(APP_TITLE)
    st.caption("Выбор входного CSV → конфиг правил → запуск нормализации → лог")


def sidebar_inputs():
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
    save_dir = folder_picker("Выбор папки сохранения", start_path=Path.cwd(), key="save_dir")
    out_name = st.sidebar.text_input("Имя выходного CSV", value="normalized.csv")
    log_name = st.sidebar.text_input("Имя лога (.txt)", value="normalize_log.txt")
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
        index=0,
    )

    st.sidebar.divider()
    delimiter = st.sidebar.text_input("Разделитель", value=",")
    encoding = st.sidebar.selectbox("Кодировка", options=["auto", "utf-8", "cp1251", "latin1"], index=0)
    chunksize = st.sidebar.number_input(
        "Размер чанка", min_value=10_000, max_value=2_000_000, step=50_000, value=100_000
    )

    st.sidebar.divider()
    st.sidebar.header("Дедупликация")
    columns = read_columns_head(input_path, delimiter, encoding)
    dedup_enabled = st.sidebar.checkbox("Включить дедупликацию", value=False)
    dedup_field = st.sidebar.selectbox(
        "Поле для дедупа",
        options=columns if columns else ["(колонки не обнаружены)"],
        disabled=not (dedup_enabled and columns),
        index=0,
    )
    dedup_subset = [dedup_field] if (dedup_enabled and columns) else []

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


def run_button(input_path, output_path, log_path, profile, delimiter, encoding, chunksize, dedup_enabled, dedup_subset):
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
            )
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
    )

    # очистка временного файла
    if uploaded_tmp and Path(uploaded_tmp).exists():
        try:
            os.remove(uploaded_tmp)
        except Exception:
            pass


if __name__ == "__main__":
    main()
