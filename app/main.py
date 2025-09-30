# app/main.py
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

# Добавляем корень проекта в PYTHONPATH
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.pipeline.runner import run_pipeline  # noqa: E402

APP_TITLE = "CSV Normalizer — MVP"


def read_columns_head(path: str, delimiter: str, encoding: str) -> list[str]:
    if not path or not Path(path).exists():
        return []
    encodings_to_try = [encoding] if encoding != "auto" else ["utf-8", "cp1251", "latin1"]
    for enc in encodings_to_try:
        try:
            df = pd.read_csv(path, sep=delimiter, nrows=0, dtype=str, keep_default_na=False, encoding=enc)
            return list(df.columns)
        except Exception:
            continue
    return []


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

    output_path = st.sidebar.text_input("Путь для выходного CSV", value="")
    log_path = st.sidebar.text_input("Путь для лога (.txt)", value="")

    st.sidebar.divider()
    st.sidebar.header("Профиль правил")
    profile = st.sidebar.selectbox(
        "Выберите YAML-профиль",
        options=[
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

    # Дедупликация
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

    return input_path, output_path, log_path, profile, delimiter, encoding, chunksize, uploaded_tmp, dedup_enabled, dedup_subset


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
    st.dataframe(df)


def run_button(input_path, output_path, log_path, profile, delimiter, encoding, chunksize, dedup_enabled, dedup_subset):
    st.subheader("Запуск")
    can_run = all([input_path, output_path, log_path, profile])
    run = st.button("🚀 Запустить нормализацию", disabled=not can_run, type="primary")
    if not can_run:
        st.caption("Заполните пути к файлам и профиль правил.")

    if run:
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

        # Кнопки скачивания
        st.subheader("Скачать результаты")
        try:
            if Path(output_path).exists():
                with open(output_path, "rb") as fh:
                    st.download_button("⬇️ Скачать CSV", data=fh.read(), file_name=Path(output_path).name, mime="text/csv")
        except Exception as e:
            st.warning(f"Не удалось подготовить CSV для скачивания: {e}")

        try:
            if Path(log_path).exists():
                with open(log_path, "rb") as fh:
                    st.download_button("⬇️ Скачать лог (.txt)", data=fh.read(), file_name=Path(log_path).name, mime="text/plain")
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

    # Очистка временного файла (если был)
    if uploaded_tmp and Path(uploaded_tmp).exists():
        try:
            os.remove(uploaded_tmp)
        except Exception:
            pass


if __name__ == "__main__":
    main()
