# app/main.py
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

# Добавляем корень проекта в PYTHONPATH, чтобы работали импорты вида "from core...."
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.pipeline.runner import run_pipeline  # noqa: E402  (после вставки ROOT в sys.path)

APP_TITLE = "CSV Normalizer — MVP"


def page_header():
    st.title(APP_TITLE)
    st.caption("Выбор входного CSV → конфиг правил → запуск нормализации → лог")


def sidebar_inputs():
    st.sidebar.header("Файлы")

    mode = st.sidebar.radio(
        "Источник входного файла", ["Локальный путь", "Загрузка файла"], index=0
    )

    input_path = None
    uploaded_tmp = None

    if mode == "Локальный путь":
        input_path = st.sidebar.text_input("Путь к входному CSV", value="")
    else:
        up = st.sidebar.file_uploader("Загрузите CSV", type=["csv"])
        if up is not None:
            # Сохраняем загруженный файл во временный
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
            # сюда добавим новые профили позже
        ],
        index=0,
    )

    st.sidebar.divider()
    delimiter = st.sidebar.text_input("Разделитель (по умолчанию ,)", value=",")
    chunksize = st.sidebar.number_input(
        "Размер чанка", min_value=10_000, max_value=2_000_000, step=50_000, value=100_000
    )

    return input_path, output_path, log_path, profile, delimiter, chunksize, uploaded_tmp


def show_preview(input_path: str, delimiter: str):
    st.subheader("Предпросмотр входных данных")
    if not input_path or not Path(input_path).exists():
        st.info("Укажите существующий входной CSV, чтобы увидеть предпросмотр.")
        return

    try:
        df = pd.read_csv(
            input_path,
            sep=delimiter,
            nrows=200,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(
                input_path,
                sep=delimiter,
                nrows=200,
                dtype=str,
                keep_default_na=False,
                encoding="cp1251",
            )
        except Exception as e:
            st.error(f"Не удалось прочитать CSV (cp1251): {e}")
            return
    except Exception as e:
        st.error(f"Не удалось прочитать CSV: {e}")
        return

    st.write(f"Строк в предпросмотре: {len(df)}  •  Колонок: {len(df.columns)}")
    st.dataframe(df)


def run_button(input_path, output_path, log_path, profile, delimiter, chunksize):
    st.subheader("Запуск")
    can_run = all([input_path, output_path, log_path, profile])
    run = st.button("🚀 Запустить нормализацию", disabled=not can_run, type="primary")
    if not can_run:
        st.caption("Заполните пути к файлам и профиль правил.")

    if run:
        with st.spinner("Обработка… это может занять время на больших файлах"):
            # Сейчас delimiter берётся из YAML (по умолчанию ',').
            # Если нужно жёстко переопределять из UI — позже добавим в runner параметр.
            run_pipeline(
                input_csv=input_path,
                output_csv=output_path,
                config_yaml=profile,
                log_txt=log_path,
                chunksize=int(chunksize),
            )
        st.success("Готово! Результат и лог записаны.")

        # Показать хвост лога
        try:
            with open(log_path, "r", encoding="utf-8") as fh:
                text = fh.read()
            st.subheader("Лог (фрагмент)")
            st.text(text[-8000:])  # последние ~8к символов
        except Exception as e:
            st.warning(f"Не удалось прочитать лог: {e}")


def main():
    page_header()
    input_path, output_path, log_path, profile, delimiter, chunksize, uploaded_tmp = sidebar_inputs()
    show_preview(input_path, delimiter)
    run_button(input_path, output_path, log_path, profile, delimiter, chunksize)

    # Попытка очистить временный файл (если он был создан загрузчиком)
    if uploaded_tmp and Path(uploaded_tmp).exists():
        try:
            os.remove(uploaded_tmp)
        except Exception:
            pass


if __name__ == "__main__":
    main()
