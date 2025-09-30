# Changelog
Все заметные изменения этого проекта будут документироваться в этом файле.

Формат основан на [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/),
и проект следует [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-09-30
### Added
- MVP: пайплайн обработки CSV чанками.
- Правило `email`: trim → invisible cleanup → lower → regex валидация; при невалидности очищаем ячейку.
- Логирование в .txt с разделом “ПОЧТА”.
- Streamlit UI (фиолетовая тема) с предпросмотром и запуском.
