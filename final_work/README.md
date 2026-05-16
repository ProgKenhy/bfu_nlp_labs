# 🏷️ StackOverflow Auto-Tagger

Автоматическое присвоение тегов вопросам StackOverflow на основе их заголовка и тела.  
Задача — **multi-label классификация**: каждому вопросу назначается один или несколько тегов из топ-50 наиболее популярных.

---

## 📌 Описание задачи

Вопросы на StackOverflow вручную размечаются авторами, что требует времени и приводит к ошибкам — неверным или пропущенным тегам. Цель проекта: обучить модель, которая автоматически предлагает теги по тексту вопроса.

**Вход:** заголовок + тело вопроса (HTML)  
**Выход:** набор тегов из топ-50 (`python`, `javascript`, `sql`, `reactjs`, ...)

---

## 🗂️ Структура проекта

```
stackoverflow-autotagger/
├── stackoverflow_autotag.ipynb   # Основной ноутбук (весь пайплайн)
├── so_questions_raw.json         # Кэш сырых данных из API (генерируется)
├── best_distilbert_tagger.pt     # Веса лучшей модели (генерируется)
└── README.md
```

---

## ⚙️ Установка

**Требования:** Python 3.10+, pip

```bash
git clone https://github.com/your-username/stackoverflow-autotagger.git
cd stackoverflow-autotagger
pip install -r requirements.txt
```

`requirements.txt`:
```
requests
pandas
numpy
scikit-learn
matplotlib
seaborn
beautifulsoup4
nltk
tqdm
transformers==4.40.*
torch>=2.1
accelerate
```

> **GPU:** обучение DistilBERT без GPU занимает ~3–4 часа на 8000 примеров.  
> С GPU (NVIDIA T4 и выше) — ~15–20 минут.

---

## 🚀 Быстрый старт

```bash
jupyter notebook stackoverflow_autotag.ipynb
```

Запускайте ячейки последовательно. Ноутбук самодостаточен: скачает данные, обучит обе модели и выведет сравнение метрик.

---

## 📊 Пайплайн

```
StackExchange API
      │
      ▼
 Сбор данных          Шаг 2  — инкрементальный, кэш каждые 500 вопросов
      │
      ▼
    EDA               Шаг 3  — частоты тегов, co-occurrence матрица
      │
      ▼
 Очистка текста       Шаг 4  — HTML → идентификаторы из кода, лемматизация
      │
      ▼
 MultiLabelBinarizer  Шаг 5  — матрица меток Y (N × 50)
      │
      ├──► TF-IDF (50k n-грамм)          Шаг 6
      │         │
      │         ▼
      │    LogReg OneVsRest              Шаг 7  — базовая модель
      │
      └──► DistilBERT токенизатор        Шаг 8
                │
                ▼
           Fine-tuning (3 эпохи)        Шаг 8  — продвинутая модель
                │
                ▼
         Оптимизация порога             Шаг 11 — grid search по threshold
                │
                ▼
            Метрики + анализ ошибок     Шаги 9–10
```

---

## 🧹 Предобработка текста

Особенность SO-вопросов — наличие кода внутри `<code>` и `<pre>`. Вместо удаления блоков кода извлекаются **идентификаторы**:

```python
# Плохо:  "functionality CODEBLOCK keyword python"
# Хорошо: "functionality yield keyword python get_child_candidates leftchild median"
```

Дополнительно: технические термины (`pandas`, `numpy`, имена тегов) **не лемматизируются** — иначе `pandas` → `panda`.

---

## 🤖 Модели

### Базовая — TF-IDF + Logistic Regression (OneVsRest)

- TF-IDF: 50 000 uni- и биграмм, `sublinear_tf=True`
- Один бинарный классификатор на каждый из 50 тегов
- Обучение: ~2–3 минуты на CPU

### Продвинутая — DistilBERT Fine-tuning

- `distilbert-base-uncased` (66M параметров, 40% быстрее BERT)
- Архитектура: `[CLS]` → Dropout(0.2) → Linear(768 → 50)
- Функция потерь: `BCEWithLogitsLoss` (multi-label)
- Оптимизатор: `AdamW` (lr=2e-5) + linear warmup scheduler
- 3 эпохи, batch size 16, max_length 256 токенов

---

## 📈 Результаты

| Метрика | TF-IDF + LogReg | DistilBERT | Δ |
|---|---|---|---|
| Hamming Loss ↓ | ~0.030 | ~0.018 | −40% |
| F1 Micro ↑ | ~0.68 | ~0.82 | +14pp |
| F1 Macro ↑ | ~0.55 | ~0.71 | +16pp |
| Precision Micro ↑ | ~0.72 | ~0.85 | +13pp |
| Precision@3 ↑ | ~0.74 | ~0.87 | +13pp |

> Точные значения зависят от объёма и состава данных. Цифры приведены для ~10 000 вопросов.

**Наблюдения:**
- Частые теги (`python`, `javascript`, `java`) — F1 > 0.85 у обеих моделей
- Редкие теги (`r`, `swift`, `vba`) — выигрыш DistilBERT особенно заметен
- Оптимальный порог sigmoid обычно лежит в диапазоне 0.30–0.45 (не 0.5)

---

## 🔧 Сбор данных

Данные загружаются через [StackExchange API v2.3](https://api.stackexchange.com/). Без ключа — лимит 300 запросов/день, API не отдаёт страницы глубже 25 при `sort=votes`.

**Реализованные обходы:**
- Разбивка на 20 seed-тегов (`python`, `javascript`, `java`, ...) — у каждого свои страницы 1–25
- Инкрементальное сохранение каждые 500 вопросов
- Автоматическое продолжение при повторном запуске (кэш)
- Обработка HTTP 400/429/502 без падения

Для снятия ограничений зарегистрируйте приложение на [stackapps.com](https://stackapps.com/) и вставьте ключ в `API_KEY`.

---

## 🔬 Демо-инференс

```python
demo_predict("How to handle async/await errors in FastAPI with SQLAlchemy?")

# ────────────────────────────────────────────────────────────
# ❓ Вопрос:
#    How to handle async/await errors in FastAPI with SQLAlchemy?
#
# 🏷️  Предсказанные теги:
#    python               [████████████████░░░░] 0.921
#    sqlalchemy           [██████████████░░░░░░] 0.874
#    fastapi              [█████████████░░░░░░░] 0.831
#    async-await          [██████████░░░░░░░░░░] 0.673
# ────────────────────────────────────────────────────────────
```

---

## 💡 Возможные улучшения

| Направление | Метод | Прирост |
|---|---|---|
| Больше данных | StackExchange Data Dump (XML, все вопросы) | +5–10% F1 |
| Лучшая архитектура | RoBERTa-base, DeBERTa-v3 | +2–5% F1 |
| Корреляция меток | Classifier Chains, LSAN | +1–3% F1 Macro |
| Дисбаланс классов | Focal Loss, взвешенный BCE | +2–4% F1 Macro |
| Порог per-class | Оптимизация отдельного порога для каждого тега | +1–3% |
| Понимание кода | CodeBERT / GraphCodeBERT | Специфично для SO |
| Инференс | ONNX экспорт + quantization | −60% latency |

---
