# План: Triangular Arbitrage Bot (MEXC)

## Фаза 0 — Структура проекта
```
1. Создать структуру директорий → verify: дерево совпадает с рекомендованной
2. docker-compose.yml (backend + frontend) → verify: docker compose config без ошибок
3. .env.example + .gitignore → verify: .env не попадает в git
```

## Фаза 1 — MEXC WebSocket клиент
```
1. ws_client.py: подключение wss://wbs.mexc.com/ws → verify: ping/pong без ошибок
2. Батчинг подписок по 10 топиков → verify: подписка на 30 пар = 3 батча
3. Парсинг стакана (bid/ask) в OrderBook dataclass → verify: поля реально есть в ответе
4. Reconnect при обрыве → verify: логи показывают переподключение
```

## Фаза 2 — ArbitrageEngine (ядро)
```
1. models.py: Triangle(base, quote1, quote2), Opportunity(triangle, profit_pct, ts)
2. Загрузить все пары MEXC, построить граф треугольников → verify: найдены N треугольников
3. engine.py: на каждый tick стакана пересчитать bid/ask цепочки → verify: 0% комиссия, формула: sell*sell*buy vs 1.0
4. Логировать возможности > 0.02% → verify: в консоли видны сигналы
```

## Фаза 3 — WebSocket API для фронта
```
1. ws_server.py: отдавать топ-10 возможностей + latency метрики → verify: wscat подключается
2. MonitoringBus: очередь между engine и ws_server → verify: данные текут без блокировок
```

## Фаза 4 — Фронтенд (Next.js)
```
1. Таб OPPORTUNITIES: таблица треугольников с profit%, ts → verify: данные обновляются live
2. Таб LATENCY: WS Feed / REST RTT / Server→Browser, цвет по порогам → verify: все три метрики отображаются
3. tabular-nums, тултипы вниз → verify: нет дёргания колонок
```

## Фаза 5 — Деплой
```
1. Dockerfile для backend → verify: образ собирается без ошибок
2. git pull → docker compose build --no-cache → up -d → verify: логи чистые
3. REST RTT keep-alive сессия → verify: повторный запрос < 50ms в Latency таб
```

## Фаза 6 (после MVP) — ExecutionEngine
```
1. executor.py: 3 market-ордера последовательно через REST → verify: тест на маленькой сумме
2. Риск-менеджмент: если 2-й или 3-й ордер не прошёл → reverse + лог → verify: симуляция отказа
3. Статистика: возможностей/час, средняя прибыль → verify: отображается в отдельном таб
```

---

## Критические ограничения (из уроков предыдущего проекта)

- REST только для ордеров (~3-5 req/sec), всё остальное — WebSocket
- Одна постоянная `aiohttp.ClientSession` с keep-alive для ордеров
- Батчи подписок по 10 топиков максимум
- Проверить поля ответа MEXC API вручную перед кодом
- `docker compose restart` НЕ перечитывает .env — всегда `--force-recreate` или `build`
- Для публичных эндпоинтов — отдельная сессия без лишних заголовков
- `time.time()` для Unix timestamp, НЕ `asyncio.get_event_loop().time()`

## Стартовая точка

**Фаза 1 (ws_client.py)** — без рабочего WS-клиента остальное бессмысленно.
