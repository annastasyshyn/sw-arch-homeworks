# Lab 4: Мікросервіси з Messaging Queue

Відповідні скріншоти у папці `assets/`.

## Архітектура

- `facade-service`:
  - приймає клієнтські `POST/GET`;
  - для `POST` пише лог у `logging-service` і кладе update-повідомлення в Hazelcast Queue `counter-updates`;
  - для `GET` читає баланс із `counter-service`, логи з `logging-service`.
- `counter-service`:
  - consumer Hazelcast Queue (producer/consumer схема);
  - застосовує транзакції до PostgreSQL.
- `logging-service-1/2/3`:
  - отримують `POST /log`;
  - зберігають логи в Hazelcast Distributed Map `logs`.
- `config-server`:
  - тримає in-memory registry адрес сервісів;
  - повертає список інстансів за іменем сервісу.
- `hazelcast-1/2/3`:
  - кластер MQ/Data Grid.

## Запуск

```bash
git checkout micro_mq
docker compose up -d --build
```

Перевірка контейнерів:

```bash
docker compose ps
```

## Базова перевірка: 10 POST + GET

```bash
./scripts/post_10_get.sh
```

- `POST /transaction` повертає `{"transaction_id":"...","balance":null}` (асинхронно);
- `GET /user/User1` повертає коректний баланс і список транзакцій;
- `GET /accounts` повертає коректні агреговані баланси.

## Перевірка розподілу між logging-instance

```bash
docker compose logs logging-service-1 logging-service-2 logging-service-3
```

У логах  записи `Received POST /log ...` у трьох інстансах.

## Перевірка відмовостійкості counter-service

1. Поставити `counter-service` на паузу:

```bash
docker compose pause counter-service
```

Відправити транзакції через facade:

```bash
curl -sS -X POST "http://localhost:8000/transaction" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"UserPaused","amount":11}'

curl -sS -X POST "http://localhost:8000/transaction" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"UserPaused","amount":22}'
```

Перевірити `GET` під час недоступності counter:

```bash
curl -sS "http://localhost:8000/user/UserPaused"
curl -sS "http://localhost:8000/accounts"
```

Очікувано: `balance: null`, `balances: null`.

Відновити `counter-service`:

```bash
docker compose unpause counter-service
sleep 4
```

Перевірити, що черга догнана:

```bash
curl -sS "http://localhost:8000/user/UserPaused"
docker compose logs --since 5m counter-service
```
