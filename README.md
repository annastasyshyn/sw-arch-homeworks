# Task 3 report

Три екземпляри **logging-service** (окремі контейнери), кластер **Hazelcast** (3 ноди), **PostgreSQL** для балансів у **counter-service**, **facade** випадково обирає logging і при збої переходить до наступного.

---

## Архітектура


| Компонент                 | Роль                                                                                                |
| ------------------------- | --------------------------------------------------------------------------------------------------- |
| **facade-service**        | HTTP для клієнта; `LOGGING_URLS` — список URL трьох logging; випадковий вибір + retry               |
| **logging-service-1/2/3** | Hazelcast-клієнт → Distributed Map `logs`; у консолі видно `LOGGING_INSTANCE_ID` і оброблені запити |
| **hazelcast-1/2/3**       | Сервери кластера (TCP discovery у `hazelcast/hazelcast.yaml`)                                       |
| **counter-service**       | Баланси в PostgreSQL (`DATABASE_URL`)                                                               |
| **postgres**              | Сховище для counter                                                                                 |


Клієнт працює лише з **facade** (`POST /transaction`, `GET /user/{user_id}`, `GET /accounts`). Метрики часу до logging/counter: `GET /metrics`, скидання: `POST /metrics/reset`.

---

## Запуск

```bash
git checkout micro_hazelcast
docker compose up -d --build
```

**Facade:** [http://localhost:8000](http://localhost:8000)  

## 10 POST + GET

```bash
./scripts/post_10_get.sh
```

<img src="screenshots/imagepng" alt="10 POST + GET" />

Переглянути, **який екземпляр logging** обробив запити:

```bash
docker compose logs logging-service-1 logging-service-2 logging-service-3
```

Вивід:

```
logging-service-1-1  | ...
logging-service-1-1  | [logging-service instance logging-1] Connected to Hazelcast cluster
logging-service-1-1  | ...
logging-service-1-1  | [logging-service instance logging-1] Received POST /log transaction_id=a73ba29d-a6f6-45c5-8838-064820073032 user_id=User1 amount=1
logging-service-1-1  | ...
logging-service-1-1  | [logging-service instance logging-1] Received POST /log transaction_id=ad241eaf-4a21-434f-9944-0c000b7b537b user_id=User1 amount=3
logging-service-1-1  | [logging-service instance logging-1] Received POST /log transaction_id=89a05800-a3ba-4cc7-b97b-f943d580438e user_id=User1 amount=4
logging-service-1-1  | [logging-service instance logging-1] Received POST /log transaction_id=e39f613b-953f-4d8b-a459-45f360ac94d8 user_id=User1 amount=6
logging-service-1-1  | [logging-service instance logging-1] Received POST /log transaction_id=b0909adc-c255-4344-854d-763414d6bcc1 user_id=User1 amount=9
logging-service-1-1  | ...

logging-service-2-1  | ...
logging-service-2-1  | [logging-service instance logging-2] Connected to Hazelcast cluster
logging-service-2-1  | ...
logging-service-2-1  | [logging-service instance logging-2] Received POST /log transaction_id=4605a787-7c60-4720-991c-fdf084e4a465 user_id=User1 amount=2
logging-service-2-1  | ...

logging-service-3-1  | ...
logging-service-3-1  | [logging-service instance logging-3] Connected to Hazelcast cluster
logging-service-3-1  | ...
logging-service-3-1  | [logging-service instance logging-3] Received POST /log transaction_id=de2b57ba-21cf-416f-a918-96662a6af397 user_id=User1 amount=5
logging-service-3-1  | [logging-service instance logging-3] Received POST /log transaction_id=b5df2c54-0e9b-4442-b9c4-f2cc89aa0e58 user_id=User1 amount=7
logging-service-3-1  | [logging-service instance logging-3] Received POST /log transaction_id=44c8dc64-1382-4092-a475-37ccad41af3a user_id=User1 amount=8
logging-service-3-1  | [logging-service instance logging-3] Received POST /log transaction_id=50f7bd8b-9a19-4a9f-bc29-b1d6acde549f user_id=User1 amount=10
logging-service-3-1  | ...
```

*Вивід урізано для зручності та читабельності.*

---

## Відмовостійкість (для протоколу)

1. 

```bash
# Вимкнути 1–2 logging
docker compose stop logging-service-2 logging-service-3
```

```
curl -sS -X POST "http://localhost:8000/transaction" -H "Content-Type: application/json" -d '{"user_id":"User1","amount":1}' && echo && curl -sS "http://localhost:8000/user/User1" && echo && curl -sS "http://localhost:8000/accounts"
```

<img src="screenshots/image1.png" alt="10 POST + GET" />

POST /transaction успішний, адже повернувся transaction_id і новий balance: 57.
Обидва GET теж успішні:
GET /user/User1 повертає актуальний баланс 57 і список транзакцій (включно з новою f42a2c0a-...). GET /accounts повертає {"balances":{"User1":57}}.
Отже, система продовжує коректно працювати через facade навіть при відключенні двох logging-service.

```
docker compose start logging-service-2 logging-service-3
```

2. 

```
# Вимкнути 1-2 ноди Hazelcast
docker compose stop hazelcast-2 hazelcast-3
```
```
curl -sS -X POST "http://localhost:8000/transaction" -H "Content-Type: application/json" -d '{"user_id":"User1","amount":1}' && echo && curl -sS "http://localhost:8000/user/User1" && echo && curl -sS "http://localhost:8000/accounts"
```

<img src="screenshots/image2.png" alt="10 POST + GET" />

POST /transaction успішний:{"transaction_id":"3808635e-a707-4a83-92d6-ef04f7ecc537","balance":58}
GET /user/User1 успішний, повертає balance: 58
є щойно створена транзакція 3808635e-a707-4a83-92d6-ef04f7ecc537
GET /accounts успішний, повертає {"balances":{"User1":58}}

```
docker compose start hazelcast-2 hazelcast-3
```

---

## Тест продуктивності та порівняння

Той самий клієнт, що й у базовій лабі (`test_client.py`), два сценарії:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python test_client.py --scenario independent --clients 10 --requests 10000
python test_client.py --scenario conflict --clients 10 --requests 10000
curl -s http://localhost:8000/metrics | python3 -m json.tool
```

|---|---|---|
| Task| Сценарій| Час (секунд)| Throughput (req/s) |
|1| Independent | 167.32 | 597.41 |
|1| Conflict | 174.86 | 571.90 |
|3| Independent | 258.11 | 387.43|
|3| Conflict | 271.26 | 368.65 |

<img src="screenshots/image3.png" />
<img src="screenshots/image4.png" />

Hazelcast-версія дала кращу доступність/стійкість і правильну архітектуру з distributed storage, але явного приросту часу не показала (що можна спостерігати в таблиці вище), що очікувано через синхронні міжсервісні виклики і мережеві накладні витрати.”
