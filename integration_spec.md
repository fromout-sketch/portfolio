# Интеграция ИС OMS с сервисом доставки "Достависта"

**Спецификация требований к разработке адаптера**

**Версия:** 3.0 (PlantUML)  
**Дата:** 2025-05-16  
**Автор:** [Дарья Буздалина]  

---

## Аннотация (Executive Summary)

Данный документ представляет собой полную спецификацию требований к разработке микросервиса-адаптера, интегрирующего систему управления заказами Почты России (ИС OMS) с внешним сервисом курьерской доставки «Достависта».

В рамках работы был разработан и описан полный цикл взаимодействия: от получения заказа из системы-источника, его обогащения, создания в системе Достависты, до обработки всех возможных сценариев изменения статусов (включая отмены, возвраты, назначение курьера и завершение доставки). Ключевым требованием проекта была асинхронная, отказоустойчивая передача данных через шину Kafka и REST API, а также реализация сложной статусной модели с маппингом между системами.

В документе детализированы:
- **Бизнес-сценарии:** полный жизненный цикл заказа (создание, назначение, отмена, доставка, возврат).
- **Техническая архитектура:** интеграция через очереди сообщений (Kafka) и синхронные REST API, обработка ошибок и механизм повторных попыток (retry-политика).
- **Логика преобразования данных:** детальный маппинг полей между системами для гарантии целостности данных.
- **Статусная модель:** сложная логика маппинга статусов Достависты на внутренние статусы OMS (более 20 статусов).
- **Наглядные диаграммы:** последовательности и BPMN-схемы для каждого сценария, выполненные в формате PlantUML.

Этот проект демонстрирует навыки в системном анализе, проектировании высоконагруженных интеграционных решений, глубоком понимании RESTful API и событийно-ориентированной архитектуры.

---

## 1. Введение и Назначение

### 1.1 Цель документа

Документ описывает требования к разрабатываемому сервису-адаптеру для интеграции внутренней системы управления заказами (OMS) Почты России и внешней системы курьерской доставки «Достависта» в рамках пилотного проекта по гиперлокальной доставке.

### 1.2 Назначение адаптера

Адаптер является промежуточным слоем, который обеспечивает:
- Получение событий о заказах из очереди сообщений (Kafka).
- Преобразование (маппинг) данных из внутреннего формата OMS в формат API Достависта.
- Отправку команд (создание, отмена) в Достависту через синхронный REST API.
- Прием веб-хуков (callbacks) от Достависты об изменении статусов заказов и доставок.
- Преобразование статусов Достависты во внутреннюю статусную модель OMS и отправку их обратно в систему-источник.

### 1.3 Ограничения

- Интеграция рассматривается только для заказов гиперлокальной доставки (`service_cdb_id=4, order_type_cdb_id=3`).
- Обработка события `UPDATED_BY_CLIENT` (редактирование заказа) выходит за рамки данной версии.

---

## 2. Общая архитектура и поток данных

### 2.1 Диаграмма последовательности (High-Level)

```plantuml
@startuml
title High-Level Sequence Diagram
participant "Система-источник" as Source
participant "ИС OMS" as OMS
participant "Kafka" as Kafka
participant "Адаптер Достависта" as Adapter
participant "ИС Достависта" as Dostavista
participant "БД Адаптера" as DB

Source -> OMS: Создание/отмена заказа
OMS -> Kafka: Публикация события (PASSED_FOR_EXECUTION / CANCELLED_BY_CLIENT)
Kafka -> Adapter: Вычитывание сообщения
Adapter -> DB: Сохранение/обновление заказа

alt Создание заказа
    Adapter -> Dostavista: POST /create-order
    Dostavista --> Adapter: Ответ (order_id, delivery_id)
    Adapter -> DB: Обновление (system_contractor_order_id)
    Adapter -> OMS: POST /tasks/{id}/accept (подтверждение)
else Отмена клиентом
    Adapter -> DB: Обновление статуса (canceled)
    note over Adapter: Отмена в Достависту отправляет OMS
end

Dostavista -> Adapter: Webhook (изменение статуса)
Adapter -> DB: Сохранение в delivery_history
Adapter -> OMS: POST /tasks/{id}/events (статус) /cancel (отмена)
@enduml

### 2.2 BPMN-описание процесса (Activity Diagram)

plantuml
@startuml
title BPMN-like Process Flow
start
:Получение события из Kafka;
if (Тип события?) then (PASSED_FOR_EXECUTION)
  :Создание записи в БД;
  :Маппинг данных в формат Достависты;
  :Отправка запроса create-order;
  if (Успешно?) then (Да)
    :Обновление БД (order_id, delivery_id);
    :Отправка ACCEPT в OMS;
  else (Нет)
    :Повторная попытка (retry);
    if (Попытки исчерпаны?) then (Да)
      :Отмена заказа в OMS;
    else (Нет)
      backward :Повторить отправку;
    endif
  endif
else (CANCELLED_BY_CLIENT)
  :Обновление статуса в БД на canceled;
  note right
    OMS сам отменяет заказ в Достависте
  end note
endif

partition "Веб-хук от Достависты" {
  :Получение уведомления;
  if (Тип уведомления?) then (delivery)
    :Сохранение в delivery_history;
    :Маппинг статуса в OMS-статус;
    :Отправка события в OMS (/events);
  else (order)
    :Определение типа отмены;
    :Формирование причины и флагов;
    :Отправка отмены в OMS (/cancel);
  endif
}
stop
@enduml

---

## 3. Сценарий 1: Создание заказа в Достависта

### 3.1 Предусловие
Система-источник передает в OMS событие PASSED_FOR_EXECUTION для заказа гиперлокальной доставки.

OMS публикует сообщение в Kafka-топик OMS-TYPE-ORDER_EVENTS_DOSTAVISTA.

### 3.2 Основной сценарий
Адаптер вычитывает сообщение из Kafka.

Проверяет условие: order_event_type_name = ORDER_STATUS_TRANSITION и order_status_name = PASSED_FOR_EXECUTION.

Создает запись заказа в своей БД.

Выполняет маппинг полей сообщения на структуру API Достависта (см. раздел 7).

Отправляет POST-запрос на создание заказа в Достависту (/api/business/1.5/create-order).

При успешном ответе (код 200, is_successful: true):

Обновляет запись в БД, сохраняя system_contractor_order_id, delivery_id и другие атрибуты.

Отправляет в OMS подтверждение о принятии заказа (ACCEPTED_FOR_EXECUTION_BY_CONTRACTOR) через REST API.

При неуспешном ответе:

Инициирует процесс повторных попыток (см. раздел 9).

При исчерпании попыток отменяет заказ в OMS через API.

### 3.3 Детальная диаграмма последовательности создания заказа

plantuml
@startuml
title Создание заказа в Достависта
participant "Kafka" as Kafka
participant "Адаптер" as Adapter
participant "БД" as DB
participant "Достависта" as Dostavista
participant "OMS" as OMS

Kafka -> Adapter: Сообщение (PASSED_FOR_EXECUTION)
Adapter -> Adapter: Проверка типа события
Adapter -> DB: Создание записи order (status=new)
Adapter -> Adapter: Маппинг данных
Adapter -> Dostavista: POST /create-order (matter, points, ...)

alt Успешный ответ
    Dostavista --> Adapter: 200 OK {order_id, delivery_id, ...}
    Adapter -> DB: Обновление order (system_contractor_order_id, delivery_id)
    Adapter -> OMS: POST /tasks/{task_id}/accept
    OMS --> Adapter: 200 OK
else Ошибка
    Dostavista --> Adapter: 4xx/5xx
    Adapter -> Adapter: Retry-процесс (см. раздел 9)
    alt Попытки исчерпаны
        Adapter -> OMS: POST /tasks/{task_id}/cancel
    end
end
@enduml

---

## 4. Сценарий 2: Обработка события отмены заказа клиентом (CANCELLED_BY_CLIENT)
### 4.1 Предусловие
Клиент отменяет заказ в системе-источнике.

OMS генерирует событие CANCELLED_BY_CLIENT и публикует его в Kafka.

### 4.2 Сценарий
Адаптер получает сообщение с order_status_name = CANCELLED_BY_CLIENT.

Обновляет статус заказа в своей БД (order_status = canceled).

Важно: В данной версии адаптер не отправляет команду отмены в Достависту. Этот процесс полностью делегирован на OMS, который самостоятельно отправляет запрос на отмену в Достависту, используя system_contractor_order_id. Роль адаптера — синхронизировать локальный статус.

### 4.3 Диаграмма последовательности отмены клиентом
plantuml
@startuml
title Отмена заказа клиентом
participant "Kafka" as Kafka
participant "Адаптер" as Adapter
participant "БД" as DB

Kafka -> Adapter: Сообщение (CANCELLED_BY_CLIENT)
Adapter -> Adapter: Проверка status_name
Adapter -> DB: Обновление order_status = canceled
note over Adapter: Отмена в Достависту отправляется OMS, адаптер не участвует
@enduml

---
## 5. Сценарий 3: Обработка событий от Достависты и передача статусов в OMS

###  5.1 Предусловие
В системе Достависта происходит изменение статуса заказа или доставки.

Достависта отправляет веб-хук (POST) на Callback URL адаптера.

### 5.2 Сценарий обработки статусов доставки
Адаптер получает веб-хук о статусе доставки (delivery).

Записывает полученные данные в историю доставок (delivery_history).

На основе статуса доставки (например, courier_assigned, parcel_picked_up, finished) формирует соответствующий статус для OMS (ROUTE_ASSIGNED, CARGO_PICKED_UP, CARGO_DELIVERED).

Если требуется, запрашивает у Достависты детализацию стоимости (для расчета компенсаций).

Отправляет итоговое событие в OMS через REST API (/v2.0/tasks/{id}/events).

### 5.3 Сценарий обработки статусов заказа (Отмены)
Адаптер получает веб-хук о статусе заказа (order) со статусом canceled.

Определяет тип отмены по полю status_description:

Canceled via API -> CANCELLED_BY_CLIENT. Устанавливает флаг is_payment_returnable на основе истории статусов.

Все остальные причины -> CANCELLED_BY_CONTRACTOR. Маппит status_description на внутренний код причины отмены.

Специальные статусы (Груз утерян, Заказ не был выполнен) маппятся на CARGO_LOST_BY_CONTRACTOR и EXECUTION_FAILED.

Отправляет итоговое событие в OMS.

### 5.4 Диаграмма последовательности обработки веб-хуков

plantuml
@startuml
title Обработка веб-хуков от Достависты
participant "Достависта" as Dostavista
participant "Адаптер" as Adapter
participant "БД" as DB
participant "OMS" as OMS

Dostavista -> Adapter: Webhook (delivery_changed / order_changed)
Adapter -> Adapter: Проверка подписи (X-DV-Signature)

alt Тип: delivery_changed
    Adapter -> DB: Сохранение в delivery_history
    Adapter -> Adapter: Маппинг статуса доставки -> OMS-статус
    alt Необходима детализация стоимости
        Adapter -> Dostavista: GET /receipts?order_id=...
        Dostavista --> Adapter: Данные о стоимости
    end
    Adapter -> OMS: POST /v2.0/tasks/{id}/events
else Тип: order_changed (status=canceled)
    Adapter -> Adapter: Определение типа отмены (по status_description)
    Adapter -> DB: Проверка истории статусов (для is_payment_returnable)
    Adapter -> OMS: POST /v2.0/tasks/{id}/cancel (или /orders/{id}/cancel)
end

OMS --> Adapter: 200 OK / Ошибка
alt Ошибка от OMS
    Adapter -> DB: Сохранение в retry_tab_oms
    note over Adapter: Повторные попытки (см. раздел 9)
end
@enduml

### 5.5 Пример маппинга статусов

Статус Достависты	Статус OMS
planned	EXTERNAL_ID_ASSIGNED
courier_assigned	ROUTE_ASSIGNED
courier_departed	COURIER_APPOINTED
courier_at_pickup	COURIER_ARRIVED_AT_PICKUP_PLACE
parcel_picked_up	CARGO_PICKED_UP
courier_arrived	COURIER_ARRIVED_AT_DELIVERY_PLACE
finished	CARGO_DELIVERED
return_courier_picked_up	CARGO_RETURN_INITIATED
return_finished	CARGO_RETURNED
canceled (по инициативе клиента)	CANCELLED_BY_CLIENT
canceled (по инициативе исполнителя)	CANCELLED_BY_CONTRACTOR / EXECUTION_FAILED / CARGO_LOST_BY_CONTRACTOR

---

## 6. Сценарий 4: Дополнительная функциональность — Расчет стоимости доставки

### 6.1 Назначение
Предоставить мобильному приложению Почты России возможность предварительного расчета стоимости доставки.

### 6.2 Сценарий
МП ПР -> Тарификатор -> Адаптер: Получение запроса на расчет (POST /dostavista/v1.0/tariff).

Адаптер создает запись расчета в своей БД.

Выполняет маппинг полей запроса и отправляет запрос на расчет в Достависту (POST /calculate-order).

Получает результат, сохраняет в БД и возвращает в Тарификатор.

### 6.3 Диаграмма последовательности расчета стоимости

plantuml
@startuml
title Расчет стоимости доставки
participant "Тарификатор" as Tarif
participant "Адаптер" as Adapter
participant "БД" as DB
participant "Достависта" as Dostavista

Tarif -> Adapter: POST /dostavista/v1.0/tariff (вес, адреса, ...)
Adapter -> DB: Создание записи расчета
Adapter -> Adapter: Маппинг в формат Достависты
Adapter -> Dostavista: POST /calculate-order
Dostavista --> Adapter: Ответ (стоимость)
Adapter -> DB: Обновление записи расчета (стоимость)
Adapter --> Tarif: Ответ со стоимостью
@enduml

## 7. Спецификация маппинга данных

### 7.1 Маппинг OMS -> Достависта (Создание заказа)
Поле в БД Адаптера	Источник (OMS Kafka)	Поле в API Достависта	Примечание
client_order_id	task_id	points[1].client_order_id	Уникальный идентификатор таска
subject_data	order_data.subject_data	matter	Описание груза
declared_value_data	declared_value	insurance_amount	Страховая сумма. Переводится из копеек в рубли.
total_weight_kg	weight_total	total_weight_kg	Округление в большую сторону
pickup_contact_name	pickup.contact_name	points[0].contact_person.name	Имя отправителя
pickup_contact_phone	pickup.contact_phone	points[0].contact_person.phone	Телефон отправителя
pickup_plain_address	pickup.place.address.plain_address	points[0].address	Адрес отправителя
consignee_contact_name	delivery.consignee.contact_name	points[1].contact_person.name	Имя получателя
consignee_contact_phone	delivery.consignee.contact_phone	points[1].contact_person.phone	Телефон получателя

### 7.2 Маппинг Достависта -> OMS (Статусы)

Поле из Webhook Достависта	Поле для OMS
event_datetime	contractor_executed_at
delivery.client_order_id (или order.points[2].client_order_id)	task_id (в URL запроса)
delivery.status	execution_status_name (после преобразования)
courier.name, courier.phone	event_data.courier_name, event_data.courier_phone
status_description	cancelation_reason_comment (для отмен)

---

## 8. Справочники и структура базы данных

### 8.1 Основные сущности БД Адаптера
order — основная таблица заказов:

id (UUID)

client_order_id (task_id из OMS)

system_contractor_order_id (order_id из Достависты)

order_status

subject_data

delivery_id

Данные об адресах и контактах (pickup/delivery)

delivery_history — история статусов доставок:

delivery_id

client_order_id

status

event_datetime

retry_tab_oms / retry_tab_dostavista — таблицы для повторных попыток отправки сообщений.

### 8.2 Справочник причин отмены (OMS)
Код	Описание
102	Невозможно связаться с отправителем
103	Отмена по инициативе отправителя
104	Адрес не верный
105	Не успели
106	Технические причины
107	Вес/объем превышает заявленный
108	Прочее

---

## 9. Обработка ошибок и отказоустойчивость`

### 9.1 Политика повторных попыток (Retry)
Для обеспечения надёжности доставки сообщений в системе реализован механизм повторных попыток при сбоях.

Хранение: сообщения, которые не удалось отправить, сохраняются в таблицу retry_tab с полями message, retry_count, error_code и status.

Статусы:

RETRY — попытка повторной отправки.

STORAGE — отправка прекращена (обычно после получения 4xx ошибки или исчерпания попыток).

SENT — успешно отправлено.

Алгоритм:

Количество попыток (n) и интервал (t) — конфигурируемые параметры.

Повторные отправки выполняются с экспоненциально увеличивающимся интервалом (t, 2t, 3t...).

После превышения лимита попыток (n) статус меняется на STORAGE, и инициируется процесс отмены заказа.

### 9.2 Обработка негативных ответов

От Достависты: при получении ошибки (код != 200) адаптер инициирует retry-процесс. При исчерпании попыток заказ отменяется в OMS.

От OMS: при получении ошибки адаптер сохраняет запрос в retry_tab_oms и повторяет попытки согласно политике. При неудаче заказ также отменяется в OMS.

---

## Заключение

Данная спецификация описывает надёжное и масштабируемое решение для интеграции между OMS Почты России и внешней курьерской службой «Достависта». Основные архитектурные решения (асинхронное взаимодействие через Kafka, синхронный REST API для критических операций, надёжная политика повторных попыток и гибкий маппинг данных) обеспечивают высокую отказоустойчивость системы и точность передачи статусов заказов в рамках пилотного проекта гиперлокальной доставки.

Прилагаемые диаграммы последовательности и BPMN-схема, выполненные в PlantUML, наглядно иллюстрируют все ключевые потоки, что делает документ полноценным артефактом для портфолио.