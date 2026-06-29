### Описание проекта
Проект состоит из трех-нодового кластера кафки, ui для кафки и самого приложения собраного в отдельный образ докера. Все поднимается через docker-compose

Код приложения размещен в Practice-6\app\cmd\. Каждая папка содержит код продьюсера или консьюмера.
Директория ca содержит сгененрированные сертификаты необходимые для настройки шифрования и ACL
### Запуск проекта

- `docker-compose build` 
  Собираем проект
- `docker-compose up -d`
  Запускаем кластер и приложение
- `docker ps`
  Проверяем, что все образы были подняты

- `Module-5\Practice-6\add_topics.sh`
Создает топики

- `Module-5\Practice-6\add_ACL.sh`
Создает ACL из задания

##### Запуск Producer
Указываем имя брокера, наименование топика
`docker exec -it practice-6-practice-1 producer kafka-1:1092 topic-1`
`docker exec -it practice-6-practice-1 producer kafka-1:1092 topic-2`

Убеждаемся, что данные отправлены 
```
Created Producer rdkafka#producer-1
Сообщение отправлено в топик topic-2 [2] оффсет 0
Сообщение отправлено в топик topic-2 [0] оффсет 0
Сообщение отправлено в топик topic-2 [0] оффсет 1
```

##### Запуск Consumer
Указываем имя брокера и наименование топика
`docker exec -it practice-6-practice-1 consumer kafka-1:1092 topic-1`

```
Консьюмер создан rdkafka#consumer-1
% Получено сообщение в топик topic-1[0]@0:
{Name:First user FavoriteNumber:256 FavoriteColor:blue}
% Заголовки: [Msg_id="msg_0"]
% Получено сообщение в топик topic-1[0]@1:
{Name:First user FavoriteNumber:525 FavoriteColor:blue}
% Заголовки: [Msg_id="msg_3"]
% Получено сообщение в топик topic-1[0]@2:
{Name:First user FavoriteNumber:963 FavoriteColor:blue}
% Заголовки: [Msg_id="msg_5"]
```

`docker exec -it practice-6-practice-1 consumer kafka-1:1092 topic-2`

Получаем ошибку доступа

```
Консьюмер создан rdkafka#consumer-1
% Error: Broker: Topic authorization failed: Subscribed topic not available: topic-2: Broker: Topic authorization failed
2026/06/29 19:58:46 % Error: Broker: Topic authorization failed: Subscribed topic not available: topic-2: Broker: Topic authorization failed
```