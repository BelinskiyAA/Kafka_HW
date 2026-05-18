### Описание проекта
Проект состоит из трех-нодового кластера кафки, ui для кафки и самого приложения собраного в отдельный образ докера. Все поднимается через docker-compose

Код приложения размещен в practice2\cmd\. Каждая папка содержит код продьюсера или консьюмеров.
### Запуск проекта

- `docker-compose build` 
  Собираем проект
- `docker-compose up -d`
  Запускаем кластер и приложение
- `docker ps`
  Проверяем, что все образы были подняты
- `docker exec -it module-1-kafka-0-1 kafka-topics.sh --bootstrap-server localhost:9092 --create --topic m1Practice2 --partitions 3 --replication-factor 2`
Создвем топик

##### Запуск продьюсера
Указываем имя брокера, наименование топика и количество отправляемых сообщений
`docker exec -it module-1-practice-1 producer kafka-0:9092 m1Practice2 1000`

##### Запуск SingleMessageConsumer
Указываем имя брокера, группу и наименование топика
`docker exec -it module-1-practice-1 singleMessageConsumer kafka-0:9092 single_group m1Practice2`

##### Запуск BatchMessageConsumer
Указываем имя брокера, группу, количество сообщений в пачке и наименование топика
`docker exec -it module-1-practice-1 batchMessageConsumer kafka-0:9092 batch_group 10 m1Practice2`