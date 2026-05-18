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

##### Запуск продьюсера
Указываем имя брокера, наименование топика и количество отправляемых сообщений
`docker exec -it module-1-practice-1 producer kafka-0:9092 module1_practice2 100`

##### Запуск SingleMessageConsumer
Указываем имя брокера, группу и наименование топика
`docker exec -it module-1-practice-1 singleMessageConsumer kafka-0:9092 single_group module1_practice2`

##### Запуск BatchMessageConsumer
Указываем имя брокера, группу, количество сообщений в пачке и наименование топика
`docker exec -it module-1-practice-1 batchMessageConsumer kafka-0:9092 batch_group 10 module1_practice2`