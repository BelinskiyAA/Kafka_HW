### Описание проекта
Проект состоит из кластера кафки, ui для кафки и самого приложения собраного в отдельный образ докера. Все поднимается через docker-compose

Код приложения размещен в Practice-3. 
Проект состоит из  
- Practice-3\cmd\processor
Отвечает за блокирование и цензурирование сообщений. Обработку и сохранение блокированных пользователей, а также списка слов для цензурирования
- Practice-3\cmd\consumer
Вычитывает отфильтрованные сообщения
- Practice-3\cmd\service
Представляет собой веб-сервер, который принимает сообщения от пользователей. А также позволяет пользователю заблокировать или разблокировать того или иного пользователя.
- Practice-3\cmd\words
Представляет собой cli приложение, которое добавляет или удаляет слова в список слов для цензурирования.

### Запуск проекта

- `docker-compose build && docker-compose up -d` 
  Собираем проект и поднимаем кластер и приложение
- `docker ps`
  Проверяем, что все образы были подняты

- `docker exec -it module-2-kafka-1  kafka-topics.sh --create --bootstrap-server localhost:9092 --topic messages`
- `docker exec -it module-2-kafka-1  kafka-topics.sh --create --bootstrap-server localhost:9092 --topic filtered_messages`
- `docker exec -it module-2-kafka-1  kafka-topics.sh --create --bootstrap-server localhost:9092 --topic filtered_messages-table`
- `docker exec -it module-2-kafka-1  kafka-topics.sh --create --bootstrap-server localhost:9092 --topic blocked_users`
- `docker exec -it module-2-kafka-1  kafka-topics.sh --create --bootstrap-server localhost:9092 --topic blocked_users-group-table`
- `docker exec -it module-2-kafka-1  kafka-topics.sh --create --bootstrap-server localhost:9092 --topic blocked-words`
- `docker exec -it module-2-kafka-1  kafka-topics.sh --create --bootstrap-server localhost:9092 --topic blocked-words-group-table`
Создаем топики


##### Запуск процессора
`docker exec -it module-2-practice-1 processor`

##### Запуск consumer
`docker exec -it module-2-practice-1 consumer`

##### Запуск приложения для удаления/добавления слов для цензурирования
Указываем слово и тип действия
Добавляем в список
`docker exec -it module-2-practice-1 words -word арбуз -cmd add`
Удаляем из спсика
`docker exec -it module-2-practice-1 words -word арбуз -cmd rm`
##### Вебсервер
Вебсервер доступен по адресу [localhost:8000](http://localhost:8000)

Отправка сообщения
`curl --location 'http://localhost:8000/{USER_ID_FROM}/send' \
--header 'Content-Type: application/json' \
--data '{
  "to": {USER_ID_TO},
  "message": "{MESSAGE}"
}'`

Например:
`curl --location 'http://localhost:8000/1/send' \
--header 'Content-Type: application/json' \
--data '{
  "to": 2,
  "message": "тыква арбуз кот"
}'`

Блокировка сообщений от пользователя
`curl --location 'http://localhost:8000/{USER_ID}/block' \
--header 'Content-Type: application/json' \
--data '{
  "blocked_user_id": {BLOCKED_USER_ID}
}'`

Пример
`curl --location 'http://localhost:8000/1/block' \
--header 'Content-Type: application/json' \
--data '{
  "blocked_user_id": 2
}'`


Разблокировка сообщений от пользователя
`curl --location 'http://localhost:8000/{USER_ID}/unblock' \
--header 'Content-Type: application/json' \
--data '{
  "blocked_user_id": {UNBLOCKED_USER_ID}
}'`

Пример
`curl --location 'http://localhost:8000/1/unblock' \
--header 'Content-Type: application/json' \
--data '{
  "blocked_user_id": 2
}'`


#### Тестирование

- Поднять проект
- Создать необходимые топики
- Запустить processor и consumer

Тест 1
- Отправить сообщение через http://localhost:8000/{USER_ID_FROM}/send от пользователя 777 пользователю 555
- Убедиться в логах processor и consumer, что сообщение 'доставлено'

Тест 2
- Добавить через `words -word {СЛОВО} -cmd add`  слово в список для цензурирования
- Отправить сообщение с "запрещенным" словом через http://localhost:8000/{USER_ID_FROM}/send от пользователя 777 пользователю 555
- Убедиться в логах processor и consumer, что сообщение 'доставлено', а "запрещенное" слово заменено на ***

Тест 3
- Удалить через `words -word {СЛОВО} -cmd rm` слово из списока для цензурирования
- Отправить сообщение с "запрещенным" словом через http://localhost:8000/{USER_ID_FROM}/send от пользователя 777 пользователю 555
- Убедиться в логах processor и consumer, что сообщение 'доставлено'. Удаленное  слово не цензурируется

Тест 4
- Отправить запрос на добавление в блок пользователя 404 от пользователя 777 через http://localhost:8000/{USER_ID_FROM}/block
- Отправить сообщение через http://localhost:8000/{USER_ID_FROM}/send от пользователя 404 пользователю 777
- Убедиться в логах processor и consumer, что сообщение 'заблокировано'.
- Отправить сообщение через http://localhost:8000/{USER_ID_FROM}/send от пользователя 555 пользователю 777
- Убедиться в логах processor и consumer, что сообщение 'доставлено'.

Тест 5
- Отправить запрос на разблокирование пользователя 404 от пользователя 777 через http://localhost:8000/{USER_ID_FROM}/unblock
- Отправить сообщение через http://localhost:8000/{USER_ID_FROM}/send от пользователя 404 пользователю 777
- Убедиться в логах processor и consumer, что сообщение 'доставлено'.

Дополнительно можно проверить работу цензуры и блокировки