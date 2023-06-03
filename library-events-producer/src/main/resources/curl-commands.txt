POST WITH-NULL-LIBRARY-EVENT-ID
---------------------
curl -i \
-d '{"libraryEventId":null,"libraryEventType": "NEW","book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}' \
-H "Content-Type: application/json" \
-X POST http://localhost:8080/v1/libraryevent

PUT WITH ID - 1
--------------
curl -i \
-d '{"libraryEventId":1,"libraryEventType": "UPDATE","book":{"bookId":456,"bookName":"Kafka Using Spring Boot 2.X","bookAuthor":"Dilip"}}' \
-H "Content-Type: application/json" \
-X PUT http://localhost:8080/v1/libraryevent

curl -i \
-d '{"libraryEventId":2,"libraryEventType": "UPDATE","book":{"bookId":456,"bookName":"Kafka Using Spring Boot 2.X","bookAuthor":"Dilip"}}' \
-H "Content-Type: application/json" \
-X PUT http://localhost:8080/v1/libraryevent



PUT WITH ID
---------------------
curl -i \
-d '{"libraryEventId":123,"libraryEventType": "UPDATE","book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}' \
-H "Content-Type: application/json" \
-X PUT http://localhost:8080/v1/libraryevent

curl -i \
-d '{"libraryEventId":999,"libraryEventType": "UPDATE","book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}' \
-H "Content-Type: application/json" \
-X PUT http://localhost:8080/v1/libraryevent

curl -i \
-d '{"libraryEventId":2,"libraryEventType": "UPDATE","book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}' \
-H "Content-Type: application/json" \
-X PUT http://localhost:8080/v1/libraryevent


PUT WITHOUT ID
---------------------
curl -i \
-d '{"libraryEventId":null,"libraryEventType": "UPDATE","book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}' \
-H "Content-Type: application/json" \
-X PUT http://localhost:8080/v1/libraryevent


./kafka-topics.sh --create --topic library-events.DLT --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
