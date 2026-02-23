
# Project Plan Prompt
This app is a REST API application.
- Exposes the post and put endpoint to publish library events.
- Both of these events will publish the message to a kafka topic.
- The library event will have these props:
  - libraryEventId, Evennt type as ADD, UPDATE and Book Details.
  - Book:
    - bookId, bookName annd bookAuthor

This project will use springboot 4, java 25.

Can you please create a Product Requirements Document.