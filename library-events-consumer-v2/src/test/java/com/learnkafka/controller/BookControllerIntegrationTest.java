    void deleteBook_notFound_shouldReturn404() {
        ResponseEntity<Void> response = restTemplate.exchange(
                "/v1/books/999", HttpMethod.DELETE, null, Void.class);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
                "/v1/books/1", HttpMethod.DELETE, null, Void.class);
    void deleteBook_shouldDeleteAndReturn204() {
        ResponseEntity<BookResponseDto> response = restTemplate.exchange(
                "/v1/books/999", HttpMethod.PUT, new HttpEntity<>(updateDto),
                BookResponseDto.class);
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;

    void updateBook_notFound_shouldReturn404() {
        ResponseEntity<BookResponseDto> response = restTemplate.exchange(
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
    void updateBook_shouldUpdateAndReturn200() {
import tools.jackson.databind.ObjectMapper;

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
    void createBook_invalidPayload_shouldReturn400() {
@SpringBootTest
@AutoConfigureMockMvc
        assertNotNull(response.getBody());
        assertEquals(10, response.getBody().bookId());
        assertEquals("Domain-Driven Design", response.getBody().bookName());
        assertNull(response.getBody().libraryEventId());
        ResponseEntity<BookResponseDto> response = restTemplate.postForEntity(
                "/v1/books", bookDto, BookResponseDto.class);
    void createBook_shouldPersistAndReturn201() {
    private MockMvc mockMvc;
        ResponseEntity<BookResponseDto> response = restTemplate.getForEntity(
                "/v1/books/999", BookResponseDto.class);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        ResponseEntity<BookResponseDto> response = restTemplate.getForEntity(
                "/v1/books/1", BookResponseDto.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(1, response.getBody().bookId());
        assertEquals("Clean Code", response.getBody().bookName());
        assertEquals("Robert C. Martin", response.getBody().bookAuthor());
        assertNotNull(response.getBody().libraryEventId());
    void getBookById_shouldReturnBook() {
    void getAllBooks_shouldReturnEmptyList() throws Exception {
        mockMvc.perform(get("/v1/books"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(0));
        ResponseEntity<List<BookResponseDto>> response = restTemplate.exchange(
                "/v1/books", HttpMethod.GET, null,
                new ParameterizedTypeReference<>() {});
    void getAllBooks_shouldReturnAllBooks() throws Exception {
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().isEmpty());
        mockMvc.perform(get("/v1/books"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
    void getBookById_shouldReturnBook() throws Exception {
package com.learnkafka.controller;

        mockMvc.perform(get("/v1/books/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bookId").value(1))
                .andExpect(jsonPath("$.bookName").value("Clean Code"))
                .andExpect(jsonPath("$.bookAuthor").value("Robert C. Martin"))
                .andExpect(jsonPath("$.libraryEventId").isNotEmpty())
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
    void getBookById_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(get("/v1/books/999"))
                .andExpect(status().isNotFound());
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
    void createBook_shouldPersistAndReturn201() throws Exception {

import java.util.List;
        mockMvc.perform(post("/v1/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(bookDto)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.bookId").value(10))
                .andExpect(jsonPath("$.bookName").value("Domain-Driven Design"))
                .andExpect(jsonPath("$.bookAuthor").value("Eric Evans"))
                .andExpect(jsonPath("$.libraryEventId").isEmpty())
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private TestRestTemplate restTemplate;
    void createBook_invalidPayload_shouldReturn400() throws Exception {
    @Autowired
    private BookRepository bookRepository;
        mockMvc.perform(post("/v1/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(bookDto)))
                .andExpect(status().isBadRequest());
    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
    void updateBook_shouldUpdateAndReturn200() throws Exception {
    }

    @Test
        mockMvc.perform(put("/v1/books/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bookName").value("Clean Code 2nd Edition"))
                .andExpect(jsonPath("$.bookAuthor").value("Robert C. Martin"));
        assertNotNull(response.getBody());
        assertTrue(response.getBody().isEmpty());
    }

    @Test
    void getAllBooks_shouldReturnAllBooks() {
    void updateBook_notFound_shouldReturn404() throws Exception {
        persistBookWithLibraryEvent(2, "Effective Java", "Joshua Bloch");

        mockMvc.perform(put("/v1/books/999")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isNotFound());
        assertEquals(2, response.getBody().size());
    }

    void deleteBook_shouldDeleteAndReturn204() throws Exception {
    void getBookById_shouldReturnBook() {
        persistBookWithLibraryEvent(1, "Clean Code", "Robert C. Martin");
        mockMvc.perform(delete("/v1/books/1"))
                .andExpect(status().isNoContent());
                "/v1/books/1", BookResponseDto.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(1, response.getBody().bookId());
        assertEquals("Clean Code", response.getBody().bookName());
    void deleteBook_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(delete("/v1/books/999"))
                .andExpect(status().isNotFound());
    void getBookById_notFound_shouldReturn404() {
        ResponseEntity<BookResponseDto> response = restTemplate.getForEntity(
                "/v1/books/999", BookResponseDto.class);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    @Test
    void createBook_shouldPersistAndReturn201() {
        BookDto bookDto = new BookDto(10, "Domain-Driven Design", "Eric Evans");

        ResponseEntity<BookResponseDto> response = restTemplate.postForEntity(
                "/v1/books", bookDto, BookResponseDto.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(10, response.getBody().bookId());
        assertEquals("Domain-Driven Design", response.getBody().bookName());
        assertNull(response.getBody().libraryEventId());
        assertTrue(bookRepository.findById(10).isPresent());
    }

    @Test
    void createBook_invalidPayload_shouldReturn400() {
        BookDto bookDto = new BookDto(null, "", "");

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/v1/books", bookDto, String.class);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void updateBook_shouldUpdateAndReturn200() {
        persistBookWithLibraryEvent(1, "Clean Code", "Robert C. Martin");
        BookDto updateDto = new BookDto(1, "Clean Code 2nd Edition", "Robert C. Martin");

        ResponseEntity<BookResponseDto> response = restTemplate.exchange(
                "/v1/books/1", HttpMethod.PUT, new HttpEntity<>(updateDto),
                BookResponseDto.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Clean Code 2nd Edition", response.getBody().bookName());

        Book updatedBook = bookRepository.findById(1).orElseThrow();
        assertEquals("Clean Code 2nd Edition", updatedBook.getBookName());
    }

    @Test
    void updateBook_notFound_shouldReturn404() {
        BookDto updateDto = new BookDto(999, "Non-existent", "Nobody");

        ResponseEntity<BookResponseDto> response = restTemplate.exchange(
                "/v1/books/999", HttpMethod.PUT, new HttpEntity<>(updateDto),
                BookResponseDto.class);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    @Test
    void deleteBook_shouldDeleteAndReturn204() {
        persistBookWithLibraryEvent(1, "Clean Code", "Robert C. Martin");

        ResponseEntity<Void> response = restTemplate.exchange(
                "/v1/books/1", HttpMethod.DELETE, null, Void.class);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        assertFalse(bookRepository.findById(1).isPresent());
    }

    @Test
    void deleteBook_notFound_shouldReturn404() {
        ResponseEntity<Void> response = restTemplate.exchange(
                "/v1/books/999", HttpMethod.DELETE, null, Void.class);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    private void persistBookWithLibraryEvent(Integer bookId, String bookName, String bookAuthor) {
        LibraryEvent libraryEvent = new LibraryEvent(null, LibraryEventType.ADD, null);
        LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

        Book book = new Book(bookId, bookName, bookAuthor);
        book.setLibraryEvent(savedEvent);
        bookRepository.save(book);
    }
}
