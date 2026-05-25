package com.learnkafka.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    OpenAPI libraryEventsOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Library Events Consumer API")
                        .description("API contracts for library event and book endpoints")
                        .version("v1")
                        .contact(new Contact().name("Library Events Team")));
    }
}

