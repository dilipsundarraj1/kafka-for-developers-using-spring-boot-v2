package com.learnkafka.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI libraryEventsOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Library Events Producer API")
                        .description("REST API for publishing library events (ADD/UPDATE) to Kafka. " +
                                "Events are produced to the 'library-events' topic and consumed by downstream services.")
                        .version("v1")
                        .contact(new Contact()
                                .name("Learn Kafka")
                                .email("support@learnkafka.com"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0")))
                .servers(List.of(
                        new Server().url("http://localhost:8080").description("Local Development"),
                        new Server().url("http://kafka.prod.com:8080").description("Production")
                ));
    }
}
