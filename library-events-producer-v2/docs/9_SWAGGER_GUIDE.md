# Swagger / OpenAPI 3 — Why & How

<!-- TOC -->
* [Swagger / OpenAPI 3 — Why & How](#swagger--openapi-3--why--how)
  * [Why Swagger (OpenAPI) Is Better Than Plain REST Documentation](#why-swagger-openapi-is-better-than-plain-rest-documentation)
    * [1. Living Documentation, Not Stale Wiki Pages](#1-living-documentation-not-stale-wiki-pages)
    * [2. Interactive Try-It-Out UI](#2-interactive-try-it-out-ui)
    * [3. Machine-Readable Contract (`/api-docs`)](#3-machine-readable-contract-api-docs)
    * [4. Standardized Validation Visibility](#4-standardized-validation-visibility)
    * [5. Better Than Alternatives](#5-better-than-alternatives)
  * [Implementation in This Project](#implementation-in-this-project)
    * [1. Dependency (`build.gradle`)](#1-dependency-buildgradle)
    * [2. OpenAPI Bean (`OpenApiConfig.java`)](#2-openapi-bean-openapiconfigjava)
    * [3. Controller Annotations](#3-controller-annotations)
    * [4. application.yml Configuration](#4-applicationyml-configuration)
    * [5. Accessing the Docs](#5-accessing-the-docs)
  * [Disabling Swagger in Production](#disabling-swagger-in-production)
  * [Generating a Client SDK from the Spec](#generating-a-client-sdk-from-the-spec)
<!-- TOC -->

## Why Swagger (OpenAPI) Is Better Than Plain REST Documentation

### 1. Living Documentation, Not Stale Wiki Pages

Traditional API docs (Confluence pages, Word docs, READMEs) drift out of sync the moment someone changes an endpoint. OpenAPI specs are **generated from the actual code**, so the docs are always correct by definition. If the code ships, the docs reflect it.

### 2. Interactive Try-It-Out UI

Swagger UI renders every endpoint as an interactive form. Developers, QA engineers, and even product managers can:
- Execute real HTTP requests directly from the browser
- See live request/response payloads
- Explore validation constraints and enum values

No Postman collection to maintain, no curl commands to memorize.

### 3. Machine-Readable Contract (`/api-docs`)

The raw OpenAPI JSON at `/api-docs` is consumed by:
- **Code generators** — client SDKs in any language (Python, TypeScript, Go, etc.) via `openapi-generator`
- **Contract testing tools** — Pact, Dredd, or Prism can validate server/client conformance
- **API gateways** — AWS API Gateway, Kong, and Apigee import OpenAPI specs directly
- **CI pipelines** — diff the spec between commits to detect breaking changes automatically

### 4. Standardized Validation Visibility

`springdoc-openapi` reads your Jakarta Validation annotations (`@NotNull`, `@NotBlank`, `@AssertTrue`) and surfaces them in the spec. Consumers see exactly what is required without reading source code.

### 5. Better Than Alternatives

| Approach | Auto-generated | Interactive UI | Machine-readable | Type-safe |
|---|---|---|---|---|
| README / Wiki | No | No | No | No |
| Postman collection | Partial | Yes | Partial | No |
| RAML / API Blueprint | Manual | With tooling | Yes | Partial |
| **OpenAPI 3 (Swagger)** | **Yes** | **Yes** | **Yes** | **Yes** |

---

## Implementation in This Project

### 1. Dependency (`build.gradle`)

```groovy
implementation 'org.springdoc:springdoc-openapi-starter-webmvc-ui:2.8.3'
```

`springdoc-openapi-starter-webmvc-ui` bundles:
- `springdoc-openapi-starter-webmvc-api` — generates the OpenAPI spec by scanning Spring MVC components at startup
- `swagger-ui` — the embedded Swagger UI static assets served at `/swagger-ui.html`

> **Spring Boot 4.x note:** If you get a dependency resolution error, check [Maven Central](https://central.sonatype.com/artifact/org.springdoc/springdoc-openapi-starter-webmvc-ui) for the latest version compatible with Spring Boot 4.x. The springdoc team tracks each Spring Boot major version with a matching major release.

### 2. OpenAPI Bean (`OpenApiConfig.java`)

`src/main/java/com/learnkafka/config/OpenApiConfig.java` provides the top-level metadata: title, description, version, contact, license, and server URLs.

```java
@Bean
public OpenAPI libraryEventsOpenAPI() {
    return new OpenAPI()
            .info(new Info().title("Library Events Producer API").version("v1") ...)
            .servers(List.of(...));
}
```

### 3. Controller Annotations

Key OpenAPI annotations used on `LibraryEventsController`:

| Annotation | Purpose |
|---|---|
| `@Tag` | Groups all endpoints under a named section in the UI |
| `@Operation` | Describes what an endpoint does (summary + detail) |
| `@ApiResponse` / `@ApiResponses` | Documents each HTTP status code and its payload schema |
| `@Parameter` | Describes path/query parameters (name, description, required) |

### 4. application.yml Configuration

```yaml
springdoc:
  api-docs:
    path: /api-docs          # raw OpenAPI JSON
  swagger-ui:
    path: /swagger-ui.html   # interactive UI
    operationsSorter: method  # sorts endpoints by HTTP method (DELETE, GET, POST, PUT)
```

### 5. Accessing the Docs

Once the application is running:

| URL | Content |
|---|---|
| `http://localhost:8080/swagger-ui.html` | Interactive Swagger UI |
| `http://localhost:8080/api-docs` | Raw OpenAPI 3.0 JSON spec |
| `http://localhost:8080/api-docs.yaml` | Raw OpenAPI 3.0 YAML spec |

---

## Disabling Swagger in Production

Expose the UI only in non-production profiles by adding to `application-prod.yml`:

```yaml
springdoc:
  api-docs:
    enabled: false
  swagger-ui:
    enabled: false
```

This is a best practice — there is no reason to expose internal API contracts publicly in production.

---

## Generating a Client SDK from the Spec

```bash
# Install the generator CLI
brew install openapi-generator

# Generate a Java client
openapi-generator generate \
  -i http://localhost:8080/api-docs \
  -g java \
  -o ./generated-client

# Generate a TypeScript/Axios client
openapi-generator generate \
  -i http://localhost:8080/api-docs \
  -g typescript-axios \
  -o ./generated-ts-client
```
