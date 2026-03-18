---
name: db migration
description: This project uses flyway for managing database schema. 
---

## About this skill

- Flyway migrations are located in `src/main/resources/db/migration/` and follow the naming convention `V<version>__<description>.sql`. 
- The application is configured with `ddl-auto: none`, so all schema changes must be made through new versioned migrations. 
- SkiSee `docs/8_FLYWAY_SCHEMA_MANAGEMENT.md` for details on migration file structure and execution.