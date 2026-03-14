# --- Этап 1: Сборка (Build) ---
FROM gradle:8.5-jdk21 AS build
WORKDIR /app

# Копируем только файлы конфигурации Gradle для кэширования зависимостей
COPY build.gradle settings.gradle ./
# Копируем исходный код
COPY src ./src

# Собираем проект, пропуская тесты (их лучше гонять в CI отдельно)
RUN gradle clean build -x test

# --- Этап 2: Запуск (Runtime) ---
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Копируем только исполняемый JAR-файл из этапа сборки
# Обычно Spring Boot создает JAR в build/libs/
COPY --from=build /app/build/libs/*.jar app.jar

# Запускаем приложение
ENTRYPOINT ["java", "-jar", "app.jar"]