package com.learnkafka.health;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component("kafkaReadiness")
public class KafkaReadinessHealthIndicator implements HealthIndicator {

    private final String bootstrapServers;

    public KafkaReadinessHealthIndicator(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public Health health() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) Duration.ofSeconds(2).toMillis());
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) Duration.ofSeconds(2).toMillis());
        config.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                Duration.ofSeconds(1).toMillis());

        try (AdminClient adminClient = AdminClient.create(config)) {
            int brokerCount = adminClient.describeCluster().nodes().get(2, TimeUnit.SECONDS).size();
            if (brokerCount == 0) {
                return Health.down()
                        .withDetail("bootstrapServers", bootstrapServers)
                        .withDetail("reason", "No brokers available")
                        .build();
            }
            return Health.up()
                    .withDetail("bootstrapServers", bootstrapServers)
                    .withDetail("brokerCount", brokerCount)
                    .build();
        } catch (Exception ex) {
            return Health.down(ex)
                    .withDetail("bootstrapServers", bootstrapServers)
                    .build();
        }
    }
}