package com.vidasalud.ms_vidasalud_kafka_admin.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    @Value("${vidasalud.kafka.topic.citas:vidasalud.citas.eventos}")
    private String citasTopic;

    @Value("${vidasalud.kafka.topic.auditoria:vidasalud.auditoria.stream}")
    private String auditoriaTopic;

    @Bean
    public NewTopic citasEventosTopic() {
        return TopicBuilder.name(citasTopic)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000") // 7 dias
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }

    @Bean
    public NewTopic auditoriaStreamTopic() {
        return TopicBuilder.name(auditoriaTopic)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000") // 30 dias
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }
}
