package com.vidasalud.ms_vidasalud_kafka_admin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class TopicRequestDTO {
    @NotBlank(message = "El nombre del topico es obligatorio")
    private String topicName;

    @Min(value = 1, message = "Debe tener al menos 1 particion")
    private int partitions = 3;

    @Min(value = 1, message = "Factor de replicacion minimo 1")
    private short replicationFactor = 1;

    private String retentionMs = "604800000";
    private String cleanupPolicy = "delete";
}
