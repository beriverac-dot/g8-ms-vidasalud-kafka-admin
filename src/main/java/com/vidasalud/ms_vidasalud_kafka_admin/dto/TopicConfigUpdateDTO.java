package com.vidasalud.ms_vidasalud_kafka_admin.dto;

import lombok.Data;

@Data
public class TopicConfigUpdateDTO {
    private Integer newPartitionCount;
    private String retentionMs;
    private String cleanupPolicy;
}
