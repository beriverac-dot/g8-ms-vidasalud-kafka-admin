package com.vidasalud.ms_vidasalud_kafka_admin.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TopicInfoDTO {
    private String name;
    private int partitions;
    private int replicationFactor;
    private String retentionMs;
    private String cleanupPolicy;
}
