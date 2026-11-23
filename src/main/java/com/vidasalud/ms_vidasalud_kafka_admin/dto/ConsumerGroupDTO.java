package com.vidasalud.ms_vidasalud_kafka_admin.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ConsumerGroupDTO {
    private String groupId;
    private String state;
    private int memberCount;
    private String coordinator;
}
