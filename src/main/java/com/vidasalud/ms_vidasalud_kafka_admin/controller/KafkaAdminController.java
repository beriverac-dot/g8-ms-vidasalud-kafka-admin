package com.vidasalud.ms_vidasalud_kafka_admin.controller;

import com.vidasalud.ms_vidasalud_kafka_admin.dto.TopicConfigUpdateDTO;
import com.vidasalud.ms_vidasalud_kafka_admin.dto.TopicRequestDTO;
import com.vidasalud.ms_vidasalud_kafka_admin.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @GetMapping("/topics")
    public ResponseEntity<List>> listTopics() throws Exception {
        return ResponseEntity.ok(kafkaAdminService.listTopics());
    }

    @PostMapping("/topics")
    public ResponseEntity<String> createTopic(@Valid @RequestBody TopicRequestDTO request) throws Exception {
        kafkaAdminService.createTopic(request);
        return ResponseEntity.ok("Topico '" + request.getTopicName() + "' creado correctamente con " + request.getPartitions() + " particiones.");
    }

    @PutMapping("/topics/{name}/config")
    public ResponseEntity<String> updateTopic(@PathVariable String name, @RequestBody TopicConfigUpdateDTO request) throws Exception {
        kafkaAdminService.updateTopic(name, request);
        return ResponseEntity.ok("Configuracion del topico '" + name + "' actualizada correctamente.");
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<String> deleteTopic(@PathVariable String name) throws Exception {
        kafkaAdminService.deleteTopic(name);
        return ResponseEntity.ok("Topico '" + name + "' eliminado correctamente.");
    }

    @GetMapping("/consumer-groups")
    public ResponseEntity<List>> listConsumerGroups() throws Exception {
        return ResponseEntity.ok(kafkaAdminService.listConsumerGroups());
    }
}
