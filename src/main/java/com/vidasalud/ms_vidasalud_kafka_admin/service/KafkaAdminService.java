package com.vidasalud.ms_vidasalud_kafka_admin.service;

import com.vidasalud.ms_vidasalud_kafka_admin.dto.TopicConfigUpdateDTO;
import com.vidasalud.ms_vidasalud_kafka_admin.dto.TopicRequestDTO;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class KafkaAdminService {

    private final KafkaAdmin kafkaAdmin;

    public KafkaAdminService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    private AdminClient createClient() {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    public void createTopic(TopicRequestDTO req) throws ExecutionException, InterruptedException {
        try (AdminClient client = createClient()) {
            Map<String, String> configs = new HashMap<>();
            if (req.getRetentionMs() != null && !req.getRetentionMs().isBlank()) {
                configs.put(TopicConfig.RETENTION_MS_CONFIG, req.getRetentionMs());
            }
            if (req.getCleanupPolicy() != null && !req.getCleanupPolicy().isBlank()) {
                configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, req.getCleanupPolicy());
            }
            NewTopic newTopic = new NewTopic(req.getTopicName(), req.getPartitions(), req.getReplicationFactor())
                    .configs(configs);
            client.createTopics(Collections.singletonList(newTopic)).all().get();
        }
    }

    public List> listTopics() throws ExecutionException, InterruptedException {
        try (AdminClient client = createClient()) {
            Set<String> names = client.listTopics().names().get();
            if (names.isEmpty()) return Collections.emptyList();

            Map<String, TopicDescription> descriptions = client.describeTopics(names).allTopicNames().get();
            List<ConfigResource> resources = names.stream()
                    .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name))
                    .toList();
            Map<ConfigResource, Config> configs = client.describeConfigs(resources).all().get();

            List> result = new ArrayList<>();
            for (String name : names) {
                TopicDescription desc = descriptions.get(name);
                Config cfg = configs.get(new ConfigResource(ConfigResource.Type.TOPIC, name));
                Map<String, Object> info = new LinkedHashMap<>();
                info.put("name", name);
                info.put("partitions", desc.partitions().size());
                info.put("replicationFactor", desc.partitions().isEmpty() ? 0 : desc.partitions().get(0).replicas().size());
                if (cfg != null) {
                    ConfigEntry retention = cfg.get(TopicConfig.RETENTION_MS_CONFIG);
                    ConfigEntry cleanup = cfg.get(TopicConfig.CLEANUP_POLICY_CONFIG);
                    info.put("retention.ms", retention != null ? retention.value() : "default");
                    info.put("cleanup.policy", cleanup != null ? cleanup.value() : "default");
                }
                result.add(info);
            }
            return result;
        }
    }

    public void updateTopic(String topicName, TopicConfigUpdateDTO dto) throws ExecutionException, InterruptedException {
        try (AdminClient client = createClient()) {
            if (dto.getNewPartitionCount() != null && dto.getNewPartitionCount() > 0) {
                client.createPartitions(Map.of(topicName, NewPartitions.increaseTo(dto.getNewPartitionCount()))).all().get();
            }

            List<AlterConfigOp> ops = new ArrayList<>();
            if (dto.getRetentionMs() != null && !dto.getRetentionMs().isBlank()) {
                ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, dto.getRetentionMs()), AlterConfigOp.OpType.SET));
            }
            if (dto.getCleanupPolicy() != null && !dto.getCleanupPolicy().isBlank()) {
                ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, dto.getCleanupPolicy()), AlterConfigOp.OpType.SET));
            }
            if (!ops.isEmpty()) {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
                client.incrementalAlterConfigs(Map.of(resource, ops)).all().get();
            }
        }
    }

    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        try (AdminClient client = createClient()) {
            client.deleteTopics(Collections.singletonList(topicName)).all().get();
        }
    }

    public List> listConsumerGroups() throws ExecutionException, InterruptedException {
        try (AdminClient client = createClient()) {
            Collection<ConsumerGroupListing> listings = client.listConsumerGroups().all().get();
            List<String> groupIds = listings.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
            if (groupIds.isEmpty()) return Collections.emptyList();

            Map<String, ConsumerGroupDescription> descriptions = client.describeConsumerGroups(groupIds).all().get();
            List> groups = new ArrayList<>();
            for (ConsumerGroupDescription desc : descriptions.values()) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("groupId", desc.groupId());
                item.put("state", desc.state().toString());
                item.put("memberCount", desc.members().size());
                item.put("coordinator", desc.coordinator() != null ? desc.coordinator().host() : "N/A");
                groups.add(item);
            }
            return groups;
        }
    }
}
