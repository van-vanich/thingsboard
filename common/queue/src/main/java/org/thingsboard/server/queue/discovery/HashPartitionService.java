/**
 * Copyright © 2016-2021 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.queue.discovery;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.queue.ServiceQueue;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;
import org.thingsboard.server.queue.settings.TbQueueRuleEngineSettings;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
@Slf4j
public class HashPartitionService implements PartitionService {

    @Value("${queue.core.topic}")
    private String coreTopic;
    @Value("${queue.core.partitions:100}")
    private Integer corePartitions;
    @Value("${queue.partitions.hash_function_name:murmur3_128}")
    private String hashFunctionName;
    private String balancerName = "consistent hashing";

    private final ApplicationEventPublisher applicationEventPublisher;
    private final TbServiceInfoProvider serviceInfoProvider;
    private final TenantRoutingInfoService tenantRoutingInfoService;
    private final TbQueueRuleEngineSettings tbQueueRuleEngineSettings;

    private ConcurrentMap<TopicPartitionInfoKey, TopicPartitionInfo> tpiCache = new ConcurrentHashMap<>();

    private Map<String, TopicPartitionInfo> tbCoreNotificationTopics = new HashMap<>();
    private Map<String, TopicPartitionInfo> tbRuleEngineNotificationTopics = new HashMap<>();

    private HashFunction hashFunction;

    private SolveWithConsistentHashing consistentHashing;
    private OldAlgo oldAlgo;

    public HashPartitionService(TbServiceInfoProvider serviceInfoProvider,
                                TenantRoutingInfoService tenantRoutingInfoService,
                                ApplicationEventPublisher applicationEventPublisher,
                                TbQueueRuleEngineSettings tbQueueRuleEngineSettings) {
        this.serviceInfoProvider = serviceInfoProvider;
        this.tenantRoutingInfoService = tenantRoutingInfoService;
        this.applicationEventPublisher = applicationEventPublisher;
        this.tbQueueRuleEngineSettings = tbQueueRuleEngineSettings;
    }

    @PostConstruct
    public void init() {
        this.hashFunction = forName(hashFunctionName);
        ConcurrentMap<ServiceQueue, String> partitionTopics = new ConcurrentHashMap<>();
        ConcurrentMap<ServiceQueue, Integer> partitionSizes = new ConcurrentHashMap<>();
        partitionSizes.put(new ServiceQueue(ServiceType.TB_CORE), corePartitions);
        partitionTopics.put(new ServiceQueue(ServiceType.TB_CORE), coreTopic);
        tbQueueRuleEngineSettings.getQueues().forEach(queueConfiguration -> {
            partitionTopics.put(new ServiceQueue(ServiceType.TB_RULE_ENGINE, queueConfiguration.getName()), queueConfiguration.getTopic());
            partitionSizes.put(new ServiceQueue(ServiceType.TB_RULE_ENGINE, queueConfiguration.getName()), queueConfiguration.getPartitions());
        });
        List<String> topics = new ArrayList<>();
        for (Map.Entry<ServiceQueue, String> entry : partitionTopics.entrySet()) {
            topics.add(entry.getValue());
        }

        this.consistentHashing = new SolveWithConsistentHashing(topics, applicationEventPublisher, tenantRoutingInfoService);
        this.oldAlgo = new OldAlgo(applicationEventPublisher, tenantRoutingInfoService, partitionTopics, partitionSizes);
    }

    @Override
    public TopicPartitionInfo resolve(ServiceType serviceType, TenantId tenantId, EntityId entityId) {
        return resolve(new ServiceQueue(serviceType), tenantId, entityId);
    }

    @Override
    public TopicPartitionInfo resolve(ServiceType serviceType, String queueName, TenantId tenantId, EntityId entityId) {
        return resolve(new ServiceQueue(serviceType, queueName), tenantId, entityId);
    }

    private TopicPartitionInfo resolve(ServiceQueue serviceQueue, TenantId tenantId, EntityId entityId) {
        int hash = hashFunction.newHasher()
                .putLong(entityId.getId().getMostSignificantBits())
                .putLong(entityId.getId().getLeastSignificantBits()).hash().asInt();
        Integer partitionSize = oldAlgo.getPartitionSizes().get(serviceQueue);
        int partition;
        if (partitionSize != null) {
            partition = Math.abs(hash % partitionSize);
        } else {
            //TODO: In 2.6/3.1 this should not happen because all Rule Engine Queues will be in the DB and we always know their partition sizes.
            partition = 0;
        }
        boolean isolatedTenant = oldAlgo.isIsolated(serviceQueue, tenantId);
        TopicPartitionInfoKey cacheKey = new TopicPartitionInfoKey(serviceQueue, isolatedTenant ? tenantId : null, partition);
        return tpiCache.computeIfAbsent(cacheKey, key -> oldAlgo.buildTopicPartitionInfo(serviceQueue, tenantId, partition));
    }

    @Override
    public synchronized void recalculatePartitions(ServiceInfo currentService, List<ServiceInfo> otherServices) {
        if (balancerName.equals("consistent hashing")) {
            otherServices.add(0, currentService);
            consistentHashing.recalculatePartitions(currentService, otherServices);
        } else {
            tpiCache.clear();
            oldAlgo.recalculatePartitions(currentService, otherServices);
        }
    }

    @Override
    public Set<String> getAllServiceIds(ServiceType serviceType) {
        Set<String> result = new HashSet<>();
        ServiceInfo current = serviceInfoProvider.getServiceInfo();
        if (current.getServiceTypesList().contains(serviceType.name())) {
            result.add(current.getServiceId());
        }
        List<ServiceInfo> currentOtherServices = oldAlgo.getCurrentOtherServices();
        if (currentOtherServices != null) {
            for (ServiceInfo serviceInfo : currentOtherServices) {
                if (serviceInfo.getServiceTypesList().contains(serviceType.name())) {
                    result.add(serviceInfo.getServiceId());
                }
            }
        }
        return result;
    }

    @Override
    public TopicPartitionInfo getNotificationsTopic(ServiceType serviceType, String serviceId) {
        switch (serviceType) {
            case TB_CORE:
                return tbCoreNotificationTopics.computeIfAbsent(serviceId,
                        id -> buildNotificationsTopicPartitionInfo(serviceType, serviceId));
            case TB_RULE_ENGINE:
                return tbRuleEngineNotificationTopics.computeIfAbsent(serviceId,
                        id -> buildNotificationsTopicPartitionInfo(serviceType, serviceId));
            default:
                return buildNotificationsTopicPartitionInfo(serviceType, serviceId);
        }
    }

    private TopicPartitionInfo buildNotificationsTopicPartitionInfo(ServiceType serviceType, String serviceId) {
        return new TopicPartitionInfo(serviceType.name().toLowerCase() + ".notifications." + serviceId, null, null, false);
    }

    @Override
    public int resolvePartitionIndex(UUID entityId, int partitions) {
        int hash = hashFunction.newHasher()
                .putLong(entityId.getMostSignificantBits())
                .putLong(entityId.getLeastSignificantBits()).hash().asInt();
        return Math.abs(hash % partitions);
    }

    public static HashFunction forName(String name) {
        switch (name) {
            case "murmur3_32":
                return Hashing.murmur3_32();
            case "murmur3_128":
                return Hashing.murmur3_128();
            case "sha256":
                return Hashing.sha256();
            default:
                throw new IllegalArgumentException("Can't find hash function with name " + name);
        }
    }

}
