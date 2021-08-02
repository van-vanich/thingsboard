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
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
public class ConsistentHashingWithBoundedLoadsPartitionResolver implements PartitionResolver {

    private static final String TOPIC_PREFIX = "topic";

    private Integer virtualNodesCount;

    private Map<ServiceInfo, Integer> nowInBucket;
    private Map<String, ServiceInfo> topicPartitionMapping;
    private ConcurrentNavigableMap<Long, VirtualServiceInfo> virtualNodeHash = new ConcurrentSkipListMap<>();

    public ConsistentHashingWithBoundedLoadsPartitionResolver(Integer virtualNodesCount) {
        this.virtualNodesCount = virtualNodesCount;
    }

    @Override
    public ServiceInfo resolveByPartitionIdx(List<ServiceInfo> servers, Integer partitionIdx) {
        // TODO - rename calculateTopicPartitionMapping method and move it to interface
        //  make ConsistentHashingWithBoundedLoadsPartitionResolver and RoundRobinPartitionResolver not services, but local objects

        log.info("topic-{} => {}", partitionIdx, topicPartitionMapping.get(TOPIC_PREFIX + partitionIdx));
        return topicPartitionMapping.get(TOPIC_PREFIX + partitionIdx);
    }

    @Override
    public Map<String, ServiceInfo> distributionTopicPartitionsBetweenNodes(List<ServiceInfo> nodes, int partitionSize) {
        if (nodes == null || partitionSize <= 0 || nodes.size() == 0) {
            return new HashMap<>();
        }
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < partitionSize; i++) {
            topics.add(TOPIC_PREFIX + i);
        }
        virtualNodeHash = createVirtualNodes(nodes);
        Map<String, ServiceInfo> result = searchVirtualNodesForTopics(topics, nodes.size());
        logPartitionDistribution(topics.size(), nodes.size());
        topicPartitionMapping = result;
        return result;
    }

    private void logPartitionDistribution(int countTopic, int countNode) {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (Map.Entry<ServiceInfo, Integer> entry : nowInBucket.entrySet()) {
            min = Math.min(min, entry.getValue());
            max = Math.max(max, entry.getValue());
        }
        log.info("Client count = {}, Node count = {}, min clients in node = {}, max clients in node = {}",
                countTopic, countNode, min, max);
    }

    ConcurrentSkipListMap<Long, VirtualServiceInfo> createVirtualNodes(List<ServiceInfo> nodes) {
        ConcurrentSkipListMap<Long, VirtualServiceInfo> vNodeHash = new ConcurrentSkipListMap<>();
        nowInBucket = new HashMap<>(nodes.size());
        System.out.print(virtualNodesCount);
        for (ServiceInfo serviceInfo : nodes) {
            for (int i = 0; i < virtualNodesCount; i++) {
                VirtualServiceInfo virtualNode = new VirtualServiceInfo(serviceInfo, i);
                final long hash = getHash(virtualNode);
                vNodeHash.put(hash, virtualNode);
            }
            nowInBucket.put(serviceInfo, 0);
        }
        return vNodeHash;
    }

    private Map<String, ServiceInfo> searchVirtualNodesForTopics(List<String> topics, int countNode) {
        Map<String, ServiceInfo> result = new HashMap<>(topics.size());
        int floor = topics.size() / countNode;
        int ceil = getCeil(topics.size(), countNode);
        for (int i = 0; i < topics.size(); i++) {
            ServiceInfo serviceInfo = addTopic(topics.get(i), i < floor * countNode ? floor : ceil);
            result.put(topics.get(i), serviceInfo);
            log.info("{} go to {}", topics.get(i), "service" + serviceInfo.getServiceId());
        }
        return result;
    }

    int getCeil(int cntClient, int cntNode) {
        if (cntClient <= 0 || cntNode <= 0) return -1;
        return (cntClient / cntNode) + ((cntClient % cntNode != 0) ? 1 : 0);
    }

    ServiceInfo addTopic(String topic, int limitTopicInNode) {
        long hash = getHash(topic);
        return searchVirtualServiceInfo(hash, limitTopicInNode);
    }

    long getHash(String string) {
        HashFunction hashFunction = Hashing.sha256();
        long multiply = 137;
        if (string.charAt(1) == 'o') multiply = 47;
        return hashFunction.newHasher().putBytes(string.getBytes(StandardCharsets.UTF_8)).hash().asLong() * multiply;
    }

    long getHash(VirtualServiceInfo virtualNode) {
        return getHash("" + (virtualNode.getId() * 47 + Short.MAX_VALUE) + "=VN=" + virtualNode.getServiceInfo().toString());
    }

    private ServiceInfo searchVirtualServiceInfo(long hash, int limitTopicInNode) {
        ServiceInfo serviceInfo = searchVirtualNode(hash, Long.MAX_VALUE, limitTopicInNode);
        if (serviceInfo == null) {
            serviceInfo = searchVirtualNode(Long.MIN_VALUE, hash - 1, limitTopicInNode);
        }
        return serviceInfo;
    }

    private ServiceInfo searchVirtualNode(long start, long finish, int limitTopicInNode) {
        ConcurrentNavigableMap<Long, VirtualServiceInfo> sublist = virtualNodeHash.subMap(start, true, finish, true);
        for (Map.Entry<Long, VirtualServiceInfo> entry : sublist.entrySet()) {
            ServiceInfo serviceInfo = entry.getValue().getServiceInfo();
            if (nowInBucket.get(serviceInfo) < limitTopicInNode) {
                nowInBucket.put(serviceInfo, nowInBucket.get(serviceInfo) + 1);
                return serviceInfo;
            }
        }
        return null;
    }
}
