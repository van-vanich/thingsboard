package org.thingsboard.server.queue.discovery;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
@Component
@ConditionalOnExpression("'${partitions.replace_algorithm_name:null}'=='consistent'")
public class SolveWithConsistentHashing implements PartitionResolver{

    @Getter
    private final int COPY_VIRTUAL_NODE = 200;
    private int countTopic = 0;
    private int countNode = 0;
    private Map<ServiceInfo, Integer> nowInBucket;
    private Map<String, ServiceInfo> answer;
    private List<ServiceInfo> lastServers = new ArrayList<>();
    private int lastPartitionsTotal;

    private ConcurrentSkipListMap<Long, VirtualServiceInfo> virtualNodeHash = new ConcurrentSkipListMap<>();

    @Override
    public ServiceInfo resolveByPartitionIdx(List<ServiceInfo> servers, Integer partitionIdx, int partitionsTotal) {
        if (!(servers.equals(lastServers) && partitionsTotal == lastPartitionsTotal)) {
            balancePartitionService(servers, partitionsTotal);
            lastPartitionsTotal = partitionsTotal;
            lastServers = servers;
        }
        return answer.get("topic" + partitionIdx);
    }

    public Map<String, ServiceInfo> balancePartitionService(List<ServiceInfo> nodes, int partitionSize) {

        List<String> topics = new ArrayList<>();
        for (int i=0; i<partitionSize; i++) {
            topics.add("topic" + i);
        }

        if (nodes == null) {
            return answer = new HashMap<>();
        }
        if (topics.size() == 0 || nodes.size() == 0) {
            return answer = new HashMap<>();
        }

        setCountTopicAndNode(topics.size(), nodes.size());

        virtualNodeHash = createVirtualNodes(nodes);

        log.warn("count Client = " + countTopic + ", count Node = " + countNode);

        return answer = searchVirtualNodesForTopics(topics);

    }

    private void setCountTopicAndNode(int countTopic, int countNode) {
        this.countTopic = countTopic;
        this.countNode = countNode;
    }

    ConcurrentSkipListMap<Long, VirtualServiceInfo> createVirtualNodes(List<ServiceInfo> nodes) {

        ConcurrentSkipListMap<Long, VirtualServiceInfo> vNodeHash = new ConcurrentSkipListMap<>();
        nowInBucket = new HashMap<>(countNode);
        for (ServiceInfo serviceInfo : nodes) {
            for (int i = 0; i < COPY_VIRTUAL_NODE; i++) {
                VirtualServiceInfo virtualNode = new VirtualServiceInfo(serviceInfo, i);
                final long hash = getHash(virtualNode);

                vNodeHash.put(hash, virtualNode);
            }
            nowInBucket.put(serviceInfo, 0);
        }
        return vNodeHash;
    }

    private Map<String, ServiceInfo> searchVirtualNodesForTopics(List<String> topics) {
        Map<String, ServiceInfo> answer = new HashMap<>(topics.size());
        int floor = countTopic / countNode;
        int ceil = getCeil(countTopic, countNode);
        for (int i=0; i<topics.size(); i++) {
            ServiceInfo serviceInfo = addTopic(topics.get(i), i < floor * countNode ? floor : ceil);
            answer.put(topics.get(i), serviceInfo);
        }

        return answer;
    }

    int getCeil(int cntClient, int cntNode) {

        if (cntClient <= 0 || cntNode <= 0) return -1;
        return (cntClient / cntNode) + ((cntClient % cntNode != 0) ? 1 : 0);
    }

    ServiceInfo addTopic(String topic, int limitTopicInNode) {

        long hash = getHash(topic);
        return searchVirtualServiceInfo(hash, limitTopicInNode);
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
        if (serviceInfo == null) serviceInfo = searchVirtualNode(Long.MIN_VALUE, hash - 1, limitTopicInNode);
        Objects.requireNonNull(serviceInfo, "No solution found");
        return serviceInfo;
    }
}
