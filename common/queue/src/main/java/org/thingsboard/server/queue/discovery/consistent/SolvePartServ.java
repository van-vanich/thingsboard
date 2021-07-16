package org.thingsboard.server.queue.discovery.consistent;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
public class SolvePartServ implements PartitionService {

    private final int COPY_VIRTUAL_NODE = 200;
    private int countTopic = 0;
    private int countNode = 0;
    private Map<Node, Integer> nowInBucket;
    private ConcurrentSkipListMap<Long, VirtualNode> virtualNodeHash = new ConcurrentSkipListMap<>();

    public int getCOPY_VIRTUAL_NODE() {
        return COPY_VIRTUAL_NODE;
    }

    @Override
    public Map<Topic, Node> balancePartitionService(List<Node> nodes, List<Topic> topics) {

        if (topics == null || nodes == null) return new HashMap<>();
        if (topics.size() == 0 || nodes.size() == 0) return new HashMap<>();

        setCountTopicAndNode(topics.size(), nodes.size());

        virtualNodeHash = createVirtualNodes(nodes);


        Map<Topic, Node> answer = searchVirtualNodesForTopics(topics);


        log.warn("cnt Client = " + countTopic + ", cntNode = " + countNode);
        return answer;

    }

    private void setCountTopicAndNode(int countTopic, int countNode) {
        this.countTopic = countTopic;
        this.countNode = countNode;
    }

    public ConcurrentSkipListMap<Long, VirtualNode> createVirtualNodes(List<Node> nodes) {

        ConcurrentSkipListMap<Long, VirtualNode> vNodeHash = new ConcurrentSkipListMap<>();
        nowInBucket = new HashMap<>(countNode);
        for (Node node : nodes) {
            for (int i = 0; i < COPY_VIRTUAL_NODE; i++) {
                VirtualNode virtualNode = new VirtualNode(node, i);
                final long hash = getHash(virtualNode);

                vNodeHash.put(hash, virtualNode);
            }
            nowInBucket.put(node, 0);
        }
        return vNodeHash;
    }

    private Map<Topic, Node> searchVirtualNodesForTopics(List<Topic> topics) {
        Map<Topic, Node> answer = new HashMap<>(topics.size());
        int floor = countTopic / countNode;
        int ceil = getCeil(countTopic, countNode);
        for (int i=0; i<topics.size(); i++) {

            Node node = addTopic(topics.get(i), i < floor * countNode ? floor : ceil);
            answer.put(topics.get(i), node);
        }

        return answer;
    }

    int getCeil(int cntClient, int cntNode) {

        if (cntClient <= 0 || cntNode <= 0) return -1;
        return (cntClient / cntNode) + ((cntClient % cntNode != 0) ? 1 : 0);
    }

    public Node addTopic(Topic topic, int limitTopicInNode) {

        long hash = getHash(topic);
        return searchVirtualNode(hash, limitTopicInNode);
    }

    long getHash(Topic topic) {
        return getHash("topic_" + topic.getName());
    }

    long getHash(VirtualNode virtualNode) {
        return getHash("" + (virtualNode.getId() * 47 + Short.MAX_VALUE) + "=VN=" + virtualNode.getNode().getName());
    }


    long getHash(Object object) {
        HashFunction hashFunction = Hashing.sha256();
        long hash = hashFunction.newHasher().putBytes(object.toString().getBytes(StandardCharsets.UTF_8)).hash().asLong();
        return hash;
    }

    private Node searchVirtualNode(long hash, int limitTopicInNode) {

        Node node = searchVirtualNode(hash, Long.MAX_VALUE, limitTopicInNode);
        if (node == null) node = searchVirtualNode(Long.MIN_VALUE, hash - 1, limitTopicInNode);
        Objects.requireNonNull(node, "No solution found");
        return node;
    }


    private Node searchVirtualNode(long start, long finish, int limitTopicInNode) {
        ConcurrentNavigableMap<Long, VirtualNode> sublist = virtualNodeHash.subMap(start, true, finish, true);
        for (Map.Entry<Long, VirtualNode> entry : sublist.entrySet()) {
            Node node = entry.getValue().getNode();
            if (nowInBucket.get(node) < limitTopicInNode) {
                nowInBucket.put(node, nowInBucket.get(node) + 1);
                return node;
            }
        }
        return null;
    }


}
