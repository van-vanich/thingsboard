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

    private ConcurrentSkipListMap<Long, VNode> vNodeHash;
    private int countClient = 0;
    private int countNode = 0;
    private final int COPY_VNODE = 200;
    private Map<Node, Integer> nowInBucket = new HashMap<>();


    public int getCOPY_VNODE() {
        return COPY_VNODE;
    }


    @Override
    public Map<Topic, Node> balancePartitionService(List<Node> nodes, List<Topic> topics) {


        setCountTopicAndNode(topics.size(), nodes.size());

        createVirtualNodes(nodes);

        Map<Topic, Node> answer = searchVNodesForTopics(topics);


        log.warn("cnt Client = " + countClient + ", cntNode = " + countNode);
        return answer;

    }

    private void setCountTopicAndNode(int countTopic, int countNode) {
        this.countClient = countTopic;
        this.countNode = countNode;
    }

    private void createVirtualNodes(List<Node> nodes) {

        vNodeHash = new ConcurrentSkipListMap<>();

        for (Node node : nodes) {
            for (int i = 0; i< COPY_VNODE; i++) {
                VNode vNode = new VNode(node, i);
                final long hash = getHash(vNode);

                vNodeHash.put(hash, vNode);
            }
            nowInBucket.put(node, 0);
        }
    }

    private Map<Topic, Node> searchVNodesForTopics(List<Topic> topics) {
        Map<Topic, Node> answer = new HashMap<>();
        int floor = countClient / countNode;
        int ceil = getCeil(countClient, countNode);
        for (int i=0; i<topics.size(); i++) {

            Node node = addTopic(topics.get(i), i < floor * countNode ? floor : ceil);
            answer.put(topics.get(i), node);
        }

        return answer;
    }

    public Node addTopic(Topic topic, int limitTopicInNode) {

        long hash = getHash(topic);
        return searchVNode(hash, limitTopicInNode);
    }


    int getCeil(int cntClient, int cntNode) {

        if (cntClient <= 0 || cntNode <= 0) return -1;
        return (cntClient / cntNode) + ((cntClient % cntNode != 0) ? 1 : 0);
    }


    private Node searchVNode(long hash, int limitTopicInNode) {

        Node node = searchVNode(hash, Long.MAX_VALUE, limitTopicInNode);
        if (node == null) node = searchVNode(Long.MIN_VALUE, hash - 1, limitTopicInNode);
        Objects.requireNonNull(node, "No solution found");
        return node;
    }


    private Node searchVNode(long start, long finish, int limitTopicInNode) {
        ConcurrentNavigableMap<Long, VNode> sublist = vNodeHash.subMap(start, true, finish, true);
        for (Map.Entry<Long, VNode> entry : sublist.entrySet()) {
            Node node = entry.getValue().getNode();
            if (nowInBucket.get(node) < limitTopicInNode) {
                nowInBucket.put(node, nowInBucket.get(node) + 1);
                return node;
            }
        }
        return null;
    }


    public long getHash(Topic topic) {
        return getHash("topic_" + topic.getName());
    }


    public long getHash(VNode vNode) {
        return getHash("" + (vNode.getId() * 47 + Short.MAX_VALUE) + "=VN=" + vNode.getNode().getName());
    }


    private long getHash(Object object) {
        HashFunction hashFunction = Hashing.sha256();
        long hash = hashFunction.newHasher().putBytes(object.toString().getBytes(StandardCharsets.UTF_8)).hash().asLong();

        return hash;
    }

}
