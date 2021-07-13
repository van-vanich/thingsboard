package org.thingsboard.server.queue.discovery.consistent;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
public class SolvePartServ implements PartServ {

    private ConcurrentSkipListMap<Long, VNode> vNodeHash;
    private List<Node> allNode = new ArrayList<>();
    private int cntClient = 0;
    private int cntNode = 0;
    private final int SIZE_VNODE = 150;
    private Map<Node, Integer> nowInBucket = new HashMap<>();


    public int getSIZE_VNODE() {
        return SIZE_VNODE;
    }


    @Override
    public Map<Topic, Node> resolvePart(List<Node> nodes, List<Topic> topics) {

        HashMap<Topic, Node> answer = new HashMap<>();

        this.cntClient = topics.size();
        this.cntNode = nodes.size();

        vNodeHash = new ConcurrentSkipListMap<>();

        for (Node node : nodes) {
            for (int i=0; i<SIZE_VNODE; i++) {
                VNode vNode = new VNode(node, i);
                final long hash = getHash(vNode);

                vNodeHash.put(hash, vNode);
            }
            nowInBucket.put(node, 0);
        }


        int floor = topics.size() / nodes.size();
        int ceil = getCeil(cntClient, cntNode);
        for (int i=0; i<topics.size(); i++) {

                Node node = addTopic(topics.get(i), i < floor * nodes.size() ? floor : ceil);
                answer.put(topics.get(i), node);
        }

        log.warn("cnt Client = " + cntClient + ", cntNode = " + cntNode);
        return answer;

    }


    public Node addTopic(Topic topic, int ceil) {

        long hash = getHash(topic);
        return searchVNode(hash, ceil);
    }


    int getCeil(int cntClient, int cntNode) {

        if (cntClient <= 0 || cntNode <= 0) return -1;
        return (cntClient / cntNode) + ((cntClient % cntNode != 0) ? 1 : 0);
    }


    private Node searchVNode(long hash, int ceil) {

        Node node = searchVNode(hash, Long.MAX_VALUE, ceil);
        if (node == null) node = searchVNode(Long.MIN_VALUE, hash - 1, ceil);
        Objects.requireNonNull(node, "No solution found");
        return node;

    }


    private Node searchVNode(long start, long finish, int ceil) {
        ConcurrentNavigableMap<Long, VNode> sublist = vNodeHash.subMap(start, true, finish, true);
        for (Map.Entry<Long, VNode> entry : sublist.entrySet()) {
            Node node = entry.getValue().getNode();
            if (nowInBucket.get(node) < ceil) {
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
