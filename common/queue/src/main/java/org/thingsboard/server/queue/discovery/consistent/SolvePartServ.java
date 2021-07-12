package org.thingsboard.server.queue.discovery.consistent;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SolvePartServ implements PartServ {

    private ConcurrentSkipListMap<Integer, VNode> vNodeHash;
    private List<Node> allNode = new ArrayList<>();
    private int cntClient = 0;
    private int cntNode = 0;
    private final int SIZE_VNODE = 200;
    private Map<Node, Integer> nowInBucket = new HashMap<>();

    @Override
    public Map<Topic, Node> resolvePart(List<Node> nodes, List<Topic> topics) {

        HashMap<Topic, Node> answer = new HashMap<>();

        cntClient = 0;
        this.cntNode = nodes.size();

        vNodeHash = new ConcurrentSkipListMap<>();

        for (Node node : nodes) {
            for (int i=0; i<SIZE_VNODE; i++) {
                VNode vNode = new VNode(node, i);
                vNodeHash.put(getHash(vNode), vNode);
            }
            nowInBucket.put(node, 0);
        }


        for (Topic topic : topics) {
            Node node = addTopic(topic);
            answer.put(topic, node);
        }
        System.out.println("cnt Client " + cntClient + ", cntNode = " + cntNode);
        return answer;

    }


    public Node addTopic(Topic topic) {
        cntClient++;
        int ceil = getCeil(cntClient, cntNode);
        return searchVNode(getHash(topic), ceil);
    }

    int getCeil(int cntClient, int cntNode) {
        if (cntNode == 0) return 0;
        return (cntClient / cntNode) + ((cntClient % cntNode != 0) ? 1 : 0);
    }

    private Node searchVNode(int hash, int ceil) {

        Node node = searchVNode(hash, Integer.MAX_VALUE, ceil);
        if (node == null) node = searchVNode(Integer.MIN_VALUE, hash - 1, ceil);
        Objects.requireNonNull(node, "No solution found");
        return node;

    }

    private Node searchVNode(int start, int finish, int ceil) {
        ConcurrentNavigableMap<Integer, VNode> sublist = vNodeHash.subMap(start, true, finish, true);
        for (Map.Entry<Integer, VNode> entry : sublist.entrySet()) {
            Node node = entry.getValue().getNode();
            if (nowInBucket.get(node) < ceil) {
                nowInBucket.put(node, nowInBucket.get(node) + 1);
                return node;
            }
        }
        return null;
    }


    private int getHash(Object object) {
        HashFunction hashFunction = Hashing.murmur3_32();
        return hashFunction.newHasher().putBytes(object.toString().getBytes(Charset.forName("UTF-8"))).hash().asInt();
    }

//    public int hashCode(String string) {
//        long hash = 0;
//        long step = 1;
//        for (int i=0; i<string.length(); i++) {
//            long now = string.charAt(i);
//            hash = (hash + ((now * step) % Integer.MAX_VALUE) % Integer.MAX_VALUE);
//            step = (int) ((step * 47l) % Integer.MAX_VALUE);
//        }
//
//        return (int) ((hash + Integer.MAX_VALUE) % Integer.MAX_VALUE);
//    }
}
