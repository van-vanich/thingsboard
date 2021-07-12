package org.thingsboard.server.queue.discovery.consistent;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class SolvePartServ implements PartServ {

    private ConcurrentSkipListMap<Integer, VNode> vNodeHash;
    private List<Node> allNode = new ArrayList<>();
    private int cntClient = 0;
    private int cntNode = 0;

    @Override
    public Map<Topic, Node> resolvePart(List<Node> nodes, List<Topic> topics) {

        HashMap<Topic, Node> answer = new HashMap<>();

        cntClient = 0;
        this.cntNode = nodes.size();

        vNodeHash = new ConcurrentSkipListMap<>();

        for (Node node : nodes) {
            VNode vNode = new VNode(node);
            List<Integer> hash = vNode.getHashVNode();
            for (int i : hash) {
//                System.out.println(i + " " + vNode.getNode());
                vNodeHash.put(i, vNode);
            }
        }


        for (Topic topic : topics) {

            VNode vNode = addTopic(topic);
//            System.out.println(vNode.getNode().getName());

            answer.put(topic, vNode.getNode());
//            System.out.println(topic + " => " + vNode.getNode());
        }
        System.out.println("cnt Client " + cntClient + ", cntNode = " + cntNode);
        return answer;

    }

    public VNode addTopic(Topic topic) {
        cntClient++;
        int limit = (cntClient / cntNode) + ((cntClient % cntNode != 0) ? 1 : 0);
//        System.out.println(limit);

        HashFunction hash = Hashing.murmur3_32();
        int hashTopic = (hash.newHasher().putInt(cntClient).putInt(topic.hashCode()).hash().asInt() + Integer.MAX_VALUE) % Integer.MAX_VALUE ;
        return searchVNode(hashTopic, limit);
    }

    public VNode searchVNode(int hash, int limit) {
        int nowPosition;
        if (vNodeHash.higherKey(hash) == null) nowPosition = vNodeHash.higherKey(Integer.MIN_VALUE);
        else nowPosition = vNodeHash.higherKey(hash);

//        System.out.println(nowPosition);
        while (vNodeHash.higherKey(nowPosition) != null && vNodeHash.get(nowPosition).getNowInBasket() >= limit) {
            nowPosition = vNodeHash.higherKey(nowPosition);

        }

//        System.out.println(nowPosition);
        if (vNodeHash.get(nowPosition) != null && vNodeHash.get(nowPosition).getNowInBasket() >= limit)  {
            return searchVNode(Integer.MIN_VALUE, limit);

        }

        vNodeHash.get(nowPosition).addToBasket(1);

        return vNodeHash.get(nowPosition);
    }

    public int hashCode(String string) {
        long hash = 0;
        long step = 1;
        for (int i=0; i<string.length(); i++) {
            long now = string.charAt(i);
            hash = (hash + ((now * step) % Integer.MAX_VALUE) % Integer.MAX_VALUE);
            step = (int) ((step * 47l) % Integer.MAX_VALUE);
        }

        return (int) ((hash + Integer.MAX_VALUE) % Integer.MAX_VALUE);
    }
}
