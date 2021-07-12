package org.thingsboard.server.queue.discovery.consistent;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
public class SolvePartServ implements PartServ{

    private ConcurrentSkipListMap<Integer, org.algo.VNode> vNodeHash;
    private List<org.algo.Node> allNode = new ArrayList<>();
    private int cntClient = 0;
    private int cntNode = 0;
    @Override
    public Map<org.algo.Topic, org.algo.Node> resolvePart(List<org.algo.Node> nodes, List<org.algo.Topic> topics) {

        HashMap<org.algo.Topic, org.algo.Node> answer = new HashMap<>();

        cntClient = 0;
        this.cntNode = nodes.size();

        for (org.algo.Node node : nodes) {
            org.algo.VNode vNode = new org.algo.VNode(node);
            List<Integer> hash = vNode.getHashVNode();
            for (int i : hash) {
                vNodeHash.put(i, vNode);
            }
        }


        for (org.algo.Topic topic : topics) {

            org.algo.VNode vNode = addTopic(topic);
            System.out.println(vNode.getNode().getName());
            log.error("lkjhbkjhbjkh");
            answer.put(topic, vNode.getNode());
            System.out.println(topic + " => " + vNode.getNode());
        }
        System.out.println("cnt Client " + cntClient + ", cntNode = " + cntNode);
        return answer;

    }

    public org.algo.VNode addTopic(org.algo.Topic topic) {
        cntClient++;
        int limit = (cntClient / cntNode) + ((cntClient % cntNode != 0) ? 1 : 0);
        System.out.println(limit);
        return searchVNode(hashCode(topic.getName()), limit);
    }

    public org.algo.VNode searchVNode(int hash, int limit) {
        int nowPosition = vNodeHash.higherKey(hash);
        while (vNodeHash.higherKey(nowPosition) != null && vNodeHash.get(nowPosition).getNowInBasket() >= limit) {
            nowPosition = vNodeHash.higherKey(nowPosition);

        }

        if (vNodeHash.get(nowPosition) != null && vNodeHash.get(nowPosition).getNowInBasket() >= limit)  return searchVNode(-1, limit);

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
