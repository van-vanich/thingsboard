package org.thingsboard.server.queue.discovery.consistent;


import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class SolvePartitionServiceTest {
    private SolvePartServ resolver;

    @Before
    public void init() {
        resolver = new SolvePartServ();
    }

    @Test
    public void getCeil() {


        assertEquals(1, resolver.getCeil(1, 1));
        assertEquals(4, resolver.getCeil(12, 3));
        assertEquals(8, resolver.getCeil(54, 7));

    }

    @Test//(expected = Exception.class)
    public void getCeilWithException(){

        assertEquals(-1, resolver.getCeil(0,0));
        assertEquals(-1, resolver.getCeil(5, -5));
        assertEquals(-1, resolver.getCeil(-5, 5));
    }

    void countDifferentState(Map<Topic, Node> pastState, Map<Topic, Node> presentState, int pastCountNode, int presentCountNode) {
        assertEquals("Maps size different", pastState.size(), presentState.size());
        int diff = 0;

        for (Map.Entry<Topic, Node> entry : pastState.entrySet()) {
            if (!entry.getValue().equals(presentState.get(entry.getKey()))) {
                diff++;
            }
        }

        int percentTopics = diff * 100 / pastState.size();
        int percentNodes = 100 * Math.abs(pastCountNode - presentCountNode) / Math.max(pastCountNode, presentCountNode);

        log.warn("replace {}% topics and {}% nodes", percentTopics, percentNodes);
    }

    public Map<Topic, Node> testResolvePart(int topicsCount, int nodesCount) {
        List<Topic> topics = topicsList(topicsCount);
        List<Node> nodes = nodesList(nodesCount);

        for (int i = 0; i < nodesCount; i++) {
            nodes.add(new Node("node" + i));
        }

        Map<Topic, Node> solution = resolver.balancePartitionService(nodes, topics);

        checkBalanced(solution, topicsCount, nodes);
        checkVirtualNodesBetweenTopics(nodes, topics, resolver.getCOPY_VNODE());

        return solution;
    }

    private List<Topic> topicsList(int count) {
        List<Topic> answer = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            answer.add(new Topic("topic" + i));
        }
        return answer;
    }

    private List<Node> nodesList(int count) {

        List<Node> answer = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            answer.add(new Node("node" + i));
        }
        return answer;
    }

    void checkBalanced(Map<Topic, Node> solution, int topicsCount, List<Node> nodes) {

        int floor = topicsCount / nodes.size();
        int ceil = floor + ((topicsCount % nodes.size() > 0) ? 1 : 0);
        for (Node node: nodes){

            int cntTopicUseNode = 0;
            for (Map.Entry<Topic, Node> entry : solution.entrySet()) {
                if (entry.getValue().equals(node)) cntTopicUseNode++;
            }
            assertTrue((cntTopicUseNode >= floor && cntTopicUseNode <= ceil));
        }
        log.warn("floor = {}, ceil = {}", floor, ceil);
    }

    void checkVirtualNodesBetweenTopics(List<Node> nodes, List<Topic> topics, int SIZE_VNODE) {

        ConcurrentSkipListMap<Long, VNode> vNodeHash = resolver.createVirtualNodes(nodes);

        List<Long> topicsHash = getTopicHash(topics);

        for (int i=1; i<topicsHash.size(); i++) {
            long startHash = topicsHash.get(i - 1);
            long finishHash = topicsHash.get(i);
            ConcurrentNavigableMap<Long, VNode> sublist = vNodeHash.subMap(startHash, true, finishHash, true);
            HashSet<Node> nodeBetweenTopics = new HashSet<>();
            for (Map.Entry<Long, VNode> entry: sublist.entrySet()) {
                nodeBetweenTopics.add(entry.getValue().getNode());
            }
            log.warn("nodes between topics = {}, need = {}" , nodeBetweenTopics.size(), nodes.size());
            assertTrue( nodeBetweenTopics.size() + (nodes.size() / 2) >= nodes.size());
        }
    }

    public List<Long> getTopicHash(List<Topic> topics) {
        List<Long> topicsHash = new ArrayList<>();
        for (Topic topic: topics) {
            topicsHash.add(resolver.getHash(topic));
        }

        topicsHash.sort((aLong, t1) -> {
            if (aLong < t1) return -1;
            if (aLong == t1) return 0;
            return 1;
        });

        for (Long hash: topicsHash) {
            System.out.println(hash);
        }
        return topicsHash;
    }

    @Test
    public void multiTest() {
        testResolvePart(6,6);
        testResolvePart(6,3);
        testResolvePart(5,3);


        countDifferentState(testResolvePart(6, 3), testResolvePart(6, 2), 3 , 2);
        log.warn("optimal replace is 2 topics");


        countDifferentState(testResolvePart(30, 6), testResolvePart(30, 5), 6 , 5);
        log.warn("optimal replace is 5 topics");

        countDifferentState(testResolvePart(100, 10), testResolvePart(100, 9), 10, 9);
        log.warn("optimal replace is 10 topics");


        countDifferentState(testResolvePart(100, 9), testResolvePart(100, 8), 9, 8);
        log.warn("optimal replace is 11-12 topics");
    }

    @Test
    public void deleteTwoNodes() {

        List<Topic> topics = topicsList(6);
        List<Node> nodes = nodesList(6);


        Map<Topic, Node> pastState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics, resolver.getCOPY_VNODE());

        nodes.remove(4);
        nodes.remove(1);

        Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);
        countDifferentState(pastState, presentState, nodes.size() + 2, nodes.size());
        checkBalanced(presentState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics, resolver.getCOPY_VNODE());
    }

    @Test
    public void fourTopicsThreeNodes() {

        List<Topic> topics = topicsList(4);
        List<Node> nodes = nodesList(3);

        Map<Topic, Node> state = resolver.balancePartitionService(nodes, topics);
        checkBalanced(state, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics, resolver.getCOPY_VNODE());
    }

    @Test
    public void sixTopicSixNodeUpdate() {

        List<Topic> topics = topicsList(6);
        List<Node> nodes = nodesList(6);

        Map<Topic, Node> pastState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics, resolver.getCOPY_VNODE());

        for (int i=0; i<6; i++) {
            Node nodeRemoved = nodes.get(i);
            nodes.remove(i);
            Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);

            countDifferentState(pastState, presentState, nodes.size() + 1, nodes.size());
            checkBalanced(presentState, topics.size(), nodes);
            checkVirtualNodesBetweenTopics(nodes, topics, resolver.getCOPY_VNODE());

            pastState = presentState;
            nodes.add(i, nodeRemoved);
        }
    }

    @Test
    public void turnOffAllOddNodes() {

        List<Topic> topics = topicsList(25);
        List<Node> nodes = nodesList(20);

        Map<Topic, Node> pastState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics, resolver.getCOPY_VNODE());

        for (int i = 0; i < 10; i++) {
            nodes.remove(i);
        }

        Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(presentState, topics.size(), nodes);
        countDifferentState(pastState, presentState, nodes.size() + 10, nodes.size());
        checkVirtualNodesBetweenTopics(nodes, topics, resolver.getCOPY_VNODE());
    }

    @Test
    public void twentyNodeToFiveNode() {

        List<Topic> topics = topicsList(20);
        List<Node> nodes = nodesList(20);

        Map<Topic, Node> pastState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics, resolver.getCOPY_VNODE());

        for (int i = 0; i < 15; i++) {
            nodes.remove(nodes.size() - 1);
        }

        Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(presentState, topics.size(), nodes);
        countDifferentState(pastState, presentState, 20, 5);
        checkVirtualNodesBetweenTopics(nodes, topics, resolver.getCOPY_VNODE());

    }

}
