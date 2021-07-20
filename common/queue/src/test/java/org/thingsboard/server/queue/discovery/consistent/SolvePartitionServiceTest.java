package org.thingsboard.server.queue.discovery.consistent;

import lombok.extern.slf4j.Slf4j;
import org.junit.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

@Slf4j
public class SolvePartitionServiceTest {
    private SolvePartitionService resolver;

    @Before
    public void init() {
        resolver = new SolvePartitionService();
    }

    @Test
    public void getCeil() {


        assertEquals(1, resolver.getCeil(1, 1));
        assertEquals(4, resolver.getCeil(12, 3));
        assertEquals(8, resolver.getCeil(54, 7));

    }

    @Test
    public void getCeilWithException() {

        assertEquals("div zero",-1, resolver.getCeil(0, 0));
        assertEquals(-1, resolver.getCeil(5, -5));
        assertEquals(-1, resolver.getCeil(-5, 5));
    }

    void countDifferentState(Map<Topic, Node> pastState, Map<Topic, Node> presentState,
                             int pastCountNode, int presentCountNode) {
        assertEquals("Maps size different", pastState.size(), presentState.size());
        int countReplaceTopic = 0;

        for (Map.Entry<Topic, Node> entry : pastState.entrySet()) {
            if (!entry.getValue().equals(presentState.get(entry.getKey()))) {
                countReplaceTopic++;
            }
        }

        double percentTopics = countReplaceTopic * 100D / pastState.size();
        double percentNodes = 100D * Math.abs(pastCountNode - presentCountNode) / Math.max(pastCountNode, presentCountNode);

        log.warn("replace {}% topics and {}% nodes", percentTopics, percentNodes);
    }

    Map<Topic, Node> testResolvePart(int topicsCount, int nodesCount) {
        List<Topic> topics = topicsList(topicsCount);
        List<Node> nodes = nodesList(nodesCount);

        Map<Topic, Node> solution = resolver.balancePartitionService(nodes, topics);

        checkBalanced(solution, topicsCount, nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), solution);

        return solution;
    }

    List<Topic> topicsList(int count) {
        List<Topic> answer = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            answer.add(new Topic("topic" + i));
        }
        return answer;
    }

    List<Node> nodesList(int count) {

        List<Node> answer = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            answer.add(new Node("node" + i));
        }
        return answer;
    }

    void checkBalanced(Map<Topic, Node> solution, int topicsCount, List<Node> nodes) {

        int floor = topicsCount / nodes.size();
        int ceil = floor + ((topicsCount % nodes.size() > 0) ? 1 : 0);
        for (Node node : nodes) {

            int cntTopicUseNode = 0;
            for (Map.Entry<Topic, Node> entry : solution.entrySet()) {
                if (entry.getValue().equals(node)) cntTopicUseNode++;
            }
            assertTrue("Replace not balanced", (cntTopicUseNode >= floor && cntTopicUseNode <= ceil));
        }
        log.warn("floor = {}, ceil = {}", floor, ceil);
    }

    void checkVirtualNodesBetweenTopics(List<Node> nodes, List<Topic> topics) {

        ConcurrentSkipListMap<Long, VirtualNode> virtualNodeHash = resolver.createVirtualNodes(nodes);

        List<Long> topicsHash = getTopicHash(topics);

        int average = 0;

        for (int i = 1; i < topicsHash.size(); i++) {
            long startHash = topicsHash.get(i - 1);
            long finishHash = topicsHash.get(i);
            ConcurrentNavigableMap<Long, VirtualNode> sublist =
                    virtualNodeHash.subMap(startHash, true, finishHash, true);
            int nodeBetweenTopics = getNodeBetweenTopics(sublist);
            average += nodeBetweenTopics;
        }
        average += topics.size() - 1;
        average /= topics.size();
        log.warn("average virtual node = {}", average);
        assertTrue("Virtual node has bad place on circle",average >= Math.min((nodes.size()),resolver.getCOPY_VIRTUAL_NODE() - 5) * 0.5);
    }

    private int getNodeBetweenTopics(ConcurrentNavigableMap<Long, VirtualNode> sublist) {
        HashSet<Node> nodeBetweenTopics = new HashSet<>();
        for (Map.Entry<Long, VirtualNode> entry : sublist.entrySet()) {
            nodeBetweenTopics.add(entry.getValue().getNode());
        }
        return nodeBetweenTopics.size();
    }

    private List<Long> getTopicHash(List<Topic> topics) {
        List<Long> topicsHash = new ArrayList<>();
        for (Topic topic : topics) {
            topicsHash.add(resolver.getHash(topic));
        }

        topicsHash.sort((aLong, t1) -> {
            if (aLong < t1) return -1;
            if (aLong.equals(t1)) return 0;
            return 1;
        });

        return topicsHash;
    }

    private void beforeEqualsNowAndUnique(int sizeTopicListBefore, Map<Topic, Node> now) {
        Set<Topic> topics = new HashSet<>(sizeTopicListBefore);
        for (Map.Entry<Topic, Node> entry : now.entrySet()) {
            topics.add(entry.getKey());
        }
        assertEquals(topics.size(), sizeTopicListBefore);
    }


    @Test
    public void multiTest() {
        testResolvePart(6, 6);
        testResolvePart(6, 3);
        testResolvePart(5, 3);

        countDifferentState(testResolvePart(6, 3),
                testResolvePart(6, 2), 3, 2);

        countDifferentState(testResolvePart(30, 6),
                testResolvePart(30, 5), 6, 5);

        countDifferentState(testResolvePart(100, 10),
                testResolvePart(100, 9), 10, 9);

        countDifferentState(testResolvePart(100, 9),
                testResolvePart(100, 8), 9, 8);
    }

    @Test
    public void deleteTwoNodes() {

        List<Topic> topics = topicsList(6);
        List<Node> nodes = nodesList(6);

        Map<Topic, Node> pastState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), pastState);

        nodes.remove(4);
        nodes.remove(1);

        Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);
        countDifferentState(pastState, presentState, nodes.size() + 2, nodes.size());
        checkBalanced(presentState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), presentState);
    }

    @Test
    public void deleteTwoRandomNodes() {

        List<Topic> topics = topicsList(6);
        List<Node> nodes = nodesList(6);


        Map<Topic, Node> pastState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), pastState);

        nodes.remove((int)(Math.random() * nodes.size()));
        nodes.remove((int)(Math.random() * nodes.size()));

        Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);
        countDifferentState(pastState, presentState, nodes.size() + 2, nodes.size());
        checkBalanced(presentState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), presentState);
    }

    @Test
    public void fourTopicsThreeNodes() {

        List<Topic> topics = topicsList(4);
        List<Node> nodes = nodesList(3);

        Map<Topic, Node> state = resolver.balancePartitionService(nodes, topics);
        checkBalanced(state, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), state);
    }

    @Test
    public void sixTopicSixNodeUpdate() {

        List<Topic> topics = topicsList(6);
        List<Node> nodes = nodesList(6);

        Map<Topic, Node> pastState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), pastState);

        for (int i = 0; i < 6; i++) {
            Node nodeRemoved = nodes.get(i);
            nodes.remove(i);
            Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);

            countDifferentState(pastState, presentState, nodes.size() + 1, nodes.size());
            checkBalanced(presentState, topics.size(), nodes);
            checkVirtualNodesBetweenTopics(nodes, topics);
            beforeEqualsNowAndUnique(topics.size(), presentState);

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
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), pastState);

        for (int i = 0; i < 10; i++) {
            nodes.remove(i);
        }

        Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(presentState, topics.size(), nodes);
        countDifferentState(pastState, presentState, nodes.size() + 10, nodes.size());
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), presentState);
    }

    @Test
    public void twentyNodeToFiveNode() {

        List<Topic> topics = topicsList(20);
        List<Node> nodes = nodesList(20);

        Map<Topic, Node> pastState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), pastState);

        for (int i = 0; i < 15; i++) {
            nodes.remove((int)(Math.random() * nodes.size()));
        }

        Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(presentState, topics.size(), nodes);
        countDifferentState(pastState, presentState, 20, 5);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), presentState);
    }

    @Test
    public void emptyNode() {
        List<Topic> topics = topicsList(20);
        List<Node> nodes = nodesList(0);
        resolver.balancePartitionService(nodes, topics);

    }

    @Test
    public void nullTopic() {
        List<Topic> topics = null;
        List<Node> nodes = nodesList(5);
        resolver.balancePartitionService(nodes, topics);

    }

    @Test
    public void nullNode() {
        List<Topic> topics = topicsList(5);
        List<Node> nodes = null;
        resolver.balancePartitionService(nodes, topics);

    }

    @Test
    public void hardTest() {
        List<Topic> topics = topicsList(10000);
        List<Node> nodes = nodesList(10000);
        Map<Topic, Node> state = resolver.balancePartitionService(nodes, topics);
        checkBalanced(state, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), state);

        for (int i = 0; i < 5000; i++) {
            nodes.remove((int)(nodes.size() * Math.random()));
        }

        countDifferentState(state, resolver.balancePartitionService(nodes, topics) , 10000, 5000);

    }

    @Test
    public void deleteFirstNode() {
        List<Topic> topics = topicsList(100);
        List<Node> nodes = nodesList(100);
        Map<Topic, Node> state = resolver.balancePartitionService(nodes, topics);
        checkBalanced(state, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), state);

        nodes.remove(0);

        countDifferentState(state, resolver.balancePartitionService(nodes, topics) , 100, 99);

    }

    @Test
    public void addTwoNodes(){
        List<Topic> topics = topicsList(10);
        List<Node> nodes = nodesList(4);


        Map<Topic, Node> pastState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), pastState);

        nodes.add(new Node("node4"));
        nodes.add(new Node("node5"));

        Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);
        countDifferentState(pastState, presentState, nodes.size() - 2, nodes.size());
        checkBalanced(presentState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), presentState);
    }

    @Test
    public void removeTwoAddTwoNodes() {
        List<Topic> topics = topicsList(6);
        List<Node> nodes = nodesList(6);

        Map<Topic, Node> pastState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), pastState);

        Node nodeReplaceFirst = nodes.get(2);
        Node nodeReplaceSecond = nodes.get(4);

        nodes.remove(nodeReplaceFirst);
        nodes.remove(nodeReplaceSecond);

        Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);
        countDifferentState(pastState, presentState, nodes.size() + 2, nodes.size());
        checkBalanced(presentState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), presentState);
        pastState = presentState;

        nodes.add(nodeReplaceFirst);
        nodes.add(nodeReplaceSecond);
        presentState = resolver.balancePartitionService(nodes, topics);
        countDifferentState(pastState, presentState, nodes.size() - 2, nodes.size());
        checkBalanced(presentState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), presentState);

    }

    @Test
    public void fromFiveToTwenty() {
        List<Topic> topics = topicsList(20);
        List<Node> nodes = nodesList(5);

        Map<Topic, Node> pastState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), pastState);

        for (int i=5; i<20; i++) {
            nodes.add(new Node("node" + (i)));
        }

        Map<Topic, Node> presentState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(presentState, topics.size(), nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics.size(), presentState);
        countDifferentState(pastState, presentState, nodes.size() - 15, nodes.size());

    }
}
