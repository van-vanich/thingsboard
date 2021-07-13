package org.thingsboard.server.queue.discovery.consistent;


import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

@Slf4j
public class SolvePartServTest {
    private SolvePartServ resolver;

    @Before
    public void init() {
        resolver = new SolvePartServ();
    }

    int countDifferentState(Map<Topic, Node> pastState, Map<Topic, Node> presentState) {
        assertEquals("Maps size different", pastState.size(), presentState.size());
        int diff = 0;

        for (Map.Entry<Topic, Node> entry : pastState.entrySet()) {
            if (!entry.getValue().equals(presentState.get(entry.getKey()))) {
                diff++;
            }
        }

        log.warn("replace {} topics", diff);
        return diff;
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

    public Map<Topic, Node> testResolvePart(int topicsCount, int nodesCount) {
        List<Topic> topics = new ArrayList<>();
        List<Node> nodes = new ArrayList<>();

        for (int i = 0; i < topicsCount; i++) {
            topics.add(new Topic("topic" + i));
        }

        for (int i = 0; i < nodesCount; i++) {
            nodes.add(new Node("node" + i));
        }

        Map<Topic, Node> solution = resolver.resolvePart(nodes, topics);
        int floor = topicsCount/nodesCount;
        int ceil = floor + ((topicsCount % nodesCount > 0) ? 1 : 0);

        checkBalanced(solution, topicsCount, nodes);

        return solution;
    }

    void checkBalanced(Map<Topic, Node> solution, int topicsCount, List<Node> nodes) {

        int floor = topicsCount / nodes.size();
        int ceil = floor + ((topicsCount % nodes.size() > 0) ? 1 : 0);
        for (Node node: nodes){

            int cntTopicUseNode = 0;
            for (Map.Entry<Topic, Node> entry : solution.entrySet()) {
                if (entry.getValue().equals(node)) cntTopicUseNode++;
            }
            assertEquals(true, (cntTopicUseNode >= floor && cntTopicUseNode <= ceil));
        }
        log.warn("floor = {}, ceil = {}", floor, ceil);
    }

    @Test
    public void multiTest() {
        testResolvePart(6,6);
        testResolvePart(6,3);
        testResolvePart(5,3);


        countDifferentState(testResolvePart(6, 3), testResolvePart(6, 2));
        log.warn("optimal replace is 2 topics");


        countDifferentState(testResolvePart(30, 6), testResolvePart(30, 5));
        log.warn("optimal replace is 5 topics");

        countDifferentState(testResolvePart(100, 10), testResolvePart(100, 9));
        log.warn("optimal replace is 10 topics");


        countDifferentState(testResolvePart(100, 9), testResolvePart(100, 8));
        log.warn("optimal replace is 11-12 topics");
    }

    @Test
    public void sixTopicSixNodeUpdate() {
        List<Topic> topics = new ArrayList<>();
        List<Node> nodes = new ArrayList<>();

        for (int i = 0; i < 6; i++) {
            topics.add(new Topic("topic" + i));
        }

        for (int i = 0; i < 6; i++) {
            nodes.add(new Node("node" + i));
        }


        Map<Topic, Node> pastState = resolver.resolvePart(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);

        for (int i=0; i<6; i++) {
            Node nodeRemoved = nodes.get(i);
            nodes.remove(i);
            Map<Topic, Node> presentState = resolver.resolvePart(nodes, topics);

            countDifferentState(pastState, presentState);
            checkBalanced(presentState, topics.size(), nodes);

            pastState = presentState;
            nodes.add(i, nodeRemoved);
        }
    }

    @Test
    public void fourTopicsThreeNodes() {
        List<Topic> topics = new ArrayList<>();
        List<Node> nodes = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            topics.add(new Topic("topic" + i));
        }

        for (int i = 0; i < 3; i++) {
            nodes.add(new Node("node" + i));
        }

        Map<Topic, Node> state = resolver.resolvePart(nodes, topics);
        checkBalanced(state, topics.size(), nodes);
    }

    @Test
    public void twentyNodeToFiveNode() {
        List<Topic> topics = new ArrayList<>();
        List<Node> nodes = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            topics.add(new Topic("topic" + i));
        }

        for (int i = 0; i < 20; i++) {
            nodes.add(new Node("node" + i));
        }

        Map<Topic, Node> pastState = resolver.resolvePart(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);

        for (int i = 0; i < 15; i++) {
            nodes.remove(nodes.size() - 1);
        }

        Map<Topic, Node> presentState = resolver.resolvePart(nodes, topics);
        checkBalanced(presentState, topics.size(), nodes);
        countDifferentState(pastState, presentState);

    }

    @Test
    public void turnOffAllOddNodes() {
        List<Topic> topics = new ArrayList<>();
        List<Node> nodes = new ArrayList<>();

        for (int i = 0; i < 25; i++) {
            topics.add(new Topic("topic" + i));
        }

        for (int i = 0; i < 20; i++) {
            nodes.add(new Node("node" + i));
        }

        Map<Topic, Node> pastState = resolver.resolvePart(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);

        for (int i = 0; i < 10; i++) {
            nodes.remove(i);
        }

        Map<Topic, Node> presentState = resolver.resolvePart(nodes, topics);
        checkBalanced(presentState, topics.size(), nodes);
        countDifferentState(pastState, presentState);
    }

    @Test
    public void deleteTwoNodes() {
        List<Topic> topics = new ArrayList<>();
        List<Node> nodes = new ArrayList<>();

        for (int i = 0; i < 6; i++) {
            topics.add(new Topic("topic" + i));
        }

        for (int i = 0; i < 6; i++) {
            nodes.add(new Node("node" + i));
        }


        Map<Topic, Node> pastState = resolver.resolvePart(nodes, topics);
        checkBalanced(pastState, topics.size(), nodes);

        nodes.remove(4);
        nodes.remove(1);

        Map<Topic, Node> presentState = resolver.resolvePart(nodes, topics);
        countDifferentState(pastState, presentState);
        checkBalanced(presentState, topics.size(), nodes);
    }


}
