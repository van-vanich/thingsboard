package org.thingsboard.server.queue.discovery.consistent;


import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Slf4j
public class SolvePartServTest {


    private int countDifferentState(HashMap<Topic, Node> pastState, HashMap<Topic, Node> presentState) {
        assertEquals("Maps size different", pastState.size(), presentState.size());
        int diff = 0;

        for (Map.Entry<Topic, Node> entry : pastState.entrySet()) {
            if (!entry.getValue().equals(presentState.get(entry.getKey()))) {
                diff++;
//                log.warn("past Node = {} , present Node = {}" , entry.getValue(), presentState.get(entry.getKey()));
            }
        }

//        System.out.println("!" + diff);
        log.warn("replace {} topics", diff);
        return diff;
    }

    @Test //(expected = Exception.class)
    public void getCeil() {
        SolvePartServ resolve = new SolvePartServ();

        assertEquals(8, resolve.getCeil(54, 7));
        assertEquals(1, resolve.getCeil(1, 1));
        assertEquals(4, resolve.getCeil(12, 3));
        assertEquals(0, resolve.getCeil(0,0));

        assertThat(resolve.getCeil(1, 1), is(1));
    }


    public Map<Topic, Node> testResolvePart(int topicsCount, int nodesCount) {
//        log.warn("topicCount {} nodesCount {}", topicsCount, nodesCount);
        List<Topic> topics = new ArrayList<>();
        List<Node> nodes = new ArrayList<>();

        for (int i = 0; i < topicsCount; i++) {
            topics.add(new Topic("topic" + i));
        }

        for (int i = 0; i < nodesCount; i++) {
            nodes.add(new Node("node" + i));
        }

        SolvePartServ service = new SolvePartServ();
        Map<Topic, Node> solution = service.resolvePart(nodes, topics);
//        solution.forEach((topic, node) -> log.warn("topic {} node {}", topic, node));
        int optimalReplaceFloor = topicsCount / nodesCount;
        int optimalReplaceCeil = topicsCount / nodesCount + (topicsCount % nodesCount > 0 ? 1 : 0);

//        log.warn("optimal replace is {},{}", optimalReplaceFloor, optimalReplaceCeil);
        return solution;
    }

    @Test
    public void multiTest() {
        testResolvePart(6,6);
        testResolvePart(6,3);
        testResolvePart(5,3);


        countDifferentState((HashMap<Topic, Node>) testResolvePart(6, 3),(HashMap<Topic, Node>) testResolvePart(6, 2));
        log.warn("optimal replace is 2 topics");


        countDifferentState((HashMap<Topic, Node>) testResolvePart(30, 6),(HashMap<Topic, Node>) testResolvePart(30, 5));
        log.warn("optimal replace is 5 topics");

        countDifferentState((HashMap<Topic, Node>) testResolvePart(100, 10),(HashMap<Topic, Node>) testResolvePart(100, 9));
        log.warn("optimal replace is 10 topics");


        countDifferentState((HashMap<Topic, Node>) testResolvePart(100, 9),(HashMap<Topic, Node>) testResolvePart(100, 8));
        log.warn("optimal replace is 11-12 topics");

//        countDifferentState((HashMap<Topic, Node>) testResolvePart(100000, 10),(HashMap<Topic, Node>) testResolvePart(100000, 9));
//        log.warn("optimal replace is 10000 topics");

    }

}
