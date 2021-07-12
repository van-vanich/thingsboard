package org.thingsboard.server.queue.discovery.consistent;


import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Slf4j
public class SolvePartServTest {

//    public static final String TOPIC = "topic";
//    private String alphabet = "qwertyuiopasdfghjklzxcvbnm";
//
//    private SolvePartServ loadBalancer = new SolvePartServ();
//
//    @Test
//    public void oneNode(){
//        List<Node> nodes = new ArrayList<>();
//        List<Topic> topics = generateTopics(10);
//        nodes.add(new Node("node1 "));
//
//
//        HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
//        int floor = topics.size() / nodes.size();
//        int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
////        assertEquals(true, checkAnswer(answer, floor, ceil));
//
//    }
//
//    private List<Topic> generateTopics(int max) {
//        List<Topic> topics = new ArrayList<>(max);
//        for (int i=0; i<max; i++) {
//            topics.add(new Topic(TOPIC + i));
//        }
//        return topics;
//    }
//
//    @Test
//    public void nodesInOneLine(){
//        List<Node> nodes = new ArrayList<>();
//        List<Topic> topics = new ArrayList<>();
//
//        String[] name = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
//        for (int i=0; i<10; i++) {
//            topics.add(new Topic(name[i]));
//        }
//
//        HashMap<Topic, Node> last = new HashMap<>();
//        for (int i=0; i<10; i++) {
//            System.out.println("-------------------------------------");
//            System.out.println(i);
//            nodes.add(new Node("node" + i));
//            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
//            if (i > 0) System.out.println("replace == " + countDifferentState(last, answer));
//            last = answer;
//            int floor = topics.size() / nodes.size();
//            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
////            assertEquals(true, checkAnswer(answer, floor, ceil));
////
//        }
//    }
//
//    @Test
//    public void addNodes(){
//        List<Node> nodes = new ArrayList<>();
//        List<Topic> topics = new ArrayList<>();
//
//
//        String[] name = {"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"};
//        for (int i=0; i<10; i++) {
//            topics.add(new Topic(name[i]));
//        }
//
//        HashMap<Topic, Node> last = new HashMap<>();
//        for (int i=0; i<10; i++) {
//            System.out.println("-------------------------------------");
//            System.out.println(i);
//            nodes.add(new Node("node" + i));
//            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
//            if (i > 0) System.out.println("replace == " + countDifferentState(last, answer));
//            last = answer;
//            int floor = topics.size() / nodes.size();
//            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
////            assertEquals(true, checkAnswer(answer, floor, ceil));
////
//        }
//    }
//
//    @Test
//    public void deleteNodes() {
//        List<Node> nodes = new ArrayList<>();
//        List<Topic> topics = new ArrayList<>();
//
//        String[] name = {"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"};
//        for (int i=0; i<10; i++) {
//            topics.add(new Topic(name[i]));
//        }
//
//        HashMap<Topic, Node> last = new HashMap<>();
//        for (int i=0; i<5; i++) {
//            System.out.println("-------------------------------------");
//            System.out.println(i);
//            nodes.add(new Node("node" + i));
//            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
//            if (i > 0) System.out.println("replace == " + countDifferentState(last, answer));
//            last = answer;
//            int floor = topics.size() / nodes.size();
//            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
////            assertEquals(true, checkAnswer(answer, floor, ceil));
////
//        }
//
//        System.out.println("@!$!#$%!%^!@#$^@#$^ $% Y^");
//
//        for (int i=0; i<4; i++) {
//            System.out.println("-------------------------------------");
////            System.out.println(i);
//            nodes.remove(4 - i);
//            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
//            System.out.println("replace == " + countDifferentState(last, answer));
//            last = answer;
//            int floor = topics.size() / nodes.size();
//            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
////            assertEquals(true, checkAnswer(answer, floor, ceil));
////
//        }
//    }
//
//    @Test
//    public void deleteNodesOrderByAdd() {
//        List<Node> nodes = new ArrayList<>();
//        List<Topic> topics = new ArrayList<>();
//
//        String[] name = {"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"};
//        for (int i=0; i<10; i++) {
//            topics.add(new Topic(name[i]));
//        }
//
//        HashMap<Topic, Node> last = new HashMap<>();
//        for (int i=0; i<5; i++) {
//            System.out.println("-------------------------------------");
//            System.out.println(i);
//            nodes.add(new Node("node" + i));
//            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
//            if (i > 0) System.out.println("replace == " + countDifferentState(last, answer));
//            last = answer;
//            int floor = topics.size() / nodes.size();
//            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
////            assertEquals(true, checkAnswer(answer, floor, ceil));
////
//        }
//
//        System.out.println("@!$!#$%!%^!@#$^@#$^ $% Y^");
//
//        for (int i=0; i<4; i++) {
//            System.out.println("-------------------------------------");
////            System.out.println(i);
//            nodes.remove(0);
//            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
//            System.out.println("replace == " + countDifferentState(last, answer));
//            last = answer;
//            int floor = topics.size() / nodes.size();
//            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
////            assertEquals(true, checkAnswer(answer, floor, ceil));
////
//        }
//    }
//
//    @Test
//    public void createTopicFor4Node() {
//
//        List<Node> nodes = new ArrayList<>();
//        List<Topic> topics = new ArrayList<>();
//
//        HashMap<Topic, Node> last = new HashMap<>();
//
//        for (int i=0; i<4; i++) {
//            System.out.println("-------------------------------------");
////            System.out.println(i);
//            nodes.add(new Node("node " + i));
//            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
//            //System.out.println(checkAnswer(last, answer, topics));
//            last = answer;
//            int floor = topics.size() / nodes.size();
//            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
////            assertEquals(true, checkAnswer(answer, floor, ceil));
////
//        }
//
//        String[] name = {"qaqa", "wawa", "eaea", "rara", "tata", "yaya", "uaua", "iaia", "oaoa", "papa", "sasa", "dada",
//                        "fafa", "gaga", "haha", "jaja", "kaka", "lala", "zaza", "xaxa", "caca", "vava", "baba", "nana", "mama"};
//        for (int i=0; i<25; i++) {
//            topics.add(new Topic(name[i]));
//            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
//            System.out.println("replace == " + countDifferentState(last, answer));
//            last = answer;
//        }
//
//    }
//
//    @Test
//    public void deleteNodeWhen100Topic() {
//        List<Node> nodes = new ArrayList<>();
//        List<Topic> topics = new ArrayList<>();
//
//        HashMap<Topic, Node> last = new HashMap<>();
//
//        for (int i=0; i<100; i++) {
//            String name = "";
//            for (int j=0; j<7; j++) {
//                name += alphabet.charAt((int) Math.random() * 26);
//            }
//            topics.add(new Topic(name));
//        }
//
//
//        for (int i=0; i<10; i++) {
//            System.out.println("-------------------------------------");
////            System.out.println(i);
//            nodes.add(new Node("node " + i));
//            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
//            System.out.println("replace == " + countDifferentState(last, answer));
//            last = answer;
////            assertEquals(true, checkAnswer(answer, floor, ceil));
////
//        }
//
//
//        last = new HashMap<>();
//
//        for (int i=0; i<9; i++) {
//            nodes.remove(nodes.size() - 1);
//            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
//            System.out.println("replace == " + countDifferentState(last, answer));
//            last = answer;
//
//        }
//    }

    private int countDifferentState(HashMap<Topic, Node> pastState, HashMap<Topic, Node> presentState) {
        assertEquals("Maps size different", pastState.size(), presentState.size());
        int diff = 0;

        for (Map.Entry<Topic, Node> entry : pastState.entrySet()) {
            if (!entry.getValue().equals(presentState.get(entry.getKey()))) diff++;
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
        log.warn("topicCount {} nodesCount {}", topicsCount, nodesCount);
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
        solution.forEach((topic, node) -> log.warn("topic {} node {}", topic, node));
        return solution;
    }

    @Test
    public void multiTest() {
        testResolvePart(6,6);
        testResolvePart(6,3);
        testResolvePart(5,3);
        countDifferentState((HashMap<Topic, Node>) testResolvePart(6, 3),(HashMap<Topic, Node>) testResolvePart(6, 2));
        countDifferentState((HashMap<Topic, Node>) testResolvePart(30, 5),(HashMap<Topic, Node>) testResolvePart(30, 6));

    }

}
