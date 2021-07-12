package org.thingsboard.server.queue.discovery.consistent;


import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class SolvePartServTest {

    private String alphabet = "qwertyuiopasdfghjklzxcvbnm";

    private SolvePartServ loadBalancer = new SolvePartServ();

    @Test
    public void oneNode(){
        List<Node> nodes = new ArrayList<>();
        List<Topic> topics = new ArrayList<>();
        nodes.add(new Node("node1 "));
        for (int i=0; i<10; i++) {
            topics.add(new Topic("" + (char)('a' + i)));
        }

        HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
        int floor = topics.size() / nodes.size();
        int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
//        assertEquals(true, checkAnswer(answer, floor, ceil));

    }

    @Test
    public void nodesInOneLine(){
        List<Node> nodes = new ArrayList<>();
        List<Topic> topics = new ArrayList<>();
        for (int i=0; i<10; i++) {
            topics.add(new Topic("" + (char)('a' + i)));
        }

        HashMap<Topic, Node> last = new HashMap<>();
        for (int i=0; i<10; i++) {
            System.out.println("-------------------------------------");
            System.out.println(i);
            nodes.add(new Node("node" + i));
            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
            if (i > 0) System.out.println("replace == " + checkAnswer(last, answer, topics));
            last = answer;
            int floor = topics.size() / nodes.size();
            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
//            assertEquals(true, checkAnswer(answer, floor, ceil));
//
        }
    }

    @Test
    public void addNodes(){
        List<Node> nodes = new ArrayList<>();
        List<Topic> topics = new ArrayList<>();
        for (int i=0; i<10; i++) {
            String name = "";
            for (int j=0; j<5; j++) {
                name += alphabet.charAt((int)(Math.random() * 26));
            }
            topics.add(new Topic(name));
        }

        HashMap<Topic, Node> last = new HashMap<>();
        for (int i=0; i<10; i++) {
            System.out.println("-------------------------------------");
            System.out.println(i);
            nodes.add(new Node("node" + i));
            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
            if (i > 0) System.out.println("replace == " + checkAnswer(last, answer, topics));
            last = answer;
            int floor = topics.size() / nodes.size();
            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
//            assertEquals(true, checkAnswer(answer, floor, ceil));
//
        }
    }

    @Test
    public void deleteNodes() {
        List<Node> nodes = new ArrayList<>();
        List<Topic> topics = new ArrayList<>();
        for (int i=0; i<10; i++) {
            String name = "";
            for (int j=0; j<5; j++) {
                name += alphabet.charAt((int)(Math.random() * 26));
            }
            topics.add(new Topic(name));
        }

        HashMap<Topic, Node> last = new HashMap<>();
        for (int i=0; i<5; i++) {
            System.out.println("-------------------------------------");
            System.out.println(i);
            nodes.add(new Node("node" + i));
            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
            if (i > 0) System.out.println("replace == " + checkAnswer(last, answer, topics));
            last = answer;
            int floor = topics.size() / nodes.size();
            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
//            assertEquals(true, checkAnswer(answer, floor, ceil));
//
        }

        System.out.println("@!$!#$%!%^!@#$^@#$^ $% Y^");

        for (int i=0; i<4; i++) {
            System.out.println("-------------------------------------");
//            System.out.println(i);
            nodes.remove(4 - i);
            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
            System.out.println("replace == " + checkAnswer(last, answer, topics));
            last = answer;
            int floor = topics.size() / nodes.size();
            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
//            assertEquals(true, checkAnswer(answer, floor, ceil));
//
        }
    }

    @Test
    public void createTopicFor4Node() {

        List<Node> nodes = new ArrayList<>();
        List<Topic> topics = new ArrayList<>();

        HashMap<Topic, Node> last = new HashMap<>();

        for (int i=0; i<4; i++) {
            System.out.println("-------------------------------------");
//            System.out.println(i);
            nodes.add(new Node("node " + i));
            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
            //System.out.println(checkAnswer(last, answer, topics));
            last = answer;
            int floor = topics.size() / nodes.size();
            int ceil = topics.size() / nodes.size() + ((topics.size() % nodes.size() != 0) ? 1 : 0);
//            assertEquals(true, checkAnswer(answer, floor, ceil));
//
        }

        for (int i=0; i<25; i++) {
            String name = "";
            for (int j=0; j<5; j++) {
                name += alphabet.charAt((int)(Math.random() * 26));
            }
            topics.add(new Topic(name));
            HashMap<Topic, Node> answer = (HashMap<Topic, Node>) loadBalancer.resolvePart(nodes, topics);
            System.out.println("replace == " + checkAnswer(last, answer, topics));
            last = answer;
        }

    }

    private int checkAnswer(HashMap<Topic, Node> last, HashMap<Topic, Node> now, List<Topic> topics) {
        System.out.println("#!");
        if (last.size() == 0) return -1;
        int diff = 0;
        for (Topic topic: topics) {

            Node nodeLast = last.get(topic);
            Node nodeNow = now.get(topic);
            if (!nodeNow.equals(nodeLast)) {
//                System.out.println(nodeLast.getName() + " => " + nodeNow);
                diff++;
            }
        }

//        System.out.println("!" + diff);
        return diff;
    }


}
