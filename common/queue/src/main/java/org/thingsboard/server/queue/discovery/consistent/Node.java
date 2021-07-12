package org.thingsboard.server.queue.discovery.consistent;

public class Node {
    private final String name;

    // створення ноди і її хешів
    public Node(String name){
        this.name = name;
    }

    @Override
    public String toString() {
        return "Node{" +
                "name='" + name + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }
}
