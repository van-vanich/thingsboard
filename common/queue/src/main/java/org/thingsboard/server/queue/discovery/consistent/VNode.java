package org.thingsboard.server.queue.discovery.consistent;

import java.util.ArrayList;

public class VNode {
    private org.algo.Node node;
    private ArrayList<Integer> hashVNode = new ArrayList<>();
    private final int cntVNode = 200;
    private int nowInBasket = 0;

    public VNode(org.algo.Node node) {
        this.node = node;
        createHash();
    }

    private void createHash() {
        for (int i=0; i<cntVNode; i++) {
            int hash = (int)(Math.random() * Integer.MAX_VALUE);
            hashVNode.add(hash);
        }
    }

    public org.algo.Node getNode() {
        return node;
    }

    public ArrayList<Integer> getHashVNode() {
        return hashVNode;
    }

    public int getNowInBasket() {
        return nowInBasket;
    }

    public void addToBasket(int weight) {
        nowInBasket += weight;
    }
}
