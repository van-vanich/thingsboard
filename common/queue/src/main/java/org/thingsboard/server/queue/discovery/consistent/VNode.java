package org.thingsboard.server.queue.discovery.consistent;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.ArrayList;

public class VNode {
    private Node node;
    private ArrayList<Integer> hashVNode = new ArrayList<>();
    private final int cntVNode = 200;
    private int nowInBasket = 0;

    public VNode(Node node) {
        this.node = node;
        createHash();
    }

    private void createHash() {
        for (int i=0; i<cntVNode; i++) {
            HashFunction hash = Hashing.murmur3_32();
//            System.out.println((hash.newHasher().putInt(i).hash().asInt() + Integer.MAX_VALUE) % Integer.MAX_VALUE);
            hashVNode.add((hash.newHasher().putInt(i).putInt(node.hashCode()).hash().asInt() + Integer.MAX_VALUE ) % Integer.MAX_VALUE);
        }
    }

    public Node getNode() {
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
