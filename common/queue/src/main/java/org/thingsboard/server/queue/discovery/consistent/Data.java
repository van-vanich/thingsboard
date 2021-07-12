package org.thingsboard.server.queue.discovery.consistent;

public class Data {
    private int key;
    private String name;
    private boolean use = false;
    private org.algo.Node node;
    private int weight;

    public Data(String name, int weight) {
        this.name = name;
        this.key = hashCode(name);
        this.weight = weight;
    }

    public int getWeight() {
        return weight;
    }

    public void setUse(org.algo.Node node){
        this.use = true;
        this.node = node;
    }

    public org.algo.Node getNode() {
        return node;
    }

    public boolean getUse(){
        return use;
    }

    public int getKey() {
        return key;
    }

    public String getName() {
        return name;
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
