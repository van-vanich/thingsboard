package org.thingsboard.server.queue.discovery.consistent;


import lombok.Data;

@Data
public class VirtualNode {
    private final Node node;
    private final int id;
}
