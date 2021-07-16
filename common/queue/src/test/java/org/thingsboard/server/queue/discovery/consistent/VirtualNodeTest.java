package org.thingsboard.server.queue.discovery.consistent;


import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class VirtualNodeTest {

    @Test
    public void name() {
        VirtualNode virtualNode = new VirtualNode(new Node("node-1") , 1);
        log.warn(virtualNode.toString());
    }
}