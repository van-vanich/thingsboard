package org.thingsboard.server.queue.discovery.consistent;


import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class VNodeTest {

    @Test
    public void name() {
        VNode vNode = new VNode(new Node("node-1") , 1);
        log.warn(vNode.toString());
    }
}