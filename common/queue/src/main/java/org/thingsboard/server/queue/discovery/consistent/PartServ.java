package org.thingsboard.server.queue.discovery.consistent;

import java.util.List;
import java.util.Map;

public interface PartServ {
    // topic == data
    Map<Topic, Node> resolvePart(List<Node> nodes, List<Topic> topics);
}
