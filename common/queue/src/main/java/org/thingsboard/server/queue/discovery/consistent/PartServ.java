package org.thingsboard.server.queue.discovery.consistent;

import java.util.List;
import java.util.Map;

public interface PartServ {
    // topic == data
    Map<org.algo.Topic, org.algo.Node> resolvePart(List<org.algo.Node> nodes, List<org.algo.Topic> topics);
}
