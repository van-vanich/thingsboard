package org.thingsboard.server.queue.discovery.consistent;

import java.util.List;
import java.util.Map;

public interface PartitionService {
    Map<Topic, Node> balancePartitionService(List<Node> nodes, List<Topic> topics);
}
