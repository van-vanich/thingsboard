package org.thingsboard.server.queue.discovery;


import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;

import java.util.List;

@Slf4j
@Component
@ConditionalOnExpression("'${partitions.replace_algorithm_name:null}'=='distributed'")
public class SolveWithDistributedHashing implements PartitionResolver{

    @Override
    public ServiceInfo resolveByPartitionIdx(List<ServiceInfo> servers, Integer partitionIdx, int size) {
        if (servers == null || servers.isEmpty()) {
            return null;
        }
        return servers.get(partitionIdx % servers.size());
    }
}
