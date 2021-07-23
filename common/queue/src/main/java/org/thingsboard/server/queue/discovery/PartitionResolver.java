package org.thingsboard.server.queue.discovery;

import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;

import java.util.List;

public interface PartitionResolver {
    ServiceInfo resolveByPartitionIdx(List<ServiceInfo> servers, Integer partitionIdx, int partitionsTotal);
}
