package org.thingsboard.server.queue.discovery;

import lombok.Data;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;

@Data
public class VirtualServiceInfo {
    private final ServiceInfo serviceInfo;
    private final int id;
}
