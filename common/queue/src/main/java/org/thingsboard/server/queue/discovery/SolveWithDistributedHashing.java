/**
 * Copyright © 2016-2021 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.queue.discovery;


import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;

import java.util.List;

@Slf4j
@Service
@ConditionalOnExpression("'${queue.partitions.replace_algorithm_name:null}'=='distributed'")
public class SolveWithDistributedHashing implements PartitionResolver{

    @Override
    public ServiceInfo resolveByPartitionIdx(List<ServiceInfo> servers, Integer partitionIdx, int size) {
        if (servers == null || servers.isEmpty()) {
            return null;
        }
        log.info("Distributed say {} to {}", partitionIdx, servers.get(partitionIdx % servers.size()));
        return servers.get(partitionIdx % servers.size());
    }
}
