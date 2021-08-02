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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnExpression("'${queue.partitions.algorithm_name:null}'=='consistent_hashing_with_bounded_loads'")
public class ConsistentHashingWithBoundedLoadsPartitionResolverFactory implements PartitionResolverFactory {

    @Value("${queue.partitions.virtual_nodes_count:200}")
    private Integer virtualNodesCount;

    @Override
    public PartitionResolver createPartitionResolver() {
        return new ConsistentHashingWithBoundedLoadsPartitionResolver(virtualNodesCount);
    }
}
