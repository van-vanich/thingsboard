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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class SolveWithConsistentHashingTest {

    private final SolveWithConsistentHashing resolver = new SolveWithConsistentHashing();

    @Test
    public void getCeil() {
        assertEquals(1, resolver.getCeil(1, 1));
        assertEquals(4, resolver.getCeil(12, 3));
        assertEquals(8, resolver.getCeil(54, 7));
    }

    @Test
    public void getCeilWithException() {
        assertEquals("div zero",-1, resolver.getCeil(0, 0));
        assertEquals(-1, resolver.getCeil(5, -5));
        assertEquals(-1, resolver.getCeil(-5, 5));
    }

    void countDifferentState(Map<String, ServiceInfo> pastState, Map<String, ServiceInfo> presentState,
                             int pastCountNode, int presentCountNode) {
        assertEquals("Maps size different", pastState.size(), presentState.size());
        int countReplaceTopic = 0;

        for (Map.Entry<String, ServiceInfo> entry : pastState.entrySet()) {
            if (!entry.getValue().equals(presentState.get(entry.getKey()))) {
                countReplaceTopic++;
            }
        }
        double maxPercent = 100D;
        double percentTopics = getPercentTopics(pastState.size(), countReplaceTopic, maxPercent);
        double percentNodes = getPercentNodes(pastCountNode, presentCountNode, maxPercent);
        log.warn("replace {}% topics and {}% nodes", percentTopics, percentNodes);
    }

    private double getPercentNodes(int pastCountNode, int presentCountNode, double maxPercent) {
        return maxPercent * Math.abs(pastCountNode - presentCountNode) / Math.max(pastCountNode, presentCountNode);
    }

    private double getPercentTopics(int pastStateSize, int countReplaceTopic, double maxPercent) {
        double percentTopics = countReplaceTopic * maxPercent / pastStateSize;
        return percentTopics;
    }

    Map<String, ServiceInfo> testResolvePart(int topicsCount, int nodesCount) {
        List<ServiceInfo> nodes = nodesList(nodesCount);
        Map<String, ServiceInfo> solution = resolver.balancePartitionService(nodes, topicsCount);
        checkAllError(topicsCount, nodes, solution);
        return solution;
    }

   List<ServiceInfo> nodesList(int count) {
       List<ServiceInfo> otherServers = new ArrayList<>();
       for (int i = 1; i <= count; i++) {
           otherServers.add(createServiceInfo(i));
       }
        return otherServers;
    }

    private ServiceInfo createServiceInfo(int position) {
        return ServiceInfo.newBuilder()
                .setServiceId("tb-rule-" + position)
                .setTenantIdMSB(TenantId.NULL_UUID.getMostSignificantBits())
                .setTenantIdLSB(TenantId.NULL_UUID.getLeastSignificantBits())
                .addAllServiceTypes(Collections.singletonList(ServiceType.TB_CORE.name()))
                .build();
    }

    void checkBalanced(Map<String, ServiceInfo> solution, int topicsCount, List<ServiceInfo> nodes) {
        int floor = topicsCount / nodes.size();
        int ceil = floor + ((topicsCount % nodes.size() > 0) ? 1 : 0);
        for (ServiceInfo node : nodes) {
            int cntTopicUseNode = 0;
            for (Map.Entry<String, ServiceInfo> entry : solution.entrySet()) {
                if (entry.getValue().equals(node)) cntTopicUseNode++;
            }
            assertTrue("Replace not balanced", (cntTopicUseNode >= floor && cntTopicUseNode <= ceil));
        }
        log.warn("floor = {}, ceil = {}", floor, ceil);
    }

    void checkVirtualNodesBetweenTopics(List<ServiceInfo> nodes, int topics) {
        ConcurrentSkipListMap<Long, VirtualServiceInfo> virtualNodeHash = resolver.createVirtualNodes(nodes);
        List<Long> topicsHash = getTopicHash(topics);
        int average = 0;
        for (int i = 1; i < topicsHash.size(); i++) {
            long startHash = topicsHash.get(i - 1);
            long finishHash = topicsHash.get(i);
            ConcurrentNavigableMap<Long, VirtualServiceInfo> sublist =
                    virtualNodeHash.subMap(startHash, true, finishHash, true);
            int nodeBetweenTopics = getNodeBetweenTopics(sublist);
            average += nodeBetweenTopics;
        }
        average += topics - 1;
        average /= topics;
        log.warn("average virtual node = {}", average);
        assertTrue("Virtual node has bad place on circle",average >= Math.min((nodes.size()),resolver.getCOPY_VIRTUAL_NODE() - 5) * 0.5);
    }

    private int getNodeBetweenTopics(ConcurrentNavigableMap<Long, VirtualServiceInfo> sublist) {
        HashSet<ServiceInfo> nodeBetweenTopics = new HashSet<>();
        for (Map.Entry<Long, VirtualServiceInfo> entry : sublist.entrySet()) {
            nodeBetweenTopics.add(entry.getValue().getServiceInfo());
        }
        return nodeBetweenTopics.size();
    }

    private List<Long> getTopicHash(int topics) {
        List<Long> topicsHash = new ArrayList<>();
        for (int i=0; i<topics; i++) {
            topicsHash.add(resolver.getHash("topic" + i));
        }
        topicsHash.sort((aLong, t1) -> {
            if (aLong < t1) return -1;
            if (aLong.equals(t1)) return 0;
            return 1;
        });
        return topicsHash;
    }

    private void beforeEqualsNowAndUnique(int sizeTopicListBefore, Map<String, ServiceInfo> now) {
        Set<String> topics = new HashSet<>(sizeTopicListBefore);
        for (Map.Entry<String, ServiceInfo> entry : now.entrySet()) {
            topics.add(entry.getKey());
        }
        assertEquals(topics.size(), sizeTopicListBefore);
    }


    @Test
    public void multiTest() {
        testResolvePart(6, 6);
        testResolvePart(6, 3);
        testResolvePart(5, 3);

        countDifferentState(testResolvePart(6, 3),
                testResolvePart(6, 2), 3, 2);
        countDifferentState(testResolvePart(30, 6),
                testResolvePart(30, 5), 6, 5);
        countDifferentState(testResolvePart(100, 10),
                testResolvePart(100, 9), 10, 9);
        countDifferentState(testResolvePart(100, 9),
                testResolvePart(100, 8), 9, 8);
    }

    @Test
    public void deleteTwoNodes() {
        int topics = 6;
        List<ServiceInfo> nodes = nodesList(6);

        Map<String, ServiceInfo> pastState = resolver.balancePartitionService(nodes, topics);
        checkAllError(topics, nodes, pastState);

        nodes.remove(4);
        nodes.remove(1);

        Map<String, ServiceInfo> presentState = resolver.balancePartitionService(nodes, topics);
        countDifferentState(pastState, presentState, nodes.size() + 2, nodes.size());
        checkAllError(topics, nodes, presentState);
    }

    @Test
    public void deleteTwoRandomNodes() {
        int topics = 6;
        List<ServiceInfo> nodes = nodesList(6);

        Map<String, ServiceInfo> pastState = resolver.balancePartitionService(nodes, topics);
        checkAllError(topics, nodes, pastState);

        nodes.remove((int)(Math.random() * nodes.size()));
        nodes.remove((int)(Math.random() * nodes.size()));

        Map<String, ServiceInfo> presentState = resolver.balancePartitionService(nodes, topics);
        countDifferentState(pastState, presentState, nodes.size() + 2, nodes.size());
        checkAllError(topics, nodes, presentState);
    }

    @Test
    public void fourTopicsThreeNodes() {
        int topics = 4;
        List<ServiceInfo> nodes = nodesList(3);
        Map<String, ServiceInfo> state = resolver.balancePartitionService(nodes, topics);
        checkAllError(topics, nodes, state);
    }

    @Test
    public void sixTopicSixNodeUpdate() {
        int topics = 6;
        List<ServiceInfo> nodes = nodesList(6);

        Map<String, ServiceInfo> pastState = resolver.balancePartitionService(nodes, topics);
        checkAllError(topics, nodes, pastState);
        for (int i = 0; i < 6; i++) {
            ServiceInfo nodeRemoved = nodes.get(i);
            nodes.remove(i);
            Map<String, ServiceInfo> presentState = resolver.balancePartitionService(nodes, topics);
            countDifferentState(pastState, presentState, nodes.size() + 1, nodes.size());
            checkAllError(topics, nodes, presentState);
            pastState = presentState;
            nodes.add(i, nodeRemoved);
        }
    }

    @Test
    public void turnOffAllOddNodes() {
        int topics = 25;
        List<ServiceInfo> nodes = nodesList(20);

        Map<String, ServiceInfo> pastState = resolver.balancePartitionService(nodes, topics);
        checkAllError(topics, nodes, pastState);

        for (int i = 0; i < 10; i++) {
            nodes.remove(i);
        }

        Map<String, ServiceInfo> presentState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(presentState, topics, nodes);
        countDifferentState(pastState, presentState, nodes.size() + 10, nodes.size());
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics, presentState);
    }

    @Test
    public void twentyNodeToFiveNode() {
        int topics = 20;
        List<ServiceInfo> nodes = nodesList(20);

        Map<String, ServiceInfo> pastState = resolver.balancePartitionService(nodes, topics);
        checkAllError(topics, nodes, pastState);

        for (int i = 0; i < 15; i++) {
            nodes.remove((int)(Math.random() * nodes.size()));
        }

        Map<String, ServiceInfo> presentState = resolver.balancePartitionService(nodes, topics);
        checkBalanced(presentState, topics, nodes);
        countDifferentState(pastState, presentState, 20, 5);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics, presentState);
    }

    @Test
    public void emptyNode() {
        int topics = 20;
        List<ServiceInfo> nodes = nodesList(0);
        resolver.balancePartitionService(nodes, topics);
    }

    @Test
    public void nullNode() {
        int topics = 5;
        List<ServiceInfo> nodes = null;
        resolver.balancePartitionService(nodes, topics);
    }

    @Test
    public void hardTest() {
        int topics = 10000;
        List<ServiceInfo> nodes = nodesList(10000);
        Map<String, ServiceInfo> state = resolver.balancePartitionService(nodes, topics);
        checkAllError(topics, nodes, state);
        for (int i = 0; i < 5000; i++) {
            nodes.remove((int)(nodes.size() * Math.random()));
        }
        countDifferentState(state, resolver.balancePartitionService(nodes, topics) , 10000, 5000);
    }

    @Test
    public void deleteFirstNode() {
        int topics = 100;
        List<ServiceInfo> nodes = nodesList(100);
        Map<String, ServiceInfo> state = resolver.balancePartitionService(nodes, topics);
        checkAllError(topics, nodes, state);
        nodes.remove(0);
        countDifferentState(state, resolver.balancePartitionService(nodes, topics) , 100, 99);
    }

    @Test
    public void addTwoNodes(){
        int topics = 10;
        List<ServiceInfo> nodes = nodesList(4);

        Map<String, ServiceInfo> pastState = resolver.balancePartitionService(nodes, topics);
        checkAllError(topics, nodes, pastState);

        nodes.add(createServiceInfo(5));
        nodes.add(createServiceInfo(6));

        Map<String, ServiceInfo> presentState = resolver.balancePartitionService(nodes, topics);
        countDifferentState(pastState, presentState, nodes.size() - 2, nodes.size());
        checkAllError(topics, nodes, presentState);
    }

    @Test
    public void removeTwoAddTwoNodes() {
        int topics = 6;
        List<ServiceInfo> nodes = nodesList(6);

        Map<String, ServiceInfo> pastState = resolver.balancePartitionService(nodes, topics);
        checkAllError(topics, nodes, pastState);

        ServiceInfo nodeReplaceFirst = nodes.get(2);
        ServiceInfo nodeReplaceSecond = nodes.get(4);
        nodes.remove(nodeReplaceFirst);
        nodes.remove(nodeReplaceSecond);

        Map<String, ServiceInfo> presentState = resolver.balancePartitionService(nodes, topics);
        countDifferentState(pastState, presentState, nodes.size() + 2, nodes.size());
        checkAllError(topics, nodes, presentState);
        pastState = presentState;

        nodes.add(nodeReplaceFirst);
        nodes.add(nodeReplaceSecond);
        presentState = resolver.balancePartitionService(nodes, topics);
        countDifferentState(pastState, presentState, nodes.size() - 2, nodes.size());
        checkAllError(topics, nodes, presentState);

    }

    @Test
    public void fromFiveToTwenty() {
        int topics = 20;
        List<ServiceInfo> nodes = nodesList(5);

        Map<String, ServiceInfo> pastState = resolver.balancePartitionService(nodes, 20);
        checkAllError(topics, nodes, pastState);

        for (int i=6; i<=20; i++) {
            nodes.add(createServiceInfo(i));
        }

        Map<String, ServiceInfo> presentState = resolver.balancePartitionService(nodes, topics);
        checkAllError(topics, nodes, presentState);
        countDifferentState(pastState, presentState, nodes.size() - 15, nodes.size());

    }

    private void checkAllError(int topics, List<ServiceInfo> nodes, Map<String, ServiceInfo> state) {
        checkBalanced(state, topics, nodes);
        checkVirtualNodesBetweenTopics(nodes, topics);
        beforeEqualsNowAndUnique(topics, state);
    }
}