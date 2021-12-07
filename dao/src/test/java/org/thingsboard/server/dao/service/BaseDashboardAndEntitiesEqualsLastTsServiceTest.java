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
package org.thingsboard.server.dao.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.RuleChainId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.common.data.rule.RuleChain;
import org.thingsboard.server.common.data.rule.RuleChainMetaData;
import org.thingsboard.server.common.data.rule.RuleNode;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;

@Slf4j
public abstract class BaseDashboardAndEntitiesEqualsLastTsServiceTest extends AbstractServiceTest {

    TenantId tenantId;
    AssetId assetId;
    RuleChainId ruleChainId;
    List<DeviceId> devices;
    List<EntityRelation> entityRelations;
    String configGenerator = "{" +
            "\"msgCount\": 0," +
            "\"periodInSeconds\": 60," +
            "\"jsScript\": \"function getRandomInt(max) {\n" +
            "  return Math.floor(Math.random() * max);\n" +
            "}\n" +
            "\n" +
            "var msg = { Totalizer: getRandomInt(100) };\n" +
            "var metadata = {deviceName: \"PLEASE_SET_BUS_NAME\"};\n" +
            "var msgType = \"POST_TELEMETRY_REQUEST\";\n" +
            "\nreturn { msg: msg, metadata: metadata, msgType: msgType };\"," +
            "\"originatorId\": \"PLEASE_SET_ORIGINATOR_ID\"," +
            "\"originatorType\": \"DEVICE\"" +
            "}";

    String configChangeOriginator = "{\n" +
            "          \"originatorSource\": \"RELATED\",\n" +
            "          \"relationsQuery\": {\n" +
            "            \"direction\": \"TO\",\n" +
            "            \"maxLevel\": 1,\n" +
            "            \"filters\": [\n" +
            "              {\n" +
            "                \"relationType\": \"Contains\",\n" +
            "                \"entityTypes\": [\n" +
            "                  \"ASSET\"\n" +
            "                ]\n" +
            "              }\n" +
            "            ]\n" +
            "          }\n" +
            "        }";
    String configOriginatorAttribute = "{\n" +
            "          \"tellFailureIfAbsent\": false,\n" +
            "          \"clientAttributeNames\": [],\n" +
            "          \"sharedAttributeNames\": [],\n" +
            "          \"serverAttributeNames\": [],\n" +
            "          \"latestTsKeyNames\": [\n" +
            "            \"genericCumulativeObj\"\n" +
            "          ],\n" +
            "          \"getLatestValueWithTs\": false\n" +
            "        }";
    String configJsScript = "{\n" +
            "\"jsScript\": \"var currentGenericCumulativeObj;\n" +
            "if (typeof metadata.genericCumulativeObj !== 'undefined') {\n" +
            "   currentGenericCumulativeObj = JSON.parse(metadata.genericCumulativeObj);\n" +
            "} else {\n" +
            "   currentGenericCumulativeObj = {};\n}\n" +
            "\n" +
            "currentGenericCumulativeObj[metadata.deviceName] = msg.Totalizer;\n" +
            "\n" +
            "var newMsg = {};\n" +
            "newMsg.genericCumulativeObj = currentGenericCumulativeObj;\n" +
            "//metadata.ts = Date.now();\n" +
            "return {msg: newMsg, metadata: metadata, msgType: msgType};\"}";

    @Before
    public void setUp() {
        createAndSaveTenant();
        devices = List.of(createDevice("Bus A"),
                createDevice("Bus B"),
                createDevice("Bus C"),
                createDevice("Bus D"));
        assetId = createAsset();
        setRelationFromDeviceToAsset();
        ruleChainId = createRuleChain();
        Assert.assertNotNull(ruleChainId);
        setUpRuleChain();
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        log.error("------------------------------------------------------------------");
        Thread.sleep(69000);
        List<RuleNode> ruleChainNodes = ruleChainService.getRuleChainNodes(tenantId, ruleChainId);
        for (RuleNode ruleNode : ruleChainNodes) {
            log.error(ruleNode.getName());
            log.error("\n{}", ruleNode.getConfiguration().asText());
            log.error("");
        }
        log.error(tsService.findAllLatest(tenantId, assetId).get().toString());
        log.error(tsService.findLatest(tenantId, assetId, List.of("genericCumulativeObj")).get().toString());
        for (DeviceId deviceId : devices) {
            log.error(tsService.findLatest(tenantId, deviceId, List.of("Totalizer")).get().toString());
        }
        List<RuleNode> ruleChains = ruleChainService.getRuleChainNodes(tenantId, ruleChainId);
        log.error(ruleChains.toString());
        for (RuleNode ruleNode : ruleChains) {
            log.error(ruleNode.toString());
        }
    }

    @After
    public void delete() {
        for (DeviceId deviceId : devices) {
            deviceService.deleteDevice(tenantId, deviceId);
        }
        assetService.deleteAsset(tenantId, assetId);
        tenantService.deleteTenant(tenantId);
    }

    private void createAndSaveTenant() {
        Tenant tenant = new Tenant();
        tenant.setTitle("My tenant" + UUID.randomUUID());
        Tenant savedTenant = tenantService.saveTenant(tenant);
        Assert.assertNotNull(savedTenant);
        tenantId = savedTenant.getId();
    }

    private DeviceId createDevice(String name) {
        Device device = new Device();
        device.setName(name);
        device.setType("default");
        device.setTenantId(tenantId);
        Device savedDevice = deviceService.saveDevice(device);
        assertNotNull(savedDevice);
        return savedDevice.getId();
    }

    private AssetId createAsset() {
        Asset asset = new Asset();
        asset.setType("test");
        asset.setName("Bus");
        asset.setTenantId(tenantId);
        Asset savedAsset = assetService.saveAsset(asset);
        assertNotNull(savedAsset);
        return savedAsset.getId();
    }

    private void setRelationFromDeviceToAsset() {
        entityRelations = new ArrayList<>();
        for (DeviceId deviceId : devices) {
            EntityRelation entityRelation = new EntityRelation(deviceId, assetId, EntityRelation.CONTAINS_TYPE);
            entityRelations.add(entityRelation);
            relationService.saveRelation(tenantId, entityRelation);
            entityRelation = new EntityRelation(assetId, deviceId, EntityRelation.CONTAINS_TYPE);
            entityRelations.add(entityRelation);
            relationService.saveRelation(tenantId, entityRelation);
        }
        List<EntityRelation> byTo = relationService.findByTo(tenantId, assetId, RelationTypeGroup.COMMON);
        for (EntityRelation entityRelation : byTo) {
            log.error(entityRelation.getFrom().toString());
            log.error(entityRelation.getTo().toString());
            log.error("--------------------------------");
        }
    }

    private RuleChainId createRuleChain() {
        RuleChain ruleChain = new RuleChain();
        ruleChain.setTenantId(tenantId);
        ruleChain.setName("GenericCumulativeDailyTotal" + UUID.randomUUID());


        RuleChain savedRuleChain = ruleChainService.saveRuleChain(ruleChain);
        Assert.assertNotNull(savedRuleChain);
        Assert.assertNotNull(savedRuleChain.getId());
        Assert.assertTrue(savedRuleChain.getCreatedTime() > 0);
        Assert.assertEquals(ruleChain.getTenantId(), savedRuleChain.getTenantId());
        Assert.assertEquals(ruleChain.getName(), savedRuleChain.getName());

        return savedRuleChain.getId();
    }

    private void setUpRuleChain() {
        RuleChainMetaData ruleChainMetaData = new RuleChainMetaData();
        ruleChainMetaData.setFirstNodeIndex(0);

        ruleChainMetaData.setRuleChainId(ruleChainId);

        List<RuleNode> generators = new ArrayList<>();
        for (DeviceId deviceId : devices) {
            generators.add(createRuleNodeGenerator(deviceId));
        }

        RuleNode savedTimeseriesDevices = createRuleNodeSavedTimeseries();
        RuleNode changeOriginator = createRuleNodeChangeOriginator();
        RuleNode checkpoint = createRuleNodeCheckPoint();
        RuleNode originatorAttribute = createRuleNodeOriginatorAttribute();
        RuleNode script = createRuleNodeScript();
        RuleNode savedTimeseriesAsset = createRuleNodeSavedTimeseries();

        List<RuleNode> ruleNodes = new ArrayList<>();
        ruleNodes.addAll(generators);
        ruleNodes.addAll(List.of(
                savedTimeseriesDevices,
                changeOriginator,
                checkpoint,
                originatorAttribute,
                script,
                savedTimeseriesAsset
        ));

        ruleChainMetaData.setNodes(ruleNodes);
        ruleChainMetaData.addConnectionInfo(0, 4, "Success");
        ruleChainMetaData.addConnectionInfo(1, 4, "Success");
        ruleChainMetaData.addConnectionInfo(2, 4, "Success");
        ruleChainMetaData.addConnectionInfo(3, 4, "Success");
        ruleChainMetaData.addConnectionInfo(4, 5, "Success");
        ruleChainMetaData.addConnectionInfo(5, 6, "Success");
        ruleChainMetaData.addConnectionInfo(6, 7, "Success");
        ruleChainMetaData.addConnectionInfo(7, 8, "Success");
        ruleChainMetaData.addConnectionInfo(8, 9, "Success");

        ruleChainService.saveRuleChainMetaData(tenantId, ruleChainMetaData);
        log.error(ruleChainService.getRuleChainNodes(tenantId, ruleChainId).toString());
    }

    private RuleNode createRuleNodeGenerator(DeviceId deviceId) {
        RuleNode ruleNodeGenerator = new RuleNode();
        ruleNodeGenerator.setRuleChainId(ruleChainId);
        ruleNodeGenerator.setType("org.thingsboard.rule.engine.debug.TbMsgGeneratorNode");
        ruleNodeGenerator.setName("Bus For " + deviceId);
        ruleNodeGenerator.setConfiguration(JacksonUtil.valueToTree(configGenerator
                .replaceFirst("PLEASE_SET_ORIGINATOR_ID", deviceId.toString())
                .replaceFirst("PLEASE_SET_BUS_NAME", deviceService.findDeviceById(tenantId, deviceId).getName())));
        return ruleNodeGenerator;
    }

    private RuleNode createRuleNodeSavedTimeseries() {
        RuleNode ruleNodeSavedTimeseries = new RuleNode();
        ruleNodeSavedTimeseries.setType("org.thingsboard.rule.engine.telemetry.TbMsgTimeseriesNode");
        ruleNodeSavedTimeseries.setName("Save");
        ruleNodeSavedTimeseries.setConfiguration(JacksonUtil.valueToTree("{\"defaultTTL\": 99999}"));
        return ruleNodeSavedTimeseries;
    }

    private RuleNode createRuleNodeChangeOriginator() {
        RuleNode ruleNodeChangeOriginator = new RuleNode();
        ruleNodeChangeOriginator.setType("org.thingsboard.rule.engine.transform.TbChangeOriginatorNode");
        ruleNodeChangeOriginator.setName("To Asset");
        ruleNodeChangeOriginator.setConfiguration(JacksonUtil.valueToTree(configChangeOriginator));
        return ruleNodeChangeOriginator;
    }

    private RuleNode createRuleNodeCheckPoint() {
        RuleNode ruleNodeCheckPoint = new RuleNode();
        ruleNodeCheckPoint.setType("org.thingsboard.rule.engine.flow.TbCheckpointNode");
        ruleNodeCheckPoint.setName("SyncByAsset");
        ruleNodeCheckPoint.setConfiguration(JacksonUtil.valueToTree("{\"queueName\": \"SequentialByOriginator\"}"));
        return ruleNodeCheckPoint;
    }

    private RuleNode createRuleNodeOriginatorAttribute() {
        RuleNode ruleNodeOriginatorAttribute = new RuleNode();
        ruleNodeOriginatorAttribute.setType("org.thingsboard.rule.engine.metadata.TbGetAttributesNode");
        ruleNodeOriginatorAttribute.setName("Get genericCumulativeObj");
        ruleNodeOriginatorAttribute.setConfiguration(JacksonUtil.valueToTree(configOriginatorAttribute));
        return ruleNodeOriginatorAttribute;
    }

    private RuleNode createRuleNodeScript() {
        RuleNode ruleNodeScript = new RuleNode();
        ruleNodeScript.setType("org.thingsboard.rule.engine.transform.TbTransformMsgNode");
        ruleNodeScript.setName("add previous keys to current object");
        ruleNodeScript.setConfiguration(JacksonUtil.valueToTree(configJsScript));
        return ruleNodeScript;
    }

}
