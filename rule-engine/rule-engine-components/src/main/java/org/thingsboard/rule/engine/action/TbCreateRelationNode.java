/**
 * Copyright © 2016-2022 The Thingsboard Authors
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
package org.thingsboard.rule.engine.action;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.rule.engine.util.EntityContainer;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.HasTenantId;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DashboardId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EdgeId;
import org.thingsboard.server.common.data.id.EntityViewId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.List;
import java.util.function.Function;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "create relation",
        configClazz = TbCreateRelationNodeConfiguration.class,
        nodeDescription = "Finds target Entity by entity name pattern and (entity type pattern for Asset, Device) and then create a relation to Originator Entity by type and direction." +
                " If Selected entity type: Asset, Device or Customer will create new Entity if it doesn't exist and selected checkbox 'Create new entity if not exists'.<br>" +
                " In case that relation from the message originator to the selected entity not exist and  If selected checkbox 'Remove current relations'," +
                " before creating the new relation all existed relations to message originator by type and direction will be removed.<br>" +
                " If relation from the message originator to the selected entity created and If selected checkbox 'Change originator to related entity'," +
                " outbound message will be processed as a message from this entity.",
        nodeDetails = "If the relation already exists or successfully created -  Message send via <b>Success</b> chain, otherwise <b>Failure</b> chain will be used.",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbActionNodeCreateRelationConfig",
        icon = "add_circle"
)
public class TbCreateRelationNode extends TbAbstractRelationActionNode<TbCreateRelationNodeConfiguration> {

    @Override
    protected TbCreateRelationNodeConfiguration loadEntityNodeActionConfig(TbNodeConfiguration configuration) throws TbNodeException {
        return TbNodeUtils.convert(configuration, TbCreateRelationNodeConfiguration.class);
    }

    @Override
    protected boolean createEntityIfNotExists() {
        return config.isCreateEntityIfNotExists();
    }

    @Override
    protected ListenableFuture<RelationContainer> doProcessEntityRelationAction(TbContext ctx, TbMsg msg, EntityContainer entity, String relationType) {
        ListenableFuture<Boolean> future = createRelationIfAbsent(ctx, msg, entity, relationType);
        return Futures.transform(future, result -> {
            if (result && config.isChangeOriginatorToRelatedEntity()) {
                TbMsg tbMsg = ctx.transformMsg(msg, msg.getType(), entity.getEntityId(), msg.getMetaData(), msg.getData());
                return new RelationContainer(tbMsg, result);
            }
            return new RelationContainer(msg, result);
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Boolean> createRelationIfAbsent(TbContext ctx, TbMsg msg, EntityContainer entityContainer, String relationType) {
        SearchDirectionIds sdId = processSingleSearchDirection(msg, entityContainer);
        return Futures.transformAsync(deleteCurrentRelationsIfNeeded(ctx, msg, sdId, relationType), v ->
                        checkRelationAndCreateIfAbsent(ctx, entityContainer, relationType, sdId),
                ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Void> deleteCurrentRelationsIfNeeded(TbContext ctx, TbMsg msg, SearchDirectionIds sdId, String relationType) {
        if (config.isRemoveCurrentRelations()) {
            return deleteOriginatorRelations(ctx, findOriginatorRelations(ctx, msg, sdId, relationType));
        }
        return Futures.immediateFuture(null);
    }

    private ListenableFuture<List<EntityRelation>> findOriginatorRelations(TbContext ctx, TbMsg msg, SearchDirectionIds sdId, String relationType) {
        if (sdId.isOriginatorDirectionFrom()) {
            return ctx.getRelationService().findByFromAndTypeAsync(ctx.getTenantId(), msg.getOriginator(), relationType, RelationTypeGroup.COMMON);
        } else {
            return ctx.getRelationService().findByToAndTypeAsync(ctx.getTenantId(), msg.getOriginator(), relationType, RelationTypeGroup.COMMON);
        }
    }

    private ListenableFuture<Void> deleteOriginatorRelations(TbContext ctx, ListenableFuture<List<EntityRelation>> originatorRelationsFuture) {
        return Futures.transformAsync(originatorRelationsFuture, originatorRelations -> {
            if (!CollectionUtils.isEmpty(originatorRelations)) {
                List<ListenableFuture<Boolean>> list = deleteRelationsAndPushEventMessages(ctx, originatorRelations);
                return Futures.transform(Futures.allAsList(list), result -> null, ctx.getDbCallbackExecutor());
            }
            return Futures.immediateFuture(null);
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Boolean> checkRelationAndCreateIfAbsent(TbContext ctx, EntityContainer entityContainer, String relationType, SearchDirectionIds sdId) {
        return Futures.transformAsync(checkRelation(ctx, sdId, relationType), relationPresent -> {
            if (relationPresent) {
                return Futures.immediateFuture(true);
            }
            return processCreateRelation(ctx, entityContainer, sdId, relationType);
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Boolean> checkRelation(TbContext ctx, SearchDirectionIds sdId, String relationType) {
        return ctx.getRelationService().checkRelation(ctx.getTenantId(), sdId.getFromId(), sdId.getToId(), relationType, RelationTypeGroup.COMMON);
    }

    private ListenableFuture<Boolean> processCreateRelation(TbContext ctx, EntityContainer entityContainer, SearchDirectionIds sdId, String relationType) {
        switch (entityContainer.getEntityType()) {
            case ASSET:
                return processAsset(ctx, entityContainer, sdId, relationType);
            case DEVICE:
                return processDevice(ctx, entityContainer, sdId, relationType);
            case CUSTOMER:
                return processCustomer(ctx, entityContainer, sdId, relationType);
            case DASHBOARD:
                return processDashboard(ctx, entityContainer, sdId, relationType);
            case ENTITY_VIEW:
                return processView(ctx, entityContainer, sdId, relationType);
            case EDGE:
                return processEdge(ctx, entityContainer, sdId, relationType);
            case TENANT:
                return processTenant(ctx, entityContainer, sdId, relationType);
        }
        return Futures.immediateFuture(true);
    }

    private ListenableFuture<Boolean> processView(TbContext ctx, EntityContainer entityContainer, SearchDirectionIds sdId, String relationType) {
        return createRelationAndPushEvent(ctx, entityContainer, sdId, relationType, ec ->
                ctx.getEntityViewService().findEntityViewByIdAsync(ctx.getTenantId(), new EntityViewId(entityContainer.getEntityId().getId())));
    }

    private ListenableFuture<Boolean> processEdge(TbContext ctx, EntityContainer entityContainer, SearchDirectionIds sdId, String relationType) {
        return createRelationAndPushEvent(ctx, entityContainer, sdId, relationType, ec ->
                ctx.getEdgeService().findEdgeByIdAsync(ctx.getTenantId(), new EdgeId(entityContainer.getEntityId().getId())));
    }

    private ListenableFuture<Boolean> processDevice(TbContext ctx, EntityContainer entityContainer, SearchDirectionIds sdId, String relationType) {
        return createRelationAndPushEvent(ctx, entityContainer, sdId, relationType, ec ->
                ctx.getDeviceService().findDeviceByIdAsync(ctx.getTenantId(), new DeviceId(entityContainer.getEntityId().getId())));
    }

    private ListenableFuture<Boolean> processAsset(TbContext ctx, EntityContainer entityContainer, SearchDirectionIds sdId, String relationType) {
        return createRelationAndPushEvent(ctx, entityContainer, sdId, relationType, ec ->
                ctx.getAssetService().findAssetByIdAsync(ctx.getTenantId(), new AssetId(entityContainer.getEntityId().getId())));
    }

    private ListenableFuture<Boolean> processCustomer(TbContext ctx, EntityContainer entityContainer, SearchDirectionIds sdId, String relationType) {
        return createRelationAndPushEvent(ctx, entityContainer, sdId, relationType, ec ->
                ctx.getCustomerService().findCustomerByIdAsync(ctx.getTenantId(), new CustomerId(entityContainer.getEntityId().getId())));
    }

    private ListenableFuture<Boolean> processDashboard(TbContext ctx, EntityContainer entityContainer, SearchDirectionIds sdId, String relationType) {
        return createRelationAndPushEvent(ctx, entityContainer, sdId, relationType, ec ->
                ctx.getDashboardService().findDashboardByIdAsync(ctx.getTenantId(), new DashboardId(entityContainer.getEntityId().getId())));
    }

    private ListenableFuture<Boolean> processTenant(TbContext ctx, EntityContainer entityContainer, SearchDirectionIds sdId, String relationType) {
        return createRelationAndPushEvent(ctx, entityContainer, sdId, relationType, ec ->
                ctx.getTenantService().findTenantByIdAsync(ctx.getTenantId(), TenantId.fromUUID(ec.getEntityId().getId())));
    }

    private <T extends HasTenantId> ListenableFuture<Boolean> createRelationAndPushEvent(TbContext ctx, EntityContainer entityContainer,
                                                                                         SearchDirectionIds sdId, String relationType, Function<EntityContainer, ListenableFuture<T>> entityFinder) {
        return Futures.transformAsync(entityFinder.apply(entityContainer), in -> {
            if (in != null) {
                ListenableFuture<Boolean> relationSaveFuture = processSave(ctx, sdId, relationType);
                return pushEvent(ctx, relationSaveFuture, sdId, relationType);
            } else {
                return Futures.immediateFuture(true);
            }
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Boolean> pushEvent(TbContext ctx, ListenableFuture<Boolean> future, SearchDirectionIds sdId, String relationType) {
        return Futures.transform(future, res -> {
            if (res) {
                pushEventUpdateOrCreateRelation(ctx, sdId, relationType);
            }
            return res;
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Boolean> processSave(TbContext ctx, SearchDirectionIds sdId, String relationType) {
        return ctx.getRelationService().saveRelationAsync(ctx.getTenantId(), new EntityRelation(sdId.getFromId(), sdId.getToId(), relationType, RelationTypeGroup.COMMON));
    }

    protected void pushEventUpdateOrCreateRelation(TbContext ctx, SearchDirectionIds sdId, String relationType) {
        EntityRelation relation = new EntityRelation(sdId.getFromId(), sdId.getToId(), relationType);
        ctx.enqueueEntityRelationEvents(relation, DataConstants.ENTITY_RELATION_ADD_OR_UPDATE);
    }

}
