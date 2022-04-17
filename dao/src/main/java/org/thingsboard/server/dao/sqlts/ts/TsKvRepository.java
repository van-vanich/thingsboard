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
package org.thingsboard.server.dao.sqlts.ts;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.annotation.Transactional;
import org.thingsboard.server.dao.model.sqlts.ts.TsKvCompositeKey;
import org.thingsboard.server.dao.model.sqlts.ts.TsKvEntity;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface TsKvRepository extends JpaRepository<TsKvEntity, TsKvCompositeKey> {

    @Query("SELECT tskv FROM TsKvEntity tskv WHERE tskv.entityId = :entityId " +
            "AND tskv.key = :entityKey AND tskv.ts >= :startTs AND tskv.ts < :endTs")
    List<TsKvEntity> findAllWithLimit(@Param("entityId") UUID entityId,
                                      @Param("entityKey") int key,
                                      @Param("startTs") long startTs,
                                      @Param("endTs") long endTs,
                                      Pageable pageable);

    @Transactional
    @Modifying
    @Query("DELETE FROM TsKvEntity tskv WHERE tskv.entityId = :entityId " +
            "AND tskv.key = :entityKey AND tskv.ts >= :startTs AND tskv.ts < :endTs")
    void delete(@Param("entityId") UUID entityId,
                @Param("entityKey") int key,
                @Param("startTs") long startTs,
                @Param("endTs") long endTs);

    @Async
    @Query("SELECT new TsKvEntity(MAX(tskv.strValue)) FROM TsKvEntity tskv " +
            "WHERE tskv.strValue IS NOT NULL " +
            "AND tskv.entityId = :entityId AND tskv.key = :entityKey AND tskv.ts >= :startTs AND tskv.ts < :endTs")
    CompletableFuture<TsKvEntity> findStringMax(@Param("entityId") UUID entityId,
                                                @Param("entityKey") int entityKey,
                                                @Param("startTs") long startTs,
                                                @Param("endTs") long endTs);

    @Async
    @Query("SELECT new TsKvEntity(MAX(COALESCE(tskv.longValue, -9223372036854775807)), " +
            "MAX(COALESCE(tskv.doubleValue, -1.79769E+308)), " +
            "SUM(CASE WHEN tskv.longValue IS NULL THEN 0 ELSE 1 END), " +
            "SUM(CASE WHEN tskv.doubleValue IS NULL THEN 0 ELSE 1 END), " +
            "'MAX') FROM TsKvEntity tskv " +
            "WHERE tskv.entityId = :entityId AND tskv.key = :entityKey AND tskv.ts >= :startTs AND tskv.ts < :endTs")
    CompletableFuture<TsKvEntity> findNumericMax(@Param("entityId") UUID entityId,
                                                 @Param("entityKey") int entityKey,
                                                 @Param("startTs") long startTs,
                                                 @Param("endTs") long endTs);


    @Async
    @Query("SELECT new TsKvEntity(MIN(tskv.strValue)) FROM TsKvEntity tskv " +
            "WHERE tskv.strValue IS NOT NULL " +
            "AND tskv.entityId = :entityId AND tskv.key = :entityKey AND tskv.ts >= :startTs AND tskv.ts < :endTs")
    CompletableFuture<TsKvEntity> findStringMin(@Param("entityId") UUID entityId,
                                                @Param("entityKey") int entityKey,
                                                @Param("startTs") long startTs,
                                                @Param("endTs") long endTs);

    @Async
    @Query("SELECT new TsKvEntity(MIN(COALESCE(tskv.longValue, 9223372036854775807)), " +
            "MIN(COALESCE(tskv.doubleValue, 1.79769E+308)), " +
            "SUM(CASE WHEN tskv.longValue IS NULL THEN 0 ELSE 1 END), " +
            "SUM(CASE WHEN tskv.doubleValue IS NULL THEN 0 ELSE 1 END), " +
            "'MIN') FROM TsKvEntity tskv " +
            "WHERE tskv.entityId = :entityId AND tskv.key = :entityKey AND tskv.ts >= :startTs AND tskv.ts < :endTs")
    CompletableFuture<TsKvEntity> findNumericMin(
            @Param("entityId") UUID entityId,
            @Param("entityKey") int entityKey,
            @Param("startTs") long startTs,
            @Param("endTs") long endTs);

    @Async
    @Query("SELECT new TsKvEntity(SUM(CASE WHEN tskv.booleanValue IS NULL THEN 0 ELSE 1 END), " +
            "SUM(CASE WHEN tskv.strValue IS NULL THEN 0 ELSE 1 END), " +
            "SUM(CASE WHEN tskv.longValue IS NULL THEN 0 ELSE 1 END), " +
            "SUM(CASE WHEN tskv.doubleValue IS NULL THEN 0 ELSE 1 END), " +
            "SUM(CASE WHEN tskv.jsonValue IS NULL THEN 0 ELSE 1 END)) FROM TsKvEntity tskv " +
            "WHERE tskv.entityId = :entityId AND tskv.key = :entityKey AND tskv.ts >= :startTs AND tskv.ts < :endTs")
    CompletableFuture<TsKvEntity> findCount(@Param("entityId") UUID entityId,
                                            @Param("entityKey") int entityKey,
                                            @Param("startTs") long startTs,
                                            @Param("endTs") long endTs);

    @Async
    @Query("SELECT new TsKvEntity(SUM(COALESCE(tskv.longValue, 0)), " +
            "SUM(COALESCE(tskv.doubleValue, 0.0)), " +
            "SUM(CASE WHEN tskv.longValue IS NULL THEN 0 ELSE 1 END), " +
            "SUM(CASE WHEN tskv.doubleValue IS NULL THEN 0 ELSE 1 END), " +
            "'AVG') FROM TsKvEntity tskv " +
            "WHERE tskv.entityId = :entityId AND tskv.key = :entityKey AND tskv.ts >= :startTs AND tskv.ts < :endTs")
    CompletableFuture<TsKvEntity> findAvg(@Param("entityId") UUID entityId,
                                          @Param("entityKey") int entityKey,
                                          @Param("startTs") long startTs,
                                          @Param("endTs") long endTs);

    @Async
    @Query("SELECT new TsKvEntity(SUM(COALESCE(tskv.longValue, 0)), " +
            "SUM(COALESCE(tskv.doubleValue, 0.0)), " +
            "SUM(CASE WHEN tskv.longValue IS NULL THEN 0 ELSE 1 END), " +
            "SUM(CASE WHEN tskv.doubleValue IS NULL THEN 0 ELSE 1 END), " +
            "'SUM') FROM TsKvEntity tskv " +
            "WHERE tskv.entityId = :entityId AND tskv.key = :entityKey AND tskv.ts >= :startTs AND tskv.ts < :endTs")
    CompletableFuture<TsKvEntity> findSum(@Param("entityId") UUID entityId,
                                          @Param("entityKey") int entityKey,
                                          @Param("startTs") long startTs,
                                          @Param("endTs") long endTs);

    @Transactional(timeout = 3600) // 1h in sec
    @Query(value = "WITH deleted AS (DELETE FROM ts_kv WHERE (key NOT IN :keys AND ts < :expirationTime) RETURNING *) SELECT count(*) FROM deleted",
            nativeQuery = true)
    Long cleanup(@Param("expirationTime") long expirationTime,
                 @Param("keys") List<Integer> keys);

    @Transactional(timeout = 3600) // 1h in sec
    @Query(value = "WITH deleted AS (DELETE FROM ts_kv WHERE (key NOT IN :keys AND ts < :expirationTime AND entity_id IN (" +
            "(SELECT device.id as entity_id FROM device WHERE device.tenant_id = :tenantId and device.customer_id = :customerId) UNION ALL " +
            "(SELECT asset.id as entity_id FROM asset WHERE asset.tenant_id = :tenantId and asset.customer_id = :customerId) UNION ALL " +
            "(SELECT customer.id as entity_id FROM customer WHERE customer.tenant_id = :tenantId and customer.id = :customerId)" +
            ")) RETURNING *) SELECT count(*) FROM deleted",
            nativeQuery = true)
    Long cleanUp(@Param("expirationTime") long expirationTime,
                 @Param("keys") List<Integer> keys,
                 @Param("tenantId") UUID tenantId,
                 @Param("customerId") UUID customerId);
}
