/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datasophon.api.utils;

import com.datasophon.api.master.ActorUtils;
import com.datasophon.api.master.DAGBuildActor;
import com.datasophon.api.service.ClusterServiceCommandHostCommandService;
import com.datasophon.api.service.ClusterServiceCommandHostService;
import com.datasophon.api.service.ClusterServiceCommandService;
import com.datasophon.common.Constants;
import com.datasophon.common.command.StartExecuteCommandCommand;
import com.datasophon.common.enums.CommandType;
import com.datasophon.common.utils.Result;
import com.datasophon.dao.entity.ClusterServiceCommandEntity;
import com.datasophon.dao.entity.ClusterServiceCommandHostCommandEntity;
import com.datasophon.dao.entity.ClusterServiceCommandHostEntity;

import java.util.List;
import java.util.Map;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import akka.actor.ActorRef;

/**
 * Utility class that consolidates common operations across the task-center
 * query chain: pagination assembly, state-code enrichment, batch command
 * persistence, and Akka actor dispatch.
 *
 * <p>Before this class existed, the same offset/limit calculation, state-code
 * setting loop, batch-save triplet, and actor-dispatch snippet were copied
 * across three service implementations.  Centralising them here means a
 * change in any one of those patterns only needs to happen in one place.</p>
 */
public final class PageUtils {

    private PageUtils() {
        throw new IllegalStateException("Utility class");
    }

    // ===== Pagination =====

    /**
     * Execute a paginated query, enrich every result rows with the correct
     * {@code commandStateCode} (or {@code serviceStateCode}) via the supplied
     * callback, and wrap the result in the standard {@link Result} envelope.
     *
     * @param service  MyBatis-Plus service that owns the entity table
     * @param wrapper  query condition (should <b>not</b> include limit/offset)
     * @param page     1-based page number
     * @param pageSize number of rows per page
     * @param enricher callback that sets transient fields on each entity
     * @return a {@link Result} containing {@code data} (the page) and {@code total}
     */
    public static <T> Result paginate(ServiceImpl<?, T> service,
                                      QueryWrapper<T> wrapper,
                                      int page,
                                      int pageSize,
                                      EntityEnricher<T> enricher) {
        int total = service.count(wrapper);
        List<T> list = service.list(
                wrapper.last("limit " + offset(page, pageSize) + "," + pageSize));
        if (enricher != null) {
            list.forEach(enricher::enrich);
        }
        return Result.success(list).put(Constants.TOTAL, total);
    }

    /** Overload when no enrichment is needed. */
    public static <T> Result paginate(ServiceImpl<?, T> service,
                                      QueryWrapper<T> wrapper,
                                      int page,
                                      int pageSize) {
        return paginate(service, wrapper, page, pageSize, null);
    }

    /**
     * Paginate for the {@code LambdaQueryChainWrapper} style used by
     * {@code ClusterServiceCommandHostServiceImpl}.
     */
    public static <T> Result paginateFromChain(List<T> list,
                                               int total,
                                               EntityEnricher<T> enricher) {
        if (enricher != null) {
            list.forEach(enricher::enrich);
        }
        return Result.success(list).put(Constants.TOTAL, total);
    }

    private static int offset(int page, int pageSize) {
        return (page - 1) * pageSize;
    }

    // ===== State-code enrichment =====

    /**
     * Callback interface for setting transient fields (state codes, duration,
     * etc.) on entities returned by a paginated query.
     */
    @FunctionalInterface
    public interface EntityEnricher<T> {
        void enrich(T entity);
    }

    /** Enricher for {@link ClusterServiceCommandEntity}. */
    public static void enrichCommandEntity(ClusterServiceCommandEntity e) {
        e.setCommandStateCode(e.getCommandState().getValue());
    }

    /** Enricher for {@link ClusterServiceCommandHostEntity}. */
    public static void enrichCommandHostEntity(ClusterServiceCommandHostEntity e) {
        e.setCommandStateCode(e.getCommandState().getValue());
    }

    /** Enricher for {@link ClusterServiceCommandHostCommandEntity}. */
    public static void enrichHostCommandEntity(ClusterServiceCommandHostCommandEntity e) {
        e.setCommandStateCode(e.getCommandState().getValue());
    }

    // ===== Batch command persistence =====

    /**
     * Persist the three-tier command hierarchy (Command → CommandHost →
     * HostCommand) in a single transaction.  This pattern was duplicated
     * across {@code generateCommand}, {@code generateServiceCommand}, and
     * {@code generateServiceRoleCommand}.
     */
    public static void batchSaveCommands(ClusterServiceCommandService commandService,
                                         ClusterServiceCommandHostService commandHostService,
                                         ClusterServiceCommandHostCommandService hostCommandService,
                                         List<ClusterServiceCommandEntity> commands,
                                         List<ClusterServiceCommandHostEntity> commandHosts,
                                         List<ClusterServiceCommandHostCommandEntity> hostCommands) {
        commandService.saveBatch(commands);
        commandHostService.saveBatch(commandHosts);
        hostCommandService.saveBatch(hostCommands);
    }

    // ===== Actor dispatch =====

    /**
     * Send a {@link StartExecuteCommandCommand} to the {@link DAGBuildActor}.
     * Previously this two-line snippet was duplicated in every
     * {@code generate*} method that auto-starts execution.
     */
    public static void dispatchToDAGActor(List<String> commandIds,
                                          Integer clusterId,
                                          CommandType commandType) {
        ActorRef dagBuildActor = ActorUtils.getLocalActor(
                DAGBuildActor.class, ActorUtils.getActorRefName(DAGBuildActor.class));
        dagBuildActor.tell(
                new StartExecuteCommandCommand(commandIds, clusterId, commandType),
                ActorRef.noSender());
    }

    // ===== Command-host map helper =====

    /**
     * Get-or-create a {@link ClusterServiceCommandHostEntity} for a given
     * hostname within the supplied map.  This pattern appeared in every
     * {@code generate*} method.
     */
    public static ClusterServiceCommandHostEntity getOrCreateCommandHost(
            Map<String, ClusterServiceCommandHostEntity> hostMap,
            List<ClusterServiceCommandHostEntity> commandHostList,
            String commandId,
            String hostname) {
        return hostMap.computeIfAbsent(hostname, h -> {
            ClusterServiceCommandHostEntity host =
                    ProcessUtils.generateCommandHostEntity(commandId, h);
            commandHostList.add(host);
            return host;
        });
    }
}
