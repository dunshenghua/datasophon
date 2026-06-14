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

package com.datasophon.api.service.command;

import com.datasophon.api.master.ActorUtils;
import com.datasophon.api.master.DAGBuildActor;
import com.datasophon.api.service.ClusterServiceCommandHostCommandService;
import com.datasophon.api.service.ClusterServiceCommandHostService;
import com.datasophon.api.utils.ProcessUtils;
import com.datasophon.common.command.StartExecuteCommandCommand;
import com.datasophon.common.enums.CommandType;
import com.datasophon.dao.entity.ClusterServiceCommandEntity;
import com.datasophon.dao.entity.ClusterServiceCommandHostCommandEntity;
import com.datasophon.dao.entity.ClusterServiceCommandHostEntity;
import com.datasophon.dao.enums.RoleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.baomidou.mybatisplus.extension.service.IService;

import akka.actor.ActorRef;

/**
 * Accumulates the 3-tier command entity bundle (Command → CommandHost → HostCommand)
 * with hostname deduplication, then batch-saves and optionally dispatches to DAGBuildActor.
 *
 * <p>Lifecycle: create → addCommand → addHostCommand (repeat) → saveOnly / saveAndDispatch
 *
 * <p>Each call to {@link #addCommand} resets the hostname dedup map, because
 * CommandHost entities are scoped to a single commandId.
 */
public class CommandBundleBuilder {

    private final List<ClusterServiceCommandEntity> commandEntities = new ArrayList<>();
    private final List<ClusterServiceCommandHostEntity> hostEntities = new ArrayList<>();
    private final List<ClusterServiceCommandHostCommandEntity> hostCommandEntities = new ArrayList<>();
    private final List<String> commandIds = new ArrayList<>();

    /** Per-command hostname dedup map; reset on each {@link #addCommand} call. */
    private Map<String, ClusterServiceCommandHostEntity> hostDedupMap = new HashMap<>();

    /**
     * Creates a top-level command entity, registers it, and resets the hostname
     * dedup map for subsequent host-command additions.
     *
     * @return the newly created command entity (for attaching host commands)
     */
    public ClusterServiceCommandEntity addCommand(Integer clusterId, CommandType commandType,
                                                  String serviceName, Integer serviceInstanceId) {
        ClusterServiceCommandEntity commandEntity =
                ProcessUtils.generateCommandEntity(clusterId, commandType, serviceName);
        commandEntity.setServiceInstanceId(serviceInstanceId);
        commandEntities.add(commandEntity);
        commandIds.add(commandEntity.getCommandId());
        // Each command has its own commandId, so host entities are not shared across commands
        hostDedupMap = new HashMap<>();
        return commandEntity;
    }

    /**
     * Creates a host-level command entity. If a CommandHostEntity for this hostname
     * already exists within the current command, reuses it (deduplication).
     *
     * @param commandEntity the parent command (must have been created via {@link #addCommand})
     * @return the newly created host-command entity
     */
    public ClusterServiceCommandHostCommandEntity addHostCommand(
            ClusterServiceCommandEntity commandEntity,
            String hostname,
            String serviceRoleName,
            RoleType serviceRoleType,
            CommandType commandType) {
        String commandId = commandEntity.getCommandId();

        ClusterServiceCommandHostEntity commandHost = hostDedupMap.computeIfAbsent(hostname, k -> {
            ClusterServiceCommandHostEntity newHost =
                    ProcessUtils.generateCommandHostEntity(commandId, hostname);
            hostEntities.add(newHost);
            return newHost;
        });

        ClusterServiceCommandHostCommandEntity hostCommand =
                ProcessUtils.generateCommandHostCommandEntity(
                        commandType, commandId, serviceRoleName, serviceRoleType, commandHost);
        hostCommandEntities.add(hostCommand);
        return hostCommand;
    }

    /** Returns true if no command entities have been added. */
    public boolean isEmpty() {
        return commandEntities.isEmpty();
    }

    /** Returns true if no host entities have been added (i.e., no roles were selected). */
    public boolean isHostEmpty() {
        return hostEntities.isEmpty();
    }

    /** Returns the collected command IDs as an unmodifiable list. */
    public List<String> getCommandIds() {
        return Collections.unmodifiableList(commandIds);
    }

    /** Returns the command IDs joined by comma, suitable for Result.success(). */
    public String getCommandIdsAsString() {
        return String.join(",", commandIds);
    }

    /**
     * Batch-saves all accumulated entities without dispatching to DAGBuildActor.
     * Used by {@code generateCommand} which does not auto-execute.
     */
    public void saveOnly(IService<ClusterServiceCommandEntity> commandService,
                         IService<ClusterServiceCommandHostEntity> hostService,
                         IService<ClusterServiceCommandHostCommandEntity> hostCommandService) {
        if (!commandEntities.isEmpty()) {
            commandService.saveBatch(commandEntities);
        }
        if (!hostEntities.isEmpty()) {
            hostService.saveBatch(hostEntities);
        }
        if (!hostCommandEntities.isEmpty()) {
            hostCommandService.saveBatch(hostCommandEntities);
        }
    }

    /**
     * Batch-saves all accumulated entities and dispatches to DAGBuildActor for execution.
     * Used by {@code generateServiceCommand} and {@code generateServiceRoleCommand}
     * which auto-execute after generation.
     */
    public void saveAndDispatch(Integer clusterId, CommandType commandType,
                                IService<ClusterServiceCommandEntity> commandService,
                                IService<ClusterServiceCommandHostEntity> hostService,
                                IService<ClusterServiceCommandHostCommandEntity> hostCommandService) {
        saveOnly(commandService, hostService, hostCommandService);
        if (!commandIds.isEmpty()) {
            ActorRef dagBuildActor =
                    ActorUtils.getLocalActor(DAGBuildActor.class, ActorUtils.getActorRefName(DAGBuildActor.class));
            dagBuildActor.tell(
                    new StartExecuteCommandCommand(commandIds, clusterId, commandType),
                    ActorRef.noSender());
        }
    }
}
