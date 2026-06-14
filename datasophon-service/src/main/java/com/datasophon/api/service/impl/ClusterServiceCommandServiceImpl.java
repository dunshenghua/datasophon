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

package com.datasophon.api.service.impl;

import com.datasophon.api.enums.Status;
import com.datasophon.api.master.ActorUtils;
import com.datasophon.api.master.DAGBuildActor;
import com.datasophon.api.service.ClusterInfoService;
import com.datasophon.api.service.ClusterServiceCommandHostCommandService;
import com.datasophon.api.service.ClusterServiceCommandHostService;
import com.datasophon.api.service.ClusterServiceCommandService;
import com.datasophon.api.service.ClusterServiceInstanceService;
import com.datasophon.api.service.ClusterServiceRoleInstanceService;
import com.datasophon.api.service.FrameServiceRoleService;
import com.datasophon.api.service.FrameServiceService;
import com.datasophon.api.utils.ProcessUtils;
import com.datasophon.common.Constants;
import com.datasophon.common.cache.CacheUtils;
import com.datasophon.common.command.StartExecuteCommandCommand;
import com.datasophon.common.enums.CommandType;
import com.datasophon.common.utils.Result;
import com.datasophon.dao.entity.ClusterInfoEntity;
import com.datasophon.dao.entity.ClusterServiceCommandEntity;
import com.datasophon.dao.entity.ClusterServiceCommandHostCommandEntity;
import com.datasophon.dao.entity.ClusterServiceCommandHostEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceEntity;
import com.datasophon.dao.entity.ClusterServiceRoleInstanceEntity;
import com.datasophon.dao.entity.FrameServiceEntity;
import com.datasophon.dao.entity.FrameServiceRoleEntity;
import com.datasophon.dao.enums.RoleType;
import com.datasophon.dao.mapper.ClusterServiceCommandMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import akka.actor.ActorRef;

import cn.hutool.core.date.BetweenFormatter;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.EnumUtil;

@Service("clusterServiceCommandService")
public class ClusterServiceCommandServiceImpl
        extends
            ServiceImpl<ClusterServiceCommandMapper, ClusterServiceCommandEntity>
        implements
            ClusterServiceCommandService {
    
    private static final Logger logger = LoggerFactory.getLogger(ClusterServiceCommandServiceImpl.class);
    
    @Autowired
    private ClusterInfoService clusterInfoService;
    
    @Autowired
    private ClusterServiceCommandHostService commandHostService;
    
    @Autowired
    private ClusterServiceCommandHostCommandService hostCommandService;
    
    @Autowired
    private FrameServiceService frameServiceService;
    
    @Autowired
    private FrameServiceRoleService frameServiceRoleService;
    
    @Autowired
    private ClusterServiceCommandService commandService;
    
    @Autowired
    private ClusterServiceInstanceService serviceInstanceService;
    
    @Autowired
    private ClusterServiceRoleInstanceService roleInstanceService;
    
    @Override
    @Transactional
    public Result generateCommand(Integer clusterId, CommandType commandType, List<String> serviceNames) {
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        CommandBuildContext ctx = new CommandBuildContext();

        Map<String, List<String>> serviceRoleHostMap = (Map<String, List<String>>) CacheUtils
                .get(clusterInfo.getClusterCode() + Constants.UNDERLINE + Constants.SERVICE_ROLE_HOST_MAPPING);

        for (String serviceName : serviceNames) {
            ClusterServiceInstanceEntity serviceInstance =
                    serviceInstanceService.getServiceInstanceByClusterIdAndServiceName(clusterId, serviceName);
            ClusterServiceCommandEntity commandEntity =
                    ProcessUtils.generateCommandEntity(clusterId, commandType, serviceName);
            commandEntity.setServiceInstanceId(serviceInstance.getId());
            ctx.beginCommand(commandEntity);

            FrameServiceEntity frameService =
                    frameServiceService.getServiceByFrameCodeAndServiceName(clusterInfo.getClusterFrame(), serviceName);
            Result result =
                    frameServiceRoleService.getServiceRoleList(clusterId, String.valueOf(frameService.getId()), null);
            List<FrameServiceRoleEntity> serviceRoleList = (List<FrameServiceRoleEntity>) result.getData();
            for (FrameServiceRoleEntity serviceRole : serviceRoleList) {
                if (Objects.nonNull(serviceRoleHostMap)
                        && serviceRoleHostMap.containsKey(serviceRole.getServiceRoleName())) {
                    List<String> hosts = serviceRoleHostMap.get(serviceRole.getServiceRoleName());
                    for (String hostname : hosts) {
                        if (!alreadyExistsServiceRole(serviceRole.getServiceRoleName(), hostname, clusterId)) {
                            ctx.addRoleOnHost(commandType, commandEntity.getCommandId(),
                                    hostname, serviceRole.getServiceRoleName(), serviceRole.getServiceRoleType());
                        }
                    }
                }
            }
        }
        if (!ctx.hasHostCommands()) {
            logger.warn("No service role selected");
            return Result.error(Status.NO_SERVICE_ROLE_SELECTED.getMsg());
        }
        persistCommandEntities(ctx);
        return Result.success(ctx.joinedCommandIds());
    }
    
    private boolean alreadyExistsServiceRole(String serviceRoleName, String hostname, Integer clusterId) {
        ClusterServiceRoleInstanceEntity serviceRole =
                roleInstanceService.getOneServiceRole(serviceRoleName, hostname, clusterId);
        if (Objects.nonNull(serviceRole)) {
            return true;
        }
        return false;
    }
    
    @Override
    public Result getServiceCommandlist(Integer clusterId, Integer page, Integer pageSize) {
        Integer offset = (page - 1) * pageSize;
        List<ClusterServiceCommandEntity> list = this.list(new QueryWrapper<ClusterServiceCommandEntity>()
                .eq(Constants.CLUSTER_ID, clusterId)
                .orderByDesc(Constants.CREATE_TIME).last("limit " + offset + "," + pageSize));
        Integer total = this.count(new QueryWrapper<ClusterServiceCommandEntity>()
                .eq(Constants.CLUSTER_ID, clusterId));
        for (ClusterServiceCommandEntity commandEntity : list) {
            populateTransientFields(commandEntity);
        }
        return Result.success(list).put(Constants.TOTAL, total);
    }
    
    /**
     * 1、生成指令
     * 2、生成主机指令
     * 3、生产主机上操作指令
     *
     * @param clusterId
     * @param commandType
     * @param serviceInstanceIds
     * @return
     */
    @Override
    public Result generateServiceCommand(Integer clusterId, CommandType commandType, List<String> serviceInstanceIds) {
        CommandBuildContext ctx = new CommandBuildContext();
        for (String serviceInstanceId : serviceInstanceIds) {
            int id = Integer.parseInt(serviceInstanceId);
            List<ClusterServiceRoleInstanceEntity> roleInstanceList =
                    roleInstanceService.getServiceRoleInstanceListByServiceId(id);
            if (Objects.isNull(roleInstanceList) || roleInstanceList.isEmpty()) {
                continue;
            }
            ClusterServiceInstanceEntity serviceInstance = serviceInstanceService.getById(id);
            ClusterServiceCommandEntity commandEntity =
                    ProcessUtils.generateCommandEntity(clusterId, commandType, serviceInstance.getServiceName());
            commandEntity.setServiceInstanceId(id);
            ctx.beginCommand(commandEntity);

            for (ClusterServiceRoleInstanceEntity roleInstance : roleInstanceList) {
                ctx.addRoleOnHost(commandType, commandEntity.getCommandId(),
                        roleInstance.getHostname(), roleInstance.getServiceRoleName(), roleInstance.getRoleType());
            }
        }
        if (ctx.hasCommands()) {
            persistCommandEntities(ctx);
            dispatchExecution(ctx.commandIds, clusterId, commandType);
        }
        return Result.success(ctx.joinedCommandIds());
    }
    
    @Override
    public Result generateServiceRoleCommands(Integer clusterId, CommandType commandType,
                                              Map<Integer, List<String>> instanceIdMap) {
        Result result = null;
        for (Map.Entry<Integer, List<String>> entry : instanceIdMap.entrySet()) {
            result = generateServiceRoleCommand(clusterId, commandType, entry.getKey(), entry.getValue());
        }
        return result;
    }
    
    @Override
    public Result generateServiceRoleCommand(Integer clusterId, CommandType commandType, Integer serviceInstanceId,
                                             List<String> serviceRoleInstanceIds) {
        CommandBuildContext ctx = new CommandBuildContext();

        ClusterServiceInstanceEntity serviceInstance = serviceInstanceService.getById(serviceInstanceId);
        ClusterServiceCommandEntity commandEntity =
                ProcessUtils.generateCommandEntity(clusterId, commandType, serviceInstance.getServiceName());
        commandEntity.setServiceInstanceId(serviceInstanceId);
        ctx.beginCommand(commandEntity);

        for (String serviceRoleInstanceId : serviceRoleInstanceIds) {
            int id = Integer.parseInt(serviceRoleInstanceId);
            ClusterServiceRoleInstanceEntity roleInstance = roleInstanceService.getById(id);
            ctx.addRoleOnHost(commandType, commandEntity.getCommandId(),
                    roleInstance.getHostname(), roleInstance.getServiceRoleName(), roleInstance.getRoleType());
        }
        persistCommandEntities(ctx);
        dispatchExecution(ctx.commandIds, clusterId, commandType);
        return Result.success(ctx.joinedCommandIds());
    }
    
    @Override
    public void startExecuteCommand(Integer clusterId, String commandType, String commandIds) {
        List<String> list = Arrays.asList(commandIds.split(","));
        CommandType command = EnumUtil.fromString(CommandType.class, commandType);
        dispatchExecution(list, clusterId, command);
    }
    
    @Override
    public void cancelCommand(String commandId) {
        // command , command host, host command状态置为取消
        
    }
    
    @Override
    public ClusterServiceCommandEntity getLastRestartCommand(Integer serviceInstanceId) {
        return this.getOne(
                new QueryWrapper<ClusterServiceCommandEntity>().eq(Constants.SERVICE_INSTANCE_ID, serviceInstanceId)
                        .eq(Constants.COMMAND_TYPE, CommandType.RESTART_SERVICE.getValue()).or()
                        .eq(Constants.COMMAND_TYPE, CommandType.INSTALL_SERVICE.getValue())
                        .orderByDesc(Constants.CREATE_TIME).last("limit 1"));
    }
    
    @Override
    public ClusterServiceCommandEntity getCommandById(String commandId) {
        return this.getOne(
                new QueryWrapper<ClusterServiceCommandEntity>().eq("command_id", commandId));
    }

    private void persistCommandEntities(CommandBuildContext ctx) {
        commandService.saveBatch(ctx.commands);
        commandHostService.saveBatch(ctx.commandHosts);
        hostCommandService.saveBatch(ctx.hostCommands);
    }

    private void dispatchExecution(List<String> commandIds, Integer clusterId, CommandType commandType) {
        ActorRef dagBuildActor =
                ActorUtils.getLocalActor(DAGBuildActor.class, ActorUtils.getActorRefName(DAGBuildActor.class));
        dagBuildActor.tell(new StartExecuteCommandCommand(commandIds, clusterId, commandType), ActorRef.noSender());
    }

    private void populateTransientFields(ClusterServiceCommandEntity entity) {
        entity.setCommandStateCode(entity.getCommandState().getValue());
        Date endTime = entity.getEndTime() != null ? entity.getEndTime() : new Date();
        long between = DateUtil.between(entity.getCreateTime(), endTime, DateUnit.MS);
        entity.setDurationTime(DateUtil.formatBetween(between, BetweenFormatter.Level.SECOND));
    }

    private static class CommandBuildContext {

        final List<ClusterServiceCommandEntity> commands = new ArrayList<>();
        final List<ClusterServiceCommandHostEntity> commandHosts = new ArrayList<>();
        final List<ClusterServiceCommandHostCommandEntity> hostCommands = new ArrayList<>();
        final List<String> commandIds = new ArrayList<>();
        private Map<String, ClusterServiceCommandHostEntity> currentHostMap = new HashMap<>();

        void beginCommand(ClusterServiceCommandEntity command) {
            commands.add(command);
            commandIds.add(command.getCommandId());
            currentHostMap = new HashMap<>();
        }

        void addRoleOnHost(CommandType commandType, String commandId,
                           String hostname, String serviceRoleName, RoleType serviceRoleType) {
            ClusterServiceCommandHostEntity commandHost = currentHostMap.get(hostname);
            if (commandHost == null) {
                commandHost = ProcessUtils.generateCommandHostEntity(commandId, hostname);
                commandHosts.add(commandHost);
                currentHostMap.put(hostname, commandHost);
            }
            ClusterServiceCommandHostCommandEntity hostCommand =
                    ProcessUtils.generateCommandHostCommandEntity(
                            commandType, commandId, serviceRoleName, serviceRoleType, commandHost);
            hostCommands.add(hostCommand);
        }

        boolean hasCommands() {
            return !commands.isEmpty();
        }

        boolean hasHostCommands() {
            return !hostCommands.isEmpty();
        }

        String joinedCommandIds() {
            return String.join(",", commandIds);
        }
    }
}
