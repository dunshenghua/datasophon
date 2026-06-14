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
import com.datasophon.api.service.command.CommandBundleBuilder;
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
import com.datasophon.dao.enums.CommandState;
import com.datasophon.dao.mapper.ClusterServiceCommandMapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
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
    private ClusterServiceInstanceService serviceInstanceService;

    @Autowired
    private ClusterServiceRoleInstanceService roleInstanceService;

    @Override
    @Transactional
    public Result generateCommand(Integer clusterId, CommandType commandType, List<String> serviceNames) {
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);

        CommandBundleBuilder builder = new CommandBundleBuilder();

        Map<String, List<String>> serviceRoleHostMap = (Map<String, List<String>>) CacheUtils
                .get(clusterInfo.getClusterCode() + Constants.UNDERLINE + Constants.SERVICE_ROLE_HOST_MAPPING);

        for (String serviceName : serviceNames) {
            // 1、查询服务实例
            ClusterServiceInstanceEntity serviceInstance =
                    serviceInstanceService.getServiceInstanceByClusterIdAndServiceName(clusterId, serviceName);

            // 2、生成操作指令
            ClusterServiceCommandEntity commandEntity =
                    builder.addCommand(clusterId, commandType, serviceName, serviceInstance.getId());
            String commandId = commandEntity.getCommandId();

            // 3、查询服务的服务角色
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
                        if (alreadyExistsServiceRole(serviceRole.getServiceRoleName(), hostname, clusterId)) {
                            continue;
                        }
                        // 4、生成主机操作指令（主机去重由 builder 内部处理）
                        builder.addHostCommand(commandEntity, hostname,
                                serviceRole.getServiceRoleName(), serviceRole.getServiceRoleType(), commandType);
                    }
                }
            }
        }
        if (builder.isHostEmpty()) {
            logger.warn("No service role selected");
            return Result.error(Status.NO_SERVICE_ROLE_SELECTED.getMsg());
        }
        // generateCommand 仅保存实体，不触发 Actor 执行（保持现有行为）
        builder.saveOnly(this, commandHostService, hostCommandService);
        return Result.success(builder.getCommandIdsAsString());
    }

    private boolean alreadyExistsServiceRole(String serviceRoleName, String hostname, Integer clusterId) {
        ClusterServiceRoleInstanceEntity serviceRole =
                roleInstanceService.getOneServiceRole(serviceRoleName, hostname, clusterId);
        return Objects.nonNull(serviceRole);
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
            commandEntity.setCommandStateCode(commandEntity.getCommandState().getValue());
            Date createTime = commandEntity.getCreateTime();
            Date endTime = commandEntity.getEndTime();
            if (Objects.isNull(endTime)) {
                endTime = new Date();
            }
            long between = DateUtil.between(createTime, endTime, DateUnit.MS);
            String durationTime = DateUtil.formatBetween(between, BetweenFormatter.Level.SECOND);
            commandEntity.setDurationTime(durationTime);
        }
        return Result.success(list).put(Constants.TOTAL, total);
    }

    /**
     * 生成服务实例级操作指令（按服务实例 ID 列表）
     *
     * 1、生成指令
     * 2、生成主机指令
     * 3、生产主机上操作指令
     */
    @Override
    public Result generateServiceCommand(Integer clusterId, CommandType commandType, List<String> serviceInstanceIds) {
        CommandBundleBuilder builder = new CommandBundleBuilder();

        for (String serviceInstanceId : serviceInstanceIds) {
            int id = Integer.parseInt(serviceInstanceId);
            // 查询服务对应的服务角色实例
            List<ClusterServiceRoleInstanceEntity> roleInstanceList =
                    roleInstanceService.getServiceRoleInstanceListByServiceId(id);
            if (Objects.isNull(roleInstanceList) || roleInstanceList.isEmpty()) {
                continue;
            }
            ClusterServiceInstanceEntity serviceInstance = serviceInstanceService.getById(id);
            ClusterServiceCommandEntity commandEntity =
                    builder.addCommand(clusterId, commandType, serviceInstance.getServiceName(), id);

            for (ClusterServiceRoleInstanceEntity roleInstance : roleInstanceList) {
                builder.addHostCommand(commandEntity, roleInstance.getHostname(),
                        roleInstance.getServiceRoleName(), roleInstance.getRoleType(), commandType);
            }
        }

        if (builder.isEmpty()) {
            return Result.error(Status.NO_SERVICE_EXECUTE.getMsg());
        }
        builder.saveAndDispatch(clusterId, commandType, this, commandHostService, hostCommandService);
        return Result.success(builder.getCommandIdsAsString());
    }

    @Override
    public Result generateServiceRoleCommands(Integer clusterId, CommandType commandType,
                                              Map<Integer, List<String>> instanceIdMap) {
        if (instanceIdMap == null || instanceIdMap.isEmpty()) {
            return Result.error(Status.NO_SERVICE_EXECUTE.getMsg());
        }
        // 使用统一 builder 累积所有 serviceInstanceId 的指令，修复之前只返回最后一次结果的 Bug
        CommandBundleBuilder builder = new CommandBundleBuilder();
        for (Map.Entry<Integer, List<String>> entry : instanceIdMap.entrySet()) {
            buildRoleCommandIntoBuilder(builder, clusterId, commandType, entry.getKey(), entry.getValue());
        }
        if (builder.isEmpty()) {
            return Result.error(Status.NO_SERVICE_EXECUTE.getMsg());
        }
        builder.saveAndDispatch(clusterId, commandType, this, commandHostService, hostCommandService);
        return Result.success(builder.getCommandIdsAsString());
    }

    @Override
    public Result generateServiceRoleCommand(Integer clusterId, CommandType commandType, Integer serviceInstanceId,
                                             List<String> serviceRoleInstanceIds) {
        CommandBundleBuilder builder = new CommandBundleBuilder();
        buildRoleCommandIntoBuilder(builder, clusterId, commandType, serviceInstanceId, serviceRoleInstanceIds);

        if (builder.isEmpty()) {
            return Result.error(Status.NO_SERVICE_EXECUTE.getMsg());
        }
        builder.saveAndDispatch(clusterId, commandType, this, commandHostService, hostCommandService);
        return Result.success(builder.getCommandIdsAsString());
    }

    /**
     * 将角色实例级指令填充到 builder 中（供 generateServiceRoleCommand 和 generateServiceRoleCommands 共用）。
     */
    private void buildRoleCommandIntoBuilder(CommandBundleBuilder builder, Integer clusterId,
                                             CommandType commandType, Integer serviceInstanceId,
                                             List<String> serviceRoleInstanceIds) {
        ClusterServiceInstanceEntity serviceInstance = serviceInstanceService.getById(serviceInstanceId);
        ClusterServiceCommandEntity commandEntity =
                builder.addCommand(clusterId, commandType, serviceInstance.getServiceName(), serviceInstanceId);

        for (String serviceRoleInstanceId : serviceRoleInstanceIds) {
            int id = Integer.parseInt(serviceRoleInstanceId);
            ClusterServiceRoleInstanceEntity roleInstance = roleInstanceService.getById(id);
            builder.addHostCommand(commandEntity, roleInstance.getHostname(),
                    roleInstance.getServiceRoleName(), roleInstance.getRoleType(), commandType);
        }
    }

    @Override
    public void startExecuteCommand(Integer clusterId, String commandType, String commandIds) {
        List<String> list = Arrays.asList(commandIds.split(","));
        CommandType command = EnumUtil.fromString(CommandType.class, commandType);
        ActorRef dagBuildActor = getDagBuildActor();
        dagBuildActor.tell(new StartExecuteCommandCommand(list, clusterId, command), ActorRef.noSender());
    }

    @Override
    public void cancelCommand(String commandId) {
        ClusterServiceCommandEntity command = this.getOne(
                new QueryWrapper<ClusterServiceCommandEntity>().eq("command_id", commandId));
        if (command == null) {
            logger.warn("Command not found: {}", commandId);
            return;
        }

        CommandState currentState = command.getCommandState();
        if (currentState == CommandState.SUCCESS || currentState == CommandState.FAILED) {
            logger.warn("Cannot cancel command in terminal state: {}, commandId: {}", currentState, commandId);
            return;
        }

        // 将 command、commandHost、hostCommand 中运行中/待运行的记录状态置为取消
        List<String> commandIds = Collections.singletonList(commandId);
        ProcessUtils.updateCommandStateToFailed(commandIds);

        // 更新顶层 command 实体状态
        command.setCommandState(CommandState.CANCEL);
        command.setEndTime(new Date());
        this.updateById(command);

        // 级联更新 CommandHost 状态
        UpdateWrapper<ClusterServiceCommandHostEntity> hostUpdate = new UpdateWrapper<>();
        hostUpdate.eq("command_id", commandId)
                .in("command_state", CommandState.RUNNING.getValue(), CommandState.WAIT.getValue())
                .set("command_state", CommandState.CANCEL.getValue());
        commandHostService.update(hostUpdate);

        // 级联更新 HostCommand 状态
        UpdateWrapper<ClusterServiceCommandHostCommandEntity> hcUpdate = new UpdateWrapper<>();
        hcUpdate.eq("command_id", commandId)
                .in("command_state", CommandState.RUNNING.getValue(), CommandState.WAIT.getValue())
                .set("command_state", CommandState.CANCEL.getValue());
        hostCommandService.update(hcUpdate);
    }

    @Override
    public ClusterServiceCommandEntity getLastRestartCommand(Integer serviceInstanceId) {
        return this.getOne(
                new QueryWrapper<ClusterServiceCommandEntity>()
                        .in(Constants.COMMAND_TYPE,
                                CommandType.RESTART_SERVICE.getValue(),
                                CommandType.INSTALL_SERVICE.getValue())
                        .eq(Constants.SERVICE_INSTANCE_ID, serviceInstanceId)
                        .orderByDesc(Constants.CREATE_TIME)
                        .last("limit 1"));
    }

    @Override
    public ClusterServiceCommandEntity getCommandById(String commandId) {
        return this.getOne(
                new QueryWrapper<ClusterServiceCommandEntity>().eq("command_id", commandId));
    }

    /**
     * 集中 DAGBuildActor 查找，避免多处重复 ActorUtils.getLocalActor() 调用。
     */
    private ActorRef getDagBuildActor() {
        return ActorUtils.getLocalActor(DAGBuildActor.class, ActorUtils.getActorRefName(DAGBuildActor.class));
    }
}
