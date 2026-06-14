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

package com.datasophon.api.controller;

import com.datasophon.api.enums.Status;
import com.datasophon.api.security.UserPermission;
import com.datasophon.api.service.ClusterServiceCommandService;
import com.datasophon.common.enums.CommandType;
import com.datasophon.common.utils.Result;
import com.datasophon.dao.entity.ClusterServiceCommandEntity;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cn.hutool.core.util.EnumUtil;

@RestController
@RequestMapping("api/cluster/service/command")
public class ClusterServiceCommandController {

    @Autowired
    private ClusterServiceCommandService clusterServiceCommandService;

    /**
     * 查询集群服务指令列表
     */
    @RequestMapping("/getServiceCommandlist")
    public Result list(Integer clusterId, Integer page, Integer pageSize) {
        return clusterServiceCommandService.getServiceCommandlist(clusterId, page, pageSize);
    }

    /**
     * 生成服务安装操作指令
     */
    @UserPermission
    @RequestMapping("/generateCommand")
    public Result generateCommand(Integer clusterId, String commandType, String serviceNames) {
        if (StringUtils.isBlank(serviceNames)) {
            return Result.error("serviceNames must not be blank");
        }
        CommandType command = parseCommandType(commandType);
        List<String> list = Arrays.asList(serviceNames.split(","));
        return clusterServiceCommandService.generateCommand(clusterId, command, list);
    }

    /**
     * 生成服务实例操作指令
     */
    @RequestMapping("/generateServiceCommand")
    @UserPermission
    public Result generateServiceCommand(Integer clusterId, String commandType, String serviceInstanceIds) {
        if (StringUtils.isBlank(serviceInstanceIds)) {
            return Result.error(Status.NO_SERVICE_EXECUTE.getMsg());
        }
        CommandType command = parseCommandType(commandType);
        List<String> ids = Arrays.asList(serviceInstanceIds.split(","));
        return clusterServiceCommandService.generateServiceCommand(clusterId, command, ids);
    }

    /**
     * 生成服务角色实例操作指令
     */
    @RequestMapping("/generateServiceRoleCommand")
    @UserPermission
    public Result generateServiceRoleCommand(Integer clusterId, String commandType, Integer serviceInstanceId,
                                             String serviceRoleInstancesIds) {
        if (StringUtils.isBlank(serviceRoleInstancesIds)) {
            return Result.error("serviceRoleInstancesIds must not be blank");
        }
        CommandType command = parseCommandType(commandType);
        List<String> ids = Arrays.asList(serviceRoleInstancesIds.split(","));
        return clusterServiceCommandService.generateServiceRoleCommand(clusterId, command, serviceInstanceId, ids);
    }

    /**
     * 启动执行指令
     */
    @RequestMapping("/startExecuteCommand")
    @UserPermission
    public Result startExecuteCommand(Integer clusterId, String commandType, String commandIds) {
        if (StringUtils.isBlank(commandIds)) {
            return Result.error("commandIds must not be blank");
        }
        clusterServiceCommandService.startExecuteCommand(clusterId, commandType, commandIds);
        return Result.success();
    }

    @RequestMapping("/cancelCommand")
    public Result cancelCommand(String commandId) {
        clusterServiceCommandService.cancelCommand(commandId);
        return Result.success();
    }

    /**
     * 信息
     */
    @RequestMapping("/info/{id}")
    public Result info(@PathVariable("id") Integer id) {
        ClusterServiceCommandEntity clusterServiceCommand = clusterServiceCommandService.getById(id);

        return Result.success().put("clusterServiceCommand", clusterServiceCommand);
    }

    /**
     * 保存
     */
    @RequestMapping("/save")
    public Result save(@RequestBody ClusterServiceCommandEntity clusterServiceCommand) {
        clusterServiceCommandService.save(clusterServiceCommand);

        return Result.success();
    }

    /**
     * 修改
     */
    @RequestMapping("/update")
    public Result update(@RequestBody ClusterServiceCommandEntity clusterServiceCommand) {
        clusterServiceCommandService.updateById(clusterServiceCommand);

        return Result.success();
    }

    /**
     * 删除
     */
    @RequestMapping("/delete")
    public Result delete(@RequestBody Integer[] ids) {
        clusterServiceCommandService.removeByIds(Arrays.asList(ids));

        return Result.success();
    }

    /**
     * 统一 CommandType 解析，避免各接口重复处理。
     */
    private CommandType parseCommandType(String commandType) {
        return EnumUtil.fromString(CommandType.class, commandType);
    }
}
