/*
 *
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
 *
 */

package com.datasophon.api.service.impl;

import com.datasophon.api.service.ClusterServiceInstanceRoleGroupService;
import com.datasophon.api.service.ClusterServiceInstanceService;
import com.datasophon.api.service.ClusterServiceRoleGroupConfigService;
import com.datasophon.api.service.ClusterServiceRoleInstanceService;
import com.datasophon.common.Constants;
import com.datasophon.common.model.Generators;
import com.datasophon.common.model.ServiceConfig;
import com.datasophon.dao.entity.ClusterServiceInstanceEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceRoleGroup;
import com.datasophon.dao.entity.ClusterServiceRoleGroupConfig;
import com.datasophon.dao.entity.FrameServiceEntity;
import com.datasophon.dao.enums.NeedRestart;
import com.datasophon.dao.enums.ServiceState;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;

import cn.hutool.crypto.SecureUtil;

@Component
public class ServiceConfigPersistenceHandler {

    private static final Logger logger = LoggerFactory.getLogger(ServiceConfigPersistenceHandler.class);

    @Autowired
    private ClusterServiceInstanceService serviceInstanceService;

    @Autowired
    private ClusterServiceInstanceRoleGroupService roleGroupService;

    @Autowired
    private ClusterServiceRoleGroupConfigService groupConfigService;

    @Autowired
    private ClusterServiceRoleInstanceService roleInstanceService;

    public ClusterServiceInstanceEntity getServiceInstanceByClusterIdAndServiceName(
            Integer clusterId, String serviceName) {
        return serviceInstanceService.getServiceInstanceByClusterIdAndServiceName(clusterId, serviceName);
    }

    public ClusterServiceInstanceEntity createServiceInstance(
            Integer clusterId, String serviceName, FrameServiceEntity frameServiceEntity) {
        ClusterServiceInstanceEntity entity = new ClusterServiceInstanceEntity();
        entity.setClusterId(clusterId);
        entity.setServiceState(ServiceState.WAIT_INSTALL);
        entity.setServiceName(serviceName);
        entity.setLabel(frameServiceEntity.getLabel());
        entity.setCreateTime(new Date());
        entity.setUpdateTime(new Date());
        entity.setNeedRestart(NeedRestart.NO);
        entity.setFrameServiceId(frameServiceEntity.getId());
        entity.setSortNum(frameServiceEntity.getSortNum());
        serviceInstanceService.save(entity);
        return entity;
    }

    public ClusterServiceInstanceRoleGroup createDefaultRoleGroup(
            Integer clusterId, String serviceName,
            ClusterServiceInstanceEntity serviceInstanceEntity) {
        ClusterServiceInstanceRoleGroup roleGroup = new ClusterServiceInstanceRoleGroup();
        roleGroup.setServiceInstanceId(serviceInstanceEntity.getId());
        roleGroup.setClusterId(clusterId);
        roleGroup.setRoleGroupName("默认角色组");
        roleGroup.setServiceName(serviceName);
        roleGroup.setRoleGroupType("default");
        roleGroupService.save(roleGroup);
        return roleGroup;
    }

    public ClusterServiceInstanceRoleGroup createAutoRoleGroup(
            ClusterServiceInstanceEntity serviceInstanceEntity) {
        int count =
                roleGroupService.count(
                        new QueryWrapper<ClusterServiceInstanceRoleGroup>()
                                .eq(Constants.ROLE_GROUP_TYPE, "auto")
                                .eq(Constants.SERVICE_INSTANCE_ID, serviceInstanceEntity.getId()));
        ClusterServiceInstanceRoleGroup roleGroup = new ClusterServiceInstanceRoleGroup();
        int num = count + 1;
        roleGroup.setRoleGroupName("RoleGroup" + num);
        roleGroup.setServiceInstanceId(serviceInstanceEntity.getId());
        roleGroup.setServiceName(serviceInstanceEntity.getServiceName());
        roleGroup.setClusterId(serviceInstanceEntity.getClusterId());
        roleGroup.setRoleGroupType("auto");
        roleGroupService.save(roleGroup);
        return roleGroup;
    }

    public void saveRoleGroupConfig(
            Integer clusterId, String serviceName,
            List<ServiceConfig> configList,
            HashMap<Generators, List<ServiceConfig>> configFileMap,
            Integer roleGroupId, int configVersion) {
        ClusterServiceRoleGroupConfig roleGroupConfig = new ClusterServiceRoleGroupConfig();
        roleGroupConfig.setRoleGroupId(roleGroupId);
        roleGroupConfig.setClusterId(clusterId);
        roleGroupConfig.setCreateTime(new Date());
        roleGroupConfig.setUpdateTime(new Date());
        roleGroupConfig.setServiceName(serviceName);
        roleGroupConfig.setConfigVersion(configVersion);
        buildConfig(configList, configFileMap, roleGroupConfig);
        groupConfigService.save(roleGroupConfig);
    }

    public boolean isConfigChanged(
            ClusterServiceInstanceEntity serviceInstanceEntity,
            List<ServiceConfig> newConfigList) {
        List<ServiceConfig> originalConfigs =
                listServiceConfigByServiceInstance(serviceInstanceEntity);
        if (originalConfigs == null || originalConfigs.isEmpty()) {
            return true;
        }
        Map<String, Object> originalConfigMap =
                originalConfigs.stream()
                        .collect(
                                Collectors.toMap(
                                        ServiceConfig::getName,
                                        ServiceConfig::getValue,
                                        (v1, v2) -> v1));
        for (ServiceConfig serviceConfig : newConfigList) {
            String configName = serviceConfig.getName();
            String variableValue = String.valueOf(serviceConfig.getValue());
            if (originalConfigMap.containsKey(configName)) {
                String configValue = String.valueOf(originalConfigMap.get(configName));
                if (!variableValue.equals(configValue)) {
                    return true;
                }
            } else {
                return true;
            }
        }
        return false;
    }

    public void markNeedRestart(Integer roleGroupId, ClusterServiceInstanceEntity serviceInstanceEntity) {
        roleInstanceService.updateToNeedRestart(roleGroupId);
        roleGroupService.updateToNeedRestart(roleGroupId);
        serviceInstanceEntity.setNeedRestart(NeedRestart.YES);
    }

    public void updateServiceInstance(ClusterServiceInstanceEntity serviceInstanceEntity, FrameServiceEntity frameServiceEntity) {
        serviceInstanceEntity.setUpdateTime(new Date());
        serviceInstanceEntity.setLabel(frameServiceEntity.getLabel());
        serviceInstanceService.updateById(serviceInstanceEntity);
    }

    public ClusterServiceRoleGroupConfig getConfigByRoleGroupId(Integer roleGroupId) {
        return groupConfigService.getConfigByRoleGroupId(roleGroupId);
    }

    public ClusterServiceInstanceRoleGroup getDefaultRoleGroup(Integer serviceInstanceId) {
        return roleGroupService.getRoleGroupByServiceInstanceId(serviceInstanceId);
    }

    private List<ServiceConfig> listServiceConfigByServiceInstance(
            ClusterServiceInstanceEntity serviceInstance) {
        ClusterServiceInstanceRoleGroup roleGroup =
                roleGroupService.getRoleGroupByServiceInstanceId(serviceInstance.getId());
        if (Objects.isNull(roleGroup)) {
            return null;
        }
        ClusterServiceRoleGroupConfig config =
                groupConfigService.getConfigByRoleGroupId(roleGroup.getId());
        if (Objects.isNull(config) || config.getConfigJson() == null) {
            return null;
        }
        return com.alibaba.fastjson.JSONArray.parseArray(config.getConfigJson(), ServiceConfig.class);
    }

    private void buildConfig(
            List<ServiceConfig> list,
            HashMap<Generators, List<ServiceConfig>> configFileMap,
            ClusterServiceRoleGroupConfig roleGroupConfig) {
        String configJson = (list != null) ? JSONObject.toJSONString(list) : "[]";
        String configFileJson = (configFileMap != null) ? JSONObject.toJSONString(configFileMap) : "{}";
        roleGroupConfig.setConfigJson(configJson);
        roleGroupConfig.setConfigJsonMd5(SecureUtil.md5(configJson));
        roleGroupConfig.setConfigFileJson(configFileJson);
        roleGroupConfig.setConfigFileJsonMd5(SecureUtil.md5(configFileJson));
    }
}
