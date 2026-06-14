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

import static com.datasophon.common.Constants.META_PATH;

import com.datasophon.api.enums.Status;
import com.datasophon.api.exceptions.ServiceException;
import com.datasophon.api.load.GlobalVariables;
import com.datasophon.api.load.ServiceConfigMap;
import com.datasophon.api.load.ServiceInfoMap;
import com.datasophon.api.load.ServiceRoleMap;
import com.datasophon.api.service.ClusterInfoService;
import com.datasophon.api.service.ClusterServiceCommandHostCommandService;
import com.datasophon.api.service.ClusterServiceCommandService;
import com.datasophon.api.service.ClusterServiceInstanceConfigService;
import com.datasophon.api.service.ClusterServiceInstanceRoleGroupService;
import com.datasophon.api.service.ClusterServiceInstanceService;
import com.datasophon.api.service.ClusterServiceRoleGroupConfigService;
import com.datasophon.api.service.ClusterServiceRoleInstanceService;
import com.datasophon.api.service.FrameInfoService;
import com.datasophon.api.service.FrameServiceRoleService;
import com.datasophon.api.service.FrameServiceService;
import com.datasophon.api.service.ServiceInstallService;
import com.datasophon.api.strategy.ServiceRoleStrategy;
import com.datasophon.api.strategy.ServiceRoleStrategyContext;
import com.datasophon.api.utils.ServiceInstallConstants;
import com.datasophon.common.Constants;
import com.datasophon.common.cache.CacheUtils;
import com.datasophon.common.model.DAG;
import com.datasophon.common.model.Generators;
import com.datasophon.common.model.HostServiceRoleMapping;
import com.datasophon.common.model.ServiceConfig;
import com.datasophon.common.model.ServiceInfo;
import com.datasophon.common.model.ServiceNode;
import com.datasophon.common.model.ServiceNodeEdge;
import com.datasophon.common.model.ServiceRoleHostMapping;
import com.datasophon.common.model.ServiceRoleInfo;
import com.datasophon.common.utils.CollectionUtils;
import com.datasophon.common.utils.Result;
import com.datasophon.dao.entity.ClusterInfoEntity;
import com.datasophon.dao.entity.ClusterServiceCommandEntity;
import com.datasophon.dao.entity.ClusterServiceCommandHostCommandEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceRoleGroup;
import com.datasophon.dao.entity.ClusterServiceRoleGroupConfig;
import com.datasophon.dao.entity.FrameServiceEntity;
import com.datasophon.dao.entity.FrameServiceRoleEntity;
import com.datasophon.dao.enums.NeedRestart;
import com.datasophon.dao.enums.ServiceState;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;

import cn.hutool.core.io.FileUtil;

@Service("serviceInstallService")
@Transactional
public class ServiceInstallServiceImpl implements ServiceInstallService {

    private static final Logger logger = LoggerFactory.getLogger(ServiceInstallServiceImpl.class);

    @Autowired
    private ClusterInfoService clusterInfoService;

    @Autowired
    FrameInfoService frameInfoService;

    @Autowired
    FrameServiceService frameService;

    @Autowired
    FrameServiceRoleService frameServiceRoleService;

    @Autowired
    ClusterServiceCommandService commandService;

    @Autowired
    private ClusterServiceInstanceService serviceInstanceService;

    @Autowired
    private ClusterServiceInstanceConfigService serviceInstanceConfigService;

    @Autowired
    private ClusterServiceCommandHostCommandService hostCommandService;

    @Autowired
    private ClusterServiceInstanceRoleGroupService roleGroupService;

    @Autowired
    private ClusterServiceRoleGroupConfigService groupConfigService;

    @Autowired
    private ClusterServiceRoleInstanceService roleInstanceService;

    @Autowired
    private ServiceConfigLoader configLoader;

    @Autowired
    private ServiceRoleMappingValidator mappingValidator;

    // ==================== Public API Methods ====================

    @Override
    public Result getServiceConfigOption(Integer clusterId, String serviceName) {
        // --- Boundary protection ---
        if (clusterId == null) {
            return Result.error(Status.CLUSTER_ID_IS_NULL.getMsg());
        }
        if (StringUtils.isBlank(serviceName)) {
            return Result.error(Status.SERVICE_NAME_IS_NULL.getMsg());
        }

        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        Map<String, String> globalVariables = safeGlobalVariables(clusterId);

        // --- Config loading (delegated) ---
        List<ServiceConfig> list;
        ClusterServiceInstanceEntity serviceInstance =
                serviceInstanceService.getServiceInstanceByClusterIdAndServiceName(
                        clusterId, serviceName);
        if (Objects.nonNull(serviceInstance)) {
            list = configLoader.loadConfigForExistingInstance(serviceInstance);
        } else {
            list = configLoader.loadConfigFromFrameTemplate(
                    clusterInfo.getClusterFrame(), serviceName, globalVariables);
        }

        // --- Strategy extension point ---
        ServiceRoleStrategy serviceRoleHandler =
                ServiceRoleStrategyContext.getServiceRoleHandler(serviceName);
        if (Objects.nonNull(serviceRoleHandler)) {
            serviceRoleHandler.getConfig(clusterId, list);
        }

        return Result.success(list);
    }

    @Override
    public Result saveServiceConfig(
            Integer clusterId, String serviceName, List<ServiceConfig> list,
            Integer roleGroupId) {
        // --- Boundary protection ---
        if (clusterId == null) {
            return Result.error(Status.CLUSTER_ID_IS_NULL.getMsg());
        }
        if (StringUtils.isBlank(serviceName)) {
            return Result.error(Status.SERVICE_NAME_IS_NULL.getMsg());
        }
        if (CollectionUtils.isEmpty(list)) {
            return Result.error(Status.SERVICE_CONFIG_LIST_IS_EMPTY.getMsg());
        }

        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        FrameServiceEntity frameServiceEntity =
                frameService.getServiceByFrameCodeAndServiceName(
                        clusterInfo.getClusterFrame(), serviceName);
        if (frameServiceEntity == null) {
            return Result.error(
                    Status.SERVICE_NOT_FOUND_IN_FRAME.getMsg().replace("{0}", serviceName));
        }

        // --- Cache config for downstream workers ---
        ServiceConfigMap.put(
                clusterInfo.getClusterCode() + Constants.UNDERLINE + serviceName + Constants.CONFIG,
                list);

        // --- Strategy config handler ---
        Map<String, String> globalVariables = safeGlobalVariables(clusterId);
        ServiceRoleStrategy serviceRoleHandler =
                ServiceRoleStrategyContext.getServiceRoleHandler(serviceName);
        if (Objects.nonNull(serviceRoleHandler)) {
            serviceRoleHandler.handlerConfig(
                    clusterId, list,
                    ServiceRoleStrategyContext.getServiceName(serviceName));
        }

        // --- Process variables (delegated) ---
        Map<String, ServiceConfig> configMap =
                configLoader.processConfigVariables(
                        clusterId, serviceName, list, globalVariables);

        // --- Build config-file map (delegated) ---
        Map<Generators, List<ServiceConfig>> configFileMap =
                configLoader.buildConfigFileMap(
                        clusterInfo.getClusterFrame(), serviceName, configMap);
        if (ServiceInstallConstants.PROMETHEUS_SERVICE_NAME.equalsIgnoreCase(serviceName)) {
            logger.info("add worker and node to prometheus");
            configLoader.addPrometheusHostNodes(clusterId, configFileMap);
        }

        // --- Persistence branch ---
        ClusterServiceInstanceEntity serviceInstanceEntity =
                serviceInstanceService.getServiceInstanceByClusterIdAndServiceName(
                        clusterId, serviceName);
        if (Objects.isNull(serviceInstanceEntity)) {
            handleFirstInstall(
                    clusterId, serviceName, frameServiceEntity, list, configFileMap);
        } else {
            handleConfigUpdate(
                    clusterId, serviceName, serviceInstanceEntity,
                    frameServiceEntity, list, configFileMap, roleGroupId);
        }

        return Result.success();
    }

    @Override
    public Result saveServiceRoleHostMapping(Integer clusterId,
            List<ServiceRoleHostMapping> list) {
        // --- Boundary protection ---
        if (clusterId == null) {
            return Result.error(Status.CLUSTER_ID_IS_NULL.getMsg());
        }
        if (CollectionUtils.isEmpty(list)) {
            return Result.error(Status.ROLE_HOST_MAPPING_LIST_IS_EMPTY.getMsg());
        }

        // --- Validation (delegated) ---
        mappingValidator.checkSameNodeConstraint(clusterId, list);
        for (ServiceRoleHostMapping mapping : list) {
            mappingValidator.validateRoleCardinality(mapping);
        }

        // --- Cache update ---
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        String hostMapKey =
                clusterInfo.getClusterCode()
                        + Constants.UNDERLINE
                        + Constants.SERVICE_ROLE_HOST_MAPPING;
        @SuppressWarnings("unchecked")
        HashMap<String, List<String>> map = CacheUtils.constainsKey(hostMapKey)
                ? (HashMap<String, List<String>>) CacheUtils.get(hostMapKey)
                : new HashMap<>();

        for (ServiceRoleHostMapping mapping : list) {
            map.put(mapping.getServiceRole(), mapping.getHosts());

            // --- Strategy handler ---
            ServiceRoleStrategy serviceRoleHandler =
                    ServiceRoleStrategyContext.getServiceRoleHandler(
                            mapping.getServiceRole());
            String serviceName = ServiceRoleStrategyContext.getServiceName(
                    mapping.getServiceRole());
            if (Objects.nonNull(serviceRoleHandler)) {
                serviceRoleHandler.handler(clusterId, mapping.getHosts(), serviceName);
            }
        }

        CacheUtils.put(hostMapKey, map);
        return Result.success();
    }

    @Override
    public Result saveHostServiceRoleMapping(Integer clusterId,
            List<HostServiceRoleMapping> list) {
        // --- Boundary protection ---
        if (clusterId == null) {
            return Result.error(Status.CLUSTER_ID_IS_NULL.getMsg());
        }
        if (CollectionUtils.isEmpty(list)) {
            return Result.error(Status.HOST_ROLE_MAPPING_LIST_IS_EMPTY.getMsg());
        }

        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        HashMap<String, List<String>> map = new HashMap<>();
        for (HostServiceRoleMapping mapping : list) {
            if (StringUtils.isBlank(mapping.getHost())) {
                logger.warn("Skipping HostServiceRoleMapping with blank host");
                continue;
            }
            map.put(mapping.getHost(), mapping.getServiceRoles());
        }
        CacheUtils.put(
                clusterInfo.getClusterCode()
                        + Constants.UNDERLINE
                        + Constants.HOST_SERVICE_ROLE_MAPPING,
                map);
        return Result.success();
    }

    @Override
    public Result getServiceRoleDeployOverview(Integer clusterId) {
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        @SuppressWarnings("unchecked")
        HashMap<String, List<String>> map =
                (HashMap<String, List<String>>) CacheUtils.get(
                        clusterInfo.getClusterCode()
                                + Constants.UNDERLINE
                                + Constants.SERVICE_ROLE_HOST_MAPPING);
        return Result.success(map);
    }

    @Override
    public Result startInstallService(Integer clusterId, List<String> commandIds) {
        Collection<ClusterServiceCommandEntity> commands = commandService.listByIds(commandIds);
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        DAG<String, ServiceNode, ServiceNodeEdge> dag = new DAG<>();
        for (ClusterServiceCommandEntity command : commands) {
            List<ClusterServiceCommandHostCommandEntity> commandHostList =
                    hostCommandService.getHostCommandListByCommandId(command.getCommandId());
            List<ServiceRoleInfo> masterRoles = new ArrayList<>();
            List<ServiceRoleInfo> elseRoles = new ArrayList<>();
            ServiceNode serviceNode = new ServiceNode();
            String serviceKey =
                    clusterInfo.getClusterFrame() + Constants.UNDERLINE + command.getServiceName();
            ServiceInfo serviceInfo = ServiceInfoMap.get(serviceKey);
            for (ClusterServiceCommandHostCommandEntity hostCommand : commandHostList) {
                String key =
                        clusterInfo.getClusterFrame()
                                + Constants.UNDERLINE
                                + command.getServiceName()
                                + Constants.UNDERLINE
                                + hostCommand.getServiceRoleName();
                ServiceRoleInfo serviceRoleInfo = ServiceRoleMap.get(key);
                serviceRoleInfo.setHostname(hostCommand.getHostname());
                serviceRoleInfo.setHostCommandId(hostCommand.getHostCommandId());
                serviceRoleInfo.setClusterId(clusterId);
                serviceRoleInfo.setParentName(command.getServiceName());
                if (Constants.MASTER.equals(serviceRoleInfo.getRoleType())) {
                    masterRoles.add(serviceRoleInfo);
                } else {
                    elseRoles.add(serviceRoleInfo);
                }
            }
            serviceNode.setMasterRoles(masterRoles);
            serviceNode.setElseRoles(elseRoles);
            dag.addNode(command.getServiceName(), serviceNode);
            if (serviceInfo.getDependencies().size() > 0) {
                for (String dependency : serviceInfo.getDependencies()) {
                    dag.addEdge(dependency, command.getServiceName());
                }
            }
        }
        return Result.success();
    }

    @Override
    public void downloadPackage(String packageName, HttpServletResponse response)
            throws IOException {
        // --- Boundary protection ---
        if (StringUtils.isBlank(packageName)) {
            throw new ServiceException("Package name is required");
        }
        File file = new File(Constants.MASTER_MANAGE_PACKAGE_PATH
                + Constants.SLASH + packageName);
        if (!file.exists()) {
            throw new ServiceException(Status.RESOURCE_FILE_NOT_FOUND.getMsg());
        }

        // --- File streaming ---
        OutputStream out = null;
        try (FileInputStream inputStream = new FileInputStream(file)) {
            response.reset();
            response.setContentType("application/octet-stream");
            response.addHeader("Content-Length", "" + file.length());
            response.setHeader("Content-Disposition",
                    "attachment;filename=" + packageName);
            out = response.getOutputStream();
            int length;
            byte[] buffer = new byte[1024];
            while ((length = inputStream.read(buffer)) != -1) {
                out.write(buffer, 0, length);
            }
        } finally {
            if (out != null) {
                out.flush();
                out.close();
            }
        }
    }

    @Override
    public void downloadResource(String frameCode, String serviceRoleName,
            String resource,
            HttpServletResponse response) throws IOException {
        // --- Boundary protection ---
        FrameServiceRoleEntity entity =
                frameServiceRoleService.getServiceRoleByFrameCodeAndServiceRoleName(
                        frameCode, serviceRoleName);
        if (entity == null) {
            throw new ServiceException(Status.SERVICE_ROLE_NOT_FOUND.getMsg());
        }
        ServiceRoleInfo roleInfo =
                JSONObject.parseObject(entity.getServiceRoleJson(), ServiceRoleInfo.class);
        if (roleInfo == null || roleInfo.getParentName() == null) {
            throw new ServiceException(Status.SERVICE_ROLE_NOT_FOUND.getMsg());
        }

        String metaPath = FileUtil.getAbsolutePath(META_PATH);
        File file = new File(metaPath + Constants.SLASH + frameCode
                + Constants.SLASH + roleInfo.getParentName()
                + Constants.SLASH + resource);
        if (!file.exists()) {
            throw new ServiceException(Status.RESOURCE_FILE_NOT_FOUND.getMsg());
        }

        // --- File streaming ---
        OutputStream out = null;
        try (FileInputStream fis = new FileInputStream(file)) {
            response.reset();
            response.setContentType("application/octet-stream");
            response.addHeader("Content-Length", "" + file.length());
            response.setHeader("Content-Disposition",
                    "attachment;filename=" + file.getName());
            out = response.getOutputStream();
            int length;
            byte[] buffer = new byte[1024];
            while ((length = fis.read(buffer)) != -1) {
                out.write(buffer, 0, length);
            }
        } finally {
            if (out != null) {
                out.flush();
                out.close();
            }
        }
    }

    @Override
    public Result getServiceRoleHostMapping(Integer clusterId) {
        return null;
    }

    @Override
    public Result checkServiceDependency(Integer clusterId, String serviceIds) {
        // --- Boundary protection ---
        if (clusterId == null) {
            return Result.error(Status.CLUSTER_ID_IS_NULL.getMsg());
        }
        if (StringUtils.isBlank(serviceIds)) {
            return Result.error(Status.SERVICE_IDS_IS_NULL.getMsg());
        }

        // --- Build lookup maps ---
        List<ClusterServiceInstanceEntity> serviceInstanceList =
                serviceInstanceService.listRunningServiceInstance(clusterId);
        Map<String, ClusterServiceInstanceEntity> instanceMap =
                serviceInstanceList.stream()
                        .collect(Collectors.toMap(
                                ClusterServiceInstanceEntity::getServiceName,
                                e -> e,
                                (v1, v2) -> v1));

        List<FrameServiceEntity> list = frameService.listServices(serviceIds);
        Map<String, FrameServiceEntity> serviceMap =
                list.stream()
                        .collect(Collectors.toMap(
                                FrameServiceEntity::getServiceName,
                                e -> e,
                                (v1, v2) -> v1));

        // --- Infrastructure dependency check (delegated) ---
        String infraError = mappingValidator.checkInfrastructureDependencies(
                instanceMap, serviceMap);
        if (infraError != null) {
            return Result.error(infraError);
        }

        // --- Dynamic dependency check (delegated) ---
        String dynamicError = mappingValidator.checkDynamicDependencies(
                list, instanceMap, serviceMap);
        if (dynamicError != null) {
            return Result.error(dynamicError);
        }

        return Result.success();
    }

    // ==================== Private Helper Methods ====================

    /**
     * Null-safe access to GlobalVariables. Returns empty map if cluster
     * has no variables loaded yet.
     */
    private Map<String, String> safeGlobalVariables(Integer clusterId) {
        Map<String, String> vars = GlobalVariables.get(clusterId);
        return vars != null ? vars : Collections.emptyMap();
    }

    /**
     * First-time service installation: create service instance, default
     * role group, and initial role group config.
     */
    private void handleFirstInstall(
            Integer clusterId, String serviceName,
            FrameServiceEntity frameServiceEntity,
            List<ServiceConfig> configs,
            Map<Generators, List<ServiceConfig>> configFileMap) {

        ClusterServiceInstanceEntity instance =
                saveServiceInstance(clusterId, serviceName, frameServiceEntity);
        ClusterServiceInstanceRoleGroup defaultGroup =
                saveServiceInstanceRoleGroup(clusterId, serviceName, instance);
        saveInitialRoleGroupConfig(
                clusterId, serviceName, configs, configFileMap, defaultGroup);
        CacheUtils.put("UseRoleGroup_" + instance.getId(), defaultGroup.getId());
    }

    /**
     * Create the initial role group config with version 1.
     */
    private void saveInitialRoleGroupConfig(
            Integer clusterId, String serviceName,
            List<ServiceConfig> configs,
            Map<Generators, List<ServiceConfig>> configFileMap,
            ClusterServiceInstanceRoleGroup roleGroup) {

        ClusterServiceRoleGroupConfig config = new ClusterServiceRoleGroupConfig();
        config.setRoleGroupId(roleGroup.getId());
        config.setClusterId(clusterId);
        config.setCreateTime(new Date());
        config.setUpdateTime(new Date());
        config.setServiceName(serviceName);
        config.setConfigVersion(1);
        configLoader.buildRoleGroupConfig(configs, configFileMap, config);
        groupConfigService.save(config);
    }

    /**
     * Service already installed - check for config changes and create
     * a new role group config version if needed.
     */
    private void handleConfigUpdate(
            Integer clusterId, String serviceName,
            ClusterServiceInstanceEntity serviceInstance,
            FrameServiceEntity frameServiceEntity,
            List<ServiceConfig> configs,
            Map<Generators, List<ServiceConfig>> configFileMap,
            Integer roleGroupId) {

        boolean configChanged = configLoader.isConfigNeedUpdate(serviceInstance, configs);

        ClusterServiceRoleGroupConfig currentConfig =
                resolveCurrentRoleGroupConfig(serviceInstance, roleGroupId);

        if (configChanged) {
            createUpdatedRoleGroupConfig(
                    clusterId, serviceInstance,
                    configs, configFileMap,
                    roleGroupId, currentConfig);
        }

        // Update service instance metadata
        serviceInstance.setUpdateTime(new Date());
        serviceInstance.setLabel(frameServiceEntity.getLabel());
        serviceInstanceService.updateById(serviceInstance);
    }

    /**
     * Resolve which role group config is currently active.
     * If roleGroupId is null, use the default role group.
     * Also caches the active role group ID.
     */
    private ClusterServiceRoleGroupConfig resolveCurrentRoleGroupConfig(
            ClusterServiceInstanceEntity serviceInstance,
            Integer roleGroupId) {

        ClusterServiceRoleGroupConfig config;
        if (Objects.isNull(roleGroupId)) {
            ClusterServiceInstanceRoleGroup group =
                    roleGroupService.getRoleGroupByServiceInstanceId(
                            serviceInstance.getId());
            if (group == null) {
                throw new ServiceException(Status.ROLE_GROUP_NOT_FOUND.getMsg());
            }
            config = groupConfigService.getConfigByRoleGroupId(group.getId());
        } else {
            config = groupConfigService.getConfigByRoleGroupId(roleGroupId);
            if (config == null) {
                throw new ServiceException(Status.ROLE_GROUP_NOT_FOUND.getMsg());
            }
        }
        CacheUtils.put(
                "UseRoleGroup_" + serviceInstance.getId(),
                config.getRoleGroupId());
        return config;
    }

    /**
     * Create a new versioned role group config when config has changed.
     * If roleGroupId is null: create a new "auto" role group (version 1).
     * If roleGroupId is specified: increment version, mark restart needed.
     */
    private void createUpdatedRoleGroupConfig(
            Integer clusterId,
            ClusterServiceInstanceEntity serviceInstance,
            List<ServiceConfig> configs,
            Map<Generators, List<ServiceConfig>> configFileMap,
            Integer roleGroupId,
            ClusterServiceRoleGroupConfig currentConfig) {

        ClusterServiceRoleGroupConfig newConfig = new ClusterServiceRoleGroupConfig();
        newConfig.setClusterId(clusterId);
        newConfig.setCreateTime(new Date());
        newConfig.setUpdateTime(new Date());
        newConfig.setServiceName(serviceInstance.getServiceName());

        if (Objects.isNull(roleGroupId)) {
            // New auto role group
            ClusterServiceInstanceRoleGroup newGroup = saveNewRoleGroup(serviceInstance);
            newConfig.setConfigVersion(1);
            newConfig.setRoleGroupId(newGroup.getId());
            CacheUtils.put(
                    "UseRoleGroup_" + serviceInstance.getId(), newGroup.getId());
        } else {
            // Increment version on existing role group
            newConfig.setConfigVersion(currentConfig.getConfigVersion() + 1);
            newConfig.setRoleGroupId(currentConfig.getRoleGroupId());
            roleInstanceService.updateToNeedRestart(roleGroupId);
            roleGroupService.updateToNeedRestart(roleGroupId);
            serviceInstance.setNeedRestart(NeedRestart.YES);
        }

        configLoader.buildRoleGroupConfig(configs, configFileMap, newConfig);
        groupConfigService.save(newConfig);
    }

    private ClusterServiceInstanceRoleGroup saveNewRoleGroup(
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

    private ClusterServiceInstanceRoleGroup saveServiceInstanceRoleGroup(
            Integer clusterId,
            String serviceName,
            ClusterServiceInstanceEntity serviceInstanceEntity) {
        ClusterServiceInstanceRoleGroup clusterServiceInstanceRoleGroup =
                new ClusterServiceInstanceRoleGroup();
        clusterServiceInstanceRoleGroup.setServiceInstanceId(serviceInstanceEntity.getId());
        clusterServiceInstanceRoleGroup.setClusterId(clusterId);
        clusterServiceInstanceRoleGroup.setRoleGroupName("默认角色组");
        clusterServiceInstanceRoleGroup.setServiceName(serviceName);
        clusterServiceInstanceRoleGroup.setRoleGroupType("default");
        roleGroupService.save(clusterServiceInstanceRoleGroup);
        return clusterServiceInstanceRoleGroup;
    }

    private ClusterServiceInstanceEntity saveServiceInstance(
            Integer clusterId, String serviceName,
            FrameServiceEntity frameServiceEntity) {
        ClusterServiceInstanceEntity serviceInstanceEntity;
        serviceInstanceEntity = new ClusterServiceInstanceEntity();
        serviceInstanceEntity.setClusterId(clusterId);
        serviceInstanceEntity.setServiceState(ServiceState.WAIT_INSTALL);
        serviceInstanceEntity.setServiceName(serviceName);
        serviceInstanceEntity.setLabel(frameServiceEntity.getLabel());
        serviceInstanceEntity.setCreateTime(new Date());
        serviceInstanceEntity.setUpdateTime(new Date());
        serviceInstanceEntity.setNeedRestart(NeedRestart.NO);
        serviceInstanceEntity.setFrameServiceId(frameServiceEntity.getId());
        serviceInstanceEntity.setSortNum(frameServiceEntity.getSortNum());
        serviceInstanceService.save(serviceInstanceEntity);
        return serviceInstanceEntity;
    }
}
