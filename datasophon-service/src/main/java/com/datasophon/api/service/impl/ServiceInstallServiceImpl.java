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

import com.datasophon.api.exceptions.ServiceException;
import com.datasophon.api.load.ServiceConfigMap;
import com.datasophon.api.load.ServiceInfoMap;
import com.datasophon.api.load.ServiceRoleMap;
import com.datasophon.api.service.ClusterInfoService;
import com.datasophon.api.service.ClusterServiceCommandHostCommandService;
import com.datasophon.api.service.ClusterServiceCommandService;
import com.datasophon.api.service.FrameServiceRoleService;
import com.datasophon.api.service.FrameServiceService;
import com.datasophon.api.service.ServiceInstallService;
import com.datasophon.api.strategy.ServiceRoleStrategy;
import com.datasophon.api.strategy.ServiceRoleStrategyContext;
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
import com.datasophon.common.utils.Result;
import com.datasophon.dao.entity.ClusterInfoEntity;
import com.datasophon.dao.entity.ClusterServiceCommandEntity;
import com.datasophon.dao.entity.ClusterServiceCommandHostCommandEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceRoleGroup;
import com.datasophon.dao.entity.ClusterServiceRoleGroupConfig;
import com.datasophon.dao.entity.FrameServiceEntity;
import com.datasophon.dao.entity.FrameServiceRoleEntity;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.alibaba.fastjson.JSONObject;

import cn.hutool.core.io.FileUtil;

@Service("serviceInstallService")
public class ServiceInstallServiceImpl implements ServiceInstallService {

    private static final Logger logger = LoggerFactory.getLogger(ServiceInstallServiceImpl.class);

    private static final String PROMETHEUS = "prometheus";

    private static final String USE_ROLE_GROUP_PREFIX = "UseRoleGroup_";

    @Autowired
    private ClusterInfoService clusterInfoService;

    @Autowired
    private FrameServiceService frameService;

    @Autowired
    private FrameServiceRoleService frameServiceRoleService;

    @Autowired
    private ClusterServiceCommandService commandService;

    @Autowired
    private ClusterServiceCommandHostCommandService hostCommandService;

    @Autowired
    private ServiceConfigResolver configResolver;

    @Autowired
    private ServiceConfigPersistenceHandler persistenceHandler;

    @Autowired
    private ServiceRoleValidator validator;

    @Override
    public Result getServiceConfigOption(Integer clusterId, String serviceName) {
        if (Objects.isNull(clusterId)) {
            return Result.error("clusterId is required");
        }
        if (StringUtils.isBlank(serviceName)) {
            return Result.error("serviceName is required");
        }
        List<ServiceConfig> list = configResolver.resolveConfigOptions(clusterId, serviceName);
        return Result.success(list);
    }

    @Override
    @Transactional
    public Result saveServiceConfig(
            Integer clusterId, String serviceName, List<ServiceConfig> list,
            Integer roleGroupId) {
        if (Objects.isNull(clusterId)) {
            return Result.error("clusterId is required");
        }
        if (StringUtils.isBlank(serviceName)) {
            return Result.error("serviceName is required");
        }
        if (list == null || list.isEmpty()) {
            return Result.error("service config list is empty");
        }

        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        if (Objects.isNull(clusterInfo)) {
            return Result.error("cluster not found: " + clusterId);
        }

        FrameServiceEntity frameServiceEntity =
                frameService.getServiceByFrameCodeAndServiceName(
                        clusterInfo.getClusterFrame(), serviceName);
        if (Objects.isNull(frameServiceEntity)) {
            return Result.error("unknown service: " + serviceName);
        }

        // Apply strategy handler
        ServiceRoleStrategy serviceRoleHandler =
                ServiceRoleStrategyContext.getServiceRoleHandler(serviceName);
        if (Objects.nonNull(serviceRoleHandler)) {
            serviceRoleHandler.handlerConfig(
                    clusterId, list, ServiceRoleStrategyContext.getServiceName(serviceName));
        }

        // Resolve global variables and persist INPUT-type configs
        configResolver.resolveGlobalVariables(clusterId, serviceName, list);

        // Cache configs
        ServiceConfigMap.put(
                clusterInfo.getClusterCode() + Constants.UNDERLINE + serviceName + Constants.CONFIG,
                list);

        // Build config name map
        HashMap<String, ServiceConfig> configNameMap = new HashMap<>();
        for (ServiceConfig serviceConfig : list) {
            configNameMap.put(serviceConfig.getName(), serviceConfig);
        }

        // Build config file map
        HashMap<Generators, List<ServiceConfig>> configFileMap =
                configResolver.buildConfigFileMap(serviceName, clusterInfo, configNameMap);

        // Prometheus-specific: add host scrape targets
        if (PROMETHEUS.equals(serviceName.toLowerCase())) {
            logger.info("add worker and node to prometheus");
            configResolver.addHostNodeToPrometheus(clusterId, configFileMap);
        }

        // Persist
        ClusterServiceInstanceEntity serviceInstanceEntity =
                getServiceInstance(clusterId, serviceName);

        if (Objects.isNull(serviceInstanceEntity)) {
            saveNewServiceConfig(clusterId, serviceName, list, configFileMap, frameServiceEntity);
        } else {
            updateExistingServiceConfig(
                    clusterId, serviceName, list, configFileMap, frameServiceEntity,
                    serviceInstanceEntity, roleGroupId);
        }

        return Result.success();
    }

    private ClusterServiceInstanceEntity getServiceInstance(Integer clusterId, String serviceName) {
        return persistenceHandler.getServiceInstanceByClusterIdAndServiceName(clusterId, serviceName);
    }

    private void saveNewServiceConfig(
            Integer clusterId, String serviceName, List<ServiceConfig> list,
            HashMap<Generators, List<ServiceConfig>> configFileMap,
            FrameServiceEntity frameServiceEntity) {
        ClusterServiceInstanceEntity serviceInstanceEntity =
                persistenceHandler.createServiceInstance(clusterId, serviceName, frameServiceEntity);
        ClusterServiceInstanceRoleGroup roleGroup =
                persistenceHandler.createDefaultRoleGroup(clusterId, serviceName, serviceInstanceEntity);
        persistenceHandler.saveRoleGroupConfig(
                clusterId, serviceName, list, configFileMap, roleGroup.getId(), 1);
        CacheUtils.put(
                USE_ROLE_GROUP_PREFIX + serviceInstanceEntity.getId(),
                roleGroup.getId());
    }

    private void updateExistingServiceConfig(
            Integer clusterId, String serviceName, List<ServiceConfig> list,
            HashMap<Generators, List<ServiceConfig>> configFileMap,
            FrameServiceEntity frameServiceEntity,
            ClusterServiceInstanceEntity serviceInstanceEntity,
            Integer roleGroupId) {
        boolean configChanged = persistenceHandler.isConfigChanged(serviceInstanceEntity, list);

        // Resolve current role group config
        ClusterServiceRoleGroupConfig roleGroupConfig;
        if (Objects.isNull(roleGroupId)) {
            ClusterServiceInstanceRoleGroup roleGroup =
                    persistenceHandler.getDefaultRoleGroup(serviceInstanceEntity.getId());
            if (Objects.isNull(roleGroup)) {
                throw new ServiceException(
                        "default role group not found for service: " + serviceName);
            }
            roleGroupConfig = persistenceHandler.getConfigByRoleGroupId(roleGroup.getId());
        } else {
            roleGroupConfig = persistenceHandler.getConfigByRoleGroupId(roleGroupId);
        }

        if (Objects.nonNull(roleGroupConfig)) {
            CacheUtils.put(
                    USE_ROLE_GROUP_PREFIX + serviceInstanceEntity.getId(),
                    roleGroupConfig.getRoleGroupId());
        }

        if (configChanged) {
            if (Objects.isNull(roleGroupId)) {
                // No specific role group → create a new auto-named role group
                ClusterServiceInstanceRoleGroup newRoleGroup =
                        persistenceHandler.createAutoRoleGroup(serviceInstanceEntity);
                persistenceHandler.saveRoleGroupConfig(
                        clusterId, serviceName, list, configFileMap, newRoleGroup.getId(), 1);
                CacheUtils.put(
                        USE_ROLE_GROUP_PREFIX + serviceInstanceEntity.getId(),
                        newRoleGroup.getId());
            } else {
                // Specific role group → increment version, mark need restart
                int newVersion = (roleGroupConfig != null)
                        ? roleGroupConfig.getConfigVersion() + 1 : 1;
                persistenceHandler.saveRoleGroupConfig(
                        clusterId, serviceName, list, configFileMap,
                        roleGroupId, newVersion);
                persistenceHandler.markNeedRestart(roleGroupId, serviceInstanceEntity);
            }
        }

        // Update service instance timestamp and label
        persistenceHandler.updateServiceInstance(serviceInstanceEntity, frameServiceEntity);
    }

    @Override
    @Transactional
    public Result saveServiceRoleHostMapping(Integer clusterId, List<ServiceRoleHostMapping> list) {
        if (Objects.isNull(clusterId)) {
            return Result.error("clusterId is required");
        }
        if (list == null || list.isEmpty()) {
            return Result.error("service role host mapping list is empty");
        }

        validator.validateSameNodeConstraint(clusterId, list);

        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        if (Objects.isNull(clusterInfo)) {
            return Result.error("cluster not found: " + clusterId);
        }
        String hostMapKey =
                clusterInfo.getClusterCode()
                        + Constants.UNDERLINE
                        + Constants.SERVICE_ROLE_HOST_MAPPING;
        HashMap<String, List<String>> map = new HashMap<>();
        if (CacheUtils.constainsKey(hostMapKey)) {
            map = (HashMap<String, List<String>>) CacheUtils.get(hostMapKey);
        }

        for (ServiceRoleHostMapping serviceRoleHostMapping : list) {
            validator.validateRoleHostMapping(serviceRoleHostMapping);

            map.put(serviceRoleHostMapping.getServiceRole(), serviceRoleHostMapping.getHosts());

            ServiceRoleStrategy serviceRoleHandler =
                    ServiceRoleStrategyContext.getServiceRoleHandler(
                            serviceRoleHostMapping.getServiceRole());
            String serviceName = ServiceRoleStrategyContext.getServiceName(
                    serviceRoleHostMapping.getServiceRole());
            if (Objects.nonNull(serviceRoleHandler)) {
                serviceRoleHandler.handler(clusterId, serviceRoleHostMapping.getHosts(), serviceName);
            }
        }

        CacheUtils.put(hostMapKey, map);
        return Result.success();
    }

    @Override
    public Result saveHostServiceRoleMapping(Integer clusterId, List<HostServiceRoleMapping> list) {
        if (Objects.isNull(clusterId)) {
            return Result.error("clusterId is required");
        }
        if (list == null || list.isEmpty()) {
            return Result.error("host service role mapping list is empty");
        }
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        if (Objects.isNull(clusterInfo)) {
            return Result.error("cluster not found: " + clusterId);
        }
        HashMap<String, List<String>> map = new HashMap<>();
        for (HostServiceRoleMapping hostServiceRoleMapping : list) {
            map.put(hostServiceRoleMapping.getHost(), hostServiceRoleMapping.getServiceRoles());
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
        if (Objects.isNull(clusterId)) {
            return Result.error("clusterId is required");
        }
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        if (Objects.isNull(clusterInfo)) {
            return Result.error("cluster not found: " + clusterId);
        }
        HashMap<String, List<String>> map =
                (HashMap<String, List<String>>) CacheUtils.get(
                        clusterInfo.getClusterCode()
                                + Constants.UNDERLINE
                                + Constants.SERVICE_ROLE_HOST_MAPPING);
        return Result.success(map);
    }

    @Override
    public Result startInstallService(Integer clusterId, List<String> commandIds) {
        if (Objects.isNull(clusterId)) {
            return Result.error("clusterId is required");
        }
        if (commandIds == null || commandIds.isEmpty()) {
            return Result.error("commandIds is empty");
        }
        Collection<ClusterServiceCommandEntity> commands = commandService.listByIds(commandIds);
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        if (Objects.isNull(clusterInfo)) {
            return Result.error("cluster not found: " + clusterId);
        }
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
    public void downloadPackage(String packageName, HttpServletResponse response) throws IOException {
        if (StringUtils.isBlank(packageName)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "packageName is required");
            return;
        }
        File file = new File(Constants.MASTER_MANAGE_PACKAGE_PATH + Constants.SLASH + packageName);
        if (!file.exists()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "package not found: " + packageName);
            return;
        }

        try (FileInputStream inputStream = new FileInputStream(file)) {
            response.reset();
            response.setContentType("application/octet-stream");
            response.addHeader("Content-Length", "" + file.length());
            response.setHeader("Content-Disposition", "attachment;filename=" + packageName);
            OutputStream out = response.getOutputStream();
            int length;
            byte[] buffer = new byte[1024];
            while ((length = inputStream.read(buffer)) != -1) {
                out.write(buffer, 0, length);
            }
            out.flush();
        }
    }

    @Override
    public void downloadResource(String frameCode, String serviceRoleName, String resource,
                                 HttpServletResponse response) throws IOException {
        if (StringUtils.isBlank(frameCode) || StringUtils.isBlank(serviceRoleName)
                || StringUtils.isBlank(resource)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "frameCode, serviceRoleName, and resource are all required");
            return;
        }
        String metaPath = FileUtil.getAbsolutePath(META_PATH);
        FrameServiceRoleEntity entity =
                frameServiceRoleService.getServiceRoleByFrameCodeAndServiceRoleName(
                        frameCode, serviceRoleName);
        if (Objects.isNull(entity)) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND,
                    "service role not found: " + frameCode + "/" + serviceRoleName);
            return;
        }
        ServiceRoleInfo roleInfo =
                JSONObject.parseObject(entity.getServiceRoleJson(), ServiceRoleInfo.class);

        File file = new File(metaPath + Constants.SLASH + frameCode + Constants.SLASH
                + roleInfo.getParentName() + Constants.SLASH + resource);
        if (!file.exists()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND,
                    "resource file not found: " + resource);
            return;
        }

        try (FileInputStream fis = new FileInputStream(file)) {
            response.reset();
            response.setContentType("application/octet-stream");
            response.addHeader("Content-Length", "" + file.length());
            response.setHeader("Content-Disposition", "attachment;filename=" + file.getName());
            OutputStream out = response.getOutputStream();
            int length;
            byte[] buffer = new byte[1024];
            while ((length = fis.read(buffer)) != -1) {
                out.write(buffer, 0, length);
            }
            out.flush();
        }
    }

    @Override
    public Result getServiceRoleHostMapping(Integer clusterId) {
        return null;
    }

    @Override
    public Result checkServiceDependency(Integer clusterId, String serviceIds) {
        if (Objects.isNull(clusterId)) {
            return Result.error("clusterId is required");
        }
        return validator.validateServiceDependencies(clusterId, serviceIds);
    }
}
