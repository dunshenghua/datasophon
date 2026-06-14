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

import com.datasophon.api.exceptions.ServiceException;
import com.datasophon.api.load.GlobalVariables;
import com.datasophon.api.service.ClusterInfoService;
import com.datasophon.api.service.ClusterServiceInstanceRoleGroupService;
import com.datasophon.api.service.ClusterServiceInstanceService;
import com.datasophon.api.service.ClusterServiceRoleGroupConfigService;
import com.datasophon.api.service.ClusterVariableService;
import com.datasophon.api.service.FrameServiceService;
import com.datasophon.api.service.host.ClusterHostService;
import com.datasophon.api.strategy.ServiceRoleStrategy;
import com.datasophon.api.strategy.ServiceRoleStrategyContext;
import com.datasophon.common.Constants;
import com.datasophon.common.model.Generators;
import com.datasophon.common.model.ServiceConfig;
import com.datasophon.common.utils.PlaceholderUtils;
import com.datasophon.dao.entity.ClusterHostDO;
import com.datasophon.dao.entity.ClusterInfoEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceRoleGroup;
import com.datasophon.dao.entity.ClusterServiceRoleGroupConfig;
import com.datasophon.dao.entity.ClusterVariable;
import com.datasophon.dao.entity.FrameServiceEntity;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;

@Component
public class ServiceConfigResolver {

    private static final Logger logger = LoggerFactory.getLogger(ServiceConfigResolver.class);

    @Autowired
    private ClusterInfoService clusterInfoService;

    @Autowired
    private FrameServiceService frameService;

    @Autowired
    private ClusterVariableService variableService;

    @Autowired
    private ClusterHostService hostService;

    @Autowired
    private ClusterServiceInstanceService serviceInstanceService;

    @Autowired
    private ClusterServiceInstanceRoleGroupService roleGroupService;

    @Autowired
    private ClusterServiceRoleGroupConfigService groupConfigService;

    public List<ServiceConfig> resolveConfigOptions(Integer clusterId, String serviceName) {
        ClusterInfoEntity clusterInfo = clusterInfoService.getById(clusterId);
        if (Objects.isNull(clusterInfo)) {
            throw new ServiceException("cluster not found: " + clusterId);
        }

        Map<String, String> globalVariables = GlobalVariables.get(clusterId);

        ClusterServiceInstanceEntity serviceInstance =
                serviceInstanceService.getServiceInstanceByClusterIdAndServiceName(
                        clusterId, serviceName);

        List<ServiceConfig> list;
        if (Objects.nonNull(serviceInstance)) {
            list = listServiceConfigByServiceInstance(serviceInstance);
        } else {
            list = loadConfigFromFrame(clusterInfo, serviceName, globalVariables);
        }

        applyStrategyGetConfig(serviceName, clusterId, list);
        return list;
    }

    private List<ServiceConfig> loadConfigFromFrame(
            ClusterInfoEntity clusterInfo, String serviceName, Map<String, String> globalVariables) {
        FrameServiceEntity frameServiceEntity =
                frameService.getServiceByFrameCodeAndServiceName(
                        clusterInfo.getClusterFrame(), serviceName);
        if (Objects.isNull(frameServiceEntity)) {
            throw new ServiceException("unknown service: " + serviceName);
        }
        String serviceConfig = frameServiceEntity.getServiceConfig();
        if (StringUtils.isBlank(serviceConfig)) {
            return new ArrayList<>();
        }
        serviceConfig =
                PlaceholderUtils.replacePlaceholders(
                        serviceConfig, globalVariables, Constants.REGEX_VARIABLE);
        return JSONArray.parseArray(serviceConfig, ServiceConfig.class);
    }

    List<ServiceConfig> listServiceConfigByServiceInstance(
            ClusterServiceInstanceEntity serviceInstance) {
        ClusterServiceInstanceRoleGroup roleGroup =
                roleGroupService.getRoleGroupByServiceInstanceId(serviceInstance.getId());
        if (Objects.isNull(roleGroup)) {
            throw new ServiceException("default role group not found for service: " + serviceInstance.getServiceName());
        }
        ClusterServiceRoleGroupConfig config =
                groupConfigService.getConfigByRoleGroupId(roleGroup.getId());
        if (Objects.isNull(config) || StringUtils.isBlank(config.getConfigJson())) {
            return new ArrayList<>();
        }
        return JSONArray.parseArray(config.getConfigJson(), ServiceConfig.class);
    }

    private void applyStrategyGetConfig(String serviceName, Integer clusterId, List<ServiceConfig> list) {
        ServiceRoleStrategy serviceRoleHandler =
                ServiceRoleStrategyContext.getServiceRoleHandler(serviceName);
        if (Objects.nonNull(serviceRoleHandler)) {
            serviceRoleHandler.getConfig(clusterId, list);
        }
    }

    public void resolveGlobalVariables(
            Integer clusterId, String serviceName, List<ServiceConfig> list) {
        Map<String, String> globalVariables = GlobalVariables.get(clusterId);
        for (ServiceConfig serviceConfig : list) {
            String configName = serviceConfig.getName();
            if (StringUtils.isBlank(configName)) {
                continue;
            }
            String variableName = "${" + configName + "}";
            String variableValue = String.valueOf(serviceConfig.getValue());
            if (Constants.INPUT.equals(serviceConfig.getType())) {
                addToGlobalVariable(clusterId, serviceName, variableName, variableValue);
            }
            globalVariables.put(variableName, variableValue);
        }
    }

    private void addToGlobalVariable(
            Integer clusterId, String serviceName, String variableName, String value) {
        ClusterVariable clusterVariable =
                variableService.getVariableByVariableName(variableName, clusterId);
        if (Objects.nonNull(clusterVariable)) {
            if (!value.equals(clusterVariable.getVariableValue())) {
                clusterVariable.setServiceName(serviceName);
                clusterVariable.setVariableValue(value);
                variableService.updateById(clusterVariable);
            }
        } else {
            clusterVariable = new ClusterVariable();
            clusterVariable.setClusterId(clusterId);
            clusterVariable.setServiceName(serviceName);
            clusterVariable.setVariableName(variableName);
            clusterVariable.setVariableValue(value);
            variableService.save(clusterVariable);
        }
    }

    public HashMap<Generators, List<ServiceConfig>> buildConfigFileMap(
            String serviceName,
            ClusterInfoEntity clusterInfo,
            Map<String, ServiceConfig> configMap) {
        HashMap<Generators, List<ServiceConfig>> configFileMap = new HashMap<>();
        FrameServiceEntity frameServiceEntity =
                frameService.getServiceByFrameCodeAndServiceName(
                        clusterInfo.getClusterFrame(), serviceName);
        if (Objects.isNull(frameServiceEntity)
                || StringUtils.isBlank(frameServiceEntity.getConfigFileJson())) {
            return configFileMap;
        }
        Map<JSONObject, JSONArray> rawConfigMap =
                JSONObject.parseObject(frameServiceEntity.getConfigFileJson(), Map.class);
        for (JSONObject fileJson : rawConfigMap.keySet()) {
            Generators generators = fileJson.toJavaObject(Generators.class);
            List<ServiceConfig> serviceConfigs =
                    rawConfigMap.get(fileJson).toJavaList(ServiceConfig.class);
            for (ServiceConfig config : serviceConfigs) {
                logger.info(config.getName());
                if (configMap.containsKey(config.getName())) {
                    ServiceConfig newConfig = configMap.get(config.getName());
                    config.setValue(newConfig.getValue());
                    config.setHidden(newConfig.isHidden());
                    config.setRequired(newConfig.isRequired());
                }
            }
            configFileMap.put(generators, serviceConfigs);
        }
        return configFileMap;
    }

    public void addHostNodeToPrometheus(
            Integer clusterId, HashMap<Generators, List<ServiceConfig>> configFileMap) {
        List<ClusterHostDO> hostList =
                hostService.list(
                        new QueryWrapper<ClusterHostDO>()
                                .eq(Constants.MANAGED, 1)
                                .eq(Constants.CLUSTER_ID, clusterId));
        if (hostList == null || hostList.isEmpty()) {
            logger.info("no managed hosts found for cluster {}, skip prometheus host injection", clusterId);
            return;
        }

        Generators workerGenerators = new Generators();
        workerGenerators.setFilename("worker.json");
        workerGenerators.setOutputDirectory("configs");
        workerGenerators.setConfigFormat("custom");
        workerGenerators.setTemplateName("scrape.ftl");

        Generators nodeGenerators = new Generators();
        nodeGenerators.setFilename("linux.json");
        nodeGenerators.setOutputDirectory("configs");
        nodeGenerators.setConfigFormat("custom");
        nodeGenerators.setTemplateName("scrape.ftl");

        ArrayList<ServiceConfig> workerServiceConfigs = new ArrayList<>();
        ArrayList<ServiceConfig> nodeServiceConfigs = new ArrayList<>();
        for (ClusterHostDO clusterHostDO : hostList) {
            ServiceConfig serviceConfig = new ServiceConfig();
            serviceConfig.setName("worker_" + clusterHostDO.getHostname());
            serviceConfig.setValue(clusterHostDO.getHostname() + ":8585");
            serviceConfig.setRequired(true);
            workerServiceConfigs.add(serviceConfig);

            ServiceConfig nodeServiceConfig = new ServiceConfig();
            nodeServiceConfig.setName("node_" + clusterHostDO.getHostname());
            nodeServiceConfig.setValue(clusterHostDO.getHostname() + ":9100");
            nodeServiceConfig.setRequired(true);
            nodeServiceConfigs.add(nodeServiceConfig);
        }
        configFileMap.put(workerGenerators, workerServiceConfigs);
        configFileMap.put(nodeGenerators, nodeServiceConfigs);
    }
}
