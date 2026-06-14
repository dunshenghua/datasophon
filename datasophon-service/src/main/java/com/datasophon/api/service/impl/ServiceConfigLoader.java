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
import com.datasophon.api.exceptions.ServiceException;
import com.datasophon.api.service.ClusterServiceInstanceRoleGroupService;
import com.datasophon.api.service.ClusterServiceRoleGroupConfigService;
import com.datasophon.api.service.ClusterVariableService;
import com.datasophon.api.service.FrameServiceService;
import com.datasophon.api.service.host.ClusterHostService;
import com.datasophon.api.utils.ServiceInstallConstants;
import com.datasophon.common.Constants;
import com.datasophon.common.model.Generators;
import com.datasophon.common.model.ServiceConfig;
import com.datasophon.common.utils.PlaceholderUtils;
import com.datasophon.dao.entity.ClusterHostDO;
import com.datasophon.dao.entity.ClusterServiceInstanceEntity;
import com.datasophon.dao.entity.ClusterServiceInstanceRoleGroup;
import com.datasophon.dao.entity.ClusterServiceRoleGroupConfig;
import com.datasophon.dao.entity.ClusterVariable;
import com.datasophon.dao.entity.FrameServiceEntity;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;

import cn.hutool.crypto.SecureUtil;

/**
 * Handles all config-related operations for the service installation workflow:
 * loading configs from DB or frame templates, variable substitution,
 * config-file map building, change detection, and config serialization.
 *
 * <p>Extracted from ServiceInstallServiceImpl to separate config concerns
 * from orchestration, mapping, and deployment logic.</p>
 */
@Component
public class ServiceConfigLoader {

    private static final Logger logger = LoggerFactory.getLogger(ServiceConfigLoader.class);

    @Autowired
    private FrameServiceService frameService;

    @Autowired
    private ClusterServiceInstanceRoleGroupService roleGroupService;

    @Autowired
    private ClusterServiceRoleGroupConfigService groupConfigService;

    @Autowired
    private ClusterVariableService variableService;

    @Autowired
    private ClusterHostService hostService;

    /**
     * Load the current config list from the default role group of an
     * existing service instance.
     *
     * @param serviceInstance the service instance entity
     * @return config list, or empty list if no config is found
     */
    public List<ServiceConfig> loadConfigForExistingInstance(
            ClusterServiceInstanceEntity serviceInstance) {
        ClusterServiceInstanceRoleGroup roleGroup =
                roleGroupService.getRoleGroupByServiceInstanceId(serviceInstance.getId());
        if (roleGroup == null) {
            logger.warn("No default role group found for service instance {}",
                    serviceInstance.getId());
            return Collections.emptyList();
        }
        ClusterServiceRoleGroupConfig config =
                groupConfigService.getConfigByRoleGroupId(roleGroup.getId());
        if (config == null || StringUtils.isBlank(config.getConfigJson())) {
            logger.warn("No config found for role group {}", roleGroup.getId());
            return Collections.emptyList();
        }
        return JSONArray.parseArray(config.getConfigJson(), ServiceConfig.class);
    }

    /**
     * Load the default config list from the frame service DDL template,
     * with all ${...} placeholders replaced by global variables.
     *
     * @param clusterFrame the frame code (e.g. "DDP-1.0")
     * @param serviceName the service name (e.g. "HDFS")
     * @param globalVariables variable map for placeholder substitution
     * @return parsed config list
     * @throws ServiceException if the frame service is not found
     */
    public List<ServiceConfig> loadConfigFromFrameTemplate(
            String clusterFrame,
            String serviceName,
            Map<String, String> globalVariables) {
        FrameServiceEntity frameServiceEntity =
                frameService.getServiceByFrameCodeAndServiceName(clusterFrame, serviceName);
        if (frameServiceEntity == null) {
            throw new ServiceException(
                    Status.SERVICE_NOT_FOUND_IN_FRAME.getMsg().replace("{0}", serviceName));
        }
        String serviceConfig = frameServiceEntity.getServiceConfig();
        if (StringUtils.isBlank(serviceConfig)) {
            return Collections.emptyList();
        }
        Map<String, String> vars = globalVariables != null
                ? globalVariables : Collections.emptyMap();
        serviceConfig = PlaceholderUtils.replacePlaceholders(
                serviceConfig, vars, Constants.REGEX_VARIABLE);
        return JSONArray.parseArray(serviceConfig, ServiceConfig.class);
    }

    /**
     * For each config item of type "input", persist as a ClusterVariable
     * (upsert). Always update the in-memory globalVariables map.
     *
     * @return a name-to-ServiceConfig lookup map
     */
    public Map<String, ServiceConfig> processConfigVariables(
            Integer clusterId,
            String serviceName,
            List<ServiceConfig> configs,
            Map<String, String> globalVariables) {
        Map<String, ServiceConfig> configMap = new HashMap<>();
        for (ServiceConfig serviceConfig : configs) {
            String configName = serviceConfig.getName();
            String variableName = "${" + configName + "}";
            String variableValue = String.valueOf(serviceConfig.getValue());
            if (Constants.INPUT.equals(serviceConfig.getType())) {
                upsertClusterVariable(clusterId, serviceName, variableName, variableValue);
            }
            if (globalVariables != null) {
                globalVariables.put(variableName, variableValue);
            }
            configMap.put(serviceConfig.getName(), serviceConfig);
        }
        return configMap;
    }

    /**
     * Parse the frame service's configFileJson and merge with user-supplied
     * config values.
     *
     * @return a map of Generators to List of ServiceConfig
     */
    public Map<Generators, List<ServiceConfig>> buildConfigFileMap(
            String clusterFrame,
            String serviceName,
            Map<String, ServiceConfig> configMap) {
        Map<Generators, List<ServiceConfig>> configFileMap = new HashMap<>();
        FrameServiceEntity frameServiceEntity =
                frameService.getServiceByFrameCodeAndServiceName(clusterFrame, serviceName);
        if (frameServiceEntity == null
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
                logger.debug("Processing config: {}", config.getName());
                if (configMap.containsKey(config.getName())) {
                    ServiceConfig userConfig = configMap.get(config.getName());
                    config.setValue(userConfig.getValue());
                    config.setHidden(userConfig.isHidden());
                    config.setRequired(userConfig.isRequired());
                }
            }
            configFileMap.put(generators, serviceConfigs);
        }
        return configFileMap;
    }

    /**
     * Compare the proposed config list against the currently persisted config.
     *
     * @return true if any value changed or new configs were added
     */
    public boolean isConfigNeedUpdate(
            ClusterServiceInstanceEntity serviceInstance,
            List<ServiceConfig> newConfigs) {
        List<ServiceConfig> originalConfigs = loadConfigForExistingInstance(serviceInstance);
        Map<String, Object> originalConfigMap =
                originalConfigs.stream()
                        .collect(Collectors.toMap(
                                ServiceConfig::getName,
                                ServiceConfig::getValue,
                                (v1, v2) -> v1));
        for (ServiceConfig serviceConfig : newConfigs) {
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

    /**
     * Serialize config list + configFileMap into JSON, compute MD5 hashes,
     * and populate the given ClusterServiceRoleGroupConfig entity.
     */
    public void buildRoleGroupConfig(
            List<ServiceConfig> configs,
            Map<Generators, List<ServiceConfig>> configFileMap,
            ClusterServiceRoleGroupConfig roleGroupConfig) {
        String configJson = JSONObject.toJSONString(configs);
        String configFileJson = JSONObject.toJSONString(configFileMap);
        roleGroupConfig.setConfigJson(configJson);
        roleGroupConfig.setConfigJsonMd5(SecureUtil.md5(configJson));
        roleGroupConfig.setConfigFileJson(configFileJson);
        roleGroupConfig.setConfigFileJsonMd5(SecureUtil.md5(configFileJson));
    }

    /**
     * Add worker and node-exporter scrape configs for every managed host
     * in the cluster to the configFileMap. Prometheus-specific special case.
     */
    void addPrometheusHostNodes(
            Integer clusterId,
            Map<Generators, List<ServiceConfig>> configFileMap) {
        List<ClusterHostDO> hostList =
                hostService.list(
                        new QueryWrapper<ClusterHostDO>()
                                .eq(Constants.MANAGED, 1)
                                .eq(Constants.CLUSTER_ID, clusterId));

        Generators workerGenerators = new Generators();
        workerGenerators.setFilename(ServiceInstallConstants.PROMETHEUS_WORKER_FILENAME);
        workerGenerators.setOutputDirectory(ServiceInstallConstants.PROMETHEUS_OUTPUT_DIRECTORY);
        workerGenerators.setConfigFormat("custom");
        workerGenerators.setTemplateName(ServiceInstallConstants.PROMETHEUS_SCRAPE_TEMPLATE);

        Generators nodeGenerators = new Generators();
        nodeGenerators.setFilename(ServiceInstallConstants.PROMETHEUS_NODE_FILENAME);
        nodeGenerators.setOutputDirectory(ServiceInstallConstants.PROMETHEUS_OUTPUT_DIRECTORY);
        nodeGenerators.setConfigFormat("custom");
        nodeGenerators.setTemplateName(ServiceInstallConstants.PROMETHEUS_SCRAPE_TEMPLATE);

        ArrayList<ServiceConfig> workerServiceConfigs = new ArrayList<>();
        ArrayList<ServiceConfig> nodeServiceConfigs = new ArrayList<>();
        for (ClusterHostDO clusterHostDO : hostList) {
            ServiceConfig workerConfig = new ServiceConfig();
            workerConfig.setName("worker_" + clusterHostDO.getHostname());
            workerConfig.setValue(clusterHostDO.getHostname()
                    + ":" + ServiceInstallConstants.PROMETHEUS_WORKER_PORT);
            workerConfig.setRequired(true);
            workerServiceConfigs.add(workerConfig);

            ServiceConfig nodeConfig = new ServiceConfig();
            nodeConfig.setName("node_" + clusterHostDO.getHostname());
            nodeConfig.setValue(clusterHostDO.getHostname()
                    + ":" + ServiceInstallConstants.PROMETHEUS_NODE_PORT);
            nodeConfig.setRequired(true);
            nodeServiceConfigs.add(nodeConfig);
        }
        configFileMap.put(workerGenerators, workerServiceConfigs);
        configFileMap.put(nodeGenerators, nodeServiceConfigs);
    }

    /**
     * Upsert a ClusterVariable: update if value changed, create if new.
     */
    private void upsertClusterVariable(
            Integer clusterId, String serviceName,
            String variableName, String value) {
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
}
