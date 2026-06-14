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

import com.datasophon.api.enums.Status;
import com.datasophon.api.exceptions.ServiceException;
import com.datasophon.api.service.ClusterServiceInstanceService;
import com.datasophon.api.service.ClusterServiceRoleInstanceService;
import com.datasophon.api.service.FrameServiceService;
import com.datasophon.common.model.ServiceRoleHostMapping;
import com.datasophon.common.utils.CollectionUtils;
import com.datasophon.common.utils.Result;
import com.datasophon.dao.entity.ClusterServiceInstanceEntity;
import com.datasophon.dao.entity.ClusterServiceRoleInstanceEntity;
import com.datasophon.dao.entity.FrameServiceEntity;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ServiceRoleValidator {

    private static final List<String> MUST_AT_SAME_NODE_BASIC_SERVICE =
            Arrays.asList("Grafana", "AlertManager", "Prometheus");

    @Autowired
    private ClusterServiceRoleInstanceService roleInstanceService;

    @Autowired
    private ClusterServiceInstanceService serviceInstanceService;

    @Autowired
    private FrameServiceService frameService;

    public void validateRoleHostMapping(ServiceRoleHostMapping serviceRoleHostMapping) {
        if (serviceRoleHostMapping == null) {
            throw new ServiceException("service role host mapping is null");
        }
        String serviceRole = serviceRoleHostMapping.getServiceRole();
        List<String> hosts = serviceRoleHostMapping.getHosts();
        if (hosts == null || hosts.isEmpty()) {
            throw new ServiceException("hosts list is empty for service role: " + serviceRole);
        }

        if ("JournalNode".equals(serviceRole) && hosts.size() != 3) {
            throw new ServiceException(Status.THREE_JOURNALNODE_DEPLOYMENTS_REQUIRED.getMsg());
        }
        if ("NameNode".equals(serviceRole) && hosts.size() != 2) {
            throw new ServiceException(Status.TWO_NAMENODES_NEED_TO_BE_DEPLOYED.getMsg());
        }
        if ("ZKFC".equals(serviceRole) && hosts.size() != 2) {
            throw new ServiceException(Status.TWO_ZKFC_DEVICES_ARE_REQUIRED.getMsg());
        }
        if ("ResourceManager".equals(serviceRole) && hosts.size() != 2) {
            throw new ServiceException(Status.TWO_RESOURCEMANAGER_ARE_DEPLOYED.getMsg());
        }
        if ("ZkServer".equals(serviceRole) && (hosts.size() & 1) == 0) {
            throw new ServiceException(Status.ODD_NUMBER_ARE_REQUIRED_FOR_ZKSERVER.getMsg());
        }
        if ("DorisFE".equals(serviceRole) && (hosts.size() & 1) == 0) {
            throw new ServiceException(Status.ODD_NUMBER_ARE_REQUIRED_FOR_DORISFE.getMsg());
        }
        if ("KyuubiServer".equals(serviceRole) && hosts.size() != 2) {
            throw new ServiceException(Status.TWO_KYUUBISERVERS_NEED_TO_BE_DEPLOYED.getMsg());
        }
    }

    public void validateSameNodeConstraint(Integer clusterId, List<ServiceRoleHostMapping> list) {
        if (list == null || list.isEmpty()) {
            return;
        }
        Set<String> hostnameSet =
                list.stream()
                        .filter(s -> MUST_AT_SAME_NODE_BASIC_SERVICE.contains(s.getServiceRole()))
                        .map(ServiceRoleHostMapping::getHosts)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        if (CollectionUtils.isEmpty(hostnameSet)) {
            return;
        }

        Set<String> installedHostnameSet =
                roleInstanceService.lambdaQuery()
                        .eq(ClusterServiceRoleInstanceEntity::getClusterId, clusterId)
                        .in(
                                ClusterServiceRoleInstanceEntity::getServiceName,
                                MUST_AT_SAME_NODE_BASIC_SERVICE)
                        .list().stream()
                        .map(ClusterServiceRoleInstanceEntity::getHostname)
                        .collect(Collectors.toSet());
        hostnameSet.addAll(installedHostnameSet);

        if (hostnameSet.size() > 1) {
            throw new ServiceException(Status.BASIC_SERVICE_SELECT_MOST_ONE_HOST.getMsg());
        }
    }

    public Result validateServiceDependencies(Integer clusterId, String serviceIds) {
        if (StringUtils.isBlank(serviceIds)) {
            return Result.error("serviceIds is empty");
        }

        List<ClusterServiceInstanceEntity> serviceInstanceList =
                serviceInstanceService.listRunningServiceInstance(clusterId);
        Map<String, ClusterServiceInstanceEntity> instanceMap =
                serviceInstanceList.stream()
                        .collect(
                                Collectors.toMap(
                                        ClusterServiceInstanceEntity::getServiceName,
                                        e -> e,
                                        (v1, v2) -> v1));

        List<FrameServiceEntity> list = frameService.listServices(serviceIds);
        if (list == null || list.isEmpty()) {
            return Result.error("no services found for ids: " + serviceIds);
        }
        Map<String, FrameServiceEntity> serviceMap =
                list.stream()
                        .collect(
                                Collectors.toMap(
                                        FrameServiceEntity::getServiceName,
                                        e -> e,
                                        (v1, v2) -> v1));

        if (!instanceMap.containsKey("ALERTMANAGER") && !serviceMap.containsKey("ALERTMANAGER")) {
            return Result.error(
                    "service install depends on alertmanager ,please make sure you have selected it or that alertmanager is normal and running");
        }
        if (!instanceMap.containsKey("GRAFANA") && !serviceMap.containsKey("GRAFANA")) {
            return Result.error(
                    "service install depends on grafana ,please make sure you have selected it or that grafana is normal and running");
        }
        if (!instanceMap.containsKey("PROMETHEUS") && !serviceMap.containsKey("PROMETHEUS")) {
            return Result.error(
                    "service install depends on prometheus ,please make sure you have selected it or that prometheus is normal and running");
        }

        for (FrameServiceEntity frameServiceEntity : list) {
            String dependencies = frameServiceEntity.getDependencies();
            if (StringUtils.isBlank(dependencies)) {
                continue;
            }
            for (String dependService : dependencies.split(",")) {
                if (StringUtils.isNotBlank(dependService)
                        && !instanceMap.containsKey(dependService)
                        && !serviceMap.containsKey(dependService)) {
                    return Result.error(
                            ""
                                    + frameServiceEntity.getServiceName()
                                    + " install depends on "
                                    + dependService
                                    + ",please make sure that you have selected it or that "
                                    + dependService
                                    + " is normal and running");
                }
            }
        }
        return Result.success();
    }
}
