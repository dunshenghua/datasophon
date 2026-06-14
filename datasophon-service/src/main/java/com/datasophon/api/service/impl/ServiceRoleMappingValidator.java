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
import com.datasophon.api.service.ClusterServiceRoleInstanceService;
import com.datasophon.api.utils.ServiceInstallConstants;
import com.datasophon.common.model.ServiceRoleHostMapping;
import com.datasophon.common.utils.CollectionUtils;
import com.datasophon.dao.entity.ClusterServiceRoleInstanceEntity;
import com.datasophon.dao.entity.FrameServiceEntity;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles all validation logic for the service installation workflow:
 * role cardinality rules, same-node constraints, infrastructure
 * dependency checks, and dynamic dependency resolution.
 *
 * <p>Extracted from ServiceInstallServiceImpl to separate validation
 * concerns from orchestration and persistence logic.</p>
 */
@Component
public class ServiceRoleMappingValidator {

    @Autowired
    private ClusterServiceRoleInstanceService roleInstanceService;

    /**
     * Validate that the number of hosts assigned to a role matches
     * the required cardinality (e.g., NameNode requires exactly 2 hosts).
     *
     * @param mapping the role-to-hosts mapping to validate
     * @throws ServiceException if cardinality is violated
     */
    public void validateRoleCardinality(ServiceRoleHostMapping mapping) {
        String serviceRole = mapping.getServiceRole();
        List<String> hosts = mapping.getHosts();

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

    /**
     * Verify that monitoring roles (Grafana, AlertManager, Prometheus)
     * are all assigned to the same physical host, including any
     * already-installed instances.
     *
     * @param clusterId the cluster ID
     * @param mappings the proposed role-to-hosts mappings
     * @throws ServiceException if the constraint is violated
     */
    public void checkSameNodeConstraint(Integer clusterId,
            List<ServiceRoleHostMapping> mappings) {
        Set<String> hostnameSet =
                mappings.stream()
                        .filter(s -> ServiceInstallConstants.MUST_AT_SAME_NODE_ROLES
                                .contains(s.getServiceRole()))
                        .map(ServiceRoleHostMapping::getHosts)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        if (CollectionUtils.isEmpty(hostnameSet)) {
            return;
        }

        Set<String> installedHostnameSet =
                roleInstanceService.lambdaQuery()
                        .eq(ClusterServiceRoleInstanceEntity::getClusterId, clusterId)
                        .in(ClusterServiceRoleInstanceEntity::getServiceName,
                                ServiceInstallConstants.MUST_AT_SAME_NODE_ROLES)
                        .list().stream()
                        .map(ClusterServiceRoleInstanceEntity::getHostname)
                        .collect(Collectors.toSet());
        hostnameSet.addAll(installedHostnameSet);

        if (hostnameSet.size() > 1) {
            throw new ServiceException(Status.BASIC_SERVICE_SELECT_MOST_ONE_HOST.getMsg());
        }
    }

    /**
     * Check that all required infrastructure services (AlertManager, Grafana,
     * Prometheus) are either already running or selected in this install batch.
     *
     * @param runningInstanceMap map of running service instances (serviceName -&gt; entity)
     * @param selectedServiceMap map of selected services (serviceName -&gt; entity)
     * @return error message if a dependency is missing, null if all present
     */
    public String checkInfrastructureDependencies(
            Map<String, ?> runningInstanceMap,
            Map<String, ?> selectedServiceMap) {
        for (String infraService :
                ServiceInstallConstants.INFRASTRUCTURE_SERVICE_DEPENDENCIES) {
            if (!runningInstanceMap.containsKey(infraService)
                    && !selectedServiceMap.containsKey(infraService)) {
                return "service install depends on "
                        + infraService.toLowerCase()
                        + " ,please make sure you have selected it or that "
                        + infraService.toLowerCase()
                        + " is normal and running";
            }
        }
        return null;
    }

    /**
     * For each selected service, verify that all declared dependencies
     * (from FrameServiceEntity.dependencies) are satisfied.
     *
     * @param selectedServices the list of frame service entities being installed
     * @param runningInstanceMap map of running service instances
     * @param selectedServiceMap map of selected services
     * @return error message if a dependency is missing, null if all satisfied
     */
    public String checkDynamicDependencies(
            List<FrameServiceEntity> selectedServices,
            Map<String, ?> runningInstanceMap,
            Map<String, ?> selectedServiceMap) {
        for (FrameServiceEntity frameServiceEntity : selectedServices) {
            String deps = frameServiceEntity.getDependencies();
            if (StringUtils.isBlank(deps)) {
                continue;
            }
            for (String dependService : deps.split(",")) {
                String trimmed = dependService.trim();
                if (StringUtils.isNotBlank(trimmed)
                        && !runningInstanceMap.containsKey(trimmed)
                        && !selectedServiceMap.containsKey(trimmed)) {
                    return frameServiceEntity.getServiceName()
                            + " install depends on " + trimmed
                            + ",please make sure that you have selected it or that "
                            + trimmed + " is normal and running";
                }
            }
        }
        return null;
    }
}
