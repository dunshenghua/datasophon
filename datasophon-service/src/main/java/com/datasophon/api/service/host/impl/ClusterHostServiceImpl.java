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

package com.datasophon.api.service.host.impl;

import com.datasophon.api.enums.Status;
import com.datasophon.api.master.ActorUtils;
import com.datasophon.api.master.PrometheusActor;
import com.datasophon.api.master.RackActor;
import com.datasophon.api.service.ClusterInfoService;
import com.datasophon.api.service.ClusterRackService;
import com.datasophon.api.service.ClusterServiceRoleInstanceService;
import com.datasophon.api.service.host.ClusterHostService;
import com.datasophon.api.service.host.dto.QueryHostListPageDTO;
import com.datasophon.common.Constants;
import com.datasophon.common.cache.CacheUtils;
import com.datasophon.common.command.ExecuteCmdCommand;
import com.datasophon.common.command.GenerateHostPrometheusConfig;
import com.datasophon.common.command.GenerateRackPropCommand;
import com.datasophon.common.model.HostInfo;
import com.datasophon.common.utils.Result;
import com.datasophon.dao.entity.ClusterHostDO;
import com.datasophon.dao.entity.ClusterInfoEntity;
import com.datasophon.dao.entity.ClusterRack;
import com.datasophon.dao.entity.ClusterServiceRoleInstanceEntity;
import com.datasophon.dao.enums.RoleType;
import com.datasophon.dao.enums.ServiceRoleState;
import com.datasophon.dao.mapper.ClusterHostMapper;
import com.datasophon.domain.host.enums.HostState;

import org.apache.commons.lang3.StringUtils;

import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import akka.actor.ActorRef;
import cn.hutool.crypto.SecureUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("clusterHostService")
@Transactional
public class ClusterHostServiceImpl extends ServiceImpl<ClusterHostMapper, ClusterHostDO>
        implements
            ClusterHostService {
    
    @Autowired
    ClusterHostMapper hostMapper;
    
    @Autowired
    ClusterServiceRoleInstanceService roleInstanceService;
    
    @Autowired
    ClusterInfoService clusterInfoService;
    
    @Autowired
    ClusterRackService clusterRackService;

    @Override
    @Transactional(readOnly = true)
    public ClusterHostDO getClusterHostByHostname(String hostname) {
        return hostMapper.getClusterHostByHostname(hostname);
    }
    
    @Override
    @Transactional(readOnly = true)
    public Result listByPage(Integer clusterId, String hostname, String ip, String cpuArchitecture, Integer hostState,
                             String orderField, String orderType, Integer page, Integer pageSize) {
        int offset = (page - 1) * pageSize;

        QueryWrapper<ClusterHostDO> baseQuery = buildHostQuery(clusterId, hostname, ip, cpuArchitecture, hostState);

        List<ClusterHostDO> list = this.list(buildHostQuery(clusterId, hostname, ip, cpuArchitecture, hostState)
                .orderByAsc("asc".equals(orderType), orderField)
                .orderByDesc("desc".equals(orderType), orderField)
                .last("limit " + offset + "," + pageSize));

        int count = this.count(baseQuery);

        List<QueryHostListPageDTO> dtos = enrichHostListResults(list, clusterId);
        return Result.success(dtos).put(Constants.TOTAL, count);
    }
    
    @Override
    @Transactional(readOnly = true)
    public List<ClusterHostDO> getHostListByClusterId(Integer clusterId) {
        return this.list(new QueryWrapper<ClusterHostDO>()
                .eq(Constants.CLUSTER_ID, clusterId)
                .eq(Constants.MANAGED, 1));
    }
    
    @Override
    @Transactional(readOnly = true)
    public Result getRoleListByHostname(Integer clusterId, String hostname) {
        List<ClusterServiceRoleInstanceEntity> list =
                roleInstanceService.getServiceRoleListByHostnameAndClusterId(hostname, clusterId);
        for (ClusterServiceRoleInstanceEntity roleInstanceEntity : list) {
            roleInstanceEntity.setServiceRoleStateCode(roleInstanceEntity.getServiceRoleState().getValue());
        }
        return Result.success(list);
    }
    
    /**
     * 批量删除主机。
     * 删除主机，首先停止主机上的服务
     * 其次删除主机 worker，同时移除 Prometheus hosts
     * 然后删除主机运行的实例
     *
     * @param hostIds
     * @return
     */
    @Override
    @Transactional
    public Result deleteHosts(String hostIds) {
        String[] ids = hostIds.split(Constants.COMMA);
        log.info("Deleting {} host(s): {}", ids.length, hostIds);

        for (String hostId : ids) {
            ClusterHostDO host = this.getById(hostId);
            if (host == null) {
                log.warn("Host with id {} not found, skipping", hostId);
                continue;
            }

            Result validationError = validateHostDeletion(host);
            if (validationError != null) {
                return validationError;
            }

            ClusterInfoEntity clusterInfo = clusterInfoService.getById(host.getClusterId());
            String clusterCode = clusterInfo.getClusterCode();

            removeHostFromDatabase(hostId, host, clusterCode);
            performPostDeletionCleanup(host, clusterInfo);
        }
        return Result.success();
    }
    
    @Override
    @Transactional(readOnly = true)
    public Result getRack(Integer clusterId) {
        ArrayList<JSONObject> list = new ArrayList<>();
        JSONObject rack = new JSONObject();
        rack.put("rack", "/default-rack");
        list.add(rack);
        return Result.success(list);
    }
    
    @Override
    public void removeHostByClusterId(Integer clusterId) {
        this.remove(new QueryWrapper<ClusterHostDO>().eq(Constants.CLUSTER_ID, clusterId));
    }
    
    @Override
    public void updateBatchNodeLabel(List<String> hostIds, String nodeLabel) {
        List<ClusterHostDO> list = this.lambdaQuery().in(ClusterHostDO::getId, hostIds).list();
        for (ClusterHostDO clusterHostDO : list) {
            clusterHostDO.setNodeLabel(nodeLabel);
        }
        this.updateBatchById(list);
    }
    
    @Override
    @Transactional(readOnly = true)
    public List<ClusterHostDO> getHostListByIds(List<String> ids) {
        return this.lambdaQuery().in(ClusterHostDO::getId, ids).or().in(ClusterHostDO::getHostname, ids).list();
    }
    
    @Override
    public Result assignRack(Integer clusterId, String rack, String hostIds) {
        List<String> ids = Arrays.asList(hostIds.split(","));
        List<ClusterHostDO> list = this.lambdaQuery().in(ClusterHostDO::getId, ids).list();
        for (ClusterHostDO clusterHostDO : list) {
            clusterHostDO.setRack(rack);
        }
        this.updateBatchById(list);
        // tell rack actor
        GenerateRackPropCommand command = new GenerateRackPropCommand();
        command.setClusterId(clusterId);
        ActorRef rackActor = ActorUtils.getLocalActor(RackActor.class, "rackActor");
        rackActor.tell(command, ActorRef.noSender());
        return Result.success();
    }
    
    @Override
    @Transactional(readOnly = true)
    public List<ClusterHostDO> getClusterHostByRack(Integer clusterId, String rack) {
        return this.list(new QueryWrapper<ClusterHostDO>()
                .eq(Constants.CLUSTER_ID, clusterId)
                .eq(Constants.RACK, rack));
    }

    @Override
    @Transactional(readOnly = true)
    public List<ClusterHostDO> listManagedHostsByClusterId(Integer clusterId) {
        return this.list(new QueryWrapper<ClusterHostDO>()
                .eq(Constants.CLUSTER_ID, clusterId)
                .eq(Constants.MANAGED, 1)
                .orderByAsc(Constants.HOSTNAME));
    }

    // ======================== listByPage helpers ========================

    private QueryWrapper<ClusterHostDO> buildHostQuery(Integer clusterId, String hostname, String ip,
                                                       String cpuArchitecture, Integer hostState) {
        return new QueryWrapper<ClusterHostDO>()
                .eq(Constants.CLUSTER_ID, clusterId)
                .eq(Constants.MANAGED, 1)
                .eq(StringUtils.isNotBlank(cpuArchitecture), Constants.CPU_ARCHITECTURE, cpuArchitecture)
                .eq(hostState != null, Constants.HOST_STATE, hostState)
                .like(StringUtils.isNotBlank(ip), Constants.IP, ip)
                .like(StringUtils.isNotBlank(hostname), Constants.HOSTNAME, hostname);
    }

    private Map<String, String> buildRackNameMap(Integer clusterId) {
        List<ClusterRack> racks = clusterRackService.queryClusterRack(clusterId);
        if (racks == null || racks.isEmpty()) {
            return Collections.emptyMap();
        }
        return racks.stream()
                .collect(Collectors.toMap(rack -> rack.getId() + "", ClusterRack::getRack));
    }

    private List<QueryHostListPageDTO> enrichHostListResults(List<ClusterHostDO> hostList, Integer clusterId) {
        if (hostList.isEmpty()) {
            return new ArrayList<>();
        }
        Map<String, String> rackMap = buildRackNameMap(clusterId);

        List<QueryHostListPageDTO> result = new ArrayList<>(hostList.size());
        for (ClusterHostDO clusterHostDO : hostList) {
            QueryHostListPageDTO dto = new QueryHostListPageDTO();
            BeanUtils.copyProperties(clusterHostDO, dto);

            int serviceRoleNum = roleInstanceService.count(new QueryWrapper<ClusterServiceRoleInstanceEntity>()
                    .eq(Constants.HOSTNAME, clusterHostDO.getHostname()));
            dto.setServiceRoleNum(serviceRoleNum);
            dto.setHostState(clusterHostDO.getHostState().getValue());
            dto.setRack(rackMap.getOrDefault(dto.getRack(), "/default-rack"));
            result.add(dto);
        }
        return result;
    }

    // ======================== deleteHosts helpers ========================

    private Result validateHostDeletion(ClusterHostDO host) {
        List<ClusterServiceRoleInstanceEntity> runningServices =
                roleInstanceService.list(new QueryWrapper<ClusterServiceRoleInstanceEntity>()
                        .eq(Constants.CLUSTER_ID, host.getClusterId())
                        .eq(Constants.HOSTNAME, host.getHostname())
                        .eq(Constants.SERVICE_ROLE_STATE, ServiceRoleState.RUNNING)
                        .ne(Constants.ROLE_TYPE, RoleType.CLIENT));
        if (!runningServices.isEmpty()) {
            List<String> runningRoles = runningServices.stream()
                    .map(ClusterServiceRoleInstanceEntity::getServiceRoleName)
                    .collect(Collectors.toList());
            log.warn("Cannot delete host {}: running roles {}", host.getHostname(), runningRoles);
            return Result.error(host.getHostname() + Status.HOST_EXIT_ONE_RUNNING_ROLE.getMsg() + runningRoles);
        }

        List<ClusterServiceRoleInstanceEntity> installedServices =
                roleInstanceService.list(new QueryWrapper<ClusterServiceRoleInstanceEntity>()
                        .eq(Constants.CLUSTER_ID, host.getClusterId())
                        .eq(Constants.HOSTNAME, host.getHostname()));
        if (!installedServices.isEmpty()) {
            List<String> installedRoles = installedServices.stream()
                    .map(ClusterServiceRoleInstanceEntity::getServiceRoleName)
                    .collect(Collectors.toList());
            log.warn("Cannot delete host {}: installed roles {}", host.getHostname(), installedRoles);
            return Result.error(host.getHostname() + Status.HOST_EXIT_ONE_INSTALLED_ROLE.getMsg() + installedRoles);
        }

        return null;
    }

    private void removeHostFromDatabase(String hostId, ClusterHostDO host, String clusterCode) {
        String distributeAgentKey = clusterCode + Constants.UNDERLINE + Constants.START_DISTRIBUTE_AGENT
                + Constants.UNDERLINE + host.getHostname();
        if (CacheUtils.constainsKey(distributeAgentKey)) {
            CacheUtils.removeKey(distributeAgentKey);
        }
        this.removeById(hostId);
        log.info("Removed host {} (id={}) from cluster {}", host.getHostname(), hostId, clusterCode);
    }

    private void performPostDeletionCleanup(ClusterHostDO host, ClusterInfoEntity clusterInfo) {
        if (host.getHostState() != HostState.OFFLINE) {
            stopWorkerOnHost(host.getHostname());
        }
        schedulePrometheusRefresh(clusterInfo.getId());
        removeHostFromCache(host.getHostname(), clusterInfo.getClusterCode());
    }

    private void stopWorkerOnHost(String hostname) {
        try {
            ActorRef execCmdActor = ActorUtils.getRemoteActor(hostname, "executeCmdActor");
            ExecuteCmdCommand command = new ExecuteCmdCommand();
            ArrayList<String> commands = new ArrayList<>();
            commands.add("service");
            commands.add("datasophon-worker");
            commands.add("stop");
            command.setCommands(commands);
            execCmdActor.tell(command, ActorRef.noSender());
            log.info("Sent stop-worker command to host {}", hostname);
        } catch (Exception e) {
            log.warn("Failed to send stop-worker command to host {}", hostname, e);
        }
    }

    private void schedulePrometheusRefresh(Integer clusterId) {
        ActorRef prometheusActor =
                ActorUtils.getLocalActor(PrometheusActor.class, ActorUtils.getActorRefName(PrometheusActor.class));
        GenerateHostPrometheusConfig command = new GenerateHostPrometheusConfig();
        command.setClusterId(clusterId);
        ActorUtils.actorSystem.scheduler().scheduleOnce(
                FiniteDuration.apply(3L, TimeUnit.SECONDS),
                prometheusActor,
                command,
                ActorUtils.actorSystem.dispatcher(),
                ActorRef.noSender());
    }

    @SuppressWarnings("unchecked")
    private void removeHostFromCache(String hostname, String clusterCode) {
        Map<String, HostInfo> hostMap =
                (Map<String, HostInfo>) CacheUtils.get(clusterCode + Constants.HOST_MAP);
        if (Objects.nonNull(hostMap)) {
            hostMap.remove(hostname);
            log.debug("Removed host {} from host map cache", hostname);
        }

        String md5 = SecureUtil.md5(hostname);
        String md5Key = clusterCode + Constants.HOST_MD5;
        if (CacheUtils.constainsKey(md5Key)
                && md5.equals(CacheUtils.getString(md5Key))) {
            CacheUtils.removeKey(md5Key);
        }
    }
}
