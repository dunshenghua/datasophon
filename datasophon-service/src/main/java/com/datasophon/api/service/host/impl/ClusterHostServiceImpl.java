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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
    public ClusterHostDO getClusterHostByHostname(String hostname) {
        return hostMapper.getClusterHostByHostname(hostname);
    }

    // ────────────────────────────────────────────
    //  listByPage — 分页查询主机列表
    // ────────────────────────────────────────────

    @Override
    public Result listByPage(Integer clusterId, String hostname, String ip, String cpuArchitecture, Integer hostState,
                             String orderField, String orderType, Integer page, Integer pageSize) {
        // 1. 共享过滤条件（count 与 data 使用同一组条件，修复原来 count 缺少 ip 过滤的 bug）
        int count = this.count(buildHostPageQueryWrapper(clusterId, hostname, ip, cpuArchitecture, hostState));
        if (count == 0) {
            return Result.success(new ArrayList<>()).put(Constants.TOTAL, 0);
        }

        // 2. 数据查询：相同过滤条件 + 排序 + 分页
        int offset = (page - 1) * pageSize;
        List<ClusterHostDO> hosts = this.list(
                buildHostPageQueryWrapper(clusterId, hostname, ip, cpuArchitecture, hostState)
                        .orderByAsc("asc".equals(orderType), orderField)
                        .orderByDesc("desc".equals(orderType), orderField)
                        .last("limit " + offset + "," + pageSize));

        if (hosts.isEmpty()) {
            return Result.success(new ArrayList<>()).put(Constants.TOTAL, count);
        }

        // 3. 批量获取增强数据（机架映射、角色计数）
        Map<String, String> rackMap = buildRackMap(clusterId);
        Map<String, Integer> roleCountMap = batchCountRoleInstances(hosts);

        // 4. DTO 组装
        List<QueryHostListPageDTO> dtos = hosts.stream()
                .map(h -> convertToHostDTO(h, rackMap, roleCountMap))
                .collect(Collectors.toList());

        return Result.success(dtos).put(Constants.TOTAL, count);
    }

    /**
     * 构建主机分页查询的共享 WHERE 条件。
     * count 查询与 data 查询都使用此方法，保证过滤条件一致。
     */
    private QueryWrapper<ClusterHostDO> buildHostPageQueryWrapper(
            Integer clusterId, String hostname, String ip,
            String cpuArchitecture, Integer hostState) {
        return new QueryWrapper<ClusterHostDO>()
                .eq(Constants.CLUSTER_ID, clusterId)
                .eq(Constants.MANAGED, 1)
                .eq(StringUtils.isNotBlank(cpuArchitecture), Constants.CPU_ARCHITECTURE, cpuArchitecture)
                .eq(hostState != null, Constants.HOST_STATE, hostState)
                .like(StringUtils.isNotBlank(ip), "ip", ip)
                .like(StringUtils.isNotBlank(hostname), Constants.HOSTNAME, hostname);
    }

    /**
     * 构建机架 ID → 名称的映射表，null 安全。
     */
    private Map<String, String> buildRackMap(Integer clusterId) {
        List<ClusterRack> racks = clusterRackService.queryClusterRack(clusterId);
        if (racks == null || racks.isEmpty()) {
            return Collections.emptyMap();
        }
        return racks.stream().collect(Collectors.toMap(
                r -> String.valueOf(r.getId()),
                ClusterRack::getRack,
                (existing, replacement) -> replacement));
    }

    /**
     * 批量查询各主机上的角色实例数量（单次 GROUP BY 替代 N 次 count）。
     */
    private Map<String, Integer> batchCountRoleInstances(List<ClusterHostDO> hosts) {
        List<String> hostnames = hosts.stream()
                .map(ClusterHostDO::getHostname)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (hostnames.isEmpty()) {
            return Collections.emptyMap();
        }
        List<Map<String, Object>> rows = roleInstanceService.listMaps(
                new QueryWrapper<ClusterServiceRoleInstanceEntity>()
                        .select(Constants.HOSTNAME, "count(*) as cnt")
                        .in(Constants.HOSTNAME, hostnames)
                        .groupBy(Constants.HOSTNAME));
        Map<String, Integer> result = new HashMap<>();
        for (Map<String, Object> row : rows) {
            String h = (String) row.get(Constants.HOSTNAME);
            Number cnt = (Number) row.get("cnt");
            result.put(h, cnt != null ? cnt.intValue() : 0);
        }
        return result;
    }

    /**
     * 将主机实体转换为分页列表 DTO，null 安全。
     */
    private QueryHostListPageDTO convertToHostDTO(
            ClusterHostDO host, Map<String, String> rackMap,
            Map<String, Integer> roleCountMap) {
        QueryHostListPageDTO dto = new QueryHostListPageDTO();
        BeanUtils.copyProperties(host, dto);
        dto.setHostState(host.getHostState() != null ? host.getHostState().getValue() : null);
        dto.setRack(rackMap.getOrDefault(dto.getRack(), "/default-rack"));
        dto.setServiceRoleNum(roleCountMap.getOrDefault(host.getHostname(), 0));
        return dto;
    }

    // ────────────────────────────────────────────
    //  getHostListByClusterId — 集群下所有受管主机
    // ────────────────────────────────────────────

    @Override
    public List<ClusterHostDO> getHostListByClusterId(Integer clusterId) {
        return this.list(new QueryWrapper<ClusterHostDO>()
                .eq(Constants.CLUSTER_ID, clusterId)
                .eq(Constants.MANAGED, 1)
                .orderByAsc(Constants.HOSTNAME));
    }

    // ────────────────────────────────────────────
    //  getRoleListByHostname — 查看主机上的角色列表
    // ────────────────────────────────────────────

    @Override
    public Result getRoleListByHostname(Integer clusterId, String hostname) {
        List<ClusterServiceRoleInstanceEntity> list =
                roleInstanceService.getServiceRoleListByHostnameAndClusterId(hostname, clusterId);
        list.forEach(role -> {
            if (role.getServiceRoleState() != null) {
                role.setServiceRoleStateCode(role.getServiceRoleState().getValue());
            }
        });
        return Result.success(list);
    }

    // ────────────────────────────────────────────
    //  deleteHosts — 批量移除主机
    // ────────────────────────────────────────────

    /**
     * 批量删除主机。
     * 采用"先全部校验、再统一变更"模式：
     * Phase 1 校验所有主机可删除 → Phase 2 预加载集群信息
     * → Phase 3 执行删除/缓存清理/Worker 停止 → Phase 4 刷新 Prometheus
     */
    @Override
    @Transactional
    public Result deleteHosts(String hostIds) {
        List<String> ids = parseHostIds(hostIds);
        if (ids.isEmpty()) {
            return Result.error("请选择移除的主机!");
        }

        // ── Phase 1: 获取并校验所有主机（在任何变更之前） ──
        List<ClusterHostDO> hosts = new ArrayList<>(ids.size());
        for (String id : ids) {
            ClusterHostDO host = this.getById(id);
            if (host == null) {
                log.warn("主机不存在, id: {}", id);
                return Result.error("主机不存在, id: " + id);
            }
            Result validationError = validateHostDeletable(host);
            if (validationError != null) {
                return validationError;
            }
            hosts.add(host);
        }

        // ── Phase 2: 预加载集群信息（按 clusterId 去重） ──
        Map<Integer, ClusterInfoEntity> clusterInfoMap = new HashMap<>();
        for (ClusterHostDO host : hosts) {
            clusterInfoMap.computeIfAbsent(host.getClusterId(),
                    cid -> clusterInfoService.getById(cid));
        }

        // ── Phase 3: 执行变更 — DB 删除 + 缓存清理 + Worker 停止 ──
        Set<Integer> affectedClusterIds = new HashSet<>();
        for (ClusterHostDO host : hosts) {
            ClusterInfoEntity clusterInfo = clusterInfoMap.get(host.getClusterId());

            this.removeById(host.getId());
            log.info("已删除主机: {} (id={})", host.getHostname(), host.getId());

            if (clusterInfo != null) {
                cleanupHostCache(host, clusterInfo);
            } else {
                log.warn("集群信息不存在, clusterId: {}, 跳过缓存清理, host: {}",
                        host.getClusterId(), host.getHostname());
            }

            stopWorkerOnHost(host);
            affectedClusterIds.add(host.getClusterId());
        }

        // ── Phase 4: 后置动作 — 每个集群只刷新一次 Prometheus ──
        for (Integer clusterId : affectedClusterIds) {
            schedulePrometheusRefresh(clusterId);
        }

        return Result.success();
    }

    /**
     * 健壮的 hostId 解析：去空格、过滤空串、处理 null 输入。
     */
    private List<String> parseHostIds(String hostIds) {
        if (StringUtils.isBlank(hostIds)) {
            return Collections.emptyList();
        }
        return Arrays.stream(hostIds.split(Constants.COMMA))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
    }

    /**
     * 校验主机是否可删除：不能有运行中的非 CLIENT 角色，不能有任何已安装角色。
     *
     * @return null 表示可删除；Result.error 表示不可删除
     */
    private Result validateHostDeletable(ClusterHostDO host) {
        // 检查运行中的非 CLIENT 角色
        List<ClusterServiceRoleInstanceEntity> runningRoles = roleInstanceService.list(
                new QueryWrapper<ClusterServiceRoleInstanceEntity>()
                        .eq(Constants.CLUSTER_ID, host.getClusterId())
                        .eq(Constants.HOSTNAME, host.getHostname())
                        .eq(Constants.SERVICE_ROLE_STATE, ServiceRoleState.RUNNING)
                        .ne(Constants.ROLE_TYPE, RoleType.CLIENT));
        if (!runningRoles.isEmpty()) {
            List<String> names = runningRoles.stream()
                    .map(ClusterServiceRoleInstanceEntity::getServiceRoleName)
                    .collect(Collectors.toList());
            return Result.error(host.getHostname() + Status.HOST_EXIT_ONE_RUNNING_ROLE.getMsg() + names);
        }

        // 检查所有已安装角色
        List<ClusterServiceRoleInstanceEntity> installedRoles = roleInstanceService.list(
                new QueryWrapper<ClusterServiceRoleInstanceEntity>()
                        .eq(Constants.CLUSTER_ID, host.getClusterId())
                        .eq(Constants.HOSTNAME, host.getHostname()));
        if (!installedRoles.isEmpty()) {
            List<String> names = installedRoles.stream()
                    .map(ClusterServiceRoleInstanceEntity::getServiceRoleName)
                    .collect(Collectors.toList());
            return Result.error(host.getHostname() + Status.HOST_EXIT_ONE_INSTALLED_ROLE.getMsg() + names);
        }

        return null;
    }

    /**
     * 清理主机相关的三处缓存：分发 Agent 键、主机映射表、主机 MD5。
     */
    @SuppressWarnings("unchecked")
    private void cleanupHostCache(ClusterHostDO host, ClusterInfoEntity clusterInfo) {
        String clusterCode = clusterInfo.getClusterCode();

        // 1. 清理分发 Agent 缓存键
        String agentKey = clusterCode + Constants.UNDERLINE
                + Constants.START_DISTRIBUTE_AGENT
                + Constants.UNDERLINE + host.getHostname();
        if (CacheUtils.constainsKey(agentKey)) {
            CacheUtils.removeKey(agentKey);
        }

        // 2. 从主机映射表中移除
        Map<String, HostInfo> hostMap =
                (Map<String, HostInfo>) CacheUtils.get(clusterCode + Constants.HOST_MAP);
        if (hostMap != null) {
            hostMap.remove(host.getHostname());
        }

        // 3. 清理主机 MD5 缓存
        String md5Key = clusterCode + Constants.HOST_MD5;
        if (CacheUtils.constainsKey(md5Key)) {
            String cachedMd5 = CacheUtils.getString(md5Key);
            if (SecureUtil.md5(host.getHostname()).equals(cachedMd5)) {
                CacheUtils.removeKey(md5Key);
            }
        }
    }

    /**
     * 远程停止主机上的 Worker 进程。
     * OFFLINE 状态的主机跳过；Actor 异常不中断主流程。
     */
    private void stopWorkerOnHost(ClusterHostDO host) {
        if (host.getHostState() == HostState.OFFLINE) {
            log.info("主机 {} 处于 OFFLINE 状态, 跳过 Worker 停止", host.getHostname());
            return;
        }
        try {
            ActorRef execCmdActor = ActorUtils.getRemoteActor(host.getHostname(), "executeCmdActor");
            ExecuteCmdCommand command = new ExecuteCmdCommand();
            ArrayList<String> commands = new ArrayList<>();
            commands.add("service");
            commands.add("datasophon-worker");
            commands.add("stop");
            command.setCommands(commands);
            execCmdActor.tell(command, ActorRef.noSender());
            log.info("已发送 Worker 停止命令到主机: {}", host.getHostname());
        } catch (Exception e) {
            log.warn("停止主机 {} 上的 Worker 失败: {}", host.getHostname(), e.getMessage(), e);
        }
    }

    /**
     * 延迟刷新 Prometheus 采集配置（3 秒后执行）。
     * Actor 异常不中断主流程。
     */
    private void schedulePrometheusRefresh(Integer clusterId) {
        try {
            ActorRef prometheusActor = ActorUtils.getLocalActor(
                    PrometheusActor.class,
                    ActorUtils.getActorRefName(PrometheusActor.class));
            GenerateHostPrometheusConfig config = new GenerateHostPrometheusConfig();
            config.setClusterId(clusterId);
            ActorUtils.actorSystem.scheduler().scheduleOnce(
                    FiniteDuration.apply(3L, TimeUnit.SECONDS),
                    prometheusActor, config,
                    ActorUtils.actorSystem.dispatcher(),
                    ActorRef.noSender());
            log.info("已调度 Prometheus 配置刷新, clusterId: {}", clusterId);
        } catch (Exception e) {
            log.warn("调度 Prometheus 刷新失败, clusterId: {}: {}", clusterId, e.getMessage(), e);
        }
    }

    // ────────────────────────────────────────────
    //  其他方法
    // ────────────────────────────────────────────

    @Override
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
    public List<ClusterHostDO> getHostListByIds(List<String> ids) {
        return this.lambdaQuery().in(ClusterHostDO::getId, ids).or().in(ClusterHostDO::getHostname, ids).list();
    }

    @Override
    public Result assignRack(Integer clusterId, String rack, String hostIds) {
        if (StringUtils.isBlank(hostIds)) {
            return Result.error("hostIds 不能为空");
        }
        if (StringUtils.isBlank(rack)) {
            return Result.error("rack 不能为空");
        }
        List<String> ids = Arrays.stream(hostIds.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        if (ids.isEmpty()) {
            return Result.error("未提供有效的主机 ID");
        }
        List<ClusterHostDO> list = this.lambdaQuery().in(ClusterHostDO::getId, ids).list();
        if (list.isEmpty()) {
            return Result.error("未找到对应的主机");
        }
        for (ClusterHostDO clusterHostDO : list) {
            clusterHostDO.setRack(rack);
        }
        this.updateBatchById(list);
        // 通知 RackActor 重新生成机架配置
        GenerateRackPropCommand command = new GenerateRackPropCommand();
        command.setClusterId(clusterId);
        ActorRef rackActor = ActorUtils.getLocalActor(RackActor.class, "rackActor");
        rackActor.tell(command, ActorRef.noSender());
        return Result.success();
    }

    @Override
    public List<ClusterHostDO> getClusterHostByRack(Integer clusterId, String rack) {
        return this.list(new QueryWrapper<ClusterHostDO>()
                .eq(Constants.CLUSTER_ID, clusterId)
                .eq(Constants.RACK, rack));
    }
}
