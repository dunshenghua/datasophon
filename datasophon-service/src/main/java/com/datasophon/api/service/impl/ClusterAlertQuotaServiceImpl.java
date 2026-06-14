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

import com.datasophon.api.service.AlertGroupService;
import com.datasophon.api.service.ClusterAlertQuotaService;
import com.datasophon.common.Constants;
import com.datasophon.common.utils.Result;
import com.datasophon.dao.entity.AlertGroupEntity;
import com.datasophon.dao.entity.ClusterAlertQuota;
import com.datasophon.dao.enums.QuotaState;
import com.datasophon.dao.mapper.ClusterAlertQuotaMapper;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

@Service("clusterAlertQuotaService")
public class ClusterAlertQuotaServiceImpl extends ServiceImpl<ClusterAlertQuotaMapper, ClusterAlertQuota>
        implements
            ClusterAlertQuotaService {

    private static final Logger logger = LoggerFactory.getLogger(ClusterAlertQuotaServiceImpl.class);

    @Autowired
    private AlertGroupService alertGroupService;

    @Autowired
    private AlertRuleConfigHelper alertRuleConfigHelper;

    // ======================== Public API ========================

    @Override
    public Result getAlertQuotaList(Integer clusterId, Integer alertGroupId, String quotaName, Integer page,
                                    Integer pageSize) {
        Integer offset = (page - 1) * pageSize;

        Integer countResult = this.lambdaQuery()
                .eq(alertGroupId != null, ClusterAlertQuota::getAlertGroupId, alertGroupId)
                .like(StringUtils.isNotBlank(quotaName), ClusterAlertQuota::getAlertQuotaName, quotaName)
                .count();
        int count = countResult == null ? 0 : countResult;

        List<ClusterAlertQuota> alertQuotaList = this.lambdaQuery()
                .eq(alertGroupId != null, ClusterAlertQuota::getAlertGroupId, alertGroupId)
                .like(StringUtils.isNotBlank(quotaName), ClusterAlertQuota::getAlertQuotaName, quotaName)
                .last("limit " + offset + "," + pageSize)
                .list();

        if (alertQuotaList == null || alertQuotaList.isEmpty()) {
            return Result.successEmptyCount();
        }

        fillAlertGroupInfo(alertQuotaList);
        return Result.success(alertQuotaList).put(Constants.TOTAL, count);
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public Result start(Integer clusterId, String alertQuotaIds) {
        List<String> ids = parseAlertQuotaIds(alertQuotaIds);
        if (ids == null) {
            return Result.error("告警指标ID不能为空");
        }

        Collection<ClusterAlertQuota> alertQuotas = this.listByIds(ids);
        if (alertQuotas == null || alertQuotas.isEmpty()) {
            return Result.error("未找到对应的告警指标");
        }

        // 1. Update state to RUNNING
        Set<String> affectedCategories = new HashSet<>();
        for (ClusterAlertQuota quota : alertQuotas) {
            quota.setQuotaState(QuotaState.RUNNING);
            affectedCategories.add(quota.getServiceCategory());
        }
        this.updateBatchById(alertQuotas);
        logger.info("Started {} alert quota(s), affected categories: {}", alertQuotas.size(), affectedCategories);

        // 2. Collect all running quotas in affected categories and dispatch
        Map<String, List<ClusterAlertQuota>> quotasByCategory =
                collectRunningQuotasByCategories(affectedCategories);
        alertRuleConfigHelper.buildAndDispatch(clusterId, quotasByCategory);

        return Result.success();
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public Result stop(Integer clusterId, String alertQuotaIds) {
        List<String> ids = parseAlertQuotaIds(alertQuotaIds);
        if (ids == null) {
            return Result.error("告警指标ID不能为空");
        }

        Collection<ClusterAlertQuota> alertQuotas = this.listByIds(ids);
        if (alertQuotas == null || alertQuotas.isEmpty()) {
            return Result.error("未找到对应的告警指标");
        }

        // 1. Update state to STOPPED
        Set<String> affectedCategories = new HashSet<>();
        for (ClusterAlertQuota quota : alertQuotas) {
            quota.setQuotaState(QuotaState.STOPPED);
            affectedCategories.add(quota.getServiceCategory());
        }
        this.updateBatchById(alertQuotas);
        logger.info("Stopped {} alert quota(s), affected categories: {}", alertQuotas.size(), affectedCategories);

        // 2. Collect remaining running quotas and regenerate rule files
        Map<String, List<ClusterAlertQuota>> quotasByCategory =
                collectRunningQuotasByCategories(affectedCategories);
        if (quotasByCategory.isEmpty()) {
            return Result.success();
        }
        alertRuleConfigHelper.buildAndDispatch(clusterId, quotasByCategory);

        return Result.success();
    }

    @Override
    public Result saveAlertQuota(ClusterAlertQuota clusterAlertQuota) {
        if (clusterAlertQuota.getAlertGroupId() == null) {
            return Result.error("告警组不能为空");
        }

        AlertGroupEntity alertGroupEntity = alertGroupService.getById(clusterAlertQuota.getAlertGroupId());
        if (alertGroupEntity == null) {
            return Result.error("告警组不存在");
        }

        // Check duplicate name within the same alert group
        if (StringUtils.isNotBlank(clusterAlertQuota.getAlertQuotaName())) {
            Integer existCount = this.lambdaQuery()
                    .eq(ClusterAlertQuota::getAlertGroupId, clusterAlertQuota.getAlertGroupId())
                    .eq(ClusterAlertQuota::getAlertQuotaName, clusterAlertQuota.getAlertQuotaName())
                    .count();
            if (existCount != null && existCount > 0) {
                return Result.error("同一告警组下告警指标名称不能重复");
            }
        }

        clusterAlertQuota.setQuotaState(QuotaState.STOPPED);
        clusterAlertQuota.setCreateTime(new Date());
        clusterAlertQuota.setServiceCategory(alertGroupEntity.getAlertGroupCategory());
        this.save(clusterAlertQuota);
        return Result.success();
    }

    @Override
    public Result updateAlertQuota(ClusterAlertQuota clusterAlertQuota) {
        if (clusterAlertQuota.getId() == null) {
            return Result.error("告警指标ID不能为空");
        }

        ClusterAlertQuota existing = this.getById(clusterAlertQuota.getId());
        if (existing == null) {
            return Result.error("告警指标不存在");
        }

        // Check duplicate name if name is being changed
        if (StringUtils.isNotBlank(clusterAlertQuota.getAlertQuotaName())
                && !clusterAlertQuota.getAlertQuotaName().equals(existing.getAlertQuotaName())) {
            Integer groupId = clusterAlertQuota.getAlertGroupId() != null
                    ? clusterAlertQuota.getAlertGroupId()
                    : existing.getAlertGroupId();
            Integer existCount = this.lambdaQuery()
                    .eq(ClusterAlertQuota::getAlertGroupId, groupId)
                    .eq(ClusterAlertQuota::getAlertQuotaName, clusterAlertQuota.getAlertQuotaName())
                    .ne(ClusterAlertQuota::getId, clusterAlertQuota.getId())
                    .count();
            if (existCount != null && existCount > 0) {
                return Result.error("同一告警组下告警指标名称不能重复");
            }
        }

        clusterAlertQuota.setQuotaState(QuotaState.WAIT_TO_UPDATE);
        this.updateById(clusterAlertQuota);
        return Result.success();
    }

    @Override
    public List<ClusterAlertQuota> listAlertQuotaByServiceName(String serviceName) {
        return this.list(new QueryWrapper<ClusterAlertQuota>().eq(Constants.SERVICE_CATEGORY, serviceName));
    }

    // ======================== Private Helpers ========================

    /**
     * Parse and validate comma-separated alert quota IDs.
     *
     * @return parsed ID list, or null if input is invalid
     */
    private List<String> parseAlertQuotaIds(String alertQuotaIds) {
        if (StringUtils.isBlank(alertQuotaIds)) {
            return null;
        }
        List<String> ids = Arrays.stream(alertQuotaIds.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        return ids.isEmpty() ? null : ids;
    }

    /**
     * Enrich quota list with alert group names and quotaStateCode.
     */
    private void fillAlertGroupInfo(List<ClusterAlertQuota> alertQuotaList) {
        Set<Integer> alertGroupIdSet =
                alertQuotaList.stream().map(ClusterAlertQuota::getAlertGroupId)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());

        Map<Integer, AlertGroupEntity> groupMap;
        if (!alertGroupIdSet.isEmpty()) {
            Collection<AlertGroupEntity> groups = alertGroupService.listByIds(alertGroupIdSet);
            groupMap = groups.stream()
                    .collect(Collectors.toMap(AlertGroupEntity::getId, g -> g, (a, b) -> a));
        } else {
            groupMap = java.util.Collections.emptyMap();
        }

        for (ClusterAlertQuota quota : alertQuotaList) {
            AlertGroupEntity group = groupMap.get(quota.getAlertGroupId());
            if (group != null) {
                quota.setAlertGroupName(group.getAlertGroupName());
            }
            quota.setQuotaStateCode(quota.getQuotaState().getValue());
        }
    }

    /**
     * Query all RUNNING quotas in the given categories, deduplicate by name per category,
     * and return grouped by serviceCategory.
     */
    private Map<String, List<ClusterAlertQuota>> collectRunningQuotasByCategories(Set<String> categories) {
        if (categories == null || categories.isEmpty()) {
            return java.util.Collections.emptyMap();
        }

        List<ClusterAlertQuota> runningQuotas = this.lambdaQuery()
                .eq(ClusterAlertQuota::getQuotaState, QuotaState.RUNNING)
                .in(ClusterAlertQuota::getServiceCategory, categories)
                .list();

        if (runningQuotas == null || runningQuotas.isEmpty()) {
            return java.util.Collections.emptyMap();
        }

        // Group by category and deduplicate by alertQuotaName within each category
        return runningQuotas.stream()
                .collect(Collectors.groupingBy(ClusterAlertQuota::getServiceCategory))
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream()
                                .collect(Collectors.collectingAndThen(
                                        Collectors.toCollection(
                                                () -> new TreeSet<>(Comparator.comparing(
                                                        ClusterAlertQuota::getAlertQuotaName))),
                                        ArrayList::new))));
    }
}
