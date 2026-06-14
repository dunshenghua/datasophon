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

package com.datasophon.api.controller;

import com.datasophon.api.service.ClusterAlertQuotaService;
import com.datasophon.common.utils.Result;
import com.datasophon.dao.entity.ClusterAlertQuota;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("cluster/alert/quota")
public class ClusterAlertQuotaController {

    @Autowired
    private ClusterAlertQuotaService clusterAlertQuotaService;

    /**
     * list alert quota
     */
    @RequestMapping("/list")
    public Result info(Integer clusterId, Integer alertGroupId, String quotaName, Integer page, Integer pageSize) {
        if (page == null || page < 1) {
            page = 1;
        }
        if (pageSize == null || pageSize < 1) {
            pageSize = 10;
        }
        return clusterAlertQuotaService.getAlertQuotaList(clusterId, alertGroupId, quotaName, page, pageSize);
    }

    /**
     * enable alert quota
     */
    @RequestMapping("/start")
    public Result start(Integer clusterId, String alertQuotaIds) {
        if (clusterId == null) {
            return Result.error("集群ID不能为空");
        }
        return clusterAlertQuotaService.start(clusterId, alertQuotaIds);
    }

    /**
     * disable alert quota
     */
    @RequestMapping("/stop")
    public Result stop(Integer clusterId, String alertQuotaIds) {
        if (clusterId == null) {
            return Result.error("集群ID不能为空");
        }
        return clusterAlertQuotaService.stop(clusterId, alertQuotaIds);
    }

    /**
     * save alert quota
     */
    @RequestMapping("/save")
    public Result save(@RequestBody ClusterAlertQuota clusterAlertQuota) {
        if (clusterAlertQuota == null) {
            return Result.error("告警指标信息不能为空");
        }
        return clusterAlertQuotaService.saveAlertQuota(clusterAlertQuota);
    }

    /**
     * update alert quota
     */
    @RequestMapping("/update")
    public Result update(@RequestBody ClusterAlertQuota clusterAlertQuota) {
        if (clusterAlertQuota == null) {
            return Result.error("告警指标信息不能为空");
        }
        return clusterAlertQuotaService.updateAlertQuota(clusterAlertQuota);
    }

    /**
     * delete alert quota
     */
    @RequestMapping("/delete")
    public Result delete(@RequestBody Integer[] ids) {
        if (ids == null || ids.length == 0) {
            return Result.error("告警指标ID不能为空");
        }
        clusterAlertQuotaService.removeByIds(Arrays.asList(ids));
        return Result.success();
    }

}
