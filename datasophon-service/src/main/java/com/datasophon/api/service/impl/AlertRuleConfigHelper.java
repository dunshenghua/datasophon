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

import com.datasophon.api.master.ActorUtils;
import com.datasophon.api.master.PrometheusActor;
import com.datasophon.common.command.GenerateAlertConfigCommand;
import com.datasophon.common.model.AlertItem;
import com.datasophon.common.model.Generators;
import com.datasophon.dao.entity.ClusterAlertQuota;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import akka.actor.ActorRef;

/**
 * Responsible for building Prometheus alert rule config and dispatching it
 * to the PrometheusActor via Akka.
 */
@Component
public class AlertRuleConfigHelper {

    private static final Logger logger = LoggerFactory.getLogger(AlertRuleConfigHelper.class);

    /**
     * Build alert rule config files from the given quotas (grouped by serviceCategory)
     * and dispatch the command to PrometheusActor.
     *
     * @param clusterId        the cluster to target
     * @param quotasByCategory running quotas grouped by serviceCategory
     */
    public void buildAndDispatch(Integer clusterId, Map<String, List<ClusterAlertQuota>> quotasByCategory) {
        if (quotasByCategory == null || quotasByCategory.isEmpty()) {
            logger.info("No alert quotas to dispatch for cluster {}", clusterId);
            return;
        }

        HashMap<Generators, List<AlertItem>> configFileMap = new HashMap<>();
        for (Map.Entry<String, List<ClusterAlertQuota>> entry : quotasByCategory.entrySet()) {
            String category = entry.getKey();
            List<ClusterAlertQuota> quotas = entry.getValue();

            Generators generators = new Generators();
            generators.setFilename(category.toLowerCase() + ".yml");
            generators.setConfigFormat("prometheus");
            generators.setOutputDirectory("alert_rules");

            ArrayList<AlertItem> alertItems = new ArrayList<>();
            for (ClusterAlertQuota quota : quotas) {
                AlertItem alertItem = new AlertItem();
                alertItem.setAlertName(quota.getAlertQuotaName());
                alertItem.setAlertExpr(
                        quota.getAlertExpr() + " " + quota.getCompareMethod() + " " + quota.getAlertThreshold());
                alertItem.setClusterId(clusterId);
                alertItem.setServiceRoleName(quota.getServiceRoleName());
                alertItem.setAlertLevel(quota.getAlertLevel().getDesc());
                alertItem.setAlertAdvice(quota.getAlertAdvice());
                alertItem.setTriggerDuration(quota.getTriggerDuration());
                alertItems.add(alertItem);
            }
            configFileMap.put(generators, alertItems);
        }

        GenerateAlertConfigCommand command = new GenerateAlertConfigCommand();
        command.setClusterId(clusterId);
        command.setConfigFileMap(configFileMap);

        logger.info("Dispatching alert rule config for cluster {}, categories: {}", clusterId,
                quotasByCategory.keySet());

        ActorRef prometheusActor =
                ActorUtils.getLocalActor(PrometheusActor.class, ActorUtils.getActorRefName(PrometheusActor.class));
        prometheusActor.tell(command, ActorRef.noSender());
    }
}
