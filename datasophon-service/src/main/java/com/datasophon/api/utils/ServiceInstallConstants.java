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

package com.datasophon.api.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Constants used by the service installation workflow.
 * Extracted from ServiceInstallServiceImpl to reduce magic strings
 * and make configuration rules easier to maintain.
 */
public final class ServiceInstallConstants {

    private ServiceInstallConstants() {
    }

    /**
     * Infrastructure services that MUST be deployed (either already running
     * or selected in the current install batch).
     * Checked by checkServiceDependency.
     */
    public static final List<String> INFRASTRUCTURE_SERVICE_DEPENDENCIES =
            Collections.unmodifiableList(Arrays.asList("ALERTMANAGER", "GRAFANA", "PROMETHEUS"));

    /**
     * Monitoring roles that must reside on the same physical host.
     * Used by checkSameNodeConstraint.
     */
    public static final List<String> MUST_AT_SAME_NODE_ROLES =
            Collections.unmodifiableList(Arrays.asList("Grafana", "AlertManager", "Prometheus"));

    /**
     * Role cardinality rules. Key = role name, Value = required host count.
     * A value of -1 means "odd number required".
     */
    public static final Map<String, Integer> ROLE_CARDINALITY_RULES;
    static {
        Map<String, Integer> m = new LinkedHashMap<>();
        m.put("JournalNode", 3);
        m.put("NameNode", 2);
        m.put("ZKFC", 2);
        m.put("ResourceManager", 2);
        m.put("ZkServer", -1);
        m.put("DorisFE", -1);
        m.put("KyuubiServer", 2);
        ROLE_CARDINALITY_RULES = Collections.unmodifiableMap(m);
    }

    /**
     * Roles with odd-number requirement - used to produce correct error messages.
     */
    public static final Set<String> ODD_NUMBER_ROLES =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("ZkServer", "DorisFE")));

    // Prometheus special-case constants
    public static final String PROMETHEUS_SERVICE_NAME = "prometheus";
    public static final String PROMETHEUS_WORKER_FILENAME = "worker.json";
    public static final String PROMETHEUS_NODE_FILENAME = "linux.json";
    public static final String PROMETHEUS_SCRAPE_TEMPLATE = "scrape.ftl";
    public static final String PROMETHEUS_OUTPUT_DIRECTORY = "configs";
    public static final int PROMETHEUS_WORKER_PORT = 8585;
    public static final int PROMETHEUS_NODE_PORT = 9100;
}
