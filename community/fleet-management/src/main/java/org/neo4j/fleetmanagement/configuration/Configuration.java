/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.fleetmanagement.configuration;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.fleetmanagement.communication.model.ConfigurationResponse;
import org.neo4j.fleetmanagement.communication.model.MigrationToAura;
import org.neo4j.fleetmanagement.metrics.model.MetricsDefinition;

public class Configuration {
    private final PropertyChangeSupport changeSupport;
    private List<MetricsDefinition> metrics;
    private List<String> neo4jConfigKeyGlobs;
    private Map<TaskType, Long> taskReportingInterval;
    private List<MigrationToAura> migrationsToAura;
    public static final String METRICS_CHANGE = "metrics";
    public static final String NEO4J_CONFIGS_CHANGE = "neo4jConfigs";
    public static final String TASK_REPORTING_INTERVAL_CHANGE = "taskReportingInterval";
    public static final String MIGRATIONS_TO_AURA_CHANGE = "migrationsToAura";

    public Configuration() {
        this.changeSupport = new PropertyChangeSupport(this);
    }

    public void addPropertyChangeListener(PropertyChangeListener listener) {
        changeSupport.addPropertyChangeListener(listener);
    }

    public void removePropertyChangeListeners() {
        for (var listener : changeSupport.getPropertyChangeListeners()) {
            changeSupport.removePropertyChangeListener(listener);
        }
    }

    public void setMigrationsToAura(List<MigrationToAura> migrationsToAura) {
        var oldMigrationsToAura = this.migrationsToAura;
        this.migrationsToAura = migrationsToAura;
        changeSupport.firePropertyChange(MIGRATIONS_TO_AURA_CHANGE, oldMigrationsToAura, migrationsToAura);
    }

    public void setMetrics(List<MetricsDefinition> metrics) {
        var oldMetrics = this.metrics;
        this.metrics = metrics;
        changeSupport.firePropertyChange(METRICS_CHANGE, oldMetrics, metrics);
    }

    public List<MetricsDefinition> getMetrics() {
        return metrics;
    }

    public void setTaskReportingInterval(Map<TaskType, Long> taskReportingInterval) {
        var oldTaskReportingInterval = this.taskReportingInterval;
        this.taskReportingInterval = taskReportingInterval;
        changeSupport.firePropertyChange(
                TASK_REPORTING_INTERVAL_CHANGE, oldTaskReportingInterval, taskReportingInterval);
    }

    public Map<TaskType, Long> getTaskReportingInterval() {
        return taskReportingInterval;
    }

    public void setNeo4jConfigKeyGlobs(List<String> neo4jConfigKeyGlobs) {
        var oldNeo4jConfigs = this.neo4jConfigKeyGlobs;
        this.neo4jConfigKeyGlobs = neo4jConfigKeyGlobs;
        changeSupport.firePropertyChange(NEO4J_CONFIGS_CHANGE, oldNeo4jConfigs, neo4jConfigKeyGlobs);
    }

    public List<String> getNeo4jConfigKeyGlobs() {
        return neo4jConfigKeyGlobs;
    }

    public enum TaskType {
        TOPOLOGY,
        METRICS,
        PING,
        QUERIES,
        SECURITY_LOGS,
        MIGRATIONS_TO_AURA,
        UNKNOWN;

        public static TaskType fromString(String type) {
            try {
                return TaskType.valueOf(type.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return UNKNOWN;
            }
        }
    }

    public static void updateConfigurationIfPresent(
            Configuration configuration, ConfigurationResponse configurationResponse) throws IllegalArgumentException {
        if (configurationResponse == null) {
            return;
        }
        if (configurationResponse.getMetrics() != null
                && !configurationResponse.getMetrics().isEmpty()) {
            configuration.setMetrics(configurationResponse.getMetrics().stream()
                    .map(MetricsDefinition::from)
                    .collect(Collectors.toList()));
        }
        if (configurationResponse.getNeo4jConfigKeyGlobs() != null
                && !configurationResponse.getNeo4jConfigKeyGlobs().isEmpty()) {
            configuration.setNeo4jConfigKeyGlobs(configurationResponse.getNeo4jConfigKeyGlobs());
        }
        if (configurationResponse.getReportingIntervals() != null
                && !configurationResponse.getReportingIntervals().isEmpty()) {
            try {
                var reportingIntervals = configurationResponse.getReportingIntervals().entrySet().stream()
                        .collect(Collectors.toMap(
                                (e -> Configuration.TaskType.fromString(e.getKey())),
                                e -> Duration.parse(e.getValue()).getSeconds()));
                configuration.setTaskReportingInterval(reportingIntervals);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException(
                        "Invalid duration format in reporting intervals: " + e.getMessage(), e);
            }
        }
        if (configurationResponse.getPendingMigrationsToAura() != null) {
            configuration.setMigrationsToAura(configurationResponse.getPendingMigrationsToAura());
        }
    }
}
