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
package org.neo4j.fleetmanagement.metrics.model;

import java.util.List;
import java.util.Locale;
import org.neo4j.fleetmanagement.communication.model.MetricsDefinitionResponse;

public class MetricsDefinition {

    private final String name;
    private final String metricName;
    private final List<String> tags;
    private final JmxMetricSpecification jmxMetricSpecification;

    public MetricsDefinition(
            String name, String metricName, List<String> tags, JmxMetricSpecification jmxMetricSpecification) {
        this.name = name;
        this.metricName = metricName;
        this.tags = tags;
        this.jmxMetricSpecification = jmxMetricSpecification;
    }

    public String getName() {
        return name;
    }

    public String getMetricName() {
        return metricName;
    }

    public List<String> getTags() {
        return tags;
    }

    public JmxMetricSpecification getJmxMetricSpecification() {
        return jmxMetricSpecification;
    }

    public static class JmxMetricSpecification {
        public enum MetricTypeEnum {
            GAUGE,
            COUNTER,
            HISTOGRAM
        }

        public MetricTypeEnum metricType;
    }

    public static MetricsDefinition from(MetricsDefinitionResponse metricsDefinitionResponse) {
        return new MetricsDefinition(
                metricsDefinitionResponse.name,
                metricsDefinitionResponse.metricName,
                metricsDefinitionResponse.tags,
                new JmxMetricSpecification() {
                    {
                        metricType =
                                MetricTypeEnum.valueOf(metricsDefinitionResponse.metricType.toUpperCase(Locale.ROOT));
                    }
                });
    }
}
