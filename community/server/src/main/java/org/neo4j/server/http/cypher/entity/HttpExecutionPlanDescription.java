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
package org.neo4j.server.http.cypher.entity;

import static org.neo4j.server.http.cypher.entity.HttpProfilerStatistics.fromMapValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

public class HttpExecutionPlanDescription implements ExecutionPlanDescription {
    private final String name;
    private final List<ExecutionPlanDescription> children;
    private final Map<String, Object> arguments;
    private final Set<String> identifiers;
    private final ProfilerStatistics profilerStatistics;
    private static final DefaultValueMapper valueMapper = new DefaultValueMapper(null);

    private HttpExecutionPlanDescription(
            String name,
            List<ExecutionPlanDescription> children,
            Map<String, Object> arguments,
            Set<String> identifiers,
            ProfilerStatistics profilerStatistics) {
        this.name = name;
        this.children = children;
        this.arguments = arguments;
        this.identifiers = identifiers;
        this.profilerStatistics = profilerStatistics;
    }

    public static ExecutionPlanDescription fromAnyValue(AnyValue anyValue) {
        if (anyValue == null) {
            return EMPTY;
        } else {
            MapValue mapValue = (MapValue) anyValue;
            ProfilerStatistics profilerStatistics = fromMapValue(mapValue);

            return new HttpExecutionPlanDescription(
                    ((TextValue) mapValue.get("operatorType")).stringValue(),
                    extractChildren((ListValue) mapValue.get("children")),
                    extractArguments((MapValue) mapValue.get("args")),
                    extractIdentifiers((ListValue) mapValue.get("identifiers")),
                    profilerStatistics);
        }
    }

    private static List<ExecutionPlanDescription> extractChildren(ListValue listValue) {
        var children = new ArrayList<ExecutionPlanDescription>();
        listValue.forEach(v -> children.add(fromAnyValue(v)));
        return children;
    }

    @SuppressWarnings("unchecked")
    private static Set<String> extractIdentifiers(ListValue identifiers) {
        List<String> identifierList = (List<String>) identifiers.map(valueMapper);
        return new HashSet<>(identifierList);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractArguments(MapValue argumentsMapValue) {
        return (Map<String, Object>) argumentsMapValue.map(valueMapper);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<ExecutionPlanDescription> getChildren() {
        return children;
    }

    @Override
    public Map<String, Object> getArguments() {
        return arguments;
    }

    @Override
    public Set<String> getIdentifiers() {
        return identifiers;
    }

    @Override
    public boolean hasProfilerStatistics() {
        return profilerStatistics == null;
    }

    @Override
    public ProfilerStatistics getProfilerStatistics() {
        return profilerStatistics;
    }

    public static final ExecutionPlanDescription EMPTY = new ExecutionPlanDescription() {
        @Override
        public String getName() {
            return "";
        }

        @Override
        public List<ExecutionPlanDescription> getChildren() {
            return Collections.emptyList();
        }

        @Override
        public Map<String, Object> getArguments() {
            return Collections.emptyMap();
        }

        @Override
        public Set<String> getIdentifiers() {
            return Collections.emptySet();
        }

        @Override
        public boolean hasProfilerStatistics() {
            return false;
        }

        @Override
        public ProfilerStatistics getProfilerStatistics() {
            return null;
        }
    };
}
