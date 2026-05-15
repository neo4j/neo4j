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
package org.neo4j.procedure.builtin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.capabilities.CapabilitiesService;
import org.neo4j.capabilities.Name;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;

/**
 * Tests for {@link ListComponentsProcedure}.
 */
class ListComponentsProcedureTest {

    static Configuration configuration(
            String customConfigVersion, Boolean experimentalCypherVersionsEnabled, Boolean graphEngineEnabled) {
        return new Configuration() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> T get(Setting<T> setting) {
                if (GraphDatabaseInternalSettings.custom_kernel_version == setting) {
                    return (T) customConfigVersion;
                } else if (GraphDatabaseInternalSettings.enable_experimental_cypher_versions == setting) {
                    return (T) experimentalCypherVersionsEnabled;
                }
                return null;
            }
        };
    }

    static Context context(
            String customConfigVersion, Boolean experimentalCypherVersionsEnabled, Boolean graphEngineEnabled) {
        var capabilitiesService = Mockito.mock(CapabilitiesService.class);
        when(capabilitiesService.get(Name.of("virtual_graph.version"))).thenReturn("4711");

        var dependencyResolver = Mockito.mock(DependencyResolver.class);
        when(dependencyResolver.resolveDependency(CapabilitiesService.class)).thenReturn(capabilitiesService);
        when(dependencyResolver.resolveDependency(Configuration.class))
                .thenReturn(configuration(customConfigVersion, experimentalCypherVersionsEnabled, graphEngineEnabled));

        var graphDatabaseAPI = Mockito.mock(GraphDatabaseAPI.class);
        when(graphDatabaseAPI.dbmsInfo()).thenReturn(DbmsInfo.COMMUNITY);

        var context = Mockito.mock(Context.class);
        when(context.dependencyResolver()).thenReturn(dependencyResolver);
        when(context.graphDatabaseAPI()).thenReturn(graphDatabaseAPI);
        return context;
    }

    @Test
    void usesCustomVersionWhenConfigured() throws ProcedureException {
        // Given
        var context = context("5.27.0", false, false);
        var procedure = new ListComponentsProcedure(new QualifiedName(new String[] {"dbms"}, "components"));

        // When
        try (var result = procedure.apply(context, new AnyValue[0], null)) {
            // Then
            var row = filterByComponentName(Iterators.asList(result), "Neo4j Kernel");

            var versions = (ListValue) row[1];
            assertThat(versions.intSize()).isEqualTo(1);
            assertThat(((TextValue) versions.value(0)).stringValue()).isEqualTo("5.27.0");
            assertThat(((TextValue) row[2]).stringValue()).isEqualTo("community");
        }
    }

    @Test
    void listCypherVersions() throws ProcedureException {
        var context = context("5.27.0", false, false);
        var procedure = new ListComponentsProcedure(new QualifiedName(new String[] {"dbms"}, "components"));

        try (var result = procedure.apply(context, new AnyValue[0], null)) {
            var row = filterByComponentName(Iterators.asList(result), "Cypher");

            var versions = (ListValue) row[1];
            assertThat(versions.intSize()).isEqualTo(2);
            assertThat(((TextValue) versions.value(0)).stringValue()).isEqualTo("5");
            assertThat(((TextValue) versions.value(1)).stringValue()).isEqualTo("25");
            assertThat(((TextValue) row[2]).stringValue()).isEmpty();
        }
    }

    @Test
    void listCypherVersionsIncluding25() throws ProcedureException {
        var context = context("5.27.0", true, false);
        var procedure = new ListComponentsProcedure(new QualifiedName(new String[] {"dbms"}, "components"));

        try (var result = procedure.apply(context, new AnyValue[0], null)) {
            var row = filterByComponentName(Iterators.asList(result), "Cypher");

            var versions = (ListValue) row[1];
            assertThat(versions.intSize()).isEqualTo(2);
            assertThat(((TextValue) versions.value(0)).stringValue()).isEqualTo("5");
            assertThat(((TextValue) versions.value(1)).stringValue()).isEqualTo("25");
            assertThat(((TextValue) row[2]).stringValue()).isEmpty();
        }
    }

    private static AnyValue[] filterByComponentName(List<AnyValue[]> data, String name) {
        return data.stream()
                .filter(row -> {
                    if (row[0] instanceof TextValue cell) {
                        return name.equals(cell.stringValue());
                    }
                    return false;
                })
                .findFirst()
                .orElseThrow();
    }
}
