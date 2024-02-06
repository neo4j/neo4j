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
package org.neo4j.cypher.operations;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

public final class GraphFunctions {

    private GraphFunctions() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    public static AnyValue names(DatabaseReferenceImpl.Composite composite, SecurityContext securityContext) {
        String[] graphNames = composite.constituents().stream()
                .filter(constituent -> securityContext.databaseAccessMode().canAccessDatabase(constituent))
                .map(constituent -> constituent.fullName().name())
                .toArray(String[]::new);
        return Values.arrayValue(graphNames, false);
    }

    public static DatabaseReference graphByName(
            String databaseName, DatabaseReferenceImpl.Composite composite, SecurityContext securityContext) {
        Optional<DatabaseReference> graph = composite.getConstituentByName(databaseName);
        if (graph.isPresent() && securityContext.databaseAccessMode().canAccessDatabase(graph.get())) {
            return graph.get();
        } else {
            throw graphNotFound(composite.fullName().name(), databaseName);
        }
    }

    public static DatabaseReference graphById(
            UUID databaseId, DatabaseReferenceImpl.Composite composite, SecurityContext securityContext) {
        Optional<DatabaseReference> graph = composite.getConstituentById(databaseId);
        if (graph.isPresent() && securityContext.databaseAccessMode().canAccessDatabase(graph.get())) {
            return graph.get();
        } else {
            throw graphNotFound(composite.fullName().name(), databaseId.toString());
        }
    }

    public static MapValue graphProperties(
            String databaseName,
            DatabaseReferenceImpl.Composite composite,
            SecurityContext securityContext,
            TopologyGraphDbmsModel topologyGraphDbmsModel) {
        DatabaseReference graph = GraphFunctions.graphByName(databaseName, composite, securityContext);

        Optional<Map<String, Object>> properties = topologyGraphDbmsModel.getAliasProperties(
                graph.fullName().name(),
                graph.namespace().map(NormalizedDatabaseName::name).orElse(null));
        var builder = new MapValueBuilder();
        properties.ifPresent(
                stringObjectMap -> stringObjectMap.forEach((key, value) -> builder.add(key, Values.of(value))));
        return builder.build();
    }

    private static EntityNotFoundException graphNotFound(String compositeGraph, String graph) {
        return new EntityNotFoundException(String.format(
                "When connected to a composite database, access is allowed only to its constituents. Attempted to access '%s' while connected to '%s'",
                graph, compositeGraph));
    }
}
