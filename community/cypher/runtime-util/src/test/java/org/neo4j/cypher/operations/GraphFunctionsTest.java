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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.neo4j.configuration.helpers.RemoteUri;
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.security.DatabaseAccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.StringArray;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.virtual.MapValue;

public class GraphFunctionsTest extends CypherFunSuite {

    private static SecurityContext securityContext;
    private static DatabaseReferenceImpl.Composite composite;
    private static DatabaseReferenceImpl.Composite emptyComposite;

    // known ids
    private static final UUID compositeId = UUID.randomUUID();
    private static final UUID emptyCompositeId = UUID.randomUUID();
    private static final UUID localId = UUID.randomUUID();
    private static final UUID remoteId = UUID.randomUUID();
    private static final UUID hiddenLocalId = UUID.randomUUID();
    private static final UUID hiddenRemoteId = UUID.randomUUID();

    @BeforeAll
    public static void setup() {
        // create composite reference
        Set<DatabaseReference> constituents = new HashSet<>();
        constituents.add(internalConstituent("local", localId));
        constituents.add(internalConstituent("hiddenLocal", hiddenLocalId));
        constituents.add(remoteConstituent("remote", remoteId));
        constituents.add(remoteConstituent("hiddenRemote", hiddenRemoteId));
        composite = new DatabaseReferenceImpl.Composite(
                new NormalizedDatabaseName("composite"), mockId(compositeId), constituents);

        // create empty composite reference
        emptyComposite = new DatabaseReferenceImpl.Composite(
                new NormalizedDatabaseName("empty"), mockId(emptyCompositeId), new HashSet<>());

        // create security context
        securityContext = Mockito.mock(SecurityContext.class);
        DatabaseAccessMode accessMode = Mockito.mock(DatabaseAccessMode.class);
        Mockito.when(securityContext.databaseAccessMode()).thenReturn(accessMode);
        composite.constituents().forEach(constituent -> Mockito.when(accessMode.canAccessDatabase(constituent))
                .thenReturn(!constituent.fullName().name().contains("hidden")));
    }

    private static NamedDatabaseId mockId(UUID id) {
        var namedDatabaseId = Mockito.mock(NamedDatabaseId.class);
        Mockito.when(namedDatabaseId.databaseId()).thenReturn(DatabaseIdFactory.from(id));
        return namedDatabaseId;
    }

    @Test
    public void graphNames() {
        AnyValue names = GraphFunctions.names(composite, securityContext);
        assertThat(((StringArray) names).asObjectCopy()).containsExactly("composite.local", "composite.remote");
    }

    @Test
    public void graphNamesEmptyConstituents() {
        AnyValue names = GraphFunctions.names(emptyComposite, securityContext);
        assertEquals(0, ((StringArray) names).length());
    }

    @ParameterizedTest
    @ValueSource(strings = {"composite.local", "composite.remote"})
    public void graphByName(String name) {
        DatabaseReference graph = GraphFunctions.graphByName(name, composite, securityContext);
        assertEquals(name, graph.fullName().name());
    }

    @ParameterizedTest
    @ValueSource(strings = {"local", "composite.hiddenLocal", "composite.hiddenRemote", "composite", ""})
    public void graphByNameInvalid(String name) {
        Assertions.assertThrows(
                EntityNotFoundException.class, () -> GraphFunctions.graphByName(name, composite, securityContext));
    }

    @Test
    public void graphByNameEmptyConstituents() {
        Assertions.assertThrows(
                EntityNotFoundException.class,
                () -> GraphFunctions.graphByName("invalid", emptyComposite, securityContext));
    }

    @Test
    public void graphById() {
        for (UUID id : Arrays.asList(localId, remoteId)) {
            var graph = GraphFunctions.graphById(id, composite, securityContext);
            assertEquals(id, graph.id());
        }
    }

    @Test
    public void graphByIdInvalid() {
        for (UUID id : Arrays.asList(hiddenLocalId, hiddenRemoteId, compositeId, emptyCompositeId, UUID.randomUUID())) {
            Assertions.assertThrows(
                    EntityNotFoundException.class, () -> GraphFunctions.graphById(id, composite, securityContext));
        }
    }

    @Test
    public void graphByIdInvalidEmptyConstituents() {
        Assertions.assertThrows(
                EntityNotFoundException.class,
                () -> GraphFunctions.graphById(UUID.randomUUID(), emptyComposite, securityContext));
    }

    @ParameterizedTest
    @ValueSource(strings = {"local", "composite.hiddenLocal", "composite.hiddenRemote", "composite", ""})
    public void graphPropertiesInvalidGraph(String name) {
        Assertions.assertThrows(
                EntityNotFoundException.class,
                () -> GraphFunctions.graphProperties(
                        name, composite, securityContext, Mockito.mock(TopologyGraphDbmsModel.class)));
    }

    @Test
    public void graphPropertiesNonEmpty() {
        TopologyGraphDbmsModel topologyGraphDbmsModel = Mockito.mock(TopologyGraphDbmsModel.class);
        Map<String, Object> properties = new HashMap<>();
        properties.put("prop", "val");

        Mockito.when(topologyGraphDbmsModel.getAliasProperties(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(Optional.of(properties));

        MapValue result =
                GraphFunctions.graphProperties("composite.local", composite, securityContext, topologyGraphDbmsModel);
        assertEquals(1, result.size());
        assertEquals("val", ((StringValue) result.get("prop")).stringValue());
    }

    @Test
    public void graphPropertiesEmpty() {
        TopologyGraphDbmsModel topologyGraphDbmsModel = Mockito.mock(TopologyGraphDbmsModel.class);

        Mockito.when(topologyGraphDbmsModel.getAliasProperties(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(Optional.empty());

        MapValue result =
                GraphFunctions.graphProperties("composite.remote", composite, securityContext, topologyGraphDbmsModel);
        assertEquals(0, result.size());
    }

    private static DatabaseReference internalConstituent(String name, UUID id) {
        return new DatabaseReferenceImpl.Internal(
                new NormalizedDatabaseName(name), new NormalizedDatabaseName("composite"), mockId(id), false);
    }

    private static DatabaseReference remoteConstituent(String alias, UUID id) {
        return new DatabaseReferenceImpl.External(
                new NormalizedDatabaseName("remoteDb"),
                new NormalizedDatabaseName(alias),
                new NormalizedDatabaseName("composite"),
                Mockito.mock(RemoteUri.class),
                id);
    }
}
