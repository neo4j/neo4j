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
package org.neo4j.kernel.builtinprocs;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.values.storable.Values.stringOrNoValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class BuiltInSchemaProceduresIT extends KernelIntegrationTest {
    private final String[] nodesProcedureName = {"db", "schema", "nodeTypeProperties"};
    private final String[] relsProcedureName = {"db", "schema", "relTypeProperties"};

    @Test
    void testWeirdLabelName() throws Throwable {
        // Given

        // Node1: (:`This:is_a:label` {color: "red"})

        createNode(singletonList("This:is_a:label"), singletonList("color"), singletonList(stringValue("red")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(nodesProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(nodeEntry(
                            ":`This:is_a:label`",
                            singletonList("This:is_a:label"),
                            "color",
                            singletonList("String"),
                            true));
        }
    }

    @Test
    void testNodePropertiesRegardlessOfCreationOrder1() throws Throwable {
        // Given

        // Node1: (:A {color: "red", size: "M"})
        // Node2: (:A {origin: "Kenya"})

        createNode(
                singletonList("A"),
                Arrays.asList("color", "size"),
                Arrays.asList(stringValue("red"), stringValue("M")));
        createNode(singletonList("A"), singletonList("origin"), singletonList(stringValue("Kenya")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(nodesProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            nodeEntry(":`A`", singletonList("A"), "color", singletonList("String"), false),
                            nodeEntry(":`A`", singletonList("A"), "size", singletonList("String"), false),
                            nodeEntry(":`A`", singletonList("A"), "origin", singletonList("String"), false));
        }
    }

    @Test
    void testNodePropertiesRegardlessOfCreationOrder2() throws Throwable {
        // Given

        // Node1: (:B {origin: "Kenya"})
        // Node2 (:B {color: "red", size: "M"})

        createNode(singletonList("B"), singletonList("origin"), singletonList(stringValue("Kenya")));
        createNode(
                singletonList("B"),
                Arrays.asList("color", "size"),
                Arrays.asList(stringValue("red"), stringValue("M")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(nodesProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            nodeEntry(":`B`", singletonList("B"), "color", singletonList("String"), false),
                            nodeEntry(":`B`", singletonList("B"), "size", singletonList("String"), false),
                            nodeEntry(":`B`", singletonList("B"), "origin", singletonList("String"), false));
        }
    }

    @Test
    void testNodePropertiesRegardlessOfCreationOrder3() throws Throwable {
        // Given

        // Node1: (:C {color: "red", size: "M"})
        // Node2: (:C {origin: "Kenya", active: true})

        createNode(
                singletonList("C"),
                Arrays.asList("color", "size"),
                Arrays.asList(stringValue("red"), stringValue("M")));
        createNode(
                singletonList("C"),
                Arrays.asList("origin", "active"),
                Arrays.asList(stringValue("Kenya"), Values.booleanValue(true)));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(nodesProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            nodeEntry(":`C`", singletonList("C"), "color", singletonList("String"), false),
                            nodeEntry(":`C`", singletonList("C"), "size", singletonList("String"), false),
                            nodeEntry(":`C`", singletonList("C"), "origin", singletonList("String"), false),
                            nodeEntry(":`C`", singletonList("C"), "active", singletonList("Boolean"), false));
        }
    }

    @Test
    void testRelsPropertiesRegardlessOfCreationOrder1() throws Throwable {
        // Given

        // Node1: (A)
        // Rel1: (A)-[:R {color: "red", size: "M"}]->(A)
        // Rel2: (A)-[:R {origin: "Kenya"}]->(A)

        long emptyNode = createEmptyNode();
        createRelationship(
                emptyNode,
                "R",
                emptyNode,
                Arrays.asList("color", "size"),
                Arrays.asList(stringValue("red"), stringValue("M")));
        createRelationship(emptyNode, "R", emptyNode, singletonList("origin"), singletonList(stringValue("Kenya")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(relsProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            relEntry(":`R`", "color", singletonList("String"), false),
                            relEntry(":`R`", "size", singletonList("String"), false),
                            relEntry(":`R`", "origin", singletonList("String"), false));
        }
    }

    @Test
    void testRelsPropertiesRegardlessOfCreationOrder2() throws Throwable {
        // Given

        // Node1: (A)
        // Rel1: (A)-[:R {origin: "Kenya"}]->(A)
        // Rel2: (A)-[:R {color: "red", size: "M"}]->(A)

        long emptyNode = createEmptyNode();
        createRelationship(emptyNode, "R", emptyNode, singletonList("origin"), singletonList(stringValue("Kenya")));
        createRelationship(
                emptyNode,
                "R",
                emptyNode,
                Arrays.asList("color", "size"),
                Arrays.asList(stringValue("red"), stringValue("M")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(relsProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            relEntry(":`R`", "color", singletonList("String"), false),
                            relEntry(":`R`", "size", singletonList("String"), false),
                            relEntry(":`R`", "origin", singletonList("String"), false));
        }
    }

    @Test
    void testRelsPropertiesRegardlessOfCreationOrder3() throws Throwable {
        // Given

        // Node1: (A)
        // Rel1: (A)-[:R {color: "red", size: "M"}]->(A)
        // Rel2: (A)-[:R {origin: "Kenya", active: true}}]->(A)

        long emptyNode = createEmptyNode();
        createRelationship(
                emptyNode,
                "R",
                emptyNode,
                Arrays.asList("color", "size"),
                Arrays.asList(stringValue("red"), stringValue("M")));
        createRelationship(
                emptyNode,
                "R",
                emptyNode,
                Arrays.asList("origin", "active"),
                Arrays.asList(stringValue("Kenya"), Values.booleanValue(true)));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(relsProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            relEntry(":`R`", "color", singletonList("String"), false),
                            relEntry(":`R`", "size", singletonList("String"), false),
                            relEntry(":`R`", "origin", singletonList("String"), false),
                            relEntry(":`R`", "active", singletonList("Boolean"), false));
        }
    }

    @Test
    void testNodesShouldNotDependOnOrderOfCreationWithOverlap() throws Throwable {
        // Given

        // Node1: (:B {type:'B1})
        // Node2: (:B {type:'B2', size: 5})

        createNode(singletonList("B"), singletonList("type"), singletonList(stringValue("B1")));
        createNode(
                singletonList("B"),
                Arrays.asList("type", "size"),
                Arrays.asList(stringValue("B2"), Values.intValue(5)));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(nodesProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            nodeEntry(":`B`", singletonList("B"), "type", singletonList("String"), true),
                            nodeEntry(":`B`", singletonList("B"), "size", singletonList("Integer"), false));
        }
    }

    @Test
    void testNodesShouldNotDependOnOrderOfCreationWithOverlap2() throws Throwable {
        // Given

        // Node1: (:B {type:'B2', size: 5})
        // Node2: (:B {type:'B1})

        createNode(
                singletonList("B"),
                Arrays.asList("type", "size"),
                Arrays.asList(stringValue("B2"), Values.intValue(5)));
        createNode(singletonList("B"), singletonList("type"), singletonList(stringValue("B1")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(nodesProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            nodeEntry(":`B`", singletonList("B"), "type", singletonList("String"), true),
                            nodeEntry(":`B`", singletonList("B"), "size", singletonList("Integer"), false));
        }
    }

    @Test
    void testRelsShouldNotDependOnOrderOfCreationWithOverlap() throws Throwable {
        // Given

        // Node1: (n)
        // Rel1: (n)-[:B {type:'B1}]->(n)
        // Rel2: (n)-[:B {type:'B2', size: 5}]->(n)

        long nodeId1 = createEmptyNode();
        createRelationship(nodeId1, "B", nodeId1, singletonList("type"), singletonList(stringValue("B1")));
        createRelationship(
                nodeId1,
                "B",
                nodeId1,
                Arrays.asList("type", "size"),
                Arrays.asList(stringValue("B1"), Values.intValue(5)));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(relsProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            relEntry(":`B`", "type", singletonList("String"), true),
                            relEntry(":`B`", "size", singletonList("Integer"), false));
        }
    }

    @Test
    void testRelsShouldNotDependOnOrderOfCreationWithOverlap2() throws Throwable {
        // Given

        // Node1: (n)
        // Rel1: (n)-[:B {type:'B2', size: 5}]->(n)
        // Rel2: (n)-[:B {type:'B1}]->(n)

        long nodeId1 = createEmptyNode();
        createRelationship(
                nodeId1,
                "B",
                nodeId1,
                Arrays.asList("type", "size"),
                Arrays.asList(stringValue("B1"), Values.intValue(5)));
        createRelationship(nodeId1, "B", nodeId1, singletonList("type"), singletonList(stringValue("B1")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(relsProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            relEntry(":`B`", "type", singletonList("String"), true),
                            relEntry(":`B`", "size", singletonList("Integer"), false));
        }
    }

    @Test
    void testWithAllDifferentNodes() throws Throwable {
        // Given

        // Node1: (:A:B {prop1:"Test", prop2:12})
        // Node2: (:B {prop1:true})
        // Node3: ()
        // Node4: (:C {prop1: ["Test","Success"]}

        createNode(
                Arrays.asList("A", "B"),
                Arrays.asList("prop1", "prop2"),
                Arrays.asList(stringValue("Test"), Values.intValue(12)));
        createNode(singletonList("B"), singletonList("prop1"), singletonList(Values.TRUE));
        createEmptyNode();
        createNode(singletonList("C"), singletonList("prop1"), singletonList(Values.stringArray("Test", "Success")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(nodesProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            nodeEntry(":`A`:`B`", Arrays.asList("A", "B"), "prop1", singletonList("String"), true),
                            nodeEntry(":`A`:`B`", Arrays.asList("A", "B"), "prop2", singletonList("Integer"), true),
                            nodeEntry(":`B`", singletonList("B"), "prop1", singletonList("Boolean"), true),
                            nodeEntry(":`C`", singletonList("C"), "prop1", singletonList("StringArray"), true),
                            nodeEntry("", emptyList(), null, null, false));
        }
    }

    @Test
    void testWithSimilarNodes() throws Throwable {
        // Given

        // Node1: (:A {prop1:"Test"})
        // Node2: (:A {prop1:"Test2"})

        createNode(singletonList("A"), singletonList("prop1"), singletonList(stringValue("Test")));
        createNode(singletonList("A"), singletonList("prop1"), singletonList(stringValue("Test2")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(nodesProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .containsExactly(nodeEntry(":`A`", singletonList("A"), "prop1", singletonList("String"), true));
        }
    }

    @Test
    void testWithSimilarNodesHavingDifferentPropertyValueTypes() throws Throwable {
        // Given

        // Node1: ({prop1:"Test", prop2: 12, prop3: true})
        // Node2: ({prop1:"Test", prop2: 1.5, prop3: "Test"})
        // Node3: ({prop1:"Test"})

        createNode(
                emptyList(),
                Arrays.asList("prop1", "prop2", "prop3"),
                Arrays.asList(stringValue("Test"), Values.intValue(12), Values.booleanValue(true)));
        createNode(
                emptyList(),
                Arrays.asList("prop1", "prop2", "prop3"),
                Arrays.asList(stringValue("Test"), Values.floatValue(1.5f), stringValue("Test")));
        createNode(emptyList(), singletonList("prop1"), singletonList(stringValue("Test")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(nodesProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            nodeEntry("", emptyList(), "prop1", singletonList("String"), true),
                            nodeEntry("", emptyList(), "prop2", Arrays.asList("Integer", "Float"), false),
                            nodeEntry("", emptyList(), "prop3", Arrays.asList("String", "Boolean"), false));
        }
    }

    @Test
    void testWithSimilarNodesShouldNotDependOnOrderOfCreation() throws Throwable {
        // Given

        // Node1: ()
        // Node2: ({prop1:"Test", prop2: 12, prop3: true})
        // Node3: ({prop1:"Test", prop2: 1.5, prop3: "Test"})

        createEmptyNode();
        createNode(
                emptyList(),
                Arrays.asList("prop1", "prop2", "prop3"),
                Arrays.asList(stringValue("Test"), Values.intValue(12), Values.booleanValue(true)));
        createNode(
                emptyList(),
                Arrays.asList("prop1", "prop2", "prop3"),
                Arrays.asList(stringValue("Test"), Values.floatValue(1.5f), stringValue("Test")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(nodesProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            nodeEntry("", emptyList(), "prop1", singletonList("String"), false),
                            nodeEntry("", emptyList(), "prop2", Arrays.asList("Integer", "Float"), false),
                            nodeEntry("", emptyList(), "prop3", Arrays.asList("String", "Boolean"), false));
        }
    }

    @Test
    void testWithAllDifferentRelationships() throws Throwable {
        // Given

        // Node1: ()
        // Rel1: (Node1)-[:R{prop1:"Test", prop2:12}]->(Node1)
        // Rel2: (Node1)-[:X{prop1:true}]->(Node1)
        // Rel3: (Node1)-[:Z{}]->(Node1)

        long nodeId1 = createEmptyNode();
        createRelationship(
                nodeId1,
                "R",
                nodeId1,
                Arrays.asList("prop1", "prop2"),
                Arrays.asList(stringValue("Test"), Values.intValue(12)));
        createRelationship(nodeId1, "X", nodeId1, singletonList("prop1"), singletonList(Values.TRUE));
        createRelationship(nodeId1, "Z", nodeId1, emptyList(), emptyList());

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(relsProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            relEntry(":`R`", "prop1", singletonList("String"), true),
                            relEntry(":`R`", "prop2", singletonList("Integer"), true),
                            relEntry(":`X`", "prop1", singletonList("Boolean"), true),
                            relEntry(":`Z`", null, null, false));
        }
    }

    @Test
    void testWithSimilarRelationships() throws Throwable {
        // Given

        // Node1: ()
        // Rel1: (node1)-[:R{prop1:"Test"}]->(node1)
        // Rel2: (node1)-[:R{prop1:"Test2"}]->(node1)

        long nodeId1 = createEmptyNode();
        createRelationship(nodeId1, "R", nodeId1, singletonList("prop1"), singletonList(stringValue("Test")));
        createRelationship(nodeId1, "R", nodeId1, singletonList("prop1"), singletonList(stringValue("Test2")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(relsProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream)).contains(relEntry(":`R`", "prop1", singletonList("String"), true));
        }
    }

    @Test
    void testSchemaWithRelationshipWithoutProperties() throws Throwable {
        // Given

        // Node1: ()
        // Rel1: (node1)-[:R{prop1:"Test", prop2: 12, prop3: true}]->(node1)
        // Rel2: (node1)-[:R]->(node1)

        long nodeId1 = createEmptyNode();
        createRelationship(
                nodeId1,
                "R",
                nodeId1,
                Arrays.asList("prop1", "prop2", "prop3"),
                Arrays.asList(stringValue("Test"), Values.intValue(12), Values.booleanValue(true)));
        createRelationship(nodeId1, "R", nodeId1, emptyList(), emptyList());

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(relsProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            relEntry(":`R`", "prop1", singletonList("String"), false),
                            relEntry(":`R`", "prop2", singletonList("Integer"), false),
                            relEntry(":`R`", "prop3", singletonList("Boolean"), false));
        }
    }

    @Test
    void testWithSimilarRelationshipsHavingDifferentPropertyValueTypes() throws Throwable {
        // Given

        // Node1: ()
        // Rel1: (node1)-[:R{prop1:"Test", prop2: 12, prop3: true}]->(node1)
        // Rel2: (node1)-[:R{prop1:"Test", prop2: 1.5, prop3: "Test"}]->(node1)
        // Rel3: (node1)-[:R{prop1:"Test"}]->(node1)

        long nodeId1 = createEmptyNode();
        createRelationship(
                nodeId1,
                "R",
                nodeId1,
                Arrays.asList("prop1", "prop2", "prop3"),
                Arrays.asList(stringValue("Test"), Values.intValue(12), Values.booleanValue(true)));
        createRelationship(
                nodeId1,
                "R",
                nodeId1,
                Arrays.asList("prop1", "prop2", "prop3"),
                Arrays.asList(stringValue("Test"), Values.floatValue(1.5f), stringValue("Test")));
        createRelationship(nodeId1, "R", nodeId1, singletonList("prop1"), singletonList(stringValue("Test")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(relsProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            relEntry(":`R`", "prop1", singletonList("String"), true),
                            relEntry(":`R`", "prop2", Arrays.asList("Integer", "Float"), false),
                            relEntry(":`R`", "prop3", Arrays.asList("String", "Boolean"), false));
        }
    }

    @Test
    void testWithSimilarRelationshipsShouldNotDependOnOrderOfCreation() throws Throwable {
        // This is basically the same as the test before but the empty rel is created first
        // Given

        // Node1: ()
        // Rel1: (node1)-[:R]->(node1)
        // Rel2: (node1)-[:R{prop1:"Test", prop2: 12, prop3: true}]->(node1)
        // Rel3: (node1)-[:R{prop1:"Test", prop2: 1.5, prop3: "Test"}]->(node1)

        long nodeId1 = createEmptyNode();
        createRelationship(nodeId1, "R", nodeId1, emptyList(), emptyList());
        createRelationship(
                nodeId1,
                "R",
                nodeId1,
                Arrays.asList("prop1", "prop2", "prop3"),
                Arrays.asList(stringValue("Test"), Values.intValue(12), Values.booleanValue(true)));
        createRelationship(
                nodeId1,
                "R",
                nodeId1,
                Arrays.asList("prop1", "prop2", "prop3"),
                Arrays.asList(stringValue("Test"), Values.floatValue(1.5f), stringValue("Test")));

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(relsProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            relEntry(":`R`", "prop1", singletonList("String"), false),
                            relEntry(":`R`", "prop2", Arrays.asList("Integer", "Float"), false),
                            relEntry(":`R`", "prop3", Arrays.asList("String", "Boolean"), false));
        }
    }

    @Test
    void testWithNullableProperties() throws Throwable {
        // Given

        // Node1: (:A{prop1:"Test", prop2: 12, prop3: true})
        // Node2: (:A{prop1:"Test2", prop3: false})
        // Node3: (:A{prop1:"Test3", prop2: 42})
        // Node4: (:B{prop1:"Test4", prop2: 21})
        // Node5: (:B)

        createNode(
                singletonList("A"),
                Arrays.asList("prop1", "prop2", "prop3"),
                Arrays.asList(stringValue("Test"), Values.intValue(12), Values.booleanValue(true)));
        createNode(
                singletonList("A"),
                Arrays.asList("prop1", "prop3"),
                Arrays.asList(stringValue("Test2"), Values.booleanValue(false)));
        createNode(
                singletonList("A"),
                Arrays.asList("prop1", "prop2"),
                Arrays.asList(stringValue("Test3"), Values.intValue(42)));
        createNode(
                singletonList("B"),
                Arrays.asList("prop1", "prop2"),
                Arrays.asList(stringValue("Test4"), Values.intValue(21)));
        createNode(singletonList("B"), emptyList(), emptyList());

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName(nodesProcedureName), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream))
                    .contains(
                            nodeEntry(":`A`", singletonList("A"), "prop1", singletonList("String"), true),
                            nodeEntry(":`A`", singletonList("A"), "prop2", singletonList("Integer"), false),
                            nodeEntry(":`A`", singletonList("A"), "prop3", singletonList("Boolean"), false),
                            nodeEntry(":`B`", singletonList("B"), "prop1", singletonList("String"), false),
                            nodeEntry(":`B`", singletonList("B"), "prop2", singletonList("Integer"), false));
        }
    }

    private static AnyValue[] nodeEntry(
            String escapedLabels,
            List<String> labels,
            String propertyName,
            List<String> propertyValueTypes,
            boolean mandatory) {
        return new AnyValue[] {
            stringValue(escapedLabels),
            ValueUtils.asListValue(labels),
            stringOrNoValue(propertyName),
            ValueUtils.of(propertyValueTypes),
            Values.booleanValue(mandatory)
        };
    }

    private static AnyValue[] relEntry(
            String labelsOrRelType, String propertyName, List<String> propertyValueTypes, boolean mandatory) {
        return new AnyValue[] {
            stringOrNoValue(labelsOrRelType),
            stringOrNoValue(propertyName),
            ValueUtils.of(propertyValueTypes),
            Values.booleanValue(mandatory)
        };
    }

    private long createEmptyNode() throws Throwable {
        return createNode(emptyList(), emptyList(), emptyList());
    }

    private long createNode(List<String> labels, List<String> propKeys, List<Value> propValues) throws Throwable {
        assert labels != null;
        assert propKeys.size() == propValues.size();

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
        long nodeId = transaction.dataWrite().nodeCreate();

        for (String labelname : labels) {
            int labelId = transaction.tokenWrite().labelGetOrCreateForName(labelname);
            transaction.dataWrite().nodeAddLabel(nodeId, labelId);
        }

        for (int i = 0; i < propKeys.size(); i++) {
            String propKeyName = propKeys.get(i);
            Value propValue = propValues.get(i);
            int propKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName(propKeyName);
            transaction.dataWrite().nodeSetProperty(nodeId, propKeyId, propValue);
        }
        commit();
        return nodeId;
    }

    private void createRelationship(
            long startNode, String type, long endNode, List<String> propKeys, List<Value> propValues) throws Throwable {
        assert type != null && !type.isEmpty();
        assert propKeys.size() == propValues.size();

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        int typeId = transaction.tokenWrite().relationshipTypeGetOrCreateForName(type);
        long relId = transaction.dataWrite().relationshipCreate(startNode, typeId, endNode);

        for (int i = 0; i < propKeys.size(); i++) {
            String propKeyName = propKeys.get(i);
            Value propValue = propValues.get(i);
            int propKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName(propKeyName);
            transaction.dataWrite().relationshipSetProperty(relId, propKeyId, propValue);
        }
        commit();
    }
}
