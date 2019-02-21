/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.builtinprocs;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.values.storable.Values.stringOrNoValue;
import static org.neo4j.values.storable.Values.stringValue;

public class BuiltInSchemaProceduresIT extends KernelIntegrationTest
{

    private final String[] nodesProcedureName = {"db", "schema", "nodeTypeProperties"};
    private final String[] relsProcedureName = {"db", "schema", "relTypeProperties"};

    @Test
    public void testWeirdLabelName() throws Throwable
    {
        // Given

        // Node1: (:`This:is_a:label` {color: "red"})

        createNode( singletonList( "`This:is_a:label`" ), singletonList( "color" ),
                singletonList( stringValue( "red" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry( ":``This:is_a:label``", singletonList( "`This:is_a:label`" ), "color",
                        singletonList( "String" ), true ) ) ) );
//        printStream( stream );
    }

    @Test
    public void testNodePropertiesRegardlessOfCreationOrder1() throws Throwable
    {
        // Given

        // Node1: (:A {color: "red", size: "M"})
        // Node2: (:A {origin: "Kenya"})

        createNode( singletonList( "A" ), Arrays.asList( "color", "size" ), Arrays.asList( stringValue( "red" ), stringValue( "M" ) ) );
        createNode( singletonList( "A" ), singletonList( "origin" ), singletonList( stringValue( "Kenya" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`A`", singletonList( "A" ), "color", singletonList( "String" ), false) ),
                equalTo( nodeEntry(":`A`", singletonList( "A" ), "size", singletonList( "String" ), false) ),
                equalTo( nodeEntry(":`A`", singletonList( "A" ), "origin", singletonList( "String" ), false) ) ) );
//        printStream( stream );
    }

    @Test
    public void testNodePropertiesRegardlessOfCreationOrder2() throws Throwable
    {
        // Given

        // Node1: (:B {origin: "Kenya"})
        // Node2 (:B {color: "red", size: "M"})

        createNode( singletonList( "B" ), singletonList( "origin" ), singletonList( stringValue( "Kenya" ) ) );
        createNode( singletonList( "B" ), Arrays.asList( "color", "size" ), Arrays.asList( stringValue( "red" ), stringValue( "M" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`B`", singletonList( "B" ), "color", singletonList( "String" ), false) ),
                equalTo( nodeEntry(":`B`", singletonList( "B" ), "size", singletonList( "String" ), false) ),
                equalTo( nodeEntry(":`B`", singletonList( "B" ), "origin", singletonList( "String" ), false) ) ) );

//        printStream( stream );
    }

    @Test
    public void testNodePropertiesRegardlessOfCreationOrder3() throws Throwable
    {
        // Given

        // Node1: (:C {color: "red", size: "M"})
        // Node2: (:C {origin: "Kenya", active: true})

        createNode( singletonList( "C" ), Arrays.asList( "color", "size" ), Arrays.asList( stringValue( "red" ), stringValue( "M" ) ) );
        createNode( singletonList( "C" ), Arrays.asList( "origin", "active" ), Arrays.asList( stringValue( "Kenya" ), Values.booleanValue( true ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new AnyValue[0] );

        // Then
             assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`C`", singletonList( "C" ), "color", singletonList( "String" ), false) ),
                equalTo( nodeEntry(":`C`", singletonList( "C" ), "size", singletonList( "String" ), false) ),
                equalTo( nodeEntry(":`C`", singletonList( "C" ), "origin", singletonList( "String" ), false) ),
                equalTo( nodeEntry(":`C`", singletonList( "C" ), "active", singletonList( "Boolean" ), false) ) ) );

//        printStream( stream );
    }

    @Test
    public void testRelsPropertiesRegardlessOfCreationOrder1() throws Throwable
    {
        // Given

        // Node1: (A)
        // Rel1: (A)-[:R {color: "red", size: "M"}]->(A)
        // Rel2: (A)-[:R {origin: "Kenya"}]->(A)

        long emptyNode = createEmptyNode();
        createRelationship( emptyNode, "R", emptyNode, Arrays.asList( "color", "size" ),
                Arrays.asList( stringValue( "red" ), stringValue( "M" ) ) );
        createRelationship( emptyNode, "R", emptyNode, singletonList( "origin" ),singletonList( stringValue( "Kenya" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "color", singletonList( "String" ), false) ),
                equalTo( relEntry(":`R`", "size", singletonList( "String" ), false) ),
                equalTo( relEntry(":`R`", "origin", singletonList( "String" ), false) ) ) );
//        printStream( stream );
    }

    @Test
    public void testRelsPropertiesRegardlessOfCreationOrder2() throws Throwable
    {
        // Given

        // Node1: (A)
        // Rel1: (A)-[:R {origin: "Kenya"}]->(A)
        // Rel2: (A)-[:R {color: "red", size: "M"}]->(A)

        long emptyNode = createEmptyNode();
        createRelationship( emptyNode, "R", emptyNode, singletonList( "origin" ),singletonList( stringValue( "Kenya" ) ) );
        createRelationship( emptyNode, "R", emptyNode, Arrays.asList( "color", "size" ),
                Arrays.asList( stringValue( "red" ), stringValue( "M" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "color", singletonList( "String" ), false) ),
                equalTo( relEntry(":`R`", "size", singletonList( "String" ), false) ),
                equalTo( relEntry(":`R`", "origin", singletonList( "String" ), false) ) ) );

//        printStream( stream );
    }

    @Test
    public void testRelsPropertiesRegardlessOfCreationOrder3() throws Throwable
    {
        // Given

        // Node1: (A)
        // Rel1: (A)-[:R {color: "red", size: "M"}]->(A)
        // Rel2: (A)-[:R {origin: "Kenya", active: true}}]->(A)

        long emptyNode = createEmptyNode();
        createRelationship( emptyNode, "R", emptyNode, Arrays.asList( "color", "size" ),
                Arrays.asList( stringValue( "red" ), stringValue( "M" ) ) );
        createRelationship( emptyNode, "R", emptyNode, Arrays.asList( "origin", "active" ),
                Arrays.asList( stringValue( "Kenya" ), Values.booleanValue( true ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "color", singletonList( "String" ), false) ),
                equalTo( relEntry(":`R`", "size", singletonList( "String" ), false) ),
                equalTo( relEntry(":`R`", "origin", singletonList( "String" ), false) ),
                equalTo( relEntry(":`R`", "active", singletonList( "Boolean" ), false))) );

//        printStream( stream );
    }

    @Test
    public void testNodesShouldNotDependOnOrderOfCreationWithOverlap() throws Throwable
    {
        // Given

        // Node1: (:B {type:'B1})
        // Node2: (:B {type:'B2', size: 5})

        createNode( singletonList( "B" ), singletonList( "type" ), singletonList( stringValue( "B1" ) ) );
        createNode( singletonList( "B" ), Arrays.asList( "type", "size" ), Arrays.asList( stringValue( "B2" ), Values.intValue( 5 ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`B`", singletonList( "B" ), "type", singletonList( "String" ), true) ),
                equalTo( nodeEntry(":`B`", singletonList( "B" ), "size", singletonList( "Integer" ), false) ) ) );

//         printStream( stream );
    }

    @Test
    public void testNodesShouldNotDependOnOrderOfCreationWithOverlap2() throws Throwable
    {
        // Given

        // Node1: (:B {type:'B2', size: 5})
        // Node2: (:B {type:'B1})

        createNode( singletonList( "B" ), Arrays.asList( "type", "size" ), Arrays.asList( stringValue( "B2" ), Values.intValue( 5 ) ) );
        createNode( singletonList( "B" ), singletonList( "type" ), singletonList( stringValue( "B1" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`B`", singletonList( "B" ), "type", singletonList( "String" ), true) ),
                equalTo( nodeEntry(":`B`", singletonList( "B" ), "size", singletonList( "Integer" ), false) ) ) );

//         printStream( stream );
    }

    @Test
    public void testRelsShouldNotDependOnOrderOfCreationWithOverlap() throws Throwable
    {
        // Given

        // Node1: (n)
        // Rel1: (n)-[:B {type:'B1}]->(n)
        // Rel2: (n)-[:B {type:'B2', size: 5}]->(n)

        long nodeId1 = createEmptyNode();
        createRelationship( nodeId1, "B", nodeId1, singletonList( "type" ), singletonList( stringValue( "B1" ) ) );
        createRelationship( nodeId1, "B", nodeId1, Arrays.asList( "type", "size" ), Arrays.asList( stringValue( "B1" ), Values.intValue( 5 ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`B`", "type", singletonList( "String" ), true) ),
                equalTo( relEntry(":`B`", "size", singletonList( "Integer" ), false) ) ) );

//        printStream( stream );
    }

    @Test
    public void testRelsShouldNotDependOnOrderOfCreationWithOverlap2() throws Throwable
    {
        // Given

        // Node1: (n)
        // Rel1: (n)-[:B {type:'B2', size: 5}]->(n)
        // Rel2: (n)-[:B {type:'B1}]->(n)

        long nodeId1 = createEmptyNode();
        createRelationship( nodeId1, "B", nodeId1, Arrays.asList( "type", "size" ), Arrays.asList( stringValue( "B1" ), Values.intValue( 5 ) ) );
        createRelationship( nodeId1, "B", nodeId1, singletonList( "type" ), singletonList( stringValue( "B1" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`B`", "type", singletonList( "String" ), true) ),
                equalTo( relEntry(":`B`", "size", singletonList( "Integer" ), false) ) ) );

//        printStream( stream );
    }

    @Test
    public void testWithAllDifferentNodes() throws Throwable
    {
        // Given

        // Node1: (:A:B {prop1:"Test", prop2:12})
        // Node2: (:B {prop1:true})
        // Node3: ()
        // Node4: (:C {prop1: ["Test","Success"]}

        createNode( Arrays.asList( "A", "B" ), Arrays.asList( "prop1", "prop2" ), Arrays.asList( stringValue( "Test" ), Values.intValue( 12 ) ) );
        createNode( singletonList( "B" ), singletonList( "prop1" ), singletonList( Values.TRUE ) );
        createEmptyNode();
        createNode( singletonList( "C" ), singletonList( "prop1" ), singletonList( Values.stringArray( "Test", "Success" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`A`:`B`", Arrays.asList( "A", "B" ), "prop1", singletonList( "String" ), true) ),
                equalTo( nodeEntry(":`A`:`B`", Arrays.asList( "A", "B" ), "prop2", singletonList( "Integer" ), true) ),
                equalTo( nodeEntry(":`B`", singletonList( "B" ), "prop1", singletonList( "Boolean" ), true) ),
                equalTo( nodeEntry(":`C`", singletonList( "C" ), "prop1", singletonList( "StringArray" ), true) ),
                equalTo( nodeEntry("", emptyList(), null, null, false) ) ) );

        // printStream( stream );
    }

    @Test
    public void testWithSimilarNodes() throws Throwable
    {
        // Given

        // Node1: (:A {prop1:"Test"})
        // Node2: (:A {prop1:"Test2"})

        createNode( singletonList( "A" ), singletonList( "prop1" ), singletonList( stringValue( "Test" ) ) );
        createNode( singletonList( "A" ), singletonList( "prop1" ), singletonList( stringValue( "Test2" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), contains(
                equalTo( nodeEntry(":`A`", singletonList("A"), "prop1", singletonList( "String" ), true) ) ) );

        // printStream( stream );
    }

    @Test
    public void testWithSimilarNodesHavingDifferentPropertyValueTypes() throws Throwable
    {
        // Given

        // Node1: ({prop1:"Test", prop2: 12, prop3: true})
        // Node2: ({prop1:"Test", prop2: 1.5, prop3: "Test"})
        // Node3: ({prop1:"Test"})

        createNode( emptyList(), Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createNode( emptyList(), Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( stringValue( "Test" ), Values.floatValue( 1.5f ), stringValue( "Test" ) ) );
        createNode( emptyList(), singletonList( "prop1" ), singletonList( stringValue( "Test" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry("", emptyList(), "prop1", singletonList( "String" ), true) ),
                equalTo( nodeEntry("", emptyList(), "prop2", Arrays.asList( "Integer", "Float" ), false) ),
                equalTo( nodeEntry("", emptyList(), "prop3", Arrays.asList( "String", "Boolean" ), false) ) ) );

        // printStream( stream );
    }

    @Test
    public void testWithSimilarNodesShouldNotDependOnOrderOfCreation() throws Throwable
    {
        // Given

        // Node1: ()
        // Node2: ({prop1:"Test", prop2: 12, prop3: true})
        // Node3: ({prop1:"Test", prop2: 1.5, prop3: "Test"})

        createEmptyNode();
        createNode( emptyList(), Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createNode( emptyList(), Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( stringValue( "Test" ), Values.floatValue( 1.5f ), stringValue( "Test" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry("", emptyList(), "prop1", singletonList( "String" ), false) ),
                equalTo( nodeEntry("", emptyList(), "prop2", Arrays.asList( "Integer", "Float" ), false) ),
                equalTo( nodeEntry("", emptyList(), "prop3", Arrays.asList( "String", "Boolean" ), false) ) ) );

//        printStream( stream );
    }

    @Test
    public void testWithAllDifferentRelationships() throws Throwable
    {
        // Given

        // Node1: ()
        // Rel1: (Node1)-[:R{prop1:"Test", prop2:12}]->(Node1)
        // Rel2: (Node1)-[:X{prop1:true}]->(Node1)
        // Rel3: (Node1)-[:Z{}]->(Node1)

        long nodeId1 = createEmptyNode();
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1", "prop2" ), Arrays.asList( stringValue( "Test" ), Values.intValue( 12 ) ) );
        createRelationship( nodeId1, "X", nodeId1, singletonList( "prop1" ), singletonList( Values.TRUE ) );
        createRelationship( nodeId1, "Z", nodeId1, emptyList(), emptyList() );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ),
                containsInAnyOrder(
                        equalTo( relEntry(":`R`", "prop1", singletonList( "String" ), true) ),
                        equalTo( relEntry(":`R`", "prop2", singletonList( "Integer" ), true) ),
                        equalTo( relEntry(":`X`", "prop1", singletonList( "Boolean" ), true) ),
                        equalTo( relEntry(":`Z`", null, null, false) ) ) );

        // printStream( stream );
    }

    @Test
    public void testWithSimilarRelationships() throws Throwable
    {
        // Given

        // Node1: ()
        // Rel1: (node1)-[:R{prop1:"Test"}]->(node1)
        // Rel2: (node1)-[:R{prop1:"Test2"}]->(node1)

        long nodeId1 = createEmptyNode();
        createRelationship( nodeId1, "R", nodeId1, singletonList( "prop1" ), singletonList( stringValue( "Test" ) ) );
        createRelationship( nodeId1, "R", nodeId1, singletonList( "prop1" ), singletonList( stringValue( "Test2" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ),
                containsInAnyOrder(
                        equalTo( relEntry(":`R`", "prop1", singletonList( "String" ), true ) ) ) );

        //printStream( stream );
    }

    @Test
    public void testSchemaWithRelationshipWithoutProperties() throws Throwable
    {
        // Given

        // Node1: ()
        // Rel1: (node1)-[:R{prop1:"Test", prop2: 12, prop3: true}]->(node1)
        // Rel2: (node1)-[:R]->(node1)

        long nodeId1 = createEmptyNode();
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createRelationship( nodeId1, "R", nodeId1, emptyList(), emptyList() );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "prop1", singletonList( "String" ), false) ),
                equalTo( relEntry(":`R`", "prop2", singletonList( "Integer" ), false) ),
                equalTo( relEntry(":`R`", "prop3", singletonList( "Boolean" ), false) ) ) );

        //printStream( stream );
    }

    @Test
    public void testWithSimilarRelationshipsHavingDifferentPropertyValueTypes() throws Throwable
    {
        // Given

        // Node1: ()
        // Rel1: (node1)-[:R{prop1:"Test", prop2: 12, prop3: true}]->(node1)
        // Rel2: (node1)-[:R{prop1:"Test", prop2: 1.5, prop3: "Test"}]->(node1)
        // Rel3: (node1)-[:R{prop1:"Test"}]->(node1)

        long nodeId1 = createEmptyNode();
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( stringValue( "Test" ), Values.floatValue( 1.5f ), stringValue( "Test" ) ) );
        createRelationship( nodeId1, "R", nodeId1, singletonList( "prop1" ), singletonList( stringValue( "Test" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "prop1", singletonList( "String" ), true) ),
                equalTo( relEntry(":`R`", "prop2", Arrays.asList( "Integer", "Float" ), false) ),
                equalTo( relEntry(":`R`", "prop3", Arrays.asList( "String", "Boolean" ), false) ) ) );

        //printStream( stream );
    }

    @Test
    public void testWithSimilarRelationshipsShouldNotDependOnOrderOfCreation() throws Throwable
    {
        // This is basically the same as the test before but the empty rel is created first
        // Given

        // Node1: ()
        // Rel1: (node1)-[:R]->(node1)
        // Rel2: (node1)-[:R{prop1:"Test", prop2: 12, prop3: true}]->(node1)
        // Rel3: (node1)-[:R{prop1:"Test", prop2: 1.5, prop3: "Test"}]->(node1)

        long nodeId1 = createEmptyNode();
        createRelationship( nodeId1, "R", nodeId1, emptyList(), emptyList() );
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( stringValue( "Test" ), Values.floatValue( 1.5f ), stringValue( "Test" ) ) );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "prop1", singletonList( "String" ), false) ),
                equalTo( relEntry(":`R`", "prop2", Arrays.asList( "Integer", "Float" ), false) ),
                equalTo( relEntry(":`R`", "prop3", Arrays.asList( "String", "Boolean" ), false) ) ) );

        //printStream( stream );
    }

    @Test
    public void testWithNullableProperties() throws Throwable
    {
        // Given

        // Node1: (:A{prop1:"Test", prop2: 12, prop3: true})
        // Node2: (:A{prop1:"Test2", prop3: false})
        // Node3: (:A{prop1:"Test3", prop2: 42})
        // Node4: (:B{prop1:"Test4", prop2: 21})
        // Node5: (:B)

        createNode( singletonList( "A" ), Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createNode( singletonList( "A" ), Arrays.asList( "prop1", "prop3" ), Arrays.asList( stringValue( "Test2" ), Values.booleanValue( false ) ) );
        createNode( singletonList( "A" ), Arrays.asList( "prop1", "prop2" ), Arrays.asList( stringValue( "Test3" ), Values.intValue( 42 ) ) );
        createNode( singletonList( "B" ), Arrays.asList( "prop1", "prop2" ), Arrays.asList( stringValue( "Test4" ), Values.intValue( 21 ) ) );
        createNode( singletonList( "B" ), emptyList(), emptyList() );

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`A`", singletonList("A"), "prop1", singletonList( "String" ), true) ),
                equalTo( nodeEntry(":`A`", singletonList("A"), "prop2", singletonList( "Integer" ), false) ),
                equalTo( nodeEntry(":`A`", singletonList("A"), "prop3", singletonList( "Boolean" ), false) ),
                equalTo( nodeEntry(":`B`", singletonList("B"), "prop1", singletonList( "String" ), false) ),
                equalTo( nodeEntry(":`B`", singletonList("B"), "prop2", singletonList( "Integer" ), false) ) ) );

        //printStream( stream );
    }

    private AnyValue[] nodeEntry( String escapedLabels, List<String> labels, String propertyName, List<String> propertyValueTypes, boolean mandatory )
    {
        return new AnyValue[]{stringValue( escapedLabels ), ValueUtils.asListValue( labels ),
                stringOrNoValue( propertyName ), ValueUtils.of( propertyValueTypes ),
                Values.booleanValue( mandatory )};
    }

    private AnyValue[] relEntry( String labelsOrRelType, String propertyName, List<String> propertyValueTypes,
            boolean mandatory )
    {
        return new AnyValue[]{stringOrNoValue( labelsOrRelType ), stringOrNoValue( propertyName ),
                ValueUtils.of( propertyValueTypes ), Values.booleanValue( mandatory )};
    }

    private long createEmptyNode() throws Throwable
    {
        return createNode( emptyList(), emptyList(), emptyList() );
    }

    private long createNode( List<String> labels, List<String> propKeys, List<Value> propValues ) throws Throwable
    {
        assert labels != null;
        assert propKeys.size() == propValues.size();

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId = transaction.dataWrite().nodeCreate();

        for ( String labelname : labels )
        {
            int labelId = transaction.tokenWrite().labelGetOrCreateForName( labelname );
            transaction.dataWrite().nodeAddLabel( nodeId, labelId );
        }

        for ( int i = 0; i < propKeys.size(); i++ )
        {
            String propKeyName = propKeys.get( i );
            Value propValue = propValues.get( i );
            int propKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( propKeyName );
            transaction.dataWrite().nodeSetProperty( nodeId, propKeyId, propValue );
        }
        commit();
        return nodeId;
    }

    private void createRelationship( long startNode, String type, long endNode, List<String> propKeys, List<Value> propValues ) throws Throwable
    {
        assert type != null && !type.equals( "" );
        assert propKeys.size() == propValues.size();

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        int typeId = transaction.tokenWrite().relationshipTypeGetOrCreateForName( type );
        long relId = transaction.dataWrite().relationshipCreate( startNode, typeId, endNode );

        for ( int i = 0; i < propKeys.size(); i++ )
        {
            String propKeyName = propKeys.get( i );
            Value propValue = propValues.get( i );
            int propKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( propKeyName );
            transaction.dataWrite().relationshipSetProperty( relId, propKeyId, propValue );
        }
        commit();
    }

    /*
      This method can be used to print to result stream to System.out -> Useful for debugging
     */
    @SuppressWarnings( "unused" )
    private void printStream( RawIterator<AnyValue[],ProcedureException> stream ) throws Throwable
    {
        for ( AnyValue[] row : asList( stream ) )
        {
            for ( Object column : row )
            {
                System.out.println( column );
            }
            System.out.println();
        }
    }
}
