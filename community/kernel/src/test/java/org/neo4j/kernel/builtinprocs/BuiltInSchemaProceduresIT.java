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
import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;

public class BuiltInSchemaProceduresIT extends KernelIntegrationTest
{

    private final String[] nodesProcedureName = {"db", "schema", "nodeTypeProperties"};
    private final String[] relsProcedureName = {"db", "schema", "relTypeProperties"};

    @Test
    public void testWeirdLabelName() throws Throwable
    {
        // Given

        // Node1: (:`This:is_a:label` {color: "red"})

        createNode( Arrays.asList( "`This:is_a:label`" ), Arrays.asList( "color" ), Arrays.asList( Values.stringValue( "red" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":``This:is_a:label``", Arrays.asList( "`This:is_a:label`" ), "color", Arrays.asList( "String" ), true) ) ) );
//        printStream( stream );
    }

    @Test
    public void testNodePropertiesRegardlessOfCreationOrder1() throws Throwable
    {
        // Given

        // Node1: (:A {color: "red", size: "M"})
        // Node2: (:A {origin: "Kenya"})

        createNode( Arrays.asList( "A" ), Arrays.asList( "color", "size" ), Arrays.asList( Values.stringValue( "red" ), Values.stringValue( "M" ) ) );
        createNode( Arrays.asList( "A" ), Arrays.asList( "origin" ), Arrays.asList( Values.stringValue( "Kenya" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`A`", Arrays.asList( "A" ), "color", Arrays.asList( "String" ), false) ),
                equalTo( nodeEntry(":`A`", Arrays.asList( "A" ), "size", Arrays.asList( "String" ), false) ),
                equalTo( nodeEntry(":`A`", Arrays.asList( "A" ), "origin", Arrays.asList( "String" ), false) ) ) );
//        printStream( stream );
    }

    @Test
    public void testNodePropertiesRegardlessOfCreationOrder2() throws Throwable
    {
        // Given

        // Node1: (:B {origin: "Kenya"})
        // Node2 (:B {color: "red", size: "M"})

        createNode( Arrays.asList( "B" ), Arrays.asList( "origin" ), Arrays.asList( Values.stringValue( "Kenya" ) ) );
        createNode( Arrays.asList( "B" ), Arrays.asList( "color", "size" ), Arrays.asList( Values.stringValue( "red" ), Values.stringValue( "M" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`B`", Arrays.asList( "B" ), "color", Arrays.asList( "String" ), false) ),
                equalTo( nodeEntry(":`B`", Arrays.asList( "B" ), "size", Arrays.asList( "String" ), false) ),
                equalTo( nodeEntry(":`B`", Arrays.asList( "B" ), "origin", Arrays.asList( "String" ), false) ) ) );

//        printStream( stream );
    }

    @Test
    public void testNodePropertiesRegardlessOfCreationOrder3() throws Throwable
    {
        // Given

        // Node1: (:C {color: "red", size: "M"})
        // Node2: (:C {origin: "Kenya", active: true})

        createNode( Arrays.asList( "C" ), Arrays.asList( "color", "size" ), Arrays.asList( Values.stringValue( "red" ), Values.stringValue( "M" ) ) );
        createNode( Arrays.asList( "C" ), Arrays.asList( "origin", "active" ), Arrays.asList( Values.stringValue( "Kenya" ), Values.booleanValue( true ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`C`", Arrays.asList( "C" ), "color", Arrays.asList( "String" ), false) ),
                equalTo( nodeEntry(":`C`", Arrays.asList( "C" ), "size", Arrays.asList( "String" ), false) ),
                equalTo( nodeEntry(":`C`", Arrays.asList( "C" ), "origin", Arrays.asList( "String" ), false) ),
                equalTo( nodeEntry(":`C`", Arrays.asList( "C" ), "active", Arrays.asList( "Boolean" ), false) ) ) );

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
                Arrays.asList( Values.stringValue( "red" ), Values.stringValue( "M" ) ) );
        createRelationship( emptyNode, "R", emptyNode, Arrays.asList( "origin" ), Arrays.asList( Values.stringValue( "Kenya" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "color", Arrays.asList( "String" ), false) ),
                equalTo( relEntry(":`R`", "size", Arrays.asList( "String" ), false) ),
                equalTo( relEntry(":`R`", "origin", Arrays.asList( "String" ), false) ) ) );
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
        createRelationship( emptyNode, "R", emptyNode, Arrays.asList( "origin" ), Arrays.asList( Values.stringValue( "Kenya" ) ) );
        createRelationship( emptyNode, "R", emptyNode, Arrays.asList( "color", "size" ),
                Arrays.asList( Values.stringValue( "red" ), Values.stringValue( "M" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "color", Arrays.asList( "String" ), false) ),
                equalTo( relEntry(":`R`", "size", Arrays.asList( "String" ), false) ),
                equalTo( relEntry(":`R`", "origin", Arrays.asList( "String" ), false) ) ) );

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
                Arrays.asList( Values.stringValue( "red" ), Values.stringValue( "M" ) ) );
        createRelationship( emptyNode, "R", emptyNode, Arrays.asList( "origin", "active" ),
                Arrays.asList( Values.stringValue( "Kenya" ), Values.booleanValue( true ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "color", Arrays.asList( "String" ), false) ),
                equalTo( relEntry(":`R`", "size", Arrays.asList( "String" ), false) ),
                equalTo( relEntry(":`R`", "origin", Arrays.asList( "String" ), false) ),
                equalTo( relEntry(":`R`", "active", Arrays.asList( "Boolean" ), false))) );

//        printStream( stream );
    }

    @Test
    public void testNodesShouldNotDependOnOrderOfCreationWithOverlap() throws Throwable
    {
        // Given

        // Node1: (:B {type:'B1})
        // Node2: (:B {type:'B2', size: 5})

        createNode( Arrays.asList( "B" ), Arrays.asList( "type" ), Arrays.asList( Values.stringValue( "B1" ) ) );
        createNode( Arrays.asList( "B" ), Arrays.asList( "type", "size" ), Arrays.asList( Values.stringValue( "B2" ), Values.intValue( 5 ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`B`", Arrays.asList( "B" ), "type", Arrays.asList( "String" ), true) ),
                equalTo( nodeEntry(":`B`", Arrays.asList( "B" ), "size", Arrays.asList( "Integer" ), false) ) ) );

//         printStream( stream );
    }

    @Test
    public void testNodesShouldNotDependOnOrderOfCreationWithOverlap2() throws Throwable
    {
        // Given

        // Node1: (:B {type:'B2', size: 5})
        // Node2: (:B {type:'B1})

        createNode( Arrays.asList( "B" ), Arrays.asList( "type", "size" ), Arrays.asList( Values.stringValue( "B2" ), Values.intValue( 5 ) ) );
        createNode( Arrays.asList( "B" ), Arrays.asList( "type" ), Arrays.asList( Values.stringValue( "B1" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`B`", Arrays.asList( "B" ), "type", Arrays.asList( "String" ), true) ),
                equalTo( nodeEntry(":`B`", Arrays.asList( "B" ), "size", Arrays.asList( "Integer" ), false) ) ) );

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
        createRelationship( nodeId1, "B", nodeId1, Arrays.asList( "type" ), Arrays.asList( Values.stringValue( "B1" ) ) );
        createRelationship( nodeId1, "B", nodeId1, Arrays.asList( "type", "size" ), Arrays.asList( Values.stringValue( "B1" ), Values.intValue( 5 ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`B`", "type", Arrays.asList( "String" ), true) ),
                equalTo( relEntry(":`B`", "size", Arrays.asList( "Integer" ), false) ) ) );

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
        createRelationship( nodeId1, "B", nodeId1, Arrays.asList( "type", "size" ), Arrays.asList( Values.stringValue( "B1" ), Values.intValue( 5 ) ) );
        createRelationship( nodeId1, "B", nodeId1, Arrays.asList( "type" ), Arrays.asList( Values.stringValue( "B1" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`B`", "type", Arrays.asList( "String" ), true) ),
                equalTo( relEntry(":`B`", "size", Arrays.asList( "Integer" ), false) ) ) );

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

        createNode( Arrays.asList( "A", "B" ), Arrays.asList( "prop1", "prop2" ), Arrays.asList( Values.stringValue( "Test" ), Values.intValue( 12 ) ) );
        createNode( Arrays.asList( "B" ), Arrays.asList( "prop1" ), Arrays.asList( Values.booleanValue( true ) ) );
        createEmptyNode();
        createNode( Arrays.asList( "C" ), Arrays.asList( "prop1" ), Arrays.asList( Values.stringArray( "Test", "Success" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`A`:`B`", Arrays.asList( "A", "B" ), "prop1", Arrays.asList( "String" ), true) ),
                equalTo( nodeEntry(":`A`:`B`", Arrays.asList( "A", "B" ), "prop2", Arrays.asList( "Integer" ), true) ),
                equalTo( nodeEntry(":`B`", Arrays.asList( "B" ), "prop1", Arrays.asList( "Boolean" ), true) ),
                equalTo( nodeEntry(":`C`", Arrays.asList( "C" ), "prop1", Arrays.asList( "StringArray" ), true) ),
                equalTo( nodeEntry("", Arrays.asList(), null, null, false) ) ) );

        // printStream( stream );
    }

    @Test
    public void testWithSimilarNodes() throws Throwable
    {
        // Given

        // Node1: (:A {prop1:"Test"})
        // Node2: (:A {prop1:"Test2"})

        createNode( Arrays.asList( "A" ), Arrays.asList( "prop1" ), Arrays.asList( Values.stringValue( "Test" ) ) );
        createNode( Arrays.asList( "A" ), Arrays.asList( "prop1" ), Arrays.asList( Values.stringValue( "Test2" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), contains(
                equalTo( nodeEntry(":`A`", Arrays.asList("A"), "prop1", Arrays.asList( "String" ), true) ) ) );

        // printStream( stream );
    }

    @Test
    public void testWithSimilarNodesHavingDifferentPropertyValueTypes() throws Throwable
    {
        // Given

        // Node1: ({prop1:"Test", prop2: 12, prop3: true})
        // Node2: ({prop1:"Test", prop2: 1.5, prop3: "Test"})
        // Node3: ({prop1:"Test"})

        createNode( Arrays.asList(), Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( Values.stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createNode( Arrays.asList(), Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( Values.stringValue( "Test" ), Values.floatValue( 1.5f ), Values.stringValue( "Test" ) ) );
        createNode( Arrays.asList(), Arrays.asList( "prop1" ), Arrays.asList( Values.stringValue( "Test" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry("", Arrays.asList(), "prop1", Arrays.asList( "String" ), true) ),
                equalTo( nodeEntry("", Arrays.asList(), "prop2", Arrays.asList( "Integer", "Float" ), false) ),
                equalTo( nodeEntry("", Arrays.asList(), "prop3", Arrays.asList( "String", "Boolean" ), false) ) ) );

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
        createNode( Arrays.asList(), Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( Values.stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createNode( Arrays.asList(), Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( Values.stringValue( "Test" ), Values.floatValue( 1.5f ), Values.stringValue( "Test" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry("", Arrays.asList(), "prop1", Arrays.asList( "String" ), false) ),
                equalTo( nodeEntry("", Arrays.asList(), "prop2", Arrays.asList( "Integer", "Float" ), false) ),
                equalTo( nodeEntry("", Arrays.asList(), "prop3", Arrays.asList( "String", "Boolean" ), false) ) ) );

        // printStream( stream );
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
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1", "prop2" ), Arrays.asList( Values.stringValue( "Test" ), Values.intValue( 12 ) ) );
        createRelationship( nodeId1, "X", nodeId1, Arrays.asList( "prop1" ), Arrays.asList( Values.booleanValue( true ) ) );
        createRelationship( nodeId1, "Z", nodeId1, Arrays.asList(), Arrays.asList() );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ),
                containsInAnyOrder(
                        equalTo( relEntry(":`R`", "prop1", Arrays.asList( "String" ), true) ),
                        equalTo( relEntry(":`R`", "prop2", Arrays.asList( "Integer" ), true) ),
                        equalTo( relEntry(":`X`", "prop1", Arrays.asList( "Boolean" ), true) ),
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
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1" ), Arrays.asList( Values.stringValue( "Test" ) ) );
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1" ), Arrays.asList( Values.stringValue( "Test2" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ),
                containsInAnyOrder(
                        equalTo( relEntry(":`R`", "prop1", Arrays.asList( "String" ), true ) ) ) );

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
                Arrays.asList( Values.stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList(), Arrays.asList() );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "prop1", Arrays.asList( "String" ), false) ),
                equalTo( relEntry(":`R`", "prop2", Arrays.asList( "Integer" ), false) ),
                equalTo( relEntry(":`R`", "prop3", Arrays.asList( "Boolean" ), false) ) ) );

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
                Arrays.asList( Values.stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( Values.stringValue( "Test" ), Values.floatValue( 1.5f ), Values.stringValue( "Test" ) ) );
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1" ), Arrays.asList( Values.stringValue( "Test" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "prop1", Arrays.asList( "String" ), true) ),
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
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList(), Arrays.asList() );
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( Values.stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createRelationship( nodeId1, "R", nodeId1, Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( Values.stringValue( "Test" ), Values.floatValue( 1.5f ), Values.stringValue( "Test" ) ) );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( relsProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( relEntry(":`R`", "prop1", Arrays.asList( "String" ), false) ),
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

        createNode( Arrays.asList( "A" ), Arrays.asList( "prop1", "prop2", "prop3" ),
                Arrays.asList( Values.stringValue( "Test" ), Values.intValue( 12 ), Values.booleanValue( true ) ) );
        createNode( Arrays.asList( "A" ), Arrays.asList( "prop1", "prop3" ), Arrays.asList( Values.stringValue( "Test2" ), Values.booleanValue( false ) ) );
        createNode( Arrays.asList( "A" ), Arrays.asList( "prop1", "prop2" ), Arrays.asList( Values.stringValue( "Test3" ), Values.intValue( 42 ) ) );
        createNode( Arrays.asList( "B" ), Arrays.asList( "prop1", "prop2" ), Arrays.asList( Values.stringValue( "Test4" ), Values.intValue( 21 ) ) );
        createNode( Arrays.asList( "B" ), Arrays.asList(), Arrays.asList() );

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( nodesProcedureName ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( nodeEntry(":`A`", Arrays.asList("A"), "prop1", Arrays.asList( "String" ), true) ),
                equalTo( nodeEntry(":`A`", Arrays.asList("A"), "prop2", Arrays.asList( "Integer" ), false) ),
                equalTo( nodeEntry(":`A`", Arrays.asList("A"), "prop3", Arrays.asList( "Boolean" ), false) ),
                equalTo( nodeEntry(":`B`", Arrays.asList("B"), "prop1", Arrays.asList( "String" ), false) ),
                equalTo( nodeEntry(":`B`", Arrays.asList("B"), "prop2", Arrays.asList( "Integer" ), false) ) ) );

        //printStream( stream );
    }

    private Object[] nodeEntry( String escapedLabels, List<String> labels, String propertyName, List<String> propertyValueTypes, Boolean mandatory )
    {
        return new Object[]{escapedLabels, labels, propertyName, propertyValueTypes, mandatory};
    }

    private Object[] relEntry( String labelsOrRelType, String propertyName, List<String> propertyValueTypes, Boolean mandatory )
    {
        return new Object[]{labelsOrRelType, propertyName, propertyValueTypes, mandatory};
    }

    private long createEmptyNode() throws Throwable
    {
        return createNode( Arrays.asList(), Arrays.asList(), Arrays.asList() );
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
    private void printStream( RawIterator<Object[],ProcedureException> stream ) throws Throwable
    {
        Iterator<Object[]> iterator = asList( stream ).iterator();
        while ( iterator.hasNext() )
        {
            Object[] row = iterator.next();
            for ( Object column : row )
            {
                System.out.println( column );
            }
            System.out.println();
        }
    }
}
