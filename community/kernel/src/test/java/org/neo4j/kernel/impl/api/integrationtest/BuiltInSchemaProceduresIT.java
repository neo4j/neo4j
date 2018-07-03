/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;

public class BuiltInSchemaProceduresIT extends KernelIntegrationTest
{
    @Test
    public void testSchemaTableWithNodes() throws Throwable
    {
        // Given

        // Node1: (:A:B {prop1:"Test", prop2:12})
        // Node2: (:B {prop1:true})
        // Node3: ()
        // Node4: (:C {prop1: ["Test","Success"]}

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId1 = transaction.dataWrite().nodeCreate();
        long nodeId2 = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().nodeCreate(); // Node3
        long nodeId4 = transaction.dataWrite().nodeCreate();
        int labelId1 = transaction.tokenWrite().labelGetOrCreateForName( "A" );
        int labelId2 = transaction.tokenWrite().labelGetOrCreateForName( "B" );
        int labelId3 = transaction.tokenWrite().labelGetOrCreateForName( "C" );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int prop2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop1, Values.stringValue("Test") );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop2, Values.intValue(12) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop1, Values.booleanValue( true ) );
        transaction.dataWrite().nodeSetProperty( nodeId4, prop1, Values.stringArray( "Test","Success" ) );
        transaction.dataWrite().nodeAddLabel( nodeId1, labelId1 );
        transaction.dataWrite().nodeAddLabel( nodeId1, labelId2 );
        transaction.dataWrite().nodeAddLabel( nodeId2, labelId2 );
        transaction.dataWrite().nodeAddLabel( nodeId4, labelId3 );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "propertySchema" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"Node", Arrays.asList( "A", "B" ), "prop1", "STRING"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "A", "B" ), "prop2", "INTEGER"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "B" ), "prop1", "BOOLEAN"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "C" ), "prop1", "STRINGARRAY"} ),
                equalTo( new Object[]{"Node", Arrays.asList(), null, null} )) );

        // printStream( stream );
    }

    @Test
    public void testSchemaTableWithSimilarNodes() throws Throwable
    {
        // Given

        // Node1: (:A {prop1:"Test"})
        // Node2: (:A {prop1:"Test2"})

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId1 = transaction.dataWrite().nodeCreate();
        long nodeId2 = transaction.dataWrite().nodeCreate();
        int labelId1 = transaction.tokenWrite().labelGetOrCreateForName( "A" );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop1, Values.stringValue("Test") );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop1, Values.stringValue("Test2") );
        transaction.dataWrite().nodeAddLabel( nodeId1, labelId1 );
        transaction.dataWrite().nodeAddLabel( nodeId2, labelId1 );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "propertySchema" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), contains(
                equalTo( new Object[]{"Node", Arrays.asList("A"), "prop1", "STRING"} )) );

        // printStream( stream );
    }

    @Test
    public void testSchemaTableWithSimilarNodesHavingDifferentPropertyValueTypes() throws Throwable
    {
        // Given

        // Node1: ({prop1:"Test", prop2: 12, prop3: true})
        // Node2: ({prop1:"Test", prop2: 1.5, prop3: "Test"})
        // Node3: ()

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        transaction.dataWrite().nodeCreate();  // Node3
        long nodeId1 = transaction.dataWrite().nodeCreate();
        long nodeId2 = transaction.dataWrite().nodeCreate();
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int prop2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );
        int prop3 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop3" );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop1, Values.stringValue("Test") );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop1, Values.stringValue("Test") );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop2, Values.intValue( 12 ) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop2, Values.floatValue( 1.5f ) );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop3, Values.booleanValue( true ) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop3, Values.stringValue("Test") );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "propertySchema" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"Node", Arrays.asList(), "prop1", "STRING"} ),
                equalTo( new Object[]{"Node", Arrays.asList(), "prop2", "NUMBER"} ),
                equalTo( new Object[]{"Node", Arrays.asList(), "prop3", "ANY"} ) ) );

        // printStream( stream );
    }

    @Test
    public void testSchemaTableWithRelationships() throws Throwable
    {
        // Given

        // Node1: ()
        // Rel1: (Node1)-[:R{prop1:"Test", prop2:12}]->(Node1)
        // Rel2: (Node1)-[:X{prop1:true}]->(Node1)
        // Rel3: (Node1)-[:Z{}]->(Node1)

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId1 = transaction.dataWrite().nodeCreate();
        int typeR = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
        int typeX = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "X" );
        int typeZ = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "Z" );
        long relId1 = transaction.dataWrite().relationshipCreate( nodeId1,typeR,nodeId1 );
        long relId2 = transaction.dataWrite().relationshipCreate( nodeId1,typeX,nodeId1 );
        transaction.dataWrite().relationshipCreate( nodeId1,typeZ,nodeId1 );  // Rel3
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int prop2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );
        transaction.dataWrite().relationshipSetProperty( relId1, prop1, Values.stringValue("Test") );
        transaction.dataWrite().relationshipSetProperty( relId1, prop2, Values.intValue(12) );
        transaction.dataWrite().relationshipSetProperty( relId2, prop1, Values.booleanValue( true ) );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "propertySchema" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder( equalTo( new Object[]{"Relationship", Arrays.asList( "R" ), "prop1", "STRING"} ),
                equalTo( new Object[]{"Relationship", Arrays.asList( "R" ), "prop2", "INTEGER"} ),
                equalTo( new Object[]{"Relationship", Arrays.asList( "X" ), "prop1", "BOOLEAN"} ),
                equalTo( new Object[]{"Relationship", Arrays.asList( "Z" ), null, null} ),
                equalTo( new Object[]{"Node", Arrays.asList(), null, null} ) ) );

        // printStream( stream );
    }

    @Test
    public void testSchemaTableWithSimilarRelationships() throws Throwable
    {
        // Given

        // Node1: ()
        // Rel1: (node1)-[:R{prop1:"Test"}]->(node1)
        // Rel2: (node1)-[:R{prop1:"Test"}]->(node1)

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId1 = transaction.dataWrite().nodeCreate();
        int typeId = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
        long relId1 = transaction.dataWrite().relationshipCreate( nodeId1, typeId, nodeId1 );
        long relId2 = transaction.dataWrite().relationshipCreate( nodeId1, typeId, nodeId1 );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        transaction.dataWrite().relationshipSetProperty( relId1, prop1, Values.stringValue("Test") );
        transaction.dataWrite().relationshipSetProperty( relId2, prop1, Values.stringValue("Test2") );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "propertySchema" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"Relationship", Arrays.asList( "R" ), "prop1", "STRING"} ),
                equalTo( new Object[]{"Node", Arrays.asList(), null, null} ) ) );

        //printStream( stream );
    }

    @Test
    public void testSchemaTableWithSimilarRelationshipsHavingDifferentPropertyValueTypes() throws Throwable
    {
        // Given

        // Node1: ()
        // Rel1: (node1)-[:R{prop1:"Test", prop2: 12, prop3: true}]->(node1)
        // Rel2: (node1)-[:R{prop1:"Test", prop2: 1.5, prop3: "Test"}]->(node1)
        // Rel3: (node1)-[:R]->(node1)

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId1 = transaction.dataWrite().nodeCreate();
        int typeId = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
        long relId1 = transaction.dataWrite().relationshipCreate( nodeId1, typeId, nodeId1 );
        long relId2 = transaction.dataWrite().relationshipCreate( nodeId1, typeId, nodeId1 );
        transaction.dataWrite().relationshipCreate( nodeId1, typeId, nodeId1 ); // Rel3
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int prop2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );
        int prop3 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop3" );
        transaction.dataWrite().relationshipSetProperty( relId1, prop1, Values.stringValue("Test") );
        transaction.dataWrite().relationshipSetProperty( relId2, prop1, Values.stringValue("Test") );
        transaction.dataWrite().relationshipSetProperty( relId1, prop2, Values.intValue( 12 ) );
        transaction.dataWrite().relationshipSetProperty( relId2, prop2, Values.floatValue( 1.5f ) );
        transaction.dataWrite().relationshipSetProperty( relId1, prop3, Values.booleanValue( true ) );
        transaction.dataWrite().relationshipSetProperty( relId2, prop3, Values.stringValue("Test") );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "propertySchema" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"Node", Arrays.asList(), null, null} ),
                equalTo( new Object[]{"Relationship", Arrays.asList( "R" ), "prop1", "STRING?"} ),
                equalTo( new Object[]{"Relationship", Arrays.asList( "R" ), "prop2", "NUMBER?"} ),
                equalTo( new Object[]{"Relationship", Arrays.asList( "R" ), "prop3", "ANY?"} ) ) );

        //printStream( stream );
    }

    @Test
    public void testSchemaTableWithNullableProperties() throws Throwable
    {
        // Given

        // Node1: (:A{prop1:"Test", prop2: 12, prop3: true})
        // Node2: (:A{prop1:"Test2", prop3: false})
        // Node3: (:A{prop1:"Test3", prop2: 42})
        // Node4: (:B{prop1:"Test4", prop2: 21})
        // Node5: (:B)

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId1 = transaction.dataWrite().nodeCreate();
        long nodeId2 = transaction.dataWrite().nodeCreate();
        long nodeId3 = transaction.dataWrite().nodeCreate();
        long nodeId4 = transaction.dataWrite().nodeCreate();
        long nodeId5 = transaction.dataWrite().nodeCreate();
        int labelA = transaction.tokenWrite().labelGetOrCreateForName( "A" );
        int labelB = transaction.tokenWrite().labelGetOrCreateForName( "B" );
        transaction.dataWrite().nodeAddLabel( nodeId1, labelA );
        transaction.dataWrite().nodeAddLabel( nodeId2, labelA );
        transaction.dataWrite().nodeAddLabel( nodeId3, labelA );
        transaction.dataWrite().nodeAddLabel( nodeId4, labelB );
        transaction.dataWrite().nodeAddLabel( nodeId5, labelB );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int prop2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );
        int prop3 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop3" );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop1, Values.stringValue("Test") );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop1, Values.stringValue("Test2") );
        transaction.dataWrite().nodeSetProperty( nodeId3, prop1, Values.stringValue("Test3") );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop2, Values.intValue( 12 ) );
        transaction.dataWrite().nodeSetProperty( nodeId3, prop2, Values.intValue( 42 ) );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop3, Values.booleanValue( true ) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop3, Values.booleanValue( false ) );

        transaction.dataWrite().nodeSetProperty( nodeId4, prop1, Values.stringValue( "Test4" ) );
        transaction.dataWrite().nodeSetProperty( nodeId4, prop2, Values.intValue( 21 ) );

        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "propertySchema" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"Node", Arrays.asList( "A" ), "prop1", "STRING"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "A" ), "prop2", "INTEGER?"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "A" ), "prop3", "BOOLEAN?"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "B" ), "prop1", "STRING?"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "B" ), "prop2", "INTEGER?"} ) ) );

        //printStream( stream );
    }

    @Test
    public void testSchemaTablesValueStatusUpgradeForNumbers() throws Throwable
    {
        // Given (not valid cypher)

        // Node1: (:A{prop1:(long), prop2: (float), prop3:(long), prop4:(long)})
        // Node2: (:A{prop1:(int),prop2: (double), prop3:(float), prop4:(string)})
        // Node3: (:A{prop1:(byte)})
        // Node4: (:A{prop1:(short)})

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId1 = transaction.dataWrite().nodeCreate();
        long nodeId2 = transaction.dataWrite().nodeCreate();
        long nodeId3 = transaction.dataWrite().nodeCreate();
        long nodeId4 = transaction.dataWrite().nodeCreate();
        int labelA = transaction.tokenWrite().labelGetOrCreateForName( "A" );
        transaction.dataWrite().nodeAddLabel( nodeId1, labelA );
        transaction.dataWrite().nodeAddLabel( nodeId2, labelA );
        transaction.dataWrite().nodeAddLabel( nodeId3, labelA );
        transaction.dataWrite().nodeAddLabel( nodeId4, labelA );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int prop2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );
        int prop3 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop3" );
        int prop4 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop4" );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop1, Values.longValue( 1 ) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop1, Values.intValue( 1 ) );
        transaction.dataWrite().nodeSetProperty( nodeId3, prop1, Values.byteValue( (byte) 1 ) );
        transaction.dataWrite().nodeSetProperty( nodeId4, prop1, Values.shortValue( (short) 1 ) );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop2, Values.floatValue( 2.1f ) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop2, Values.doubleValue( 2.1d ) );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop3, Values.longValue( 1 ) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop3, Values.floatValue( 2.1f ) );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop4, Values.longValue( 1 ) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop4, Values.stringValue( "Text" ) );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "propertySchema" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"Node", Arrays.asList( "A" ), "prop1", "INTEGRAL"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "A" ), "prop2", "FLOATINGPOINT?"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "A" ), "prop3", "NUMBER?"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "A" ), "prop4", "ANY?"} ) ) );

        //printStream( stream );
    }

    @Test
    public void testSchemaTablesValueStatusUpgradeForNumberArrays() throws Throwable
    {
        // Given (not valid cypher)

        // Node1: (:A{prop1:(long array), prop2: (float array)}, prop3:(long array), prop4:(long array)})
        // Node2: (:A{prop1:(int array),prop2: (double array), prop3:(float array), prop4:(string array)})
        // Node3: (:A{prop1:(byte array)})
        // Node4: (:A{prop1:(short array)})

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId1 = transaction.dataWrite().nodeCreate();
        long nodeId2 = transaction.dataWrite().nodeCreate();
        long nodeId3 = transaction.dataWrite().nodeCreate();
        long nodeId4 = transaction.dataWrite().nodeCreate();
        int labelA = transaction.tokenWrite().labelGetOrCreateForName( "A" );
        transaction.dataWrite().nodeAddLabel( nodeId1, labelA );
        transaction.dataWrite().nodeAddLabel( nodeId2, labelA );
        transaction.dataWrite().nodeAddLabel( nodeId3, labelA );
        transaction.dataWrite().nodeAddLabel( nodeId4, labelA );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int prop2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );
        int prop3 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop3" );
        int prop4 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop4" );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop1, Values.longArray( new long[]{1L} ) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop1, Values.intArray( new int[]{1} ) );
        transaction.dataWrite().nodeSetProperty( nodeId3, prop1, Values.byteArray( new byte[]{(byte) 1} ) );
        transaction.dataWrite().nodeSetProperty( nodeId4, prop1, Values.shortArray( new short[]{(short) 1} ) );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop2, Values.floatArray( new float[]{2.1f} ) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop2, Values.doubleArray( new double[]{2.1d} ) );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop3, Values.longArray( new long[]{1L} ) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop3, Values.floatArray( new float[]{2.1f} ) );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop4, Values.longArray( new long[]{1L} ) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop4, Values.stringArray( "Text" ) );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "propertySchema" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"Node", Arrays.asList( "A" ), "prop1", "INTEGRALARRAY"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "A" ), "prop2", "FLOATINGPOINTARRAY?"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "A" ), "prop3", "NUMBER_ARRAY?"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "A" ), "prop4", "ANY?"} ) ) );

        // printStream( stream );
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
        }
    }
}
