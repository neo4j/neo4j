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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.StubResourceManager;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;

public class BuiltInSchemaProceduresIT extends KernelIntegrationTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final ResourceTracker resourceTracker = new StubResourceManager();

    @Test
    public void testSchemaTableWithNodes() throws Throwable
    {
        // Given

        // Node1: (:A:B {prop1:"Test", prop2:12})
        // Node2: (:B {prop1:true})
        // Node3: ()

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId1 = transaction.dataWrite().nodeCreate();
        long nodeId2 = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().nodeCreate(); // Node3
        int labelId1 = transaction.tokenWrite().labelGetOrCreateForName( "A" );
        int labelId2 = transaction.tokenWrite().labelGetOrCreateForName( "B" );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int prop2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop1, Values.stringValue("Test") );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop2, Values.intValue(12) );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop1, Values.booleanValue( true ) );
        transaction.dataWrite().nodeAddLabel( nodeId1, labelId1 );
        transaction.dataWrite().nodeAddLabel( nodeId1, labelId2 );
        transaction.dataWrite().nodeAddLabel( nodeId2, labelId2 );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "schemaAsTable" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"Node", Arrays.asList( "A", "B" ), "prop1", "STRING"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "A", "B" ), "prop2", "INTEGER"} ),
                equalTo( new Object[]{"Node", Arrays.asList( "B" ), "prop1", "BOOLEAN"} ),
                equalTo( new Object[]{"Node", Arrays.asList(), null, null} )) );

        // Just for printing out the result if needed
//        printStream( stream );
    }

    @Test
    public void testSchemaTableWithSimilarNodes() throws Throwable
    {
        // Given

        // Node1: (:A {prop1:"Test"})
        // Node2: (:A {prop1:"Test"})

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId1 = transaction.dataWrite().nodeCreate();
        long nodeId2 = transaction.dataWrite().nodeCreate();
        int labelId1 = transaction.tokenWrite().labelGetOrCreateForName( "A" );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        transaction.dataWrite().nodeSetProperty( nodeId1, prop1, Values.stringValue("Test") );
        transaction.dataWrite().nodeSetProperty( nodeId2, prop1, Values.stringValue("Test") );
        transaction.dataWrite().nodeAddLabel( nodeId1, labelId1 );
        transaction.dataWrite().nodeAddLabel( nodeId2, labelId1 );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "schemaAsTable" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"Node", Arrays.asList("A"), "prop1", "STRING"} )) );

        // Just for printing out the result if needed
        //printStream( stream );
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
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "schemaAsTable" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"Node", Arrays.asList(), "prop1", "STRING"} ),
                equalTo( new Object[]{"Node", Arrays.asList(), "prop2", "NUMBER"} ),
                equalTo( new Object[]{"Node", Arrays.asList(), "prop3", "ANYVALUE"} ) ) );

        // Just for printing out the result if needed
        //printStream( stream );
    }

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
