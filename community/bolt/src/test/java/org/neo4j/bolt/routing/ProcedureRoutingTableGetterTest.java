/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.routing;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ProcedureRoutingTableGetterTest
{
    private final ProcedureRoutingTableGetter getter;

    ProcedureRoutingTableGetterTest()
    {
        getter = new ProcedureRoutingTableGetter();
    }

    @Test
    void shouldRunTheStateWithTheCorrectParams() throws Exception
    {
        var statementProcessor = mock( StatementProcessor.class );
        var routingTableContext = getRoutingTableContext();
        var databaseName = "dbName";

        doReturn( mock( StatementMetadata.class ) ).when( statementProcessor ).run( anyString(), any(), any(), any(), any(), any() );

        getter.get( statementProcessor, routingTableContext, List.of(), databaseName );

        verify( statementProcessor )
                .run( "CALL dbms.routing.getRoutingTable($routingContext, $databaseName)",
                      getExpectedParams( routingTableContext, databaseName ), List.of(), null, AccessMode.READ, Map.of() );
    }

    @Test
    void shouldCompleteWithOneRecordFromTheResultQuery() throws Throwable
    {
        var statementProcessor = mock( StatementProcessor.class );
        var queryId = 123;
        var statementMetadata = mock( StatementMetadata.class );
        var boltResult = mock( BoltResult.class );
        var expectedRoutingTable = routingTable();

        doReturn( statementMetadata ).when( statementProcessor ).run( anyString(), any(), any(), any(), any(), any() );
        doReturn( queryId ).when( statementMetadata ).queryId();
        doReturn( getFieldNames() ).when( boltResult ).fieldNames();
        mockStreamResult( statementProcessor, queryId, boltResult, recordConsumer ->
        {
            recordConsumer.beginRecord( 2 );
            recordConsumer.consumeField( expectedRoutingTable.get( "ttl" ) );
            recordConsumer.consumeField( expectedRoutingTable.get( "servers" ) );
            recordConsumer.endRecord();
        } );

        var future = getter.get( statementProcessor, getRoutingTableContext(), List.of(), "dbName" );

        var routingTable = future.get( 100, TimeUnit.MILLISECONDS );

        assertEquals( expectedRoutingTable, routingTable );
    }

    @Test
    void shouldCompleteFailureIfSomeErrorOccurDuringRecordConsumer() throws Throwable
    {
        var statementProcessor = mock( StatementProcessor.class );
        var queryId = 123;
        var statementMetadata = mock( StatementMetadata.class );
        var boltResult = mock( BoltResult.class );

        doReturn( statementMetadata ).when( statementProcessor ).run( anyString(), any(), any(), any(), any(), any() );
        doReturn( queryId ).when( statementMetadata ).queryId();
        doReturn( getFieldNames() ).when( boltResult ).fieldNames();
        mockStreamResult( statementProcessor, queryId, boltResult, recordConsumer ->
        {
            recordConsumer.beginRecord( 2 );
            recordConsumer.onError();
        } );

        var future = getter.get( statementProcessor, getRoutingTableContext(), List.of(), "dbName" );

        try
        {
            future.join();
            fail( "The method should throws an exception" );
        }
        catch ( CompletionException exception )
        {
            assertEquals( RuntimeException.class, exception.getCause().getClass() );
        }
    }

    @Test
    void shouldCompleteWithFailureIfTheRunMethodThrowsException() throws Throwable
    {
        var statementProcessor = mock( StatementProcessor.class );
        var expectedException = new TransactionFailureException( "Something wrong", new RuntimeException() );

        doThrow( expectedException ).when( statementProcessor ).run( anyString(), any(), any(), any(), any(), any() );

        var future = getter.get( statementProcessor, getRoutingTableContext(), List.of(), "dbName" );

        try
        {
            future.join();
            fail( "The method should throws an exception" );
        }
        catch ( CompletionException exception )
        {
            assertEquals( expectedException, exception.getCause() );
        }
    }

    @Test
    void shouldCompleteWithFailureIfTheStreamResultMethodThrowsException() throws Throwable
    {
        var statementProcessor = mock( StatementProcessor.class );
        var queryId = 123;
        var statementMetadata = mock( StatementMetadata.class );
        var expectedException = new TransactionFailureException( "Something wrong", new RuntimeException() );

        doReturn( statementMetadata ).when( statementProcessor ).run( anyString(), any(), any(), any(), any(), any() );
        doReturn( queryId ).when( statementMetadata ).queryId();
        doThrow( expectedException ).when( statementProcessor ).streamResult( anyInt(), any() );
        var future = getter.get( statementProcessor, getRoutingTableContext(), List.of(), "dbName" );

        try
        {
            future.join();
            fail( "The method should throws an exception" );
        }
        catch ( CompletionException exception )
        {
            assertEquals( expectedException, exception.getCause() );
        }
    }

    private static void mockStreamResult( StatementProcessor statementProcessor, int queryId, BoltResult boltResult,
            UnsafeConsumer<BoltResult.RecordConsumer> answer ) throws Throwable
    {
        doAnswer( invocationOnMock ->
                  {
                      var consumer = invocationOnMock.getArgument( 0, BoltResult.RecordConsumer.class );
                      answer.accept( consumer );
                      return true;
                  } )
                .when( boltResult )
                .handleRecords( any(), eq( 1L ) );

        doAnswer( invocationOnMock ->
                  {
                      var resultHandler = invocationOnMock.getArgument( 1, ResultConsumer.class );
                      resultHandler.consume( boltResult );
                      return mock( Bookmark.class );
                  } )
                .when( statementProcessor )
                .streamResult( eq( queryId ), any() );
    }

    private static MapValue routingTable()
    {
        var builder = new MapValueBuilder();
        builder.add( "ttl", Values.intValue( 300 ) );
        var serversBuilder = ListValueBuilder.newListBuilder();
        builder.add( "servers", serversBuilder.build() );
        return builder.build();
    }

    private static String[] getFieldNames()
    {
        return new String[]{"ttl", "servers"};
    }

    private static MapValue getExpectedParams( MapValue routingContext, String databaseName )
    {
        var builder = new MapValueBuilder();
        builder.add( "routingContext", routingContext );
        builder.add( "databaseName", Values.stringOrNoValue( databaseName ) );
        return builder.build();
    }

    private static MapValue getRoutingTableContext()
    {
        var builder = new MapValueBuilder();
        builder.add( "context", Values.stringOrNoValue( "give me some context" ) );
        return builder.build();
    }

    interface UnsafeConsumer<T>
    {
        void accept( T value ) throws Throwable;
    }
}
