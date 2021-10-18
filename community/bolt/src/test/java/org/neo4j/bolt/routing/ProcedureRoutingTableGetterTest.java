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
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.bolt.transaction.DefaultProgramResultReference;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
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
        var loginContext = LoginContext.AUTH_DISABLED;
        var transactionManager = mock( TransactionManager.class );
        var routingTableContext = getRoutingTableContext();
        var databaseName = "dbName";
        var metadata = mock( StatementMetadata.class );
        var programResult = new DefaultProgramResultReference( "123", metadata );

        doReturn( programResult ).when( transactionManager ).runProgram( anyString(), any(), anyString(), anyString(), any(), any(),
                                                                         anyBoolean(), any(), any(), any() );

        getter.get( "123", loginContext, transactionManager, routingTableContext, List.of(), databaseName, "123" );

        verify( transactionManager )
                .runProgram( anyString(), eq( loginContext ), eq( "system" ), eq( "CALL dbms.routing.getRoutingTable($routingContext, $databaseName)" ),
                             eq( getExpectedParams( routingTableContext, databaseName ) ),
                             eq( emptyList() ), eq( true ), eq( Map.of() ), eq( null ), eq( "123" ) );
    }

    @Test
    void shouldCompleteWithOneRecordFromTheResultQuery() throws Throwable
    {
        var loginContext = LoginContext.AUTH_DISABLED;
        var transactionManager = mock( TransactionManager.class );
        var queryId = 123;
        var statementMetadata = mock( StatementMetadata.class );
        var programResult = new DefaultProgramResultReference( "123", statementMetadata );
        var boltResult = mock( BoltResult.class );
        var expectedRoutingTable = routingTable();

        doReturn( programResult ).when( transactionManager )
                                 .runProgram( anyString(), any(), anyString(), anyString(), any( MapValue.class ), anyList(), anyBoolean(), anyMap(), any(),
                                              anyString() );
        doReturn( queryId ).when( statementMetadata ).queryId();
        doReturn( getFieldNames() ).when( boltResult ).fieldNames();
        mockStreamResult( transactionManager, boltResult, recordConsumer ->
        {
            recordConsumer.beginRecord( 2 );
            recordConsumer.consumeField( expectedRoutingTable.get( "ttl" ) );
            recordConsumer.consumeField( expectedRoutingTable.get( "servers" ) );
            recordConsumer.endRecord();
        } );

        var future = getter.get( "123", loginContext, transactionManager, getRoutingTableContext(), List.of(), "dbName", "123" );

        var routingTable = future.get( 100, TimeUnit.MILLISECONDS );

        assertEquals( expectedRoutingTable, routingTable );
    }

    @Test
    void shouldCompleteFailureIfSomeErrorOccurDuringRecordConsumer() throws Throwable
    {
        var loginContext = LoginContext.AUTH_DISABLED;
        var transactionManager = mock( TransactionManager.class );
        var queryId = 123;
        var statementMetadata = mock( StatementMetadata.class );
        var programResult = new DefaultProgramResultReference( "123", statementMetadata );
        var boltResult = mock( BoltResult.class );

        doReturn( programResult ).when( transactionManager )
                                 .runProgram( anyString(), any(), anyString(), anyString(), any(), any(), anyBoolean(), any(), any(), any() );
        doReturn( queryId ).when( statementMetadata ).queryId();
        doReturn( getFieldNames() ).when( boltResult ).fieldNames();
        mockStreamResult( transactionManager, boltResult, recordConsumer ->
        {
            recordConsumer.beginRecord( 2 );
            recordConsumer.onError();
        } );

        var future = getter.get( "123", loginContext, transactionManager, getRoutingTableContext(), List.of(), "dbName", "123" );

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
        var loginContext = LoginContext.AUTH_DISABLED;
        var transactionManager = mock( TransactionManager.class );
        var expectedException = new TransactionFailureException( "Something wrong", new RuntimeException() );

        doThrow( expectedException ).when( transactionManager ).runProgram( anyString(), any(), anyString(), anyString(), any(),
                                                                            any(), anyBoolean(), any(), any(), any() );

        var future = getter.get( "123", loginContext, transactionManager, getRoutingTableContext(), List.of(), "dbName", "123" );

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
        var loginContext = LoginContext.AUTH_DISABLED;
        var transactionManager = mock( TransactionManager.class );
        var queryId = 123;
        var statementMetadata = mock( StatementMetadata.class );
        var programResult = new DefaultProgramResultReference( "123", statementMetadata );
        var expectedException = new RuntimeException( new TransactionFailureException( "Something wrong", new RuntimeException() ) );

        doReturn( programResult ).when( transactionManager ).runProgram( anyString(), any(), anyString(), anyString(), any(),
                                                                         any(), anyBoolean(), any(), any(), any() );
        doReturn( queryId ).when( statementMetadata ).queryId();
        doThrow( expectedException ).when( transactionManager ).pullData( anyString(), anyInt(), anyLong(), any( ResultConsumer.class ) );

        var future = getter.get( "123", loginContext, transactionManager, getRoutingTableContext(), List.of(), "dbName", "123" );

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

    private void mockStreamResult( TransactionManager transactionManager, BoltResult boltResult, UnsafeConsumer<BoltResult.RecordConsumer> answer )
            throws Throwable
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
                      var resultHandler = invocationOnMock.getArgument( 3, ResultConsumer.class );
                      resultHandler.consume( boltResult );
                      return mock( Bookmark.class );
                  } )
                .when( transactionManager )
                .pullData( any( String.class ), any( Integer.class ), any( Long.class ), any( ResultConsumer.class ) );
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
