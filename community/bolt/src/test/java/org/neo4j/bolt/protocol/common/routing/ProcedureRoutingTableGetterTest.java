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
package org.neo4j.bolt.protocol.common.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.testing.mock.StatementMockFactory;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.error.statement.StatementExecutionException;
import org.neo4j.bolt.tx.error.statement.StatementStreamingException;
import org.neo4j.bolt.tx.statement.Statement;
import org.neo4j.kernel.api.exceptions.Status.Request;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

class ProcedureRoutingTableGetterTest {
    private final ProcedureRoutingTableGetter getter;

    ProcedureRoutingTableGetterTest() {
        getter = new ProcedureRoutingTableGetter();
    }

    @Test
    void shouldRunTheStateWithTheCorrectParams() throws Exception {
        var transaction = mock(Transaction.class);
        var routingTableContext = getRoutingTableContext();
        var databaseName = "dbName";
        var statement = mock(Statement.class);

        doReturn(statement).when(transaction).run(anyString(), any());

        getter.get(transaction, routingTableContext, databaseName);

        verify(transaction)
                .run(
                        eq("CALL dbms.routing.getRoutingTable($routingContext, $databaseName)"),
                        eq(getExpectedParams(routingTableContext, databaseName)));
    }

    @Test
    void shouldCompleteWithOneRecordFromTheResultQuery() throws Throwable {
        var transaction = mock(Transaction.class);
        var expectedRoutingTable = routingTable();
        var statement = StatementMockFactory.newInstance(stmt -> stmt.withResults(expectedRoutingTable));

        doReturn(statement).when(transaction).run(anyString(), any(MapValue.class));

        var future = getter.get(transaction, getRoutingTableContext(), "dbName");

        var routingTable = future.get(100, TimeUnit.MILLISECONDS);

        assertEquals(expectedRoutingTable, routingTable);
    }

    @Test
    void shouldCompleteFailureIfSomeErrorOccurDuringRecordConsumer() throws Throwable {
        var transaction = Mockito.mock(Transaction.class);
        var statement = StatementMockFactory.newInstance(stmt -> stmt.withFieldNames(getFieldNames()));

        doReturn(statement).when(transaction).run(anyString(), any());

        doAnswer(invocation -> {
                    var responseHandler = invocation.<ResponseHandler>getArgument(0);

                    var recordHandler = responseHandler.onBeginStreaming(List.of("ttl", "servers"));
                    recordHandler.onBegin();
                    recordHandler.onFailure();

                    responseHandler.onFailure(Error.from(Request.Invalid, "Something went wrong"));
                    return null;
                })
                .when(statement)
                .consume(any(), anyLong());

        var future = getter.get(transaction, getRoutingTableContext(), "dbName");

        try {
            future.join();
            fail("The method should throw an exception");
        } catch (CompletionException exception) {
            assertEquals(RuntimeException.class, exception.getCause().getClass());
        }
    }

    @Test
    void shouldCompleteWithFailureIfTheRunMethodThrowsException() throws Throwable {
        var transaction = Mockito.mock(Transaction.class);
        var expectedException = new StatementExecutionException("Something went horribly wrong!");

        doThrow(expectedException).when(transaction).run(anyString(), any());

        var future = getter.get(transaction, getRoutingTableContext(), "dbName");

        try {
            future.join();
            fail("The method should throws an exception");
        } catch (CompletionException exception) {
            assertEquals(expectedException, exception.getCause());
        }
    }

    @Test
    void shouldCompleteWithFailureIfTheStreamResultMethodThrowsException() throws Throwable {
        var transaction = Mockito.mock(Transaction.class);
        var statement = StatementMockFactory.newInstance();

        var expectedException = new StatementStreamingException("Something went horribly wrong!");

        Mockito.doReturn(statement).when(transaction).run(anyString(), any());
        Mockito.doThrow(expectedException).when(statement).consume(any(), anyLong());

        var future = getter.get(transaction, getRoutingTableContext(), "dbName");

        try {
            future.join();
            fail("The method should throws an exception");
        } catch (CompletionException exception) {
            assertEquals(expectedException, exception.getCause());
        }
    }

    private static MapValue routingTable() {
        var builder = new MapValueBuilder();
        builder.add("ttl", Values.intValue(300));
        var serversBuilder = ListValueBuilder.newListBuilder();
        builder.add("servers", serversBuilder.build());
        return builder.build();
    }

    private static String[] getFieldNames() {
        return new String[] {"ttl", "servers"};
    }

    private static MapValue getExpectedParams(MapValue routingContext, String databaseName) {
        var builder = new MapValueBuilder();
        builder.add("routingContext", routingContext);
        builder.add("databaseName", Values.stringOrNoValue(databaseName));
        return builder.build();
    }

    private static MapValue getRoutingTableContext() {
        var builder = new MapValueBuilder();
        builder.add("context", Values.stringOrNoValue("give me some context"));
        return builder.build();
    }
}
