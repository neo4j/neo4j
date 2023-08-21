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
package org.neo4j.server.http.cypher;

import static java.lang.Long.parseLong;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.tx.TransactionOwner;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.testing.mock.MockResult;
import org.neo4j.bolt.testing.mock.StatementMockFactory;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.error.statement.StatementExecutionException;
import org.neo4j.exceptions.SyntaxException;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.Status.General;
import org.neo4j.kernel.api.exceptions.Status.HasStatus;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.NotificationConfiguration;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.http.cypher.format.api.InputEventStream;
import org.neo4j.server.http.cypher.format.api.InputFormatException;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;
import org.neo4j.server.http.cypher.format.api.TransactionUriScheme;
import org.neo4j.values.virtual.MapValue;

class InvocationTest {
    private static final Statement NULL_STATEMENT = null;
    private static final String TX_ID = "123";

    private static final String SERIALIZED_BOOKMARK = "BOOKMARK!";

    private final InternalLog log = mock(InternalLog.class);
    private final TransactionManager transactionManager = mock(TransactionManager.class);
    private final Transaction transaction = mock(Transaction.class);
    private org.neo4j.bolt.tx.statement.Statement statement;
    private final InternalLogProvider logProvider = mock(InternalLogProvider.class);
    private final InternalTransaction internalTransaction = mock(InternalTransaction.class);
    private final TransactionRegistry registry = mock(TransactionRegistry.class);
    private final OutputEventStream outputEventStream = mock(OutputEventStream.class);
    private final MemoryTracker memoryTracker = mock(MemoryTracker.class);
    private final AuthManager authManager = mock(AuthManager.class);
    private final String[] DEFAULT_FIELD_NAMES = new String[] {"c1", "c2", "c3"};

    @BeforeEach
    void setUp() throws Exception {
        this.statement = generateStatementMock();

        when(this.transaction.id()).thenReturn("123");

        when(transactionManager.create(
                        any(TransactionType.class),
                        any(TransactionOwner.class),
                        anyString(),
                        any(AccessMode.class),
                        anyList(),
                        nullable(Duration.class),
                        anyMap(),
                        nullable(NotificationConfiguration.class)))
                .thenReturn(transaction);

        when(transaction.run(anyString(), Mockito.eq(MapValue.EMPTY))).thenReturn(statement);
        when(transaction.commit()).thenReturn(SERIALIZED_BOOKMARK);
    }

    @Test
    void shouldExecuteStatements() throws Throwable {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(parseLong(TX_ID)),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder(transactionManager, transaction, this.statement, registry);
        txManagerOrder.verify(registry).begin(handle);
        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run("query", MapValue.EMPTY);
        txManagerOrder.verify(this.statement).consume(any(ResponseHandler.class), anyLong());
        txManagerOrder.verify(transaction).commit();
        txManagerOrder.verify(registry).forget(123L);
        txManagerOrder.verify(transaction).close();
        verifyNoMoreInteractions(transactionManager);

        // then verify output
        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeStatementStart(statement, List.of("c1", "c2", "c3"));
        verifyDefaultResultRows(outputOrder);
        outputOrder
                .verify(outputEventStream)
                .writeStatementEnd(any(), any(), any(), any()); // todo work out why the actual args fails
        //        outputOrder.verify( outputEventStream ).writeStatementEnd( query( QueryExecutionType.QueryType.WRITE
        // ), QueryStatistics.EMPTY,
        //                                                                   HttpExecutionPlanDescription.EMPTY,
        // emptyList() );
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(
                        TransactionNotificationState.COMMITTED, uriScheme.txCommitUri(123L), -1, SERIALIZED_BOOKMARK);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldSuspendTransactionAndReleaseForOtherRequestsAfterExecutingStatements() throws Throwable {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                false);

        // when
        invocation.execute(outputEventStream);

        // then
        InOrder transactionOrder = inOrder(registry);
        transactionOrder.verify(registry).release(123L, handle);

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder(transactionManager, transaction, this.statement);
        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run("query", MapValue.EMPTY);
        txManagerOrder.verify(this.statement).consume(any(ResponseHandler.class), anyLong());
        txManagerOrder.verifyNoMoreInteractions();

        // then verify output
        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeStatementStart(statement, List.of("c1", "c2", "c3"));
        verifyDefaultResultRows(outputOrder);
        outputOrder
                .verify(outputEventStream)
                .writeStatementEnd(any(), any(), any(), any()); // todo work out why the actual args fails
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(TransactionNotificationState.OPEN, uriScheme.txCommitUri(123L), 0, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldResumeTransactionWhenExecutingStatementsOnSecondRequest() throws Throwable {
        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);
        var statement1 = generateStatementMock();
        var statement2 = generateStatementMock();
        when(transaction.run(eq("queryA"), Mockito.eq(MapValue.EMPTY))).thenReturn(statement1);
        when(transaction.run(eq("queryB"), Mockito.eq(MapValue.EMPTY))).thenReturn(statement2);
        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statementA = new Statement("queryA", map());
        Statement statementB = new Statement("queryB", map());
        when(inputEventStream.read()).thenReturn(statementA, NULL_STATEMENT);

        // given initial invocation
        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                false);

        InOrder txManagerOrder = inOrder(transactionManager, transaction, statement1, statement2, registry);

        invocation.execute(outputEventStream);

        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run("queryA", MapValue.EMPTY);
        txManagerOrder.verify(statement1).consume(any(ResponseHandler.class), anyLong());

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeStatementStart(statementA, List.of("c1", "c2", "c3"));
        verifyDefaultResultRows(outputOrder);

        outputOrder
                .verify(outputEventStream)
                .writeStatementEnd(any(), any(), any(), any()); // todo work out why the actual args fails
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(TransactionNotificationState.OPEN, uriScheme.txCommitUri(123L), 0, null);

        txManagerOrder.verify(registry).release(123L, handle);

        when(inputEventStream.read()).thenReturn(statementB, NULL_STATEMENT);

        // when an additional invocation is triggered it should resume the initial transaction
        invocation.execute(outputEventStream);

        // then verify transactionManager interaction
        txManagerOrder.verify(transaction).run("queryB", MapValue.EMPTY);
        txManagerOrder.verify(statement2).consume(any(ResponseHandler.class), anyLong());

        // verify output
        outputOrder.verify(outputEventStream).writeStatementStart(statementB, List.of("c1", "c2", "c3"));
        verifyDefaultResultRows(outputOrder);

        outputOrder
                .verify(outputEventStream)
                .writeStatementEnd(any(), any(), any(), any()); // todo work out why the actual args fails
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(TransactionNotificationState.OPEN, uriScheme.txCommitUri(123L), 0, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldCommitTransactionAndTellRegistryToForgetItsHandle() throws Throwable {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then
        InOrder transactionOrder = inOrder(internalTransaction, registry);
        transactionOrder.verify(registry).forget(123L);

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder(transactionManager, transaction, this.statement);
        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run("query", MapValue.EMPTY);
        txManagerOrder.verify(transaction).commit();
        txManagerOrder.verify(transaction).close();
        txManagerOrder.verifyNoMoreInteractions();

        // then verify output
        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeStatementStart(statement, List.of("c1", "c2", "c3"));
        verifyDefaultResultRows(outputOrder);
        outputOrder
                .verify(outputEventStream)
                .writeStatementEnd(any(), any(), any(), any()); // todo work out why the actual args fails
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(
                        TransactionNotificationState.COMMITTED, uriScheme.txCommitUri(123L), -1, SERIALIZED_BOOKMARK);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldRollbackTransactionAndTellRegistryToForgetItsHandle() throws Exception {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        RollbackInvocation invocation = new RollbackInvocation(log, handle);

        // when
        invocation.execute(outputEventStream);

        // then
        InOrder transactionOrder = inOrder(transaction, registry);
        transactionOrder.verify(transaction).rollback();
        transactionOrder.verify(registry).forget(123L);
        transactionOrder.verify(transaction).close();

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(TransactionNotificationState.ROLLED_BACK, null, -1, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldCreateTransactionContextOnlyWhenFirstNeeded() throws Throwable {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        // when
        TransactionHandle handle = getTransactionHandle(registry);
        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder(transactionManager, transaction, this.statement);
        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run("query", MapValue.EMPTY);
        txManagerOrder.verify(this.statement).consume(any(ResponseHandler.class), anyLong());
        txManagerOrder.verify(transaction).commit();
        txManagerOrder.verify(transaction).close();
        txManagerOrder.verifyNoMoreInteractions();

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeStatementStart(statement, List.of("c1", "c2", "c3"));
        verifyDefaultResultRows(outputOrder);
        outputOrder
                .verify(outputEventStream)
                .writeStatementEnd(any(), any(), any(), any()); // todo work out why the actual args fails
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(
                        TransactionNotificationState.COMMITTED, uriScheme.txCommitUri(123L), -1, SERIALIZED_BOOKMARK);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldRollbackTransactionIfExecutionErrorOccurs() throws Exception {
        // given
        when(transaction.run("query", MapValue.EMPTY)).thenThrow(new MockStatementExecutionException());

        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder(transactionManager, transaction, this.statement);
        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run("query", MapValue.EMPTY);
        txManagerOrder.verify(transaction).rollback();
        txManagerOrder.verify(transaction).close();
        txManagerOrder.verifyNoMoreInteractions();

        verify(registry).forget(123L);

        InOrder outputOrder = inOrder(outputEventStream);

        outputOrder.verify(outputEventStream).writeFailure(Status.Statement.ExecutionFailed, "Something went wrong");
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri(123L), -1, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldHandleCommitError() throws Throwable {
        // given
        var commitError = mock(TransactionException.class);
        when(commitError.getMessage()).thenReturn("Something went wrong!");
        when(transaction.commit()).thenThrow(commitError);

        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then
        verify(log).error(eq("Failed to commit transaction."), any(TransactionException.class));
        verify(registry).forget(123L);

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder(transactionManager, transaction, this.statement);
        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run("query", MapValue.EMPTY);
        txManagerOrder.verify(this.statement).consume(any(ResponseHandler.class), anyLong());
        txManagerOrder.verify(transaction).commit();
        txManagerOrder.verify(transaction).close();
        verifyNoMoreInteractions(transactionManager);

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeStatementStart(statement, List.of("c1", "c2", "c3"));
        verifyDefaultResultRows(outputOrder);
        outputOrder
                .verify(outputEventStream)
                .writeStatementEnd(any(), any(), any(), any()); // todo work out why the actual args fails
        outputOrder.verify(outputEventStream).writeFailure(any(), any()); // todo check error properly here
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(TransactionNotificationState.UNKNOWN, uriScheme.txCommitUri(123L), -1, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldHandleErrorWhenStartingTransaction() throws Throwable {
        // given
        when(transactionManager.create(
                        any(TransactionType.class),
                        any(TransactionOwner.class),
                        anyString(),
                        any(AccessMode.class),
                        anyList(),
                        nullable(Duration.class),
                        anyMap(),
                        nullable(NotificationConfiguration.class)))
                .thenThrow(mock(TransactionException.class));

        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then
        verify(log).error(eq("Failed to start transaction"), any(TransactionException.class));

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeFailure(any(), any()); // todo more specific
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(
                        TransactionNotificationState.NO_TRANSACTION, uriScheme.txCommitUri(123L), -1, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldHandleAuthorizationErrorWhenStartingTransaction() throws Throwable {
        // given
        when(transactionManager.create(
                        any(TransactionType.class),
                        any(TransactionOwner.class),
                        anyString(),
                        any(AccessMode.class),
                        anyList(),
                        nullable(Duration.class),
                        anyMap(),
                        nullable(NotificationConfiguration.class)))
                .thenThrow(new AuthorizationViolationException("Forbidden"));

        when(registry.begin(any(TransactionHandle.class))).thenReturn(1337L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(1337L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then
        //        verifyNoMoreInteractions(log);

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeFailure(Status.Security.Forbidden, "Forbidden");
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(
                        TransactionNotificationState.NO_TRANSACTION, uriScheme.txCommitUri(1337L), -1, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldHandleCypherSyntaxError() throws Exception {
        // given
        String queryText = "matsch (n) return n";
        when(transaction.run(queryText, MapValue.EMPTY))
                .thenThrow(new RuntimeException(new SyntaxException("did you mean MATCH?")));

        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement(queryText, map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then
        verify(registry).forget(123L);

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder(transactionManager, transaction, this.statement);
        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run(queryText, MapValue.EMPTY);
        txManagerOrder.verify(transaction).rollback();
        txManagerOrder.verify(transaction).close();
        txManagerOrder.verifyNoMoreInteractions();

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeFailure(Status.Statement.SyntaxError, "did you mean MATCH?");
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri(123L), -1, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldHandleExecutionEngineThrowingUndeclaredCheckedExceptions() throws Exception {
        // given
        when(transaction.run("query", MapValue.EMPTY)).thenThrow(new RuntimeException("BOO"));

        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then
        verify(registry).forget(123L);

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder(transactionManager, transaction, this.statement);
        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run("query", MapValue.EMPTY);
        txManagerOrder.verify(transaction).rollback();
        txManagerOrder.verify(transaction).close();
        txManagerOrder.verifyNoMoreInteractions();

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeFailure(Status.Statement.ExecutionFailed, "BOO");
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri(123L), -1, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldHandleRollbackError() throws Exception {
        // given
        doThrow(new RuntimeException("BOO")).when(transaction).run("query", MapValue.EMPTY);
        doThrow(new IllegalStateException("Something went wrong"))
                .when(transaction)
                .rollback();

        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        verify(log).error(eq("Failed to roll back transaction."), any(IllegalStateException.class));
        verify(registry).forget(123L);

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder(transactionManager, transaction);
        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run("query", MapValue.EMPTY);
        txManagerOrder.verify(transaction).rollback();
        txManagerOrder.verify(transaction).close();
        txManagerOrder.verifyNoMoreInteractions();

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeFailure(Status.Statement.ExecutionFailed, "BOO");
        outputOrder
                .verify(outputEventStream)
                .writeFailure(Status.Transaction.TransactionRollbackFailed, "Something went wrong");
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(TransactionNotificationState.UNKNOWN, uriScheme.txCommitUri(123L), -1, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldInterruptTransaction() throws Throwable {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        invocation.execute(outputEventStream);

        // when
        handle.terminate();

        // then
        verify(transaction).interrupt();
    }

    @Test
    void deadlockExceptionHasCorrectStatus() throws Throwable {
        // given
        when(transaction.run("query", MapValue.EMPTY)).thenThrow(new DeadlockDetectedException("deadlock"));

        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then
        verify(registry).forget(123L);

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder(transactionManager, transaction);
        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run("query", MapValue.EMPTY);
        txManagerOrder.verify(transaction).rollback();
        txManagerOrder.verify(transaction).close();
        txManagerOrder.verifyNoMoreInteractions();

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeFailure(Status.Transaction.DeadlockDetected, "deadlock");
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri(123L), -1, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void startTransactionWithRequestedTimeout() throws Exception {
        // given
        TransactionManager txManager = mock(TransactionManager.class);
        TransactionHandle handle = new TransactionHandle(
                "neo4j",
                mock(TransactionRegistry.class),
                uriScheme,
                true,
                AUTH_DISABLED,
                mock(ClientConnectionInfo.class),
                100,
                txManager,
                mock(InternalLogProvider.class),
                mock(MemoryTracker.class),
                mock(AuthManager.class),
                true,
                emptyList());

        InputEventStream inputEventStream = mock(InputEventStream.class);
        when(inputEventStream.read()).thenReturn(null);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then
        verify(txManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.READ,
                        emptyList(),
                        Duration.ofMillis(100),
                        Collections.emptyMap(),
                        null);
    }

    @Test
    void shouldHandleInputParsingErrorWhenReadingStatements() throws Exception {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        when(inputEventStream.read())
                .thenThrow(new InputFormatException("Cannot parse input", new IOException("JSON ERROR")));

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then
        verify(transaction).rollback();
        verify(transaction).close();
        verify(registry).forget(123L);

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeFailure(Status.Request.InvalidFormat, "Cannot parse input");
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri(123L), -1, null);
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldHandleConnectionErrorWhenReadingStatementsInImplicitTransaction() throws Exception {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        when(inputEventStream.read())
                .thenThrow(new ConnectionException("Connection error", new IOException("Broken pipe")));

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(123L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        var e = assertThrows(RuntimeException.class, () -> invocation.execute(outputEventStream));
        assertTrue(e.getMessage().contains("Connection error"));

        // then
        verify(transaction).rollback();
        verify(transaction).close();
        verify(registry).forget(123L);

        verifyNoInteractions(outputEventStream);
    }

    @Test
    void shouldKeepTransactionOpenIfConnectionErrorWhenReadingStatementsInExplicitTransaction() throws Exception {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(1337L);
        TransactionHandle handle = getTransactionHandle(registry, false, true);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        when(inputEventStream.read())
                .thenThrow(new ConnectionException("Connection error", new IOException("Broken pipe")));

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(1337L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        var e = assertThrows(RuntimeException.class, () -> invocation.execute(outputEventStream));
        assertTrue(e.getMessage().contains("Connection error"));

        // then
        verify(transaction, never()).rollback();
        verify(transaction, never()).commit();
        verify(transaction, never()).close();
        verify(registry, never()).forget(1337L);

        verifyNoInteractions(outputEventStream);
    }

    @Test
    void shouldHandleConnectionErrorWhenWritingOutputInImplicitTransaction() throws Throwable {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(1337L);
        TransactionHandle handle = getTransactionHandle(registry);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(1337L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        doThrow(new ConnectionException("Connection error", new IOException("Broken pipe")))
                .when(outputEventStream)
                .writeStatementEnd(
                        any(), any(),
                        any(), any());

        // when
        var e = assertThrows(RuntimeException.class, () -> invocation.execute(outputEventStream));
        assertTrue(e.getMessage().contains("Connection error"));

        // then
        verify(transaction).rollback();
        verify(transaction).close();
        verify(registry).forget(1337L);

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeStatementStart(statement, List.of("c1", "c2", "c3"));
        verifyDefaultResultRows(outputOrder);
        outputOrder
                .verify(outputEventStream)
                .writeStatementEnd(any(), any(), any(), any()); // todo work out why the actual args fails
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldKeepTransactionOpenIfConnectionErrorWhenWritingOutputInImplicitTransaction() throws Throwable {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(1337L);
        TransactionHandle handle = getTransactionHandle(registry, false, true);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(1337L),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                false);

        doThrow(new ConnectionException("Connection error", new IOException("Broken pipe")))
                .when(outputEventStream)
                .writeStatementEnd(
                        any(), any(),
                        any(), any());

        // when
        var e = assertThrows(RuntimeException.class, () -> invocation.execute(outputEventStream));
        assertTrue(e.getMessage().contains("Connection error"));

        // then
        verify(transaction, never()).rollback();
        verify(transaction, never()).close();
        verify(registry, never()).forget(1337L);

        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeStatementStart(statement, List.of("c1", "c2", "c3"));
        verifyDefaultResultRows(outputOrder);
        outputOrder
                .verify(outputEventStream)
                .writeStatementEnd(any(), any(), any(), any()); // todo work out why the actual args fails
        verifyNoMoreInteractions(outputEventStream);
    }

    @Test
    void shouldAllocateAndFreeMemory() throws Throwable {
        var handle = getTransactionHandle(registry, false, true);
        var memoryPool = mock(MemoryPool.class);
        var inputEventStream = mock(InputEventStream.class);

        when(registry.begin(any(TransactionHandle.class))).thenReturn(1337L);
        when(inputEventStream.read()).thenReturn(new Statement("query", map()), NULL_STATEMENT);

        var invocation = new Invocation(
                mock(InternalLog.class), handle, uriScheme.txCommitUri(1337L), memoryPool, inputEventStream, true);

        invocation.execute(outputEventStream);

        verify(memoryPool, times(2)).reserveHeap(Statement.SHALLOW_SIZE);
        verify(memoryPool, times(2)).releaseHeap(Statement.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryPool);
    }

    @Test
    void shouldFreeMemoryOnException() throws Throwable {
        var handle = getTransactionHandle(registry, false, true);
        var memoryPool = mock(MemoryPool.class);
        var inputEventStream = mock(InputEventStream.class);

        when(registry.begin(any(TransactionHandle.class))).thenReturn(1337L);
        when(inputEventStream.read()).thenReturn(new Statement("query", map()), NULL_STATEMENT);

        doThrow(new ConnectionException("Something broke", new IOException("Oh no")))
                .when(outputEventStream)
                .writeStatementEnd(any(), any(), any(), any());

        var invocation = new Invocation(
                mock(InternalLog.class), handle, uriScheme.txCommitUri(1337L), memoryPool, inputEventStream, true);

        assertThrows(RuntimeException.class, () -> invocation.execute(outputEventStream));

        verify(memoryPool).reserveHeap(Statement.SHALLOW_SIZE);
        verify(memoryPool).releaseHeap(Statement.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryPool);
    }

    @Test
    void shouldExecuteStatementsWithWriteTransaction() throws Throwable {
        // given
        when(registry.begin(any(TransactionHandle.class))).thenReturn(123L);
        TransactionHandle handle = getTransactionHandle(registry, true, false);

        InputEventStream inputEventStream = mock(InputEventStream.class);
        Statement statement = new Statement("query", map());
        when(inputEventStream.read()).thenReturn(statement, NULL_STATEMENT);

        Invocation invocation = new Invocation(
                log,
                handle,
                uriScheme.txCommitUri(parseLong(TX_ID)),
                mock(MemoryPool.class, RETURNS_MOCKS),
                inputEventStream,
                true);

        // when
        invocation.execute(outputEventStream);

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder(transactionManager, transaction, this.statement);
        txManagerOrder
                .verify(transactionManager)
                .create(
                        TransactionType.IMPLICIT,
                        handle,
                        "neo4j",
                        AccessMode.WRITE,
                        emptyList(),
                        null,
                        Collections.emptyMap(),
                        null);
        txManagerOrder.verify(transaction).run("query", MapValue.EMPTY);
        txManagerOrder.verify(this.statement).consume(any(ResponseHandler.class), anyLong());
        txManagerOrder.verify(transaction).commit();
        txManagerOrder.verify(transaction).close();
        txManagerOrder.verifyNoMoreInteractions();

        // then verify output
        InOrder outputOrder = inOrder(outputEventStream);
        outputOrder.verify(outputEventStream).writeStatementStart(statement, List.of("c1", "c2", "c3"));
        verifyDefaultResultRows(outputOrder);
        outputOrder
                .verify(outputEventStream)
                .writeStatementEnd(any(), any(), any(), any()); // todo work out why the actual args fails
        //        outputOrder.verify( outputEventStream ).writeStatementEnd( query( QueryExecutionType.QueryType.WRITE
        // ), QueryStatistics.EMPTY,
        //                                                                   HttpExecutionPlanDescription.EMPTY,
        // emptyList() );
        outputOrder
                .verify(outputEventStream)
                .writeTransactionInfo(
                        TransactionNotificationState.COMMITTED, uriScheme.txCommitUri(123L), -1, SERIALIZED_BOOKMARK);
        verifyNoMoreInteractions(outputEventStream);
    }

    private void verifyDefaultResultRows(InOrder outputOrder) {
        outputOrder
                .verify(outputEventStream)
                .writeRecord(
                        eq(List.of("c1", "c2", "c3")),
                        argThat(new ValuesMatcher(Map.of("c1", "v1", "c2", "v2", "c3", "v3"))));
        outputOrder
                .verify(outputEventStream)
                .writeRecord(
                        eq(List.of("c1", "c2", "c3")),
                        argThat(new ValuesMatcher(Map.of("c1", "v4", "c2", "v5", "c3", "v6"))));
    }

    private TransactionHandle getTransactionHandle(TransactionRegistry registry) {
        return getTransactionHandle(registry, true, true);
    }

    private TransactionHandle getTransactionHandle(
            TransactionRegistry registry, boolean implicitTransaction, boolean readOnly) {
        return new TransactionHandle(
                "neo4j",
                registry,
                uriScheme,
                implicitTransaction,
                AUTH_DISABLED,
                mock(ClientConnectionInfo.class),
                anyLong(),
                transactionManager,
                logProvider,
                memoryTracker,
                authManager,
                readOnly,
                emptyList());
    }

    private org.neo4j.bolt.tx.statement.Statement generateStatementMock() {
        return StatementMockFactory.newFactory()
                .withResults(MockResult.newInstance(factory -> factory.withField("c1", "c2", "c3")
                        .withRecord(stringValue("v1"), stringValue("v2"), stringValue("v3"))
                        .withRecord(stringValue("v4"), stringValue("v5"), stringValue("v6"))
                        .withMetadata("type", stringValue("w"))
                        .withMetadata("db", stringValue("neo4j"))))
                .build();
    }

    private static class ValuesMatcher implements ArgumentMatcher<Function<String, Object>> {

        private final Map<String, Object> values;

        private ValuesMatcher(Map<String, Object> values) {
            this.values = values;
        }

        @Override
        public boolean matches(Function<String, Object> valueExtractor) {
            return values.entrySet().stream()
                    .anyMatch(entry -> entry.getValue().equals(valueExtractor.apply(entry.getKey())));
        }
    }

    private static final TransactionUriScheme uriScheme = new TransactionUriScheme() {
        @Override
        public URI txUri(long id) {
            return URI.create("transaction/" + id);
        }

        @Override
        public URI txCommitUri(long id) {
            return URI.create("transaction/" + id + "/commit");
        }

        @Override
        public URI dbUri() {
            return URI.create("data/");
        }
    };

    private class MockStatementExecutionException extends StatementExecutionException implements HasStatus {

        public MockStatementExecutionException() {
            super("Something went wrong");
        }

        @Override
        public Status status() {
            return General.UnknownError;
        }
    }
}
