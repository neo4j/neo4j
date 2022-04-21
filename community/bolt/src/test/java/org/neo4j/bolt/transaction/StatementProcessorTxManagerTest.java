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
package org.neo4j.bolt.transaction;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.bolt.runtime.statemachine.impl.ExplicitTxStatementMetadata;
import org.neo4j.bolt.runtime.statemachine.impl.StatementProcessorProvider;
import org.neo4j.bolt.v4.messaging.DiscardResultConsumer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.virtual.MapValue;

class StatementProcessorTxManagerTest {
    private final StatementProcessorProvider statementProcessorProvider = mock(StatementProcessorProvider.class);
    private final StatementProcessor statementProcessor = mock(StatementProcessor.class);

    @Test
    void shouldBeginTransaction() throws Exception {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();
        verify(statementProcessor).beginTransaction(emptyList(), Duration.ZERO, AccessMode.WRITE, emptyMap());
    }

    @Test
    void shouldRunCypherInTransaction() throws Exception {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();

        txManager.runQuery(transactionId, "RETURN 1", MapValue.EMPTY);
        verify(statementProcessor).run("RETURN 1", MapValue.EMPTY);
    }

    @Test
    void shouldCommitTransaction() throws Exception {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();

        // no error thrown
        txManager.commit(transactionId);
        verify(statementProcessor).commitTransaction();
    }

    @Test
    void shouldRollbackTransaction() throws Exception {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();

        txManager.rollback(transactionId);
        verify(statementProcessor).reset();
    }

    @Test
    void shouldFailToRunMoreStatementsInRolledBackTx() throws Exception {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        var transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();

        txManager.rollback(transactionId);
        assertThrows(TransactionNotFoundException.class, () -> txManager.runQuery(transactionId, "R", MapValue.EMPTY));
    }

    @Test
    void shouldThrowOnKernelException() throws Exception {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());
        doThrow(mock(KernelException.class)).when(statementProcessor).run(any(), any());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();

        assertThrows(KernelException.class, () -> txManager.runQuery(transactionId, "RETURN 1", MapValue.EMPTY));
    }

    @Test
    void shouldFailToRunInNonExistentTransaction() {
        StatementProcessorTxManager txManager = new StatementProcessorTxManager();

        assertThrows(
                TransactionNotFoundException.class,
                () -> txManager.runQuery("Does Not Exist!", "RETURN 1", MapValue.EMPTY));
    }

    @Test
    void shouldFailToCommitNonExistentTransaction() {
        StatementProcessorTxManager txManager = new StatementProcessorTxManager();

        assertThrows(TransactionNotFoundException.class, () -> txManager.commit("Does Not Exist!"));
    }

    @Test
    void shouldFailToRollbackNonExistentTransaction() {
        StatementProcessorTxManager txManager = new StatementProcessorTxManager();

        assertThrows(TransactionNotFoundException.class, () -> txManager.rollback("Does Not Exist!"));
    }

    @Test
    void shouldFailToRunMoreStatementsInCommittedTx() throws Exception {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();

        txManager.commit(transactionId);
        assertThrows(TransactionNotFoundException.class, () -> txManager.runQuery(transactionId, "R", MapValue.EMPTY));
    }

    @Test
    void shouldRunProgram() throws Exception {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        ProgramResultReference programResultReference = txManager.runProgram(
                "123",
                AUTH_DISABLED,
                "neo4j",
                "RETURN 1",
                MapValue.EMPTY,
                emptyList(),
                false,
                Map.of(),
                Duration.ofMillis(100),
                "connectionId");
        assertThat(programResultReference).isNotNull();
        verify(statementProcessor)
                .run("RETURN 1", MapValue.EMPTY, emptyList(), Duration.ofMillis(100), AccessMode.WRITE, Map.of());
    }

    @Test
    void shouldAddAndRemoveStatementProcessor() throws Exception {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        // finds the statement processor
        txManager.runProgram(
                "123",
                AUTH_DISABLED,
                "neo4j",
                "RETURN 1",
                MapValue.EMPTY,
                emptyList(),
                false,
                Map.of(),
                Duration.ofMillis(100),
                "connectionId");

        txManager.removeStatementProcessorProvider("connectionId");

        assertThrows(
                RuntimeException.class,
                () -> txManager.runProgram(
                        "123",
                        AUTH_DISABLED,
                        "neo4j",
                        "RETURN 1",
                        MapValue.EMPTY,
                        emptyList(),
                        true,
                        Map.of(),
                        Duration.ofMillis(100),
                        "connectionId"));
    }

    @Test
    void shouldStreamResultsFollowingProgramRun() throws Throwable {
        StatementMetadata statementMetadata = StatementMetadata.EMPTY;
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());
        doReturn(statementMetadata).when(statementProcessor).run(any(), any(), any(), any(), any(), any());

        doAnswer((Answer<Void>) invocation -> {
                    ResultConsumer resultConsumer = invocation.getArgument(1);
                    resultConsumer.consume(BoltResult.EMPTY);
                    resultConsumer.consume(BoltResult.EMPTY);
                    resultConsumer.consume(BoltResult.EMPTY);
                    return null;
                })
                .when(statementProcessor)
                .streamResult(anyInt(), any(ResultConsumer.class));

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        ProgramResultReference programResultReference = txManager.runProgram(
                "123",
                AUTH_DISABLED,
                "neo4j",
                "RETURN 1",
                MapValue.EMPTY,
                emptyList(),
                false,
                Map.of(),
                Duration.ofMillis(100),
                "connectionId");
        assertThat(programResultReference).isNotNull();

        ResultConsumer resultConsumer = mock(ResultConsumer.class);

        txManager.pullData(
                programResultReference.transactionId(),
                programResultReference.statementMetadata().queryId(),
                -1,
                resultConsumer);
        verify(resultConsumer, times(3)).consume(any());
    }

    @Test
    void shouldStreamResultsFollowingBeginRun() throws Throwable {
        StatementMetadata statementMetadata = StatementMetadata.EMPTY;
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());
        doReturn(statementMetadata).when(statementProcessor).run(any(), any());

        doAnswer((Answer<Void>) invocation -> {
                    ResultConsumer resultConsumer = invocation.getArgument(1);
                    resultConsumer.consume(BoltResult.EMPTY);
                    resultConsumer.consume(BoltResult.EMPTY);
                    resultConsumer.consume(BoltResult.EMPTY);
                    return null;
                })
                .when(statementProcessor)
                .streamResult(anyInt(), any(ResultConsumer.class));
        ResultConsumer resultConsumer = mock(ResultConsumer.class);

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");

        var metadata = txManager.runQuery(transactionId, "RETURN 1", MapValue.EMPTY);

        txManager.pullData(transactionId, metadata.queryId(), -1, resultConsumer);
        verify(resultConsumer, times(3)).consume(any());
    }

    @Test
    void shouldDiscardData() throws Throwable {
        StatementMetadata statementMetadata = new ExplicitTxStatementMetadata(new String[] {}, 0);
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());
        doReturn(statementMetadata).when(statementProcessor).run(any(), any());
        ResultConsumer resultConsumer = mock(ResultConsumer.class);

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();

        var metadata = txManager.runQuery(transactionId, "RETURN 1", MapValue.EMPTY);
        assertThat(metadata).isNotNull();

        txManager.discardData(transactionId, metadata.queryId(), 20, resultConsumer);

        verify(statementProcessor).streamResult(0, resultConsumer);
    }

    @Test
    void shouldCancelData() throws Throwable {
        StatementMetadata statementMetadata = new ExplicitTxStatementMetadata(new String[] {}, 0);
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());
        doReturn(statementMetadata).when(statementProcessor).run(any(), any());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();

        var metadata = txManager.runQuery(transactionId, "RETURN 1", MapValue.EMPTY);
        assertThat(metadata).isNotNull();

        txManager.cancelData(transactionId, metadata.queryId());

        verify(statementProcessor).streamResult(anyInt(), any(DiscardResultConsumer.class));
    }

    @Test
    void shouldInterruptTransaction() throws Throwable {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();

        txManager.interrupt(transactionId);
        verify(statementProcessor).markCurrentTransactionForTermination();
    }

    @Test
    void shouldReturnInTransactionNoOpenStatementsTransactionStatus() throws Throwable {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();

        var transactionStatus = txManager.transactionStatus(transactionId);
        assertThat(transactionStatus.value()).isEqualTo(TransactionStatus.Value.IN_TRANSACTION_NO_OPEN_STATEMENTS);
    }

    @Test
    void shouldReturnTerminatingTransactionStatus() throws Throwable {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());
        doReturn(Status.General.InvalidArguments).when(statementProcessor).validateTransaction();

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");
        assertThat(transactionId).isNotNull();

        var transactionStatus = txManager.transactionStatus(transactionId);
        assertThat(transactionStatus.value()).isEqualTo(TransactionStatus.Value.INTERRUPTED);
    }

    @Test
    void shouldReturnClosedNotExistsTransactionStatus() {
        StatementProcessorTxManager txManager = new StatementProcessorTxManager();

        var transactionStatus = txManager.transactionStatus("DOES NOT EXIST!");
        assertThat(transactionStatus.value()).isEqualTo(TransactionStatus.Value.CLOSED_OR_DOES_NOT_EXIST);
    }

    @Test
    void shouldReturnInTransactionWithOpenStatementsTransactionStatus() throws Throwable {
        StatementMetadata statementMetadata = StatementMetadata.EMPTY;
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());
        doReturn(statementMetadata).when(statementProcessor).run(any(), any());
        doReturn(true).when(statementProcessor).hasOpenStatement();

        doAnswer((Answer<Void>) invocation -> {
                    ResultConsumer resultConsumer = invocation.getArgument(1);
                    resultConsumer.consume(BoltResult.EMPTY);
                    resultConsumer.consume(BoltResult.EMPTY);
                    resultConsumer.consume(BoltResult.EMPTY);
                    return null;
                })
                .when(statementProcessor)
                .streamResult(anyInt(), any(ResultConsumer.class));

        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");

        txManager.runQuery(transactionId, "RETURN 1", MapValue.EMPTY);

        var transactionStatus = txManager.transactionStatus(transactionId);
        assertThat(transactionStatus.value()).isEqualTo(TransactionStatus.Value.IN_TRANSACTION_OPEN_STATEMENT);
    }

    @Test
    void shouldNotThrowWhenInterruptingNullTxId() {
        StatementProcessorTxManager txManager = new StatementProcessorTxManager();
        assertDoesNotThrow(() -> txManager.interrupt(null));
    }

    @Test
    void shouldCleanupTransaction() throws Throwable {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());
        StatementProcessorTxManager txManager = new StatementProcessorTxManager();

        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        String transactionId = txManager.begin(
                AUTH_DISABLED, "neo4j", emptyList(), false, Collections.emptyMap(), Duration.ZERO, "connectionId");

        txManager.cleanUp(new CleanUpTransactionContext(transactionId));

        assertThat(txManager.getCurrentNoOfOpenTx()).isEqualTo(0);
    }

    @Test
    void shouldCleanupConnection() throws Throwable {
        doReturn(statementProcessor)
                .when(statementProcessorProvider)
                .getStatementProcessor(any(), anyString(), anyString());
        StatementProcessorTxManager txManager = new StatementProcessorTxManager();

        txManager.initialize(new InitializeContext("connectionId", statementProcessorProvider));

        txManager.cleanUp(new CleanUpConnectionContext("connectionId"));

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> txManager.begin(
                        AUTH_DISABLED,
                        "neo4j",
                        emptyList(),
                        false,
                        Collections.emptyMap(),
                        Duration.ZERO,
                        "connectionId"));
        assertThat(exception.getMessage())
                .contains("StatementProcessorProvider for connectionId: connectionId not found.");
    }
}
