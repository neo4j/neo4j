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
package org.neo4j.bolt.fsm;

import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineHandleAssertions.assertThat;

import java.util.List;
import org.mockito.Mockito;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.test.annotation.CommunityStateMachineTestExtension;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.annotation.fsm.initializer.Authenticated;
import org.neo4j.bolt.testing.assertions.MapValueAssertions;
import org.neo4j.bolt.testing.assertions.StateMachineHandleAssertions;
import org.neo4j.bolt.testing.messages.BoltMessages;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.statement.Statement;
import org.neo4j.boltmessages.request.RequestMessage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.storable.Values;

@CommunityStateMachineTestExtension
class ReadyStateIT {

    @StateMachineTest
    void shouldMoveToInterruptedOnInterrupt(
            @Authenticated StateMachineHandle fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.connection().interrupt();

        fsm.process(messages.run("RETURN 1"), recorder);
        assertThat(recorder).hasIgnoredResponse();

        assertThat(fsm).isInterrupted();
    }

    private void shouldCloseConnectionOnMessage(StateMachineHandle fsm, RequestMessage message) {
        var recorder = new ResponseRecorder();

        assertThat(fsm).shouldKillConnection(it -> it.process(message, recorder));

        assertThat(recorder).hasFailureResponse(Status.Request.Invalid);
    }

    @StateMachineTest
    void shouldCloseConnectionOnPull(@Authenticated StateMachineHandle fsm, BoltMessages messages) {
        shouldCloseConnectionOnMessage(fsm, messages.pull());
    }

    @StateMachineTest
    void shouldCloseConnectionOnDiscard(@Authenticated StateMachineHandle fsm, BoltMessages messages) {
        shouldCloseConnectionOnMessage(fsm, messages.discard());
    }

    @StateMachineTest
    void shouldCloseConnectionOnCommit(@Authenticated StateMachineHandle fsm, BoltMessages messages) {
        shouldCloseConnectionOnMessage(fsm, messages.commit());
    }

    @StateMachineTest
    void shouldCloseConnectionOnRollback(@Authenticated StateMachineHandle fsm, BoltMessages messages) {
        shouldCloseConnectionOnMessage(fsm, messages.rollback());
    }

    @StateMachineTest
    void shouldCloseConnectionOnGoodbye(@Authenticated StateMachineHandle fsm, BoltMessages messages) {
        shouldCloseConnectionOnMessage(fsm, messages.goodbye());
    }

    @StateMachineTest(until = @Version(major = 5, minor = 6))
    void shouldMoveToAutoCommitOnRunNoDb_succ(
            @Authenticated StateMachineHandle fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        // When
        fsm.process(messages.run("CREATE (n {k:'k'}) RETURN n.k"), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(meta -> MapValueAssertions.assertThat(meta)
                        .containsKey("fields")
                        .containsKey("t_first")
                        .doesNotContainKey("db"));

        StateMachineHandleAssertions.assertThat(fsm).isInState(States.AUTO_COMMIT);
    }

    static void mockTransaction(StateMachineHandle fsm, String homeDb) throws Throwable {
        var txMock = Mockito.mock(Transaction.class);
        var statementMock = Mockito.mock(Statement.class);
        Mockito.doReturn(homeDb).when(fsm.connection()).selectedDefaultDatabase();
        Mockito.doReturn(txMock)
                .when(fsm.connection())
                .beginTransaction(
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any());
        Mockito.doReturn(statementMock).when(txMock).run(Mockito.any(), Mockito.any());
        Mockito.doReturn(1L).when(statementMock).id();
        Mockito.doReturn(List.of("coolField")).when(statementMock).fieldNames();
    }

    @StateMachineTest(since = @Version(major = 5, minor = 8))
    void shouldMoveToAutoCommitOnRunWithDb_succ(
            @Authenticated StateMachineHandle fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        // Given
        final var expectedHomeDb = "neo5j";
        mockTransaction(fsm, expectedHomeDb);

        // When
        fsm.process(messages.run("CREATE (n {k:'k'}) RETURN n.k"), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(meta -> MapValueAssertions.assertThat(meta)
                        .containsKey("fields")
                        .containsKey("t_first")
                        .containsEntry("db", Values.of(expectedHomeDb)));

        Mockito.verify(fsm.connection()).resolveDefaultDatabase();

        StateMachineHandleAssertions.assertThat(fsm).isInState(States.AUTO_COMMIT);
    }

    @StateMachineTest
    void shouldMoveToAutoCommitOnRunOnSelectedDb_succ(
            @Authenticated StateMachineHandle fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        // Given
        mockTransaction(fsm, "someOtherDb");

        // When
        fsm.process(messages.run("CREATE (n {k:'k'}) RETURN n.k", "neo5j"), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(meta -> MapValueAssertions.assertThat(meta)
                        .containsKey("fields")
                        .containsKey("t_first")
                        .doesNotContainKey("db"));

        Mockito.verify(fsm.connection(), Mockito.never()).resolveDefaultDatabase();

        StateMachineHandleAssertions.assertThat(fsm).isInState(States.AUTO_COMMIT);
    }

    @StateMachineTest(until = @Version(major = 5, minor = 6))
    void shouldMoveToInTransactionOnBeginNoDb_succ(
            @Authenticated StateMachineHandle fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        // When
        fsm.process(messages.begin(), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(meta -> MapValueAssertions.assertThat(meta).doesNotContainKey("db"));

        Mockito.verify(fsm.connection(), Mockito.never()).resolveDefaultDatabase();

        StateMachineHandleAssertions.assertThat(fsm).isInState(States.IN_TRANSACTION);
    }

    @StateMachineTest(since = @Version(major = 5, minor = 8))
    void shouldMoveToInTransactionWithBeginWithDb_succ(
            @Authenticated StateMachineHandle fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        // When
        fsm.process(messages.begin(), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(meta -> MapValueAssertions.assertThat(meta)
                        .containsEntry("db", Values.of(fsm.connection().selectedDefaultDatabase())));

        Mockito.verify(fsm.connection()).resolveDefaultDatabase();

        StateMachineHandleAssertions.assertThat(fsm).isInState(States.IN_TRANSACTION);
    }

    @StateMachineTest()
    void shouldMoveToInTransactionWithBeginOnSelectedDb_succ(
            @Authenticated StateMachineHandle fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        // Given
        mockTransaction(fsm, "someOtherDb");

        // When
        fsm.process(messages.begin("neo4j"), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(meta -> MapValueAssertions.assertThat(meta).doesNotContainKey("db"));

        Mockito.verify(fsm.connection(), Mockito.never()).resolveDefaultDatabase();

        StateMachineHandleAssertions.assertThat(fsm).isInState(States.IN_TRANSACTION);
    }

    @StateMachineTest(since = @Version(major = 5, minor = 1))
    void shouldMoveBackToAuthenticationStateAfterALogoffMessage(
            StateMachineHandle fsm, BoltMessages messages, ResponseRecorder recorder) throws StateMachineException {
        // Given
        fsm.process(messages.hello(), recorder);
        fsm.process(messages.logon(), recorder);

        // When
        fsm.process(messages.logoff(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse();

        StateMachineHandleAssertions.assertThat(fsm).isInState(States.AUTHENTICATION);
    }
}
