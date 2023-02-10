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
package org.neo4j.bolt.fsm;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40.ABSENT_DB_NAME;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;

import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v40.fsm.AutoCommitState;
import org.neo4j.bolt.protocol.v40.fsm.FailedState;
import org.neo4j.bolt.protocol.v40.fsm.InTransactionState;
import org.neo4j.bolt.protocol.v40.fsm.InterruptedState;
import org.neo4j.bolt.protocol.v44.message.request.RunMessage;
import org.neo4j.bolt.protocol.v50.message.request.BeginMessage;
import org.neo4j.bolt.protocol.v51.fsm.AuthenticationState;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.test.annotation.CommunityStateMachineTestExtension;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.annotation.fsm.initializer.Authenticated;
import org.neo4j.bolt.testing.assertions.MapValueAssertions;
import org.neo4j.bolt.testing.assertions.StateMachineAssertions;
import org.neo4j.bolt.testing.messages.BoltMessages;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.api.exceptions.Status;

@CommunityStateMachineTestExtension
class ReadyStateIT {

    @StateMachineTest
    void shouldMoveToInterruptedOnInterrupt(
            @Authenticated StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws BoltConnectionFatality {
        fsm.connection().interrupt();

        fsm.process(messages.run("RETURN 1"), recorder);
        assertThat(recorder).hasIgnoredResponse();

        assertThat(fsm).isInState(InterruptedState.class);
    }

    private void shouldCloseConnectionOnMessage(StateMachine fsm, RequestMessage message) {
        var recorder = new ResponseRecorder();

        assertThat(fsm)
                .shouldKillConnection(it -> it.process(message, recorder))
                .isInInvalidState();

        assertThat(recorder).hasFailureResponse(Status.Request.Invalid);
        assertThat(fsm).isInInvalidState();
    }

    @StateMachineTest
    void shouldCloseConnectionOnPull(@Authenticated StateMachine fsm, BoltMessages messages) {
        shouldCloseConnectionOnMessage(fsm, messages.pull());
    }

    @StateMachineTest
    void shouldCloseConnectionOnDiscard(@Authenticated StateMachine fsm, BoltMessages messages) {
        shouldCloseConnectionOnMessage(fsm, messages.discard());
    }

    @StateMachineTest
    void shouldCloseConnectionOnCommit(@Authenticated StateMachine fsm, BoltMessages messages) {
        shouldCloseConnectionOnMessage(fsm, messages.commit());
    }

    @StateMachineTest
    void shouldCloseConnectionOnRollback(@Authenticated StateMachine fsm, BoltMessages messages) {
        shouldCloseConnectionOnMessage(fsm, messages.rollback());
    }

    @StateMachineTest
    void shouldCloseConnectionOnGoodbye(@Authenticated StateMachine fsm, BoltMessages messages) {
        shouldCloseConnectionOnMessage(fsm, messages.goodbye());
    }

    @StateMachineTest
    void shouldMoveToAutoCommitOnRun_succ(
            @Authenticated StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        // When
        fsm.process(messages.run("CREATE (n {k:'k'}) RETURN n.k"), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse(meta -> MapValueAssertions.assertThat(meta)
                .containsKey("fields")
                .containsKey("t_first"));

        StateMachineAssertions.assertThat(fsm).isInState(AutoCommitState.class);
    }

    @StateMachineTest
    void shouldMoveToInTransactionOnBegin_succ(
            @Authenticated StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        // When
        fsm.process(messages.begin(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(InTransactionState.class);
    }

    @StateMachineTest
    void shouldMoveToFailedStateOnRun_fail(
            @Authenticated StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        // Given
        var runMessage = mock(RunMessage.class);
        when(runMessage.databaseName()).thenReturn(ABSENT_DB_NAME);
        when(runMessage.statement()).thenThrow(new RuntimeException("Fail"));

        // When
        fsm.process(runMessage, recorder);

        // Then
        assertThat(recorder).hasFailureResponse(Status.General.UnknownError);
        StateMachineAssertions.assertThat(fsm).isInState(FailedState.class);
    }

    @StateMachineTest
    void shouldMoveToFailedStateOnBegin_fail(
            @Authenticated StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        // Given
        var beginMessage = mock(BeginMessage.class);
        when(beginMessage.databaseName()).thenReturn(ABSENT_DB_NAME);
        when(beginMessage.bookmarks()).thenThrow(new RuntimeException("Fail"));

        // When
        fsm.process(beginMessage, recorder);

        // Then
        assertThat(recorder).hasFailureResponse(Status.General.UnknownError);

        StateMachineAssertions.assertThat(fsm).isInState(FailedState.class);
    }

    @StateMachineTest(since = @Version(major = 5, minor = 1))
    void shouldMoveBackToAuthenticationStateAfterALogoffMessage(
            StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws BoltConnectionFatality {
        // Given
        fsm.process(messages.hello(), recorder);
        fsm.process(messages.logon(), recorder);

        // When
        fsm.process(messages.logoff(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(AuthenticationState.class);
    }
}
