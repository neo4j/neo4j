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
package org.neo4j.bolt.fsm.v40;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.protocol.v40.messaging.request.RollbackMessage.INSTANCE;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.testing.assertions.MapValueAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.begin;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.discard;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.goodbye;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.hello;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.pull;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.reset;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.run;
import static org.neo4j.values.storable.BooleanValue.TRUE;
import static org.neo4j.values.storable.Values.longValue;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.Signal;
import org.neo4j.bolt.protocol.common.message.result.ResponseHandler;
import org.neo4j.bolt.protocol.v40.fsm.FailedState;
import org.neo4j.bolt.protocol.v40.fsm.InTransactionState;
import org.neo4j.bolt.protocol.v40.fsm.InterruptedState;
import org.neo4j.bolt.protocol.v40.fsm.ReadyState;
import org.neo4j.bolt.protocol.v40.fsm.StateMachineV40;
import org.neo4j.bolt.protocol.v40.messaging.request.CommitMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.RunMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.api.exceptions.Status;

class InTransactionStateIT extends BoltStateMachineV4StateTestBase {
    @Test
    void shouldMoveFromInTxToReadyOnCommit_succ() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInTxState();

        // When
        machine.process(CommitMessage.INSTANCE, recorder);

        // Then
        assertThat(recorder).hasSuccessResponse(meta -> assertThat(meta).containsKey("bookmark"));

        assertThat(machine).isInState(ReadyState.class);
    }

    @Test
    void shouldMoveFromInTxToReadyOnRollback_succ() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInTxState();

        // When
        machine.process(INSTANCE, recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(
                        meta -> assertThat(meta).doesNotContainKey("bookmark").doesNotContainKey("db"));

        assertThat(machine).isInState(ReadyState.class);
    }

    @Test
    void shouldStayInTxOnDiscard_succ() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInTxState();

        // When
        machine.process(discard(100L), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(
                        meta -> assertThat(meta).doesNotContainKey("bookmark").containsKey("db"));

        assertThat(machine).isInState(InTransactionState.class);
    }

    @Test
    void shouldStayInTxOnDiscard_succ_hasMore() throws Throwable {
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInTxState("Unwind [1, 2, 3] as n return n");

        machine.process(discard(2), recorder);

        assertThat(recorder)
                .hasSuccessResponse(
                        meta -> assertThat(meta).containsEntry("has_more", TRUE).doesNotContainKey("db"));

        machine.process(discard(2), recorder);

        assertThat(recorder).hasSuccessResponse(meta -> assertThat(meta)
                .containsKey("type")
                .containsKey("t_last")
                .doesNotContainKey("bookmark")
                .containsKey("db")
                .doesNotContainKey("has_more"));

        assertThat(machine).isInState(InTransactionState.class);
    }

    @Test
    void shouldStayInTxOnPull_succ() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInTxState();

        // When
        machine.process(pull(100), recorder);

        // Then
        assertThat(recorder).hasRecord().hasSuccessResponse(meta -> assertThat(meta)
                .containsKey("type")
                .containsKey("t_last")
                .doesNotContainKey("bookmark")
                .containsKey("db"));

        assertThat(machine).isInState(InTransactionState.class);
    }

    @Test
    void shouldStayInTxOnPull_succ_hasMore() throws Throwable {
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInTxState("Unwind [1, 2, 3] as n return n");

        machine.process(pull(2), recorder);

        assertThat(recorder).hasRecord(longValue(1)).hasRecord(longValue(2)).hasSuccessResponse(meta -> assertThat(meta)
                .containsEntry("has_more", TRUE)
                .doesNotContainKey("db"));

        machine.process(pull(2), recorder);

        assertThat(recorder).hasRecord(longValue(3)).hasSuccessResponse(meta -> assertThat(meta)
                .containsKey("type")
                .containsKey("t_last")
                .doesNotContainKey("bookmark")
                .containsKey("db"));

        assertThat(machine).isInState(InTransactionState.class);
    }

    @Test
    void shouldStayInTxOnAnotherRun_succ() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInTxState();

        // When
        machine.process(new RunMessage("MATCH (n) RETURN n LIMIT 1"), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse(meta -> assertThat(meta).doesNotContainKey("bookmark"));

        assertThat(machine).isInState(InTransactionState.class);
    }

    @Test
    void shouldMoveFromInTxToFailedOnAnotherRun_fail() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInTxState();

        // When
        machine.process(new RunMessage("any string"), recorder);

        // Then
        assertThat(machine).isInState(FailedState.class);
    }

    @Test
    void shouldMoveFromInTxToInterruptedOnInterrupt() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInTxState();

        // When
        machine.process(Signal.INTERRUPT, recorder);

        // Then
        assertThat(machine).isInState(InterruptedState.class);
    }

    @ParameterizedTest
    @MethodSource("pullAllDiscardAllMessages")
    void shouldMoveFromInTxStateToFailedStateOnfail(RequestMessage message) throws Throwable {
        // Given
        var machine = getBoltStateMachineInTxState();

        var handler = mock(ResponseHandler.class);
        doThrow(new RuntimeException("Fail")).when(handler).onPullRecords(any(), anyLong());
        doThrow(new RuntimeException("Fail")).when(handler).onDiscardRecords(any(), anyLong());

        // When
        machine.process(message, handler);

        // Then
        assertThat(machine).isInState(FailedState.class);
    }

    @ParameterizedTest
    @MethodSource("illegalV4Messages")
    void shouldCloseConnectionOnIllegalV4MessagesInTxStreamingState(RequestMessage message) throws Throwable {
        var machine = newStateMachine();

        machine.process(hello(), nullResponseHandler());

        machine.process(begin(), nullResponseHandler());
        machine.process(run("CREATE (n {k:'k'}) RETURN n.k"), nullResponseHandler());

        assertThat(machine).isInState(InTransactionState.class);

        var recorder = new ResponseRecorder();

        assertThat(machine)
                .shouldKillConnection(fsm -> fsm.process(message, recorder))
                .isInInvalidState();

        assertThat(recorder).hasFailureResponse(Status.Request.Invalid);
    }

    private static Stream<RequestMessage> illegalV4Messages() {
        return Stream.of(hello(), begin(), reset(), goodbye());
    }

    private static Stream<RequestMessage> pullAllDiscardAllMessages() throws BoltIOException {
        return Stream.of(pull(100L), discard(100L));
    }

    private StateMachineV40 getBoltStateMachineInTxState() throws BoltConnectionFatality {
        return getBoltStateMachineInTxState("CREATE (n {k:'k'}) RETURN n.k");
    }

    private StateMachineV40 getBoltStateMachineInTxState(String query) throws BoltConnectionFatality {
        var machine = newStateMachine();
        machine.process(hello(), nullResponseHandler());

        machine.process(begin(), nullResponseHandler());

        assertThat(machine).isInState(InTransactionState.class);

        machine.process(run(query), nullResponseHandler());

        assertThat(machine).isInState(InTransactionState.class);

        return machine;
    }
}
