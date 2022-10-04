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

import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.testing.assertions.MapValueAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.begin;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.commit;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.discard;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.goodbye;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.hello;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.pull;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.reset;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.rollback;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.run;
import static org.neo4j.values.storable.BooleanValue.TRUE;
import static org.neo4j.values.storable.Values.longValue;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.fsm.v40.BoltStateMachineV4StateTestBase;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.Signal;
import org.neo4j.bolt.protocol.v40.fsm.AutoCommitState;
import org.neo4j.bolt.protocol.v40.fsm.InterruptedState;
import org.neo4j.bolt.protocol.v40.fsm.ReadyState;
import org.neo4j.bolt.protocol.v40.fsm.StateMachineV40;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.api.exceptions.Status;

class AutoCommitStateIT extends BoltStateMachineV4StateTestBase {
    @Test
    void shouldMoveFromAutoCommitToReadyOnPull_succ() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInAutoCommitState();

        // When
        machine.process(pull(100L), recorder);

        // Then
        assertThat(recorder).hasRecord().hasSuccessResponse(meta -> assertThat(meta)
                .containsKey("type")
                .containsKey("t_last")
                .containsKey("bookmark")
                .containsKey("db"));

        assertThat(machine).isInState(ReadyState.class);
    }

    @Test
    void shouldMoveFromAutoCommitToReadyOnPull_succ_hasMore() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInAutoCommitState("Unwind [1, 2, 3] as n return n");

        // When
        machine.process(pull(2L), recorder);

        // Then
        assertThat(recorder).hasRecord(longValue(1)).hasRecord(longValue(2)).hasSuccessResponse(meta -> assertThat(meta)
                .containsEntry("has_more", TRUE)
                .doesNotContainKey("db")
                .doesNotContainKey("bookmark"));

        machine.process(pull(2L), recorder);

        assertThat(recorder).hasRecord(longValue(3)).hasSuccessResponse(meta -> assertThat(meta)
                .containsKey("type")
                .containsKey("t_last")
                .containsKey("bookmark")
                .containsKey("db"));

        assertThat(machine).isInState(ReadyState.class);
    }

    @Test
    void shouldMoveFromAutoCommitToReadyOnDiscardAll_succ() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInAutoCommitState();

        // When
        machine.process(discard(2L), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(
                        meta -> assertThat(meta).containsKey("bookmark").containsKey("db"));

        assertThat(machine).isInState(ReadyState.class);
    }

    @Test
    void shouldMoveFromAutoCommitToInterruptedOnInterrupt() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInAutoCommitState();

        // When
        machine.process(Signal.INTERRUPT, recorder);

        // Then
        assertThat(machine).isInState(InterruptedState.class);
    }

    @ParameterizedTest
    @MethodSource("illegalV4Messages")
    void shouldCloseConnectionOnIllegalV4MessagesInAutoCommitState(RequestMessage message) throws Throwable {
        // Given
        ResponseRecorder recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInAutoCommitState("CREATE (n {k:'k'}) RETURN n.k");

        assertThat(machine)
                .shouldKillConnection(fsm -> fsm.process(message, recorder))
                .isInInvalidState();

        assertThat(recorder).hasFailureResponse(Status.Request.Invalid);
    }

    private static Stream<RequestMessage> illegalV4Messages() {
        return Stream.of(hello(), run("any string"), begin(), rollback(), commit(), reset(), goodbye());
    }

    private StateMachineV40 getBoltStateMachineInAutoCommitState() throws BoltConnectionFatality {
        return getBoltStateMachineInAutoCommitState("CREATE (n {k:'k'}) RETURN n.k");
    }

    private StateMachineV40 getBoltStateMachineInAutoCommitState(String query) throws BoltConnectionFatality {
        var machine = newStateMachine();

        machine.process(hello(), nullResponseHandler());
        machine.process(run(query), nullResponseHandler());

        assertThat(machine).isInState(AutoCommitState.class);

        return machine;
    }
}
