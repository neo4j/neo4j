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
package org.neo4j.bolt.v4.runtime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40.ABSENT_DB_NAME;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.begin;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.commit;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.discard;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.goodbye;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.hello;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.pull;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.rollback;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.run;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.Signal;
import org.neo4j.bolt.protocol.v40.fsm.FailedState;
import org.neo4j.bolt.protocol.v40.fsm.InterruptedState;
import org.neo4j.bolt.protocol.v40.fsm.StateMachineV40;
import org.neo4j.bolt.protocol.v40.messaging.request.RunMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.api.exceptions.Status;

class FailedStateIT extends BoltStateMachineV4StateTestBase {
    @ParameterizedTest
    @MethodSource("ignoredMessages")
    void shouldIgnoreMessages(RequestMessage message) throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInFailedState();

        // When
        machine.process(message, recorder);

        // Then
        assertThat(recorder).hasIgnoredResponse();

        assertThat(machine).isInState(FailedState.class);
    }

    @Test
    void shouldMoveToInterruptedOnInterruptSignal() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInFailedState();

        // When
        machine.process(Signal.INTERRUPT, recorder);

        // Then
        assertThat(recorder).hasSuccessResponse();
        assertThat(machine).isInState(InterruptedState.class);
    }

    @ParameterizedTest
    @MethodSource("illegalV4Messages")
    void shouldCloseConnectionOnIllegalV4Messages(RequestMessage message) throws Throwable {
        var recorder = new ResponseRecorder();
        var machine = getBoltStateMachineInFailedState();

        assertThat(machine)
                .shouldKillConnection(fsm -> fsm.process(message, recorder))
                .isInInvalidState();

        assertThat(recorder).hasFailureResponse(Status.Request.Invalid);
    }

    private StateMachineV40 getBoltStateMachineInFailedState() throws BoltConnectionFatality {
        var recorder = new ResponseRecorder();
        var machine = newStateMachine();

        RunMessage runMessage = mock(RunMessage.class);
        when(runMessage.databaseName()).thenReturn(ABSENT_DB_NAME);
        when(runMessage.statement()).thenThrow(new RuntimeException("error here"));

        machine.process(hello(), nullResponseHandler());
        machine.process(runMessage, recorder);

        assertThat(recorder).hasFailureResponse(Status.General.UnknownError);

        assertThat(machine).isInState(FailedState.class);

        return machine;
    }

    private static Stream<RequestMessage> ignoredMessages() {
        return Stream.of(discard(2L), pull(2L), run("A cypher query"), rollback(), commit());
    }

    private static Stream<RequestMessage> illegalV4Messages() {
        return Stream.of(hello(), begin(), goodbye());
    }
}
