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

import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;

import java.util.List;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.state.InterruptedState;
import org.neo4j.bolt.protocol.v40.fsm.state.FailedState;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.test.annotation.CommunityStateMachineTestExtension;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.annotation.fsm.initializer.Failed;
import org.neo4j.bolt.testing.messages.BoltMessages;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.api.exceptions.Status;

@CommunityStateMachineTestExtension
class FailedStateIT {

    @StateMachineTest
    void shouldIgnoreMessages(@Failed StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws Throwable {
        var candidates = List.of(
                messages.discard(2L),
                messages.pull(2L),
                messages.run("A cypher query"),
                messages.rollback(),
                messages.commit());

        for (var message : candidates) {
            fsm.process(message, recorder);

            assertThat(recorder).hasIgnoredResponse();

            assertThat(fsm).isInState(FailedState.class);
        }
    }

    @StateMachineTest
    void shouldMoveToInterruptedOnInterruptSignal(
            @Failed StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws BoltConnectionFatality {
        fsm.connection().interrupt();

        fsm.process(messages.pull(), recorder);
        assertThat(recorder).hasIgnoredResponse();

        assertThat(fsm).isInState(InterruptedState.class);
    }

    @StateMachineTest
    void shouldTerminateConnectionOnHello(@Failed StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) {
        assertThat(fsm)
                .shouldKillConnection(machine -> machine.process(messages.hello(), recorder))
                .isInInvalidState();

        assertThat(recorder).hasFailureResponse(Status.Request.Invalid);
    }

    @StateMachineTest
    void shouldTerminateConnectionOnBegin(@Failed StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) {
        assertThat(fsm)
                .shouldKillConnection(machine -> machine.process(messages.begin(), recorder))
                .isInInvalidState();

        assertThat(recorder).hasFailureResponse(Status.Request.Invalid);
    }

    @StateMachineTest
    void shouldTerminateConnectionOnGoodbye(
            @Failed StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) {
        assertThat(fsm)
                .shouldKillConnection(machine -> machine.process(messages.goodbye(), recorder))
                .isInInvalidState();

        assertThat(recorder).hasFailureResponse(Status.Request.Invalid);
    }
}
