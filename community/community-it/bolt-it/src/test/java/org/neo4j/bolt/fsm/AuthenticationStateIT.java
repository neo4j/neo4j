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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;

import org.assertj.core.api.Assertions;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.error.state.IllegalTransitionException;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.test.annotation.CommunityStateMachineTestExtension;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.assertions.StateMachineAssertions;
import org.neo4j.bolt.testing.messages.BoltMessages;
import org.neo4j.bolt.testing.response.ResponseRecorder;

@CommunityStateMachineTestExtension
public class AuthenticationStateIT {

    @StateMachineTest(since = @Version(major = 5, minor = 1))
    public void shouldAcceptLogonMessageAndMoveToReadyState(
            StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws StateMachineException {
        // Given
        fsm.process(messages.hello(), recorder);
        assertThat(recorder).hasSuccessResponse();
        StateMachineAssertions.assertThat(fsm).isInState(States.AUTHENTICATION);

        // When
        fsm.process(messages.logon(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse();
        StateMachineAssertions.assertThat(fsm).isInState(States.READY);
    }

    @StateMachineTest(since = @Version(major = 5, minor = 1))
    public void shouldNotAcceptBeginMessage(StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        // Given
        fsm.process(messages.hello(), recorder);
        assertThat(recorder).hasSuccessResponse();
        StateMachineAssertions.assertThat(fsm).isInState(States.AUTHENTICATION);

        // Then
        var e = assertThrows(IllegalTransitionException.class, () -> fsm.process(messages.begin(), recorder));
        Assertions.assertThat(e.getMessage()).contains("cannot be handled by a session in the AUTHENTICATION state.");
    }
}
