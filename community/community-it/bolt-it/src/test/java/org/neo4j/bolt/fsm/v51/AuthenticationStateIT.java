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

package org.neo4j.bolt.fsm.v51;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltV51Messages.begin;
import static org.neo4j.bolt.testing.messages.BoltV51Messages.hello;
import static org.neo4j.bolt.testing.messages.BoltV51Messages.logon;

import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.v51.fsm.AuthenticationState;
import org.neo4j.bolt.protocol.v51.fsm.ReadyState;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.testing.assertions.StateMachineAssertions;
import org.neo4j.bolt.testing.response.ResponseRecorder;

public class AuthenticationStateIT extends BoltStateMachineV51StateTestBase {

    @Test
    public void shouldAcceptLogonMessageAndMoveToReadyState() throws BoltConnectionFatality {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachine();

        // When
        machine.process(hello(), recorder);
        assertThat(recorder).hasSuccessResponse();
        StateMachineAssertions.assertThat(machine).isInState(AuthenticationState.class);

        machine.process(logon(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse();
        StateMachineAssertions.assertThat(machine).isInState(ReadyState.class);
    }

    @Test
    public void shouldNotAcceptABeginMessageAndError() throws BoltConnectionFatality {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachine();

        // When
        machine.process(hello(), recorder);
        assertThat(recorder).hasSuccessResponse();
        StateMachineAssertions.assertThat(machine).isInState(AuthenticationState.class);

        // Then
        var e = assertThrows(BoltProtocolBreachFatality.class, () -> machine.process(begin(), recorder));
        assertEquals(
                "Message 'org.neo4j.bolt.protocol.v50.message.request.BeginMessage@782' cannot be handled by a session in the AUTHENTICATION state.",
                e.getMessage());
    }
}
