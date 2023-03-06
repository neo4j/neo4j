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
package org.neo4j.bolt.protocol.common.fsm;

import org.neo4j.bolt.protocol.v40.fsm.state.ConnectedState;
import org.neo4j.bolt.protocol.v51.fsm.state.NegotiationState;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTestExtension;
import org.neo4j.bolt.testing.assertions.StateMachineAssertions;

@StateMachineTestExtension
public class StateTransitionStateMachineTest {

    @StateMachineTest(include = {@Version(major = 4), @Version(major = 5, minor = 0)})
    void shouldBeInConnectedStateWhenCreated(StateMachine fsm) {
        StateMachineAssertions.assertThat(fsm).isInState(ConnectedState.class);
    }

    @StateMachineTest(exclude = {@Version(major = 4), @Version(major = 5, minor = 0)})
    void shouldBeInNegotiationStateWhenCreated(StateMachine fsm) {
        StateMachineAssertions.assertThat(fsm).isInState(NegotiationState.class);
    }
}
