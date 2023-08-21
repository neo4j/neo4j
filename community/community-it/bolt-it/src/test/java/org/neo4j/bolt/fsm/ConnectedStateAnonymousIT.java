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

import static org.neo4j.bolt.testing.assertions.MapValueAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;
import static org.neo4j.values.storable.Values.stringValue;

import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.test.annotation.CommunityStateMachineTestExtension;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.messages.BoltMessages;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.internal.Version;

@CommunityStateMachineTestExtension
class ConnectedStateAnonymousIT {

    @StateMachineTest(until = @org.neo4j.bolt.testing.annotation.Version(major = 5, minor = 1))
    void shouldHandleHelloMessage(StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        fsm.process(messages.hello(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse(meta -> assertThat(meta)
                .containsEntry("server", stringValue("Neo4j/" + Version.getNeo4jVersion()))
                .containsEntry("connection_id", stringValue("bolt-test")));

        assertThat(fsm).isInState(States.READY);
    }
}
