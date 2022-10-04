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
package org.neo4j.bolt.fsm.v41;

import static org.neo4j.bolt.protocol.common.fsm.StateMachineSPIImpl.BOLT_SERVER_VERSION_PREFIX;
import static org.neo4j.bolt.testing.assertions.MapValueAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;
import static org.neo4j.values.storable.Values.stringValue;

import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.v40.fsm.ReadyState;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.internal.Version;

class ConnectedStateIT extends BoltStateMachineV41StateTestBase {
    @Test
    void shouldHandleHelloMessage() throws Throwable {
        // Given
        var machine = newStateMachine();
        var recorder = new ResponseRecorder();

        // When
        machine.process(newHelloMessage(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse(meta -> assertThat(meta)
                .containsEntry("server", stringValue(BOLT_SERVER_VERSION_PREFIX + Version.getNeo4jVersion()))
                .containsEntry("connection_id", stringValue("bolt-test")));

        assertThat(machine).isInState(ReadyState.class);
    }
}
