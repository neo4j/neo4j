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
package org.neo4j.bolt.runtime;

import static org.neo4j.bolt.testing.assertions.MapValueAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.bolt.protocol.v44.BoltProtocolV44;
import org.neo4j.bolt.protocol.v44.fsm.StateMachineV44;
import org.neo4j.bolt.testing.messages.BoltV44Messages;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.Version;
import org.neo4j.values.storable.Values;

class BoltConnectionAuthIT {

    @RegisterExtension
    static final SessionExtension env = new SessionExtension().withAuthEnabled(true);

    protected StateMachineV44 newStateMachine() {
        return (StateMachineV44) env.newMachine(BoltProtocolV44.VERSION);
    }

    @Test
    void shouldGiveCredentialsExpiredStatusOnExpiredCredentials() throws Throwable {
        // Given it is important for client applications to programmatically
        // identify expired credentials as the cause of not being authenticated
        var machine = newStateMachine();
        var recorder = new ResponseRecorder();

        // When
        var hello = BoltV44Messages.hello(newBasicAuthToken("neo4j", "neo4j"));

        machine.process(hello, recorder);
        machine.process(BoltV44Messages.run("CREATE ()"), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(meta -> assertThat(meta).containsEntry("credentials_expired", Values.TRUE))
                .hasFailureResponse(Status.Security.CredentialsExpired);
    }

    @Test
    void shouldGiveKernelVersionOnInit() throws Throwable {
        // Given it is important for client applications to programmatically
        // identify expired credentials as the cause of not being authenticated
        var machine = newStateMachine();
        var recorder = new ResponseRecorder();
        var version = "Neo4j/" + Version.getNeo4jVersion();

        // When
        var hello = BoltV44Messages.hello(newBasicAuthToken("neo4j", "neo4j"));

        machine.process(hello, recorder);
        machine.process(BoltV44Messages.run("CREATE ()"), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(
                        meta -> assertThat(meta).extractingEntry("server").isEqualTo(version));
    }

    @Test
    void shouldCloseConnectionAfterAuthenticationFailure() throws Throwable {
        // Given
        var machine = newStateMachine();
        var recorder = new ResponseRecorder();

        // When... then
        var hello = BoltV44Messages.hello(newBasicAuthToken("neo4j", "j4oen"));

        assertThat(machine).shouldKillConnection(fsm -> fsm.process(hello, recorder));
        assertThat(recorder).hasFailureResponse(Status.Security.Unauthorized);
    }
}
