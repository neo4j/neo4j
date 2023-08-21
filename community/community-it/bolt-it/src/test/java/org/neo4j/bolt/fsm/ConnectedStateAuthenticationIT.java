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

import java.util.Map;
import org.neo4j.bolt.test.annotation.CommunityStateMachineTestExtension;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.messages.BoltMessages;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.Version;
import org.neo4j.values.storable.Values;

@CommunityStateMachineTestExtension
class ConnectedStateAuthenticationIT {

    @SettingsFunction
    static void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(GraphDatabaseSettings.auth_enabled, true);
    }

    @StateMachineTest(until = @org.neo4j.bolt.testing.annotation.Version(major = 5, minor = 1))
    void shouldGiveCredentialsExpiredStatusOnExpiredCredentials(
            StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        fsm.process(messages.hello("neo4j", "neo4j"), recorder);
        fsm.process(messages.run("CREATE ()"), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(meta -> assertThat(meta).containsEntry("credentials_expired", Values.TRUE))
                .hasFailureResponse(Status.Security.CredentialsExpired);
    }

    @StateMachineTest(until = @org.neo4j.bolt.testing.annotation.Version(major = 5, minor = 1))
    void shouldGiveKernelVersionOnInit(StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws Throwable {
        var version = "Neo4j/" + Version.getNeo4jVersion();

        fsm.process(messages.hello("neo4j", "neo4j"), recorder);

        assertThat(recorder)
                .hasSuccessResponse(
                        meta -> assertThat(meta).extractingEntry("server").isEqualTo(version));
    }

    @StateMachineTest(until = @org.neo4j.bolt.testing.annotation.Version(major = 5, minor = 1))
    void shouldCloseConnectionAfterAuthenticationFailure(
            StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws Throwable {
        assertThat(fsm).shouldKillConnection(it -> it.process(messages.hello("neo4j", "j4oen"), recorder));

        assertThat(recorder).hasFailureResponse(Status.Security.Unauthorized);
    }
}
