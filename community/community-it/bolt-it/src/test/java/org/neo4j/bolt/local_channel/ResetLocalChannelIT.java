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
package org.neo4j.bolt.local_channel;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Connected;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.client.UnwiredTestConnection;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.boltmessages.AccessMode;
import org.neo4j.boltmessages.request.authentication.HelloMessage;
import org.neo4j.boltmessages.request.connection.ResetMessage;
import org.neo4j.boltmessages.request.connection.RoutingContext;
import org.neo4j.boltmessages.request.transaction.BeginMessage;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@IncludeTransport({TransportType.LOCAL})
public class ResetLocalChannelIT extends AbstractLocalChannelIT {

    @TransportTest
    void shouldFailAResetWhenInUnauthenticatedState(@Connected BoltTestConnection connection) {
        connection.unwired(unwired -> {
            unwired.sendRequest(ResetMessage.getInstance());

            assertFailure(unwired.receiveResponse());
        });
    }

    @TransportTest
    void shouldFailAResetWhenInAuthenticationState(@Connected BoltTestConnection connection) {
        connection.unwired(unwired -> {
            // This will take us to authentication state.
            unwired.sendRequest(
                    new HelloMessage("test/embedded", List.of(), new RoutingContext(false, Map.of()), null));
            assertSuccess(unwired.receiveResponse());

            unwired.sendRequest(ResetMessage.getInstance());

            assertFailure(unwired.receiveResponse());
        });
    }

    @TransportTest
    void shouldResetToReadyStateWhenAuthenticated(@Connected BoltTestConnection connection) {
        connection.unwired(unwired -> {
            login(unwired);

            begin(unwired);

            unwired.sendRequest(ResetMessage.getInstance());
            assertSuccess(unwired.receiveResponse());

            // This should pass as after reset past Authentication state we should end back in
            begin(unwired);
        });
    }

    private static void begin(UnwiredTestConnection unwired) {
        unwired.sendRequest(
                new BeginMessage(List.of(), Duration.of(10, ChronoUnit.SECONDS), AccessMode.WRITE, Map.of(), "neo4j"));
        assertSuccess(unwired.receiveResponse());
    }
}
