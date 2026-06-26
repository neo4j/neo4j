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

package org.neo4j.bolt;

import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.setup.preset.EnableTelemetry;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.testing.messages.factory.TelemetryMessageBuilder;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@EnableTelemetry
@IncludeWire(since = @Version(major = 5, minor = 4))
public class TelemetryDisabledIT {

    @ProtocolTest
    void shouldProcessTelemetryMessage(@Authenticated BoltTestConnection connection, BoltWire wire) {
        connection
                .send(wire.telemetry(TelemetryMessageBuilder::withExecute))
                .send(wire.telemetry(TelemetryMessageBuilder::withUnmanagedTransactions))
                .send(wire.telemetry(TelemetryMessageBuilder::withManagedTransactionFunctions))
                .send(wire.telemetry(TelemetryMessageBuilder::withImplicitTransactions));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(4);
    }
}
