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

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.test.util.ServerUtil;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Ensures that Bolt correctly transmits NOOP chunks when a connection is in idle for extended periods of time while
 * awaiting a server response.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class KeepAliveIT {

    @Inject
    private Neo4jWithSocket server;

    @SettingsFunction
    static void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(BoltConnector.connection_keep_alive, Duration.ofMillis(20));
        settings.put(BoltConnector.connection_keep_alive_streaming_scheduling_interval, Duration.ofMillis(10));
    }

    @BeforeEach
    void prepare() throws ProcedureException {
        ServerUtil.installSleepProcedure(this.server);
    }

    @ProtocolTest
    @ExcludeWire(@Version(major = 4, minor = 2, range = 2))
    void shouldSendNoOpForLongRunningTx(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        connection.send(wire.run("CALL boltissue.sleep(100)")).send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess()
                .hasReceivedNoopChunks()
                .receivesSuccess();
    }

    @ProtocolTest
    @IncludeWire(@Version(major = 4, minor = 0))
    void shouldNotSendNoOpForLongRunningTxInLegacyVersions(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        connection.send(wire.run("CALL boltissue.sleep(100)")).send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess()
                .hasReceivedNoopChunks(0)
                .receivesSuccess();
    }
}
