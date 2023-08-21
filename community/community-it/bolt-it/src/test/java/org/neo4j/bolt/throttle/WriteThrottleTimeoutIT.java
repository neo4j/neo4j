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
package org.neo4j.bolt.throttle;

import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.runtime.throttle.ChannelWriteThrottleHandler;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.connection.transport.ExcludeTransport;
import org.neo4j.bolt.test.annotation.setup.FactoryFunction;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Ensures that Bolt correctly terminates connections when a client fails to consume messages thus triggering
 * backpressure through the outgoing message buffer.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
public class WriteThrottleTimeoutIT {

    private final AssertableLogProvider internalLogProvider = new AssertableLogProvider();

    @Inject
    private OtherThread otherThread;

    @FactoryFunction
    void customizeDatabase(TestDatabaseManagementServiceBuilder factory) {
        factory.setInternalLogProvider(this.internalLogProvider);
    }

    @SettingsFunction
    static void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_high_water_mark, (int)
                ByteUnit.kibiBytes(64));
        settings.put(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_low_water_mark, (int)
                ByteUnit.kibiBytes(16));
        settings.put(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_max_duration, Duration.ofSeconds(30));
    }

    @BeforeEach
    void prepare() {
        this.otherThread.set(5, TimeUnit.MINUTES);
    }

    @AfterEach
    void cleanup() {
        this.internalLogProvider.clear();
    }

    // Restrict to raw transports as we do not get direct access to WebSocket sockets
    @TransportTest
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    @ExcludeTransport({TransportType.WEBSOCKET, TransportType.WEBSOCKET_TLS, TransportType.LOCAL})
    void sendingButNotReceivingClientShouldBeKilledWhenWriteThrottleMaxDurationIsReached(
            BoltWire wire, @Authenticated TransportConnection connection) {
        connection.setOption(StandardSocketOptions.SO_RCVBUF, (int) ByteUnit.kibiBytes(32));

        var sender = otherThread.execute(() -> {
            // TODO: There seems to be additional buffering going on somewhere thus making this flakey unless we keep
            //       spamming the server until the error is raised
            while (!Thread.interrupted()) {
                // we're explicitly choosing a query which returns a significant number of results while only taking a
                // small amount of bytes to be encoded so that we don't accidentally fill up the send buffer first which
                // would cause this test to lock up
                connection
                        .send(wire.run("UNWIND range(0, 1000000) AS x RETURN randomUuid()"))
                        .send(wire.pull());
            }

            return null;
        });

        Assertions.assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(() -> otherThread.get().awaitFuture(sender))
                .withRootCauseInstanceOf(IOException.class);

        LogAssertions.assertThat(internalLogProvider)
                .forClass(ChannelWriteThrottleHandler.class)
                .forLevel(ERROR)
                .assertExceptionForLogMessage("Fatal error occurred when handling a client connection")
                .hasStackTraceContaining("Outbound network buffer has failed to flush within mandated period of");
    }
}
