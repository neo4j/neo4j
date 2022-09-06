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
package org.neo4j.bolt.transport;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.protocol.common.handler.HouseKeeperHandler;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltDefaultWire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@ExtendWith(OtherThreadExtension.class)
public class BoltThrottleMaxDurationIT {
    @Inject
    private Neo4jWithSocket server;

    @Inject
    private OtherThread otherThread;

    private AssertableLogProvider logProvider;

    private HostnamePort address;
    private TransportConnection connection;
    private final BoltWire wire = new BoltDefaultWire();

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        server.setGraphDatabaseFactory(getTestGraphDatabaseFactory());
        server.setConfigure(getSettingsFunction());
        server.init(testInfo);

        otherThread.set(5, TimeUnit.MINUTES);

        address = server.lookupDefaultConnector();
    }

    @AfterEach
    public void cleanup() throws IOException {
        if (connection != null) {
            connection.disconnect();
        }
    }

    protected TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory() {
        TestDatabaseManagementServiceBuilder factory = new TestDatabaseManagementServiceBuilder();

        logProvider = new AssertableLogProvider();

        factory.setInternalLogProvider(logProvider);

        return factory;
    }

    protected static Consumer<Map<Setting<?>, Object>> getSettingsFunction() {
        return settings -> {
            settings.put(
                    BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout, Duration.ofMinutes(5));
            settings.put(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_high_water_mark, (int)
                    ByteUnit.kibiBytes(64));
            settings.put(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_low_water_mark, (int)
                    ByteUnit.kibiBytes(16));
            settings.put(
                    BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_max_duration, Duration.ofSeconds(30));
            settings.put(BoltConnector.encryption_level, OPTIONAL);
        };
    }

    @Test
    public void sendingButNotReceivingClientShouldBeKilledWhenWriteThrottleMaxDurationIsReached() throws Exception {
        var largeString = " ".repeat((int) ByteUnit.kibiBytes(64));

        // Explicitly set the receive buffer size ridiculously small so that we know when it will fill up
        this.connection = new SocketConnection(address)
                .connect()
                .setOption(StandardSocketOptions.SO_RCVBUF, (int) ByteUnit.kibiBytes(32))
                .sendDefaultProtocolVersion()
                .send(wire.hello());

        assertThat(connection).negotiatesDefaultVersion().receivesSuccess();

        var sender = otherThread.execute(() -> {
            // TODO: There seems to be additional buffering going on somewhere thus making this flakey unless we keep
            //       spamming the server until the error is raised
            while (!Thread.interrupted()) {
                connection
                        .send(wire.run("RETURN $data as data", asMapValue(singletonMap("data", largeString))))
                        .send(wire.pull());
            }

            return null;
        });

        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(() -> otherThread.get().awaitFuture(sender))
                .withRootCauseInstanceOf(SocketException.class);

        assertThat(logProvider)
                .forClass(HouseKeeperHandler.class)
                .forLevel(ERROR)
                .assertExceptionForLogMessage("Fatal error occurred when handling a client connection")
                .hasStackTraceContaining("Outbound network buffer has failed to flush within mandated period of");
    }
}
