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

import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.bolt.runtime.throttle.ChannelReadThrottleHandler;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.setup.FactoryFunction;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.util.ServerUtil;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Ensures that Bolt throttles incoming messages when the configured watermarks are exceeded.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class ReadThrottleIT {

    private final AssertableLogProvider internalLogProvider = new AssertableLogProvider();

    @Inject
    private Neo4jWithSocket server;

    @FactoryFunction
    void customizeDatabase(TestDatabaseManagementServiceBuilder factory) {
        factory.setInternalLogProvider(this.internalLogProvider);
    }

    @SettingsFunction
    static void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(BoltConnectorInternalSettings.bolt_inbound_message_throttle_high_water_mark, 8);
        settings.put(BoltConnectorInternalSettings.bolt_inbound_message_throttle_low_water_mark, 3);
    }

    @BeforeEach
    void prepare() throws ProcedureException {
        ServerUtil.installSleepProcedure(this.server);
    }

    @AfterEach
    void cleanup() {
        this.internalLogProvider.clear();
    }

    @TransportTest
    void largeNumberOfSlowRunningJobsShouldChangeAutoReadState(
            BoltWire wire, @Authenticated TransportConnection connection) throws IOException {
        var numberOfRunDiscardPairs = 20;

        // when
        for (int i = 0; i < numberOfRunDiscardPairs; i++) {
            connection.send(wire.run("CALL boltissue.sleep(50)")).send(wire.discard());
        }

        // expect
        for (int i = 0; i < numberOfRunDiscardPairs; i++) {
            BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);
        }

        LogAssertions.assertThat(internalLogProvider)
                .forClass(ChannelReadThrottleHandler.class)
                .forLevel(WARN)
                .containsMessages("Disabling message processing");
        LogAssertions.assertThat(internalLogProvider)
                .forClass(ChannelReadThrottleHandler.class)
                .forLevel(INFO)
                .containsMessages("Enabling message processing");
    }
}
