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
package org.neo4j.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.connectors.ArrowConnectorInternalSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension(configurationCallback = "configure")
public class ArrowServerIT {

    @Inject
    GraphDatabaseAPI db;

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private FlightClient client;

    @BeforeEach
    void setup() {
        this.client = FlightClient.builder()
                .location(Location.forGrpcInsecure(
                        "localhost",
                        ArrowConnectorInternalSettings.listen_address
                                .defaultValue()
                                .getPort()))
                .allocator(new RootAllocator(Long.MAX_VALUE))
                .build();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        this.client.close();
    }

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(ArrowConnectorInternalSettings.enabled, true);
        builder.setInternalLogProvider(logProvider);
    }

    @Test
    void shouldHaveArrowServerRunning() {
        var server = db.getDependencyResolver().resolveDependency(ArrowServer.class);

        assertThat(server).isNotNull();
        assertThat(server.isRunning()).isTrue();
    }

    @Test
    void shouldStopArrowServer() throws Exception {
        var server = db.getDependencyResolver().resolveDependency(ArrowServer.class);

        assertThat(server).isNotNull();
        assertThat(server.isRunning()).isTrue();

        server.stop();

        assertThat(server.isRunning()).isFalse();
    }

    @Test
    void shouldLogArrowServerLifecycleEvents() throws Exception {
        LogAssertions.assertThat(logProvider)
                .forClass(ArrowServer.class)
                .forLevel(AssertableLogProvider.Level.INFO)
                .containsMessages(
                        "Configured Arrow connector with listener address localhost:8791", "Arrow server started");

        var server = db.getDependencyResolver().resolveDependency(ArrowServer.class);
        server.stop();
        server.shutdown();

        LogAssertions.assertThat(logProvider)
                .forClass(ArrowServer.class)
                .forLevel(AssertableLogProvider.Level.INFO)
                .containsMessages("Requested Arrow server shutdown", "Arrow server has been shut down");
    }
}
