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
package org.neo4j.server.rest.discovery;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.server.configuration.ServerSettings;

class DiscoverableURIsTest {
    private final BiConsumer<String, String> consumer = mock(BiConsumer.class);
    private final ConnectorPortRegister portRegister = mock(ConnectorPortRegister.class);

    @Test
    void shouldNotInvokeConsumerWhenEmpty() {
        DiscoverableURIs empty = new DiscoverableURIs.Builder(null).build();

        empty.forEach(consumer);

        verify(consumer, never()).accept(anyString(), any());
    }

    @Test
    void shouldInvokeConsumerForEachKey() {
        var discoverables = new DiscoverableURIs.Builder(null)
                .addEndpoint("a", "/test")
                .addEndpoint("b", "/data")
                .addEndpoint("c", "/{name}/data")
                .build();

        discoverables.forEach(consumer);

        verify(consumer).accept("a", "/test");
        verify(consumer).accept("b", "/data");
        verify(consumer).accept("c", "/{name}/data");
    }

    @Test
    void shouldSetBoltPort() {
        var config = configWithBoltEnabled();
        var discoverables = new DiscoverableURIs.Builder(null)
                .addBoltEndpoint(config, portRegister)
                .build();

        discoverables.forEach(consumer);

        verify(consumer).accept("bolt_direct", "bolt://localhost:7687");
        verify(consumer).accept("bolt_routing", "neo4j://localhost:7687");
    }

    @Test
    void shouldLookupBoltPort() {
        var config = Config.newBuilder()
                .set(Map.of(BoltConnector.enabled, true, BoltConnector.advertised_address, new SocketAddress(0)))
                .build();
        var register = new ConnectorPortRegister();
        register.register(ConnectorType.BOLT, new InetSocketAddress(1337));

        var discoverables = new DiscoverableURIs.Builder(null)
                .addBoltEndpoint(config, register)
                .build();

        discoverables.forEach(consumer);

        verify(consumer).accept("bolt_direct", "bolt://localhost:1337");
        verify(consumer).accept("bolt_routing", "neo4j://localhost:1337");
    }

    @Test
    void shouldSetBoltHostAndPortWithDefaultAdvertisedAddress() {
        var config = Config.newBuilder()
                .set(Map.of(
                        BoltConnector.enabled,
                        true,
                        GraphDatabaseSettings.default_advertised_address,
                        new SocketAddress("myCat.com")))
                .build();
        var discoverables = new DiscoverableURIs.Builder(null)
                .addBoltEndpoint(config, portRegister)
                .build();

        discoverables.forEach(consumer);

        verify(consumer).accept("bolt_direct", "bolt://myCat.com:7687");
        verify(consumer).accept("bolt_routing", "neo4j://myCat.com:7687");
    }

    @Test
    void shouldNotSetBoltHostWhenHostIsNotExplicitlySet() {
        var config = Config.newBuilder()
                .set(Map.of(BoltConnector.enabled, true, BoltConnector.advertised_address, new SocketAddress(1234)))
                .build();
        var discoverables = new DiscoverableURIs.Builder(null)
                .addBoltEndpoint(config, portRegister)
                .build();

        discoverables.forEach(consumer);

        verify(consumer).accept("bolt_direct", "bolt://localhost:1234");
        verify(consumer).accept("bolt_routing", "neo4j://localhost:1234");
    }

    @Test
    void shouldSetBoltHostWhenHostIsExplicitlySet() {
        var config = Config.newBuilder()
                .set(Map.of(
                        BoltConnector.enabled,
                        true,
                        BoltConnector.advertised_address,
                        new SocketAddress("myCat.com", 1234)))
                .build();
        var discoverables = new DiscoverableURIs.Builder(null)
                .addBoltEndpoint(config, portRegister)
                .build();

        discoverables.forEach(consumer);

        verify(consumer).accept("bolt_direct", "bolt://myCat.com:1234");
        verify(consumer).accept("bolt_routing", "neo4j://myCat.com:1234");
    }

    @Test
    void shouldOverrideBoltHostWhenHostIsExplicitlySet() {
        var config = Config.newBuilder()
                .set(Map.of(
                        BoltConnector.enabled, true,
                        GraphDatabaseSettings.default_advertised_address, new SocketAddress("myDog.com"),
                        BoltConnector.advertised_address, new SocketAddress("myCat.com", 1234)))
                .build();
        var discoverables = new DiscoverableURIs.Builder(null)
                .addBoltEndpoint(config, portRegister)
                .build();

        discoverables.forEach(consumer);

        verify(consumer).accept("bolt_direct", "bolt://myCat.com:1234");
        verify(consumer).accept("bolt_routing", "neo4j://myCat.com:1234");
    }

    @Test
    void shouldOverrideBoltEndpoints() {
        var config = Config.newBuilder()
                .set(Map.of(
                        BoltConnector.enabled,
                        true,
                        GraphDatabaseSettings.default_advertised_address,
                        new SocketAddress("myDog.com"), // ignored
                        BoltConnector.advertised_address,
                        new SocketAddress("myCat.com", 1234), // ignored
                        ServerSettings.bolt_discoverable_address,
                        URI.create("dog://myDog.com"),
                        ServerSettings.bolt_routing_discoverable_address,
                        URI.create("cat://myCat.com")))
                .build();

        var discoverables = new DiscoverableURIs.Builder(null)
                .addBoltEndpoint(config, portRegister)
                .build();

        discoverables.forEach(consumer);

        verify(consumer).accept("bolt_direct", "dog://myDog.com");
        verify(consumer).accept("bolt_routing", "cat://myCat.com");
    }

    @Test
    void shouldUpdateAllUnsetFields() {
        var config = Config.newBuilder()
                .set(Map.of(
                        BoltConnector.enabled,
                        true,
                        ServerSettings.bolt_discoverable_address,
                        URI.create("dog://myDog.com")))
                .build();
        var discoverables = new DiscoverableURIs.Builder(null)
                .addEndpoint("a", "/test")
                .addEndpoint("b", "/{name}/data")
                .addBoltEndpoint(config, portRegister)
                .build();

        discoverables.update(URI.create("cat://myCat.com:1234"));

        discoverables.forEach(consumer);

        verify(consumer).accept("a", "cat://myCat.com:1234/test");
        verify(consumer).accept("b", "cat://myCat.com:1234/{name}/data");
        verify(consumer).accept("bolt_direct", "dog://myDog.com");
        verify(consumer).accept("bolt_routing", "neo4j://myCat.com:7687");
    }

    private static Config configWithBoltEnabled() {
        return Config.newBuilder().set(BoltConnector.enabled, true).build();
    }
}
