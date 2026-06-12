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

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Ensures that Bolt correctly handles the {@code ROUTE} messages and returns well-formed results.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExcludeWire(until = @Version(major = 4, minor = 2))
public class RoutingTableIT {

    @Inject
    private Neo4jWithSocket server;

    @SuppressWarnings("unchecked")
    private static void assertRoutingTableHasCorrectShape(Map<String, Object> routingTable) {
        Assertions.assertThat(routingTable).containsKeys("ttl", "servers");
        Assertions.assertThat(routingTable.get("ttl")).isInstanceOf(Long.class);
        Assertions.assertThat(routingTable.get("servers"))
                .isInstanceOf(List.class)
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .allSatisfy(srv -> {
                    Assertions.assertThat(srv).isInstanceOf(Map.class);
                    var server = (Map<String, Object>) srv;
                    Assertions.assertThat(server).containsKeys("role", "addresses");
                    Assertions.assertThat(server.get("role")).isIn("READ", "WRITE", "ROUTE");
                    Assertions.assertThat(server.get("addresses"))
                            .isInstanceOf(List.class)
                            .asInstanceOf(InstanceOfAssertFactories.LIST)
                            .allSatisfy(addr -> Assertions.assertThat(addr).isInstanceOf(String.class));
                });
    }

    @ProtocolTest
    void shouldRespondToRouteMessage(BoltWire wire, @Authenticated BoltTestConnection connection) {
        connection.send(wire.route());

        assertThat(connection)
                .receivesSuccess(metadata -> Assertions.assertThat(metadata)
                        .hasEntrySatisfying(
                                "rt",
                                rt -> Assertions.assertThat(rt)
                                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                                        .satisfies(RoutingTableIT::assertRoutingTableHasCorrectShape)));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    void shouldReturnTheSameRoutingForTwoDifferentUsers(BoltWire wire, @Authenticated BoltTestConnection connection) {
        // Send the routing message and assure it is as intended
        connection.send(wire.route(null, null, "neo4j"));
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(metadata -> Assertions.assertThat(metadata)
                        .hasEntrySatisfying(
                                "rt",
                                rt -> Assertions.assertThat(rt)
                                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                                        .satisfies(RoutingTableIT::assertRoutingTableHasCorrectShape)));

        // add a new user
        connection.send(wire.run("CREATE USER neo4j2 SET PASSWORD 'neo4jneo4jneo4j' CHANGE NOT REQUIRED"));
        connection.send(wire.pull());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        // Logoff
        connection.send(wire.logoff());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        // Logback on as a different user
        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j2",
                "credentials", "neo4jneo4jneo4j")));
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        // Send another route and ensure that it is also correct and the same.
        connection.send(wire.route(null, null, "neo4j"));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(metadata -> Assertions.assertThat(metadata)
                        .hasEntrySatisfying(
                                "rt",
                                rt -> Assertions.assertThat(rt)
                                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                                        .satisfies(RoutingTableIT::assertRoutingTableHasCorrectShape)));
    }

    @ProtocolTest
    void shouldRespondToRouteMessageWithBookmark(BoltWire wire, @Authenticated BoltTestConnection connection) {
        connection.send(wire.route(null, List.of("test-bookmark"), null));

        assertThat(connection)
                .receivesSuccess(metadata -> Assertions.assertThat(metadata)
                        .hasEntrySatisfying(
                                "rt",
                                rt -> Assertions.assertThat(rt)
                                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                                        .satisfies(RoutingTableIT::assertRoutingTableHasCorrectShape)));
    }

    @ProtocolTest
    void shouldReturnFailureIfRoutingTableFailedToReturn(BoltWire wire, @Authenticated BoltTestConnection connection) {
        connection.send(wire.route(null, null, "DOESNT_EXIST!"));
        assertThat(connection).receivesFailure();

        connection.send(wire.reset());
        assertThat(connection).receivesSuccess();

        connection.send(wire.route());
        assertThat(connection)
                .receivesSuccess(metadata -> Assertions.assertThat(metadata)
                        .hasEntrySatisfying(
                                "rt",
                                rt -> Assertions.assertThat(rt)
                                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                                        .satisfies(RoutingTableIT::assertRoutingTableHasCorrectShape)));
    }

    @ProtocolTest
    void shouldIgnoreRouteMessageWhenInFailedState(BoltWire wire, @Authenticated BoltTestConnection connection) {
        connection.send(wire.run("✨✨✨ Magical Crash String ✨✨✨"));
        assertThat(connection).receivesFailure();

        connection.send(wire.route());
        assertThat(connection).receivesIgnored();
    }
}
