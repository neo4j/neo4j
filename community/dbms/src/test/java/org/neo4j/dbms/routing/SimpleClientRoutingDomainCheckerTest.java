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
package org.neo4j.dbms.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.helpers.SocketAddressParser;
import org.neo4j.logging.NullLogProvider;

class SimpleClientRoutingDomainCheckerTest {

    private ClientRoutingDomainChecker checkerFromConfig(Config config) {
        return SimpleClientRoutingDomainChecker.fromConfig(config, NullLogProvider.getInstance());
    }

    @Test
    void fromConfigTest() {
        var config = Config.newBuilder()
                .set(
                        GraphDatabaseSettings.client_side_router_enforce_for_domains,
                        Set.of("a-domain.com", "*.my-domain.com", "*.my-other-domain.com:*****"))
                .build();

        var domainChecker = checkerFromConfig(config);

        assertShouldGetClientRouting(
                domainChecker,
                "a-domain.com",
                "a-domain.com:17867",
                "foo.my-domain.com",
                "foo.my-domain.com:7687",
                "foo.my-other-domain.com:17867");

        assertShouldNotGetClientRouting(
                domainChecker,
                "my-domain.com",
                "foo.a-domain.com",
                "foo.a-domain.com:7687",
                "foo.my-other-domain.com",
                "foo.my-other-domain.com:7687");
    }

    private static void assertShouldGetClientRouting(
            ClientRoutingDomainChecker domainChecker, String... expectedDomains) {
        assertThat(expectedDomains).allSatisfy(s -> assertThat(domainChecker.shouldGetClientRouting(
                        SocketAddressParser.socketAddress(s, 7687, SocketAddress::new)))
                .as("should get client routing")
                .isTrue());
    }

    private static void assertShouldNotGetClientRouting(
            ClientRoutingDomainChecker domainChecker, String... expectedDomains) {
        assertThat(expectedDomains).allSatisfy(s -> assertThat(domainChecker.shouldGetClientRouting(
                        SocketAddressParser.socketAddress(s, 7687, SocketAddress::new)))
                .as("should NOT get client routing")
                .isFalse());
    }

    @Test
    void shouldRespondToConfigChanges() {
        // given
        String clientRoutingDomain = "foo.com";

        Config config = Config.defaults();
        var checker = checkerFromConfig(config);
        SocketAddress socketAddress = SocketAddressParser.socketAddress(clientRoutingDomain, SocketAddress::new);

        // then
        assertTrue(checker.isEmpty());
        assertFalse(checker.shouldGetClientRouting(socketAddress));

        // when
        config.setDynamic(
                GraphDatabaseSettings.client_side_router_enforce_for_domains,
                Set.of(clientRoutingDomain),
                this.getClass().getName());

        // then
        assertFalse(checker.isEmpty());
        assertTrue(checker.shouldGetClientRouting(socketAddress));

        // when
        config.setDynamic(
                GraphDatabaseSettings.client_side_router_enforce_for_domains,
                Set.of(),
                this.getClass().getName());

        // then
        assertTrue(checker.isEmpty());
        assertFalse(checker.shouldGetClientRouting(socketAddress));
    }
}
