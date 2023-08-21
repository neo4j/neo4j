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
package org.neo4j.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.helpers.SocketAddress;

class SettingConstraintsTest {
    @Test
    void invalidAdvertisedAddress() {
        assertThat(assertThrows(IllegalArgumentException.class, () -> Config.newBuilder()
                        .set(GraphDatabaseSettings.default_advertised_address, new SocketAddress("0.0.00.000"))
                        .build()))
                .hasMessageContaining("advertised address cannot be '0.0.0.0'");
        assertThat(assertThrows(IllegalArgumentException.class, () -> Config.newBuilder()
                        .set(GraphDatabaseSettings.default_advertised_address, new SocketAddress("::"))
                        .build()))
                .hasMessageContaining("advertised address cannot be '::'");
    }

    @Test
    void invalidDefaultAddress() {
        assertThat(assertThrows(IllegalArgumentException.class, () -> Config.newBuilder()
                        .set(GraphDatabaseSettings.default_advertised_address, new SocketAddress("localhost", 1234))
                        .build()))
                .hasMessageContaining("can not have a port");
    }

    @Test
    void validDefaultAdvertisedAddress() {
        assertDoesNotThrow(() -> Config.newBuilder()
                .set(GraphDatabaseSettings.default_advertised_address, new SocketAddress("localhost"))
                .build());
    }
}
