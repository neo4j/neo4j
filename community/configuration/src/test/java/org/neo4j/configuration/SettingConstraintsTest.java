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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.helpers.SocketAddress;

class SettingConstraintsTest {
    @Test
    void invalidAdvertisedAddress() {
        assertThatThrownBy(() -> Config.newBuilder()
                        .set(GraphDatabaseSettings.default_advertised_address, new SocketAddress("0.0.00.000"))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("advertised address cannot be '0.0.0.0'");

        assertThatThrownBy(() -> Config.newBuilder()
                        .set(GraphDatabaseSettings.default_advertised_address, new SocketAddress("::"))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("advertised address cannot be '::'");
    }

    @Test
    void invalidDefaultAddress() {
        assertThatThrownBy(() -> Config.newBuilder()
                        .set(GraphDatabaseSettings.default_advertised_address, new SocketAddress("localhost", 1234))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("can not have a port");
    }

    @Test
    void validDefaultAdvertisedAddress() {
        assertThatCode(() -> Config.newBuilder()
                        .set(GraphDatabaseSettings.default_advertised_address, new SocketAddress("localhost"))
                        .build())
                .doesNotThrowAnyException();
    }
}
