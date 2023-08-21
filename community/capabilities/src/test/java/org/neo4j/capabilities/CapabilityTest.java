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
package org.neo4j.capabilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.capabilities.Type.STRING;

import org.junit.jupiter.api.Test;

class CapabilityTest {

    @Test
    void testCapability() {
        var capability = new Capability<>(Name.of("dbms.test.capability"), STRING);

        assertThat(capability.name()).isEqualTo(Name.of("dbms.test.capability"));
        assertThat(capability.type()).isEqualTo(STRING);
        assertThat(capability.internal()).isTrue();
        assertThat(capability.description()).isEmpty();

        capability.setDescription("a description");
        capability.setPublic();

        assertThat(capability.description()).isEqualTo("a description");
        assertThat(capability.internal()).isFalse();
    }
}
