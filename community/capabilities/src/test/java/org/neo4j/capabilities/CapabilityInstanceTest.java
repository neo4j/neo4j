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

import org.junit.jupiter.api.Test;

class CapabilityInstanceTest {
    @Test
    void testCapabilityInstance() {
        var capability = new Capability<>(Name.of("dbms.cypher.can_create_user"), Type.BOOLEAN);
        var instance = new CapabilityInstance<>(capability);

        assertThat(instance.capability()).isEqualTo(capability);

        assertThat(instance.get()).isNull();

        instance.set(true);
        assertThat(instance.get()).isTrue();

        instance.supply(() -> false);
        assertThat(instance.get()).isFalse();
    }
}
