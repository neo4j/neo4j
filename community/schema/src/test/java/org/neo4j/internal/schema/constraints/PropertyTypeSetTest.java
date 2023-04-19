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
package org.neo4j.internal.schema.constraints;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.SchemaValueType;

class PropertyTypeSetTest {
    @Test
    void testOrder() {
        var set = new PropertyTypeSet();
        set.add(SchemaValueType.LIST_BOOLEAN);
        set.add(SchemaValueType.INTEGER);
        set.add(SchemaValueType.BOOLEAN);
        set.add(SchemaValueType.BOOLEAN);
        set.add(SchemaValueType.LIST_BOOLEAN);

        assertThat(set).containsExactly(SchemaValueType.BOOLEAN, SchemaValueType.INTEGER, SchemaValueType.LIST_BOOLEAN);
    }

    @Test
    void testUserDescription() {
        var set = new PropertyTypeSet();
        set.add(SchemaValueType.LIST_BOOLEAN);
        set.add(SchemaValueType.INTEGER);
        set.add(SchemaValueType.BOOLEAN);
        set.add(SchemaValueType.BOOLEAN);
        set.add(SchemaValueType.LIST_BOOLEAN);

        assertThat(set.userDescription()).isEqualTo("BOOLEAN | INTEGER | LIST<BOOLEAN>");
    }

    @Test
    void testEquality() {
        var a = PropertyTypeSet.of(SchemaValueType.BOOLEAN, SchemaValueType.INTEGER);
        var b = PropertyTypeSet.of(SchemaValueType.INTEGER, SchemaValueType.BOOLEAN);
        assertThat(a).isEqualTo(b);

        var c = PropertyTypeSet.of(SchemaValueType.INTEGER, SchemaValueType.STRING);
        assertThat(a).isNotEqualTo(c);
    }
}
