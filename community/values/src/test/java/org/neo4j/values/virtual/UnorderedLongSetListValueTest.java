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
package org.neo4j.values.virtual;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.virtual.VirtualValues.list;

import org.junit.jupiter.api.Test;
import org.neo4j.memory.EmptyMemoryTracker;

class UnorderedLongSetListValueTest {

    @Test
    void shouldHandleDuplicateValues() {
        var set = setListOf(1, 2, 1, 3);
        assertThat(set.intSize()).isEqualTo(3);
        assertThat(set.contains(1)).isTrue();
        assertThat(set.contains(2)).isTrue();
        assertThat(set.contains(3)).isTrue();
    }

    @Test
    void shouldSupportRandomAccess() {
        var set = setListOf(1, 2, 1, 3);
        assertThat(set.value(0)).isEqualTo(longValue(1));
        assertThat(set.value(1)).isEqualTo(longValue(2));
        assertThat(set.value(2)).isEqualTo(longValue(3));
        assertThatThrownBy(() -> set.value(3)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void shouldDropInSetList() {
        var set = setListOf(1, 2, 1, 3).drop(2);
        var expected = list(longValue(3L));

        assertThat(set).isEqualTo(expected);
        assertThat(set).hasSameHashCodeAs(expected);
    }

    @Test
    void shouldReversSetList() {
        var set = setListOf(1, 2, 1, 3).reverse();

        var expected = list(longValue(3L), longValue(2L), longValue(1L));

        assertThat(set).isEqualTo(expected);
        assertThat(set).hasSameHashCodeAs(expected);
    }

    private UnorderedLongSetListValue setListOf(long... values) {
        var builder = UnorderedLongSetListValue.heapTrackingBuilder(EmptyMemoryTracker.INSTANCE, 16);
        for (long value : values) {
            builder.add(value);
        }
        return builder.build();
    }
}
