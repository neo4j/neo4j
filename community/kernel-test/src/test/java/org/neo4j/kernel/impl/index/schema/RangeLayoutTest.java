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
package org.neo4j.kernel.impl.index.schema;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RangeLayoutTest {
    @Test
    void shouldHaveUniqueIdentifierForDifferentNumberOfSlots() {
        Map<Long, Integer> layouts = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            final RangeLayout rangeLayout = new RangeLayout(i);
            final Integer previous = layouts.put(rangeLayout.identifier(), i);
            assertNull(
                    previous,
                    String.format(
                            "Expected identifier to be unique for layout with different number of slots, but two had the same identifier, "
                                    + "firstSlotCount=%s, secondSlotCount=%s.",
                            previous, i));
        }
    }
}
