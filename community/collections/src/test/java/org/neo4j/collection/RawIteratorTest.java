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
package org.neo4j.collection;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class RawIteratorTest {
    @Test
    void shouldCreateSimpleRawIterator() {
        assertEquals(Collections.emptyList(), list(RawIterator.of()));
        assertEquals(Collections.singletonList(1), list(RawIterator.of(1)));
        assertEquals(asList(1, 2), list(RawIterator.of(1, 2)));
        assertEquals(asList(1, 2, 3), list(RawIterator.of(1, 2, 3)));
    }

    private static List<Integer> list(RawIterator<Integer, RuntimeException> iter) {
        List<Integer> out = new ArrayList<>();
        while (iter.hasNext()) {
            out.add(iter.next());
        }
        return out;
    }
}
