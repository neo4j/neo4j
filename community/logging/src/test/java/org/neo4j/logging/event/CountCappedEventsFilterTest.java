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
package org.neo4j.logging.event;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CountCappedEventsFilterTest {

    @Test
    void shouldPublishEveryCount() {
        var countCappedMiscEventFilter = new CountCappedLimitedEventFilter(1);

        assertTrue(countCappedMiscEventFilter.canPublish());
        assertTrue(countCappedMiscEventFilter.canPublish());
        assertTrue(countCappedMiscEventFilter.canPublish());
    }

    @Test
    void shouldPublishEveryThreeCount() {
        var countCappedMiscEventFilter = new CountCappedLimitedEventFilter(3);

        assertTrue(countCappedMiscEventFilter.canPublish());
        assertFalse(countCappedMiscEventFilter.canPublish());
        assertFalse(countCappedMiscEventFilter.canPublish());
        assertTrue(countCappedMiscEventFilter.canPublish());
        assertFalse(countCappedMiscEventFilter.canPublish());
        assertFalse(countCappedMiscEventFilter.canPublish());
        assertTrue(countCappedMiscEventFilter.canPublish());
    }

    @Test
    void shouldNotAllowZero() {
        assertThrows(IllegalArgumentException.class, () -> new CountCappedLimitedEventFilter(0));
    }

    @Test
    void shouldNotAllowNegative() {
        assertThrows(IllegalArgumentException.class, () -> new CountCappedLimitedEventFilter(0));
    }
}
