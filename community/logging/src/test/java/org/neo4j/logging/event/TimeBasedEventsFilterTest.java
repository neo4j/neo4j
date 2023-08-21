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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.neo4j.time.FakeClock;

class TimeBasedEventsFilterTest {

    private final FakeClock clock = new FakeClock();

    @Test
    void shouldLogIfPeriodHasPassed() {
        var maxPublishPeriod = Duration.ofSeconds(1);
        var timeBasedMiscEventFilter = new TimeBasedLimitedEventFilter(clock, maxPublishPeriod);

        assertTrue(timeBasedMiscEventFilter.canPublish());
        clock.forward(Duration.ofMillis(500));
        assertFalse(timeBasedMiscEventFilter.canPublish());
        clock.forward(Duration.ofMillis(501));
        assertTrue(timeBasedMiscEventFilter.canPublish());
        clock.forward(Duration.ofMillis(500));
        assertFalse(timeBasedMiscEventFilter.canPublish());
        clock.forward(Duration.ofMillis(500));
        assertFalse(timeBasedMiscEventFilter.canPublish());
        clock.forward(Duration.ofMillis(1));
        assertTrue(timeBasedMiscEventFilter.canPublish());
        clock.forward(Duration.ofSeconds(5));
        assertTrue(timeBasedMiscEventFilter.canPublish());
        assertFalse(timeBasedMiscEventFilter.canPublish());
        clock.forward(Duration.ofMillis(800));
        assertFalse(timeBasedMiscEventFilter.canPublish());
        clock.forward(Duration.ofMillis(500));
        assertTrue(timeBasedMiscEventFilter.canPublish());
    }
}
