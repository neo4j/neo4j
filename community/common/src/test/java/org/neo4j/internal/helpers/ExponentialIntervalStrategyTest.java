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
package org.neo4j.internal.helpers;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.DefaultIntervalStrategy.exponential;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.IntervalStrategy.IntervalProvider;

class ExponentialIntervalStrategyTest {
    private static final int NUMBER_OF_ACCESSES = 5;

    @Test
    void shouldDoubleEachTime() {
        // given
        IntervalStrategy strategy = exponential(1, 1 << NUMBER_OF_ACCESSES, MILLISECONDS);
        IntervalProvider backoff = strategy.newIntervalProvider();

        // when
        for (int i = 0; i < NUMBER_OF_ACCESSES; i++) {
            backoff.increment();
        }

        // then
        assertEquals(1 << NUMBER_OF_ACCESSES, backoff.getMillis());
    }

    @Test
    void shouldProvidePreviousTimeout() {
        // given
        IntervalStrategy strategy = exponential(1, 1 << NUMBER_OF_ACCESSES, MILLISECONDS);
        IntervalProvider backoff = strategy.newIntervalProvider();

        // when
        for (int i = 0; i < NUMBER_OF_ACCESSES; i++) {
            backoff.increment();
        }

        // then
        assertEquals(1 << NUMBER_OF_ACCESSES, backoff.getMillis());
    }

    @Test
    void shouldRespectUpperBound() {
        // given
        long upperBound = (1 << NUMBER_OF_ACCESSES) - 5;

        IntervalStrategy strategy = exponential(1, upperBound, MILLISECONDS);
        IntervalProvider backoff = strategy.newIntervalProvider();

        // when
        for (int i = 0; i < NUMBER_OF_ACCESSES; i++) {
            backoff.increment();
        }

        assertEquals(upperBound, backoff.getMillis());

        // additional increments
        backoff.increment();
        backoff.increment();
        backoff.increment();

        // then
        assertEquals(upperBound, backoff.getMillis());
    }
}
