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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class DefaultIntervalStrategyTest {
    @Test
    void shouldFailIfPassedFunctionIsNotIncreasing() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DefaultIntervalStrategy(0, 100, TimeUnit.MILLISECONDS, i -> i - 1));
    }

    @Test
    void shouldGetAndIncrementCorrectly() {
        final var strategy = new DefaultIntervalStrategy(0, 100, TimeUnit.MILLISECONDS, i -> i + 1);
        final var backoff = strategy.newIntervalProvider();
        assertEquals(0, backoff.getAndIncrement());
        assertEquals(1, backoff.getAndIncrement());
        assertEquals(2, backoff.getMillis());

        final var backoff1 = strategy.newIntervalProvider();
        assertEquals(0, backoff1.getMillis());
    }
}
