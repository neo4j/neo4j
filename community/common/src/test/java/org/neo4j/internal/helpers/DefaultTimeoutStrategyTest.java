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
package org.neo4j.internal.helpers;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultTimeoutStrategyTest
{
    @Test
    void shouldFailIfPassedFunctionIsNotIncreasing()
    {
        assertThrows( IllegalArgumentException.class, () -> new DefaultTimeoutStrategy( 0, 100, TimeUnit.MILLISECONDS, i -> i - 1 ) );
    }

    @Test
    void shouldGetAndIncrementCorrectly()
    {
        final var strategy = new DefaultTimeoutStrategy( 0, 100, TimeUnit.MILLISECONDS, i -> i + 1 );
        final var timeout = strategy.newTimeout();
        assertEquals( 0, timeout.getAndIncrement() );
        assertEquals( 1, timeout.getAndIncrement() );
        assertEquals( 2, timeout.getMillis() );

        final var timeout1 = strategy.newTimeout();
        assertEquals( 0, timeout1.getMillis() );
    }
}
