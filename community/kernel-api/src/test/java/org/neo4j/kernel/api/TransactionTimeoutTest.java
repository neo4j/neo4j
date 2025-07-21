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
package org.neo4j.kernel.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTimedOut;
import static org.neo4j.values.utils.TemporalUtil.NANOS_PER_SECOND;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class TransactionTimeoutTest {

    private static final long SAFE_SECOND_BOUNDARY = (Long.MAX_VALUE / NANOS_PER_SECOND) - 1;

    @Test
    void convertOverflowSafeDuration() {
        var duration = Duration.ofSeconds(SAFE_SECOND_BOUNDARY - 1);
        var timeout = new TransactionTimeout(duration, TransactionTimedOut);

        assertEquals(duration.toNanos(), timeout.timeoutNanos());
    }

    @Test
    void convertOverflowSafeDurationWithNanos() {
        var duration = Duration.ofSeconds(SAFE_SECOND_BOUNDARY - 1).plusNanos(999_999_999);
        var timeout = new TransactionTimeout(duration, TransactionTimedOut);

        assertEquals(duration.toNanos(), timeout.timeoutNanos());
    }

    @Test
    void convertOverflowBoundaryDuration() {
        var duration = Duration.ofSeconds(SAFE_SECOND_BOUNDARY);
        var timeout = new TransactionTimeout(duration, TransactionTimedOut);

        assertEquals(Long.MAX_VALUE, timeout.timeoutNanos());
    }

    @Test
    void convertOverflowUnsafeDuration() {
        Duration duration = Duration.ofSeconds(SAFE_SECOND_BOUNDARY + 2);
        TransactionTimeout timeout = new TransactionTimeout(duration, TransactionTimedOut);

        assertEquals(Long.MAX_VALUE, timeout.timeoutNanos());
        assertThrows(ArithmeticException.class, duration::toNanos);
    }
}
