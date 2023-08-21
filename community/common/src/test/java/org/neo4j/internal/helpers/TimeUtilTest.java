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
import static org.neo4j.internal.helpers.TimeUtil.nanosToString;
import static org.neo4j.internal.helpers.TimeUtil.parseTimeMillis;

import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

class TimeUtilTest {
    @Test
    void formatNanosToString() {
        assertEquals("1ns", nanosToString(1));
        assertEquals("10ns", nanosToString(10));
        assertEquals("100ns", nanosToString(100));
        assertEquals("1μs", nanosToString(1000));
        assertEquals("10μs100ns", nanosToString(10100));
        assertEquals("101μs10ns", nanosToString(101010));
        assertEquals("10ms101μs10ns", nanosToString(10101010));
        assertEquals("1s20ms304μs50ns", nanosToString(1020304050));
        assertEquals("1m42s30ms405μs60ns", nanosToString(102030405060L));
        assertEquals("2h50m3s40ms506μs70ns", nanosToString(10203040506070L));
        assertEquals("11d19h25m4s50ms607μs80ns", nanosToString(1020304050607080L));
    }

    @Test
    void parseMillisFromString() {
        // Individual units
        assertEquals(0, parseTimeMillis.apply("9ns"));
        assertEquals(0, parseTimeMillis.apply("9μs"));
        assertEquals(0, parseTimeMillis.apply("9us"));
        assertEquals(9, parseTimeMillis.apply("9ms"));
        assertEquals(9000, parseTimeMillis.apply("9s"));
        assertEquals(540000, parseTimeMillis.apply("9m"));
        assertEquals(60 * 60 * 9000, parseTimeMillis.apply("9h"));
        assertEquals(24 * 60 * 60 * 9000, parseTimeMillis.apply("9d"));

        // Combined units
        assertEquals(10, parseTimeMillis.apply("10ms101μs10ns"));
        assertEquals(1020, parseTimeMillis.apply("1s20ms304μs50ns"));
        assertEquals(102030L, parseTimeMillis.apply("1m42s30ms405μs60ns"));
        assertEquals(10203040L, parseTimeMillis.apply("2h50m3s40ms506μs70ns"));
        assertEquals(1020304050L, parseTimeMillis.apply("11d19h25m4s50ms607μs80ns"));

        // Lots of nanos and micos
        assertEquals(5, parseTimeMillis.apply("5050000ns"));
        assertEquals(5, parseTimeMillis.apply("5050μs"));
        assertEquals(5, parseTimeMillis.apply("5050us"));

        // Weird combinations of amounts and units
        assertEquals(3, parseTimeMillis.apply("2000μs1000000ns"));
        assertEquals(60004, parseTimeMillis.apply("1m4000μs"));
        assertEquals(120001, parseTimeMillis.apply("1m60000ms1000000ns"));
        assertEquals(60606, parseTimeMillis.apply("1m0s606ms0μs"));

        // Out of order units (why?!)
        assertEquals(3, parseTimeMillis.apply("1000000ns2000μs"));
        assertEquals(60004, parseTimeMillis.apply("4000μs1m"));
        assertEquals(120001, parseTimeMillis.apply("60000ms1000000ns1m"));

        // Combine small units that would normally be rounded down to zero but add together to make more than a
        // millisecond
        assertEquals(0, parseTimeMillis.apply("999μs"));
        assertEquals(0, parseTimeMillis.apply("999999ns"));
        assertEquals(1, parseTimeMillis.apply("999μs999999ns"));
    }

    @Test
    void nanosToStringAndBackAgain() {
        long maxDuration = 7L * 24 * 60 * 60 * 1000;
        int millisecondInNanos = 1000000;
        assertEquals("7d", nanosToString(maxDuration * millisecondInNanos));

        // 1000 iterations takes <100ms
        for (int i = 0; i < 1000; i++) {
            // Please do not imitate this method of generating random values in a test!
            // org.neo4j.test.rule.RandomRule is much better but cannot be used here without creating a circular
            // dependency.
            long randomMillis = ThreadLocalRandom.current().nextLong(1, maxDuration);
            long nanos = randomMillis * millisecondInNanos;
            assertEquals(nanos, parseTimeMillis.apply(nanosToString(nanos)) * millisecondInNanos);

            assertEquals(
                    parseTimeMillis.apply(nanosToString(nanos)),
                    parseTimeMillis.apply(
                            nanosToString(parseTimeMillis.apply(nanosToString(nanos)) * millisecondInNanos)));
        }
    }
}
