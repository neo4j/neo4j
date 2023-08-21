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
package org.neo4j.values.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.neo4j.values.AnyValue;
import org.neo4j.values.Equality;

public class AnyValueTestUtil {
    public static void assertEqual(AnyValue a, AnyValue b) {
        assertEquals(a, b, formatMessage("should be equivalent to", a, b));
        assertEquals(b, a, formatMessage("should be equivalent to", b, a));
        assertEquals(a.ternaryEquals(b), Equality.TRUE, formatMessage("should be equal to", a, b));
        assertEquals(b.ternaryEquals(a), Equality.TRUE, formatMessage("should be equal to", b, a));
        assertEquals(a.hashCode(), b.hashCode(), formatMessage("should have same hashcode as", a, b));
    }

    private static String formatMessage(String should, AnyValue a, AnyValue b) {
        return String.format(
                "%s(%s) %s %s(%s)",
                a.getClass().getSimpleName(), a, should, b.getClass().getSimpleName(), b);
    }

    public static void assertEqualValues(AnyValue a, AnyValue b) {
        assertEquals(a, b, a + " should be equivalent to " + b);
        assertEquals(b, a, a + " should be equivalent to " + b);
        assertEquals(Equality.TRUE, a.ternaryEquals(b), a + " should be equal to " + b);
        assertEquals(Equality.TRUE, b.ternaryEquals(a), a + " should be equal to " + b);
        assertEquals(a.hashCode(), b.hashCode(), a + ".hashCode() should be equivalent to " + b + ".hashCode()");
    }

    public static void assertEqualWithNoValues(AnyValue a, AnyValue b) {
        assertEquals(a, b, a + " should be equivalent to " + b);
        assertEquals(b, a, a + " should be equivalent to " + b);
        assertEquals(Equality.UNDEFINED, a.ternaryEquals(b), a + " should not be equal to " + b);
        assertEquals(Equality.UNDEFINED, b.ternaryEquals(a), a + " should not be equal to " + b);
        assertEquals(a.hashCode(), b.hashCode(), a + ".hashCode() should be equivalent to " + b + ".hashCode()");
    }

    public static void assertNotEqual(AnyValue a, AnyValue b) {
        assertNotEquals(a, b, a + " should not be equivalent to " + b);
        assertNotEquals(b, a, b + " should not be equivalent to " + a);
        assertEquals(Equality.FALSE, a.ternaryEquals(b), a + " should not equal " + b);
        assertEquals(Equality.FALSE, b.ternaryEquals(a), b + " should not equal " + a);
    }

    public static void assertIncomparable(AnyValue a, AnyValue b) {
        assertNotEquals(a, b, a + " should not be equivalent to " + b);
        assertNotEquals(b, a, b + " should not be equivalent to " + a);
        assertEquals(Equality.UNDEFINED, a.ternaryEquals(b), a + " should be incomparable to " + b);
        assertEquals(Equality.UNDEFINED, b.ternaryEquals(a), b + " should be incomparable to " + a);
    }
}
