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

import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;

import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Values;

/**
 * Helper methods for doing math on Values
 */
public final class ValueMath {
    public static final int HASH_CONSTANT = 31;

    private ValueMath() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    /**
     * Overflow safe addition of two longs
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static LongValue add(long a, long b) {
        return longValue(Math.addExact(a, b));
    }

    /**
     * Overflow safe addition of two number values.
     *
     * Will not overflow but instead widen the type as necessary.
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static NumberValue overflowSafeAdd(NumberValue a, NumberValue b) {
        if (a instanceof IntegralValue && b instanceof IntegralValue) {
            return overflowSafeAdd(a.longValue(), b.longValue());
        } else {
            return a.plus(b);
        }
    }

    /**
     * Overflow safe addition of two longs
     * <p>
     * If the result doesn't fit in a long we widen type to use double instead.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static NumberValue overflowSafeAdd(long a, long b) {
        long r = a + b;
        // Check if result overflows
        if (((a ^ r) & (b ^ r)) < 0) {
            return Values.doubleValue((double) a + (double) b);
        }
        return longValue(r);
    }

    /**
     * Addition of two doubles
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static DoubleValue add(double a, double b) {
        return Values.doubleValue(a + b);
    }

    /**
     * Overflow safe subtraction of two longs
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a - b
     */
    public static LongValue subtract(long a, long b) {
        return longValue(Math.subtractExact(a, b));
    }

    /**
     * Overflow safe subtraction of two longs
     * <p>
     * If the result doesn't fit in a long we widen type to use double instead.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a - b
     */
    public static NumberValue overflowSafeSubtract(long a, long b) {
        long r = a - b;
        // Check if result overflows
        if (((a ^ b) & (a ^ r)) < 0) {
            return Values.doubleValue((double) a - (double) b);
        }
        return longValue(r);
    }

    /**
     * Subtraction of two doubles
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a - b
     */
    public static DoubleValue subtract(double a, double b) {
        return Values.doubleValue(a - b);
    }

    /**
     * Overflow safe multiplication of two longs
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a * b
     */
    public static LongValue multiply(long a, long b) {
        return longValue(Math.multiplyExact(a, b));
    }

    /**
     * Overflow safe multiplication of two longs
     * <p>
     * If the result doesn't fit in a long we widen type to use double instead.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a * b
     */
    public static NumberValue overflowSafeMultiply(long a, long b) {
        long r = a * b;
        // Check if result overflows
        long aa = Math.abs(a);
        long ab = Math.abs(b);
        if ((aa | ab) >>> 31 != 0) {
            if (((b != 0) && (r / b != a)) || (a == Long.MIN_VALUE && b == -1)) {
                return doubleValue((double) a * (double) b);
            }
        }
        return longValue(r);
    }

    /**
     * Multiplication of two doubles
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a * b
     */
    public static DoubleValue multiply(double a, double b) {
        return Values.doubleValue(a * b);
    }

    private static boolean oppositeInfinites(double a, double b) {
        return a == Double.POSITIVE_INFINITY && b == Double.NEGATIVE_INFINITY
                || a == Double.NEGATIVE_INFINITY && b == Double.POSITIVE_INFINITY;
    }

    /** Calculates an incremental average taking into account NaN and +/-Infinity */
    public static NumberValue incrementalAverage(NumberValue runningAverage, NumberValue newNumber, double weight) {
        double newDbl = newNumber.doubleValue();
        double runningDbl = runningAverage.doubleValue();

        if (Double.isNaN(runningDbl) || Double.isNaN(newDbl)) {
            return Values.NaN;
        }

        boolean newInfinite = Double.isInfinite(newDbl);
        boolean runningInfinite = Double.isInfinite(runningDbl);

        if (!newInfinite && !runningInfinite) {
            var diff = newNumber.minus(runningAverage);
            var next = diff.dividedBy(weight);
            return overflowSafeAdd(runningAverage, next);
        }

        if (newInfinite && !runningInfinite) {
            return newNumber;
        }

        if (!newInfinite) {
            return runningAverage;
        }

        if (oppositeInfinites(newDbl, runningDbl)) {
            return Values.NaN;
        }

        return runningAverage;
    }
}
