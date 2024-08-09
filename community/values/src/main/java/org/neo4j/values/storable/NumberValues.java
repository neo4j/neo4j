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
package org.neo4j.values.storable;

import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * Static methods for computing the hashCode of primitive numbers and arrays of primitive numbers.
 * <p>
 * Also compares Value typed number arrays.
 */
@SuppressWarnings("WeakerAccess")
public final class NumberValues {
    private NumberValues() {}

    /*
     * Using the fact that the hashcode ∑x_i * 31^(i-1) can be expressed as
     * a dot product, [1, v_1, v_2, v_2, ..., v_n] • [31^n, 31^{n-1}, ..., 31, 1]. By expressing
     * it in that way the compiler is smart enough to better parallelize the
     * computation of the hash code.
     */
    static final int MAX_LENGTH = 10000;
    private static final int[] COEFFICIENTS = new int[MAX_LENGTH + 1];
    private static final long NON_DOUBLE_LONG = 0xFFE0_0000_0000_0000L; // doubles are exact integers up to 53 bits

    static {
        // We are defining the coefficient vector backwards, [1, 31, 31^2,...]
        // makes it easier and faster do find the starting position later
        COEFFICIENTS[0] = 1;
        for (int i = 1; i <= MAX_LENGTH; ++i) {
            COEFFICIENTS[i] = HASH_CONSTANT * COEFFICIENTS[i - 1];
        }
    }

    /*
     * For equality semantics it is important that the hashcode of a long
     * is the same as the hashcode of an int as long as the long can fit in 32 bits.
     */
    public static int hash(long number) {
        int asInt = (int) number;
        if (asInt == number) {
            return asInt;
        }
        return Long.hashCode(number);
    }

    public static int hash(double number) {
        long asLong = (long) number;
        if (asLong == number) {
            return hash(asLong);
        }
        long bits = Double.doubleToLongBits(number);
        return (int) (bits ^ (bits >>> 32));
    }

    /*
     * This is a slightly silly optimization but by turning the computation
     * of the hashcode into a dot product we trick the jit compiler to use SIMD
     * instructions and performance doubles.
     */
    public static int hash(byte[] values) {
        final int max = Math.min(values.length, MAX_LENGTH);
        int result = COEFFICIENTS[max];
        for (int i = 0; i < values.length && i < COEFFICIENTS.length - 1; ++i) {
            result += COEFFICIENTS[max - i - 1] * values[i];
        }
        return result;
    }

    public static int hash(short[] values) {
        final int max = Math.min(values.length, MAX_LENGTH);
        int result = COEFFICIENTS[max];
        for (int i = 0; i < values.length && i < COEFFICIENTS.length - 1; ++i) {
            result += COEFFICIENTS[max - i - 1] * values[i];
        }
        return result;
    }

    public static int hash(char[] values) {
        final int max = Math.min(values.length, MAX_LENGTH);
        int result = COEFFICIENTS[max];
        for (int i = 0; i < values.length && i < COEFFICIENTS.length - 1; ++i) {
            result += COEFFICIENTS[max - i - 1] * (values[i] + HASH_CONSTANT);
        }
        return result;
    }

    public static int hash(int[] values) {
        final int max = Math.min(values.length, MAX_LENGTH);
        int result = COEFFICIENTS[max];
        for (int i = 0; i < values.length && i < COEFFICIENTS.length - 1; ++i) {
            result += COEFFICIENTS[max - i - 1] * values[i];
        }
        return result;
    }

    public static int hash(long[] values) {
        final int max = Math.min(values.length, MAX_LENGTH);
        int result = COEFFICIENTS[max];
        for (int i = 0; i < values.length && i < COEFFICIENTS.length - 1; ++i) {
            result += COEFFICIENTS[max - i - 1] * NumberValues.hash(values[i]);
        }
        return result;
    }

    public static int hash(float[] values) {
        int result = 1;
        for (float value : values) {
            int elementHash = NumberValues.hash(value);
            result = HASH_CONSTANT * result + elementHash;
        }
        return result;
    }

    public static int hash(double[] values) {
        int result = 1;
        for (double value : values) {
            int elementHash = NumberValues.hash(value);
            result = HASH_CONSTANT * result + elementHash;
        }
        return result;
    }

    public static int hash(boolean[] value) {
        return Arrays.hashCode(value);
    }

    public static boolean numbersEqual(double fpn, long in) {
        if (in < 0) {
            if (fpn < 0.0) {
                if ((NON_DOUBLE_LONG & in) == 0L) // the high order bits are only sign bits
                { // no loss of precision if converting the long to a double, so it's safe to compare as double
                    return fpn == in;
                } else if (fpn < Long.MIN_VALUE) { // the double is too big to fit in a long, they cannot be equal
                    return false;
                } else if ((fpn == Math.floor(fpn)) && !Double.isInfinite(fpn)) // no decimals
                { // safe to compare as long
                    return in == (long) fpn;
                }
            }
        } else {
            if (!(fpn < 0.0)) {
                if ((NON_DOUBLE_LONG & in) == 0L) // the high order bits are only sign bits
                { // no loss of precision if converting the long to a double, so it's safe to compare as double
                    return fpn == in;
                } else if (fpn > Long.MAX_VALUE) { // the double is too big to fit in a long, they cannot be equal
                    return false;
                } else if ((fpn == Math.floor(fpn)) && !Double.isInfinite(fpn)) // no decimals
                { // safe to compare as long
                    return in == (long) fpn;
                }
            }
        }
        return false;
    }

    // Tested by PropertyValueComparisonTest
    public static int compareDoubleAgainstLong(double lhs, long rhs) {
        if ((NON_DOUBLE_LONG & rhs) != 0L) {
            if (Double.isNaN(lhs)) {
                return +1;
            }
            if (Double.isInfinite(lhs)) {
                return lhs < 0 ? -1 : +1;
            }
            return BigDecimal.valueOf(lhs).compareTo(BigDecimal.valueOf(rhs));
        }
        return Double.compare(lhs, rhs);
    }

    // Tested by PropertyValueComparisonTest
    public static int compareLongAgainstDouble(long lhs, double rhs) {
        return -compareDoubleAgainstLong(rhs, lhs);
    }

    public static boolean numbersEqual(IntegralArray lhs, IntegralArray rhs) {
        int length = lhs.intSize();
        if (length != rhs.intSize()) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (lhs.longValue(i) != rhs.longValue(i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean numbersEqual(FloatingPointArray lhs, FloatingPointArray rhs) {
        int length = lhs.intSize();
        if (length != rhs.intSize()) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (lhs.doubleValue(i) != rhs.doubleValue(i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean numbersEqual(FloatingPointArray fps, IntegralArray ins) {
        int length = ins.intSize();
        if (length != fps.intSize()) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (!numbersEqual(fps.doubleValue(i), ins.longValue(i))) {
                return false;
            }
        }
        return true;
    }

    public static int compareIntegerArrays(IntegralArray a, IntegralArray b) {
        int i = 0;
        int x = 0;
        int length = Math.min(a.intSize(), b.intSize());

        while (x == 0 && i < length) {
            x = Long.compare(a.longValue(i), b.longValue(i));
            i++;
        }

        if (x == 0) {
            x = a.intSize() - b.intSize();
        }

        return x;
    }

    public static int compareIntegerVsFloatArrays(IntegralArray a, FloatingPointArray b) {
        int i = 0;
        int x = 0;
        int length = Math.min(a.intSize(), b.intSize());

        while (x == 0 && i < length) {
            x = compareLongAgainstDouble(a.longValue(i), b.doubleValue(i));
            i++;
        }

        if (x == 0) {
            x = a.intSize() - b.intSize();
        }

        return x;
    }

    public static int compareFloatArrays(FloatingPointArray a, FloatingPointArray b) {
        int i = 0;
        int x = 0;
        int length = Math.min(a.intSize(), b.intSize());

        while (x == 0 && i < length) {
            x = Double.compare(a.doubleValue(i), b.doubleValue(i));
            i++;
        }

        if (x == 0) {
            x = a.intSize() - b.intSize();
        }

        return x;
    }

    public static int compareBooleanArrays(BooleanArray a, BooleanArray b) {
        int i = 0;
        int x = 0;
        int length = Math.min(a.intSize(), b.intSize());

        while (x == 0 && i < length) {
            x = Boolean.compare(a.booleanValue(i), b.booleanValue(i));
            i++;
        }

        if (x == 0) {
            x = a.intSize() - b.intSize();
        }

        return x;
    }
}
