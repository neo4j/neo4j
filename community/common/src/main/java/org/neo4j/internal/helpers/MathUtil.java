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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import org.neo4j.util.Preconditions;

public final class MathUtil {
    public static final double DEFAULT_EPSILON = 1.0E-8;

    private static final long NON_DOUBLE_LONG = 0xFFE0_0000_0000_0000L; // doubles are exact integers up to 53 bits

    private MathUtil() {
        throw new AssertionError();
    }

    /**
     * Clamp the given value between the two given bounds.
     *
     * @param value  the value to clamp
     * @param lowerBound the lower boundary to clamp {@code value}
     * @param upperBound the upper boundary to clamp {@code value}
     * @return {@code value} if {@code value} is in between the bounds, otherwise the closest bound
     * @throws IllegalArgumentException when {@code lowerBound > upperBound}
     */
    public static int clamp(int value, int lowerBound, int upperBound) {
        Preconditions.checkArgument(
                lowerBound <= upperBound,
                "given lower bound, %d; is greater than given upper bound, %d.",
                lowerBound,
                upperBound);
        return Math.max(lowerBound, Math.min(value, upperBound));
    }

    /**
     * Clamp the given value between the two given bounds.
     *
     * @param value the value to clamp
     * @param lowerBound the lower boundary to clamp {@code value}
     * @param upperBound the upper boundary to clamp {@code value}
     * @return {@code value} if {@code value} is in between the bounds, otherwise the closest bound
     * @throws IllegalArgumentException when {@code lowerBound > upperBound}
     */
    public static long clamp(long value, long lowerBound, long upperBound) {
        Preconditions.checkArgument(
                lowerBound <= upperBound,
                "given lower bound, %d; is greater than given upper bound, %d.",
                lowerBound,
                upperBound);
        return Math.max(lowerBound, Math.min(value, upperBound));
    }

    /**
     * Clamp the given value between the two given bounds.
     *
     * @param value the value to clamp
     * @param lowerBound the lower boundary to clamp {@code value}
     * @param upperBound the upper boundary to clamp {@code value}
     * @return {@code value} if {@code value} is in between the bounds, otherwise the closest bound
     * @throws IllegalArgumentException when {@code lowerBound > upperBound}
     */
    public static float clamp(float value, float lowerBound, float upperBound) {
        Preconditions.checkArgument(
                lowerBound <= upperBound,
                "given lower bound, %g; is greater than given upper bound, %g.",
                lowerBound,
                upperBound);
        return Math.max(lowerBound, Math.min(value, upperBound));
    }

    /**
     * Clamp the given value between the two given bounds.
     *
     * @param value the value to clamp
     * @param lowerBound the lower boundary to clamp {@code value}
     * @param upperBound the upper boundary to clamp {@code value}
     * @return {@code value} if {@code value} is in between the bounds, otherwise the closest bound
     * @throws IllegalArgumentException when {@code lowerBound > upperBound}
     */
    public static double clamp(double value, double lowerBound, double upperBound) {
        Preconditions.checkArgument(
                lowerBound <= upperBound,
                "given lower bound, %g; is greater than given upper bound, %g.",
                lowerBound,
                upperBound);
        return Math.max(lowerBound, Math.min(value, upperBound));
    }

    /**
     * Calculates the portion of the first value to all values passed
     * @param n The values in the set
     * @return the ratio of n[0] to the sum all n, 0 if result is {@link Double#NaN}
     */
    public static double portion(double... n) {
        assert n.length > 0;

        double first = n[0];
        if (Math.abs(first) < DEFAULT_EPSILON) {
            return 0d;
        }
        double total = Arrays.stream(n).sum();
        return first / total;
    }

    // Tested by PropertyValueComparisonTest
    public static int compareDoubleAgainstLong(double lhs, long rhs) {
        if ((NON_DOUBLE_LONG & rhs) != NON_DOUBLE_LONG) {
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

    /**
     * Return an integer one less than the given integer, or throw {@link ArithmeticException} if the given integer is
     * zero.
     *
     * @param value integer to decrement
     * @return the provided integer minus one
     * @throws ArithmeticException if the resulting integer would be less than zero
     */
    public static int decrementExactNotPastZero(int value) {
        if (value == 0) {
            throw new ArithmeticException("integer underflow past zero");
        }
        return value - 1;
    }

    /**
     * Compares two numbers given some amount of allowed error.
     */
    public static int compare(double x, double y, double eps) {
        return equals(x, y, eps) ? 0 : x < y ? -1 : 1;
    }

    /**
     * Returns true if both arguments are equal or within the range of allowed error (inclusive)
     */
    public static boolean equals(double x, double y, double eps) {
        return Math.abs(x - y) <= eps;
    }

    public static class CommonToleranceComparator implements Comparator<Double> {
        private final double epsilon;

        public CommonToleranceComparator(double epsilon) {
            this.epsilon = epsilon;
        }

        @Override
        public int compare(Double x, Double y) {
            return MathUtil.compare(x, y, epsilon);
        }
    }
}
