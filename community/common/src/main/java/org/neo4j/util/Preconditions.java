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
package org.neo4j.util;

import static java.lang.String.format;
import static org.neo4j.internal.helpers.Numbers.isPowerOfTwo;

import java.util.Arrays;

/**
 * A set of static convenience methods for checking ctor/method parameters or state.
 */
public final class Preconditions {
    private Preconditions() {
        throw new AssertionError("no instances");
    }

    /**
     * Ensures that {@code value} is greater than or equal to {@code 1} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's greater than or equal to {@code 1}
     * @throws IllegalArgumentException if {@code value} is less than 1
     */
    public static long requirePositive(long value) {
        if (value < 1) {
            throw new IllegalArgumentException("Expected positive long value, got " + value);
        }
        return value;
    }

    /**
     * Ensures that {@code value} is smaller than or equal to {@code -1} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's smaller than or equal to {@code -1}
     * @throws IllegalArgumentException if {@code value} is greater than -1
     */
    public static long requireNegative(long value) {
        if (value > -1) {
            throw new IllegalArgumentException("Expected negative long value, got " + value);
        }
        return value;
    }

    /**
     * Ensures that {@code value} is a power of 2 or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's a power of 2
     * @throws IllegalArgumentException if {@code value} is not power of 2
     */
    public static long requirePowerOfTwo(long value) {
        if (!isPowerOfTwo(value)) {
            throw new IllegalArgumentException("Expected long value to be a non zero power of 2, got " + value);
        }
        return value;
    }

    /**
     * Ensures that {@code value} is greater than or equal to {@code 1} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's greater than or equal to {@code 1}
     * @throws IllegalArgumentException if {@code value} is less than 1
     */
    public static int requirePositive(int value) {
        if (value < 1) {
            throw new IllegalArgumentException("Expected positive int value, got " + value);
        }
        return value;
    }

    /**
     * Ensures that {@code value} is smaller than or equal to {@code -1} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's smaller than or equal to {@code -1}
     * @throws IllegalArgumentException if {@code value} is greater than -1
     */
    public static int requireNegative(int value) {
        if (value > -1) {
            throw new IllegalArgumentException("Expected negative int value, got " + value);
        }
        return value;
    }

    /**
     * Ensures that {@code value} is greater than or equal to {@code 0} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's greater than or equal to {@code 0}
     * @throws IllegalArgumentException if {@code value} is less than 0
     */
    public static long requireNonNegative(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Expected non-negative long value, got " + value);
        }
        return value;
    }

    /**
     * Ensures that {@code value} is greater than or equal to {@code 0} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's greater than or equal to {@code 0}
     * @throws IllegalArgumentException if {@code value} is less than 0
     */
    public static int requireNonNegative(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("Expected non-negative int value, got " + value);
        }
        return value;
    }

    /**
     * Ensures that {@code value} is exactly zero.
     *
     * @param value a value for check
     * @return {@code value} if it's equal to {@code 0}.
     * @throws IllegalArgumentException if {@code value} is not 0
     */
    public static int requireExactlyZero(int value) {
        if (value != 0) {
            throw new IllegalArgumentException("Expected int value equal to 0, got " + value);
        }
        return value;
    }

    /**
     * Ensures that {@code value} is not {@code null} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @param message error message for the exception
     * @return {@code value} if it's not {@code null}
     * @throws IllegalArgumentException if {@code value} is {@code null}
     */
    public static <T> T requireNonNull(T value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    /**
     * Ensures that {@code array} is not empty
     * @param array array to check
     * @param <T> type of elements in the array
     */
    public static <T> void requireNonEmpty(T[] array) {
        if (array == null || array.length == 0) {
            throw new IllegalArgumentException("Expected non empty array, got " + Arrays.toString(array));
        }
    }

    /**
     * Ensures that {@code array} does not contain null elements
     * @param array array to check
     * @param <T> type of elements in the array
     */
    public static <T> void requireNoNullElements(T[] array) {
        for (T element : array) {
            if (element == null) {
                throw new IllegalArgumentException(
                        "Expected array without null elements, got " + Arrays.toString(array));
            }
        }
    }

    /**
     * Ensures that {@code expression} is {@code true} or throws {@link IllegalStateException} otherwise.
     *
     * @param expression an expression for check
     * @param message error message for the exception
     * @throws IllegalStateException if {@code expression} is {@code false}
     */
    public static void checkState(boolean expression, String message) {
        if (!expression) {
            throw new IllegalStateException(message);
        }
    }

    /**
     * Ensures that {@code expression} is {@code true} or throws {@link IllegalStateException} otherwise.
     *
     * @param expression an expression for check
     * @param message error message format
     * @param args arguments referenced by the error message format
     * @throws IllegalStateException if {@code expression} is {@code false}
     */
    public static void checkState(boolean expression, String message, Object... args) {
        if (!expression) {
            throw new IllegalStateException(args.length > 0 ? format(message, args) : message);
        }
    }

    /**
     * Ensures that {@code expression} is {@code true} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param expression an expression for check
     * @param message error message
     * @throws IllegalArgumentException if {@code expression} is {@code false}
     */
    public static void checkArgument(boolean expression, String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Ensures that {@code expression} is {@code true} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param expression an expression for check
     * @param message error message format
     * @param args arguments referenced by the error message format
     * @throws IllegalArgumentException if {@code expression} is {@code false}
     */
    public static void checkArgument(boolean expression, String message, Object... args) {
        if (!expression) {
            throw new IllegalArgumentException(args.length > 0 ? format(message, args) : message);
        }
    }

    public static void requireBetween(int value, int lowInclusive, int highExclusive) {
        if (value < lowInclusive || value >= highExclusive) {
            throw new IllegalArgumentException(String.format(
                    "Expected int value between %d (inclusive) and %d (exclusive), got %d.",
                    lowInclusive, highExclusive, value));
        }
    }

    public static void requireNoLongAddOverflow(long a, long b, String message) {
        long result = a + b;
        // Same Hackers Delight algorithm as is used in Math.addExact.
        if (((a ^ result) & (b ^ result)) < 0) {
            throw new IllegalArgumentException(String.format(message, a, b));
        }
    }

    /**
     * Ensures that {@code value} is a multiple of {@code multiple} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param valueName name of the value variable.
     * @param value to check.
     * @param multipleName name of the multiple variable.
     * @param multiple the {@code value} must be.
     */
    public static void requireMultipleOf(String valueName, long value, String multipleName, long multiple) {
        if (value % multiple != 0) {
            throw new IllegalArgumentException(
                    String.format("%s(%d) must be multiple of %s(%d)", valueName, value, multipleName, multiple));
        }
    }
}
