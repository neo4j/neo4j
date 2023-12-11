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
package org.neo4j.function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.function.ThrowingPredicate.throwingPredicate;
import static org.neo4j.function.ThrowingSupplier.throwingSupplier;

import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Constructors for basic {@link Predicate} types
 */
public final class Predicates {
    public static final IntPredicate ALWAYS_TRUE_INT = v -> true;
    public static final IntPredicate ALWAYS_FALSE_INT = v -> false;
    public static final LongPredicate ALWAYS_TRUE_LONG = v -> true;
    public static final LongPredicate ALWAYS_FALSE_LONG = v -> false;

    private static final int DEFAULT_POLL_INTERVAL = 20;
    private static final int DEFAULT_TIMEOUT_MS = 20_000;

    private Predicates() {}

    public static <T> Predicate<T> alwaysTrue() {
        return x -> true;
    }

    public static <T> Predicate<T> alwaysFalse() {
        return x -> false;
    }

    public static <T> Predicate<T> notNull() {
        return Objects::nonNull;
    }

    @SafeVarargs
    public static <T> Predicate<T> all(final Predicate<T>... predicates) {
        return all(Arrays.asList(predicates));
    }

    public static <T> Predicate<T> all(final Iterable<Predicate<T>> predicates) {
        return item -> {
            for (Predicate<T> predicate : predicates) {
                if (!predicate.test(item)) {
                    return false;
                }
            }
            return true;
        };
    }

    @SafeVarargs
    public static <T> Predicate<T> any(final Predicate<T>... predicates) {
        return any(Arrays.asList(predicates));
    }

    public static <T> Predicate<T> any(final Iterable<Predicate<T>> predicates) {
        return item -> {
            for (Predicate<T> predicate : predicates) {
                if (predicate.test(item)) {
                    return true;
                }
            }
            return false;
        };
    }

    public static <T> Predicate<T> instanceOf(final Class<?> clazz) {
        return clazz::isInstance;
    }

    public static <T> Predicate<T> instanceOfAny(final Class<?>... classes) {
        return item -> {
            if (item != null) {
                for (Class<?> clazz : classes) {
                    if (clazz.isInstance(item)) {
                        return true;
                    }
                }
            }
            return false;
        };
    }

    public static <TYPE> TYPE await(
            Supplier<TYPE> supplier,
            Predicate<TYPE> predicate,
            long timeout,
            TimeUnit timeoutUnit,
            long pollInterval,
            TimeUnit pollUnit)
            throws TimeoutException {
        return awaitEx(supplier::get, predicate::test, timeout, timeoutUnit, pollInterval, pollUnit);
    }

    public static <TYPE> TYPE await(
            Supplier<TYPE> supplier, Predicate<TYPE> predicate, long timeout, TimeUnit timeoutUnit)
            throws TimeoutException {
        return awaitEx(throwingSupplier(supplier), throwingPredicate(predicate), timeout, timeoutUnit);
    }

    public static <TYPE, EXCEPTION extends Exception> TYPE awaitEx(
            ThrowingSupplier<TYPE, EXCEPTION> supplier,
            ThrowingPredicate<TYPE, EXCEPTION> predicate,
            long timeout,
            TimeUnit timeoutUnit,
            long pollInterval,
            TimeUnit pollUnit)
            throws TimeoutException, EXCEPTION {
        Suppliers.ThrowingCapturingSupplier<TYPE, EXCEPTION> composed = Suppliers.compose(supplier, predicate);
        awaitEx(composed, timeout, timeoutUnit, pollInterval, pollUnit);
        return composed.lastInput();
    }

    public static <TYPE, EXCEPTION extends Exception> TYPE awaitEx(
            ThrowingSupplier<TYPE, ? extends EXCEPTION> supplier,
            ThrowingPredicate<TYPE, ? extends EXCEPTION> predicate,
            long timeout,
            TimeUnit timeoutUnit)
            throws TimeoutException, EXCEPTION {
        Suppliers.ThrowingCapturingSupplier<TYPE, EXCEPTION> composed = Suppliers.compose(supplier, predicate);
        awaitEx(composed, timeout, timeoutUnit);
        return composed.lastInput();
    }

    public static void await(BooleanSupplier condition) throws TimeoutException {
        awaitEx(condition::getAsBoolean, DEFAULT_TIMEOUT_MS, MILLISECONDS);
    }

    public static void await(BooleanSupplier condition, long timeout, TimeUnit unit) throws TimeoutException {
        awaitEx(condition::getAsBoolean, timeout, unit);
    }

    public static <EXCEPTION extends Exception> void awaitEx(
            ThrowingSupplier<Boolean, EXCEPTION> condition, long timeout, TimeUnit unit)
            throws TimeoutException, EXCEPTION {
        awaitEx(condition, timeout, unit, DEFAULT_POLL_INTERVAL, MILLISECONDS);
    }

    public static void await(
            BooleanSupplier condition, long timeout, TimeUnit timeoutUnit, long pollInterval, TimeUnit pollUnit)
            throws TimeoutException {
        awaitEx(condition::getAsBoolean, timeout, timeoutUnit, pollInterval, pollUnit);
    }

    public static <EXCEPTION extends Exception> void awaitEx(
            ThrowingSupplier<Boolean, EXCEPTION> condition,
            long timeout,
            TimeUnit unit,
            long pollInterval,
            TimeUnit pollUnit)
            throws TimeoutException, EXCEPTION {
        if (!tryAwaitEx(condition, timeout, unit, pollInterval, pollUnit)) {
            throw new TimeoutException(
                    "Waited for " + timeout + " " + unit + ", but " + condition + " was not accepted.");
        }
    }

    public static <EXCEPTION extends Exception> boolean tryAwaitEx(
            ThrowingSupplier<Boolean, EXCEPTION> condition,
            long timeout,
            TimeUnit timeoutUnit,
            long pollInterval,
            TimeUnit pollUnit)
            throws EXCEPTION {
        return tryAwaitEx(condition, timeout, timeoutUnit, pollInterval, pollUnit, Clock.systemUTC());
    }

    public static <EXCEPTION extends Exception> boolean tryAwaitEx(
            ThrowingSupplier<Boolean, EXCEPTION> condition,
            long timeout,
            TimeUnit timeoutUnit,
            long pollInterval,
            TimeUnit pollUnit,
            Clock clock)
            throws EXCEPTION {
        long deadlineMillis = clock.millis() + timeoutUnit.toMillis(timeout);
        long pollIntervalNanos = pollUnit.toNanos(pollInterval);

        do {
            if (condition.get()) {
                return true;
            }
            LockSupport.parkNanos(pollIntervalNanos);
        } while (clock.millis() < deadlineMillis);
        return false;
    }

    public static void awaitForever(BooleanSupplier condition, long checkInterval, TimeUnit unit) {
        long sleep = unit.toNanos(checkInterval);
        do {
            if (condition.getAsBoolean()) {
                return;
            }
            LockSupport.parkNanos(sleep);
        } while (true);
    }

    @SafeVarargs
    public static <T> Predicate<T> in(final T... allowed) {
        return in(Arrays.asList(allowed));
    }

    public static <T> Predicate<T> in(final Iterable<T> allowed) {
        return item -> {
            for (T allow : allowed) {
                if (allow.equals(item)) {
                    return true;
                }
            }
            return false;
        };
    }

    public static IntPredicate any(int[] values) {
        return v -> {
            for (int value : values) {
                if (v == value) {
                    return true;
                }
            }
            return false;
        };
    }
}
