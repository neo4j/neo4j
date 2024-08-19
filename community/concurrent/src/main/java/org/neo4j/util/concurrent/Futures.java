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
package org.neo4j.util.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.neo4j.internal.helpers.Exceptions;

/**
 * Constructors for basic {@link Future} types
 */
public final class Futures {
    private Futures() {}

    /**
     * Combine multiple @{link Future} instances into a single Future
     *
     * @param futures the @{link Future} instances to combine
     * @param <V>     The result type returned by this Future's get method
     * @return A new @{link Future} representing the combination
     */
    @SafeVarargs
    public static <V> Future<List<V>> combine(final Future<? extends V>... futures) {
        return combine(Arrays.asList(futures));
    }

    /**
     * This method guarantees that {@link Future#get()} is called on all futures.
     * If no exception occur, the results from the futures are returned in a list.
     * If exceptions occur, they are all chained together and thrown after looping over all futures.
     *
     * @param futures the {@link Future futures}
     * @param <V> The result type returned by the futures get method.
     * @return A {@link List<V> list} of results.
     * @throws ExecutionException If any of the futures throw.
     */
    public static <V> List<V> getAllResults(Iterable<? extends Future<V>> futures) throws ExecutionException {
        List<V> result =
                futures instanceof Collection ? new ArrayList<>(((Collection<?>) futures).size()) : new ArrayList<>();
        Throwable finalError = null;
        for (Future<V> future : futures) {
            try {
                result.add(future.get());
            } catch (Throwable e) {
                finalError = Exceptions.chain(finalError, e);
            }
        }
        if (finalError != null) {
            throw new ExecutionException(finalError);
        }
        return result;
    }

    public static void getAll(Iterable<? extends Future<?>> futures) throws ExecutionException {
        Throwable finalError = null;
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Throwable e) {
                finalError = Exceptions.chain(finalError, e);
            }
        }
        if (finalError != null) {
            throw new ExecutionException(finalError);
        }
    }

    /**
     * Combine multiple @{link Future} instances into a single Future
     *
     * @param futures the @{link Future} instances to combine
     * @param <V>     The result type returned by this Future's get method
     * @return A new @{link Future} representing the combination
     */
    public static <V> Future<List<V>> combine(final Iterable<? extends Future<? extends V>> futures) {
        return new Future<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean result = false;
                for (Future<? extends V> future : futures) {
                    result |= future.cancel(mayInterruptIfRunning);
                }
                return result;
            }

            @Override
            public boolean isCancelled() {
                boolean result = false;
                for (Future<? extends V> future : futures) {
                    result |= future.isCancelled();
                }
                return result;
            }

            @Override
            public boolean isDone() {
                boolean result = false;
                for (Future<? extends V> future : futures) {
                    result |= future.isDone();
                }
                return result;
            }

            @Override
            public List<V> get() throws InterruptedException, ExecutionException {
                List<V> result = new ArrayList<>();
                for (Future<? extends V> future : futures) {
                    result.add(future.get());
                }
                return result;
            }

            @Override
            public List<V> get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                List<V> result = new ArrayList<>();
                for (Future<? extends V> future : futures) {
                    long before = System.nanoTime();
                    result.add(future.get(timeout, unit));
                    timeout -= unit.convert(System.nanoTime() - before, TimeUnit.NANOSECONDS);
                }
                return result;
            }
        };
    }
}
