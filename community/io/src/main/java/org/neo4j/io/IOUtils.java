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
package org.neo4j.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.function.ThrowingPredicate;

/**
 * IO helper methods.
 */
public final class IOUtils {
    private IOUtils() {}

    /**
     * Closes given {@link Collection collection} of {@link AutoCloseable closeables}.
     *
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     * @throws IOException if an exception was thrown by one of the close methods.
     * @see #closeAll(AutoCloseable[])
     */
    public static <T extends AutoCloseable> void closeAll(Collection<T> closeables) throws IOException {
        close(IOException::new, closeables);
    }

    /**
     * Close all the provided {@link AutoCloseable closeables}, chaining exceptions, if any, into a single {@link UncheckedIOException}.
     *
     * @param closeables to call close on.
     * @param <T> the type of closeable.
     * @throws UncheckedIOException if any exception is thrown from any of the {@code closeables}.
     */
    public static <T extends AutoCloseable> void closeAllUnchecked(Collection<T> closeables) {
        try {
            closeAll(closeables);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Close all the provided {@link AutoCloseable closeables}, chaining exceptions, if any, into a single {@link UncheckedIOException}.
     *
     * @param closeables to call close on.
     * @param <T> the type of closeable.
     * @throws UncheckedIOException if any exception is thrown from any of the {@code closeables}.
     */
    @SafeVarargs
    public static <T extends AutoCloseable> void closeAllUnchecked(T... closeables) {
        closeAllUnchecked(Arrays.asList(closeables));
    }

    /**
     * Closes given {@link Collection collection} of {@link AutoCloseable closeables} ignoring all exceptions.
     *
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     * @see #closeAll(AutoCloseable[])
     */
    public static <T extends AutoCloseable> void closeAllSilently(Collection<T> closeables) {
        close((msg, cause) -> null, closeables);
    }

    /**
     * Closes given array of {@link AutoCloseable closeables}. If any {@link AutoCloseable#close()} call throws
     * {@link IOException} than it will be rethrown to the caller after calling {@link AutoCloseable#close()}
     * on other given resources. If more than one {@link AutoCloseable#close()} throw than resulting exception will
     * have suppressed exceptions. See {@link Exception#addSuppressed(Throwable)}
     *
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     * @throws IOException if an exception was thrown by one of the close methods.
     */
    @SafeVarargs
    public static <T extends AutoCloseable> void closeAll(T... closeables) throws IOException {
        close(IOException::new, closeables);
    }

    /**
     * Closes given array of {@link AutoCloseable closeables} ignoring all exceptions.
     *
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     */
    @SafeVarargs
    public static <T extends AutoCloseable> void closeAllSilently(T... closeables) {
        close((msg, cause) -> null, closeables);
    }

    /**
     * Closes the given {@code closeable} if it's not {@code null}, otherwise if {@code null} does nothing.
     * Any caught {@link IOException} will be rethrown as {@link UncheckedIOException}.
     *
     * @param closeable instance to close, if it's not {@code null}.
     */
    public static void closeUnchecked(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Close all of the given closeables, and if something goes wrong, use the given constructor to create a {@link Throwable} instance with the specific cause
     * attached. The remaining closeables will still be closed, in that case, and if they in turn throw any exceptions then these will be attached as
     * suppressed exceptions.
     *
     * @param constructor The function used to construct the parent throwable that will have the first thrown exception attached as a cause, and any
     * remaining exceptions attached as suppressed exceptions. If this function returns {@code null}, then the exception is ignored.
     * @param closeables an iterator of all the things to close, in order.
     * @param <T> the type of things to close.
     * @param <E> the type of the parent exception.
     * @throws E when any {@link AutoCloseable#close()} throws exception
     */
    public static <T extends AutoCloseable, E extends Throwable> void close(
            BiFunction<String, Throwable, E> constructor, Collection<T> closeables) throws E {
        E closeThrowable = null;
        for (T closeable : closeables) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            } catch (Throwable e) {
                if (closeThrowable == null) {
                    closeThrowable = constructor.apply("Exception closing multiple resources.", e);
                } else {
                    closeThrowable.addSuppressed(e);
                }
            }
        }
        if (closeThrowable != null) {
            throw closeThrowable;
        }
    }

    /**
     * Close all of the given closeables, and if something goes wrong, use the given constructor to create a {@link Throwable} instance with the specific cause
     * attached. The remaining closeables will still be closed, in that case, and if they in turn throw any exceptions then these will be attached as
     * suppressed exceptions.
     *
     * @param constructor The function used to construct the parent throwable that will have the first thrown exception attached as a cause, and any
     * remaining exceptions attached as suppressed exceptions. If this function returns {@code null}, then the exception is ignored.
     * @param closeables all the things to close, in order.
     * @param <T> the type of things to close.
     * @param <E> the type of the parent exception.
     * @throws E when any {@link AutoCloseable#close()} throws exception
     */
    @SafeVarargs
    public static <T extends AutoCloseable, E extends Throwable> void close(
            BiFunction<String, Throwable, E> constructor, T... closeables) throws E {
        close(constructor, Arrays.asList(closeables));
    }

    /**
     * Closes the first given number of {@link AutoCloseable closeables} in the given array
     *
     * If any {@link AutoCloseable#close()} call throws
     * {@link IOException} than it will be rethrown to the caller after calling {@link AutoCloseable#close()}
     * on other given resources. If more than one {@link AutoCloseable#close()} throw than resulting exception will
     * have suppressed exceptions. See {@link Exception#addSuppressed(Throwable)}
     *
     * @param closeables the closeables to close.
     * @param count the maximum number of closeables to close within the array, ranging from 0 until count.
     * @param <T> the type of closeable
     * @throws IOException if an exception was thrown by one of the close methods.
     */
    public static <T extends AutoCloseable> void closeFirst(T[] closeables, int count) throws IOException {
        IOException closeThrowable = null;
        for (int i = 0; i < count; i++) {
            try {
                T closeable = closeables[i];
                if (closeable != null) {
                    closeable.close();
                }
            } catch (Exception e) {
                if (closeThrowable == null) {
                    closeThrowable = new IOException("Exception closing multiple resources.", e);
                } else {
                    closeThrowable.addSuppressed(e);
                }
            }
        }
        if (closeThrowable != null) {
            throw closeThrowable;
        }
    }

    public static class AutoCloseables<E extends Exception> implements AutoCloseable {
        private final BiFunction<String, Throwable, E> constructor;
        private final MutableList<AutoCloseable> autoCloseables;

        @SafeVarargs
        public <T extends AutoCloseable> AutoCloseables(
                BiFunction<String, Throwable, E> constructor, T... autoCloseables) {
            // saves extra copy than using this(constructor, Arrays::asList);
            this.autoCloseables = Lists.mutable.with(autoCloseables);
            this.constructor = constructor;
        }

        public AutoCloseables(
                BiFunction<String, Throwable, E> constructor, Iterable<? extends AutoCloseable> autoCloseables) {
            this.autoCloseables = Lists.mutable.withAll(autoCloseables);
            this.constructor = constructor;
        }

        public <T extends AutoCloseable> T add(T autoCloseable) {
            autoCloseables.add(autoCloseable);
            return autoCloseable;
        }

        @SafeVarargs
        public final <T extends AutoCloseable> void addAll(T... autoCloseables) {
            addAll(Arrays.asList(autoCloseables));
        }

        public void addAll(Iterable<? extends AutoCloseable> autoCloseables) {
            this.autoCloseables.addAllIterable(autoCloseables);
        }

        @Override
        public void close() throws E {
            IOUtils.close(constructor, autoCloseables);
        }
    }

    public static Runnable uncheckedRunnable(ThrowingAction<IOException> runnable) {
        return () -> {
            try {
                runnable.apply();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static <T> Predicate<T> uncheckedPredicate(ThrowingPredicate<T, IOException> predicate) {
        return (T t) -> {
            try {
                return predicate.test(t);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static <T> Consumer<T> uncheckedConsumer(ThrowingConsumer<T, IOException> consumer) {
        return (T t) -> {
            try {
                consumer.accept(t);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public interface ThrowingLongConsumer<E extends Throwable> {
        void accept(long l) throws E;
    }

    public static LongConsumer uncheckedLongConsumer(ThrowingLongConsumer<IOException> consumer) {
        return (long l) -> {
            try {
                consumer.accept(l);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public interface ThrowingLongSupplier<E extends Exception> {
        long get() throws E;
    }

    public static LongSupplier uncheckedLongSupplier(ThrowingLongSupplier<IOException> consumer) {
        return () -> {
            try {
                return consumer.get();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static <T, R> Function<T, R> uncheckedFunction(ThrowingFunction<T, R, IOException> consumer) {
        return (T t) -> {
            try {
                return consumer.apply(t);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
