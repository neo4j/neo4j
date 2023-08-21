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
package org.neo4j.internal.helpers.collection;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.neo4j.function.Predicates;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.Exceptions;

/**
 * Utility methods for processing iterables. Where possible, If the iterable implements
 * {@link Resource}, it will be {@link Resource#close() closed} when the processing
 * has been completed.
 */
public final class Iterables {
    private Iterables() {
        throw new AssertionError("no instance");
    }

    public static <T> Iterable<T> empty() {
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    public static <T> ResourceIterable<T> emptyResourceIterable() {
        return (ResourceIterable<T>) EmptyResourceIterable.EMPTY_RESOURCE_ITERABLE;
    }

    /**
     * Collect all the elements available in {@code iterable} and add them to the
     * provided {@code collection}.
     * <p>
     * If the {@code iterable} implements {@link Resource} it will be
     * {@link Resource#close() closed} in a {@code finally} block after all
     * the items have been added.
     *
     * @param collection the collection to add items to.
     * @param iterable the iterable from which items will be collected
     * @param <T> the type of elements in {@code iterable}.
     * @param <C> the type of the collection to add the items to.
     * @return the {@code collection} that has been updated.
     */
    public static <T, C extends Collection<T>> C addAll(C collection, Iterable<? extends T> iterable) {
        try {
            Iterator<? extends T> iterator = iterable.iterator();
            try {
                while (iterator.hasNext()) {
                    collection.add(iterator.next());
                }
            } finally {

                Iterators.tryCloseResource(iterator);
            }
        } finally {
            tryCloseResource(iterable);
        }

        return collection;
    }

    public static <X> Iterable<X> filter(Predicate<? super X> specification, Iterable<X> i) {
        return new FilterIterable<>(i, specification);
    }

    public static <X> List<X> reverse(List<X> iterable) {
        List<X> list = asList(iterable);
        Collections.reverse(list);
        return list;
    }

    public static <FROM, TO> Iterable<TO> map(Function<? super FROM, ? extends TO> function, Iterable<FROM> from) {
        return new MapIterable<>(from, function);
    }

    @SafeVarargs
    public static <T, C extends T> Iterable<T> iterable(C... items) {
        return Arrays.asList(items);
    }

    @SuppressWarnings("unchecked")
    public static <T, C> Iterable<T> cast(Iterable<C> iterable) {
        return (Iterable<T>) iterable;
    }

    @SafeVarargs
    @SuppressWarnings("unchecked")
    public static <T> Iterable<T> concat(Iterable<? extends T>... iterables) {
        return concat(Arrays.asList((Iterable<T>[]) iterables));
    }

    public static <T> Iterable<T> concat(final Iterable<? extends Iterable<T>> iterables) {
        return new CombiningIterable<>(iterables);
    }

    public static <T, C extends T> Iterable<T> append(final C item, final Iterable<T> iterable) {
        return () -> {
            final Iterator<T> iterator = iterable.iterator();

            return new Iterator<>() {
                T last = item;

                @Override
                public boolean hasNext() {
                    return iterator.hasNext() || last != null;
                }

                @Override
                public T next() {
                    if (iterator.hasNext()) {
                        return iterator.next();
                    }
                    try {
                        return last;
                    } finally {
                        last = null;
                    }
                }

                @Override
                public void remove() {}
            };
        };
    }

    public static Object[] asArray(Iterable<Object> iterable) {
        return asArray(Object.class, iterable);
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] asArray(Class<T> componentType, Iterable<T> iterable) {
        if (iterable == null) {
            return null;
        }

        List<T> list = asList(iterable);
        return list.toArray((T[]) Array.newInstance(componentType, list.size()));
    }

    public static <T> ResourceIterable<T> asResourceIterable(final Iterable<T> iterable) {
        if (iterable instanceof ResourceIterable<?>) {
            return (ResourceIterable<T>) iterable;
        }
        return new AbstractResourceIterable<>() {
            @Override
            protected ResourceIterator<T> newIterator() {
                return Iterators.asResourceIterator(iterable.iterator());
            }

            @Override
            protected void onClosed() {
                tryCloseResource(iterable);
            }
        };
    }

    /**
     * Returns the given iterable's first element or {@code null} if no
     * element found.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after the items have been joined.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the items have been joined.
     *
     * @param values the {@link Iterable} to get elements from.
     * @param separator the separator to use between the items in {@code values}.
     * @return the joined string.
     */
    public static String toString(Iterable<?> values, String separator) {
        Iterator<?> it = values.iterator();
        try {
            StringBuilder sb = new StringBuilder();
            while (it.hasNext()) {
                sb.append(it.next());
                if (it.hasNext()) {
                    sb.append(separator);
                }
            }
            return sb.toString();
        } finally {
            Iterators.tryCloseResource(it);
            tryCloseResource(values);
        }
    }

    /**
     * Returns the given iterable's first element or {@code null} if no
     * element found.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after the first item has been retrieved, or failed to be retrieved.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the first element in the {@code iterable}, or {@code null} if no
     * element found.
     */
    public static <T> T firstOrNull(Iterable<T> iterable) {
        try {
            return Iterators.firstOrNull(iterable.iterator());
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Returns the given iterable's first element. If no element is found a
     * {@link NoSuchElementException} is thrown.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after the first item has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the first element in the {@code iterable}.
     * @throws NoSuchElementException if no element found.
     */
    public static <T> T first(Iterable<T> iterable) {
        try {
            return Iterators.first(iterable.iterator());
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Returns the given iterable's last element. If no element is found a
     * {@link NoSuchElementException} is thrown.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after the last item has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the last element in the {@code iterable}.
     * @throws NoSuchElementException if no element found.
     */
    public static <T> T last(Iterable<T> iterable) {
        try {
            return Iterators.last(iterable.iterator());
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Returns the given iterable's single element or {@code null} if no
     * element found. If there is more than one element in the iterable a
     * {@link NoSuchElementException} will be thrown.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after the single item has been retrieved, or failed to be retrieved.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the single element in {@code iterable}, or {@code null} if no
     * element found.
     * @throws NoSuchElementException if more than one element was found.
     */
    public static <T> T singleOrNull(Iterable<T> iterable) {
        try {
            return Iterators.singleOrNull(iterable.iterator());
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Returns the given iterable's single element. If there are no elements
     * or more than one element in the iterable a {@link NoSuchElementException}
     * will be thrown.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after the single item has been retrieved, or failed to be retrieved.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the single element in the {@code iterable}.
     * @throws NoSuchElementException if there isn't exactly one element.
     */
    public static <T> T single(Iterable<T> iterable) {
        try {
            return Iterators.single(iterable.iterator());
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Returns the given iterable's single element or {@code itemIfNone} if no
     * element found. If there is more than one element in the iterable a
     * {@link NoSuchElementException} will be thrown.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after the single item has been retrieved, or failed to be retrieved.
     * <p>>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @param itemIfNone item to use if none is found
     * @return the single element in {@code iterable}, or {@code null} if no
     * element found.
     * @throws NoSuchElementException if more than one element was found.
     */
    public static <T> T single(Iterable<T> iterable, T itemIfNone) {
        try {
            return Iterators.single(iterable.iterator(), itemIfNone);
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Counts the number of items in the {@code iterable} by looping through it.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after all its items have been counted.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the items have been counted.
     *
     * @param <T> the type of items in the iterator.
     * @param iterable the {@link Iterable} to count items in.
     * @return the number of items found in {@code iterable}.
     */
    public static <T> long count(Iterable<T> iterable) {
        return count(iterable, Predicates.alwaysTrue());
    }

    /**
     * Counts the number of filtered items in the {@code iterable} by looping through it.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after all its items have been counted.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the items have been counted.
     *
     * @param <T> the type of items in the iterator.
     * @param iterable the {@link Iterable} to count items in.
     * @param filter the filter to test items against
     * @return the number of found in {@code iterable}.
     */
    public static <T> long count(Iterable<T> iterable, Predicate<T> filter) {
        try {
            return Iterators.count(iterable.iterator(), filter);
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Creates a collection from an iterable.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after all its items have been added.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after all the items have been added.
     *
     * @param iterable The iterable to create the collection from.
     * @param <T> The generic type of both the iterable and the collection.
     * @return a collection containing all items from the iterable.
     */
    public static <T> Collection<T> asCollection(Iterable<T> iterable) {
        return addAll(new ArrayList<>(), iterable);
    }

    /**
     * Creates a list from an iterable.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after all its items have been added.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after all the items have been added.
     *
     * @param iterable The iterable to create the list from.
     * @param <T> The generic type of both the iterable and the list.
     * @return a list containing all items from the iterable.
     */
    public static <T> List<T> asList(Iterable<T> iterable) {
        return addAll(new ArrayList<>(), iterable);
    }

    /**
     * Creates a {@link Set} from an {@link Iterable}.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after all its items have been added.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after all the items have been added.
     *
     * @param iterable The items to create the set from.
     * @param <T> The generic type of items.
     * @return a set containing all items from the {@link Iterable}.
     */
    public static <T> Set<T> asSet(Iterable<T> iterable) {
        return addAll(new HashSet<>(), iterable);
    }

    /**
     * Creates a {@link Set} from an {@link Iterable}.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after all its items have been added.
     *
     * @param iterable The items to create the set from.
     * @param <T> The generic type of items.
     * @return a set containing all items from the {@link Iterable}.
     */
    public static <T> Set<T> asUniqueSet(Iterable<T> iterable) {
        try {
            return Iterators.addToCollectionUnique(iterable, new HashSet<>());
        } finally {
            tryCloseResource(iterable);
        }
    }

    public static Iterable<Long> asIterable(final long... array) {
        return () -> Iterators.asIterator(array);
    }

    public static Iterable<Integer> asIterable(final int... array) {
        return () -> Iterators.asIterator(array);
    }

    @SafeVarargs
    public static <T> Iterable<T> asIterable(final T... array) {
        return () -> Iterators.iterator(array);
    }

    public static <T> ResourceIterable<T> resourceIterable(final Iterable<T> iterable) {
        return new AbstractResourceIterable<>() {
            @Override
            protected ResourceIterator<T> newIterator() {
                Iterator<T> iterator = iterable.iterator();
                Resource resource = (iterator instanceof Resource) ? (Resource) iterator : Resource.EMPTY;
                return Iterators.resourceIterator(iterator, resource);
            }

            @Override
            protected void onClosed() {
                tryCloseResource(iterable);
            }
        };
    }

    public static <T> Iterable<T> option(final T item) {
        if (item == null) {
            return Collections.emptyList();
        }

        return () -> Iterators.iterator(item);
    }

    /**
     * Create a stream from the given iterable.
     * <p>
     * <b>Note:</b> returned stream needs to be closed via {@link Stream#close()} if the given iterable implements
     * {@link Resource}.
     *
     * @param iterable the iterable to convert to stream
     * @param <T> the type of elements in the given iterable
     * @return stream over the iterable elements
     * @throws NullPointerException when the given iterable is {@code null}
     */
    public static <T> Stream<T> stream(Iterable<T> iterable) {
        return stream(iterable, 0);
    }

    /**
     * Create a stream from the given iterable with given characteristics.
     * <p>
     * <b>Note:</b> returned stream needs to be closed via {@link Stream#close()} if the given iterable implements
     * {@link Resource}.
     *
     * @param iterable the iterable to convert to stream
     * @param characteristics the logical OR of characteristics for the underlying {@link Spliterator}
     * @param <T> the type of elements in the given iterable
     * @return stream over the iterable elements
     * @throws NullPointerException when the given iterable is {@code null}
     */
    public static <T> Stream<T> stream(Iterable<T> iterable, int characteristics) {
        Objects.requireNonNull(iterable);
        return Iterators.stream(iterable.iterator(), characteristics).onClose(() -> tryCloseResource(iterable));
    }

    /**
     * Method for calling a lambda function on many objects. The first exception to be encountered will be
     * thrown and subsequent processing of the remaining items will be aborted.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after all its items have been consumed.
     *
     * @param iterable iterable to iterate over
     * @param consumer lambda function to call on each object passed
     */
    public static <V> void forEach(Iterable<V> iterable, Consumer<V> consumer) {
        try {
            for (final var item : iterable) {
                consumer.accept(item);
            }
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Method for calling a lambda function on many objects when it is expected that the function might
     * throw an exception. First exception will be thrown and subsequent will be suppressed.
     * This method guarantees that all subjects will be consumed, unless {@link Error} happens.
     *
     * @param <E> the type of exception anticipated, inferred from the lambda
     * @param subjects {@link Iterable} of objects to call the function on
     * @param consumer lambda function to call on each object passed
     * @throws E if consumption fails with this exception
     */
    @SuppressWarnings("unchecked")
    public static <T, E extends Throwable> void safeForAll(Iterable<T> subjects, ThrowingConsumer<T, E> consumer)
            throws E {
        try {
            E exception = null;
            for (T instance : subjects) {
                try {
                    consumer.accept(instance);
                } catch (Throwable t) {
                    exception = Exceptions.chain(exception, (E) t);
                }
            }
            if (exception != null) {
                throw exception;
            }
        } finally {
            tryCloseResource(subjects);
        }
    }

    /**
     * Close the provided {@code iterable} if it implements {@link Resource}.
     *
     * @param iterable the iterable to check for closing
     */
    public static void tryCloseResource(Iterable<?> iterable) {
        if (iterable instanceof Resource closeable) {
            closeable.close();
        }
    }

    private static class EmptyResourceIterable<T> implements ResourceIterable<T> {
        private static final ResourceIterable<Object> EMPTY_RESOURCE_ITERABLE = new EmptyResourceIterable<>();

        @Override
        public ResourceIterator<T> iterator() {
            return Iterators.emptyResourceIterator();
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
