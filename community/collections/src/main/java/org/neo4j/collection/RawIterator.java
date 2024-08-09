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
package org.neo4j.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.neo4j.internal.helpers.collection.Iterators;

/**
 * Just like {@link Iterator}, but with the addition that {@link #hasNext()} and {@link #next()} can
 * be declared to throw a checked exception.
 *
 * @param <T> type of items in this iterator.
 * @param <EXCEPTION> type of exception thrown from {@link #hasNext()} and {@link #next()}.
 */
public interface RawIterator<T, EXCEPTION extends Exception> {
    boolean hasNext() throws EXCEPTION;

    T next() throws EXCEPTION;

    default void remove() {
        throw new UnsupportedOperationException();
    }

    ResourceRawIterator<Object, Exception> EMPTY_ITERATOR = ResourceRawIterator.of();

    @SuppressWarnings("unchecked")
    static <T, EXCEPTION extends Exception> ResourceRawIterator<T, EXCEPTION> empty() {
        return (ResourceRawIterator<T, EXCEPTION>) EMPTY_ITERATOR;
    }

    static <T, EX extends Exception> RawIterator<T, EX> of(T... values) {
        return new RawIterator<>() {
            private int position;

            @Override
            public boolean hasNext() {
                return position < values.length;
            }

            @Override
            public T next() throws EX {
                if (hasNext()) {
                    return values[position++];
                }
                throw new NoSuchElementException();
            }
        };
    }

    /**
     * Create a raw iterator from a regular iterator, assuming no exceptions are being thrown
     */
    static <T, EX extends Exception> RawIterator<T, EX> wrap(final Iterator<T> iterator) {
        return Iterators.asRawIterator(iterator);
    }
}
