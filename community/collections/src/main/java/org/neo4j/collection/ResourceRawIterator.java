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

import java.util.NoSuchElementException;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Resource;

/** A {@link RawIterator} with resources. */
public interface ResourceRawIterator<T, E extends Exception> extends RawIterator<T, E>, Resource {

    ResourceRawIterator<Object, Exception> EMPTY_ITERATOR = ResourceRawIterator.of();

    @SuppressWarnings("unchecked")
    static <T, E extends Exception> ResourceRawIterator<T, E> empty() {
        return (ResourceRawIterator<T, E>) EMPTY_ITERATOR;
    }

    static <T, E extends Exception> ResourceRawIterator<T, E> of(T... values) {
        return new ResourceRawIterator<>() {
            private int position;

            @Override
            public boolean hasNext() {
                return position < values.length;
            }

            @Override
            public T next() {
                if (hasNext()) {
                    return values[position++];
                }
                throw new NoSuchElementException();
            }

            @Override
            public void close() {}
        };
    }

    /**
     * Create a raw iterator from the provided {@link ThrowingSupplier} - the iterator will end
     * when the supplier returns null.
     */
    static <T, EX extends Exception> ResourceRawIterator<T, EX> from(ThrowingSupplier<T, EX> supplier) {
        return new AbstractPrefetchingRawIterator<>() {
            @Override
            protected T fetchNextOrNull() throws EX {
                return supplier.get();
            }

            @Override
            public void close() {}
        };
    }
}
