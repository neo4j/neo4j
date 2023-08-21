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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.ResourceUtils;

public abstract class ResourceClosingIterator<T, V> implements ResourceIterator<V> {
    public static <R> ResourceIterator<R> newResourceIterator(Iterator<R> iterator, Resource... resources) {
        return new ResourceClosingIterator<>(iterator, resources) {
            @Override
            public R map(R elem) {
                return elem;
            }
        };
    }

    /**
     * Return a {@link ResourceIterator} for the provided {@code iterable} that will also close
     * this {@code iterable} when the returned iterator is itself closed. Please note, it is
     * <b>much</b> preferred to explicitly close the {@link ResourceIterable} but this utility
     * provides a way of cleaning up resources when the {@code iterable} is never exposed to
     * client code; for example when the {@link ResourceIterator} is the return-type of a method
     * call.
     *
     * @param iterable the iterable to provider the iterator
     * @param <R> the type of elements in the given iterable
     * @return the iterator for the provided {@code iterable}
     */
    public static <R> ResourceIterator<R> fromResourceIterable(ResourceIterable<R> iterable) {
        ResourceIterator<R> iterator = iterable.iterator();
        return newResourceIterator(iterator, iterator, iterable);
    }

    private Resource[] resources;
    private final Iterator<T> iterator;

    ResourceClosingIterator(Iterator<T> iterator, Resource... resources) {
        this.resources = resources;
        this.iterator = iterator;
    }

    @Override
    public void close() {
        if (resources != null) {
            ResourceUtils.closeAll(resources);
            resources = null;
        }
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = iterator.hasNext();
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    public abstract V map(T elem);

    @Override
    public V next() {
        try {
            return map(iterator.next());
        } catch (NoSuchElementException e) {
            close();
            throw e;
        }
    }

    @Override
    public void remove() {
        iterator.remove();
    }
}
