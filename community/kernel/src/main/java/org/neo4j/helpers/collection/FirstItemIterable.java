/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.helpers.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Wraps the given iterator but keeps the first item to allow later
 * access to it, like CachingIterator but with less memory overhead.
 * @param <T> the type of elements
 */
public class FirstItemIterable<T> implements Iterable<T> {
    private final T first;
    private final Iterator<T> iterator;
    private int pos = -1;

    public FirstItemIterable(Iterable<T> data) {
        this(data.iterator());
    }

    public FirstItemIterable(Iterator<T> iterator) {
        this.iterator = iterator;
        if (iterator.hasNext()) {
            this.first = iterator.next();
            this.pos = 0;
        } else {
            this.first = null;
        }
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return pos == 0 || iterator.hasNext();
            }

            @Override
            public T next() {
                if (pos < 0) throw new NoSuchElementException();
                return pos++ == 0 ? first : iterator.next();
            }

            @Override
            public void remove() {

            }
        };
    }

    public T getFirst() {
        return first;
    }
}
