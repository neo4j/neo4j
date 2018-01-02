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


/**
 * Decorator class that wraps any iterator and remembers the current node.
 */

public class PositionedIterator<T> implements Iterator<T> {
    private Iterator<? extends T> inner;
    private T current;
    private Boolean initiated = false;

    /**
     * Creates an instance of the class, wrapping iterator
     * @param iterator The iterator to wrap
     */
    public PositionedIterator(Iterator<? extends T> iterator) {
        inner = iterator;
    }

    public boolean hasNext() {
        return inner.hasNext();
    }

    public T next() {
        initiated = true;
        current = inner.next();
        return current;
    }

    public void remove() {
        inner.remove();
    }

    /**
     * Returns the current node. Any subsequent calls to current will return the same object,
     * unless the next() method has been called.
     * @return The current node.
     */
    public T current() {
        if(!initiated)
            return next();

        return current;
    }
}
