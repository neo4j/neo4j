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
 * allows to catch, analyze and react on exceptions that are thrown by the delegate iterable
 * useful for exception conversion on iterator methods
 * Uses sun.misc.Unsafe internally to rethrow original exceptions !
 * @param <T> the type of elements
 */
public class ExceptionHandlingIterable<T> implements Iterable<T> {
    private final Iterable<T> source;
    public ExceptionHandlingIterable(Iterable<T> source) {
        this.source = source;
    }

    @Override
    public Iterator<T> iterator() {
        try {
            final Iterator<T> it = source.iterator();
            return new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    try {
                        return it.hasNext();
                    } catch (Throwable t) {
                        return exceptionOnHasNext(t);
                    }
                }

                @Override
                public T next() {
                    try {
                        return it.next();
                    } catch (Throwable t) {
                        return exceptionOnNext(t);
                    }
                }

                @Override
                public void remove() {
                    try {
                        it.remove();
                    } catch (Throwable t) {
                        exceptionOnRemove(t);
                    }
                }
            };
        } catch (Throwable t) {
            return exceptionOnIterator(t);
        }
    }

    protected void rethrow(Throwable t)
    {
        // TODO it's pretty bad that we have to do this. We should refactor our exception hierarchy
        // to eliminate the need for this hack.
        ExceptionHandlingIterable.<RuntimeException>sneakyThrow(t);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T
    {
        throw (T) throwable;
    }

    protected boolean exceptionOnHasNext(Throwable t) {
        rethrow(t);
        return false;
    }

    protected void exceptionOnRemove(Throwable t) {
    }

    protected T exceptionOnNext(Throwable t) {
        rethrow(t);
        return null;
    }

    protected Iterator<T> exceptionOnIterator(Throwable t) {
        rethrow(t);
        return null;
    }
}
