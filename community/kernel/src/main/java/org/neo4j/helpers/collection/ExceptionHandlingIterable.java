/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Iterator;

/**
 * allows to catch, analyse and react on exceptions that are thrown by the delegate iterable
 * useful for exception conversion on iterator methods
 * Uses sun.misc.Unsafe internally to rethrow original exceptions !
 * @param <T>
 */
public class ExceptionHandlingIterable<T> implements Iterable<T> {
    private final Iterable<T> source;
    private static final Unsafe unsafe = getUnsafe();
    public ExceptionHandlingIterable(Iterable<T> source) {
        this.source = source;
    }

    public static Unsafe getUnsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Error retrieving Unsafe ", e);
        }
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

    protected void rethrow(Throwable t) {
        unsafe.throwException(t);
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
