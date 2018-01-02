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

import org.junit.Test;

import java.util.Iterator;

/**
 * @author mh
 * @since 21.04.12
 */
@SuppressWarnings("unchecked")
public class ExceptionHandlingIterableTest {

    @Test(expected = IllegalStateException.class)
    public void testHandleExceptionOnIteratorCreation() {
        IteratorUtil.count(new ExceptionHandlingIterable(new Iterable() {
            public Iterator iterator() {
                throw new RuntimeException("exception on iterator");
            }
        }){
            protected Iterator exceptionOnIterator(Throwable t) {
                rethrow(new IllegalStateException());
                return super.exceptionOnIterator(t);
            }
        });
    }
    @Test(expected = IllegalStateException.class)
    public void testHandleExceptionOnNext() {
        IteratorUtil.count(new ExceptionHandlingIterable(new Iterable() {
            public Iterator iterator() {
                return new Iterator() {
                    public boolean hasNext() {
                        return true;
                    }

                    public Object next() {
                        throw new RuntimeException("exception on next");
                    }

                    public void remove() {
                    }
                };
            }
        }){
            @Override
            protected Object exceptionOnNext(Throwable t) {
                rethrow(new IllegalStateException());
                return super.exceptionOnNext(t);
            }
        });
    }
    @Test(expected = IllegalStateException.class)
    public void testHandleExceptionOnHasNext() {
        IteratorUtil.count(new ExceptionHandlingIterable(new Iterable() {
            public Iterator iterator() {
                return new Iterator() {
                    public boolean hasNext() {
                        throw new RuntimeException("exception on next");
                    }

                    public Object next() {
                        return  null;
                    }

                    public void remove() {
                    }
                };
            }
        }){
            @Override
            protected boolean exceptionOnHasNext(Throwable t) {
                rethrow(new IllegalStateException());
                return super.exceptionOnHasNext(t);
            }
        });
    }
}
