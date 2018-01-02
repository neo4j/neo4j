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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * A visitor to internalize iteration.
 *
 * @param <E> the element type the visitor accepts.
 * @param <FAILURE> the type of exception the visitor might throw
 */
public interface Visitor<E, FAILURE extends Exception>
{
    /**
     * Invoked for each element in a collection. Return <code>true</code> to
     * terminate the iteration, <code>false</code> to continue.
     *
     * @param element an element from the collection.
     * @return <code>true</code> to terminate the iteration, <code>false</code>
     *         to continue.
     * @throws FAILURE exception thrown by the visitor
     */
    boolean visit( E element ) throws FAILURE;

    final class SafeGenerics
    {
        /**
         * Useful for determining "is this an object that can visit the things I can provide?"
         *
         * Checks if the passed in object is a {@link Visitor} and if the objects it can
         * {@link Visitor#visit(Object) visit} is compatible (super type of) with the provided type. Returns the
         * visitor cast to compatible type parameters. If the passed in object is not an instance of {@link Visitor},
         * or if it is a {@link Visitor} but one that {@link Visitor#visit(Object) visits} another type of object, this
         * method returns {@code null}.
         * 
         * @param eType element type of the visitor
         * @param fType failure type of the visitor
         * @param visitor the visitor
         * @param <T> type of the elements
         * @param <F> type of the exception
         * @return the visitor cast to compatible type parameters or {@code null}
         */
        @SuppressWarnings("unchecked"/*checked through reflection*/)
        public static <T, F extends Exception>
        Visitor<? super T, ? extends F> castOrNull( Class<T> eType, Class<F> fType, Object visitor )
        {
            if ( visitor instanceof Visitor<?, ?> )
            {
                for ( Type iface : visitor.getClass().getGenericInterfaces() )
                {
                    if ( iface instanceof ParameterizedType )
                    {
                        ParameterizedType paramType = (ParameterizedType) iface;
                        if ( paramType.getRawType() == Visitor.class )
                        {
                            Type arg = paramType.getActualTypeArguments()[0];
                            if ( arg instanceof ParameterizedType )
                            {
                                arg = ((ParameterizedType) arg).getRawType();
                            }
                            if ( (arg instanceof Class<?>) && ((Class<?>) arg).isAssignableFrom( eType ) )
                            {
                                return (Visitor<? super T, ? extends F>) visitor;
                            }
                        }
                    }
                }
            }
            return null;
        }

        private SafeGenerics()
        {
        }
    }
}
