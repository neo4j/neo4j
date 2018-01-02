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
package org.neo4j.function;

import java.util.function.Predicate;

/**
 * Represents a predicate (boolean-valued function) of one argument.
 *
 * @param <T> the type of the input to the predicate
 * @param <E> the type of exception that may be thrown from the operator
 */
public interface ThrowingPredicate<T, E extends Exception>
{
    /**
     * Evaluates this predicate on the given argument.
     *
     * @param t the input argument
     * @return true if the input argument matches the predicate, otherwise false
     * @throws E an exception if the predicate fails
     */
    boolean test( T t ) throws E;

    static <TYPE> ThrowingPredicate<TYPE,RuntimeException> throwingPredicate( Predicate<TYPE> predicate )
    {
        return new ThrowingPredicate<TYPE,RuntimeException>()
        {
            @Override
            public boolean test( TYPE value )
            {
                return predicate.test( value );
            }

            @Override
            public String toString()
            {
                return predicate.toString();
            }
        };
    }
}
