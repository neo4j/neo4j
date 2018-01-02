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

/**
 * Represents an operation that accepts a single input argument and returns no result. Unlike most other functional interfaces, ThrowingConsumer is expected to
 * operate via side-effects.
 *
 * @param <T> the type of the input to the operation
 * @param <E> the type of exception that may be thrown from the function
 */
public interface ThrowingConsumer<T, E extends Exception>
{
    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     * @throws E an exception if the function fails
     */
    void accept( T t ) throws E;
}
