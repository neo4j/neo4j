/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
 * Represents an operation that accepts a single input long argument and returns no result.
 *
 * Unlike most other functional interfaces, ThrowingConsumer is expected to operate via side-effects.
 *
 * @param <E> the type of exception that may be thrown from the function.
 */
public interface ThrowingLongConsumer<E extends Exception>
{
    /**
     * Performs this operation on the given argument.
     *
     * @param t the long input argument
     * @throws E an exception if the function fails
     */
    void accept( long t ) throws E;
}
