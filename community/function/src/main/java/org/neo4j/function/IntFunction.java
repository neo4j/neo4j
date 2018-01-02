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
 * Represents a function that accepts an int-valued argument and produces a result. This is the int-consuming primitive specialization for {@link Function}.
 *
 * @param <R> the type of the result of the function
 * @deprecated Usages will be replaced by corresponding {@code java.util.function} interface and classes in 3.0.
 */
public interface IntFunction<R> extends ThrowingIntFunction<R,RuntimeException>, org.neo4j.function.primitive.FunctionFromPrimitiveInt<R>
{
    /**
     * Applies this function to the given argument.
     *
     * @param value the function argument
     * @return the function result
     */
    R apply( int value );
}
