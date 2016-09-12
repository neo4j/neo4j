/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
 * Constructors for basic {@link ThrowingFunction} and {@link ThrowingBiFunction} types
 */
public final class ThrowingFunctions
{
    private static final ThrowingUnaryOperator IDENTITY = value -> value;

    @SuppressWarnings( "unchecked" )
    public static <T, E extends Exception> ThrowingUnaryOperator<T,E> identity()
    {
        return IDENTITY;
    }

    public static <T, E extends Exception> ThrowingFunction<T,Void,E> fromConsumer( ThrowingConsumer<T,E> consumer )
    {
        return t ->
        {
            consumer.accept( t );
            return null;
        };
    }

    public static <T, E extends Exception> ThrowingFunction<Void,T,E> fromSupplier( ThrowingSupplier<T,E> supplier )
    {
        return t -> supplier.get();
    }
}
