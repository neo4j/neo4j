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
 * Constructors for basic {@link ThrowingFunction} and {@link ThrowingBiFunction} types
 */
public final class ThrowingFunctions
{
    private static final ThrowingUnaryOperator IDENTITY = new ThrowingUnaryOperator()
    {
        @Override
        public Object apply( Object value )
        {
            return value;
        }
    };

    @SuppressWarnings( "unchecked" )
    public static <T, E extends Exception> ThrowingUnaryOperator<T,E> identity()
    {
        return IDENTITY;
    }

    public static <T, E extends Exception> ThrowingFunction<T,Void,E> fromConsumer( final ThrowingConsumer<T,E> consumer )
    {
        return new ThrowingFunction<T,Void,E>()
        {
            @Override
            public Void apply( T t ) throws E
            {
                consumer.accept( t );
                return null;
            }
        };
    }

    public static <T, E extends Exception> ThrowingFunction<Void,T,E> fromSupplier( final ThrowingSupplier<T,E> supplier )
    {
        return new ThrowingFunction<Void,T,E>()
        {
            @Override
            public T apply( Void t ) throws E
            {
                return supplier.get();
            }
        };
    }
}
