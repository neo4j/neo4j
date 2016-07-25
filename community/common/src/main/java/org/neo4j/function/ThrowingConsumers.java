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
 * Constructors for basic {@link ThrowingConsumer} types
 */
public final class ThrowingConsumers
{
    private static final ThrowingConsumer<?,?> NOOP = new ThrowingConsumer()
    {
        @Override
        public void accept( Object value )
        {
            // noop
        }
    };

    /**
     * @param <T> The type to be consumed
     * @param <E> The exception that the consumer might throw
     * @return a {@link ThrowingConsumer} that does nothing.
     */
    @SuppressWarnings( "unchecked" )
    public static <T, E extends Exception> ThrowingConsumer<T,E> noop()
    {
        return (ThrowingConsumer<T,E>) NOOP;
    }
}
