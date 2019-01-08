/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.Optional;

/**
 * Wrapper around checked exceptions for rethrowing them as runtime exceptions when the signature of the containing method
 * cannot be changed to declare them.
 *
 * Thrown by {@link ThrowingFunction#catchThrown(Class, ThrowingFunction)}
 */
public class UncaughtCheckedException extends RuntimeException
{
    private final Object source;

    public UncaughtCheckedException( Object source, Throwable cause )
    {
        super( "Uncaught checked exception", cause );
        if ( cause == null )
        {
            throw new IllegalArgumentException( "Expected non-null cause" );
        }
        this.source = source;
    }

    /**
     * Check that the cause has the given type and if successful, return it.
     *
     * @param clazz class object for the desired type of the cause
     * @param <E> the desired type of the cause
     * @return the underlying cause of this exception but only if it is of desired type E, nothing otherwise
     */
    public <E extends Exception> Optional<E> getCauseIfOfType( Class<E> clazz )
    {
        Throwable cause = getCause();
        if ( clazz.isInstance( cause ) )
        {
            return Optional.of( clazz.cast( cause ) );
        }
        else
        {
            return Optional.empty();
        }
    }

    public Object source()
    {
        return source;
    }
}
