/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.executor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.neo4j.kernel.api.exceptions.Status;

public class Exceptions
{
    public static RuntimeException transform( Status defaultStatus, Throwable t )
    {
        var unwrapped = reactor.core.Exceptions.unwrap( t );
        unwrapped = transformComposite( unwrapped );

        String message = unwrapped.getMessage();

        // preserve the original exception if possible
        // or try to preserve  at least the original status
        if ( unwrapped instanceof Status.HasStatus )
        {
            if ( unwrapped instanceof RuntimeException )
            {
                return (RuntimeException) unwrapped;
            }

            return new FabricException( ((Status.HasStatus) unwrapped).status(), message, unwrapped );
        }

        return new FabricException( defaultStatus, message, unwrapped );
    }

    private static Throwable transformComposite( Throwable potentialComposite )
    {
        List<Throwable> unwrappedExceptions = reactor.core.Exceptions.unwrapMultiple( potentialComposite );
        List<Throwable> primaryExceptions = new ArrayList<>();
        List<FabricSecondaryException> secondaryExceptions = new ArrayList<>();

        unwrappedExceptions.forEach( exception ->
        {
            if ( exception instanceof FabricSecondaryException )
            {
                secondaryExceptions.add( (FabricSecondaryException) exception );
            }
            else
            {
                primaryExceptions.add( exception );
            }
        } );

        if ( !primaryExceptions.isEmpty() )
        {
            Throwable result = primaryExceptions.get( 0 );
            IntStream.range( 1, primaryExceptions.size() ).forEach( i -> result.addSuppressed( primaryExceptions.get( i ) ) );
            return result;
        }

        Set<Throwable> uniqueExceptions = new HashSet<>();
        Throwable result = secondaryExceptions.get( 0 ).getPrimaryException();
        uniqueExceptions.add( result );
        IntStream.range( 1, secondaryExceptions.size() )
                 .mapToObj( secondaryExceptions::get )
                 .map( FabricSecondaryException::getPrimaryException )
                 // multiple secondary exceptions can point to the same primary one
                 .filter( exception -> !uniqueExceptions.contains( exception ) )
                 .forEach( exception ->
                 {
                     result.addSuppressed( exception );
                     uniqueExceptions.add( exception );
                 } );
        return result;
    }
}
