/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.helpers;

import java.lang.reflect.InvocationTargetException;

public class Exceptions
{
    private static final String UNEXPECTED_MESSAGE = "Unexpected Exception";

    public static <T extends Throwable> T withCause( T exception, Throwable cause )
    {
        try
        {
            exception.initCause( cause );
        }
        catch ( Exception failure )
        {
            // OK, we did our best, guess there will be no cause
        }
        return exception;
    }

    public static RuntimeException launderedException( Throwable exception )
    {
        return launderedException( UNEXPECTED_MESSAGE, exception );
    }

    public static RuntimeException launderedException( String messageForUnexpected, Throwable exception )
    {
        return launderedException( RuntimeException.class, messageForUnexpected, exception );
    }

    public static <T extends Throwable> T launderedException( Class<T> type, Throwable exception )
    {
        return launderedException( type, UNEXPECTED_MESSAGE, exception );
    }

    public static <T extends Throwable> T launderedException( Class<T> type, String messageForUnexpected,
            Throwable exception )
    {
        if ( type.isInstance( exception ) )
        {
            return type.cast( exception );
        }
        else if ( exception instanceof Error )
        {
            throw (Error) exception;
        }
        else if ( exception instanceof InvocationTargetException )
        {
            return launderedException( type, messageForUnexpected,
                    ( (InvocationTargetException) exception ).getTargetException() );
        }
        else if ( exception instanceof RuntimeException )
        {
            throw (RuntimeException) exception;
        }
        else
        {
            throw new RuntimeException( messageForUnexpected, exception );
        }
    }
    
    /**
     * Peels off layers of causes. For example:
     * 
     * MyFarOuterException
     *   cause: MyOuterException
     *     cause: MyInnerException
     *       cause: MyException
     * and a toPeel predicate returning true for MyFarOuterException and MyOuterException
     * will return MyInnerException. If the predicate peels all exceptions null is returned. 
     * 
     * @param exception the outer exception to peel to get to an delegate cause.
     * @param toPeel {@link Predicate} for deciding what to peel. {@code true} means
     * to peel (i.e. remove), whereas the first {@code false} means stop and return.
     * @return the delegate cause of an exception, dictated by the predicate.
     */
    public static Throwable peel( Throwable exception, Predicate<Throwable> toPeel )
    {
        while ( exception != null )
        {
            if ( !toPeel.accept( exception ) )
                break;
            exception = exception.getCause();
        }
        return exception;
    }
    
    public static Predicate<Throwable> exceptionsOfType( final Class<? extends Throwable>... types )
    {
        return new Predicate<Throwable>()
        {
            @Override
            public boolean accept( Throwable item )
            {
                for ( Class<? extends Throwable> type : types )
                {
                    if ( type.isAssignableFrom( item.getClass() ) )
                    {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    private Exceptions()
    {
        // no instances
    }
}
