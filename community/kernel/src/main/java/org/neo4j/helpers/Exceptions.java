/**
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
package org.neo4j.helpers;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.Thread.State;
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
            {
                break;
            }
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

    public static Throwable rootCause( Throwable caughtException )
    {
        if ( null == caughtException )
        {
            throw new IllegalArgumentException( "Cannot obtain rootCause from (null)" );
        }
        Throwable root  = caughtException;
        Throwable cause = root.getCause();
        while ( null != cause )
        {
            root  = cause;
            cause = cause.getCause();
        }
        return root;
    }

    public static String stringify( Throwable cause )
    {
        try
        {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            PrintStream target = new PrintStream( bytes, true, "UTF-8" );
            cause.printStackTrace( target );
            target.flush();
            return bytes.toString("UTF-8");
        }
        catch(UnsupportedEncodingException e)
        {
            cause.printStackTrace(System.err);
            return "[ERROR: Unable to serialize stacktrace, UTF-8 not supported.]";
        }
    }
    
    public static String stringify( Thread thread, StackTraceElement[] elements )
    {
        StringBuilder builder = new StringBuilder(
                "\"" + thread.getName() + "\"" + (thread.isDaemon() ? " daemon": "") +
                " prio=" + thread.getPriority() +
                " tid=" + thread.getId() +
                " " + thread.getState().name().toLowerCase() + "\n" );
        builder.append( "   " + State.class.getName() + ": " + thread.getState().name().toUpperCase() + "\n" );
        for ( StackTraceElement element : elements )
        {
            builder.append( "      at " + element.getClassName() + "." + element.getMethodName() ); 
            if ( element.isNativeMethod() )
            {
                builder.append( "(Native method)" );
            }
            else if ( element.getFileName() == null )
            {
                builder.append( "(Unknown source)" );
            }
            else
            {
                builder.append( "(" + element.getFileName() + ":" + element.getLineNumber() + ")" );
            }
            builder.append( "\n" );
        }
        return builder.toString();
    }
    
    @SuppressWarnings( "rawtypes" )
    public static boolean contains( final Throwable cause, final String containsMessage, final Class... anyOfTheseClasses )
    {
        final Predicate<Throwable> anyOfClasses = isAnyOfClasses( anyOfTheseClasses );
        return contains( cause, new Predicate<Throwable>()
        {
            @Override
            public boolean accept( Throwable item )
            {
                return item.getMessage() != null && item.getMessage().contains( containsMessage ) &&
                        anyOfClasses.accept( item );
            }
        } );
    }

    @SuppressWarnings( "rawtypes" )
    public static boolean contains( Throwable cause, Class... anyOfTheseClasses )
    {
        return contains( cause, isAnyOfClasses( anyOfTheseClasses ) );
    }

    public static boolean contains( Throwable cause, Predicate<Throwable> toLookFor )
    {
        while ( cause != null )
        {
            if ( toLookFor.accept( cause ) )
            {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    public static Predicate<Throwable> isAnyOfClasses( final Class... anyOfTheseClasses )
    {
        return new Predicate<Throwable>()
        {
            @Override
            public boolean accept( Throwable item )
            {
                for ( Class cls : anyOfTheseClasses )
                {
                    if ( cls.isInstance( item ) )
                    {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    public static <E extends Throwable> E combine( E first, E second )
    {
        if ( first == null )
        {
            return second;
        }
        if ( second == null )
        {
            return first;
        }

        Throwable current = first;
        while ( current.getCause() != null )
        {
            current = current.getCause();
        }

        current.initCause( second );
        return first;
    }
}
