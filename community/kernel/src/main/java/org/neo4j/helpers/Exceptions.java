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
package org.neo4j.helpers;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.Thread.State;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import org.neo4j.kernel.impl.locking.Locks.Client;

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

    public static <T extends Throwable> T withSuppressed( T exception, Throwable... suppressed )
    {
        if ( suppressed != null )
        {
            for ( Throwable s : suppressed )
            {
                exception.addSuppressed( s );
            }
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
     * @deprecated use {@link #peel(Throwable, org.neo4j.function.Predicate)} instead
     */
    @Deprecated
    public static Throwable peel( Throwable exception, Predicate<Throwable> toPeel )
    {
        return peel( exception, Predicates.upgrade( toPeel ) );
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
     * @param toPeel {@link org.neo4j.function.Predicate} for deciding what to peel. {@code true} means
     * to peel (i.e. remove), whereas the first {@code false} means stop and return.
     * @return the delegate cause of an exception, dictated by the predicate.
     */
    public static Throwable peel( Throwable exception, org.neo4j.function.Predicate<Throwable> toPeel )
    {
        while ( exception != null )
        {
            if ( !toPeel.test( exception ) )
            {
                break;
            }
            exception = exception.getCause();
        }
        return exception;
    }

    /**
     * @deprecated use {@link org.neo4j.function.Predicates#instanceOfAny(Class[])} instead
     * @param types the exception types to check against
     * @return a predicate which determines if a {@link Throwable} is among the given types
     */
    @Deprecated
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

    @Deprecated
    public static Predicate<Throwable> exceptionWithMessage( final String message )
    {
        return new Predicate<Throwable>()
        {
            @Override
            public boolean accept( Throwable item )
            {
                return item.getMessage() != null && item.getMessage().equals( message );
            }
        };
    }

    public static Predicate<Throwable> exceptionsWithMessageContaining( final String message )
    {
        return new Predicate<Throwable>()
        {
            @Override
            public boolean accept( Throwable item )
            {
                return item.getMessage() != null && item.getMessage().contains( message );
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
        final org.neo4j.function.Predicate<Throwable> anyOfClasses = org.neo4j.function.Predicates.instanceOfAny( anyOfTheseClasses );
        return contains( cause, new org.neo4j.function.Predicate<Throwable>()
        {
            @Override
            public boolean test( Throwable item )
            {
                return item.getMessage() != null && item.getMessage().contains( containsMessage ) &&
                        anyOfClasses.test( item );
            }
        } );
    }

    public static boolean contains( Throwable cause, Class... anyOfTheseClasses )
    {
        return contains( cause, org.neo4j.function.Predicates.<Throwable>instanceOfAny( anyOfTheseClasses ) );
    }

    /**
     * @deprecated use {@link #contains(Throwable, org.neo4j.function.Predicate)} instead
     * @param cause the cause we have
     * @param toLookFor predicate for the cause we are looking for
     * @return {@code true} if the cause was found
     */
    @Deprecated
    public static boolean contains( Throwable cause, Predicate<Throwable> toLookFor )
    {
        return contains( cause, Predicates.upgrade( toLookFor ) );
    }

    public static boolean contains( Throwable cause, org.neo4j.function.Predicate<Throwable> toLookFor )
    {
        while ( cause != null )
        {
            if ( toLookFor.test( cause ) )
            {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * @deprecated use {@link org.neo4j.function.Predicates#instanceOfAny(Class[])} instead
     * @param anyOfTheseClasses classes to match against
     * @return a predicate which yields {@code true} if an item is an instance of any of the given classes
     */
    @Deprecated
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

    private static final Field THROWABLE_MESSAGE_FIELD;
    static
    {
        try
        {
            THROWABLE_MESSAGE_FIELD = Throwable.class.getDeclaredField( "detailMessage" );
            THROWABLE_MESSAGE_FIELD.setAccessible( true );
        }
        catch ( Exception e )
        {
            throw new LinkageError( "Could not get Throwable message field", e );
        }
    }

    public static <T extends Throwable> T withMessage( T cause, String message )
    {
        try
        {
            THROWABLE_MESSAGE_FIELD.set( cause, message );
            return cause;
        }
        catch ( IllegalArgumentException | IllegalAccessException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Deprecated
    public static Predicate<StackTraceElement> classImplementingInterface( final Class<Client> cls )
    {
        return new Predicate<StackTraceElement>()
        {
            @Override
            public boolean accept( StackTraceElement item )
            {
                try
                {
                    for ( Class<?> interfaceClass : Class.forName( item.getClassName() ).getInterfaces() )
                    {
                        if ( interfaceClass.equals( cls ) )
                        {
                            return true;
                        }
                    }
                    return false;
                }
                catch ( ClassNotFoundException e )
                {
                    return false;
                }
            }
        };
    }

    @Deprecated
    public static boolean containsStackTraceElement( Throwable cause,
            final Predicate<StackTraceElement> predicate )
    {
        return contains( cause, new org.neo4j.function.Predicate<Throwable>()
        {
            @Override
            public boolean test( Throwable item )
            {
                for ( StackTraceElement element : item.getStackTrace() )
                {
                    if ( predicate.accept( element ) )
                    {
                        return true;
                    }
                }
                return false;
            }
        } );
    }

    @Deprecated
    public static Predicate<StackTraceElement> forMethod( final String name )
    {
        return new Predicate<StackTraceElement>()
        {
            @Override
            public boolean accept( StackTraceElement item )
            {
                return item.getMethodName().equals( name );
            }
        };
    }

    /**
     * @deprecated use {@link #briefOneLineStackTraceInformation(org.neo4j.function.Predicate)} instead
     * @param toInclude predicate which decides which stack trace elements to include
     * @return the filtered brief stack traces
     */
    @Deprecated
    public static String briefOneLineStackTraceInformation( Predicate<StackTraceElement> toInclude )
    {
        return briefOneLineStackTraceInformation( Predicates.upgrade( toInclude ) );
    }

    public static String briefOneLineStackTraceInformation( org.neo4j.function.Predicate<StackTraceElement> toInclude )
    {
        StringBuilder builder = new StringBuilder();
        for ( StackTraceElement element : Thread.currentThread().getStackTrace() )
        {
            if ( toInclude.test( element ) )
            {
                builder.append( builder.length() > 0 ? "," : "" )
                        .append( simpleClassName( element.getClassName() ) + "#" + element.getMethodName() );
            }
        }
        return builder.toString();
    }

    private static String simpleClassName( String className )
    {
        return className.substring( className.lastIndexOf( '.' ) );
    }
}
