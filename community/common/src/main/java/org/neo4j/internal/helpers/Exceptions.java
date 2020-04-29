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
package org.neo4j.internal.helpers;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.Thread.State;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static org.neo4j.function.Predicates.instanceOfAny;

public final class Exceptions
{
    public static final UncaughtExceptionHandler SILENT_UNCAUGHT_EXCEPTION_HANDLER = ( t, e ) ->
    {   // Don't print about it
    };

    private Exceptions()
    {
        throw new AssertionError( "No instances" );
    }

    /**
     * Rethrows {@code exception} if it is an instance of {@link RuntimeException} or {@link Error}. Typical usage is:
     *
     * <pre>
     * catch (Throwable e) {
     *   ......common code......
     *   throwIfUnchecked(e);
     *   throw new RuntimeException(e);
     * }
     * </pre>
     *
     * This will only wrap checked exception in a {@code RuntimeException}. Do note that if the segment {@code common code}
     * is missing, it's preferable to use this instead:
     *
     * <pre>
     * catch (RuntimeException | Error e) {
     *   throw e;
     * }
     * catch (Throwable e) {
     *   throw new RuntimeException(e);
     * }
     * </pre>
     *
     * @param exception to rethrow.
     */
    public static void throwIfUnchecked( Throwable exception )
    {
        Objects.requireNonNull( exception );
        if ( exception instanceof RuntimeException )
        {
            throw (RuntimeException) exception;
        }
        if ( exception instanceof Error )
        {
            throw (Error) exception;
        }
    }

    /**
     * Will rethrow the provided {@code exception} if it is an instance of {@code clazz}. Typical usage is:
     *
     * <pre>
     * catch (Throwable e) {
     *   ......common code......
     *   throwIfInstanceOf(e, BarException.class);
     *   throw new RuntimeException(e);
     * }
     * </pre>
     *
     * This will filter out all {@code BarExceptions} and wrap the rest in {@code RuntimeException}. Do note that if the
     * segment {@code common code} is missing, it's preferable to use this instead:
     *
     * <pre>
     * catch (BarException e) {
     *   throw e;
     * } catch (Throwable e) {
     *   threw new RuntimeException(e);
     * }
     * </pre>
     *
     * @param exception to rethrow.
     * @param clazz a {@link Class} instance to check against.
     * @param <T> type thrown, if thrown at all.
     * @throws T if {@code exception} is an instance of {@code clazz}.
     */
    public static <T extends Throwable> void throwIfInstanceOf( Throwable exception, Class<T> clazz ) throws T
    {
        Objects.requireNonNull( exception );
        if ( clazz.isInstance( exception ) )
        {
            throw clazz.cast( exception );
        }
    }

    /**
     * Searches the entire exception hierarchy of causes and suppressed exceptions against the given predicate.
     *
     * @param e exception to start searching from.
     * @return the first exception found matching the predicate.
     */
    public static Optional<Throwable> findCauseOrSuppressed( Throwable e, Predicate<Throwable> predicate )
    {
        if ( e == null )
        {
            return Optional.empty();
        }
        if ( predicate.test( e ) )
        {
            return Optional.of( e );
        }
        if ( e.getCause() != null && e.getCause() != e )
        {
            Optional<Throwable> cause = findCauseOrSuppressed( e.getCause(), predicate );
            if ( cause.isPresent() )
            {
                return cause;
            }
        }
        if ( e.getSuppressed() != null )
        {
            for ( Throwable suppressed : e.getSuppressed() )
            {
                if ( suppressed == e )
                {
                    continue;
                }
                Optional<Throwable> cause = findCauseOrSuppressed( suppressed, predicate );
                if ( cause.isPresent() )
                {
                    return cause;
                }
            }
        }
        return Optional.empty();
    }

    public static StackTraceElement[] getPartialStackTrace( int from, int to )
    {
        return StackWalker.getInstance().walk(
                s -> s.skip( from ).limit( to - from ).map( StackWalker.StackFrame::toStackTraceElement )
                        .toArray( StackTraceElement[]::new ) );
    }

    public static String stringify( Throwable throwable )
    {
        if ( throwable == null )
        {
            return null;
        }

        StringWriter stringWriter = new StringWriter();
        throwable.printStackTrace( new PrintWriter( stringWriter ) );
        return stringWriter.toString();
    }

    public static String stringify( Thread thread, StackTraceElement[] elements )
    {
        StringBuilder builder = new StringBuilder()
                .append( '"' ).append( thread.getName() ).append( '"' ).append( thread.isDaemon() ? " daemon" : "" )
                .append( " prio=" ).append( thread.getPriority() )
                .append( " tid=" ).append( thread.getId() )
                .append( ' ' ).append( thread.getState().name().toLowerCase() ).append( '\n' );
        builder.append( "   " ).append( State.class.getName() ).append( ": " )
                .append( thread.getState().name().toUpperCase() ).append( '\n' );
        for ( StackTraceElement element : elements )
        {
            builder.append( "      at " ).append( element.getClassName() ).append( '.' )
                    .append( element.getMethodName() );
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
                builder.append( '(' ).append( element.getFileName() ).append( ':' ).append( element.getLineNumber() )
                        .append( ')' );
            }
            builder.append( "\n" );
        }
        return builder.toString();
    }

    public static boolean contains( final Throwable cause, final String containsMessage, final Class<?>... anyOfTheseClasses )
    {
        final Predicate<Throwable> anyOfClasses = instanceOfAny( anyOfTheseClasses );
        return contains( cause, item -> item.getMessage() != null && item.getMessage().contains( containsMessage ) && anyOfClasses.test( item ) );
    }

    public static boolean contains( Throwable cause, Predicate<Throwable> toLookFor )
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
        }
        catch ( IllegalArgumentException | IllegalAccessException e )
        {
            throw new RuntimeException( e );
        }
        return cause;
    }

    public static <T extends Throwable> T chain( T initial, T current )
    {
        if ( initial == null )
        {
            return current;
        }

        if ( current != null && initial != current )
        {
            initial.addSuppressed( current );
        }
        return initial;
    }

    public static <EXCEPTION extends Throwable> EXCEPTION disguiseException( Class<EXCEPTION> disguise, String message, Throwable disguised )
    {
        EXCEPTION exception;
        try
        {
            try
            {
                exception = disguise.getConstructor( String.class, Throwable.class )
                        .newInstance( message, disguised );
            }
            catch ( NoSuchMethodException e )
            {
                exception = disguise.getConstructor( String.class ).newInstance( message );
                try
                {
                    exception.initCause( disguised );
                }
                catch ( IllegalStateException ignored )
                {
                }
            }
        }
        catch ( Exception e )
        {
            throw new Error( message + ". An exception of type " + disguise.getName() +
                    " was requested to be thrown but that proved impossible", e );
        }
        return exception;
    }
}
