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
package org.neo4j.io;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;

/**
 * IO helper methods.
 */
public final class IOUtils
{
    private IOUtils()
    {
    }

    /**
     * Closes given {@link Collection collection} of {@link AutoCloseable closeables}.
     *
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     * @throws IOException
     * @see #closeAll(AutoCloseable[])
     */
    public static <T extends AutoCloseable> void closeAll( Collection<T> closeables ) throws IOException
    {
        closeAll( closeables.toArray( new AutoCloseable[closeables.size()] ) );
    }

    /**
     * Closes given {@link Collection collection} of {@link AutoCloseable closeables} ignoring all exceptions.
     *
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     * @see #closeAll(AutoCloseable[])
     */
    public static <T extends AutoCloseable> void closeAllSilently( Collection<T> closeables )
    {
        try
        {
            closeAll( closeables );
        }
        catch ( IOException ignored )
        {
        }
    }

    /**
     * Closes given array of {@link AutoCloseable closeables}. If any {@link AutoCloseable#close()} call throws
     * {@link IOException} than it will be rethrown to the caller after calling {@link AutoCloseable#close()}
     * on other given resources. If more than one {@link AutoCloseable#close()} throw than resulting exception will
     * have suppressed exceptions. See {@link Exception#addSuppressed(Throwable)}
     *
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     * @throws IOException
     */
    @SafeVarargs
    public static <T extends AutoCloseable> void closeAll( T... closeables ) throws IOException
    {
        closeAll( IOException.class, closeables );
    }

    /**
     * Closes given array of {@link AutoCloseable closeables} ignoring all exceptions.
     *
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     */
    @SafeVarargs
    public static <T extends AutoCloseable> void closeAllSilently( T... closeables )
    {
        try
        {
            closeAll( closeables );
        }
        catch ( IOException ignored )
        {
        }
    }

    /**
     * Close all given closeables and if something goes wrong throw exception of the given type.
     * Exception class should have a public constructor that accepts {@link String} and {@link Throwable} like
     * {@link RuntimeException#RuntimeException(String, Throwable)}
     *
     * @param throwableClass exception type to throw in case of failure
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     * @param <E> the type of exception
     * @throws E when any {@link AutoCloseable#close()} throws exception
     */
    @SafeVarargs
    public static <T extends AutoCloseable, E extends Throwable> void closeAll( Class<E> throwableClass,
            T... closeables ) throws E
    {
        Throwable closeThrowable = null;
        for ( T closeable : closeables )
        {
            closeThrowable = chainedClose( closeThrowable, closeable );
        }
        chainedCloseFinish( throwableClass, closeThrowable );
    }

    /**
     * This can be used to do the same thing as {@link #closeAll(AutoCloseable[])}, but can be used without needing
     * heap allocations.
     * <p>
     * Use like:
     * <p>
     * <code>
     * Throwable t = null;
     * t = chainedClose(t, resource1);
     * t = chainedClose(t, resource2);
     * chainedCloseFinish(IOException.class, t);
     * </code>
     *
     * @param currentThrowable
     * @param closeable
     * @return
     */
    public static Throwable chainedClose( Throwable currentThrowable, AutoCloseable closeable )
    {
        if ( closeable != null )
        {
            try
            {
                closeable.close();
            }
            catch ( Throwable t )
            {
                if ( currentThrowable == null )
                {
                    currentThrowable = t;
                }
                else
                {
                    currentThrowable.addSuppressed( t );
                }
            }
        }
        return currentThrowable;
    }

    /**
     * Used to complete a {@link #chainedClose(Throwable, AutoCloseable) chained close}.  Throws the given exception
     * type if the provided cause is non-null.
     *
     * @param throwableClass type to throw. If the cause is an instance of this, the cause will get thrown, otherwise
     * it gets wrapped.
     * @param cause exception from {@link #chainedClose(Throwable, AutoCloseable)}
     * @param <E> the type to throw
     * @throws E if cause in non-null
     */
    public static <E extends Throwable> void chainedCloseFinish( Class<E> throwableClass, Throwable cause ) throws E
    {
        if ( cause == null )
        {
            return;
        }

        if ( throwableClass.isInstance( cause ) )
        {
            throw (E) cause;
        }

        throw newThrowable( throwableClass, cause.getMessage(), cause );
    }

    private static <E extends Throwable> E newThrowable( Class<E> throwableClass, String message, Throwable cause )
    {
        try
        {
            Constructor<E> constructor = throwableClass.getConstructor( String.class, Throwable.class );
            return constructor.newInstance( message, cause );
        }
        catch ( Throwable t )
        {
            RuntimeException runtimeException =
                    new RuntimeException( "Unable to create exception to throw. Original message: " + message, t );
            runtimeException.addSuppressed( cause );
            throw runtimeException;
        }
    }
}
