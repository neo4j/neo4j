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
package org.neo4j.io;

import java.io.IOException;
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
        Exception closeException = null;
        for ( T closeable : closeables )
        {
            if ( closeable != null )
            {
                try
                {
                    closeable.close();
                }
                catch ( Exception e )
                {
                    if ( closeException == null )
                    {
                        closeException = e;
                    }
                    else
                    {
                        closeException.addSuppressed( e );
                    }
                }
            }
        }
        if ( closeException != null )
        {
            throw new IOException( "Exception closing multiple resources", closeException );
        }
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
}
