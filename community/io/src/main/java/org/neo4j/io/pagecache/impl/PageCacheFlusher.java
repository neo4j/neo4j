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
package org.neo4j.io.pagecache.impl;

import java.io.Flushable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;

import static org.neo4j.helpers.Exceptions.launderedException;

/**
 * A dedicated thread which constantly call {@link PageCache#flushAndForce()} until a call to {@link #halt()} is made.
 * Must be started manually by calling {@link #start()}.
 */
public class PageCacheFlusher extends Thread implements IOLimiter
{
    private final PageCache pageCache;
    private final AtomicReference<Throwable> errorRef;
    private volatile boolean halted;

    public PageCacheFlusher( PageCache pageCache )
    {
        Objects.requireNonNull( pageCache );
        this.pageCache = pageCache;
        errorRef = new AtomicReference<>();
    }

    @Override
    public final void run()
    {
        while ( !halted )
        {
            try
            {
                pageCache.flushAndForce( this );
            }
            catch ( HaltFlushException ignore )
            {
                break; // This exception means we've been asked to stop flushing.
            }
            catch ( Throwable e )
            {
                errorRef.set( e );
                break;
            }
        }
    }

    /**
     * Halts this flusher, making it stop flushing. The current call to {@link PageCache#flushAndForce()}
     * will complete before exiting this method call. If there was an error in the thread doing the flushes
     * that exception will be thrown from this method as a {@link RuntimeException}.
     */
    public void halt()
    {
        halted = true;
        try
        {
            join();
        }
        catch ( InterruptedException e )
        {
            // We can't quite decide what to do with an interrupt here, and we don't know if it was aimed at us or not,
            // so let's just pass it on.
            Thread.currentThread().interrupt();
        }
        Throwable error = errorRef.getAndSet( null );
        if ( error != null )
        {
            throw launderedException( error );
        }
    }

    @Override
    public long maybeLimitIO( long previousStamp, int recentlyCompletedIOs, Flushable flushable ) throws IOException
    {
        if ( halted )
        {
            throw new HaltFlushException();
        }
        return 0;
    }

    private static final class HaltFlushException extends IOException
    {
    }
}
