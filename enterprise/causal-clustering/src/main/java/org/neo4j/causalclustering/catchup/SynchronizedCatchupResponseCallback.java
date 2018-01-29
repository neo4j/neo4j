/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.causalclustering.catchup.storecopy.FileChunk;
import org.neo4j.causalclustering.catchup.storecopy.FileHeader;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.tx.TxPullResponse;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.function.ThrowingSupplier;

/**
 * Synchronizes the resource usage between two threads, here called
 * the manager and the request handler. The allowed state transitions
 * together with the party responsible for each is depicted in the diagrams
 * below.
 *
 *<p>
 * M: Manager
 * <br>
 * R: Request handler
 *
 * <pre>
 *
 * ----------------------------------------------
 * |    \ from |  IDLE   ACTIVE   ABORT   DONE  |
 * | to  \     |                                |
 * | -------------------------------------------|
 * | IDLE      |   -       R        -      -    |
 * | ACTIVE    |   R       -        -      -    |
 * | ABORT     |   -       M        -      -    |
 * | DONE      |   M       -        R      -    |
 * ----------------------------------------------
 * </pre>
 * <p>
 * The main pattern is that the Request handler (R) switches between
 * IDLE <-> ACTIVE when handling requests, and the manager finalizes
 * the interactions by moving from IDLE->ABORT or IDLE->DONE.
 *<p>
 * In the ABORT state it is the responsibility of the Request handler
 * to move the state to DONE after which no more usage of the resource
 * will be possible.
 *
 * <pre>
 *           R
 * IDLE <-------> ACTIVE
 *  |               |
 *  | M             | M
 *  v        R      v
 * DONE <-------- ABORT
 * </pre>
 *
 * @param <T>
 */
public class SynchronizedCatchupResponseCallback<T> implements CatchUpResponseCallback<T>
{
    private static final long IDLE = 0;
    private static final long ACTIVE = 1;
    private static final long ABORT = 2;
    private static final long DONE = 3;

    private final CatchUpResponseCallback<T> delegate;
    private final AtomicLong state = new AtomicLong( IDLE );

    SynchronizedCatchupResponseCallback( CatchUpResponseCallback<T> delegate )
    {
        this.delegate = delegate;
    }

    private void synchronize( Runnable operation )
    {
        synchronize( () -> {
            operation.run();
            return null;
        } );
    }

    private <R, E extends Exception> R synchronize( ThrowingSupplier<R,E> operation ) throws E
    {
        boolean activated = state.compareAndSet( IDLE, ACTIVE );

        if ( !activated )
        {
            if ( state.get() == ACTIVE )
            {
                throw new IllegalStateException( "Already active." );
            }
            return null;
        }

        try
        {
            return operation.get();
        }
        finally
        {
            if ( !state.compareAndSet( ACTIVE, IDLE ) )
            {
                if ( !state.compareAndSet( ABORT, DONE ) )
                {
                    //noinspection ThrowFromFinallyBlock
                    throw new IllegalStateException( "Could not finalize abort from state: " + state.get() );
                }
            }
        }
    }

    @Override
    public void onFileHeader( CompletableFuture<T> signal, FileHeader fileHeader )
    {
        synchronize( () -> delegate.onFileHeader( signal, fileHeader ) );
    }

    @Override
    public boolean onFileContent( CompletableFuture<T> signal, FileChunk fileChunk ) throws IOException
    {
        return synchronize( () -> delegate.onFileContent( signal, fileChunk ) );
    }

    @Override
    public void onFileStreamingComplete( CompletableFuture<T> signal, StoreCopyFinishedResponse response )
    {
        synchronize( () -> delegate.onFileStreamingComplete( signal, response ) );
    }

    @Override
    public void onTxPullResponse( CompletableFuture<T> signal, TxPullResponse tx )
    {
        synchronize( () -> delegate.onTxPullResponse( signal, tx ) );
    }

    @Override
    public void onTxStreamFinishedResponse( CompletableFuture<T> signal, TxStreamFinishedResponse response )
    {
        synchronize( () -> delegate.onTxStreamFinishedResponse( signal, response ) );
    }

    @Override
    public void onGetStoreIdResponse( CompletableFuture<T> signal, GetStoreIdResponse response )
    {
        synchronize( () -> delegate.onGetStoreIdResponse( signal, response ) );
    }

    @Override
    public void onCoreSnapshot( CompletableFuture<T> signal, CoreSnapshot coreSnapshot )
    {
        synchronize( () -> delegate.onCoreSnapshot( signal, coreSnapshot ) );
    }

    void abortAndAwaitDone()
    {
        while ( state.get() < ABORT )
        {
            if ( !state.compareAndSet( IDLE, DONE ) )
            {
                state.compareAndSet( ACTIVE, ABORT );
            }
        }

        if ( ! state.compareAndSet( IDLE, DONE ) )
        {
            do
            {
                LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
                state.compareAndSet( IDLE, DONE );
            }
            while ( state.get() != DONE );
        }
    }
}
