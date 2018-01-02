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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;

/**
 * {@link IndexProxy} layer that enforces the dynamic contract of {@link IndexProxy} (cf. Test)
 *
 * @see org.neo4j.kernel.impl.api.index.IndexProxy
 */
public class ContractCheckingIndexProxy extends DelegatingIndexProxy
{
    /**
     * State machine for {@link IndexProxy proxies}
     *
     * The logic of {@link ContractCheckingIndexProxy} hinges on the fact that all states
     * are always entered and checked in this order (States may be skipped though):
     *
     * INIT > STARTING > STARTED > CLOSED
     *
     * Valid state transitions are:
     *
     * INIT -[:start]-> STARTING -[:implicit]-> STARTED -[:close|:drop]-> CLOSED
     * INIT -[:close] -> CLOSED
     *
     * Additionally, {@link ContractCheckingIndexProxy} keeps track of the number of open
     * calls that started in STARTED state and are still running.  This allows us
     * to prevent calls to close() or drop() to go through while there are pending
     * commits.
     **/
    private static enum State
    {
        INIT, STARTING, STARTED, CLOSED
    }

    private final AtomicReference<State> state;
    private final AtomicInteger openCalls;

    public ContractCheckingIndexProxy( IndexProxy delegate, boolean started )
    {
        super( delegate );
        this.state = new AtomicReference<>( started ? State.STARTED : State.INIT );
        this.openCalls = new AtomicInteger( 0 );
    }

    @Override
    public void start() throws IOException
    {
        if ( state.compareAndSet( State.INIT, State.STARTING ) )
        {
            try
            {
                super.start();
            }
            finally
            {
                this.state.set( State.STARTED );
            }
        }
        else
        {
            throw new IllegalStateException( "An IndexProxy can only be started once" );
        }
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        if ( IndexUpdateMode.ONLINE == mode )
        {
            openCall( "update" );
            return new DelegatingIndexUpdater( super.newUpdater( mode ) )
            {
                @Override
                public void close() throws IOException, IndexEntryConflictException, IndexCapacityExceededException
                {
                    try
                    {
                        delegate.close();
                    }
                    finally
                    {
                        closeCall();
                    }
                }
            };
        }
        else
        {
            return super.newUpdater( mode );
        }
    }

    @Override
    public void force() throws IOException
    {
        openCall( "force" );
        try
        {
            super.force();
        }
        finally
        {
            closeCall();
        }
    }

    @Override
    public Future<Void> drop() throws IOException
    {
        if ( state.compareAndSet( State.INIT, State.CLOSED ) )
            return super.drop();

        if ( State.STARTING.equals( state.get() ) )
            throw new IllegalStateException( "Concurrent drop while creating index" );

        if ( state.compareAndSet( State.STARTED, State.CLOSED ) )
        {
            ensureNoOpenCalls( "drop" );
            return super.drop();
        }

        throw new IllegalStateException( "IndexProxy already closed" );
    }

    @Override
    public Future<Void> close() throws IOException
    {
        if ( state.compareAndSet( State.INIT, State.CLOSED ) )
            return super.close();

        if ( state.compareAndSet( State.STARTING, State.CLOSED ) )
            throw new IllegalStateException( "Concurrent close while creating index" );

        if ( state.compareAndSet( State.STARTED, State.CLOSED ) )
        {
            ensureNoOpenCalls( "close" );
            return super.close();
        }

        throw new IllegalStateException( "IndexProxy already closed" );
    }

    private void openCall( String name )
    {
        // do not open call unless we are in STARTED
        if ( State.STARTED.equals( state.get() ) )
        {
            // increment openCalls for closers to see
            openCalls.incrementAndGet();
            // ensure that the previous increment actually gets seen by closers
            if ( State.CLOSED.equals( state.get() ) )
                throw new IllegalStateException("Cannot call " + name + "() after index has been closed" );
        }
        else
            throw new IllegalStateException("Cannot call " + name + "() before index has been started" );
    }

    private void ensureNoOpenCalls(String name)
    {
        if (openCalls.get() > 0)
            throw new IllegalStateException( "Concurrent " + name + "() while updates have not completed" );

    }

    private void closeCall()
    {
        // rollback once the call finished or failed
        openCalls.decrementAndGet();
    }
}
