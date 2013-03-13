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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.kernel.api.index.NodePropertyUpdate;

/**
 * IndexContext layer that enforces the dynamic contract of IndexContext (cf. Test)
 *
 * @see org.neo4j.kernel.impl.api.index.IndexProxy
 */
public class ContractCheckingIndexProxy extends DelegatingIndexProxy
{
    /**
     * State machine for IndexContexts
     *
     * The logic of ContractCheckingIndexContext hinges on the fact that all states
     * are always entered and checked in this order (States may be skipped though):
     *
     * INIT > CREATING > CREATED > CLOSED
     *
     * Valid state transitions are:
     *
     * INIT -[:create]-> CREATING -[:implicit]-> CREATED -[:close|:drop]-> CLOSED
     * INIT -[:close] -> CLOSED
     *
     * Additionally, ContractCheckingIndexContext keeps track of the number of open
     * calls that started in CREATED state and are still running.  This allows us
     * to prevent calls to close() or drop() to go through while there are pending
     * commits.
     **/
    private static enum State {
        INIT,
        CREATING,
        CREATED,
        CLOSED
    }

    private final AtomicReference<State> state;
    private final AtomicInteger openCalls;

    public ContractCheckingIndexProxy( boolean created, IndexProxy delegate )
    {
        super( delegate );
        this.state =  new AtomicReference<State>( created ? State.CREATED : State.INIT );
        this.openCalls = new AtomicInteger( 0 );
    }

    ContractCheckingIndexProxy( IndexProxy delegate )
    {
        this(false, delegate);
    }

    @Override
    public void create() throws IOException
    {
        if ( state.compareAndSet( State.INIT, State.CREATING ) )
        {
            try
            {
                super.create();
            }
            finally
            {
                state.set( State.CREATED );
            }
        }
        else
        {
            throw new IllegalStateException( "IndexContext can only create index initially" );
        }
    }


    @Override
    public void update( Iterable<NodePropertyUpdate> updates ) throws IOException
    {
        openCall( "update" );
        try
        {
            super.update( updates );
        }
        finally
        {
            closeCall();
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

        if ( State.CREATING.equals( state.get() ) )
            throw new IllegalStateException( "Concurrent drop while creating index" );

        if ( state.compareAndSet( State.CREATED, State.CLOSED ) )
        {
            ensureNoOpenCalls( "drop" );
            return super.drop();
        }

        throw new IllegalStateException( "IndexContext already closed" );
    }

    @Override
    public Future<Void> close() throws IOException
    {
        if ( state.compareAndSet( State.INIT, State.CLOSED ) )
            return super.close();

        if ( state.compareAndSet( State.CREATING, State.CLOSED ) )
            throw new IllegalStateException( "Concurrent close while creating index" );

        if ( state.compareAndSet( State.CREATED, State.CLOSED ) )
        {
            ensureNoOpenCalls( "close" );
            return super.close();
        }

        throw new IllegalStateException( "IndexContext already closed" );
    }

    private void openCall( String name )
    {
        // do not open call unless we are in CREATED
        if ( State.CREATED.equals( state.get() ) )
        {
            // increment openCalls for closers to see
            openCalls.incrementAndGet();
            // ensure that the previous increment actually gets seen by closers
            if ( State.CLOSED.equals( state.get() ) )
                throw new IllegalStateException("Cannot call " + name + "() after index has been closed" );
        }
        else
            throw new IllegalStateException("Cannot call " + name + "() before index has been created" );
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
