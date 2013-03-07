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

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IndexContext layer that enforces the dynamic contract of IndexContext
 *
 * <ul>
 *     <li>The index may not be created twice</li>
 *     <li>The context may not be closed twice</li>
 *     <li>Close or drop both close the context</li>
 *     <li>The index may not be dropped before it has been created</li>
 * </ul>
 *
 */
public class ContractCheckingIndexContext extends DelegatingIndexContext
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
     **/
    private static enum State {
        INIT,
        CREATING,
        CREATED,
        CLOSED
    }


    private final AtomicReference<State> state = new AtomicReference<State>( State.INIT );

    public ContractCheckingIndexContext( IndexContext delegate )
    {
        super( delegate );
    }

    @Override
    public void create()
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
    public Future<Void> drop()
    {
        if ( state.compareAndSet( State.INIT, State.CLOSED ) )
            return super.drop();

        if ( State.CREATING.equals( state.get() ) )
            throw new IllegalStateException( "Concurrent drop while creating index" );

        if ( state.compareAndSet( State.CREATED, State.CLOSED ) )
            return super.drop();

        throw new IllegalStateException( "IndexContext already closed" );
    }

    @Override
    public Future<Void> close()
    {
        if ( state.compareAndSet( State.INIT, State.CLOSED ) )
            return super.close();

        if ( state.compareAndSet( State.CREATING, State.CLOSED ) )
            throw new IllegalStateException( "Concurrent close while creating index" );

        if ( state.compareAndSet( State.CREATED, State.CLOSED ) )
            return super.close();

        throw new IllegalStateException( "IndexContext already closed" );
    }
}
