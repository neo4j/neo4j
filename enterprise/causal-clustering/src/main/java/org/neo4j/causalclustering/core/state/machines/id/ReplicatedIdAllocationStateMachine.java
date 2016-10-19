/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.id;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.StateMachine;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.kernel.impl.store.id.IdType;

/**
 * Keeps track of global id allocations for all members.
 */
public class ReplicatedIdAllocationStateMachine implements StateMachine<ReplicatedIdAllocationRequest>
{
    private final StateStorage<IdAllocationState> storage;
    private IdAllocationState state;

    public ReplicatedIdAllocationStateMachine( StateStorage<IdAllocationState> storage )
    {
        this.storage = storage;
    }

    @Override
    public synchronized void applyCommand( ReplicatedIdAllocationRequest request, long commandIndex,
            Consumer<Result> callback )
    {
        if ( commandIndex <= state().logIndex() )
        {
            return;
        }

        state().logIndex( commandIndex );

        IdType idType = request.idType();

        boolean requestAccepted = request.idRangeStart() == firstUnallocated( idType );
        if ( requestAccepted )
        {
            state().firstUnallocated( idType, request.idRangeStart() + request.idRangeLength() );
        }

        callback.accept( Result.of( requestAccepted ) );
    }

    synchronized long firstUnallocated( IdType idType )
    {
        return state().firstUnallocated( idType );
    }

    @Override
    public void flush() throws IOException
    {
        storage.persistStoreData( state() );
    }

    @Override
    public long lastAppliedIndex()
    {
        return state().logIndex();
    }

    private IdAllocationState state()
    {
        if ( state == null )
        {
            state = storage.getInitialState();
        }
        return state;
    }

    public synchronized IdAllocationState snapshot()
    {
        return state().newInstance();
    }

    public synchronized void installSnapshot( IdAllocationState snapshot )
    {
        state = snapshot;
    }
}
