/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
