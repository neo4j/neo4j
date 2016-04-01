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
package org.neo4j.coreedge.raft.replication.id;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.coreedge.raft.state.Result;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.coreedge.raft.state.StateStorage;
import org.neo4j.coreedge.raft.state.id_allocation.IdAllocationState;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Keeps track of global id allocations for all members.
 */
public class ReplicatedIdAllocationStateMachine implements StateMachine<ReplicatedIdAllocationRequest>
{
    private final StateStorage<IdAllocationState> storage;
    private IdAllocationState idAllocationState;
    private final Log log;

    public ReplicatedIdAllocationStateMachine( StateStorage<IdAllocationState> storage, LogProvider logProvider )
    {
        this.storage = storage;
        this.idAllocationState = storage.getInitialState();
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public synchronized Optional<Result> applyCommand( ReplicatedIdAllocationRequest request, long commandIndex )
    {
        if( commandIndex <= idAllocationState.logIndex() )
        {
            return Optional.empty();
        }
        idAllocationState.logIndex( commandIndex );

        IdType idType = request.idType();

        if ( request.idRangeStart() == firstUnallocated( idType ) )
        {
            idAllocationState.firstUnallocated( idType, request.idRangeStart() + request.idRangeLength() );
            return Optional.of( Result.of( true ) );
        }
        else
        {
            return Optional.of( Result.of( false ) );
        }
    }

    synchronized long firstUnallocated( IdType idType )
    {
        return idAllocationState.firstUnallocated( idType );
    }

    @Override
    public void flush() throws IOException
    {
        storage.persistStoreData( idAllocationState );
    }

    public synchronized IdAllocationState snapshot()
    {
        return idAllocationState;
    }

    public synchronized void installSnapshot( IdAllocationState snapshot )
    {
        idAllocationState = snapshot;
    }
}
