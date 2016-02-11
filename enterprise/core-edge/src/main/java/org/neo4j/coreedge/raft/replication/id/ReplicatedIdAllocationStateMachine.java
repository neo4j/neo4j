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

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.coreedge.raft.state.StateStorage;
import org.neo4j.coreedge.raft.state.id_allocation.IdAllocationState;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

/**
 * This state machine keeps a track of all id-allocations for all id-types and allows a local
 * user to wait for any changes to the state.
 * <p>
 * The responsibilities of the state machine are to:
 * - keep state
 * - update state on events
 * - support waiting for changes to the state
 * <p>
 * Users of this state machine should in general act in accordance with the following formula:
 * 1) trigger update of replicated state
 * 2) wait for change to propagate
 * 3) if state is not sufficient goto 1)
 * 4) state sufficient => done
 */
public class ReplicatedIdAllocationStateMachine implements StateMachine
{
    private final CoreMember me;
    private final StateStorage<IdAllocationState> storage;
    private IdAllocationState idAllocationState;
    private final Log log;

    public ReplicatedIdAllocationStateMachine( CoreMember me, StateStorage<IdAllocationState> storage,
                                               LogProvider logProvider )
    {
        this.me = me;
        this.storage = storage;
        this.idAllocationState = storage.getInitialState();
        this.log = logProvider.getLog( getClass() );
    }

    public synchronized long getFirstNotAllocated( IdType idType )
    {
        return idAllocationState.firstUnallocated( idType );
    }

    public synchronized IdRange getHighestIdRange( CoreMember owner, IdType idType )
    {
        if ( owner != me )
        {
            /* We just keep our own latest id-range for simplicity and efficiency. */
            throw new UnsupportedOperationException();
        }

        int idRangeLength = idAllocationState.lastIdRangeLength( idType );

        if ( idRangeLength > 0 )
        {
            return new IdRange( EMPTY_LONG_ARRAY, idAllocationState.lastIdRangeStart( idType ), idRangeLength );
        }
        else
        {
            return null;
        }
    }

    private void updateFirstNotAllocated( IdType idType, long idRangeEnd )
    {
        idAllocationState.firstUnallocated( idType, idRangeEnd );
        notifyAll();
    }

    @Override
    public synchronized void applyCommand( ReplicatedContent content, long logIndex )
    {
        if ( content instanceof ReplicatedIdAllocationRequest )
        {
            if ( logIndex > idAllocationState.logIndex() )
            {
                ReplicatedIdAllocationRequest request = (ReplicatedIdAllocationRequest) content;

                IdType idType = request.idType();

                if ( request.idRangeStart() == idAllocationState.firstUnallocated( idType ) )
                {
                    if ( request.owner().equals( me ) )
                    {
                        idAllocationState.lastIdRangeStart( idType, request.idRangeStart() );
                        idAllocationState.lastIdRangeLength( idType, request.idRangeLength() );
                    }
                    updateFirstNotAllocated( idType, request.idRangeStart() + request.idRangeLength() );

                }
                idAllocationState.logIndex( logIndex );
            }
            else
            {
                log.info( "Ignoring content at index %d, since already applied up to %d",
                        logIndex, idAllocationState.logIndex() );
            }
        }
    }

    public synchronized void waitForAnyChange( long timeoutMillis ) throws InterruptedException
    {
        wait( timeoutMillis );
    }

    @Override
    public void flush() throws IOException
    {
        IdAllocationState copy = new IdAllocationState( idAllocationState );
        storage.persistStoreData( copy );
    }
}
