/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.id.IdRange;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

/**
 * This state machine keeps a track of all id-allocations for all id-types and allows a local
 * user to wait for any changes to the state.
 *
 * The responsibilities of the state machine are to:
 *  - keep state
 *  - update state on events
 *  - support waiting for changes to the state
 *
 *  Users of this state machine should in general act in accordance with the following formula:
 *    1) trigger update of replicated state
 *    2) wait for change to propagate
 *    3) if state is not sufficient goto 1)
 *    4) state sufficient => done
 */
public class ReplicatedIdAllocationStateMachine implements Replicator.ReplicatedContentListener
{
    private final CoreMember me;
    private final IdAllocationState idAllocationState;

    public ReplicatedIdAllocationStateMachine( CoreMember me, IdAllocationState idAllocationState )
    {
        this.me = me;
        this.idAllocationState = idAllocationState;
    }

    public synchronized long getFirstNotAllocated( IdType idType )
    {
        return idAllocationState.firstNotAllocated(idType);
    }

    public synchronized IdRange getHighestIdRange( CoreMember owner, IdType idType )
    {
        if ( owner != me )
        {
            /* We just keep our own latest id-range for simplicity and efficiency. */
            throw new UnsupportedOperationException();
        }

        int idRangeLength = idAllocationState.lastIdRangeLengthForMe(idType);

        if ( idRangeLength > 0 )
        {
            return new IdRange( EMPTY_LONG_ARRAY, idAllocationState.lastIdRangeStartForMe(idType), idRangeLength );
        }
        else
        {
            return null;
        }
    }

    private void updateFirstNotAllocated( IdType idType, long idRangeEnd )
    {
        idAllocationState.firstNotAllocated(idType, idRangeEnd);
        notifyAll();
    }

    @Override
    public synchronized void onReplicated( ReplicatedContent content, long logIndex )
    {
        if ( content instanceof ReplicatedIdAllocationRequest )
        {
            ReplicatedIdAllocationRequest request = (ReplicatedIdAllocationRequest) content;

            IdType idType = request.idType();

            if ( request.idRangeStart() == idAllocationState.firstNotAllocated(idType) )
            {
                if( request.owner().equals( me ) )
                {
                    idAllocationState.lastIdRangeStartForMe(idType, request.idRangeStart());
                    idAllocationState.lastIdRangeLengthForMe(idType, request.idRangeLength());
                }
                updateFirstNotAllocated( idType, request.idRangeStart() + request.idRangeLength() );
            }
        }
    }

    public synchronized void waitForAnyChange( long timeoutMillis ) throws InterruptedException
    {
        wait( timeoutMillis );
    }
}
