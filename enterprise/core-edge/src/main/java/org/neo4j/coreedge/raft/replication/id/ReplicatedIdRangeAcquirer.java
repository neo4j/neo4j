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
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class ReplicatedIdRangeAcquirer
{
    private final Replicator replicator;
    private final ReplicatedIdAllocationStateMachine idAllocationStateMachine;

    private final int allocationChunk;
    private final long retryTimeoutMillis;

    private final CoreMember me;
    private final Log log;

    public ReplicatedIdRangeAcquirer( Replicator replicator, ReplicatedIdAllocationStateMachine idAllocationStateMachine, int allocationChunk, long retryTimeoutMillis, CoreMember me, LogProvider logProvider )
    {
        this.replicator = replicator;
        this.idAllocationStateMachine = idAllocationStateMachine;
        this.allocationChunk = allocationChunk;
        this.retryTimeoutMillis = retryTimeoutMillis;
        this.me = me;
        this.log = logProvider.getLog( getClass() );
    }

    public IdAllocation acquireIds( IdType idType )
    {
        while ( true )
        {
            long firstNotAllocated = idAllocationStateMachine.getFirstNotAllocated( idType );
            ReplicatedIdAllocationRequest idAllocationRequest = new ReplicatedIdAllocationRequest( me, idType, firstNotAllocated, allocationChunk );

            try
            {
                replicator.replicate( idAllocationRequest );
            }
            catch ( Replicator.ReplicationFailedException e )
            {
                log.error( format( "Failed to acquire id range for idType %s", idType ), e );
                throw new IdGenerationException( e );
            }

            try
            {
                idAllocationStateMachine.waitForAnyChange( retryTimeoutMillis );
            }
            catch ( InterruptedException e )
            {
                log.error( format( "Failed to acquire id range for idType %s", idType ), e );
                throw new IdGenerationException( e );
            }

            IdRange myHighestIdRange = idAllocationStateMachine.getHighestIdRange( me, idType );

            if ( myHighestIdRange != null && myHighestIdRange.getRangeStart() == idAllocationRequest.idRangeStart() )
            {
                // TODO why are we subtracting 1?
                return new IdAllocation( myHighestIdRange, myHighestIdRange.getRangeStart() - 1, 0 );
            }
        }
    }
}
