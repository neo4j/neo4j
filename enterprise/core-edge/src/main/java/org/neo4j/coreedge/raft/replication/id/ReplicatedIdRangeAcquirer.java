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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

/**
 * Replicates commands to assign next available id range to this member.
 */
public class ReplicatedIdRangeAcquirer
{
    private final Replicator replicator;
    private final ReplicatedIdAllocationStateMachine idAllocationStateMachine;

    private final int allocationChunk;

    private final CoreMember me;
    private final Log log;

    public ReplicatedIdRangeAcquirer(
            Replicator replicator, ReplicatedIdAllocationStateMachine idAllocationStateMachine,
            int allocationChunk, CoreMember me, LogProvider logProvider )
    {
        this.replicator = replicator;
        this.idAllocationStateMachine = idAllocationStateMachine;
        this.allocationChunk = allocationChunk;
        this.me = me;
        this.log = logProvider.getLog( getClass() );
    }

    public IdAllocation acquireIds( IdType idType ) throws InterruptedException
    {
        while ( true )
        {
            long firstUnallocated = idAllocationStateMachine.firstUnallocated( idType );
            ReplicatedIdAllocationRequest idAllocationRequest = new ReplicatedIdAllocationRequest(
                    me, idType, firstUnallocated, allocationChunk );

            Future<Object> futureResult = replicator.replicate( idAllocationRequest, true );

            try
            {
                boolean success = (boolean) futureResult.get();
                if( success )
                {
                    IdRange idRange = new IdRange( EMPTY_LONG_ARRAY, firstUnallocated, allocationChunk );
                    return new IdAllocation( idRange, -1, 0 );
                }
                else
                {
                    log.info( "Retrying ID generation due to conflict. Request was: " + idAllocationRequest );
                }
            }
            catch ( InterruptedException | ExecutionException e )
            {
                log.error( format( "Failed to acquire id range for idType %s", idType ), e );
                throw new IdGenerationException( e );
            }
        }
    }
}
