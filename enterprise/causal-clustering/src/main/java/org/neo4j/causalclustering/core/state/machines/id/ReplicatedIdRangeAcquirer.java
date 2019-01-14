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

import java.util.Map;

import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.causalclustering.identity.MemberId;
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

    private final Map<IdType,Integer> allocationSizes;

    private final MemberId me;
    private final Log log;

    public ReplicatedIdRangeAcquirer(
            Replicator replicator, ReplicatedIdAllocationStateMachine idAllocationStateMachine,
            Map<IdType, Integer> allocationSizes, MemberId me, LogProvider logProvider )
    {
        this.replicator = replicator;
        this.idAllocationStateMachine = idAllocationStateMachine;
        this.allocationSizes = allocationSizes;
        this.me = me;
        this.log = logProvider.getLog( getClass() );
    }

    IdAllocation acquireIds( IdType idType )
    {
        while ( true )
        {
            long firstUnallocated = idAllocationStateMachine.firstUnallocated( idType );
            ReplicatedIdAllocationRequest idAllocationRequest =
                    new ReplicatedIdAllocationRequest( me, idType, firstUnallocated, allocationSizes.get( idType ) );

            if ( replicateIdAllocationRequest( idType, idAllocationRequest ) )
            {
                IdRange idRange = new IdRange( EMPTY_LONG_ARRAY, firstUnallocated, allocationSizes.get( idType ) );
                return new IdAllocation( idRange, -1, 0 );
            }
            else
            {
                log.info( "Retrying ID generation due to conflict. Request was: " + idAllocationRequest );
            }
        }
    }

    private boolean replicateIdAllocationRequest( IdType idType, ReplicatedIdAllocationRequest idAllocationRequest )
    {
        try
        {
            return (Boolean) replicator.replicate( idAllocationRequest, true ).get();
        }
        catch ( Exception e )
        {
            log.warn( format( "Failed to acquire id range for idType %s", idType ), e );
            throw new IdGenerationException( e );
        }
    }
}
