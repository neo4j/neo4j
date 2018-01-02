/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.graphdb.Resource;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.neo4j.unsafe.impl.batchimport.CalculateDenseNodePrepareStep.RADIXES;
import static org.neo4j.unsafe.impl.batchimport.CalculateDenseNodePrepareStep.radixOf;

/**
 * Runs through relationship input and counts relationships per node so that dense nodes can be designated.
 */
public class CalculateDenseNodesStep extends ProcessorStep<long[]>
{
    private final NodeRelationshipCache cache;
    private final StripedLock lock = new StripedLock( RADIXES );

    public CalculateDenseNodesStep( StageControl control, Configuration config, NodeRelationshipCache cache )
    {
        // Max 10 processors since we receive batches split by radix %10 so it doesn't make sense to have more
        super( control, "CALCULATOR", config, RADIXES );
        this.cache = cache;
    }

    @Override
    protected void process( long[] ids, BatchSender sender )
    {
        // We lock because we only want at most one processor processing ids of a certain radix.
        try ( Resource automaticallyUnlocked = lock.lock( radixOf( ids[0] ) ) )
        {
            for ( long id : ids )
            {
                if ( id != -1 )
                {
                    cache.incrementCount( id );
                }
            }
        }
    }
}
