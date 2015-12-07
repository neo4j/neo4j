/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.neo4j.unsafe.impl.batchimport.stats.Keys;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.collection.IteratorUtil.last;

/**
 * An {@link ExecutionMonitor} that prints progress in percent, knowing the max number of nodes and relationships
 * in advance.
 */
public abstract class CoarseBoundedProgressExecutionMonitor extends ExecutionMonitor.Adapter
{
    private final long totalNumberOfBatches;
    private long[] prevDoneBatches;

    public CoarseBoundedProgressExecutionMonitor( long highNodeId, long highRelationshipId,
            Configuration configuration )
    {
        super( 1, SECONDS );
        // This calculation below is aware of internals of the parallel importer and may
        // be wrong for other importers.
        this.totalNumberOfBatches =
                (highNodeId/configuration.batchSize()) * 2 + // node records encountered twice
                (highRelationshipId/configuration.batchSize()) * 3; // rel records encountered three times;
    }

    protected long total()
    {
        return totalNumberOfBatches;
    }

    @Override
    public void check( StageExecution[] executions )
    {
        update( executions );
    }

    @Override
    public void start( StageExecution[] executions )
    {
        prevDoneBatches = new long[executions.length];
    }

    private void update( StageExecution[] executions )
    {
        long diff = 0;
        for ( int i = 0; i < executions.length; i++ )
        {
            long doneBatches = doneBatches( executions[i] );
            diff += doneBatches - prevDoneBatches[i];
            prevDoneBatches[i] = doneBatches;
        }
        if ( diff > 0 )
        {
            progress( diff );
        }
    }

    /**
     * @param progress Relative progress.
     */
    protected abstract void progress( long progress );

    private long doneBatches( StageExecution execution )
    {
        return last( execution.steps() ).stats().stat( Keys.done_batches ).asLong();
    }

    @Override
    public void done( long totalTimeMillis, String additionalInformation )
    {
        long prev = 0;
        for ( long done : prevDoneBatches )
        {
            prev += done;
        }

        // Just report the last progress
        progress( totalNumberOfBatches-prev );
    }
}
