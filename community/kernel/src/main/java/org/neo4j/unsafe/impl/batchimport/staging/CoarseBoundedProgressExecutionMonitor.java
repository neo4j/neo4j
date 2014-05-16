/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.collection.IteratorUtil.last;

/**
 * An {@link ExecutionMonitor} that prints progress in percent, knowing the max number of nodes and relationships
 * in advance.
 */
public class CoarseBoundedProgressExecutionMonitor extends PollingExecutionMonitor
{
    private long totalDoneBatches;
    private final long highNodeId;
    private final long highRelationshipId;
    private int previousPercent;

    public CoarseBoundedProgressExecutionMonitor( long highNodeId, long highRelationshipId )
    {
        super( SECONDS.toMillis( 1 ) );
        this.highNodeId = highNodeId;
        this.highRelationshipId = highRelationshipId;
    }

    @Override
    protected void poll( StageExecution execution )
    {
        updatePercent( execution );
    }

    private void updatePercent( StageExecution execution )
    {
        // This calculation below is aware of internals of the parallell importer and may
        // be wrong for other importers.
        long maxNumberOfBatches =
                (highNodeId/execution.getConfig().batchSize()) * 2 + // node records encountered twice
                (highRelationshipId/execution.getConfig().batchSize()) * 3; // rel records encountered three times;

        long doneBatches = totalDoneBatches + doneBatches( execution );
        int percentThere = (int) ((doneBatches*100D)/maxNumberOfBatches);
        percentThere = min( percentThere, 100 );

        applyPercentage( percentThere );
    }

    private void applyPercentage( int percentThere )
    {
        while ( previousPercent < percentThere )
        {
            percent( ++previousPercent );
        }
    }

    protected void percent( int percent )
    {
        System.out.print( "." );
        if ( percent % 10 == 0 )
        {
            System.out.println( "  " + percent + "%" );
        }
    }

    private long doneBatches( StageExecution execution )
    {
        return last( execution.stats() ).stat( Keys.done_batches ).asLong();
    }

    @Override
    protected void end( StageExecution execution, long totalTimeMillis )
    {
        this.totalDoneBatches += doneBatches( execution );
    }

    @Override
    public void done()
    {
        applyPercentage( 100 );
    }
}
