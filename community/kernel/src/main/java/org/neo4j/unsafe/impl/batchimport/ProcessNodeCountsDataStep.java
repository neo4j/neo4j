/**
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Processes node count data received from {@link ReadNodeCountsDataStep} and stores the accumulated counts
 * into {@link CountsTracker}.
 */
public class ProcessNodeCountsDataStep extends ExecutorServiceStep<NodeRecord[]>
{
    private final NodeCountsProcessor processor;

    protected ProcessNodeCountsDataStep( StageControl control, NodeLabelsCache cache,
            int workAheadSize, int movingAverageSize, NodeStore nodeStore,
            int highLabelId, CountsAccessor.Updater countsUpdater )
    {
        super( control, "COUNT", workAheadSize, movingAverageSize, 1 );
        this.processor = new NodeCountsProcessor( nodeStore, cache, highLabelId, countsUpdater );
    }

    @Override
    protected Object process( long ticket, NodeRecord[] batch )
    {
        for ( NodeRecord node : batch )
        {
            if ( node != null )
            {
                processor.process( node );
            }
        }
        return null; // end of line
    }

    @Override
    protected void done()
    {
        processor.done();
    }
}
