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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Calculates counts per label and puts data into {@link NodeLabelsCache} for use by {@link RelationshipCountsStep}.
 */
public class NodeCountsStep extends NodeStoreProcessorStep
{
    private final long[] labelCounts;
    private final NodeLabelsCache cache;
    private final CountsTracker countsTracker;
    private final int anyLabel;

    public NodeCountsStep( StageControl control, int batchSize, NodeStore nodeStore, NodeLabelsCache cache,
            int highLabelId, CountsTracker countsTracker )
    {
        super( control, "NODE COUNTS", batchSize, nodeStore );
        this.cache = cache;
        this.anyLabel = highLabelId;
        this.countsTracker = countsTracker;
        // Instantiate with high id + 1 since we need that extra slot for the ANY count
        this.labelCounts = new long[highLabelId+1];
    }

    @Override
    protected boolean process( NodeRecord node )
    {
        long[] labels = NodeLabelsField.get( node, nodeStore );
        if ( labels.length > 0 )
        {
            for ( long labelId : labels )
            {
                labelCounts[(int) labelId]++;
            }
            cache.put( node.getId(), labels );
        }
        labelCounts[anyLabel]++;

        // No need to update the store, we're just reading things here
        return false;
    }

    @Override
    protected void done()
    {
        for ( int i = 0; i < labelCounts.length; i++ )
        {
            countsTracker.updateCountsForNode( i == anyLabel ? ReadOperations.ANY_LABEL : i, labelCounts[i] );
        }
    }
}
