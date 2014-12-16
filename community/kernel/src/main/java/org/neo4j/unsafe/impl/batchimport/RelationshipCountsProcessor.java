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

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;

/**
 * Calculates counts as labelId --[type]--> labelId for relationships with the labels coming from its start/end nodes.
 */
public class RelationshipCountsProcessor implements StoreProcessor<RelationshipRecord>
{
    /** Don't support these counts at the moment so don't compute them */
    private static final boolean COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS = false;
    private final NodeLabelsCache nodeLabelCache;
    // start node label id | relationship type | end node label id. Roughly 8Mb for 100,100,100
    private final long[][][] counts;
    private int[] startScratch = new int[20], endScratch = new int[20]; // and grows on demand
    private final CountsTracker countsTracker;
    private final int anyLabel;
    private final int anyRelationshipType;

    protected RelationshipCountsProcessor( NodeLabelsCache nodeLabelCache,
            int highLabelId, int highRelationshipTypeId, CountsTracker countsTracker )
    {
        this.nodeLabelCache = nodeLabelCache;
        this.countsTracker = countsTracker;

        // Instantiate with high id + 1 since we need that extra slot for the ANY counts
        this.counts = new long[highLabelId+1][highRelationshipTypeId+1][highLabelId+1];
        this.anyLabel = highLabelId;
        this.anyRelationshipType = highRelationshipTypeId;
    }

    @Override
    public boolean process( RelationshipRecord record )
    {
        long startNode = record.getFirstNode();
        long endNode = record.getSecondNode();
        int type = record.getType();

        // Below is logic duplication of CountsState#addRelationship

        counts[anyLabel][anyRelationshipType][anyLabel]++;
        counts[anyLabel][type][anyLabel]++;
        startScratch = nodeLabelCache.get( startNode, startScratch );
        for ( int startNodeLabelId : startScratch )
        {
            if ( startNodeLabelId == -1 )
            {   // We reached the end of it
                break;
            }

            counts[startNodeLabelId][anyRelationshipType][anyLabel]++;
            counts[startNodeLabelId][type][anyLabel]++;
            endScratch = nodeLabelCache.get( endNode, endScratch );
            for ( int endNodeLabelId : endScratch )
            {
                if ( endNodeLabelId == -1 )
                {   // We reached the end of it
                    break;
                }

                counts[startNodeLabelId][anyRelationshipType][endNodeLabelId]++;
                counts[startNodeLabelId][type][endNodeLabelId]++;
            }
        }
        endScratch = nodeLabelCache.get( endNode, endScratch );
        for ( int endNodeLabelId : endScratch )
        {
            if ( endNodeLabelId == -1 )
            {   // We reached the end of it
                break;
            }

            counts[anyLabel][anyRelationshipType][endNodeLabelId]++;
            counts[anyLabel][type][endNodeLabelId]++;
        }

        // No need to update the store, we're just reading things here
        return false;
    }

    @Override
    public void done()
    {
        for ( int startNodeLabelId = 0; startNodeLabelId < counts.length; startNodeLabelId++ )
        {
            long[][] types = counts[startNodeLabelId];
            for ( int typeId = 0; typeId < types.length; typeId++ )
            {
                long[] endNodeLabelIds = types[typeId];
                for ( int endNodeLabelId = 0; endNodeLabelId < endNodeLabelIds.length; endNodeLabelId++ )
                {
                    if ( !COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS )
                    {
                        if ( startNodeLabelId != anyLabel && endNodeLabelId != anyLabel )
                        {
                            continue;
                        }
                    }
                    int startLabel = startNodeLabelId == anyLabel ? ReadOperations.ANY_LABEL : startNodeLabelId;
                    int type = typeId == anyRelationshipType ? ReadOperations.ANY_RELATIONSHIP_TYPE : typeId;
                    int endLabel = endNodeLabelId == anyLabel ? ReadOperations.ANY_LABEL : endNodeLabelId;
                    long count = endNodeLabelIds[endNodeLabelId];
                    countsTracker.incrementRelationshipCount( startLabel, type, endLabel, count );
                }
            }
        }
    }
}
