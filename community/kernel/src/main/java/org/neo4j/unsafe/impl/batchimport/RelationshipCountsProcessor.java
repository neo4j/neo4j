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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

/**
 * Calculates counts as labelId --[type]--> labelId for relationships with the labels coming from its start/end nodes.
 */
public class RelationshipCountsProcessor implements RecordProcessor<RelationshipRecord>
{
    /** Don't support these counts at the moment so don't compute them */
    private static final boolean COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS = false;
    private final NodeLabelsCache nodeLabelCache;
    // start node label id | relationship type | end node label id. Roughly 8Mb for 100,100,100
    private final LongArray counts;
    private int[] startScratch = new int[20], endScratch = new int[20]; // and grows on demand
    private final CountsAccessor.Updater countsUpdater;
    private final int anyLabel;
    private final int anyRelationshipType;
    private final NodeLabelsCache.Client client;
    private final int itemsPerType;
    private final int itemsPerStartLabel;

    public RelationshipCountsProcessor( NodeLabelsCache nodeLabelCache,
            int highLabelId, int highRelationshipTypeId, CountsAccessor.Updater countsUpdater,
            NumberArrayFactory cacheFactory )
    {
        this.nodeLabelCache = nodeLabelCache;
        this.client = nodeLabelCache.newClient();
        this.countsUpdater = countsUpdater;

        // Make room for high id + 1 since we need that extra slot for the ANY counts
        this.anyLabel = highLabelId;
        this.anyRelationshipType = highRelationshipTypeId;
        this.itemsPerType = anyLabel+1;
        this.itemsPerStartLabel = (anyRelationshipType+1)*itemsPerType;
        this.counts = cacheFactory.newLongArray( arrayIndex( highLabelId, highRelationshipTypeId, highLabelId )+1, 0 );
    }

    private int arrayIndex( int startLabel, int relationshipType, int endLabel )
    {
        return startLabel*itemsPerStartLabel + relationshipType*itemsPerType + endLabel;
    }

    private void increment( int startLabel, int relationshipType, int endLabel )
    {
        int index = arrayIndex( startLabel, relationshipType, endLabel );
        counts.set( index, counts.get( index ) + 1 );
    }

    public void process( long startNode, int type, long endNode )
    {
        // Below is logic duplication of CountsState#addRelationship

        increment( anyLabel, anyRelationshipType, anyLabel );
        increment( anyLabel, type, anyLabel );
        startScratch = nodeLabelCache.get( client, startNode, startScratch );
        for ( int startNodeLabelId : startScratch )
        {
            if ( startNodeLabelId == -1 )
            {   // We reached the end of it
                break;
            }

            increment( startNodeLabelId, anyRelationshipType, anyLabel );
            increment( startNodeLabelId, type, anyLabel );
            endScratch = nodeLabelCache.get( client, endNode, endScratch );
            for ( int endNodeLabelId : endScratch )
            {
                if ( endNodeLabelId == -1 )
                {   // We reached the end of it
                    break;
                }

                increment( startNodeLabelId, anyRelationshipType, endNodeLabelId );
                increment( startNodeLabelId, type, endNodeLabelId );
            }
        }
        endScratch = nodeLabelCache.get( client, endNode, endScratch );
        for ( int endNodeLabelId : endScratch )
        {
            if ( endNodeLabelId == -1 )
            {   // We reached the end of it
                break;
            }

            increment( anyLabel, anyRelationshipType, endNodeLabelId );
            increment( anyLabel, type, endNodeLabelId );
        }
    }

    @Override
    public boolean process( RelationshipRecord record )
    {
        process( record.getFirstNode(), record.getType(), record.getSecondNode() );
        // No need to update the store, we're just reading things here
        return false;
    }

    @Override
    public void done()
    {
        long index = 0;
        for ( int startNodeLabelId = 0; startNodeLabelId <= anyLabel; startNodeLabelId++ )
        {
            for ( int typeId = 0; typeId <= anyRelationshipType; typeId++ )
            {
                for ( int endNodeLabelId = 0; endNodeLabelId <= anyLabel; endNodeLabelId++, index++ )
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
                    long count = counts.get( index );
                    countsUpdater.incrementRelationshipCount( startLabel, type, endLabel, count );
                }
            }
        }
    }

    public void addCountsFrom( RelationshipCountsProcessor from )
    {
        long length = counts.length();
        for ( long i = 0; i < length; i++ )
        {
            counts.set( i, counts.get( i ) + from.counts.get( i ) );
        }
    }
}
