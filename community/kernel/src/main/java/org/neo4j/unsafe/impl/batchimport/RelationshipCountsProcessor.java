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
    private final NodeLabelsCache nodeLabelCache;
    private final LongArray labelsCounts;
    private final LongArray wildcardCounts;

    private int[] startScratch = new int[20], endScratch = new int[20]; // and grows on demand
    private final CountsAccessor.Updater countsUpdater;
    private final long anyLabel;
    private final long anyRelationshipType;
    private final NodeLabelsCache.Client client;
    private final long itemsPerLabel;
    private final long itemsPerType;

    private final int START = 0;
    private final int END = 1;
    private final int SIDES = 2;

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
        this.itemsPerType = anyLabel + 1;
        this.itemsPerLabel = anyRelationshipType + 1;
        this.labelsCounts = cacheFactory.newLongArray( sideSize() * SIDES, 0 );
        this.wildcardCounts = cacheFactory.newLongArray( anyRelationshipType + 1, 0 );
    }

    public void process( long startNode, int type, long endNode )
    {
        // Below is logic duplication of CountsState#addRelationship
        increment( wildcardCounts, anyRelationshipType );
        increment( wildcardCounts, type );
        startScratch = nodeLabelCache.get( client, startNode, startScratch );
        for ( int startNodeLabelId : startScratch )
        {
            if ( startNodeLabelId == -1 )
            {   // We reached the end of it
                break;
            }

            increment( labelsCounts, startNodeLabelId, anyRelationshipType, START );
            increment( labelsCounts, startNodeLabelId, type, START );
        }
        endScratch = nodeLabelCache.get( client, endNode, endScratch );
        for ( int endNodeLabelId : endScratch )
        {
            if ( endNodeLabelId == -1 )
            {   // We reached the end of it
                break;
            }

            increment( labelsCounts, endNodeLabelId, anyRelationshipType, END );
            increment( labelsCounts, endNodeLabelId, type, END );
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
        for ( int wildcardType = 0; wildcardType <= anyRelationshipType; wildcardType++ )
        {
            int type = wildcardType == anyRelationshipType ? ReadOperations.ANY_RELATIONSHIP_TYPE : wildcardType;
            long count = wildcardCounts.get( wildcardType );
            countsUpdater.incrementRelationshipCount( ReadOperations.ANY_LABEL, type, ReadOperations
                    .ANY_LABEL, count );
        }


        for ( int labelId = 0; labelId < anyLabel; labelId++ )
        {
            for ( int typeId = 0; typeId <= anyRelationshipType; typeId++ )
            {

                long startCount = labelsCounts.get( arrayIndex( labelId, typeId, START ) );
                long endCount = labelsCounts.get( arrayIndex( labelId, typeId, END ) );
                int type = typeId == anyRelationshipType ? ReadOperations.ANY_RELATIONSHIP_TYPE : typeId;

                countsUpdater.incrementRelationshipCount( labelId, type, ReadOperations.ANY_LABEL, startCount );
                countsUpdater.incrementRelationshipCount( ReadOperations.ANY_LABEL, type, labelId, endCount );
            }
        }
    }

    public void addCountsFrom( RelationshipCountsProcessor from )
    {
        mergeCounts( labelsCounts, from.labelsCounts );
        mergeCounts( wildcardCounts, from.wildcardCounts );
    }

    private void mergeCounts( LongArray destination, LongArray part )
    {
        long length = destination.length();
        for ( long i = 0; i < length; i++ )
        {
            destination.set( i, destination.get( i ) + part.get( i ) );
        }
    }

    private long arrayIndex( long labelId, long relationshipTypeId, long side )
    {
        return (side * sideSize()) + (labelId * itemsPerLabel + relationshipTypeId);
    }

    private long sideSize()
    {
        return itemsPerType * itemsPerLabel;
    }

    private void increment( LongArray counts, long labelId, long relationshipTypeId, long side )
    {
        long index = arrayIndex( labelId, relationshipTypeId, side );
        increment( counts, index );
    }

    private void increment( LongArray counts, long index )
    {
        counts.set( index, counts.get( index ) + 1 );
    }
}
