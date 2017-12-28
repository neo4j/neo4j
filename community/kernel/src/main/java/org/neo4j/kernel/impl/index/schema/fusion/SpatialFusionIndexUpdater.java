/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.SpatialKnownIndex;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

class SpatialFusionIndexUpdater implements IndexUpdater
{
    private final Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap;
    private final Map<CoordinateReferenceSystem,IndexUpdater> currentUpdaters = new HashMap<>();
    private final long indexId;
    private final SpatialKnownIndex.Factory indexFactory;
    private final IndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private final IndexUpdateMode mode;
    private final PropertyAccessor accessor;

    SpatialFusionIndexUpdater( Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap, long indexId, SpatialKnownIndex.Factory indexFactory,
            IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, IndexUpdateMode mode )
    {
        this.indexMap = indexMap;
        this.indexId = indexId;
        this.indexFactory = indexFactory;
        this.descriptor = descriptor;
        this.samplingConfig = samplingConfig;
        this.mode = mode;
        this.accessor = null;
    }

    SpatialFusionIndexUpdater( Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap, long indexId, SpatialKnownIndex.Factory indexFactory,
            IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, PropertyAccessor accessor )
    {
        this.indexMap = indexMap;
        this.indexId = indexId;
        this.indexFactory = indexFactory;
        this.descriptor = descriptor;
        this.samplingConfig = samplingConfig;
        this.mode = null;
        this.accessor = accessor;
    }

    @Override
    public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
    {
        switch ( update.updateMode() )
        {
        case ADDED:
            selectUpdater( update.values() ).process( update );
            break;
        case CHANGED:
            // By this stage we should only have points, but they could be of different CRS, so we
            // have to check if they belong to different sub-indexes and remove/add accordingly
            IndexUpdater from = selectUpdater( update.beforeValues() );
            IndexUpdater to = selectUpdater( update.values() );
            // There are two cases:
            // - both before/after go into the same updater --> pass update into that updater
            if ( from == to )
            {
                from.process( update );
            }
            // - before go into one and after into the other --> REMOVED from one and ADDED into the other
            else
            {
                from.process( IndexEntryUpdate.remove( update.getEntityId(), update.indexKey(), update.beforeValues() ) );
                to.process( IndexEntryUpdate.add( update.getEntityId(), update.indexKey(), update.values() ) );
            }
            break;
        case REMOVED:
            selectUpdater( update.values() ).process( update );
            break;
        default:
            throw new IllegalArgumentException( "Unknown update mode" );
        }
    }

    private IndexUpdater selectUpdater( Value... values ) throws IOException
    {
        assert values.length == 1;
        PointValue pointValue = (PointValue) values[0];
        CoordinateReferenceSystem crs = pointValue.getCoordinateReferenceSystem();
        IndexUpdater updater = currentUpdaters.get( crs );
        if ( updater != null )
        {
            return updater;
        }
        if ( mode != null )
        {
            return remember( crs, indexFactory.selectAndCreate( indexMap, indexId, crs ).getOnlineAccessor( descriptor, samplingConfig ).newUpdater( mode ) );
        }
        else
        {
            return remember( crs,
                    indexFactory.selectAndCreate( indexMap, indexId, crs ).getPopulator( descriptor, samplingConfig ).newPopulatingUpdater( accessor ) );
        }
    }

    private IndexUpdater remember( CoordinateReferenceSystem crs, IndexUpdater indexUpdader )
    {
        currentUpdaters.put( crs, indexUpdader );
        return indexUpdader;
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        while ( !currentUpdaters.isEmpty() )
        {
            for ( IndexUpdater updater : currentUpdaters.values() )
            {
                updater.close();
            }
            currentUpdaters.clear();
        }
    }
}
