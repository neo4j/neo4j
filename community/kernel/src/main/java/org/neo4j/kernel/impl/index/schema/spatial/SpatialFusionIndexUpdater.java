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
package org.neo4j.kernel.impl.index.schema.spatial;

import java.io.IOException;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.spatial.SpatialSchemaIndexProvider.KnownSpatialIndex;
import org.neo4j.kernel.impl.index.schema.spatial.SpatialSchemaIndexProvider.KnownSpatialIndexFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;

class SpatialFusionIndexUpdater implements IndexUpdater
{
    private final Map<CoordinateReferenceSystem,KnownSpatialIndex> indexMap;
    private final long indexId;
    private final KnownSpatialIndexFactory indexFactory;
    private final IndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private final IndexUpdateMode mode;
    private final PropertyAccessor accessor;

    SpatialFusionIndexUpdater( Map<CoordinateReferenceSystem,KnownSpatialIndex> indexMap, long indexId, KnownSpatialIndexFactory indexFactory,
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

    SpatialFusionIndexUpdater( Map<CoordinateReferenceSystem,KnownSpatialIndex> indexMap, long indexId, KnownSpatialIndexFactory indexFactory,
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
            // Hmm, here's a little conundrum. What if we change from a value that goes into native
            // to a value that goes into fallback, or vice versa? We also don't want to blindly pass
            // all CHANGED updates to both updaters since not all values will work in them.
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
                from.process( IndexEntryUpdate.remove(
                        update.getEntityId(), update.indexKey(), update.beforeValues() ) );
                to.process( IndexEntryUpdate.add(
                        update.getEntityId(), update.indexKey(), update.values() ) );
            }
            break;
        case REMOVED:
            selectUpdater( update.values() ).process( update );
            break;
        default:
            throw new IllegalArgumentException( "Unknown update mode" );
        }
    }

    private IndexUpdater selectUpdater(Value... values) throws IOException
    {
        if (mode != null)
        {
            return indexFactory.select( indexMap, indexId, values ).getOnlineAccessor( descriptor, samplingConfig ).newUpdater( mode );
        }
        else
        {
            return indexFactory.select( indexMap, indexId, values ).getPopulator( descriptor, samplingConfig ).newPopulatingUpdater( accessor );
        }
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        // TODO this should close the updaters we have used
        forAll( ( updater ) -> ((IndexUpdater) updater).close(), indexMap.values().toArray() );
    }
}
