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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.SpatialKnownIndex;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.combineSamples;

class SpatialFusionIndexPopulator implements IndexPopulator
{
    private final long indexId;
    private final IndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private final SpatialKnownIndex.Factory indexFactory;
    private final Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap;

    SpatialFusionIndexPopulator( Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap, long indexId, IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig, SpatialKnownIndex.Factory indexFactory )
    {
        this.indexMap = indexMap;
        this.indexId = indexId;
        this.descriptor = descriptor;
        this.samplingConfig = samplingConfig;
        this.indexFactory = indexFactory;
    }

    @Override
    public void create() throws IOException
    {
        // When an index is first created using `CREATE INDEX` or `graph.schema.for*` then a call will be made here,
        // however, the KnownSpatialIndex will not yet exist, since that can only be created on demand
        if ( !indexMap.isEmpty() )
        {
            throw new IOException( "Trying to create a new spatial populator when the index had already been created." );
        }
        // TODO should we clear out all the SpatialKnownIndex on drop()?
        // Currently create -> drop -> create works due to the recreated index getting a new indexId
    }

    @Override
    public void drop() throws IOException
    {
        forAll( entry -> ((SpatialKnownIndex) entry).drop(), indexMap.values().toArray() );
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        Map<CoordinateReferenceSystem,Collection<IndexEntryUpdate<?>>> batchMap = new HashMap<>();
        for ( IndexEntryUpdate<?> update : updates )
        {
            selectUpdates( batchMap, update.values() ).add( update );
        }
        for ( CoordinateReferenceSystem crs : batchMap.keySet() )
        {
            SpatialKnownIndex index = getOrCreateInitializedIndex( crs );
            if ( index.getState() == SpatialKnownIndex.State.INIT )
            {
                // First add to sub-index, make sure to create
                index.create();
            }
            index.add( batchMap.get( crs ) );
        }
    }

    private Collection<IndexEntryUpdate<?>> selectUpdates( Map<CoordinateReferenceSystem,Collection<IndexEntryUpdate<?>>> instances, Value... values )
    {
        assert values.length == 1;
        PointValue pointValue = (PointValue) values[0];
        return instances.computeIfAbsent( pointValue.getCoordinateReferenceSystem(), k -> new ArrayList<>() );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        // No-op, uniqueness is checked for each update in add(IndexEntryUpdate)
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor ) throws IOException
    {
        return SpatialFusionIndexUpdater.updaterForPopulator( indexMap, indexId, indexFactory, descriptor, samplingConfig );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        forAll( entry -> ((SpatialKnownIndex) entry).close( populationCompletedSuccessfully ), indexMap.values().toArray() );
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        forAll( entry -> ((SpatialKnownIndex) entry).markAsFailed( failure ), indexMap.values().toArray() );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        Value[] values = update.values();
        assert values.length == 1;
        CoordinateReferenceSystem crs = ((PointValue) values[0]).getCoordinateReferenceSystem();
        SpatialKnownIndex index = getOrCreateInitializedIndex( crs );
        index.includeSample( update );
    }

    private SpatialKnownIndex getOrCreateInitializedIndex( CoordinateReferenceSystem crs )
    {
        SpatialKnownIndex index = indexFactory.selectAndCreate( indexMap, indexId, crs );
        if ( index.getState() == SpatialKnownIndex.State.NONE )
        {
            index.initialize( descriptor, samplingConfig );
        }
        return index;
    }

    @Override
    public IndexSample sampleResult()
    {
        return combineSamples( indexMap.values().stream().map( SpatialKnownIndex::sampleResult ).toArray( IndexSample[]::new ) );
    }
}
