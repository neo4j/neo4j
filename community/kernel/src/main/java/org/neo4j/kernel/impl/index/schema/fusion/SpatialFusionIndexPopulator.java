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
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.DefaultNonUniqueIndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.kernel.impl.index.schema.SamplingUtil;
import org.neo4j.kernel.impl.index.schema.SpatialCRSSchemaIndex;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;

class SpatialFusionIndexPopulator implements IndexPopulator
{
    private final long indexId;
    private final SchemaIndexDescriptor descriptor;
    private final SpatialCRSSchemaIndex.Supplier indexSupplier;
    private final Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex> indexMap;
    private final IndexSamplerWrapper sampler;

    SpatialFusionIndexPopulator( Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex> indexMap, long indexId, SchemaIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig, SpatialCRSSchemaIndex.Supplier indexSupplier )
    {
        this.indexMap = indexMap;
        this.indexId = indexId;
        this.descriptor = descriptor;
        this.indexSupplier = indexSupplier;
        this.sampler = new IndexSamplerWrapper( descriptor, samplingConfig );
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
    }

    @Override
    public void drop() throws IOException
    {
        forAll( SpatialCRSSchemaIndex::drop, indexMap.values() );
        indexMap.clear();
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
            SpatialCRSSchemaIndex index = indexSupplier.get( descriptor, indexMap, indexId, crs );
            index.startPopulation();
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
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor )
    {
        return SpatialFusionIndexUpdater.updaterForPopulator( indexMap, indexId, indexSupplier, descriptor );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        forAll( spatialKnownIndex -> spatialKnownIndex.finishPopulation( populationCompletedSuccessfully ), indexMap.values() );
    }

    @Override
    public void markAsFailed( String failure )
    {
        forAll( spatialKnownIndex -> spatialKnownIndex.markAsFailed( failure ), indexMap.values() );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        sampler.includeSample( update.values() );
    }

    @Override
    public IndexSample sampleResult()
    {
        return sampler.sampleResult();
    }

    private static class IndexSamplerWrapper
    {
        private final DefaultNonUniqueIndexSampler generalSampler;
        private final UniqueIndexSampler uniqueSampler;

        IndexSamplerWrapper( SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
        {
            switch ( descriptor.type() )
            {
            case GENERAL:
                generalSampler = new DefaultNonUniqueIndexSampler( samplingConfig.sampleSizeLimit() );
                uniqueSampler = null;
                break;
            case UNIQUE:
                generalSampler = null;
                uniqueSampler = new UniqueIndexSampler();
                break;
            default:
                throw new UnsupportedOperationException( "Unexpected index type " + descriptor.type() );
            }
        }

        void includeSample( Value[] values )
        {
            if ( uniqueSampler != null )
            {
                uniqueSampler.increment( 1 );
            }
            else
            {
                generalSampler.include( SamplingUtil.encodedStringValuesForSampling( (Object[]) values ) );
            }
        }

        IndexSample sampleResult()
        {
            if ( uniqueSampler != null )
            {
                return uniqueSampler.result();
            }
            else
            {
                return generalSampler.result();
            }
        }
    }
}
