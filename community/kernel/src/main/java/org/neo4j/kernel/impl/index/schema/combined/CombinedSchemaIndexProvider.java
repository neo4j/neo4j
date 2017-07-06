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
package org.neo4j.kernel.impl.index.schema.combined;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.NumberType;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

/**
 * Exactly what this provider will be called and where it will live is undecided, so for the time being
 * it will be called something temporary.
 *
 * The idea is to have a boosted provider which can handle some type of data and is faster, where all other
 * data is managed by the fallback provider.
 */
public class CombinedSchemaIndexProvider extends SchemaIndexProvider
{
    private final SchemaIndexProvider boostProvider;
    private final SchemaIndexProvider fallbackProvider;

    public CombinedSchemaIndexProvider( SchemaIndexProvider boostProvider, SchemaIndexProvider fallbackProvider )
    {
        super( new Descriptor( "combined", "0.1" ), 0 );
        this.boostProvider = boostProvider;
        this.fallbackProvider = fallbackProvider;
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        return new CombinedIndexPopulator(
                boostProvider.getPopulator( indexId, descriptor, samplingConfig ),
                fallbackProvider.getPopulator( indexId, descriptor, samplingConfig ) );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new CombinedIndexAccessor(
                boostProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ),
                fallbackProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ) );
    }

    @Override
    public String getPopulationFailure( long indexId ) throws IllegalStateException
    {
        String boostFailure = boostProvider.getPopulationFailure( indexId );
        String fallbackFailure = fallbackProvider.getPopulationFailure( indexId );
        if ( boostFailure != null )
        {
            return fallbackFailure == null ? boostFailure : boostFailure + " and " + fallbackFailure;
        }
        else
        {
            return fallbackFailure;
        }
    }

    @Override
    public InternalIndexState getInitialState( long indexId, IndexDescriptor descriptor )
    {
        InternalIndexState boostState = boostProvider.getInitialState( indexId, descriptor );
        InternalIndexState fallbackState = fallbackProvider.getInitialState( indexId, descriptor );
        if ( boostState != fallbackState )
        {
            throw new IllegalStateException(
                    format( "Internal providers answer with different state boost:%s, fallback:%s",
                            boostState, fallbackState ) );
        }
        return boostState;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        return null;
    }

    static <T> T select( Value[] values, T boost, T fallback )
    {
        if ( values.length > 1 )
        {
            // Multiple values must be handled by fallback
            return fallback;
        }

        Value singleValue = values[0];
        if ( singleValue.numberType() != NumberType.NO_NUMBER && !(singleValue instanceof ArrayValue) )
        {
            // It's a number, the boost can handle this
            return boost;
        }
        return fallback;
    }

    static IndexSample combineSamples( IndexSample first, IndexSample other )
    {
        return new IndexSample(
                first.indexSize() + other.indexSize(),
                first.uniqueValues() + other.uniqueValues(),
                first.sampleSize() + other.sampleSize() );
    }
}
