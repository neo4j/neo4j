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
import org.neo4j.values.storable.Value;
import static java.lang.String.format;

/**
 * This {@link SchemaIndexProvider index provider} act as one logical index but is backed by two physical
 * indexes, the native index and the lucene index. All index entries that can be handled by the native index will be directed
 * there and the rest will be directed to the lucene index.
 */
public class FusionSchemaIndexProvider extends SchemaIndexProvider
{
    public interface Selector
    {
        <T> T select( T nativeInstance, T luceneInstance, Value... values );
    }

    private final SchemaIndexProvider nativeProvider;
    private final SchemaIndexProvider luceneProvider;
    private final Selector selector;

    public FusionSchemaIndexProvider( SchemaIndexProvider nativeProvider, SchemaIndexProvider luceneProvider, Selector selector )
    {
        super( new Descriptor(
                nativeProvider.getProviderDescriptor().getKey() + "+" + luceneProvider.getProviderDescriptor().getKey(),
                nativeProvider.getProviderDescriptor().getVersion() + "+" + luceneProvider.getProviderDescriptor().getVersion() ), 0 );
        this.nativeProvider = nativeProvider;
        this.luceneProvider = luceneProvider;
        this.selector = selector;
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        return new FusionIndexPopulator(
                nativeProvider.getPopulator( indexId, descriptor, samplingConfig ),
                luceneProvider.getPopulator( indexId, descriptor, samplingConfig ), selector );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new FusionIndexAccessor(
                nativeProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ),
                luceneProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ), selector );
    }

    @Override
    public String getPopulationFailure( long indexId ) throws IllegalStateException
    {
        String nativeFailure = nativeProvider.getPopulationFailure( indexId );
        String luceneFailure = luceneProvider.getPopulationFailure( indexId );
        if ( nativeFailure != null )
        {
            return luceneFailure == null ? nativeFailure : nativeFailure + " and " + luceneFailure;
        }
        else
        {
            return luceneFailure;
        }
    }

    @Override
    public InternalIndexState getInitialState( long indexId, IndexDescriptor descriptor )
    {
        InternalIndexState nativeState = nativeProvider.getInitialState( indexId, descriptor );
        InternalIndexState luceneState = luceneProvider.getInitialState( indexId, descriptor );
        if ( nativeState != luceneState )
        {
            throw new IllegalStateException(
                    format( "Internal providers answer with different state native:%s, lucene:%s",
                            nativeState, luceneState ) );
        }
        return nativeState;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        // TODO implementation of this depends on decisions around defaults and migration. Coming soon.
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    static IndexSample combineSamples( IndexSample first, IndexSample other )
    {
        return new IndexSample(
                first.indexSize() + other.indexSize(),
                first.uniqueValues() + other.uniqueValues(),
                first.sampleSize() + other.sampleSize() );
    }
}
