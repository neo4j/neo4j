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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.DefaultNonUniqueIndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

class TemporalIndexPopulator extends TemporalIndexCache<TemporalIndexPopulator.PartPopulator<?>, IOException> implements IndexPopulator
{
    private final IndexSamplerWrapper sampler;

    TemporalIndexPopulator( long indexId,
                            SchemaIndexDescriptor descriptor,
                            IndexSamplingConfig samplingConfig,
                            TemporalIndexFiles temporalIndexFiles,
                            PageCache pageCache,
                            FileSystemAbstraction fs,
                            IndexProvider.Monitor monitor )
    {
        super( new PartFactory( pageCache, fs, temporalIndexFiles, indexId, descriptor, samplingConfig, monitor ) );
        this.sampler = new IndexSamplerWrapper( descriptor, samplingConfig );
    }

    @Override
    public synchronized void create() throws IOException
    {
        forAll( NativeSchemaIndexPopulator::clear, this );
    }

    @Override
    public synchronized void drop() throws IOException
    {
        forAll( NativeSchemaIndexPopulator::drop, this );
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        for ( IndexEntryUpdate<?> update : updates )
        {
            select( update.values()[0].valueGroup() ).batchUpdate( update );
        }
        for ( PartPopulator part : this )
        {
            part.applyUpdateBatch();
        }
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
        return new TemporalIndexPopulatingUpdater( this, accessor );
    }

    @Override
    public synchronized void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        for ( NativeSchemaIndexPopulator part : this )
        {
            part.close( populationCompletedSuccessfully );
        }
    }

    @Override
    public synchronized void markAsFailed( String failure )
    {
        for ( NativeSchemaIndexPopulator part : this )
        {
            part.markAsFailed( failure );
        }
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

    static class PartPopulator<KEY extends NativeSchemaKey> extends NativeSchemaIndexPopulator<KEY, NativeSchemaValue>
    {
        List<IndexEntryUpdate<?>> updates = new ArrayList<>();

        PartPopulator( PageCache pageCache, FileSystemAbstraction fs, TemporalIndexFiles.FileLayout<KEY> fileLayout,
                       IndexProvider.Monitor monitor, SchemaIndexDescriptor descriptor, long indexId, IndexSamplingConfig samplingConfig )
        {
            super( pageCache, fs, fileLayout.indexFile, fileLayout.layout, monitor, descriptor, indexId, samplingConfig );
        }

        void batchUpdate( IndexEntryUpdate<?> update )
        {
            updates.add( update );
        }

        void applyUpdateBatch() throws IOException, IndexEntryConflictException
        {
            try
            {
                add( updates );
            }
            finally
            {
                updates = new ArrayList<>();
            }
        }

        @Override
        IndexReader newReader()
        {
            return new TemporalIndexPartReader<>( tree, layout, samplingConfig, descriptor );
        }

        @Override
        public void includeSample( IndexEntryUpdate<?> update )
        {
            throw new UnsupportedOperationException( "please to not get here!" );
        }

        @Override
        public IndexSample sampleResult()
        {
            throw new UnsupportedOperationException( "this sampling code needs a rewrite." );
        }
    }

    static class PartFactory implements TemporalIndexCache.Factory<PartPopulator<?>, IOException>
    {
        private final PageCache pageCache;
        private final FileSystemAbstraction fs;
        private final TemporalIndexFiles temporalIndexFiles;
        private final long indexId;
        private final SchemaIndexDescriptor descriptor;
        private final IndexSamplingConfig samplingConfig;
        private final IndexProvider.Monitor monitor;

        PartFactory( PageCache pageCache, FileSystemAbstraction fs, TemporalIndexFiles temporalIndexFiles, long indexId,
                     SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig, IndexProvider.Monitor monitor )
        {
            this.pageCache = pageCache;
            this.fs = fs;
            this.temporalIndexFiles = temporalIndexFiles;
            this.indexId = indexId;
            this.descriptor = descriptor;
            this.samplingConfig = samplingConfig;
            this.monitor = monitor;
        }

        @Override
        public PartPopulator<?> newDate() throws IOException
        {
            return create( temporalIndexFiles.date() );
        }

        @Override
        public PartPopulator<?> newLocalDateTime() throws IOException
        {
            return create( temporalIndexFiles.localDateTime() );
        }

        @Override
        public PartPopulator<?> newZonedDateTime() throws IOException
        {
            return create( temporalIndexFiles.zonedDateTime() );
        }

        @Override
        public PartPopulator<?> newLocalTime() throws IOException
        {
            return create( temporalIndexFiles.localTime() );
        }

        @Override
        public PartPopulator<?> newZonedTime() throws IOException
        {
            return create( temporalIndexFiles.zonedTime() );
        }

        @Override
        public PartPopulator<?> newDuration() throws IOException
        {
            return create( temporalIndexFiles.duration() );
        }

        private <KEY extends NativeSchemaKey> PartPopulator<KEY> create( TemporalIndexFiles.FileLayout<KEY> fileLayout ) throws IOException
        {
            PartPopulator<KEY> populator = new PartPopulator<>( pageCache, fs, fileLayout, monitor, descriptor, indexId, samplingConfig );
            populator.create();
            return populator;
        }
    }
}
