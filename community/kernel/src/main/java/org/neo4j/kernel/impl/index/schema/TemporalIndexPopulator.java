/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexSampler.combineSamples;

class TemporalIndexPopulator extends TemporalIndexCache<TemporalIndexPopulator.PartPopulator<?>> implements IndexPopulator
{
    TemporalIndexPopulator( long indexId,
                            SchemaIndexDescriptor descriptor,
                            IndexSamplingConfig samplingConfig,
                            TemporalIndexFiles temporalIndexFiles,
                            PageCache pageCache,
                            FileSystemAbstraction fs,
                            IndexProvider.Monitor monitor )
    {
        super( new PartFactory( pageCache, fs, temporalIndexFiles, indexId, descriptor, samplingConfig, monitor ) );
    }

    @Override
    public synchronized void create() throws IOException
    {
        forAll( NativeSchemaIndexPopulator::clear, this );

        // We must make sure to have at least one subindex:
        // to be able to persist failure and to have the right state in the beginning
        if ( !this.iterator().hasNext() )
        {
            select( ValueGroup.DATE );
        }
    }

    @Override
    public synchronized void drop() throws IOException
    {
        forAll( NativeSchemaIndexPopulator::drop, this );
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        Map<ValueGroup,List<IndexEntryUpdate<?>>> batchMap = new HashMap<>();
        for ( IndexEntryUpdate<?> update : updates )
        {
            ValueGroup valueGroup = update.values()[0].valueGroup();
            List<IndexEntryUpdate<?>> batch = batchMap.computeIfAbsent( valueGroup, k -> new ArrayList<>() );
            batch.add( update );
        }
        for ( Map.Entry<ValueGroup,List<IndexEntryUpdate<?>>> entry : batchMap.entrySet() )
        {
            select( entry.getKey() ).add( entry.getValue() );
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
        closeInstantiateCloseLock();
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
        Value[] values = update.values();
        assert values.length == 1;
        uncheckedSelect( values[0].valueGroup() ).includeSample( update );
    }

    @Override
    public IndexSample sampleResult()
    {
        IndexSample[] indexSamples = StreamSupport.stream( this.spliterator(), false )
                .map( PartPopulator::sampleResult )
                .toArray( IndexSample[]::new );
        return combineSamples( indexSamples );
    }

    static class PartPopulator<KEY extends NativeSchemaKey<KEY>> extends NativeSchemaIndexPopulator<KEY, NativeSchemaValue>
    {
        PartPopulator( PageCache pageCache, FileSystemAbstraction fs, TemporalIndexFiles.FileLayout<KEY> fileLayout,
                       IndexProvider.Monitor monitor, SchemaIndexDescriptor descriptor, long indexId, IndexSamplingConfig samplingConfig )
        {
            super( pageCache, fs, fileLayout.indexFile, fileLayout.layout, monitor, descriptor, indexId, samplingConfig );
        }

        @Override
        IndexReader newReader()
        {
            return new TemporalIndexPartReader<>( tree, layout, samplingConfig, descriptor );
        }
    }

    static class PartFactory implements TemporalIndexCache.Factory<PartPopulator<?>>
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

        private <KEY extends NativeSchemaKey<KEY>> PartPopulator<KEY> create( TemporalIndexFiles.FileLayout<KEY> fileLayout ) throws IOException
        {
            PartPopulator<KEY> populator = new PartPopulator<>( pageCache, fs, fileLayout, monitor, descriptor, indexId, samplingConfig );
            populator.create();
            return populator;
        }
    }
}
