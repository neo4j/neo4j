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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexSampler.combineSamples;

class TemporalIndexPopulator extends TemporalIndexCache<WorkSyncedNativeIndexPopulator<?,?>> implements IndexPopulator
{
    TemporalIndexPopulator( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig, TemporalIndexFiles temporalIndexFiles, PageCache pageCache,
                            FileSystemAbstraction fs, IndexProvider.Monitor monitor )
    {
        super( new PartFactory( pageCache, fs, temporalIndexFiles, descriptor, samplingConfig, monitor ) );
    }

    @Override
    public synchronized void create()
    {
        forAll( p -> p.getActual().clear(), this );

        // We must make sure to have at least one subindex:
        // to be able to persist failure and to have the right state in the beginning
        if ( !this.iterator().hasNext() )
        {
            select( ValueGroup.DATE );
        }
    }

    @Override
    public synchronized void drop()
    {
        forAll( IndexPopulator::drop, this );
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException
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
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor )
    {
        // No-op, uniqueness is checked for each update in add(IndexEntryUpdate)
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        return new TemporalIndexPopulatingUpdater( this, accessor );
    }

    @Override
    public synchronized void close( boolean populationCompletedSuccessfully )
    {
        closeInstantiateCloseLock();
        for ( IndexPopulator part : this )
        {
            part.close( populationCompletedSuccessfully );
        }
    }

    @Override
    public synchronized void markAsFailed( String failure )
    {
        for ( IndexPopulator part : this )
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
        List<IndexSample> samples = new ArrayList<>();
        for ( IndexPopulator partPopulator : this )
        {
            samples.add( partPopulator.sampleResult() );
        }
        return combineSamples( samples );
    }

    static class PartPopulator<KEY extends NativeIndexSingleValueKey<KEY>> extends NativeIndexPopulator<KEY,NativeIndexValue>
    {
        PartPopulator( PageCache pageCache, FileSystemAbstraction fs, TemporalIndexFiles.FileLayout<KEY> fileLayout, IndexProvider.Monitor monitor,
                StoreIndexDescriptor descriptor )
        {
            super( pageCache, fs, fileLayout.indexFile, fileLayout.layout, monitor, descriptor, NO_HEADER_WRITER );
        }

        @Override
        NativeIndexReader<KEY, NativeIndexValue> newReader()
        {
            return new TemporalIndexPartReader<>( tree, layout, descriptor );
        }
    }

    static class PartFactory implements TemporalIndexCache.Factory<WorkSyncedNativeIndexPopulator<?,?>>
    {
        private final PageCache pageCache;
        private final FileSystemAbstraction fs;
        private final TemporalIndexFiles temporalIndexFiles;
        private final StoreIndexDescriptor descriptor;
        private final IndexSamplingConfig samplingConfig;
        private final IndexProvider.Monitor monitor;

        PartFactory( PageCache pageCache, FileSystemAbstraction fs, TemporalIndexFiles temporalIndexFiles, StoreIndexDescriptor descriptor,
                IndexSamplingConfig samplingConfig, IndexProvider.Monitor monitor )
        {
            this.pageCache = pageCache;
            this.fs = fs;
            this.temporalIndexFiles = temporalIndexFiles;
            this.descriptor = descriptor;
            this.samplingConfig = samplingConfig;
            this.monitor = monitor;
        }

        @Override
        public WorkSyncedNativeIndexPopulator<?,?> newDate()
        {
            return create( temporalIndexFiles.date() );
        }

        @Override
        public WorkSyncedNativeIndexPopulator<?,?> newLocalDateTime()
        {
            return create( temporalIndexFiles.localDateTime() );
        }

        @Override
        public WorkSyncedNativeIndexPopulator<?,?> newZonedDateTime()
        {
            return create( temporalIndexFiles.zonedDateTime() );
        }

        @Override
        public WorkSyncedNativeIndexPopulator<?,?> newLocalTime()
        {
            return create( temporalIndexFiles.localTime() );
        }

        @Override
        public WorkSyncedNativeIndexPopulator<?,?> newZonedTime()
        {
            return create( temporalIndexFiles.zonedTime() );
        }

        @Override
        public WorkSyncedNativeIndexPopulator<?,?> newDuration()
        {
            return create( temporalIndexFiles.duration() );
        }

        private <KEY extends NativeIndexSingleValueKey<KEY>> WorkSyncedNativeIndexPopulator<KEY,?> create( TemporalIndexFiles.FileLayout<KEY> fileLayout )
        {
            PartPopulator<KEY> populator = new PartPopulator<>( pageCache, fs, fileLayout, monitor, descriptor );
            populator.create();
            return new WorkSyncedNativeIndexPopulator<>( populator );
        }
    }
}
