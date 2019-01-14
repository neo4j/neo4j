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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.CombiningIterable;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;

import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

class TemporalIndexAccessor extends TemporalIndexCache<TemporalIndexAccessor.PartAccessor<?>> implements IndexAccessor
{
    private final SchemaIndexDescriptor descriptor;

    TemporalIndexAccessor( long indexId,
                           SchemaIndexDescriptor descriptor,
                           IndexSamplingConfig samplingConfig,
                           PageCache pageCache,
                           FileSystemAbstraction fs,
                           RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                           IndexProvider.Monitor monitor,
                           TemporalIndexFiles temporalIndexFiles ) throws IOException
    {
        super( new PartFactory( pageCache, fs, recoveryCleanupWorkCollector, monitor, descriptor, indexId, samplingConfig, temporalIndexFiles ) );
        this.descriptor = descriptor;

        temporalIndexFiles.loadExistingIndexes( this );
    }

    @Override
    public void drop() throws IOException
    {
        forAll( NativeSchemaIndexAccessor::drop, this );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return new TemporalIndexUpdater( this, mode );
    }

    @Override
    public void force( IOLimiter ioLimiter ) throws IOException
    {
        for ( NativeSchemaIndexAccessor part : this )
        {
            part.force( ioLimiter );
        }
    }

    @Override
    public void refresh()
    {
        // not required in this implementation
    }

    @Override
    public void close() throws IOException
    {
        closeInstantiateCloseLock();
        forAll( NativeSchemaIndexAccessor::close, this );
    }

    @Override
    public IndexReader newReader()
    {
        return new TemporalIndexReader( descriptor, this );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        ArrayList<BoundedIterable<Long>> allEntriesReader = new ArrayList<>();
        for ( NativeSchemaIndexAccessor<?,?> part : this )
        {
            allEntriesReader.add( part.newAllEntriesReader() );
        }

        return new BoundedIterable<Long>()
        {
            @Override
            public long maxCount()
            {
                long sum = 0L;
                for ( BoundedIterable<Long> part : allEntriesReader )
                {
                    long partMaxCount = part.maxCount();
                    if ( partMaxCount == UNKNOWN_MAX_COUNT )
                    {
                        return UNKNOWN_MAX_COUNT;
                    }
                    sum += partMaxCount;
                }
                return sum;
            }

            @Override
            public void close() throws Exception
            {
                forAll( BoundedIterable::close, allEntriesReader );
            }

            @Override
            public Iterator<Long> iterator()
            {
                return new CombiningIterable<>( allEntriesReader ).iterator();
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        List<ResourceIterator<File>> snapshotFiles = new ArrayList<>();
        for ( NativeSchemaIndexAccessor<?,?> part : this )
        {
            snapshotFiles.add( part.snapshotFiles() );
        }
        return concatResourceIterators( snapshotFiles.iterator() );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
    {
        // Not needed since uniqueness is verified automatically w/o cost for every update.
    }

    @Override
    public boolean isDirty()
    {
        return Iterators.stream( iterator() ).anyMatch( NativeSchemaIndexAccessor::isDirty );
    }

    static class PartAccessor<KEY extends NativeSchemaKey<KEY>> extends NativeSchemaIndexAccessor<KEY, NativeSchemaValue>
    {
        private final Layout<KEY,NativeSchemaValue> layout;
        private final SchemaIndexDescriptor descriptor;
        private final IndexSamplingConfig samplingConfig;

        PartAccessor( PageCache pageCache,
                      FileSystemAbstraction fs,
                      TemporalIndexFiles.FileLayout<KEY> fileLayout,
                      RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                      IndexProvider.Monitor monitor,
                      SchemaIndexDescriptor descriptor,
                      long indexId,
                      IndexSamplingConfig samplingConfig ) throws IOException
        {
            super( pageCache, fs, fileLayout.indexFile, fileLayout.layout, recoveryCleanupWorkCollector, monitor, descriptor, indexId, samplingConfig );
            this.layout = fileLayout.layout;
            this.descriptor = descriptor;
            this.samplingConfig = samplingConfig;
        }

        @Override
        public TemporalIndexPartReader<KEY> newReader()
        {
            assertOpen();
            return new TemporalIndexPartReader<>( tree, layout, samplingConfig, descriptor );
        }
    }

    static class PartFactory implements TemporalIndexCache.Factory<PartAccessor<?>>
    {
        private final PageCache pageCache;
        private final FileSystemAbstraction fs;
        private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
        private final IndexProvider.Monitor monitor;
        private final SchemaIndexDescriptor descriptor;
        private final long indexId;
        private final IndexSamplingConfig samplingConfig;
        private final TemporalIndexFiles temporalIndexFiles;

        PartFactory( PageCache pageCache,
                     FileSystemAbstraction fs,
                     RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                     IndexProvider.Monitor monitor,
                     SchemaIndexDescriptor descriptor,
                     long indexId,
                     IndexSamplingConfig samplingConfig,
                     TemporalIndexFiles temporalIndexFiles )
        {
            this.pageCache = pageCache;
            this.fs = fs;
            this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
            this.monitor = monitor;
            this.descriptor = descriptor;
            this.indexId = indexId;
            this.samplingConfig = samplingConfig;
            this.temporalIndexFiles = temporalIndexFiles;
        }

        @Override
        public PartAccessor<?> newDate() throws IOException
        {
            return createPartAccessor( temporalIndexFiles.date() );
        }

        @Override
        public PartAccessor<?> newLocalDateTime() throws IOException
        {
            return createPartAccessor( temporalIndexFiles.localDateTime() );
        }

        @Override
        public PartAccessor<?> newZonedDateTime() throws IOException
        {
            return createPartAccessor( temporalIndexFiles.zonedDateTime() );
        }

        @Override
        public PartAccessor<?> newLocalTime() throws IOException
        {
            return createPartAccessor( temporalIndexFiles.localTime() );
        }

        @Override
        public PartAccessor<?> newZonedTime() throws IOException
        {
            return createPartAccessor( temporalIndexFiles.zonedTime() );
        }

        @Override
        public PartAccessor<?> newDuration() throws IOException
        {
            return createPartAccessor( temporalIndexFiles.duration() );
        }

        private <KEY extends NativeSchemaKey<KEY>> PartAccessor<KEY> createPartAccessor( TemporalIndexFiles.FileLayout<KEY> fileLayout ) throws IOException
        {
            if ( !fs.fileExists( fileLayout.indexFile ) )
            {
                createEmptyIndex( fileLayout );
            }
            return new PartAccessor<>( pageCache, fs, fileLayout, recoveryCleanupWorkCollector, monitor, descriptor, indexId, samplingConfig );
        }

        private <KEY extends NativeSchemaKey<KEY>> void createEmptyIndex( TemporalIndexFiles.FileLayout<KEY> fileLayout ) throws IOException
        {
            IndexPopulator populator = new TemporalIndexPopulator.PartPopulator<>( pageCache, fs, fileLayout, monitor, descriptor, indexId, samplingConfig );
            populator.create();
            populator.close( true );
        }
    }
}
