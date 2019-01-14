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

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.CombiningIterable;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

class SpatialIndexAccessor extends SpatialIndexCache<SpatialIndexAccessor.PartAccessor> implements IndexAccessor
{
    private final SchemaIndexDescriptor descriptor;

    SpatialIndexAccessor( long indexId,
                           SchemaIndexDescriptor descriptor,
                           IndexSamplingConfig samplingConfig,
                           PageCache pageCache,
                           FileSystemAbstraction fs,
                           RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                           IndexProvider.Monitor monitor,
                           SpatialIndexFiles spatialIndexFiles,
                           SpaceFillingCurveConfiguration searchConfiguration ) throws IOException
    {
        super( new PartFactory( pageCache,
                                fs,
                                recoveryCleanupWorkCollector,
                                monitor,
                                descriptor,
                                indexId,
                                samplingConfig,
                                spatialIndexFiles,
                                searchConfiguration ) );
        this.descriptor = descriptor;
        spatialIndexFiles.loadExistingIndexes( this );
    }

    @Override
    public void drop() throws IOException
    {
        forAll( NativeSchemaIndexAccessor::drop, this );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return new SpatialIndexUpdater( this, mode );
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
        return new SpatialIndexReader( descriptor, this );
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
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException
    {
        for ( NativeSchemaIndexAccessor<?,?> part : this )
        {
            part.verifyDeferredConstraints( propertyAccessor );
        }
    }

    @Override
    public boolean isDirty()
    {
        return Iterators.stream( iterator() ).anyMatch( NativeSchemaIndexAccessor::isDirty );
    }

    static class PartAccessor extends NativeSchemaIndexAccessor<SpatialSchemaKey, NativeSchemaValue>
    {
        private final Layout<SpatialSchemaKey,NativeSchemaValue> layout;
        private final SchemaIndexDescriptor descriptor;
        private final IndexSamplingConfig samplingConfig;
        private final SpaceFillingCurveConfiguration searchConfiguration;

        PartAccessor( PageCache pageCache,
                      FileSystemAbstraction fs,
                      SpatialIndexFiles.SpatialFileLayout fileLayout,
                      RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                      IndexProvider.Monitor monitor,
                      SchemaIndexDescriptor descriptor,
                      long indexId,
                      IndexSamplingConfig samplingConfig,
                      SpaceFillingCurveConfiguration searchConfiguration ) throws IOException
        {
            super( pageCache, fs, fileLayout.getIndexFile(), fileLayout.layout, recoveryCleanupWorkCollector, monitor, descriptor, indexId, samplingConfig );
            this.layout = fileLayout.layout;
            this.descriptor = descriptor;
            this.samplingConfig = samplingConfig;
            this.searchConfiguration = searchConfiguration;
        }

        @Override
        public SpatialIndexPartReader<NativeSchemaValue> newReader()
        {
            assertOpen();
            return new SpatialIndexPartReader<>( tree, layout, samplingConfig, descriptor, searchConfiguration );
        }

        @Override
        public void verifyDeferredConstraints( PropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
        {
            SpatialVerifyDeferredConstraint.verify( nodePropertyAccessor, layout, tree, descriptor );
            super.verifyDeferredConstraints( nodePropertyAccessor );
        }
    }

    static class PartFactory implements Factory<PartAccessor>
    {
        private final PageCache pageCache;
        private final FileSystemAbstraction fs;
        private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
        private final IndexProvider.Monitor monitor;
        private final SchemaIndexDescriptor descriptor;
        private final long indexId;
        private final IndexSamplingConfig samplingConfig;
        private final SpatialIndexFiles spatialIndexFiles;
        private final SpaceFillingCurveConfiguration searchConfiguration;

        PartFactory( PageCache pageCache,
                     FileSystemAbstraction fs,
                     RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                     IndexProvider.Monitor monitor,
                     SchemaIndexDescriptor descriptor,
                     long indexId,
                     IndexSamplingConfig samplingConfig,
                     SpatialIndexFiles spatialIndexFiles,
                     SpaceFillingCurveConfiguration searchConfiguration )
        {
            this.pageCache = pageCache;
            this.fs = fs;
            this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
            this.monitor = monitor;
            this.descriptor = descriptor;
            this.indexId = indexId;
            this.samplingConfig = samplingConfig;
            this.spatialIndexFiles = spatialIndexFiles;
            this.searchConfiguration = searchConfiguration;
        }

        @Override
        public PartAccessor newSpatial( CoordinateReferenceSystem crs ) throws IOException
        {
            SpatialIndexFiles.SpatialFile spatialFile = spatialIndexFiles.forCrs( crs );
            if ( !fs.fileExists( spatialFile.indexFile ) )
            {
                SpatialIndexFiles.SpatialFileLayout fileLayout = spatialFile.getLayoutForNewIndex();
                createEmptyIndex( fileLayout );
                return createPartAccessor( fileLayout );
            }
            else
            {
                return createPartAccessor( spatialFile.getLayoutForExistingIndex( pageCache ) );
            }
        }

        private PartAccessor createPartAccessor( SpatialIndexFiles.SpatialFileLayout fileLayout ) throws IOException
        {
            return new PartAccessor( pageCache,
                                     fs,
                                     fileLayout,
                                     recoveryCleanupWorkCollector,
                                     monitor,
                                     descriptor,
                                     indexId,
                                     samplingConfig,
                                     searchConfiguration );
        }

        private void createEmptyIndex( SpatialIndexFiles.SpatialFileLayout fileLayout ) throws IOException
        {
            IndexPopulator populator = new SpatialIndexPopulator.PartPopulator( pageCache,
                                                                                fs,
                                                                                fileLayout,
                                                                                monitor,
                                                                                descriptor,
                                                                                indexId,
                                                                                samplingConfig,
                                                                                searchConfiguration );
            populator.create();
            populator.close( true );
        }
    }
}
