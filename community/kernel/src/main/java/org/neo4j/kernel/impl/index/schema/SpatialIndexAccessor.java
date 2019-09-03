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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.CombiningIterable;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfigProvider;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.annotations.ReporterFactory;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

class SpatialIndexAccessor extends SpatialIndexCache<SpatialIndexAccessor.PartAccessor> implements IndexAccessor
{
    private final StoreIndexDescriptor descriptor;

    SpatialIndexAccessor( StoreIndexDescriptor descriptor,
                          PageCache pageCache,
                          FileSystemAbstraction fs,
                          RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                          IndexProvider.Monitor monitor,
                          SpatialIndexFiles spatialIndexFiles,
                          SpaceFillingCurveConfiguration searchConfiguration,
                          boolean readOnly ) throws IOException
    {
        super( new PartFactory( pageCache,
                                fs,
                                recoveryCleanupWorkCollector,
                                monitor,
                                descriptor,
                                spatialIndexFiles,
                                searchConfiguration,
                                readOnly ) );
        this.descriptor = descriptor;
        spatialIndexFiles.loadExistingIndexes( this );
    }

    @Override
    public void drop()
    {
        forAll( NativeIndexAccessor::drop, this );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return new SpatialIndexUpdater( this, mode );
    }

    @Override
    public void force( IOLimiter ioLimiter )
    {
        for ( NativeIndexAccessor part : this )
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
    public void close()
    {
        closeInstantiateCloseLock();
        forAll( NativeIndexAccessor::close, this );
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
        for ( NativeIndexAccessor<?,?> part : this )
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
        for ( NativeIndexAccessor<?,?> part : this )
        {
            snapshotFiles.add( part.snapshotFiles() );
        }
        return concatResourceIterators( snapshotFiles.iterator() );
    }

    @Override
    public Map<String,Value> indexConfig()
    {
        Map<String,Value> indexConfig = new HashMap<>();
        for ( NativeIndexAccessor<?,?> part : this )
        {
            IndexConfigProvider.putAllNoOverwrite( indexConfig, part.indexConfig() );
        }
        return indexConfig;
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
    {
        for ( NativeIndexAccessor<?,?> part : this )
        {
            part.verifyDeferredConstraints( nodePropertyAccessor );
        }
    }

    @Override
    public boolean isDirty()
    {
        return Iterators.stream( iterator() ).anyMatch( NativeIndexAccessor::isDirty );
    }

    @Override
    public boolean consistencyCheck( ReporterFactory reporterFactory )
    {
        return FusionIndexBase.consistencyCheck( this, reporterFactory );
    }

    static class PartAccessor extends NativeIndexAccessor<SpatialIndexKey,NativeIndexValue>
    {
        private final IndexLayout<SpatialIndexKey,NativeIndexValue> layout;
        private final StoreIndexDescriptor descriptor;
        private final SpaceFillingCurveConfiguration searchConfiguration;
        private CoordinateReferenceSystem crs;
        private SpaceFillingCurveSettings settings;

        PartAccessor( PageCache pageCache, FileSystemAbstraction fs, SpatialIndexFiles.SpatialFileLayout fileLayout,
                RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, IndexProvider.Monitor monitor, StoreIndexDescriptor descriptor,
                SpaceFillingCurveConfiguration searchConfiguration, boolean readOnly )
        {
            super( pageCache, fs, fileLayout.getIndexFile(), fileLayout.layout, monitor, descriptor, NO_HEADER_WRITER, readOnly );
            this.layout = fileLayout.layout;
            this.descriptor = descriptor;
            this.searchConfiguration = searchConfiguration;
            this.crs = fileLayout.spatialFile.crs;
            this.settings = fileLayout.settings;
            instantiateTree( recoveryCleanupWorkCollector, headerWriter );
        }

        @Override
        public SpatialIndexPartReader<NativeIndexValue> newReader()
        {
            assertOpen();
            return new SpatialIndexPartReader<>( tree, layout, descriptor, searchConfiguration );
        }

        @Override
        public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
        {
            SpatialVerifyDeferredConstraint.verify( nodePropertyAccessor, layout, tree, descriptor );
            super.verifyDeferredConstraints( nodePropertyAccessor );
        }

        @Override
        public Map<String,Value> indexConfig()
        {
            Map<String,Value> map = new HashMap<>();
            SpatialIndexConfig.addSpatialConfig( map, crs, settings );
            return map;
        }
    }

    static class PartFactory implements Factory<PartAccessor>
    {
        private final PageCache pageCache;
        private final FileSystemAbstraction fs;
        private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
        private final IndexProvider.Monitor monitor;
        private final StoreIndexDescriptor descriptor;
        private final SpatialIndexFiles spatialIndexFiles;
        private final SpaceFillingCurveConfiguration searchConfiguration;
        private final boolean readOnly;

        PartFactory( PageCache pageCache,
                     FileSystemAbstraction fs,
                     RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                     IndexProvider.Monitor monitor,
                     StoreIndexDescriptor descriptor,
                     SpatialIndexFiles spatialIndexFiles,
                     SpaceFillingCurveConfiguration searchConfiguration,
                     boolean readOnly )
        {
            this.pageCache = pageCache;
            this.fs = fs;
            this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
            this.monitor = monitor;
            this.descriptor = descriptor;
            this.spatialIndexFiles = spatialIndexFiles;
            this.searchConfiguration = searchConfiguration;
            this.readOnly = readOnly;
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
                                     searchConfiguration,
                                     readOnly );
        }

        private void createEmptyIndex( SpatialIndexFiles.SpatialFileLayout fileLayout )
        {
            IndexPopulator populator = new SpatialIndexPopulator.PartPopulator( pageCache,
                                                                                fs,
                                                                                fileLayout,
                                                                                monitor,
                                                                                descriptor,
                                                                                searchConfiguration );
            populator.create();
            populator.close( true );
        }
    }
}
