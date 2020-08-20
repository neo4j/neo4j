/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexConfigProvider;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexSampler.combineSamples;

class SpatialIndexPopulator extends SpatialIndexCache<WorkSyncedNativeIndexPopulator<SpatialIndexKey,NativeIndexValue>> implements IndexPopulator
{
    SpatialIndexPopulator( StoreIndexDescriptor descriptor, SpatialIndexFiles spatialIndexFiles, PageCache pageCache,
            FileSystemAbstraction fs, IndexProvider.Monitor monitor, SpaceFillingCurveConfiguration configuration,
            TokenNameLookup tokenNameLookup )
    {
        super( new PartFactory( pageCache, fs, spatialIndexFiles, descriptor, monitor, configuration, tokenNameLookup ) );
    }

    @Override
    public synchronized void create()
    {
        forAll( p -> p.getActual().clear(), this );

        // We must make sure to have at least one subindex:
        // to be able to persist failure and to have the right state in the beginning
        if ( !this.iterator().hasNext() )
        {
            select( CoordinateReferenceSystem.WGS84 );
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
        Map<CoordinateReferenceSystem,List<IndexEntryUpdate<?>>> batchMap = new HashMap<>();
        for ( IndexEntryUpdate<?> update : updates )
        {
            PointValue point = (PointValue) update.values()[0];
            List<IndexEntryUpdate<?>> batch = batchMap.computeIfAbsent( point.getCoordinateReferenceSystem(), k -> new ArrayList<>() );
            batch.add( update );
        }
        for ( Map.Entry<CoordinateReferenceSystem,List<IndexEntryUpdate<?>>> entry : batchMap.entrySet() )
        {
            IndexPopulator partPopulator = select( entry.getKey() );
            partPopulator.add( entry.getValue() );
        }
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
    {
        for ( IndexPopulator part : this )
        {
            part.verifyDeferredConstraints( nodePropertyAccessor );
        }
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        return new SpatialIndexPopulatingUpdater( this, accessor );
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
        uncheckedSelect( ((PointValue) values[0]).getCoordinateReferenceSystem() ).includeSample( update );
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

    @Override
    public Map<String,Value> indexConfig()
    {
        Map<String,Value> indexConfig = new HashMap<>();
        for ( IndexPopulator part : this )
        {
            IndexConfigProvider.putAllNoOverwrite( indexConfig, part.indexConfig() );
        }
        return indexConfig;
    }

    static class PartPopulator extends NativeIndexPopulator<SpatialIndexKey,NativeIndexValue>
    {
        private final SpaceFillingCurveConfiguration configuration;
        private final SpaceFillingCurveSettings settings;
        private final CoordinateReferenceSystem crs;

        PartPopulator( PageCache pageCache, FileSystemAbstraction fs, SpatialIndexFiles.SpatialFileLayout fileLayout, IndexProvider.Monitor monitor,
                StoreIndexDescriptor descriptor, SpaceFillingCurveConfiguration configuration, TokenNameLookup tokenNameLookup )
        {
            super( pageCache, fs, fileLayout.getIndexFile(), fileLayout.layout, monitor, descriptor, NO_HEADER_WRITER, tokenNameLookup );
            this.configuration = configuration;
            this.settings = fileLayout.settings;
            this.crs = fileLayout.spatialFile.crs;
        }

        @Override
        public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
        {
            SpatialVerifyDeferredConstraint.verify( nodePropertyAccessor, layout, tree, descriptor );
            super.verifyDeferredConstraints( nodePropertyAccessor );
        }

        @Override
        boolean canCheckConflictsWithoutStoreAccess()
        {
            return false;
        }

        @Override
        ConflictDetectingValueMerger<SpatialIndexKey,NativeIndexValue,Value[]> getMainConflictDetector()
        {
            // Because of lossy point representation in index we need to always compare on node id,
            // even for unique indexes. If we don't we risk throwing constraint violation exception
            // for points that are in fact unique.
            return new ThrowingConflictDetector<>( true );
        }

        @Override
        NativeIndexReader<SpatialIndexKey, NativeIndexValue> newReader()
        {
            return new SpatialIndexPartReader<>( tree, layout, descriptor, configuration );
        }

        @Override
        public synchronized void create()
        {
            create( settings.headerWriter( BYTE_POPULATING ) );
        }

        @Override
        void markTreeAsOnline()
        {
            tree.checkpoint( IOLimiter.UNLIMITED, settings.headerWriter( BYTE_ONLINE ) );
        }

        @Override
        public Map<String,Value> indexConfig()
        {
            Map<String,Value> map = new HashMap<>();
            SpatialIndexConfig.addSpatialConfig( map, crs, settings );
            return map;
        }
    }

    static class PartFactory implements Factory<WorkSyncedNativeIndexPopulator<SpatialIndexKey,NativeIndexValue>>
    {
        private final PageCache pageCache;
        private final FileSystemAbstraction fs;
        private final SpatialIndexFiles spatialIndexFiles;
        private final StoreIndexDescriptor descriptor;
        private final IndexProvider.Monitor monitor;
        private final SpaceFillingCurveConfiguration configuration;
        private final TokenNameLookup tokenNameLookup;

        PartFactory( PageCache pageCache, FileSystemAbstraction fs, SpatialIndexFiles spatialIndexFiles, StoreIndexDescriptor descriptor,
                IndexProvider.Monitor monitor, SpaceFillingCurveConfiguration configuration, TokenNameLookup tokenNameLookup )
        {
            this.pageCache = pageCache;
            this.fs = fs;
            this.spatialIndexFiles = spatialIndexFiles;
            this.descriptor = descriptor;
            this.monitor = monitor;
            this.configuration = configuration;
            this.tokenNameLookup = tokenNameLookup;
        }

        @Override
        public WorkSyncedNativeIndexPopulator<SpatialIndexKey,NativeIndexValue> newSpatial( CoordinateReferenceSystem crs )
        {
            return create( spatialIndexFiles.forCrs( crs ).getLayoutForNewIndex() );
        }

        private WorkSyncedNativeIndexPopulator<SpatialIndexKey,NativeIndexValue> create( SpatialIndexFiles.SpatialFileLayout fileLayout )
        {
            PartPopulator populator = new PartPopulator( pageCache, fs, fileLayout, monitor, descriptor, configuration, tokenNameLookup );
            populator.create();
            return new WorkSyncedNativeIndexPopulator<>( populator );
        }
    }
}
