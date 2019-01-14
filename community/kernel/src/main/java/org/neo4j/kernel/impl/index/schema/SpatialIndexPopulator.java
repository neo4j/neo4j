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

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexSampler.combineSamples;

class SpatialIndexPopulator extends SpatialIndexCache<SpatialIndexPopulator.PartPopulator> implements IndexPopulator
{
    SpatialIndexPopulator( long indexId,
            SchemaIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            SpatialIndexFiles spatialIndexFiles,
            PageCache pageCache,
            FileSystemAbstraction fs,
            IndexProvider.Monitor monitor,
            SpaceFillingCurveConfiguration configuration )
    {
        super( new PartFactory( pageCache, fs, spatialIndexFiles, indexId, descriptor, monitor, samplingConfig, configuration ) );
    }

    @Override
    public synchronized void create() throws IOException
    {
        forAll( NativeSchemaIndexPopulator::clear, this );

        // We must make sure to have at least one subindex:
        // to be able to persist failure and to have the right state in the beginning
        if ( !this.iterator().hasNext() )
        {
            select( CoordinateReferenceSystem.WGS84 );
        }
    }

    @Override
    public synchronized void drop() throws IOException
    {
        forAll( NativeSchemaIndexPopulator::drop, this );
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IOException, IndexEntryConflictException
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
            PartPopulator partPopulator = select( entry.getKey() );
            partPopulator.add( entry.getValue() );
        }
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException, IOException
    {
        for ( IndexPopulator part : this )
        {
            part.verifyDeferredConstraints( propertyAccessor );
        }
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor )
    {
        return new SpatialIndexPopulatingUpdater( this, accessor );
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
        uncheckedSelect( ((PointValue) values[0]).getCoordinateReferenceSystem() ).includeSample( update );
    }

    @Override
    public IndexSample sampleResult()
    {
        IndexSample[] indexSamples = StreamSupport.stream( this.spliterator(), false )
                .map( PartPopulator::sampleResult )
                .toArray( IndexSample[]::new );
        return combineSamples( indexSamples );
    }

    static class PartPopulator extends NativeSchemaIndexPopulator<SpatialSchemaKey, NativeSchemaValue>
    {
        private final SpaceFillingCurveConfiguration configuration;
        private final SpaceFillingCurveSettings settings;

        PartPopulator( PageCache pageCache, FileSystemAbstraction fs, SpatialIndexFiles.SpatialFileLayout fileLayout,
                IndexProvider.Monitor monitor, SchemaIndexDescriptor descriptor, long indexId, IndexSamplingConfig samplingConfig,
                SpaceFillingCurveConfiguration configuration )
        {
            super( pageCache, fs, fileLayout.getIndexFile(), fileLayout.layout, monitor, descriptor, indexId, samplingConfig );
            this.configuration = configuration;
            this.settings = fileLayout.settings;
        }

        @Override
        public void verifyDeferredConstraints( PropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
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
        ConflictDetectingValueMerger<SpatialSchemaKey,NativeSchemaValue> getMainConflictDetector()
        {
            // Because of lossy point representation in index we need to always compare on node id,
            // even for unique indexes. If we don't we risk throwing constraint violation exception
            // for points that are in fact unique.
            return new ConflictDetectingValueMerger<>( true );
        }

        @Override
        IndexReader newReader()
        {
            return new SpatialIndexPartReader<>( tree, layout, samplingConfig, descriptor, configuration );
        }

        @Override
        public synchronized void create() throws IOException
        {
            create( settings.headerWriter( BYTE_POPULATING ) );
        }

        @Override
        void markTreeAsOnline() throws IOException
        {
            tree.checkpoint( IOLimiter.unlimited(), settings.headerWriter( BYTE_ONLINE ) );
        }
    }

    static class PartFactory implements Factory<PartPopulator>
    {
        private final PageCache pageCache;
        private final FileSystemAbstraction fs;
        private final SpatialIndexFiles spatialIndexFiles;
        private final long indexId;
        private final SchemaIndexDescriptor descriptor;
        private final IndexProvider.Monitor monitor;
        private final IndexSamplingConfig samplingConfig;
        private final SpaceFillingCurveConfiguration configuration;

        PartFactory( PageCache pageCache, FileSystemAbstraction fs, SpatialIndexFiles spatialIndexFiles, long indexId,
                SchemaIndexDescriptor descriptor, IndexProvider.Monitor monitor, IndexSamplingConfig samplingConfig,
                SpaceFillingCurveConfiguration configuration )
        {
            this.pageCache = pageCache;
            this.fs = fs;
            this.spatialIndexFiles = spatialIndexFiles;
            this.indexId = indexId;
            this.descriptor = descriptor;
            this.monitor = monitor;
            this.samplingConfig = samplingConfig;
            this.configuration = configuration;
        }

        @Override
        public PartPopulator newSpatial( CoordinateReferenceSystem crs ) throws IOException
        {
            return create( spatialIndexFiles.forCrs( crs ).getLayoutForNewIndex() );
        }

        private PartPopulator create( SpatialIndexFiles.SpatialFileLayout fileLayout ) throws IOException
        {
            PartPopulator populator = new PartPopulator( pageCache, fs, fileLayout, monitor, descriptor, indexId, samplingConfig, configuration );
            populator.create();
            return populator;
        }
    }
}
