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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.IndexUpdateApply;
import org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.IndexUpdateWork;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.GENERAL;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_ONLINE;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_POPULATING;

/**
 * A dynamically created sub-index specific to a particular coordinate reference system.
 * This allows the fusion index design to be extended to an unknown number of sub-indexes, one for each CRS.
 */
public class SpatialCRSSchemaIndex
{
    private final File indexFile;
    private final PageCache pageCache;
    private final CoordinateReferenceSystem crs;
    private final FileSystemAbstraction fs;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final SpaceFillingCurveConfiguration configuration;
    private final SpaceFillingCurve curve;

    private State state;
    private boolean dropped;
    private byte[] failureBytes;
    private SpatialSchemaKey treeKey;
    private NativeSchemaValue treeValue;
    private SpatialLayout layout;
    private NativeSchemaIndexUpdater<SpatialSchemaKey,NativeSchemaValue> singleUpdater;
    private NativeSchemaIndex<SpatialSchemaKey,NativeSchemaValue> schemaIndex;
    private WorkSync<IndexUpdateApply<SpatialSchemaKey,NativeSchemaValue>,IndexUpdateWork<SpatialSchemaKey,NativeSchemaValue>> additionsWorkSync;
    private WorkSync<IndexUpdateApply<SpatialSchemaKey,NativeSchemaValue>,IndexUpdateWork<SpatialSchemaKey,NativeSchemaValue>> updatesWorkSync;

    public SpatialCRSSchemaIndex( IndexDescriptor descriptor,
            IndexDirectoryStructure directoryStructure,
            CoordinateReferenceSystem crs,
            long indexId,
            PageCache pageCache,
            FileSystemAbstraction fs,
            IndexProvider.Monitor monitor,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            SpaceFillingCurveConfiguration configuration,
            int maxBits )
    {
        this.crs = crs;
        this.pageCache = pageCache;
        this.fs = fs;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.configuration = configuration;

        // Depends on crs
        IndexProvider.Descriptor crsDescriptor =
                new IndexProvider.Descriptor( Integer.toString( crs.getTable().getTableId() ), Integer.toString( crs.getCode() ) );
        IndexDirectoryStructure indexDir =
                IndexDirectoryStructure.directoriesBySubProvider( directoryStructure ).forProvider( crsDescriptor );
        indexFile = new File( indexDir.directoryForIndex( indexId ), "index-" + indexId );
        if ( crs.getDimension() == 2 )
        {
            curve = new HilbertSpaceFillingCurve2D( envelopeFromCRS( crs ), Math.min( 30, maxBits / 2 ) );
        }
        else if ( crs.getDimension() == 3 )
        {
            curve = new HilbertSpaceFillingCurve3D( envelopeFromCRS( crs ), Math.min( 20, maxBits / 3 ) );
        }
        else
        {
            throw new IllegalArgumentException( "Cannot create spatial index with other than 2D or 3D coordinate reference system: " + crs );
        }
        state = State.INIT;

        layout = layout( descriptor );
        treeKey = layout.newKey();
        treeValue = layout.newValue();
        schemaIndex = new NativeSchemaIndex<>( pageCache, fs, indexFile, layout, monitor, descriptor, indexId );
    }

    /**
     * Makes sure that the index is ready to populate
     */
    public void startPopulation() throws IOException
    {
        if ( state == State.INIT )
        {
            // First add to sub-index, make sure to create
            create();
        }
        if ( state != State.POPULATING )
        {
            throw new IllegalStateException( "Failed to start populating index." );
        }
    }

    /**
     * Makes sure that the index is online
     */
    public void takeOnline() throws IOException
    {
        if ( !indexExists() )
        {
            throw new IOException( "Index file does not exist." );
        }
        if ( state == State.INIT || state == State.POPULATED )
        {
            if ( state == State.INIT )
            {
                schemaIndex.instantiateTree( recoveryCleanupWorkCollector, NO_HEADER_WRITER );
            }
            online();
        }
        if ( state != State.ONLINE )
        {
            throw new IllegalStateException( "Failed to bring index online." );
        }
    }

    public IndexUpdater updaterWithCreate( boolean populating ) throws IOException
    {
        if ( populating )
        {
            if ( state == State.INIT )
            {
                // sub-index didn't exist, create in populating mode
                create();
            }
            return newPopulatingUpdater();
        }
        else
        {
            if ( state == State.INIT )
            {
                // sub-index didn't exist, create and make it online
                create();
                finishPopulation( true );
                online();
            }
            return newUpdater();
        }
    }

    // ONLINE

    public void close() throws IOException
    {
        schemaIndex.closeTree();
    }

    public BoundedIterable<Long> newAllEntriesReader()
    {
        return new NativeAllEntriesReader<>( schemaIndex.tree, layout );
    }

    public IndexReader newReader( IndexSamplingConfig samplingConfig, IndexDescriptor descriptor )
    {
        schemaIndex.assertOpen();
        return new SpatialSchemaIndexReader<>( schemaIndex.tree, layout, samplingConfig, descriptor, configuration );
    }

    public ResourceIterator<File> snapshotFiles()
    {
        return asResourceIterator( iterator( indexFile ) );
    }

    public void force( IOLimiter ioLimiter ) throws IOException
    {
        schemaIndex.tree.checkpoint( ioLimiter );
    }

    public boolean wasDirtyOnStartup()
    {
        return schemaIndex.tree.wasDirtyOnStartup();
    }

    private IndexUpdater newUpdater()
    {
        schemaIndex.assertOpen();
        try
        {
            return singleUpdater.initialize( schemaIndex.tree.writer() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    // POPULATING

    public synchronized void finishPopulation( boolean populationCompletedSuccessfully ) throws IOException
    {
        assert state == State.POPULATING;
        if ( populationCompletedSuccessfully && failureBytes != null )
        {
            throw new IllegalStateException( "Can't mark index as online after it has been marked as failure" );
        }

        if ( populationCompletedSuccessfully )
        {
            schemaIndex.assertOpen();
            markTreeAsOnline();
            state = State.POPULATED;
        }
        else
        {
            assertNotDropped();
            ensureTreeInstantiated();
            markTreeAsFailed();
            state = State.FAILED;
        }
    }

    public void add( Collection<IndexEntryUpdate<?>> updates ) throws IOException
    {
        applyWithWorkSync( additionsWorkSync, updates );
    }

    private IndexUpdater newPopulatingUpdater()
    {
        return new IndexUpdater()
        {
            private boolean closed;
            private final Collection<IndexEntryUpdate<?>> updates = new ArrayList<>();

            @Override
            public void process( IndexEntryUpdate<?> update )
            {
                assertOpen();
                updates.add( update );
            }

            @Override
            public void close() throws IOException
            {
                applyWithWorkSync( updatesWorkSync, updates );
                closed = true;
            }

            private void assertOpen()
            {
                if ( closed )
                {
                    throw new IllegalStateException( "Updater has been closed" );
                }
            }
        };
    }

    // GENERAL

    public synchronized void drop() throws IOException
    {
        try
        {
            schemaIndex.closeTree();
            schemaIndex.gbpTreeFileUtil.deleteFileIfPresent( indexFile );
        }
        finally
        {
            dropped = true;
            state = State.INIT;
        }
    }

    public void markAsFailed( String failure )
    {
        failureBytes = failure.getBytes( StandardCharsets.UTF_8 );
        state = State.FAILED;
    }

    public boolean indexExists()
    {
        return fs.fileExists( indexFile );
    }

    public String readPopulationFailure( IndexDescriptor descriptor ) throws IOException
    {
        return NativeSchemaIndexes.readFailureMessage( pageCache, indexFile, layout( descriptor ) );
    }

    public InternalIndexState readState( IndexDescriptor descriptor ) throws IOException
    {
        return NativeSchemaIndexes.readState( pageCache, indexFile, layout( descriptor ) );
    }

    private synchronized void create() throws IOException
    {
        assert state == State.INIT;
        schemaIndex.gbpTreeFileUtil.deleteFileIfPresent( indexFile );
        schemaIndex.instantiateTree( RecoveryCleanupWorkCollector.IMMEDIATE, new NativeSchemaIndexHeaderWriter( BYTE_POPULATING ) );
        additionsWorkSync = new WorkSync<>( new IndexUpdateApply<>( schemaIndex.tree, treeKey, treeValue,
                new ConflictDetectingValueMerger<>( schemaIndex.descriptor.type() == GENERAL ) ) );
        updatesWorkSync = new WorkSync<>( new IndexUpdateApply<>( schemaIndex.tree, treeKey, treeValue, new ConflictDetectingValueMerger<>( true ) ) );
        state = State.POPULATING;
    }

    private void online() throws IOException
    {
        assert state == State.POPULATED || state == State.INIT;
        singleUpdater = new NativeSchemaIndexUpdater<>( treeKey, treeValue );
        state = State.ONLINE;
    }

    private void applyWithWorkSync( WorkSync<IndexUpdateApply<SpatialSchemaKey,NativeSchemaValue>,IndexUpdateWork<SpatialSchemaKey,NativeSchemaValue>> workSync,
            Collection<? extends IndexEntryUpdate<?>> updates ) throws IOException
    {
        try
        {
            workSync.apply( new IndexUpdateWork<>( updates ) );
        }
        catch ( ExecutionException e )
        {
            throw new IOException( e );
        }
    }

    private void markTreeAsOnline() throws IOException
    {
        schemaIndex.tree.checkpoint( IOLimiter.unlimited(), pc -> pc.putByte( BYTE_ONLINE ) );
    }

    private void markTreeAsFailed() throws IOException
    {
        if ( failureBytes == null )
        {
            failureBytes = new byte[0];
        }
        schemaIndex.tree.checkpoint( IOLimiter.unlimited(), new FailureHeaderWriter( failureBytes ) );
    }

    private void assertNotDropped()
    {
        if ( dropped )
        {
            throw new IllegalStateException( "Populator has already been dropped." );
        }
    }

    private void ensureTreeInstantiated() throws IOException
    {
        if ( schemaIndex.tree == null )
        {
            schemaIndex.instantiateTree( RecoveryCleanupWorkCollector.IGNORE, NO_HEADER_WRITER );
        }
    }

    private SpatialLayout layout( IndexDescriptor descriptor )
    {
        SpatialLayout layout;
        if ( isUnique( descriptor ) )
        {
            layout = new SpatialLayoutUnique( crs, curve );
        }
        else
        {
            layout = new SpatialLayoutNonUnique( crs, curve );
        }
        return layout;
    }

    private boolean isUnique( IndexDescriptor descriptor )
    {
        switch ( descriptor.type() )
        {
        case GENERAL:
            return false;
        case UNIQUE:
            return true;
        default:
            throw new UnsupportedOperationException( "Unexpected index type " + descriptor.type() );
        }
    }

    private enum State
    {
        INIT,
        POPULATING,
        POPULATED,
        ONLINE,
        FAILED
    }

    public interface Supplier
    {
        SpatialCRSSchemaIndex get( IndexDescriptor descriptor,
                Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex> indexMap, long indexId,
                CoordinateReferenceSystem crs );
    }

    static Envelope envelopeFromCRS( CoordinateReferenceSystem crs )
    {
        Pair<double[],double[]> indexEnvelope = crs.getIndexEnvelope();
        return new Envelope( indexEnvelope.first(), indexEnvelope.other() );
    }
}
