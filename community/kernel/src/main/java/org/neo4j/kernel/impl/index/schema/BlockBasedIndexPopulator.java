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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsWriter;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.util.FeatureToggles;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.NativeIndexUpdater.initializeKeyFromUpdate;
import static org.neo4j.kernel.impl.index.schema.NativeIndexes.deleteIndex;

public class BlockBasedIndexPopulator<KEY extends NativeIndexKey<KEY>,VALUE extends NativeIndexValue> extends NativeIndexPopulator<KEY,VALUE>
{
    private static final String BLOCK_SIZE = FeatureToggles.getString( BlockBasedIndexPopulator.class, "blockSize", "1M" );

    // TODO some better ByteBuffers, right?
    private static final ByteBufferFactory BYTE_BUFFER_FACTORY = ByteBuffer::allocate;

    private final IndexSpecificSpaceFillingCurveSettingsCache spatialSettings;
    private final IndexDirectoryStructure directoryStructure;
    private final SpaceFillingCurveConfiguration configuration;
    private final boolean archiveFailedIndex;
    private final int blockSize;
    private BlockStorage<KEY,VALUE> scanUpdates;
    private IndexUpdateStorage<KEY,VALUE> externalUpdates;

    BlockBasedIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, File file, IndexLayout<KEY,VALUE> layout, IndexProvider.Monitor monitor,
            StoreIndexDescriptor descriptor, IndexSpecificSpaceFillingCurveSettingsCache spatialSettings,
            IndexDirectoryStructure directoryStructure, SpaceFillingCurveConfiguration configuration, boolean archiveFailedIndex )
    {
        super( pageCache, fs, file, layout, monitor, descriptor, new SpaceFillingCurveSettingsWriter( spatialSettings ) );
        this.spatialSettings = spatialSettings;
        this.directoryStructure = directoryStructure;
        this.configuration = configuration;
        this.archiveFailedIndex = archiveFailedIndex;
        this.blockSize = parseBlockSize();
    }

    private static int parseBlockSize()
    {
        long blockSize = ByteUnit.parse( BLOCK_SIZE );
        Preconditions.checkArgument( blockSize >= 20 && blockSize < Integer.MAX_VALUE, "Block size need to fit in int. Was " + blockSize );
        return (int) blockSize;
    }

    @Override
    public void create()
    {
        try
        {
            deleteIndex( fileSystem, directoryStructure, descriptor.getId(), archiveFailedIndex );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        super.create();
        try
        {
            scanUpdates = new BlockStorage<>( layout, BYTE_BUFFER_FACTORY, fileSystem, new File( storeFile.getParentFile(), storeFile.getName() + ".temp" ),
                    BlockStorage.Monitor.NO_MONITOR, blockSize );
            externalUpdates = new IndexUpdateStorage<>( layout, fileSystem, new File( storeFile.getParent(), storeFile.getName() + ".ext" ),
                    BYTE_BUFFER_FACTORY.newBuffer( blockSize ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void drop()
    {
        // TODO Make responsive
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates )
    {
        for ( IndexEntryUpdate<?> update : updates )
        {
            storeUpdate( update, scanUpdates );
        }
    }

    private void storeUpdate( long entityId, Value[] values, BlockStorage<KEY,VALUE> blockStorage )
    {
        try
        {
            KEY key = layout.newKey();
            VALUE value = layout.newValue();
            initializeKeyFromUpdate( key, entityId, values );
            value.from( values );
            blockStorage.add( key, value );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void storeUpdate( IndexEntryUpdate<?> update, BlockStorage<KEY,VALUE> blockStorage )
    {
        storeUpdate( update.getEntityId(), update.values(), blockStorage );
    }

    void finishUp() throws IOException, IndexEntryConflictException
    {
        scanUpdates.doneAdding();
        scanUpdates.merge();
        externalUpdates.doneAdding();
        // don't merge and sort the external updates

        // Build the tree from the scan updates
        writeScanUpdatesToTree();

        // Apply the external updates
        writeExternalUpdatesToTree();
    }

    private void writeExternalUpdatesToTree() throws IOException
    {
        try ( Writer<KEY,VALUE> writer = tree.writer();
              IndexUpdateCursor<KEY,VALUE> updates = externalUpdates.reader() )
        {
            while ( updates.next() )
            {
                switch ( updates.updateMode() )
                {
                case ADDED:
                    writer.put( updates.key(), updates.value() );
                    break;
                case REMOVED:
                    writer.remove( updates.key() );
                    break;
                case CHANGED:
                    writer.remove( updates.key() );
                    writer.put( updates.key(), updates.value() );
                    break;
                default:
                    throw new IllegalArgumentException( "Unknown update mode " + updates.updateMode() );
                }
            }
        }
    }

    private void writeScanUpdatesToTree() throws IOException, IndexEntryConflictException
    {
        ConflictDetectingValueMerger<KEY,VALUE> conflictDetector = getMainConflictDetector();
        try ( Writer<KEY,VALUE> writer = tree.writer();
              BlockReader<KEY,VALUE> reader = scanUpdates.reader() )
        {
            BlockEntryReader<KEY,VALUE> maybeBlock = reader.nextBlock();
            if ( maybeBlock != null )
            {
                try ( BlockEntryReader<KEY,VALUE> block = maybeBlock )
                {
                    while ( block.next() )
                    {
                        conflictDetector.controlConflictDetection( block.key() );
                        writer.merge( block.key(), block.value(), conflictDetector );
                        if ( conflictDetector.wasConflicting() )
                        {
                            conflictDetector.reportConflict( block.key().asValues() );
                        }
                    }
                }
            }
        }
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
    {
        // On building tree
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        return new IndexUpdater()
        {
            @Override
            public void process( IndexEntryUpdate<?> update )
            {
                try
                {
                    externalUpdates.add( update );
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            }

            @Override
            public void close()
            {
            }
        };
    }

    @Override
    NativeIndexReader<KEY,VALUE> newReader()
    {
        throw new UnsupportedOperationException( "Should not be needed because we're overriding the populating updater anyway" );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully )
    {
        // Make responsive
    }

    @Override
    public void markAsFailed( String failure )
    {

    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        // leave for now, can either sample when we build tree in the end or when updates come in
    }

    @Override
    public IndexSample sampleResult()
    {
        // leave for now, look at what NativeIndexPopulator does
        return null;
    }
}
