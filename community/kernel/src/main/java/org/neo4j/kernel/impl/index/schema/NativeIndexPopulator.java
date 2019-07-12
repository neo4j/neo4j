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
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.function.Consumer;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.storageengine.api.schema.IndexDescriptor.Type.GENERAL;
import static org.neo4j.storageengine.api.schema.IndexDescriptor.Type.UNIQUE;

/**
 * {@link IndexPopulator} backed by a {@link GBPTree}.
 *
 * @param <KEY> type of {@link NativeIndexSingleValueKey}.
 * @param <VALUE> type of {@link NativeIndexValue}.
 */
public abstract class NativeIndexPopulator<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
        extends NativeIndex<KEY,VALUE> implements IndexPopulator, ConsistencyCheckable
{
    public static final byte BYTE_FAILED = 0;
    static final byte BYTE_ONLINE = 1;
    static final byte BYTE_POPULATING = 2;

    private final KEY treeKey;
    private final VALUE treeValue;
    private final UniqueIndexSampler uniqueSampler;
    private final Consumer<PageCursor> additionalHeaderWriter;

    private ConflictDetectingValueMerger<KEY,VALUE,Value[]> mainConflictDetector;
    private ConflictDetectingValueMerger<KEY,VALUE,Value[]> updatesConflictDetector;

    private byte[] failureBytes;
    private boolean dropped;
    private boolean closed;

    NativeIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, File storeFile, IndexLayout<KEY,VALUE> layout, IndexProvider.Monitor monitor,
            StoreIndexDescriptor descriptor, Consumer<PageCursor> additionalHeaderWriter )
    {
        super( pageCache, fs, storeFile, layout, monitor, descriptor, false );
        this.treeKey = layout.newKey();
        this.treeValue = layout.newValue();
        this.additionalHeaderWriter = additionalHeaderWriter;
        switch ( descriptor.type() )
        {
        case GENERAL:
            uniqueSampler = null;
            break;
        case UNIQUE:
            uniqueSampler = new UniqueIndexSampler();
            break;
        default:
            throw new IllegalArgumentException( "Unexpected index type " + descriptor.type() );
        }
    }

    public void clear()
    {
        deleteFileIfPresent( fileSystem, storeFile );
    }

    @Override
    public synchronized void create()
    {
        create( new NativeIndexHeaderWriter( BYTE_POPULATING, additionalHeaderWriter ) );
    }

    protected synchronized void create( Consumer<PageCursor> headerWriter )
    {
        assertNotDropped();
        assertNotClosed();

        deleteFileIfPresent( fileSystem, storeFile );
        instantiateTree( RecoveryCleanupWorkCollector.immediate(), headerWriter );

        // true:  tree uniqueness is (value,entityId)
        // false: tree uniqueness is (value) <-- i.e. more strict
        mainConflictDetector = getMainConflictDetector();
        // for updates we have to have uniqueness on (value,entityId) to allow for intermediary violating updates.
        // there are added conflict checks after updates have been applied.
        updatesConflictDetector = new ThrowingConflictDetector<>( true );
    }

    ConflictDetectingValueMerger<KEY,VALUE,Value[]> getMainConflictDetector()
    {
        return new ThrowingConflictDetector<>( descriptor.type() == GENERAL );
    }

    @Override
    public synchronized void drop()
    {
        try
        {
            closeTree();
            deleteFileIfPresent( fileSystem, storeFile );
        }
        finally
        {
            dropped = true;
            closed = true;
        }
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException
    {
        processUpdates( updates, mainConflictDetector );
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
    {
        // No-op, uniqueness is checked for each update in add(IndexEntryUpdate)
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        return newPopulatingUpdater();
    }

    IndexUpdater newPopulatingUpdater()
    {
        IndexUpdater updater = new CollectingIndexUpdater( updates -> processUpdates( updates, updatesConflictDetector ) );
        if ( descriptor.type() == UNIQUE && canCheckConflictsWithoutStoreAccess() )
        {
            // The index population detects conflicts on the fly, however for updates coming in we're in a position
            // where we cannot detect conflicts while applying, but instead afterwards.
            updater = new DeferredConflictCheckingIndexUpdater( updater, this::newReader, descriptor );
        }
        return updater;
    }

    boolean canCheckConflictsWithoutStoreAccess()
    {
        return true;
    }

    abstract NativeIndexReader<KEY,VALUE> newReader();

    @Override
    public synchronized void close( boolean populationCompletedSuccessfully )
    {
        if ( populationCompletedSuccessfully && failureBytes != null )
        {
            throw new IllegalStateException( "Can't mark index as online after it has been marked as failure" );
        }

        try
        {
            assertNotDropped();
            if ( populationCompletedSuccessfully )
            {
                // Successful and completed population
                assertPopulatorOpen();
                markTreeAsOnline();
            }
            else if ( failureBytes != null )
            {
                // Failed population
                ensureTreeInstantiated();
                markTreeAsFailed();
            }
            // else cancelled population. Here we simply close the tree w/o checkpointing it and it will look like POPULATING state on next open
        }
        finally
        {
            closeTree();
            closed = true;
        }
    }

    private void assertNotDropped()
    {
        if ( dropped )
        {
            throw new IllegalStateException( "Populator has already been dropped." );
        }
    }

    private void assertNotClosed()
    {
        if ( closed )
        {
            throw new IllegalStateException( "Populator has already been closed." );
        }
    }

    @Override
    public void markAsFailed( String failure )
    {
        failureBytes = failure.getBytes( StandardCharsets.UTF_8 );
    }

    private void ensureTreeInstantiated()
    {
        if ( tree == null )
        {
            instantiateTree( RecoveryCleanupWorkCollector.ignore(), NO_HEADER_WRITER );
        }
    }

    private void assertPopulatorOpen()
    {
        if ( tree == null )
        {
            throw new IllegalStateException( "Populator has already been closed." );
        }
    }

    private void markTreeAsFailed()
    {
        Preconditions.checkState( failureBytes != null, "markAsFailed hasn't been called, populator not actually failed?" );
        tree.checkpoint( IOLimiter.UNLIMITED, new FailureHeaderWriter( failureBytes ) );
    }

    void markTreeAsOnline()
    {
        tree.checkpoint( IOLimiter.UNLIMITED, new NativeIndexHeaderWriter( BYTE_ONLINE, additionalHeaderWriter ) );
    }

    private void processUpdates( Iterable<? extends IndexEntryUpdate<?>> indexEntryUpdates, ConflictDetectingValueMerger<KEY,VALUE,Value[]> conflictDetector )
            throws IndexEntryConflictException
    {
        try ( Writer<KEY,VALUE> writer = tree.writer() )
        {
            for ( IndexEntryUpdate<?> indexEntryUpdate : indexEntryUpdates )
            {
                NativeIndexUpdater.processUpdate( treeKey, treeValue, indexEntryUpdate, writer, conflictDetector );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        switch ( descriptor.type() )
        {
        case GENERAL:
            // Don't do anything here, we'll do a scan in the end instead
            break;
        case UNIQUE:
            updateUniqueSample( update );
            break;
        default:
            throw new IllegalArgumentException( "Unexpected index type " + descriptor.type() );
        }
    }

    private void updateUniqueSample( IndexEntryUpdate<?> update )
    {
        switch ( update.updateMode() )
        {
        case ADDED:
            uniqueSampler.increment( 1 );
            break;
        case REMOVED:
            uniqueSampler.increment( -1 );
            break;
        case CHANGED:
            break;
        default:
            throw new IllegalArgumentException( "Unsupported update mode type:" + update.updateMode() );
        }
    }

    @Override
    public IndexSample sampleResult()
    {
        switch ( descriptor.type() )
        {
        case GENERAL:
            return new FullScanNonUniqueIndexSampler<>( tree, layout ).result();
        case UNIQUE:
            return uniqueSampler.result();
        default:
            throw new IllegalArgumentException( "Unexpected index type " + descriptor.type() );
        }
    }

    private static void deleteFileIfPresent( FileSystemAbstraction fs, File storeFile )
    {
        try
        {
            fs.deleteFileOrThrow( storeFile );
        }
        catch ( NoSuchFileException e )
        {
            // File does not exist, we don't need to delete
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
