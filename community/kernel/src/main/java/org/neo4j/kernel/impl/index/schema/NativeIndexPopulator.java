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

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.function.Consumer;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

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
        extends NativeIndex<KEY,VALUE> implements IndexPopulator, ConsistencyCheckableIndexPopulator
{
    public static final byte BYTE_FAILED = 0;
    static final byte BYTE_ONLINE = 1;
    static final byte BYTE_POPULATING = 2;

    private final KEY treeKey;
    private final VALUE treeValue;
    private final UniqueIndexSampler uniqueSampler;
    private final Consumer<PageCursor> additionalHeaderWriter;

    private ConflictDetectingValueMerger<KEY,VALUE> mainConflictDetector;
    private ConflictDetectingValueMerger<KEY,VALUE> updatesConflictDetector;

    private byte[] failureBytes;
    private boolean dropped;
    private boolean closed;

    NativeIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, File storeFile, IndexLayout<KEY,VALUE> layout, IndexProvider.Monitor monitor,
            StoreIndexDescriptor descriptor, Consumer<PageCursor> additionalHeaderWriter, OpenOption... openOptions )
    {
        super( pageCache, fs, storeFile, layout, monitor, descriptor, withNoStriping( openOptions ) );
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

    /**
     * Because index population is effectively single-threaded. For parallel population each thread has its own part so single-threaded even there.
     */
    private static OpenOption[] withNoStriping( OpenOption[] openOptions )
    {
        return ArrayUtils.add( openOptions, PageCacheOpenOptions.NO_CHANNEL_STRIPING );
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
        updatesConflictDetector = new ConflictDetectingValueMerger<>( true );
    }

    ConflictDetectingValueMerger<KEY,VALUE> getMainConflictDetector()
    {
        return new ConflictDetectingValueMerger<>( descriptor.type() == GENERAL );
    }

    @Override
    public synchronized void drop()
    {
        try
        {
            closeTree();
            if ( !hasOpenOption( StandardOpenOption.DELETE_ON_CLOSE ) )
            {
                // This deletion is guarded by a seemingly unnecessary check of this specific open option, but is checked before deletion
                // due to observed problems on some Windows versions where the deletion could otherwise throw j.n.f.AccessDeniedException
                deleteFileIfPresent( fileSystem, storeFile );
            }
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
            if ( populationCompletedSuccessfully )
            {
                assertPopulatorOpen();
                markTreeAsOnline();
            }
            else
            {
                assertNotDropped();
                ensureTreeInstantiated();
                markTreeAsFailed();
            }
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
        if ( failureBytes == null )
        {
            failureBytes = ArrayUtils.EMPTY_BYTE_ARRAY;
        }
        tree.checkpoint( IOLimiter.UNLIMITED, new FailureHeaderWriter( failureBytes ) );
    }

    void markTreeAsOnline()
    {
        tree.checkpoint( IOLimiter.UNLIMITED, new NativeIndexHeaderWriter( BYTE_ONLINE, additionalHeaderWriter ) );
    }

    private void processUpdates( Iterable<? extends IndexEntryUpdate<?>> indexEntryUpdates, ConflictDetectingValueMerger<KEY,VALUE> conflictDetector )
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
