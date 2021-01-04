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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.set.ImmutableSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyCheckVisitor;
import org.neo4j.index.internal.gbptree.Header;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityTokenUpdateListener;

import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.kernel.impl.index.schema.TokenScanValue.RANGE_SIZE;

/**
 * Implements {@link TokenScanStore} and thus also implements {@link LabelScanStore}.
 *
 * Uses {@link GBPTree} atop a {@link PageCache}.
 * Only a single writer is allowed at any given point in time so synchronization or merging of updates
 * need to be handled externally.
 * <p>
 * About the {@link Layout} used in this instance of {@link GBPTree}:
 * <ul>
 * <li>
 * Each keys is a combination of {@code entityId} and {@code entityIdRange} ({@code entityId/64}).
 * </li>
 * <li>
 * Each value is a 64-bit bit set (a primitive {@code long}) where each set bit in it represents
 * an entity with that token, such that {@code entityId = entityIdRange+bitOffset}.
 * </li>
 * </ul>
 * <p>
 * {@link #force(IOLimiter, PageCursorTracer)} is vital for allowing this store to be recoverable, and must be called
 * whenever Neo4j performs a checkpoint.
 * <p>
 * This store is backed by a single store file, "neostore.labelscanstore.db" for {@link LabelScanStore}.
 */
public abstract class NativeTokenScanStore implements TokenScanStore, EntityTokenUpdateListener
{
    private static final String TOKEN_SCAN_REBUILD_TAG = "tokenScanRebuild";
    /**
     * Written in header to indicate native token scan store is clean
     */
    private static final byte CLEAN = (byte) 0x00;

    /**
     * Written in header to indicate native token scan store is rebuilding
     */
    private static final byte NEEDS_REBUILDING = (byte) 0x01;

    /**
     * The type of entity this scan store is backing.
     */
    private final EntityType entityType;

    /**
     * Whether or not this token scan store is read-only.
     */
    private final boolean readOnly;

    /**
     * Monitoring internal events.
     */
    private final Monitor monitor;

    /**
     * Monitors used to pass down monitor to underlying {@link GBPTree}
     */
    private final Monitors monitors;

    /**
     * {@link PageCache} to {@link PageCache#map(Path, int, ImmutableSet)}
     * store file backing this token scan store. Passed to {@link GBPTree}.
     */
    private final PageCache pageCache;

    /**
     * Store file {@link PageCache#map(Path, int, ImmutableSet)}.
     */
    private final Path storeFile;

    /**
     * Used in {@link #start()} if the store is empty, where this will provide all data for fully populating
     * this token scan store. This can be the case when if file was corrupt or missing.
     */
    private final FullStoreChangeStream fullStoreChangeStream;

    /**
     * {@link FileSystemAbstraction} the backing file lives on.
     * Used for all file operations on the gbpTree file.
     */
    private final FileSystemAbstraction fs;

    /**
     * Layout of the database.
     */
    private final DatabaseLayout directoryStructure;
    private final Config config;
    private final PageCacheTracer cacheTracer;
    private final MemoryTracker memoryTracker;

    /**
     * The index which backs this token scan store. Instantiated in {@link #init()} and considered
     * started after call to {@link #start()}.
     */
    private GBPTree<TokenScanKey,TokenScanValue> index;

    /**
     * Set during {@link #init()} if {@link #start()} will need to rebuild the whole token scan store from
     * {@link FullStoreChangeStream}.
     */
    private boolean needsRebuild;

    /**
     * Passed to underlying {@link GBPTree} which use it to submit recovery cleanup jobs.
     */
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;

    /**
     * The single instance of {@link NativeTokenScanWriter} used for updates.
     */
    private NativeTokenScanWriter singleWriter;

    /**
     * Monitor for all writes going into this token scan store.
     */
    private NativeTokenScanWriter.WriteMonitor writeMonitor;

    /**
     * Name of the store that will be used when describing work related to this store.
     */
    private final String tokenStoreName;

    /**
     * Write rebuilding bit to header.
     */
    private static final Consumer<PageCursor> needsRebuildingWriter =
            pageCursor -> pageCursor.putByte( NEEDS_REBUILDING );

    /**
     * Write clean header.
     */
    private static final Consumer<PageCursor> writeClean = pageCursor -> pageCursor.putByte( CLEAN );

    NativeTokenScanStore( PageCache pageCache, DatabaseLayout directoryStructure, FileSystemAbstraction fs, FullStoreChangeStream fullStoreChangeStream,
            boolean readOnly, Config config, Monitors monitors, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, EntityType entityType,
            PageCacheTracer cacheTracer, MemoryTracker memoryTracker, String tokenStoreName )
    {
        this.pageCache = pageCache;
        this.fs = fs;
        this.fullStoreChangeStream = fullStoreChangeStream;
        this.directoryStructure = directoryStructure;
        this.config = config;
        this.cacheTracer = cacheTracer;
        this.memoryTracker = memoryTracker;
        boolean isLabelScanStore = entityType == EntityType.NODE;
        this.storeFile = isLabelScanStore ? directoryStructure.labelScanStore() : directoryStructure.relationshipTypeScanStore();
        this.readOnly = readOnly;
        this.monitors = monitors;
        String monitorTag = isLabelScanStore ? TokenScanStore.LABEL_SCAN_STORE_MONITOR_TAG : TokenScanStore.RELATIONSHIP_TYPE_SCAN_STORE_MONITOR_TAG;
        this.monitor = monitors.newMonitor( Monitor.class, monitorTag );
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.entityType = entityType;
        this.tokenStoreName = tokenStoreName;
    }

    @Override
    public EntityType entityType()
    {
        return entityType;
    }

    /**
     * @return {@link TokenScanReader} capable of finding entity ids with given token ids.
     * Readers will immediately see updates made by {@link TokenScanWriter}, although {@link TokenScanWriter}
     * may internally batch updates so functionality isn't reliable. The only given is that readers will
     * see at least updates from closed {@link TokenScanWriter writers}.
     */
    @Override
    public TokenScanReader newReader()
    {
        return new NativeTokenScanReader( index );
    }

    /**
     * Returns {@link TokenScanWriter} capable of making changes to this {@link TokenScanStore}.
     * Only a single writer is allowed at any given point in time.
     *
     * @param cursorTracer underlying page cursor events tracer.
     * @return {@link TokenScanWriter} capable of making changes to this {@link TokenScanStore}.
     * @throws IllegalStateException if someone else has already acquired a writer and hasn't yet
     * called {@link TokenScanWriter#close()}.
     */
    @Override
    public TokenScanWriter newWriter( PageCursorTracer cursorTracer )
    {
        assertWritable();

        try
        {
            return writer( cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    /**
     * Returns a {@link TokenScanWriter} like that from {@link #newWriter(PageCursorTracer)}, but is specialized in bulk-writing new data.
     *
     * @return {@link TokenScanWriter} capable of making changes to this {@link TokenScanStore}.
     * @throws IllegalStateException if someone else has already acquired a writer and hasn't yet
     * called {@link TokenScanWriter#close()}.
     */
    @Override
    public TokenScanWriter newBulkAppendWriter( PageCursorTracer cursorTracer )
    {
        assertWritable();

        try
        {
            return new BulkAppendNativeTokenScanWriter( index.writer( cursorTracer ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void assertWritable()
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Can't create index writer in read only mode." );
        }
    }

    @Override
    public void applyUpdates( Iterable<EntityTokenUpdate> tokenUpdates, PageCursorTracer cursorTracer )
    {
        try ( TokenScanWriter writer = newWriter( cursorTracer ) )
        {
            for ( EntityTokenUpdate update : tokenUpdates )
            {
                writer.write( update );
            }
        }
        catch ( Exception e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * Forces all changes to {@link PageCache} and creates a checkpoint so that the {@link TokenScanStore}
     * is recoverable from this point, given that the same transactions which will be applied after this point
     * and non-clean shutdown will be applied again on next startup.
     *
     * @param limiter {@link IOLimiter}.
     */
    @Override
    public void force( IOLimiter limiter, PageCursorTracer cursorTracer )
    {
        index.checkpoint( limiter, cursorTracer );
        writeMonitor.force();
    }

    @Override
    public AllEntriesTokenScanReader allEntityTokenRanges( PageCursorTracer cursorTracer )
    {
        return allEntityTokenRanges( 0, Long.MAX_VALUE, cursorTracer );
    }

    @Override
    public AllEntriesTokenScanReader allEntityTokenRanges( long fromEntityId, long toEntityId, PageCursorTracer cursorTracer )
    {
        IntFunction<Seeker<TokenScanKey,TokenScanValue>> seekProvider = tokenId ->
        {
            try
            {
                return index.seek(
                        new TokenScanKey().set( tokenId, fromEntityId / RANGE_SIZE ),
                        new TokenScanKey().set( tokenId, (toEntityId - 1) / RANGE_SIZE + 1 ), cursorTracer );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };

        int highestTokenId = -1;
        try ( Seeker<TokenScanKey,TokenScanValue> cursor = index.seek(
                new TokenScanKey().set( Integer.MAX_VALUE, Long.MAX_VALUE ),
                new TokenScanKey().set( 0, -1 ), cursorTracer ) )
        {
            if ( cursor.next() )
            {
                highestTokenId = cursor.key().tokenId;
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return new NativeAllEntriesTokenScanReader( seekProvider, highestTokenId, entityType );
    }

    @Override
    public ResourceIterator<Path> snapshotStoreFiles()
    {
        return Iterators.asResourceIterator( Iterators.iterator( storeFile ) );
    }

    @Override
    public EntityTokenUpdateListener updateListener()
    {
        return this;
    }

    /**
     * Instantiates the underlying {@link GBPTree} and its resources.
     *
     * @throws IOException on {@link PageCache} exceptions.
     */
    @Override
    public void init() throws IOException
    {
        monitor.init();

        boolean storeExists = hasStore();
        boolean isDirty;
        try
        {
            needsRebuild = !storeExists;
            if ( !storeExists )
            {
                monitor.noIndex();
            }

            isDirty = instantiateTree();
        }
        catch ( MetadataMismatchException e )
        {
            // GBPTree is corrupt. Try to rebuild.
            isDirty = true;
        }

        writeMonitor = config.get( GraphDatabaseInternalSettings.token_scan_write_log_enabled )
                       ? new TokenScanWriteMonitor( fs, directoryStructure, entityType(), config )
                       : NativeTokenScanWriter.EMPTY;
        singleWriter = new NativeTokenScanWriter( 1_000, writeMonitor );

        if ( isDirty )
        {
            monitor.notValidIndex();
            if ( !readOnly )
            {
                dropStrict();
                instantiateTree();
            }
            needsRebuild = true;
        }
    }

    private boolean hasStore()
    {
        return fs.fileExists( storeFile );
    }

    /**
     * @return true if instantiated tree needs to be rebuilt.
     */
    private boolean instantiateTree()
    {
        monitors.addMonitorListener( treeMonitor() );
        GBPTree.Monitor monitor = monitors.newMonitor( GBPTree.Monitor.class );
        MutableBoolean isRebuilding = new MutableBoolean();
        Header.Reader readRebuilding =
                headerData -> isRebuilding.setValue( headerData.get() == NEEDS_REBUILDING );
        try
        {
            index = new GBPTree<>( pageCache, storeFile, new TokenScanLayout(), monitor, readRebuilding,
                    needsRebuildingWriter, recoveryCleanupWorkCollector, readOnly, cacheTracer, immutable.empty(), tokenStoreName );
            return isRebuilding.getValue();
        }
        catch ( TreeFileNotFoundException e )
        {
            String token = entityType == EntityType.NODE ? "Label" : "Relationship type";
            throw new IllegalStateException(
                    token + " scan store file could not be found, most likely this database needs to be recovered, file:" + storeFile, e );
        }
    }

    private GBPTree.Monitor treeMonitor()
    {
        return new TokenIndexTreeMonitor();
    }

    @Override
    public void drop() throws IOException
    {
        try
        {
            dropStrict();
        }
        catch ( NoSuchFileException e )
        {
            // Even better, it didn't even exist
        }
    }

    private void dropStrict() throws IOException
    {
        if ( index != null )
        {
            index.close();
            index = null;
        }
        fs.deleteFileOrThrow( storeFile );
    }

    /**
     * Starts the store and makes it available for queries and updates.
     * Any required recovery must take place before calling this method.
     *
     * @throws IOException on {@link PageCache} exceptions.
     */
    @Override
    public void start() throws IOException
    {
        if ( needsRebuild && !readOnly )
        {
            monitor.rebuilding();
            long numberOfEntities;

            // Intentionally ignore read-only flag here when rebuilding.
            final PageCursorTracer cursorTracer = cacheTracer.createPageCursorTracer( TOKEN_SCAN_REBUILD_TAG );
            try ( TokenScanWriter writer = newBulkAppendWriter( cursorTracer ) )
            {
                numberOfEntities = fullStoreChangeStream.applyTo( writer, cursorTracer, memoryTracker );
            }

            index.checkpoint( IOLimiter.UNLIMITED, writeClean, cursorTracer );

            monitor.rebuilt( numberOfEntities );
            needsRebuild = false;
        }
    }

    private NativeTokenScanWriter writer( PageCursorTracer cursorTracer ) throws IOException
    {
        return singleWriter.initialize( index.writer( cursorTracer ) );
    }

    @Override
    public boolean isEmpty( PageCursorTracer cursorTracer ) throws IOException
    {
        try ( Seeker<TokenScanKey,TokenScanValue> cursor = index.seek(
                new TokenScanKey( 0, 0 ),
                new TokenScanKey( Integer.MAX_VALUE, Long.MAX_VALUE ), cursorTracer ) )
        {
            return !cursor.next();
        }
    }

    @Override
    public void stop()
    {   // Not needed
    }

    /**
     * Shuts down this store so that no more queries or updates can be accepted.
     *
     * @throws IOException on {@link PageCache} exceptions.
     */
    @Override
    public void shutdown() throws IOException
    {
        if ( index != null )
        {
            index.close();
            index = null;
            writeMonitor.close();
        }
    }

    @Override
    public boolean isReadOnly()
    {
        return readOnly;
    }

    @Override
    public boolean consistencyCheck( ReporterFactory reporterFactory, PageCursorTracer cursorTracer )
    {
        //noinspection unchecked
        return consistencyCheck( reporterFactory.getClass( GBPTreeConsistencyCheckVisitor.class ), cursorTracer );
    }

    private boolean consistencyCheck( GBPTreeConsistencyCheckVisitor<TokenScanKey> visitor, PageCursorTracer cursorTracer )
    {
        try
        {
            return index.consistencyCheck( visitor, cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private class TokenIndexTreeMonitor extends GBPTree.Monitor.Adaptor
    {
        @Override
        public void cleanupRegistered()
        {
            monitor.recoveryCleanupRegistered();
        }

        @Override
        public void cleanupStarted()
        {
            monitor.recoveryCleanupStarted();
        }

        @Override
        public void cleanupFinished( long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers, long durationMillis )
        {
            monitor.recoveryCleanupFinished( numberOfPagesVisited, numberOfTreeNodes, numberOfCleanedCrashPointers, durationMillis );
        }

        @Override
        public void cleanupClosed()
        {
            monitor.recoveryCleanupClosed();
        }

        @Override
        public void cleanupFailed( Throwable throwable )
        {
            monitor.recoveryCleanupFailed( throwable );
        }
    }
}
