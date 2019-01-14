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
package org.neo4j.kernel.impl.index.labelscan;

import org.apache.commons.lang3.mutable.MutableBoolean;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import org.neo4j.cursor.RawCursor;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Header;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.impl.index.GBPTreeFileUtil;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;

/**
 * {@link LabelScanStore} which is implemented using {@link GBPTree} atop a {@link PageCache}.
 * Only a single writer is allowed at any given point in time so synchronization or merging of updates
 * need to be handled externally.
 * <p>
 * About the {@link Layout} used in this instance of {@link GBPTree}:
 * <ul>
 * <li>
 * Each keys is a combination of {@code labelId} and {@code nodeIdRange} ({@code nodeId/64}).
 * </li>
 * <li>
 * Each value is a 64-bit bit set (a primitive {@code long}) where each set bit in it represents
 * a node with that label, such that {@code nodeId = nodeIdRange+bitOffset}. Range size (e.g. 64 bits)
 * is configurable on initial creation of the store, 8, 16, 32 or 64.
 * </li>
 * </ul>
 * <p>
 * {@link #force(IOLimiter)} is vital for allowing this store to be recoverable, and must be called
 * whenever Neo4j performs a checkpoint.
 * <p>
 * This store is backed by a single store file "neostore.labelscanstore.db".
 */
public class NativeLabelScanStore implements LabelScanStore
{
    /**
     * Name of the file used for the native label scan store.
     */
    public static final String FILE_NAME = DEFAULT_NAME + ".labelscanstore.db";

    /**
     * Written in header to indicate native label scan store is clean
     */
    private static final byte CLEAN = (byte) 0x00;

    /**
     * Written in header to indicate native label scan store is rebuilding
     */
    private static final byte NEEDS_REBUILDING = (byte) 0x01;

    /**
     * Whether or not this label scan store is read-only.
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
     * {@link PageCache} to {@link PageCache#map(File, int, java.nio.file.OpenOption...)}
     * store file backing this label scan store. Passed to {@link GBPTree}.
     */
    private final PageCache pageCache;

    /**
     * Store file {@link PageCache#map(File, int, java.nio.file.OpenOption...)}.
     */
    private final File storeFile;

    /**
     * Used in {@link #start()} if the store is empty, where this will provide all data for fully populating
     * this label scan store. This can be the case when changing label scan store provider on an existing database.
     */
    private final FullStoreChangeStream fullStoreChangeStream;

    /**
     * {@link FileSystemAbstraction} the backing file lives on.
     */
    private final FileSystemAbstraction fs;

    /**
     * Directory of the store to place the backing file in.
     */
    private final File storeDir;

    /**
     * Page size to use for each tree node in {@link GBPTree}. Passed to {@link GBPTree}.
     */
    private final int pageSize;

    /**
     * Used for all file operations on the gbpTree file.
     */
    private final GBPTreeFileUtil gbpTreeUtil;

    /**
     * The index which backs this label scan store. Instantiated in {@link #init()} and considered
     * started after call to {@link #start()}.
     */
    private GBPTree<LabelScanKey,LabelScanValue> index;

    /**
     * Set during {@link #init()} if {@link #start()} will need to rebuild the whole label scan store from
     * {@link FullStoreChangeStream}.
     */
    private boolean needsRebuild;

    /**
     * Passed to underlying {@link GBPTree} which use it to submit recovery cleanup jobs.
     */
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;

    /**
     * The single instance of {@link NativeLabelScanWriter} used for updates.
     */
    private NativeLabelScanWriter singleWriter;

    /**
     * Monitor for all writes going into this label scan store.
     */
    private NativeLabelScanWriter.WriteMonitor writeMonitor;

    /**
     * Write rebuilding bit to header.
     */
    private static final Consumer<PageCursor> needsRebuildingWriter =
            pageCursor -> pageCursor.putByte( NEEDS_REBUILDING );

    /**
     * Write clean header.
     */
    private static final Consumer<PageCursor> writeClean = pageCursor -> pageCursor.putByte( CLEAN );

    public NativeLabelScanStore( PageCache pageCache, FileSystemAbstraction fs, File storeDir, FullStoreChangeStream fullStoreChangeStream,
            boolean readOnly, Monitors monitors, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        this( pageCache, fs, storeDir, fullStoreChangeStream, readOnly, monitors, recoveryCleanupWorkCollector,
                /*means no opinion about page size*/ 0 );
    }

    /*
     * Test access to be able to control page size.
     */
    NativeLabelScanStore( PageCache pageCache, FileSystemAbstraction fs, File storeDir,
                FullStoreChangeStream fullStoreChangeStream, boolean readOnly, Monitors monitors,
                RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, int pageSize )
    {
        this.pageCache = pageCache;
        this.fs = fs;
        this.pageSize = pageSize;
        this.fullStoreChangeStream = fullStoreChangeStream;
        this.storeDir = storeDir;
        this.storeFile = getLabelScanStoreFile( storeDir );
        this.readOnly = readOnly;
        this.monitors = monitors;
        this.monitor = monitors.newMonitor( Monitor.class );
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.gbpTreeUtil = new GBPTreePageCacheFileUtil( pageCache );
    }

    /**
     * Returns the file backing the label scan store.
     *
     * @param storeDir The store directory to use.
     * @return the file backing the label scan store
     */
    public static File getLabelScanStoreFile( File storeDir )
    {
        return new File( storeDir, FILE_NAME );
    }

    /**
     * @return {@link LabelScanReader} capable of finding node ids with given label ids.
     * Readers will immediately see updates made by {@link LabelScanWriter}, although {@link LabelScanWriter}
     * may internally batch updates so functionality isn't reliable. The only given is that readers will
     * see at least updates from closed {@link LabelScanWriter writers}.
     */
    @Override
    public LabelScanReader newReader()
    {
        return new NativeLabelScanReader( index );
    }

    /**
     * Returns {@link LabelScanWriter} capable of making changes to this {@link LabelScanStore}.
     * Only a single writer is allowed at any given point in time.
     *
     * @return {@link LabelScanWriter} capable of making changes to this {@link LabelScanStore}.
     * @throws IllegalStateException if someone else has already acquired a writer and hasn't yet
     * called {@link LabelScanWriter#close()}.
     */
    @Override
    public LabelScanWriter newWriter()
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Can't create index writer in read only mode." );
        }

        try
        {
            return writer();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    /**
     * Forces all changes to {@link PageCache} and creates a checkpoint so that the {@link LabelScanStore}
     * is recoverable from this point, given that the same transactions which will be applied after this point
     * and non-clean shutdown will be applied again on next startup.
     *
     * @param limiter {@link IOLimiter}.
     * @throws UnderlyingStorageException on failure writing changes to {@link PageCache}.
     */
    @Override
    public void force( IOLimiter limiter ) throws UnderlyingStorageException
    {
        try
        {
            index.checkpoint( limiter );
            writeMonitor.force();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public AllEntriesLabelScanReader allNodeLabelRanges()
    {
        IntFunction<RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>> seekProvider = labelId ->
        {
            try
            {
                return index.seek(
                        new LabelScanKey().set( labelId, 0 ),
                        new LabelScanKey().set( labelId, Long.MAX_VALUE ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };

        int highestLabelId = -1;
        try ( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor = index.seek(
                new LabelScanKey().set( Integer.MAX_VALUE, Long.MAX_VALUE ),
                new LabelScanKey().set( 0, -1 ) ) )
        {
            if ( cursor.next() )
            {
                highestLabelId = cursor.get().key().labelId;
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return new NativeAllEntriesLabelScanReader( seekProvider, highestLabelId );
    }

    /**
     * @return store files, namely the single "neostore.labelscanstore.db" store file.
     */
    @Override
    public ResourceIterator<File> snapshotStoreFiles()
    {
        return asResourceIterator( iterator( storeFile ) );
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

        writeMonitor = LabelScanWriteMonitor.ENABLED ? new LabelScanWriteMonitor( fs, storeDir ) : NativeLabelScanWriter.EMPTY;
        singleWriter = new NativeLabelScanWriter( 1_000, writeMonitor );

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

    @Override
    public boolean hasStore()
    {
        return gbpTreeUtil.storeFileExists( storeFile );
    }

    @Override
    public File getLabelScanStoreFile()
    {
        return storeFile;
    }

    /**
     * @return true if instantiated tree needs to be rebuilt.
     */
    private boolean instantiateTree() throws IOException
    {
        monitors.addMonitorListener( treeMonitor() );
        GBPTree.Monitor monitor = monitors.newMonitor( GBPTree.Monitor.class );
        MutableBoolean isRebuilding = new MutableBoolean();
        Header.Reader readRebuilding =
                headerData -> isRebuilding.setValue( headerData.get() == NEEDS_REBUILDING );
        index = new GBPTree<>( pageCache, storeFile, new LabelScanLayout(), pageSize, monitor, readRebuilding,
                needsRebuildingWriter, recoveryCleanupWorkCollector );
        return isRebuilding.getValue();
    }

    private GBPTree.Monitor treeMonitor()
    {
        return new LabelIndexTreeMonitor();
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
        gbpTreeUtil.deleteFile( storeFile );
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
            long numberOfNodes;

            // Intentionally ignore read-only flag here when rebuilding.
            try ( LabelScanWriter writer = writer() )
            {
                numberOfNodes = fullStoreChangeStream.applyTo( writer );
            }

            index.checkpoint( IOLimiter.unlimited(), writeClean );

            monitor.rebuilt( numberOfNodes );
            needsRebuild = false;
        }
    }

    private NativeLabelScanWriter writer() throws IOException
    {
        return singleWriter.initialize( index.writer() );
    }

    @Override
    public boolean isEmpty() throws IOException
    {
        try ( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor = index.seek(
                new LabelScanKey( 0, 0 ),
                new LabelScanKey( Integer.MAX_VALUE, Long.MAX_VALUE ) ) )
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

    public boolean isDirty()
    {
        return index == null || index.wasDirtyOnStartup();
    }

    private class LabelIndexTreeMonitor extends GBPTree.Monitor.Adaptor
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
        public void cleanupFinished( long numberOfPagesVisited, long numberOfCleanedCrashPointers, long durationMillis )
        {
            monitor.recoveryCleanupFinished( numberOfPagesVisited, numberOfCleanedCrashPointers, durationMillis );
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
