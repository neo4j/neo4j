/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.File;
import java.io.IOException;

import org.neo4j.cursor.RawCursor;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.Hit;
import org.neo4j.index.gbptree.GBPTree;
import org.neo4j.index.gbptree.Layout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.index.gbptree.GBPTree.NO_MONITOR;
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
     * {@link PageCache} to {@link PageCache#map(File, int, java.nio.file.OpenOption...)}
     * store file backing this label scan store. Passed to {@link GBPTree}.
     */
    private final PageCache pageCache;

    /**
     * Store file {@link PageCache#map(File, int, java.nio.file.OpenOption...)}.
     */
    private final File storeFile;

    /**
     * Size of each node id range, e.g. 8, 16, 32 or 64. This value is written at creation,
     * otherwise verified against index meta data on open.
     */
    private final int rangeSize;

    /**
     * Used in {@link #start()} if the store is empty, where this will provide all data for fully populating
     * this label scan store. This can be the case when changing label scan store provider on an existing database.
     */
    private final FullStoreChangeStream fullStoreChangeStream;

    /**
     * Page size to use for each tree node in {@link GBPTree}. Passed to {@link GBPTree}.
     */
    private final int pageSize;

    /**
     * The index which backs this label scan store. Instantiated in {@link #init()} and considered
     * started after call to {@link #start()}.
     */
    private GBPTree<LabelScanKey,LabelScanValue> index;

    /**
     * Whether or not {@link #start()} has been called.
     */
    private boolean started;

    /**
     * If {@link #index} is {@code null} and {@link #started} is {@code false},
     * then it's between {@link #init()} and {@link #start()}. If {@link #newWriter()} is called at this
     * point we infer that recovery is taking place and so we notify the {@link GBPTree} about that fact.
     */
    private boolean recoveryStarted;

    private final NativeLabelScanWriter singleWriter;

    public NativeLabelScanStore( PageCache pageCache, File storeDir, int rangeSize,
            FullStoreChangeStream fullStoreChangeStream )
    {
        this( pageCache, storeDir, rangeSize, fullStoreChangeStream, 0/*means no opinion about page size*/ );
    }

    /*
     * Test access to be able to control page size.
     */
    NativeLabelScanStore( PageCache pageCache, File storeDir, int rangeSize,
            FullStoreChangeStream fullStoreChangeStream, int pageSize )
    {
        this.pageCache = pageCache;
        this.pageSize = pageSize;
        this.fullStoreChangeStream = fullStoreChangeStream;
        this.storeFile = new File( storeDir, DEFAULT_NAME + ".labelscanstore.db" );
        this.rangeSize = rangeSize;
        this.singleWriter = new NativeLabelScanWriter( rangeSize, 1_000 );
    }

    /**
     * @return {@link LabelScanReader} capable of finding node ids with given label ids.
     * Readers will immediately see updates made by {@link LabelScanWriter}, although {@link LabelScanWriter}
     * may internally batch updates so functionality isn't realiable. The only given is that readers will
     * see at least updates from closed {@link LabelScanWriter writers}.
     */
    @Override
    public LabelScanReader newReader()
    {
        return new NativeLabelScanReader( index, rangeSize );
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
        try
        {
            if ( !started && !recoveryStarted )
            {
                // Let's notify our index that recovery is about to commence, we do this once before
                // the first recovered transaction gets applied.
                index.prepareForRecovery();
                recoveryStarted = true;
            }

            return singleWriter.initialize( index.writer() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
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
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * Unsupported by this implementation.
     *
     * @return nothing since {@link UnsupportedOperationException} will be thrown.
     * @throws UnsupportedOperationException since not supported by this implementation.
     */
    @Override
    public AllEntriesLabelScanReader allNodeLabelRanges()
    {
        throw new UnsupportedOperationException( "Not supported by " + getClass() );
    }

    /**
     * @return store files, namely the single "neostore.labelscanstore.db" store file.
     * @throws IOException on file access exceptions.
     */
    @Override
    public ResourceIterator<File> snapshotStoreFiles() throws IOException
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
        index = new GBPTree<>( pageCache, storeFile, new LabelScanLayout( rangeSize ), pageSize, NO_MONITOR );
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
        if ( isEmpty() )
        {
            LabelScanStoreProvider.rebuild( this, fullStoreChangeStream );
        }
        started = true;
    }

    private boolean isEmpty() throws IOException
    {
        try ( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor = index.seek(
                new LabelScanKey().set( 0, 0 ),
                new LabelScanKey().set( Integer.MAX_VALUE, Long.MAX_VALUE ) ) )
        {
            return !cursor.next();
        }
    }

    @Override
    public void stop() throws IOException
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
        index.close();
    }
}
