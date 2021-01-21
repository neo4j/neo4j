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

import org.eclipse.collections.api.set.ImmutableSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.function.Consumer;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyCheckVisitor;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.util.Preconditions;

import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;

public class TokenIndexPopulator implements IndexPopulator, ConsistencyCheckable
{
    /**
     * Written in header to indicate native token index is clean
     *
     * NOTE that this is not the same byte as the other indexes use,
     * to be able to handle the switch from old scan stores without rebuilding.
     */
    private static final byte CLEAN = (byte) 0x00;
    static final byte ONLINE = CLEAN;

    /**
     * Written in header to indicate native token index is/needs rebuilding
     *
     * NOTE that this is not the same byte as the other indexes use,
     * to be able to handle the switch from old scan stores without rebuilding.
     */
    private static final byte NEEDS_REBUILDING = (byte) 0x01;
    static final byte POPULATING = NEEDS_REBUILDING;

    /**
     * Written in header to indicate native token index failed to population.
     *
     * NOTE that this is not the same byte as the other indexes use,
     * to be able to handle the switch from old scan stores without rebuilding.
     */
    static final byte FAILED = (byte) 0x02;

    /**
     * The type of entity this token index is backing.
     */
    private final EntityType entityType;

    /**
     * Whether or not this token index is read-only.
     */
    private final boolean readOnly;

    /**
     * Monitors used to pass down monitor to underlying {@link GBPTree}
     */
    private final Monitors monitors;

    /**
     * Tag to use when creating new monitors.
     * We need this because there could be multiple
     * {@link IndexProvider.Monitor listeners} registered
     * of the same type.
     */
    private final String monitorTag;

    /**
     * {@link PageCache} to {@link PageCache#map(Path, int, ImmutableSet)}
     * store file backing this token scan store. Passed to {@link GBPTree}.
     */
    private final PageCache pageCache;

    /**
     * IndexFiles wrapping the store file {@link PageCache#map(Path, int, ImmutableSet)}.
     */
    private final IndexFiles indexFiles;

    /**
     * {@link FileSystemAbstraction} the backing file lives on.
     */
    private final FileSystemAbstraction fs;

    /**
     * Layout of the database.
     */
    private final DatabaseLayout directoryStructure;
    private final Config config;
    private final PageCacheTracer cacheTracer;

    /**
     * The actual index which backs this token index.
     */
    private GBPTree<TokenScanKey,TokenScanValue> index;

    /**
     * The single instance of {@link TokenIndexUpdater} used for updates.
     */
    private TokenIndexUpdater singleUpdater;

    /**
     * Monitor for all writes going into this token index.
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
            pageCursor -> pageCursor.putByte( POPULATING );

    private byte[] failureBytes;
    private boolean dropped;
    private boolean closed;

    TokenIndexPopulator( PageCache pageCache, DatabaseLayout directoryStructure, IndexFiles indexFiles, FileSystemAbstraction fs,
            boolean readOnly, Config config, Monitors monitors, String monitorTag, EntityType entityType, PageCacheTracer cacheTracer,
            String tokenStoreName )
    {
        this.pageCache = pageCache;
        this.indexFiles = indexFiles;
        this.fs = fs;
        this.directoryStructure = directoryStructure;
        this.config = config;
        this.monitorTag = monitorTag;
        this.cacheTracer = cacheTracer;
        this.readOnly = readOnly;
        this.monitors = monitors;
        this.entityType = entityType;
        this.tokenStoreName = tokenStoreName;
    }

    private void instantiateTree( RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, Consumer<PageCursor> headerWriter )
    {
        GBPTree.Monitor monitor = treeMonitor();
        index = new GBPTree<>( pageCache, indexFiles.getStoreFile(), new TokenScanLayout(), monitor, NO_HEADER_READER,
                headerWriter, recoveryCleanupWorkCollector, readOnly, cacheTracer, immutable.empty(), tokenStoreName );
    }

    private GBPTree.Monitor treeMonitor()
    {
        GBPTree.Monitor treeMonitor = monitors.newMonitor( GBPTree.Monitor.class, monitorTag );
        IndexProvider.Monitor indexMonitor = monitors.newMonitor( IndexProvider.Monitor.class, monitorTag );
        return new IndexMonitorAdaptor( treeMonitor, indexMonitor, indexFiles, null );
    }

    @Override
    public synchronized void create()
    {
        assertNotDropped();
        assertNotClosed();

        indexFiles.clear();
        instantiateTree( RecoveryCleanupWorkCollector.immediate(), needsRebuildingWriter );

        writeMonitor = config.get( GraphDatabaseInternalSettings.token_scan_write_log_enabled )
                       ? new TokenScanWriteMonitor( fs, directoryStructure, entityType, config )
                       : NativeTokenScanWriter.EMPTY;
        singleUpdater = new TokenIndexUpdater( 1_000, writeMonitor );
    }

    @Override
    public synchronized void drop()
    {
        try
        {
            if ( index != null )
            {
                index.setDeleteOnClose( true );
            }
            closeTree();
            indexFiles.clear();
        }
        finally
        {
            dropped = true;
            closed = true;
        }
    }

    private void closeTree()
    {
        IOUtils.closeAllUnchecked( index );
        index = null;
        if ( writeMonitor != null )
        {
            writeMonitor.close();
        }
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates, PageCursorTracer cursorTracer ) throws IndexEntryConflictException
    {
        try ( TokenIndexUpdater updater = singleUpdater.initialize( index.writer( cursorTracer ) ) )
        {
            for ( IndexEntryUpdate<?> update : updates )
            {
                updater.process( update );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor )
    {
        // No-op, token indexes don't have any uniqueness constraints.
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor, PageCursorTracer cursorTracer )
    {
        try
        {
            return singleUpdater.initialize( index.writer( cursorTracer ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public synchronized void close( boolean populationCompletedSuccessfully, PageCursorTracer cursorTracer )
    {
        Preconditions.checkState( !(populationCompletedSuccessfully && failureBytes != null),
                "Can't mark index as online after it has been marked as failure" );

        try
        {
            assertNotDropped();
            if ( populationCompletedSuccessfully )
            {
                // Successful and completed population
                assertPopulatorOpen();
                flushTreeAndMarkAs( ONLINE, cursorTracer );
            }
            else if ( failureBytes != null )
            {
                // Failed population
                ensureTreeInstantiated();
                markTreeAsFailed( cursorTracer );
            }
            // else cancelled population. Here we simply close the tree w/o checkpointing it and it will look like POPULATING state on next open
        }
        finally
        {
            closeTree();
            closed = true;
        }
    }

    private void flushTreeAndMarkAs( byte state, PageCursorTracer cursorTracer )
    {
        index.checkpoint( IOLimiter.UNLIMITED, pageCursor -> pageCursor.putByte( state ), cursorTracer );
    }

    private void markTreeAsFailed( PageCursorTracer cursorTracer )
    {
        Preconditions.checkState( failureBytes != null, "markAsFailed hasn't been called, populator not actually failed?" );
        index.checkpoint( IOLimiter.UNLIMITED, new FailureHeaderWriter( failureBytes, FAILED ), cursorTracer );
    }

    @Override
    public void markAsFailed( String failure )
    {
        failureBytes = failure.getBytes( StandardCharsets.UTF_8 );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        // We don't do sampling for token indexes since that information is available in other ways.
    }

    @Override
    public IndexSample sample( PageCursorTracer cursorTracer )
    {
        throw new UnsupportedOperationException( "Token indexes does not support index sampling" );
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

    private void assertNotDropped()
    {
        Preconditions.checkState( !dropped, "Populator has already been dropped." );
    }

    private void assertNotClosed()
    {
        Preconditions.checkState( !closed, "Populator has already been closed." );
    }

    private void ensureTreeInstantiated()
    {
        if ( index == null )
        {
            instantiateTree( RecoveryCleanupWorkCollector.ignore(), NO_HEADER_WRITER );
        }
    }

    private void assertPopulatorOpen()
    {
        Preconditions.checkState( index != null, "Populator has already been closed." );
    }
}
