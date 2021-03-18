/*
 * Copyright (c) "Neo4j"
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
import java.nio.file.Path;
import java.util.function.Consumer;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyCheckVisitor;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.util.Preconditions;

import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;

public class TokenIndex implements ConsistencyCheckable
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
     * {@link PageCache} to {@link PageCache#map(Path, int, String, ImmutableSet)}
     * store file backing this token scan store. Passed to {@link GBPTree}.
     */
    private final PageCache pageCache;

    /**
     * IndexFiles wrapping the store file {@link PageCache#map(Path, int, String, ImmutableSet)}.
     */
    final IndexFiles indexFiles;

    /**
     * {@link FileSystemAbstraction} the backing file lives on.
     */
    final FileSystemAbstraction fs;

    private final PageCacheTracer cacheTracer;

    private final String databaseName;
    /**
     * The actual index which backs this token index.
     */
    GBPTree<TokenScanKey,TokenScanValue> index;

    /**
     * The single instance of {@link TokenIndexUpdater} used for updates.
     */
    TokenIndexUpdater singleUpdater;

    /**
     * Monitor for all writes going into this token index.
     */
    NativeTokenScanWriter.WriteMonitor writeMonitor;

    /**
     * Name of the store that will be used when describing work related to this store.
     */
    private final String tokenStoreName;

    public TokenIndex( DatabaseIndexContext databaseIndexContext, IndexFiles indexFiles, IndexDescriptor descriptor )
    {
        this.readOnly = databaseIndexContext.readOnly;
        this.monitors = databaseIndexContext.monitors;
        this.monitorTag = databaseIndexContext.monitorTag;
        this.pageCache = databaseIndexContext.pageCache;
        this.fs = databaseIndexContext.fileSystem;
        this.cacheTracer = databaseIndexContext.pageCacheTracer;
        this.databaseName = databaseIndexContext.databaseName;
        this.indexFiles = indexFiles;
        this.tokenStoreName = descriptor.getName();
    }

    void instantiateTree( RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, Consumer<PageCursor> headerWriter )
    {
        GBPTree.Monitor monitor = treeMonitor();
        index = new GBPTree<>( pageCache, indexFiles.getStoreFile(), new TokenScanLayout(), monitor, NO_HEADER_READER,
                headerWriter, recoveryCleanupWorkCollector, readOnly, cacheTracer, immutable.empty(), databaseName, tokenStoreName );
    }

    void instantiateUpdater( Config config, DatabaseLayout directoryStructure, EntityType entityType )
    {
        writeMonitor = config.get( GraphDatabaseInternalSettings.token_scan_write_log_enabled )
                       ? new TokenScanWriteMonitor( fs, directoryStructure, entityType, config )
                       : NativeTokenScanWriter.EMPTY;
        singleUpdater = new TokenIndexUpdater( 1_000, writeMonitor );
    }

    private GBPTree.Monitor treeMonitor()
    {
        GBPTree.Monitor treeMonitor = monitors.newMonitor( GBPTree.Monitor.class, monitorTag );
        IndexProvider.Monitor indexMonitor = monitors.newMonitor( IndexProvider.Monitor.class, monitorTag );
        return new IndexMonitorAdaptor( treeMonitor, indexMonitor, indexFiles, null );
    }

    void closeResources()
    {
        IOUtils.closeAllUnchecked( index, writeMonitor );
        index = null;
        writeMonitor = null;
    }

    void assertTreeOpen()
    {
        Preconditions.checkState( index != null, "Index tree has been closed or was never instantiated." );
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
}
