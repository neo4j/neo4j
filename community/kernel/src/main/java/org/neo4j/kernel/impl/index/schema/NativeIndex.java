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
import java.util.function.Consumer;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyCheckVisitor;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.index.IndexProvider;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

abstract class NativeIndex<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> implements ConsistencyCheckable
{
    final PageCache pageCache;
    final IndexFiles indexFiles;
    final IndexLayout<KEY,VALUE> layout;
    final FileSystemAbstraction fileSystem;
    final IndexDescriptor descriptor;
    private final IndexProvider.Monitor monitor;
    private final GBPTree.Monitor treeMonitor;
    private final boolean readOnly;

    protected GBPTree<KEY,VALUE> tree;

    NativeIndex( DatabaseIndexContext databaseIndexContext, IndexLayout<KEY,VALUE> layout, IndexFiles indexFiles, IndexDescriptor descriptor,
            GBPTree.Monitor treeMonitor )
    {
        this.pageCache = databaseIndexContext.pageCache;
        this.fileSystem = databaseIndexContext.fileSystem;
        this.monitor = databaseIndexContext.monitor;
        this.readOnly = databaseIndexContext.readOnly;
        this.indexFiles = indexFiles;
        this.layout = layout;
        this.descriptor = descriptor;
        this.treeMonitor = treeMonitor;
    }

    void instantiateTree( RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, Consumer<PageCursor> headerWriter )
    {
        ensureDirectoryExist();
        GBPTree.Monitor monitor = treeMonitor();
        File storeFile = indexFiles.getStoreFile();
        tree = new GBPTree<>( pageCache, storeFile, layout, 0, monitor, NO_HEADER_READER, headerWriter, recoveryCleanupWorkCollector,
                readOnly, NULL );
        afterTreeInstantiation( tree );
    }

    protected void afterTreeInstantiation( GBPTree<KEY,VALUE> tree )
    {   // no-op per default
    }

    private GBPTree.Monitor treeMonitor( )
    {
        return new NativeIndexTreeMonitor();
    }

    private void ensureDirectoryExist()
    {
        indexFiles.ensureDirectoryExist();
    }

    void closeTree()
    {
        IOUtils.closeAllUnchecked( tree );
        tree = null;
    }

    void assertOpen()
    {
        if ( tree == null )
        {
            throw new IllegalStateException( "Index has been closed" );
        }
    }

    @Override
    public boolean consistencyCheck( ReporterFactory reporterFactory )
    {
        return consistencyCheck( reporterFactory.getClass( GBPTreeConsistencyCheckVisitor.class ) );
    }

    private boolean consistencyCheck( GBPTreeConsistencyCheckVisitor<KEY> visitor )
    {
        try
        {
            return tree.consistencyCheck( visitor, TRACER_SUPPLIER.get() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private class NativeIndexTreeMonitor extends GBPTree.Monitor.Delegate
    {
        NativeIndexTreeMonitor()
        {
            super( treeMonitor );
        }

        @Override
        public void cleanupRegistered()
        {
            monitor.recoveryCleanupRegistered( indexFiles.getStoreFile(), descriptor );
            super.cleanupRegistered();
        }

        @Override
        public void cleanupStarted()
        {
            monitor.recoveryCleanupStarted( indexFiles.getStoreFile(), descriptor );
            super.cleanupStarted();
        }

        @Override
        public void cleanupFinished( long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers, long durationMillis )
        {
            monitor.recoveryCleanupFinished( indexFiles.getStoreFile(), descriptor,
                    numberOfPagesVisited, numberOfTreeNodes, numberOfCleanedCrashPointers, durationMillis );
            super.cleanupFinished( numberOfPagesVisited, numberOfTreeNodes, numberOfCleanedCrashPointers, durationMillis );
        }

        @Override
        public void cleanupClosed()
        {
            monitor.recoveryCleanupClosed( indexFiles.getStoreFile(), descriptor );
            super.cleanupClosed();
        }

        @Override
        public void cleanupFailed( Throwable throwable )
        {
            monitor.recoveryCleanupFailed( indexFiles.getStoreFile(), descriptor, throwable );
            super.cleanupFailed( throwable );
        }
    }
}
