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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyCheckVisitor;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.annotations.ReporterFactory;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;

abstract class NativeIndex<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> implements ConsistencyCheckable
{
    final PageCache pageCache;
    final File storeFile;
    final IndexLayout<KEY,VALUE> layout;
    final FileSystemAbstraction fileSystem;
    final StoreIndexDescriptor descriptor;
    private final IndexProvider.Monitor monitor;
    private final boolean readOnly;

    protected GBPTree<KEY,VALUE> tree;

    NativeIndex( PageCache pageCache, FileSystemAbstraction fs, File storeFile, IndexLayout<KEY,VALUE> layout, IndexProvider.Monitor monitor,
            StoreIndexDescriptor descriptor, boolean readOnly )
    {
        this.pageCache = pageCache;
        this.storeFile = storeFile;
        this.layout = layout;
        this.fileSystem = fs;
        this.descriptor = descriptor;
        this.monitor = monitor;
        this.readOnly = readOnly;
    }

    void instantiateTree( RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, Consumer<PageCursor> headerWriter )
    {
        ensureDirectoryExist();
        GBPTree.Monitor monitor = treeMonitor();
        tree = new GBPTree<>( pageCache, storeFile, layout, 0, monitor, NO_HEADER_READER, headerWriter, recoveryCleanupWorkCollector, readOnly );
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
        try
        {
            fileSystem.mkdirs( storeFile.getParentFile() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
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
            return tree.consistencyCheck( visitor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private class NativeIndexTreeMonitor extends GBPTree.Monitor.Adaptor
    {
        @Override
        public void cleanupRegistered()
        {
            monitor.recoveryCleanupRegistered( storeFile, descriptor );
        }

        @Override
        public void cleanupStarted()
        {
            monitor.recoveryCleanupStarted( storeFile, descriptor );
        }

        @Override
        public void cleanupFinished( long numberOfPagesVisited, long numberOfCleanedCrashPointers, long durationMillis )
        {
            monitor.recoveryCleanupFinished( storeFile, descriptor, numberOfPagesVisited, numberOfCleanedCrashPointers, durationMillis );
        }

        @Override
        public void cleanupClosed()
        {
            monitor.recoveryCleanupClosed( storeFile, descriptor );
        }

        @Override
        public void cleanupFailed( Throwable throwable )
        {
            monitor.recoveryCleanupFailed( storeFile, descriptor, throwable );
        }
    }
}
