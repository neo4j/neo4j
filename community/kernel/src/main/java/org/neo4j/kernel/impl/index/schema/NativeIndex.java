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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
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
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.monitoring.Monitors;

import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

abstract class NativeIndex<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> implements ConsistencyCheckable
{
    final PageCache pageCache;
    final IndexFiles indexFiles;
    final IndexLayout<KEY,VALUE> layout;
    final FileSystemAbstraction fileSystem;
    final IndexDescriptor descriptor;
    private final Monitors monitors;
    private final String monitorTag;
    private final boolean readOnly;

    protected GBPTree<KEY,VALUE> tree;

    NativeIndex( DatabaseIndexContext databaseIndexContext, IndexLayout<KEY,VALUE> layout, IndexFiles indexFiles, IndexDescriptor descriptor )
    {
        this.pageCache = databaseIndexContext.pageCache;
        this.fileSystem = databaseIndexContext.fileSystem;
        this.monitors = databaseIndexContext.monitors;
        this.monitorTag = databaseIndexContext.monitorTag;
        this.readOnly = databaseIndexContext.readOnly;
        this.indexFiles = indexFiles;
        this.layout = layout;
        this.descriptor = descriptor;
    }

    void instantiateTree( RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, Consumer<PageCursor> headerWriter )
    {
        ensureDirectoryExist();
        GBPTree.Monitor monitor = treeMonitor();
        Path storeFile = indexFiles.getStoreFile();
        tree = new GBPTree<>( pageCache, storeFile, layout, monitor, NO_HEADER_READER, headerWriter, recoveryCleanupWorkCollector,
                readOnly, NULL, immutable.empty(), descriptor.getName() );
        afterTreeInstantiation( tree );
    }

    protected void afterTreeInstantiation( GBPTree<KEY,VALUE> tree )
    {   // no-op per default
    }

    private GBPTree.Monitor treeMonitor()
    {
        GBPTree.Monitor treeMonitor = monitors.newMonitor( GBPTree.Monitor.class, monitorTag );
        IndexProvider.Monitor indexMonitor = monitors.newMonitor( IndexProvider.Monitor.class, monitorTag );
        return new IndexMonitorAdaptor( treeMonitor, indexMonitor, indexFiles, descriptor );
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
    public boolean consistencyCheck( ReporterFactory reporterFactory, PageCursorTracer cursorTracer )
    {
        return consistencyCheck( reporterFactory.getClass( GBPTreeConsistencyCheckVisitor.class ), cursorTracer );
    }

    private boolean consistencyCheck( GBPTreeConsistencyCheckVisitor<KEY> visitor, PageCursorTracer cursorTracer )
    {
        try
        {
            return tree.consistencyCheck( visitor, cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
