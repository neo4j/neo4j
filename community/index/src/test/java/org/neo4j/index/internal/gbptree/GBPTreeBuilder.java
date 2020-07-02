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
package org.neo4j.index.internal.gbptree;

import org.eclipse.collections.api.set.ImmutableSet;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

/**
 * Convenient builder for a {@link GBPTree}. Either created using zero-argument constructor for maximum
 * flexibility, or with constructor with arguments considered mandatory to be able to build a proper tree.
 *
 * @param <KEY> type of key in {@link GBPTree}
 * @param <VALUE> type of value in {@link GBPTree}
 */
public class GBPTreeBuilder<KEY,VALUE>
{
    private PageCache pageCache;
    private Path path;
    private Monitor monitor = NO_MONITOR;
    private Header.Reader headerReader = NO_HEADER_READER;
    private Layout<KEY,VALUE> layout;
    private Consumer<PageCursor> headerWriter = NO_HEADER_WRITER;
    private RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.immediate();
    private boolean readOnly;
    private PageCacheTracer pageCacheTracer = NULL;
    private ImmutableSet<OpenOption> openOptions = immutable.empty();

    public GBPTreeBuilder( PageCache pageCache, Path path, Layout<KEY,VALUE> layout )
    {
        with( pageCache );
        with( path );
        with( layout );
    }

    public GBPTreeBuilder<KEY,VALUE> with( Layout<KEY,VALUE> layout )
    {
        this.layout = layout;
        return this;
    }

    public GBPTreeBuilder<KEY,VALUE> with( Path file )
    {
        this.path = file;
        return this;
    }

    public GBPTreeBuilder<KEY,VALUE> with( PageCache pageCache )
    {
        this.pageCache = pageCache;
        return this;
    }

    public GBPTreeBuilder<KEY,VALUE> with( GBPTree.Monitor monitor )
    {
        this.monitor = monitor;
        return this;
    }

    public GBPTreeBuilder<KEY,VALUE> with( Header.Reader headerReader )
    {
        this.headerReader = headerReader;
        return this;
    }

    public GBPTreeBuilder<KEY,VALUE> with( Consumer<PageCursor> headerWriter )
    {
        this.headerWriter = headerWriter;
        return this;
    }

    public GBPTreeBuilder<KEY,VALUE> with( RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        return this;
    }

    public GBPTreeBuilder<KEY,VALUE> withReadOnly( boolean readOnly )
    {
        this.readOnly = readOnly;
        return this;
    }

    public GBPTreeBuilder<KEY,VALUE> with( PageCacheTracer pageCacheTracer )
    {
        this.pageCacheTracer = pageCacheTracer;
        return this;
    }

    public GBPTreeBuilder<KEY,VALUE> with( ImmutableSet<OpenOption> openOptions )
    {
        this.openOptions = openOptions;
        return this;
    }

    public GBPTree<KEY,VALUE> build()
    {
        return new GBPTree<>( pageCache, path, layout, monitor, headerReader, headerWriter, recoveryCleanupWorkCollector, readOnly, pageCacheTracer,
                openOptions, "test tree" );
    }
}
