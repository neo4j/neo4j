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
package org.neo4j.index.internal.gbptree;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;

/**
 * Convenient builder for a {@link GBPTree}. Either created using zero-argument constructor for maximum
 * flexibility, or with constructor with arguments considered mandatory to be able to build a proper tree.
 *
 * @param <KEY> type of key in {@link GBPTree}
 * @param <VALUE> type of value in {@link GBPTree}
 */
class GBPTreeBuilder<KEY,VALUE>
{
    private PageCache pageCache;
    private File file;
    private int tentativeIndexPageSize;
    private Monitor monitor = NO_MONITOR;
    private Header.Reader headerReader = NO_HEADER_READER;
    private Layout<KEY,VALUE> layout;
    private Consumer<PageCursor> headerWriter = NO_HEADER_WRITER;
    private RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.immediate();

    GBPTreeBuilder()
    {
    }

    GBPTreeBuilder( PageCache pageCache, File file, Layout<KEY,VALUE> layout )
    {
        with( pageCache );
        with( file );
        with( layout );
    }

    GBPTreeBuilder<KEY,VALUE> with( Layout<KEY,VALUE> layout )
    {
        this.layout = layout;
        return this;
    }

    GBPTreeBuilder<KEY,VALUE> with( File file )
    {
        this.file = file;
        return this;
    }

    GBPTreeBuilder<KEY,VALUE> with( PageCache pageCache )
    {
        this.pageCache = pageCache;
        return this;
    }

    GBPTreeBuilder<KEY,VALUE> withIndexPageSize( int tentativeIndexPageSize )
    {
        this.tentativeIndexPageSize = tentativeIndexPageSize;
        return this;
    }

    GBPTreeBuilder<KEY,VALUE> with( GBPTree.Monitor monitor )
    {
        this.monitor = monitor;
        return this;
    }

    GBPTreeBuilder<KEY,VALUE> with( Header.Reader headerReader )
    {
        this.headerReader = headerReader;
        return this;
    }

    GBPTreeBuilder<KEY,VALUE> with( Consumer<PageCursor> headerWriter )
    {
        this.headerWriter = headerWriter;
        return this;
    }

    GBPTreeBuilder<KEY,VALUE> with( RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        return this;
    }

    GBPTree<KEY,VALUE> build() throws IOException
    {
        return new GBPTree<>( pageCache, file, layout, tentativeIndexPageSize, monitor, headerReader, headerWriter,
                recoveryCleanupWorkCollector );
    }
}
