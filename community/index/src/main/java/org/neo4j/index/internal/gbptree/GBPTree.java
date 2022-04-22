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
package org.neo4j.index.internal.gbptree;

import static org.neo4j.index.internal.gbptree.RootLayerConfiguration.singleRoot;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;

/**
 * Merely a convenience for a single-root {@link MultiRootGBPTree}.
 *
 * A {@link GBPTree} has a special write mode {@link #writer(int, CursorContext)} when passing in {@link DataTree#W_BATCHED_SINGLE_THREADED}
 * which is more efficient when inserting consecutive entries. The single writer cannot co-exist with other parallel writers.
 */
public class GBPTree<KEY, VALUE> extends MultiRootGBPTree<SingleRoot, KEY, VALUE> implements DataTree<KEY, VALUE> {
    private final DataTree<KEY, VALUE> access;

    public GBPTree(
            PageCache pageCache,
            Path indexFile,
            Layout<KEY, VALUE> layout,
            Monitor monitor,
            Header.Reader headerReader,
            Consumer<PageCursor> headerWriter,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            DatabaseReadOnlyChecker readOnlyChecker,
            ImmutableSet<OpenOption> openOptions,
            String databaseName,
            String name,
            CursorContextFactory contextFactory)
            throws MetadataMismatchException {
        super(
                pageCache,
                indexFile,
                layout,
                monitor,
                headerReader,
                headerWriter,
                recoveryCleanupWorkCollector,
                readOnlyChecker,
                openOptions,
                databaseName,
                name,
                contextFactory,
                singleRoot());
        access = rootLayer.access(SingleRoot.SINGLE_ROOT);
    }

    @Override
    public Seeker<KEY, VALUE> allocateSeeker(CursorContext cursorContext) throws IOException {
        return access.allocateSeeker(cursorContext);
    }

    @Override
    public Seeker<KEY, VALUE> seek(Seeker<KEY, VALUE> seeker, KEY fromInclusive, KEY toExclusive) throws IOException {
        return access.seek(seeker, fromInclusive, toExclusive);
    }

    @Override
    public List<KEY> partitionedSeek(
            KEY fromInclusive, KEY toExclusive, int desiredNumberOfPartitions, CursorContext cursorContext)
            throws IOException {
        return access.partitionedSeek(fromInclusive, toExclusive, desiredNumberOfPartitions, cursorContext);
    }

    @Override
    public Writer<KEY, VALUE> writer(int flags, CursorContext cursorContext) throws IOException {
        return access.writer(flags, cursorContext);
    }

    @Override
    public long estimateNumberOfEntriesInTree(CursorContext cursorContext) throws IOException {
        return access.estimateNumberOfEntriesInTree(cursorContext);
    }
}
