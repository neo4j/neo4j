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

import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_ONLINE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.TreeInconsistencyException;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntriesReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.values.storable.Value;

public abstract class NativeIndexAccessor<KEY extends NativeIndexKey<KEY>> extends NativeIndex<KEY>
        implements IndexAccessor {
    private final NativeIndexUpdater<KEY> singleUpdater;
    final NativeIndexHeaderWriter headerWriter;

    NativeIndexAccessor(
            DatabaseIndexContext databaseIndexContext,
            IndexFiles indexFiles,
            IndexLayout<KEY> layout,
            IndexDescriptor descriptor,
            ImmutableSet<OpenOption> openOptions) {
        super(databaseIndexContext, layout, indexFiles, descriptor, openOptions);
        singleUpdater = new NativeIndexUpdater<>(layout.newKey(), indexUpdateIgnoreStrategy());
        headerWriter = new NativeIndexHeaderWriter(BYTE_ONLINE);
    }

    @Override
    public void drop() {
        tree.setDeleteOnClose(true);
        closeTree();
        indexFiles.clear();
    }

    @Override
    public NativeIndexUpdater<KEY> newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
        assertOpen();
        try {
            if (parallel) {
                return new NativeIndexUpdater<>(layout.newKey(), indexUpdateIgnoreStrategy())
                        .initialize(tree.parallelWriter(cursorContext));
            } else {
                return singleUpdater.initialize(tree.writer(cursorContext));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * {@link IndexUpdateIgnoreStrategy Ignore strategy} to be used by index updater.
     * Sub-classes are expected to override this method if they want to use something
     * other than {@link IndexUpdateIgnoreStrategy#NO_IGNORE}.
     * @return {@link IndexUpdateIgnoreStrategy} to be used by index updater.
     */
    protected IndexUpdateIgnoreStrategy indexUpdateIgnoreStrategy() {
        return IndexUpdateIgnoreStrategy.NO_IGNORE;
    }

    @Override
    public void force(CursorContext cursorContext) {
        tree.checkpoint(cursorContext);
    }

    @Override
    public void refresh() {
        // not required in this implementation
    }

    @Override
    public void close() {
        closeTree();
    }

    @Override
    public abstract ValueIndexReader newValueReader();

    @Override
    public BoundedIterable<Long> newAllEntriesValueReader(
            long fromIdInclusive, long toIdExclusive, CursorContext cursorContext) {
        return new NativeAllEntriesReader<>(tree, layout, fromIdInclusive, toIdExclusive, cursorContext);
    }

    @Override
    public ResourceIterator<Path> snapshotFiles() {
        return asResourceIterator(iterator(indexFiles.getStoreFile()));
    }

    @Override
    public long estimateNumberOfEntries(CursorContext cursorContext) {
        try {
            return tree.estimateNumberOfEntriesInTree(cursorContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (TreeInconsistencyException e) {
            return UNKNOWN_NUMBER_OF_ENTRIES;
        }
    }

    @Override
    public IndexEntriesReader[] newAllEntriesValueReader(int partitions, CursorContext cursorContext) {
        KEY lowest = layout.newKey();
        lowest.initialize(Long.MIN_VALUE);
        lowest.initValuesAsLowest();
        KEY highest = layout.newKey();
        highest.initialize(Long.MAX_VALUE);
        highest.initValuesAsHighest();
        try {
            List<KEY> partitionEdges = tree.partitionedSeek(lowest, highest, partitions, cursorContext);
            Collection<IndexEntriesReader> readers = new ArrayList<>();
            for (int i = 0; i < partitionEdges.size() - 1; i++) {
                Seeker<KEY, NullValue> seeker =
                        tree.seek(partitionEdges.get(i), partitionEdges.get(i + 1), cursorContext);
                readers.add(new IndexEntriesReader() {
                    @Override
                    public long next() {
                        return seeker.key().getEntityId();
                    }

                    @Override
                    public boolean hasNext() {
                        try {
                            return seeker.next();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }

                    @Override
                    public Value[] values() {
                        return seeker.key().asValues();
                    }

                    @Override
                    public void close() {
                        try {
                            seeker.close();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                });
            }
            return readers.toArray(IndexEntriesReader[]::new);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
