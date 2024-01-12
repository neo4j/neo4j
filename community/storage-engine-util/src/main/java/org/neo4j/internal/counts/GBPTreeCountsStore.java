/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.counts;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.counts.CountsStore;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;

/**
 * Counts store build on top of the {@link GBPTree}.
 * Changes between checkpoints are kept in memory and written out to the tree in {@link #checkpoint(FileFlushEvent, CursorContext)}.
 * Multiple {@link #updater(long, boolean, CursorContext)} appliers} can run concurrently in a lock-free manner.
 * Checkpoint will acquire a write lock, wait for currently active appliers to close while at the same time blocking new appliers to start,
 * but doesn't wait for appliers that haven't even started yet, i.e. it doesn't require a gap-free transaction sequence to be completed.
 */
public class GBPTreeCountsStore extends GBPTreeGenericCountsStore implements CountsStore {
    private static final String NAME = "Counts store";

    private static final byte TYPE_NODE = 1;
    private static final byte TYPE_RELATIONSHIP = 2;

    /**
     * Public utility method for instantiating a {@link CountsKey} for a node label id.
     * <p>
     * Key data layout for this type:
     * <pre>
     * first:  4B (lsb) labelId
     * second: 0
     * </pre>
     *
     * @param labelId id of the label.
     * @return a {@link CountsKey} for the node label id.
     */
    public static CountsKey nodeKey(int labelId) {
        return new CountsKey(TYPE_NODE, labelId, 0);
    }

    /**
     * Public utility method for instantiating a {@link CountsKey} for a node start/end label and relationship type id.
     * <p>
     * Key data layout for this type:
     * <pre>
     * first:  4B (msb) startLabelId, 4B (lsb) relationshipTypeId
     * second: 4B endLabelId
     * </pre>
     *
     * @param startLabelId id of the label of start node.
     * @param typeId       id of the relationship type.
     * @param endLabelId   id of the label of end node.
     * @return a {@link CountsKey} for the node start/end label and relationship type id.
     */
    public static CountsKey relationshipKey(int startLabelId, long typeId, int endLabelId) {
        return new CountsKey(
                TYPE_RELATIONSHIP, ((long) startLabelId << Integer.SIZE) | (typeId & 0xFFFFFFFFL), endLabelId);
    }

    public GBPTreeCountsStore(
            PageCache pageCache,
            Path file,
            FileSystemAbstraction fileSystem,
            RecoveryCleanupWorkCollector recoveryCollector,
            CountsBuilder initialCountsBuilder,
            boolean readOnly,
            Monitor monitor,
            String databaseName,
            int maxCacheSize,
            InternalLogProvider userLogProvider,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            ImmutableSet<OpenOption> openOptions)
            throws IOException {
        super(
                pageCache,
                file,
                fileSystem,
                recoveryCollector,
                new InitialCountsRebuilder(initialCountsBuilder),
                readOnly,
                NAME,
                monitor,
                databaseName,
                maxCacheSize,
                userLogProvider,
                contextFactory,
                pageCacheTracer,
                openOptions);
    }

    @Override
    public CountsUpdater updater(long txId, boolean isLast, CursorContext cursorContext) {
        CountUpdater updater = updaterImpl(txId, isLast, cursorContext);
        return updater != null ? new Incrementer(updater) : CountsUpdater.NO_OP_UPDATER;
    }

    @Override
    public CountsUpdater directUpdater(CursorContext cursorContext) throws IOException {
        return directUpdater(true, cursorContext);
    }

    public CountsUpdater directUpdater(boolean deltas, CursorContext cursorContext) throws IOException {
        CountUpdater updater = createDirectUpdater(deltas, cursorContext);
        return updater != null ? new Incrementer(updater) : CountsUpdater.NO_OP_UPDATER;
    }

    @Override
    public CountsUpdater rollbackUpdater(long txId, CursorContext cursorContext) {
        CountUpdater updater = updaterImpl(txId, false, cursorContext);
        return updater != null ? new Decrementer(updater) : CountsUpdater.NO_OP_UPDATER;
    }

    @Override
    public long nodeCount(int labelId, CursorContext cursorContext) {
        return read(nodeKey(labelId), cursorContext);
    }

    @Override
    public long estimateNodeCount(int labelId, CursorContext cursorContext) {
        return nodeCount(labelId, cursorContext);
    }

    @Override
    public long relationshipCount(int startLabelId, int typeId, int endLabelId, CursorContext cursorContext) {
        return read(relationshipKey(startLabelId, typeId, endLabelId), cursorContext);
    }

    @Override
    public long estimateRelationshipCount(int startLabelId, int typeId, int endLabelId, CursorContext cursorContext) {
        return relationshipCount(startLabelId, typeId, endLabelId, cursorContext);
    }

    @Override
    public void start(CursorContext cursorContext, MemoryTracker memoryTracker) throws IOException {
        super.start(cursorContext, memoryTracker);
    }

    @Override
    public void accept(CountsVisitor visitor, CursorContext cursorContext) {
        visitAllCounts(
                (key, count) -> {
                    if (key.type == TYPE_NODE) {
                        visitor.visitNodeCount((int) key.first, count);
                    } else if (key.type == TYPE_RELATIONSHIP) {
                        visitor.visitRelationshipCount(
                                key.extractHighFirstInt(), key.extractLowFirstInt(), key.second, count);
                    } else {
                        throw new IllegalArgumentException("Unknown key type " + key.type);
                    }
                },
                cursorContext);
    }

    public static String keyToString(CountsKey key) {
        if (key.type == TYPE_NODE) {
            return format("Node[label:%d]", key.first);
        } else if (key.type == TYPE_RELATIONSHIP) {
            return format(
                    "Relationship[startLabel:%d, type:%d, endLabel:%d]",
                    key.extractHighFirstInt(), key.extractLowFirstInt(), key.second);
        }
        throw new IllegalArgumentException("Unknown type " + key.type);
    }

    public static void dump(
            PageCache pageCache,
            FileSystemAbstraction fileSystem,
            Path file,
            PrintStream out,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            ImmutableSet<OpenOption> openOptions)
            throws IOException {
        GBPTreeGenericCountsStore.dump(
                pageCache,
                fileSystem,
                file,
                out,
                DEFAULT_DATABASE_NAME,
                NAME,
                contextFactory,
                pageCacheTracer,
                GBPTreeCountsStore::keyToString,
                openOptions);
    }

    private record Incrementer(CountUpdater actual) implements CountsUpdater {
        @Override
        public void incrementNodeCount(int labelId, long delta) {
            actual.increment(nodeKey(labelId), delta);
        }

        @Override
        public void incrementRelationshipCount(int startLabelId, int typeId, int endLabelId, long delta) {
            actual.increment(relationshipKey(startLabelId, typeId, endLabelId), delta);
        }

        @Override
        public void close() {
            actual.close();
        }
    }

    private record Decrementer(CountUpdater actual) implements CountsUpdater {
        @Override
        public void incrementNodeCount(int labelId, long delta) {
            actual.increment(nodeKey(labelId), -delta);
        }

        @Override
        public void incrementRelationshipCount(int startLabelId, int typeId, int endLabelId, long delta) {
            actual.increment(relationshipKey(startLabelId, typeId, endLabelId), -delta);
        }

        @Override
        public void close() {
            actual.close();
        }
    }

    private record InitialCountsRebuilder(CountsBuilder initialCountsBuilder) implements Rebuilder {

        @Override
        public long lastCommittedTxId() {
            return initialCountsBuilder.lastCommittedTxId();
        }

        @Override
        public void rebuild(CountUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
            initialCountsBuilder.initialize(new Incrementer(updater), cursorContext, memoryTracker);
        }
    }
}
