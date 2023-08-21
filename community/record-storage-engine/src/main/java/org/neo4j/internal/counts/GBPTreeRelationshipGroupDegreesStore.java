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
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;

/**
 * {@link RelationshipGroupDegreesStore} backed by the {@link GBPTree}.
 * @see GBPTreeGenericCountsStore
 */
public class GBPTreeRelationshipGroupDegreesStore extends GBPTreeGenericCountsStore
        implements RelationshipGroupDegreesStore {
    private static final String NAME = "Relationship group degrees store";
    static final byte TYPE_DEGREE = (byte) 3;

    public GBPTreeRelationshipGroupDegreesStore(
            PageCache pageCache,
            Path file,
            FileSystemAbstraction fileSystem,
            RecoveryCleanupWorkCollector recoveryCollector,
            DegreesRebuilder rebuilder,
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
                new RebuilderWrapper(rebuilder),
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
    public DegreeUpdater updater(long txId, boolean isLast, CursorContext cursorContext) {
        CountUpdater updater = updaterImpl(txId, isLast, cursorContext);
        return updater != null ? new TreeUpdater(updater) : DegreeUpdater.NO_OP_UPDATER;
    }

    @Override
    public DegreeUpdater directApply(CursorContext cursorContext) throws IOException {
        return directApply(false, cursorContext);
    }

    public DegreeUpdater directApply(boolean applyDeltas, CursorContext cursorContext) throws IOException {
        return new TreeUpdater(createDirectUpdater(applyDeltas, cursorContext));
    }

    @Override
    public long degree(long groupId, RelationshipDirection direction, CursorContext cursorContext) {
        return read(degreeKey(groupId, direction), cursorContext);
    }

    @Override
    public void accept(GroupDegreeVisitor visitor, CursorContext cursorContext) {
        visitAllCounts((key, count) -> visitor.degree(groupIdOf(key), directionOf(key), count), cursorContext);
    }

    @Override
    public void start(CursorContext cursorContext, MemoryTracker memoryTracker) throws IOException {
        super.start(cursorContext, memoryTracker);
    }

    private static class TreeUpdater implements DegreeUpdater, AutoCloseable {
        private final CountUpdater actual;

        TreeUpdater(CountUpdater actual) {
            this.actual = actual;
        }

        @Override
        public void increment(long groupId, RelationshipDirection direction, long delta) {
            actual.increment(degreeKey(groupId, direction), delta);
        }

        @Override
        public void close() {
            actual.close();
        }
    }

    /**
     * Public utility method for instantiating a {@link CountsKey} for a degree.
     *
     * Key data layout for this type:
     * <pre>
     * first:  [gggg,gggg][gggg,gggg][gggg,gggg][gggg,gggg] [gggg,gggg][gggg,gggg][gggg,gggg][gggg,ggdd]
     *         g: relationship group id, {@link RelationshipGroupRecord#getId()}
     *         d: {@link RelationshipDirection#id()}
     * second: 0
     * </pre>
     *
     * @param groupId relationship group ID.
     * @param direction direction for the relationship chain.
     * @return a {@link CountsKey for the relationship chain (group+direction). The returned key can be put into maps and similar.
     */
    static CountsKey degreeKey(long groupId, RelationshipDirection direction) {
        return new CountsKey(TYPE_DEGREE, groupId << 2 | direction.id(), 0);
    }

    static String keyToString(CountsKey key) {
        if (key.type == TYPE_DEGREE) {
            return format("Degree[groupId:%d, direction:%s]", groupIdOf(key), directionOf(key));
        }
        throw new IllegalArgumentException("Unknown type " + key.type);
    }

    private static RelationshipDirection directionOf(CountsKey key) {
        return RelationshipDirection.ofId((int) (key.first & 0x3));
    }

    private static long groupIdOf(CountsKey key) {
        return key.first >> 2;
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
                GBPTreeRelationshipGroupDegreesStore::keyToString,
                openOptions);
    }

    private static class RebuilderWrapper implements Rebuilder {
        private final DegreesRebuilder rebuilder;

        RebuilderWrapper(DegreesRebuilder rebuilder) {
            this.rebuilder = rebuilder;
        }

        @Override
        public void rebuild(CountUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
            rebuilder.rebuild(new TreeUpdater(updater), cursorContext, memoryTracker);
        }

        @Override
        public long lastCommittedTxId() {
            return rebuilder.lastCommittedTxId();
        }
    }

    public static class EmptyDegreesRebuilder implements DegreesRebuilder {
        private final long lastTxId;

        public EmptyDegreesRebuilder(long lastTxId) {
            this.lastTxId = lastTxId;
        }

        @Override
        public void rebuild(DegreeUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {}

        @Override
        public long lastCommittedTxId() {
            return lastTxId;
        }
    }
}
