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
package org.neo4j.kernel.api.index;

import static java.util.Collections.emptyIterator;
import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;

import java.io.Closeable;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.SwallowingIndexUpdater;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.kernel.impl.index.schema.EntityTokenRange;
import org.neo4j.values.storable.Value;

/**
 * Used for online operation of an index.
 */
public interface IndexAccessor extends Closeable, ConsistencyCheckable, MinimalIndexAccessor {
    long UNKNOWN_NUMBER_OF_ENTRIES = -1;
    IndexAccessor EMPTY = new Adapter();

    /**
     * Return an updater for applying a set of changes to this index.
     * Updates must be visible in {@link #newValueReader() value} and {@link #newTokenReader()} token} readers created after this update.
     * <p>
     * This is called with IndexUpdateMode.RECOVERY when starting up after
     * a crash or similar. Updates given then may have already been applied to this index, so
     * additional checks must be in place so that data doesn't get duplicated, but is idempotent.
     *
     * @param mode apply updates with the hint that the existing state may not be valid.
     * @param cursorContext the context to track cursor access.
     * @param parallel hint that this updater will be used in parallel with other updaters concurrently on this index.
     */
    IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel);

    /**
     * Forces this index to disk. Called at certain points from within Neo4j for example when
     * rotating the logical log. After completion of this call there cannot be any essential state that
     * hasn't been forced to disk.
     *
     * @param cursorContext underlying page cursor context
     * @throws UncheckedIOException if there was a problem forcing the state to persistent storage.
     */
    void force(CursorContext cursorContext);

    /**
     * Refreshes this index, so that {@link #newValueReader() readers} created after completion of this call
     * will see the latest updates. This happens automatically on closing {@link #newUpdater(IndexUpdateMode, CursorContext, boolean)}
     * w/ {@link IndexUpdateMode#ONLINE}, but not guaranteed for {@link IndexUpdateMode#RECOVERY}.
     * Therefore this call is complementary for updates that has taken place with {@link IndexUpdateMode#RECOVERY}.
     *
     * @throws UncheckedIOException if there was a problem refreshing the index.
     */
    void refresh();

    /**
     * Closes this index accessor. There will not be any interactions after this call.
     * After completion of this call there cannot be any essential state that hasn't been forced to disk.
     *
     * @throws UncheckedIOException if unable to close index.
     */
    @Override
    void close();

    /**
     * @return a new {@link ValueIndexReader} responsible for looking up results in the index. The returned reader must honor repeatable reads.
     * @throws UnsupportedOperationException if underline index is not Value Index
     */
    ValueIndexReader newValueReader();

    /**
     * @return a new {@link TokenIndexReader} responsible for looking up token to entity mappings from the index.
     * The returned reader must honor repeatable reads.
     * @throws UnsupportedOperationException if underline index is not Token Index
     */
    default TokenIndexReader newTokenReader() {
        throw new UnsupportedOperationException(
                "Not supported for " + getClass().getSimpleName());
    }

    /**
     * @param cursorContext underlying page cursor context
     * @return a {@link BoundedIterable} to access all entity ids indexed in this index.
     */
    default BoundedIterable<Long> newAllEntriesValueReader(CursorContext cursorContext) {
        return newAllEntriesValueReader(0, Long.MAX_VALUE, cursorContext);
    }

    /**
     * @param cursorContext underlying page cursor context
     * @return a {@link BoundedIterable} to access all entity ids indexed in the range {@code fromIdInclusive}-{@code toIdExclusive} in this index.
     */
    BoundedIterable<Long> newAllEntriesValueReader(
            long fromIdInclusive, long toIdExclusive, CursorContext cursorContext);

    /**
     * @param cursorContext underlying page cursor context
     * @return a {@link BoundedIterable} to access all token ids associated with every entity indexed in the range
     * {@code fromIdInclusive}-{@code toIdExclusive} in this index.
     */
    default BoundedIterable<EntityTokenRange> newAllEntriesTokenReader(
            long fromIdInclusive, long toIdExclusive, CursorContext cursorContext) {
        throw new UnsupportedOperationException(
                "Not supported for " + getClass().getSimpleName());
    }

    /**
     * Returns one or more {@link IndexEntriesReader readers} reading all entries in this index. The supplied {@code partitions} is a hint
     * for how many readers the caller wants back, each reader only reading a part of the whole index. The returned readers can be
     * read individually in parallel and collectively all partitions will read all the index entries in this index.
     *
     * @param partitions a hint for how many partitions will be returned.
     * @param cursorContext underlying page cursor context
     * @return the partitions that can read the index entries in this index. The implementation should strive to adhere to this number,
     * but the only real contract is that the returned number of readers is between 1 <= numberOfReturnedReaders <= partitions.
     */
    default IndexEntriesReader[] newAllEntriesValueReader(int partitions, CursorContext cursorContext) {
        BoundedIterable<Long> entriesReader = newAllEntriesValueReader(cursorContext);
        Iterator<Long> ids = entriesReader.iterator();
        IndexEntriesReader reader = new IndexEntriesReader() {
            @Override
            public Value[] values() {
                return null;
            }

            @Override
            public long next() {
                return ids.next();
            }

            @Override
            public boolean hasNext() {
                return ids.hasNext();
            }

            @Override
            public void close() {
                IOUtils.closeAllUnchecked(entriesReader);
            }
        };
        return new IndexEntriesReader[] {reader};
    }

    /**
     * Should return a full listing of all files needed by this index accessor to work with the index. The files
     * need to remain available until the resource iterator returned here is closed. This is used to duplicate created
     * indexes across clusters, among other things.
     */
    ResourceIterator<Path> snapshotFiles();

    /**
     * Validates the {@link Value value tuple} before transaction determines that it can commit.
     */
    default void validateBeforeCommit(long entityId, Value[] tuple) {
        // For most value types there are no specific validations to be made.
    }

    /**
     * @return an estimate of the number of entries, i.e. entityId+values pairs, in this index, or {@link #UNKNOWN_NUMBER_OF_ENTRIES}
     * if number of entries couldn't be determined.
     * @param cursorContext underlying page cursor context
     */
    long estimateNumberOfEntries(CursorContext cursorContext);

    class Adapter implements IndexAccessor {
        @Override
        public void drop() {}

        @Override
        public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
            return SwallowingIndexUpdater.INSTANCE;
        }

        @Override
        public void force(CursorContext cursorContext) {}

        @Override
        public void refresh() {}

        @Override
        public void close() {}

        @Override
        public ValueIndexReader newValueReader() {
            return ValueIndexReader.EMPTY;
        }

        @Override
        public BoundedIterable<Long> newAllEntriesValueReader(
                long fromIdInclusive, long toIdExclusive, CursorContext cursorContext) {
            return new BoundedIterable<>() {
                @Override
                public long maxCount() {
                    return 0;
                }

                @Override
                public void close() {}

                @Override
                public Iterator<Long> iterator() {
                    return emptyIterator();
                }
            };
        }

        @Override
        public ResourceIterator<Path> snapshotFiles() {
            return emptyResourceIterator();
        }

        @Override
        public boolean consistencyCheck(ReporterFactory reporterFactory, CursorContext cursorContext) {
            return true;
        }

        @Override
        public long estimateNumberOfEntries(CursorContext cursorContext) {
            return UNKNOWN_NUMBER_OF_ENTRIES;
        }
    }

    class Delegating implements IndexAccessor {
        private final IndexAccessor delegate;

        public Delegating(IndexAccessor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void drop() {
            delegate.drop();
        }

        @Override
        public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
            return delegate.newUpdater(mode, cursorContext, parallel);
        }

        @Override
        public void force(CursorContext cursorContext) {
            delegate.force(cursorContext);
        }

        @Override
        public void refresh() {
            delegate.refresh();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public ValueIndexReader newValueReader() {
            return delegate.newValueReader();
        }

        @Override
        public BoundedIterable<Long> newAllEntriesValueReader(CursorContext cursorContext) {
            return delegate.newAllEntriesValueReader(cursorContext);
        }

        @Override
        public BoundedIterable<Long> newAllEntriesValueReader(
                long fromIdInclusive, long toIdExclusive, CursorContext cursorContext) {
            return delegate.newAllEntriesValueReader(fromIdInclusive, toIdExclusive, cursorContext);
        }

        @Override
        public ResourceIterator<Path> snapshotFiles() {
            return delegate.snapshotFiles();
        }

        @Override
        public Map<String, Value> indexConfig() {
            return delegate.indexConfig();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public void validateBeforeCommit(long entityId, Value[] tuple) {
            delegate.validateBeforeCommit(entityId, tuple);
        }

        @Override
        public boolean consistencyCheck(ReporterFactory reporterFactory, CursorContext cursorContext) {
            return delegate.consistencyCheck(reporterFactory, cursorContext);
        }

        @Override
        public long estimateNumberOfEntries(CursorContext cursorContext) {
            return delegate.estimateNumberOfEntries(cursorContext);
        }
    }
}
