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
package org.neo4j.internal.id;

import java.io.Closeable;
import java.io.IOException;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.range.PageIdRange;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;

public interface IdGenerator extends IdSequence, Closeable, ConsistencyCheckable {
    /**
     * Allocates an ID which is available to use. The returned ID can be either of:
     * <ul>
     *     <li>a new ID, allocated as the previously highest allocated plus one, or</li>
     *     <li>a reused ID, i.e. an ID which has been used, has become deleted and now available again</li>
     * </ul>
     *
     * @param cursorContext for tracking cursor interaction.
     * @return an ID available to use, guaranteed not used anywhere else.
     */
    @Override
    long nextId(CursorContext cursorContext);

    /**
     * Allocates a range of IDs that are guaranteed to be consecutive where the returned id represents the first i.e. lowest of them.
     *
     * @param numberOfIds the number of consecutive IDs to allocate in this range.
     * @param favorSamePage if {@code true} favors an allocation where all IDs are on the same page (if ID generator has notion about number of IDs per page),
     * otherwise {@code false} if the range is allowed to cross page boundaries.
     * @param cursorContext for tracing page accesses.
     * @return the first id in the consecutive range.
     */
    long nextConsecutiveIdRange(int numberOfIds, boolean favorSamePage, CursorContext cursorContext);

    /**
     * Reserve range of ids that cover whole page of the store
     * @param cursorContext for tracking cursor interaction.
     * @param idsPerPage - number of ids per page in store that this generator responsible for
     * @return range of reserved ids
     */
    PageIdRange nextPageRange(CursorContext cursorContext, int idsPerPage);

    /**
     * Release back id leftovers from previously reserved range.
     * As possibility, the whole range can be released for future users instead of individual ids.
     * @param range range to release leftovers from
     * @param cursorContext for tracking cursor interaction.
     */
    void releasePageRange(PageIdRange range, CursorContext cursorContext);

    /**
     * @param id the highest in use + 1
     */
    void setHighId(long id);

    default void setHighestPossibleIdInUse(long highestInUse) {
        setHighId(highestInUse + 1);
    }

    void markHighestWrittenAtHighId();

    long getHighestWritten();

    long getHighId();

    long getHighestPossibleIdInUse();

    long getUnusedIdCount() throws IOException;

    TransactionalMarker transactionalMarker(CursorContext cursorContext);

    ContextualMarker contextualMarker(CursorContext cursorContext);

    @Override
    void close();

    void checkpoint(FileFlushEvent flushEvent, CursorContext cursorContext);

    /**
     * Does some maintenance. This operation isn't critical for the functionality of an IdGenerator, but may make it perform better.
     * The work happening inside this method should be work that would otherwise happen now and then inside the other methods anyway,
     * but letting a maintenance thread calling it may take some burden off of main request threads.
     *
     * @param cursorContext underlying page cursor context
     */
    void maintenance(CursorContext cursorContext);

    /**
     * Starts the id generator, signaling that the database has entered normal operations mode.
     * Updates to this id generator may have come in before this call and those operations must be treated
     * as recovery operations.
     * @param freeIdsForRebuild access to stream of ids from the store to use if this id generator needs to be rebuilt when started
     * @param cursorContext underlying page cursor context
     */
    void start(FreeIds freeIdsForRebuild, CursorContext cursorContext) throws IOException;

    /**
     * Clears internal ID caches. This should only be used in specific scenarios where ID states have changed w/o the cache knowing about it.
     * This method can also enable/disable allocation, which ties into the role this member has.
     * @see #allocationEnabled()
     */
    void clearCache(boolean allocationEnabled, CursorContext cursorContext);

    /**
     * @return whether allocation can be performed on this ID generator. If {@code true} then calls like
     * {@link #nextId(CursorContext)} works and caches are active. This state is related to the role this
     * member has, e.g. leader or single instance (effectively the same) has allocation enabled, whereas followers
     * and others don't. This state can be changed in calls to {@link #clearCache(boolean, CursorContext)}.
     */
    boolean allocationEnabled();

    /**
     *
     * @return {@link IdType} of this generator.
     */
    IdType idType();

    /**
     * @return whether this ID generator can only allocate/reuse single IDs.
     */
    boolean hasOnlySingleIds();

    /**
     * Allows iteration over free or deleted ids in the generator, see {@link #notUsedIdsIterator(long, long)}
     * @throws IOException
     */
    default PrimitiveLongResourceIterator notUsedIdsIterator() throws IOException {
        return PrimitiveLongResourceCollections.emptyIterator();
    }

    /**
     * Allows iteration over free or deleted ids in the generator, up to highId. Items are return in sorted order
     * @param fromIdInclusive The id to start from (inclusive)
     * @param toIdExclusive The id to end at (exclusive)
     * @return A resource iterator. Not that this needs to be closed!
     * @throws IOException
     */
    default PrimitiveLongResourceIterator notUsedIdsIterator(long fromIdInclusive, long toIdExclusive)
            throws IOException {
        return PrimitiveLongResourceCollections.emptyIterator();
    }

    /**
     * Allows iteration over free ids in the generator, see {@link #notUsedIdsIterator(long, long)}
     */
    default PrimitiveLongResourceIterator freeIdsIterator() throws IOException {
        return PrimitiveLongResourceCollections.emptyIterator();
    }

    /**
     * Marks IDs as being one state or another. A typical chain of interactions:
     * <ul>Allocating, creating and deleting
     *     <li>ID X allocated from high ID when reserving an ID for an entity</li>
     *     <li>{@link #markUsed(long)} when entity with ID X is committed</li>
     *     <li>{@link #markDeleted(long)} when entity with ID X is later perhaps deleted</li>
     *     <li>X can now be allocated again, but from the freelist instead of from high ID</li>
     * </ul>
     * <ul>Allocating then un-allocating in case transaction rolls back
     *     <li>ID X allocated from high ID when reserving an ID for an entity</li>
     *     <li>{@link #markUnallocated(long)} when entity with ID is rolled back, i.e. X never used</li>
     *     <li>X can now be allocated again, but from the freelist instead of from high ID</li>
     * </ul>
     */
    interface TransactionalMarker extends AutoCloseable {
        default void markUsed(long id) {
            markUsed(id, 1);
        }

        void markUsed(long id, int numberOfIds);

        default void markDeleted(long id) {
            markDeleted(id, 1);
        }

        void markDeleted(long id, int numberOfIds);

        default void markDeletedAndFree(long id) {
            markDeletedAndFree(id, 1);
        }

        void markDeletedAndFree(long id, int numberOfIds);

        /**
         * For an ID that was allocated and later not committed (e.g. tx rolled back).
         */
        default void markUnallocated(long id) {
            markUnallocated(id, 1);
        }

        void markUnallocated(long id, int numberOfIds);

        @Override
        void close();

        class Delegate implements TransactionalMarker {
            protected final TransactionalMarker actual;

            public Delegate(TransactionalMarker actual) {
                this.actual = actual;
            }

            @Override
            public void markUsed(long id, int numberOfIds) {
                actual.markUsed(id, numberOfIds);
            }

            @Override
            public void markDeleted(long id, int numberOfIds) {
                actual.markDeleted(id, numberOfIds);
            }

            @Override
            public void markDeletedAndFree(long id, int numberOfIds) {
                actual.markDeletedAndFree(id, numberOfIds);
            }

            @Override
            public void markUnallocated(long id, int numberOfIds) {
                actual.markUnallocated(id, numberOfIds);
            }

            @Override
            public void close() {
                actual.close();
            }
        }
    }

    interface ContextualMarker extends AutoCloseable {
        default void markFree(long id) {
            markFree(id, 1);
        }

        void markFree(long id, int numberOfIds);

        default void markReserved(long id) {
            markReserved(id, 1);
        }

        default void markUnreserved(long id) {
            markUnreserved(id, 1);
        }

        void markReserved(long id, int numberOfIds);

        void markUnreserved(long id, int numberOfIds);

        void markUncached(long id, int numberOfIds);

        @Override
        void close();

        class Delegate implements ContextualMarker {
            private final ContextualMarker actual;

            public Delegate(ContextualMarker actual) {
                this.actual = actual;
            }

            @Override
            public void markFree(long id, int numberOfIds) {
                actual.markFree(id, numberOfIds);
            }

            @Override
            public void markReserved(long id, int numberOfIds) {
                actual.markReserved(id, numberOfIds);
            }

            @Override
            public void markUnreserved(long id, int numberOfIds) {
                actual.markUnreserved(id, numberOfIds);
            }

            @Override
            public void markUncached(long id, int numberOfIds) {
                actual.markUncached(id, numberOfIds);
            }

            @Override
            public void close() {
                actual.close();
            }
        }
    }

    class Delegate implements IdGenerator {
        protected final IdGenerator delegate;

        public Delegate(IdGenerator delegate) {
            this.delegate = delegate;
        }

        @Override
        public long nextId(CursorContext cursorContext) {
            return delegate.nextId(cursorContext);
        }

        @Override
        public long nextConsecutiveIdRange(int numberOfIds, boolean favorSamePage, CursorContext cursorContext) {
            return delegate.nextConsecutiveIdRange(numberOfIds, favorSamePage, cursorContext);
        }

        @Override
        public PageIdRange nextPageRange(CursorContext cursorContext, int idsPerPage) {
            return delegate.nextPageRange(cursorContext, idsPerPage);
        }

        @Override
        public void releasePageRange(PageIdRange range, CursorContext cursorContext) {
            delegate.releasePageRange(range, cursorContext);
        }

        @Override
        public void setHighId(long id) {
            delegate.setHighId(id);
        }

        @Override
        public void markHighestWrittenAtHighId() {
            delegate.markHighestWrittenAtHighId();
        }

        @Override
        public long getHighestWritten() {
            return delegate.getHighestWritten();
        }

        @Override
        public long getHighId() {
            return delegate.getHighId();
        }

        @Override
        public long getHighestPossibleIdInUse() {
            return delegate.getHighestPossibleIdInUse();
        }

        @Override
        public long getUnusedIdCount() throws IOException {
            return delegate.getUnusedIdCount();
        }

        @Override
        public TransactionalMarker transactionalMarker(CursorContext cursorContext) {
            return delegate.transactionalMarker(cursorContext);
        }

        @Override
        public ContextualMarker contextualMarker(CursorContext cursorContext) {
            return delegate.contextualMarker(cursorContext);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public void checkpoint(FileFlushEvent fileFlushEvent, CursorContext cursorContext) {
            delegate.checkpoint(fileFlushEvent, cursorContext);
        }

        @Override
        public void maintenance(CursorContext cursorContext) {
            delegate.maintenance(cursorContext);
        }

        @Override
        public void start(FreeIds freeIdsForRebuild, CursorContext cursorContext) throws IOException {
            delegate.start(freeIdsForRebuild, cursorContext);
        }

        @Override
        public void clearCache(boolean allocationEnabled, CursorContext cursorContext) {
            delegate.clearCache(allocationEnabled, cursorContext);
        }

        @Override
        public boolean allocationEnabled() {
            return delegate.allocationEnabled();
        }

        @Override
        public IdType idType() {
            return delegate.idType();
        }

        @Override
        public boolean hasOnlySingleIds() {
            return delegate.hasOnlySingleIds();
        }

        @Override
        public PrimitiveLongResourceIterator notUsedIdsIterator() throws IOException {
            return delegate.notUsedIdsIterator();
        }

        @Override
        public PrimitiveLongResourceIterator notUsedIdsIterator(long fromIdInclusive, long toIdExclusive)
                throws IOException {
            return delegate.notUsedIdsIterator(fromIdInclusive, toIdExclusive);
        }

        @Override
        public boolean consistencyCheck(
                ReporterFactory reporterFactory,
                CursorContextFactory contextFactory,
                int numThreads,
                ProgressMonitorFactory progressMonitorFactory) {
            return delegate.consistencyCheck(reporterFactory, contextFactory, numThreads, progressMonitorFactory);
        }
    }

    class NoOpMarker implements TransactionalMarker, ContextualMarker {
        @Override
        public void markFree(long id, int numberOfIds) {}

        @Override
        public void markUsed(long id, int numberOfIds) {}

        @Override
        public void markDeleted(long id, int numberOfIds) {}

        @Override
        public void markDeletedAndFree(long id, int numberOfIds) {}

        @Override
        public void markUnallocated(long id, int numberOfIds) {}

        @Override
        public void markUnreserved(long id, int numberOfIds) {}

        @Override
        public void markReserved(long id, int numberOfIds) {}

        @Override
        public void markUncached(long id, int numberOfIds) {}

        @Override
        public void close() {}
    }

    NoOpMarker NOOP_MARKER = new NoOpMarker() {};
}
