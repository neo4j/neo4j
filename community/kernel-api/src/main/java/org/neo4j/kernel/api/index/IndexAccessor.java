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
package org.neo4j.kernel.api.index;

import java.io.Closeable;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.SwallowingIndexUpdater;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyIterator;
import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;

/**
 * Used for online operation of an index.
 */
public interface IndexAccessor extends Closeable, ConsistencyCheckable, MinimalIndexAccessor
{
    long UNKNOWN_NUMBER_OF_ENTRIES = -1;
    IndexAccessor EMPTY = new Adapter();

    /**
     * Return an updater for applying a set of changes to this index.
     * Updates must be visible in {@link #newReader() readers} created after this update.
     * <p>
     * This is called with IndexUpdateMode.RECOVERY when starting up after
     * a crash or similar. Updates given then may have already been applied to this index, so
     * additional checks must be in place so that data doesn't get duplicated, but is idempotent.
     */
    IndexUpdater newUpdater( IndexUpdateMode mode, PageCursorTracer cursorTracer );

    /**
     * Forces this index to disk. Called at certain points from within Neo4j for example when
     * rotating the logical log. After completion of this call there cannot be any essential state that
     * hasn't been forced to disk.
     *
     * @param ioLimiter The {@link IOLimiter} to use for implementations living on top of {@link org.neo4j.io.pagecache.PageCache}.
     * @param cursorTracer underlying page cursor tracer
     * @throws UncheckedIOException if there was a problem forcing the state to persistent storage.
     */
    void force( IOLimiter ioLimiter, PageCursorTracer cursorTracer );

    /**
     * Refreshes this index, so that {@link #newReader() readers} created after completion of this call
     * will see the latest updates. This happens automatically on closing {@link #newUpdater(IndexUpdateMode, PageCursorTracer)}
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
     * @return a new {@link IndexReader} responsible for looking up results in the index. The returned
     * reader must honor repeatable reads.
     */
    IndexReader newReader();

    /**
     * @param cursorTracer underlying page cursor tracer
     * @return a {@link BoundedIterable} to access all entity ids indexed in this index.
     */
    default BoundedIterable<Long> newAllEntriesReader( PageCursorTracer cursorTracer )
    {
        return newAllEntriesReader( 0, Long.MAX_VALUE, cursorTracer );
    }

    /**
     * @param cursorTracer underlying page cursor tracer
     * @return a {@link BoundedIterable} to access all entity ids indexed in the range {@code fromIdInclusive}-{@code toIdExclusive} in this index.
     */
    BoundedIterable<Long> newAllEntriesReader( long fromIdInclusive, long toIdExclusive, PageCursorTracer cursorTracer );

    /**
     * Returns one or more {@link IndexEntriesReader readers} reading all entries in this index. The supplied {@code partitions} is a hint
     * for how many readers the caller wants back, each reader only reading a part of the whole index. The returned readers can be
     * read individually in parallel and collectively all partitions will read all the index entries in this index.
     *
     * @param partitions a hint for how many partitions will be returned.
     * @param cursorTracer underlying page cursor tracer
     * @return the partitions that can read the index entries in this index. The implementation should strive to adhere to this number,
     * but the only real contract is that the returned number of readers is between 1 <= numberOfReturnedReaders <= partitions.
     */
    default IndexEntriesReader[] newAllIndexEntriesReader( int partitions, PageCursorTracer cursorTracer )
    {
        BoundedIterable<Long> entriesReader = newAllEntriesReader( cursorTracer );
        Iterator<Long> ids = entriesReader.iterator();
        IndexEntriesReader reader = new IndexEntriesReader()
        {
            @Override
            public Value[] values()
            {
                return null;
            }

            @Override
            public long next()
            {
                return ids.next();
            }

            @Override
            public boolean hasNext()
            {
                return ids.hasNext();
            }

            @Override
            public void close()
            {
                IOUtils.closeAllUnchecked( entriesReader );
            }
        };
        return new IndexEntriesReader[]{reader};
    }

    /**
     * Should return a full listing of all files needed by this index accessor to work with the index. The files
     * need to remain available until the resource iterator returned here is closed. This is used to duplicate created
     * indexes across clusters, among other things.
     */
    ResourceIterator<Path> snapshotFiles();

    /**
     * Verifies that each value in this index is unique.
     * Index is guaranteed to not change while this call executes.
     *
     * @param nodePropertyAccessor {@link NodePropertyAccessor} for accessing properties from database storage
     * in the event of conflicting values.
     * @throws IndexEntryConflictException for first detected uniqueness conflict, if any.
     * @throws UncheckedIOException on error reading from source files.
     */
    void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException;

    /**
     * Validates the {@link Value value tuple} before transaction determines that it can commit.
     */
    default void validateBeforeCommit( long entityId, Value[] tuple )
    {
        // For most value types there are no specific validations to be made.
    }

    /**
     * @return an estimate of the number of entries, i.e. entityId+values pairs, in this index, or {@link #UNKNOWN_NUMBER_OF_ENTRIES}
     * if number of entries couldn't be determined.
     * @param cursorTracer underlying page cursor tracer
     */
    long estimateNumberOfEntries( PageCursorTracer cursorTracer );

    class Adapter implements IndexAccessor
    {
        @Override
        public void drop()
        {
        }

        @Override
        public IndexUpdater newUpdater( IndexUpdateMode mode, PageCursorTracer cursorTracer )
        {
            return SwallowingIndexUpdater.INSTANCE;
        }

        @Override
        public void force( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
        {
        }

        @Override
        public void refresh()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public IndexReader newReader()
        {
            return IndexReader.EMPTY;
        }

        @Override
        public BoundedIterable<Long> newAllEntriesReader( long fromIdInclusive, long toIdExclusive, PageCursorTracer cursorTracer )
        {
            return new BoundedIterable<>()
            {
                @Override
                public long maxCount()
                {
                    return 0;
                }

                @Override
                public void close()
                {
                }

                @Override
                public Iterator<Long> iterator()
                {
                    return emptyIterator();
                }
            };
        }

        @Override
        public ResourceIterator<Path> snapshotFiles()
        {
            return emptyResourceIterator();
        }

        @Override
        public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor )
        {
        }

        @Override
        public boolean consistencyCheck( ReporterFactory reporterFactory, PageCursorTracer cursorTracer )
        {
            return true;
        }

        @Override
        public long estimateNumberOfEntries( PageCursorTracer cursorTracer )
        {
            return UNKNOWN_NUMBER_OF_ENTRIES;
        }
    }

    class Delegating implements IndexAccessor
    {
        private final IndexAccessor delegate;

        public Delegating( IndexAccessor delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public void drop()
        {
            delegate.drop();
        }

        @Override
        public IndexUpdater newUpdater( IndexUpdateMode mode, PageCursorTracer cursorTracer )
        {
            return delegate.newUpdater( mode, cursorTracer );
        }

        @Override
        public void force( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
        {
            delegate.force( ioLimiter, cursorTracer );
        }

        @Override
        public void refresh()
        {
            delegate.refresh();
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public IndexReader newReader()
        {
            return delegate.newReader();
        }

        @Override
        public BoundedIterable<Long> newAllEntriesReader( PageCursorTracer cursorTracer )
        {
            return delegate.newAllEntriesReader( cursorTracer );
        }

        @Override
        public BoundedIterable<Long> newAllEntriesReader( long fromIdInclusive, long toIdExclusive, PageCursorTracer cursorTracer )
        {
            return delegate.newAllEntriesReader( fromIdInclusive, toIdExclusive, cursorTracer );
        }

        @Override
        public ResourceIterator<Path> snapshotFiles()
        {
            return delegate.snapshotFiles();
        }

        @Override
        public Map<String,Value> indexConfig()
        {
            return delegate.indexConfig();
        }

        @Override
        public String toString()
        {
            return delegate.toString();
        }

        @Override
        public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
        {
            delegate.verifyDeferredConstraints( nodePropertyAccessor );
        }

        @Override
        public void validateBeforeCommit( long entityId, Value[] tuple )
        {
            delegate.validateBeforeCommit( entityId, tuple );
        }

        @Override
        public boolean consistencyCheck( ReporterFactory reporterFactory, PageCursorTracer cursorTracer )
        {
            return delegate.consistencyCheck( reporterFactory, cursorTracer );
        }

        @Override
        public long estimateNumberOfEntries( PageCursorTracer cursorTracer )
        {
            return delegate.estimateNumberOfEntries( cursorTracer );
        }
    }
}
