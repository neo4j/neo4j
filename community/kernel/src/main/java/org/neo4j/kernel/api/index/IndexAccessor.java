/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.SwallowingIndexUpdater;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

/**
 * Used for online operation of an index.
 */
public interface IndexAccessor extends Closeable
{
    /**
     * Deletes this index as well as closes all used external resources.
     * There will not be any interactions after this call.
     *
     * @throws IOException if unable to drop index.
     */
    void drop() throws IOException;

    /**
     * Return an updater for applying a set of changes to this index.
     * Updates must be visible in {@link #newReader() readers} created after this update.
     *
     * This is called with IndexUpdateMode.RECOVERY when starting up after
     * a crash or similar. Updates given then may have already been applied to this index, so
     * additional checks must be in place so that data doesn't get duplicated, but is idempotent.
     */
    IndexUpdater newUpdater( IndexUpdateMode mode );

    void flush() throws IOException;

    /**
     * Forces this index to disk. Called at certain points from within Neo4j for example when
     * rotating the logical log. After completion of this call there cannot be any essential state that
     * hasn't been forced to disk.
     *
     * @throws IOException if there was a problem forcing the state to persistent storage.
     */
    void force() throws IOException;

    /**
     * Closes this index accessor. There will not be any interactions after this call.
     * After completion of this call there cannot be any essential state that hasn't been forced to disk.
     *
     * @throws IOException if unable to close index.
     */
    @Override
    void close() throws IOException;

    /**
     * @return a new {@link IndexReader} responsible for looking up results in the index. The returned
     * reader must honor repeatable reads.
     */
    IndexReader newReader();

    BoundedIterable<Long> newAllEntriesReader();

    /**
     * Should return a full listing of all files needed by this index accessor to work with the index. The files
     * need to remain available until the resource iterator returned here is closed. This is used to duplicate created
     * indexes across clusters, among other things.
     */
    ResourceIterator<File> snapshotFiles() throws IOException;

    class Adapter implements IndexAccessor
    {
        @Override
        public void drop()
        {
        }

        @Override
        public IndexUpdater newUpdater( IndexUpdateMode mode )
        {
            return SwallowingIndexUpdater.INSTANCE;
        }

        @Override
        public void flush()
        {
        }

        @Override
        public void force()
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
        public BoundedIterable<Long> newAllEntriesReader()
        {
            return new BoundedIterable<Long>()
            {
                @Override
                public long maxCount()
                {
                    return 0;
                }

                @Override public void close() throws IOException
                {
                }

                @Override public Iterator<Long> iterator()
                {
                    return emptyIterator();
                }
            };
        }

        @Override
        public ResourceIterator<File> snapshotFiles()
        {
            return emptyIterator();
        }
    }

    class Delegator implements IndexAccessor
    {
        private final IndexAccessor delegate;

        public Delegator( IndexAccessor delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public void drop() throws IOException
        {
            delegate.drop();
        }

        @Override
        public IndexUpdater newUpdater( IndexUpdateMode mode )
        {
            return delegate.newUpdater( mode );
        }

        @Override
        public void flush() throws IOException
        {
            delegate.flush();
        }

        @Override
        public void force() throws IOException
        {
            delegate.force();
        }

        @Override
        public void close() throws IOException
        {
            delegate.close();
        }

        @Override
        public IndexReader newReader()
        {
            return delegate.newReader();
        }

        @Override
        public BoundedIterable<Long> newAllEntriesReader()
        {
            return delegate.newAllEntriesReader();
        }

        @Override
        public ResourceIterator<File> snapshotFiles() throws IOException
        {
            return delegate.snapshotFiles();
        }

        @Override
        public String toString()
        {
            return delegate.toString();
        }
    }
}
