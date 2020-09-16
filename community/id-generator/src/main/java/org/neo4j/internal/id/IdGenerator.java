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
package org.neo4j.internal.id;

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.util.VisibleForTesting;

public interface IdGenerator extends IdSequence, Closeable, ConsistencyCheckable
{
    /**
     * Allocates multiple IDs in one call.
     *
     * @param size the number of IDs to allocate.
     * @param forceConsecutiveAllocation if {@code true} the returned {@link IdRange} will guarantee that the allocation is a range of IDs
     * where all IDs are consecutive, i.e. an empty {@link IdRange#getDefragIds()}. If {@code false} there may be some of the allocated IDs
     * non-consecutive, i.e. returned as part of {@link IdRange#getDefragIds()}.
     * @param cursorTracer for tracing page accesses.
     * @return an {@link IdRange} containing the allocated IDs.
     */
    IdRange nextIdBatch( int size, boolean forceConsecutiveAllocation, PageCursorTracer cursorTracer );

    /**
     * @param id the highest in use + 1
     */
    void setHighId( long id );
    void markHighestWrittenAtHighId();
    @VisibleForTesting
    long getHighestWritten();
    long getHighId();
    long getHighestPossibleIdInUse();
    Marker marker( PageCursorTracer cursorTracer );

    @Override
    void close();
    long getNumberOfIdsInUse();
    long getDefragCount();

    void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer );

    /**
     * Does some maintenance. This operation isn't critical for the functionality of an IdGenerator, but may make it perform better.
     * The work happening inside this method should be work that would otherwise happen now and then inside the other methods anyway,
     * but letting a maintenance thread calling it may take some burden off of main request threads.
     *
     * @param awaitOngoing awaits any ongoing maintenance operation if another thread does such.
     * @param cursorTracer underlying page cursor tracer
     */
    void maintenance( boolean awaitOngoing, PageCursorTracer cursorTracer );

    /**
     * Starts the id generator, signaling that the database has entered normal operations mode.
     * Updates to this id generator may have come in before this call and those operations must be treated
     * as recovery operations.
     * @param freeIdsForRebuild access to stream of ids from the store to use if this id generator needs to be rebuilt when started
     * @param cursorTracer underlying page cursor tracer
     */
    void start( FreeIds freeIdsForRebuild, PageCursorTracer cursorTracer ) throws IOException;

    /**
     * Clears internal ID caches. This should only be used in specific scenarios where ID states have changed w/o the cache knowing about it.
     */
    void clearCache( PageCursorTracer cursorTracer );

    interface Marker extends AutoCloseable
    {
        void markUsed( long id );
        void markDeleted( long id );
        void markFree( long id );
        @Override
        void close();
    }

    class Delegate implements IdGenerator
    {
        private final IdGenerator delegate;

        public Delegate( IdGenerator delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public long nextId( PageCursorTracer cursorTracer )
        {
            return delegate.nextId( cursorTracer );
        }

        @Override
        public IdRange nextIdBatch( int size, boolean forceConsecutiveAllocation, PageCursorTracer cursorTracer )
        {
            return delegate.nextIdBatch( size, forceConsecutiveAllocation, cursorTracer );
        }

        @Override
        public void setHighId( long id )
        {
            delegate.setHighId( id );
        }

        @Override
        public void markHighestWrittenAtHighId()
        {
            delegate.markHighestWrittenAtHighId();
        }

        @Override
        public long getHighestWritten()
        {
            return delegate.getHighestWritten();
        }

        @Override
        public long getHighId()
        {
            return delegate.getHighId();
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return delegate.getHighestPossibleIdInUse();
        }

        @Override
        public Marker marker( PageCursorTracer cursorTracer )
        {
            return delegate.marker( cursorTracer );
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return delegate.getNumberOfIdsInUse();
        }

        @Override
        public long getDefragCount()
        {
            return delegate.getDefragCount();
        }

        @Override
        public void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
        {
            delegate.checkpoint( ioLimiter, cursorTracer );
        }

        @Override
        public void maintenance( boolean awaitOngoing, PageCursorTracer cursorTracer )
        {
            delegate.maintenance( awaitOngoing, cursorTracer );
        }

        @Override
        public void start( FreeIds freeIdsForRebuild, PageCursorTracer cursorTracer ) throws IOException
        {
            delegate.start( freeIdsForRebuild, cursorTracer );
        }

        @Override
        public void clearCache( PageCursorTracer cursorTracer )
        {
            delegate.clearCache( cursorTracer );
        }

        @Override
        public boolean consistencyCheck( ReporterFactory reporterFactory, PageCursorTracer cursorTracer )
        {
            return delegate.consistencyCheck( reporterFactory, cursorTracer );
        }
    }

    Marker NOOP_MARKER = new Marker()
    {
        @Override
        public void markFree( long id )
        {   // no-op
        }

        @Override
        public void markUsed( long id )
        {   // no-op
        }

        @Override
        public void markDeleted( long id )
        {   // no-op
        }

        @Override
        public void close()
        {   // no-op
        }
    };
}
