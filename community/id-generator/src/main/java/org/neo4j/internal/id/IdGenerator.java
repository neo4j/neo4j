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
package org.neo4j.internal.id;

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.util.VisibleForTesting;

public interface IdGenerator extends IdSequence, Closeable, ConsistencyCheckable
{
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
    long nextId( CursorContext cursorContext );

    /**
     * Allocates a range of IDs that are guaranteed to be consecutive where the returned id represents the first i.e. lowest of them.
     *
     * @param numberOfIds the number of consecutive IDs to allocate in this range.
     * @param favorSamePage if {@code true} favors an allocation where all IDs are on the same page (if ID generator has notion about number of IDs per page),
     * otherwise {@code false} if the range is allowed to cross page boundaries.
     * @param cursorContext for tracing page accesses.
     * @return the first id in the consecutive range.
     */
    long nextConsecutiveIdRange( int numberOfIds, boolean favorSamePage, CursorContext cursorContext );

    /**
     * @param id the highest in use + 1
     */
    void setHighId( long id );
    void markHighestWrittenAtHighId();
    @VisibleForTesting
    long getHighestWritten();
    long getHighId();
    long getHighestPossibleIdInUse();
    Marker marker( CursorContext cursorContext );

    @Override
    void close();
    long getNumberOfIdsInUse();
    long getDefragCount();

    void checkpoint( CursorContext cursorContext );

    /**
     * Does some maintenance. This operation isn't critical for the functionality of an IdGenerator, but may make it perform better.
     * The work happening inside this method should be work that would otherwise happen now and then inside the other methods anyway,
     * but letting a maintenance thread calling it may take some burden off of main request threads.
     *
     * @param cursorContext underlying page cursor context
     */
    void maintenance( CursorContext cursorContext );

    /**
     * Starts the id generator, signaling that the database has entered normal operations mode.
     * Updates to this id generator may have come in before this call and those operations must be treated
     * as recovery operations.
     * @param freeIdsForRebuild access to stream of ids from the store to use if this id generator needs to be rebuilt when started
     * @param cursorContext underlying page cursor context
     */
    void start( FreeIds freeIdsForRebuild, CursorContext cursorContext ) throws IOException;

    /**
     * Clears internal ID caches. This should only be used in specific scenarios where ID states have changed w/o the cache knowing about it.
     */
    void clearCache( CursorContext cursorContext );

    interface Marker extends AutoCloseable
    {
        default void markUsed( long id )
        {
            markUsed( id, 1 );
        }

        void markUsed( long id, int numberOfIds );

        default void markDeleted( long id )
        {
            markDeleted( id, 1 );
        }

        void markDeleted( long id, int numberOfIds );

        default void markFree( long id )
        {
            markFree( id, 1 );
        }

        void markFree( long id, int numberOfIds );

        @Override
        void close();
    }

    class Delegate implements IdGenerator
    {
        protected final IdGenerator delegate;

        public Delegate( IdGenerator delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public long nextId( CursorContext cursorContext )
        {
            return delegate.nextId( cursorContext );
        }

        @Override
        public long nextConsecutiveIdRange( int numberOfIds, boolean favorSamePage, CursorContext cursorContext )
        {
            return delegate.nextConsecutiveIdRange( numberOfIds, favorSamePage, cursorContext );
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
        public Marker marker( CursorContext cursorContext )
        {
            return delegate.marker( cursorContext );
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
        public void checkpoint( CursorContext cursorContext )
        {
            delegate.checkpoint( cursorContext );
        }

        @Override
        public void maintenance( CursorContext cursorContext )
        {
            delegate.maintenance( cursorContext );
        }

        @Override
        public void start( FreeIds freeIdsForRebuild, CursorContext cursorContext ) throws IOException
        {
            delegate.start( freeIdsForRebuild, cursorContext );
        }

        @Override
        public void clearCache( CursorContext cursorContext )
        {
            delegate.clearCache( cursorContext );
        }

        @Override
        public boolean consistencyCheck( ReporterFactory reporterFactory, CursorContext cursorContext )
        {
            return delegate.consistencyCheck( reporterFactory, cursorContext );
        }
    }

    Marker NOOP_MARKER = new Marker()
    {
        @Override
        public void markFree( long id, int numberOfIds )
        {   // no-op
        }

        @Override
        public void markUsed( long id, int numberOfIds )
        {   // no-op
        }

        @Override
        public void markDeleted( long id, int numberOfIds )
        {   // no-op
        }

        @Override
        public void close()
        {   // no-op
        }
    };
}
