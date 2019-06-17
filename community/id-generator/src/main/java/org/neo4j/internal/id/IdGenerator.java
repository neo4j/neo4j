/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import java.util.function.Supplier;

import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.io.pagecache.IOLimiter;

public interface IdGenerator extends IdSequence, Closeable
{

    /**
     * @param id the highest in use + 1
     */
    void setHighId( long id );
    long getHighId();
    long getHighestPossibleIdInUse();
    void freeId( long id );
    void deleteId( long id );
    void markIdAsUsed( long id );
    ReuseMarker reuseMarker();
    CommitMarker commitMarker();

    @Override
    void close();
    long getNumberOfIdsInUse();
    long getDefragCount();

    void checkpoint( IOLimiter ioLimiter );

    /**
     * Does some maintenance. This operation isn't critical for the functionality of an IdGenerator, but may make it perform better.
     * The work happening inside this method should be work that would otherwise happen now and then inside the other methods anyway,
     * but letting a maintenance thread calling it may take some burden off of main request threads.
     */
    void maintenance();

    /**
     * Starts the id generator, signaling that the database has entered normal operations mode.
     * Updates to this id generator may have come in before this call and those operations must be treated
     * as recovery operations.
     * @param freeIdsForRebuild access to stream of ids from the store to use if this id generator needs to be rebuilt when started
     */
    void start( FreeIds freeIdsForRebuild ) throws IOException;

    interface CommitMarker extends AutoCloseable
    {
        void markUsed( long id );
        void markDeleted( long id );
        @Override
        void close();
    }

    interface ReuseMarker extends AutoCloseable
    {
        void markFree( long id );
        void markReserved( long id );
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
        public long nextId()
        {
            return delegate.nextId();
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            return delegate.nextIdBatch( size );
        }

        @Override
        public void setHighId( long id )
        {
            delegate.setHighId( id );
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
        public ReuseMarker reuseMarker()
        {
            return delegate.reuseMarker();
        }

        @Override
        public CommitMarker commitMarker()
        {
            return delegate.commitMarker();
        }

        @Override
        public void markIdAsUsed( long id )
        {
            delegate.markIdAsUsed( id );
        }

        @Override
        public void deleteId( long id )
        {
            delegate.deleteId( id );
        }

        @Override
        public void freeId( long id )
        {
            delegate.freeId( id );
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
        public void checkpoint( IOLimiter ioLimiter )
        {
            delegate.checkpoint( ioLimiter );
        }

        @Override
        public void maintenance()
        {
            delegate.maintenance();
        }

        @Override
        public void start( FreeIds freeIdsForRebuild ) throws IOException
        {
            delegate.start( freeIdsForRebuild );
        }
    }
}
