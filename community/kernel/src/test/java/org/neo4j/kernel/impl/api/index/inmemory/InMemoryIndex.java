/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;

import static java.lang.Boolean.getBoolean;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

class InMemoryIndex
{
    private final InMemoryIndexImplementation indexData =
            getBoolean( "neo4j.index.in_memory.USE_HASH" ) ? new HashBasedIndex() : new ListBasedIndex();
    private InternalIndexState state = InternalIndexState.POPULATING;
    String failure;

    @Override
    public String toString()
    {
        if ( failure != null )
        {
            return String.format( "%s[failure=\"%s\"]%s", getClass().getSimpleName(), failure, indexData );
        }
        else
        {
            return String.format( "%s%s", getClass().getSimpleName(), indexData );
        }
    }

    final IndexPopulator getPopulator()
    {
        return new Populator();
    }

    final IndexAccessor getOnlineAccessor()
    {
        return new OnlineAccessor();
    }

    protected final PrimitiveLongIterator lookup( Object propertyValue )
    {
        return indexData.lookup( propertyValue );
    }

    protected void add( long nodeId, Object propertyValue, boolean applyIdempotently ) throws IndexEntryConflictException, IOException
    {
        indexData.add( nodeId, propertyValue, applyIdempotently );
    }

    protected void remove( long nodeId, Object propertyValue )
    {
        indexData.remove( nodeId, propertyValue );
    }

    InternalIndexState getState()
    {
        return state;
    }

    private class Populator implements IndexPopulator
    {
        @Override
        public void create()
        {
            indexData.clear();
        }

        @Override
        public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException
        {
            InMemoryIndex.this.add( nodeId, propertyValue, false );
        }

        @Override
        public IndexUpdater newPopulatingUpdater() throws IOException
        {
            return InMemoryIndex.this.newUpdater( IndexUpdateMode.ONLINE );
        }

        @Override
        public void drop() throws IOException
        {
            indexData.clear();
        }

        @Override
        public void close( boolean populationCompletedSuccessfully ) throws IOException
        {
            if ( populationCompletedSuccessfully )
            {
                state = InternalIndexState.ONLINE;
            }
        }

        @Override
        public void markAsFailed( String failureString )
        {
            failure = failureString;
            state = InternalIndexState.FAILED;
        }
    }

    private class OnlineAccessor implements IndexAccessor
    {
        @Override
        public void force() throws IOException
        {
        }

        @Override
        public void drop() throws IOException
        {
            indexData.clear();
        }

        @Override
        public IndexUpdater newUpdater( final IndexUpdateMode mode ) throws IOException
        {
            return InMemoryIndex.this.newUpdater( mode );
        }

        @Override
        public void close() throws IOException
        {
        }

        @Override
        public IndexReader newReader()
        {
            return indexData;
        }

        @Override
        public ResourceIterator<File> snapshotFiles()
        {
            return emptyIterator();
        }
    }

    protected IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return new InMemoryIndexUpdater( mode );
    }

    private class InMemoryIndexUpdater implements IndexUpdater
    {

        private final boolean applyIdempotently;

        private InMemoryIndexUpdater( IndexUpdateMode mode )
        {
            this.applyIdempotently = IndexUpdateMode.ONLINE == mode;
        }

        @Override
        public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
        {
            switch ( update.getUpdateMode() )
            {
                case ADDED:
                    add( update.getNodeId(), update.getValueAfter(), applyIdempotently );
                    break;
                case CHANGED:
                    remove( update.getNodeId(), update.getValueBefore() );
                    add( update.getNodeId(), update.getValueAfter(), applyIdempotently );
                    break;
                case REMOVED:
                    remove( update.getNodeId(), update.getValueBefore() );
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public void close() throws IOException, IndexEntryConflictException
        {
        }
    }
}
