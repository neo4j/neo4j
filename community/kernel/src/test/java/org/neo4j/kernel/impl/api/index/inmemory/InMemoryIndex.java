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

import java.io.IOException;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

abstract class InMemoryIndex
{
    private InternalIndexState state = InternalIndexState.POPULATING;
    String failure;

    abstract IndexPopulator getPopulator();

    abstract IndexAccessor getOnlineAccessor();

    abstract void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException;

    abstract void remove( long nodeId, Object propertyValue );

    protected abstract class Populator implements IndexPopulator
    {
        @Override
        public void create()
        {
            clear();
        }

        @Override
        public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException
        {
            InMemoryIndex.this.add( nodeId, propertyValue );
        }

        @Override
        public void update( Iterable<NodePropertyUpdate> updates ) throws IndexEntryConflictException, IOException
        {
            InMemoryIndex.this.update( updates );
        }

        @Override
        public void drop() throws IOException
        {
            InMemoryIndex.this.drop();
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

    protected abstract class OnlineAccessor implements IndexAccessor
    {
        @Override
        public void recover( Iterable<NodePropertyUpdate> updates ) throws IOException
        {
            InMemoryIndex.this.recover( updates );
        }

        @Override
        public void updateAndCommit( Iterable<NodePropertyUpdate> updates )
                throws IOException, IndexEntryConflictException
        {
            InMemoryIndex.this.update( updates );
        }

        @Override
        public void force() throws IOException
        {
        }

        @Override
        public void drop() throws IOException
        {
            InMemoryIndex.this.drop();
        }

        @Override
        public void close() throws IOException
        {
        }
    }

    protected void update( Iterable<NodePropertyUpdate> updates ) throws IndexEntryConflictException, IOException
    {
        for ( NodePropertyUpdate update : updates )
        {
            switch ( update.getUpdateMode() )
            {
                case ADDED:
                    add( update.getNodeId(), update.getValueAfter() );
                    break;
                case CHANGED:
                    remove( update.getNodeId(), update.getValueBefore() );
                    add( update.getNodeId(), update.getValueAfter() );
                    break;
                case REMOVED:
                    remove( update.getNodeId(), update.getValueBefore() );
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }


    protected void drop()
    {
        clear();
    }

    protected void recover( Iterable<NodePropertyUpdate> updates ) throws IOException
    {
        try
        {
            update( updates );
        }
        catch ( IndexEntryConflictException e )
        {
            throw new IllegalStateException( "Should not report index entry conflicts during recovery!", e );
        }
    }

    abstract void clear();

    InternalIndexState getState()
    {
        return state;
    }
}
