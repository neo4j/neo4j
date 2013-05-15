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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

public class InMemoryIndexProvider extends SchemaIndexProvider
{
    private final Map<Long, InMemoryIndex> indexes = new CopyOnWriteHashMap<Long, InMemoryIndex>();

    public InMemoryIndexProvider()
    {
        super( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR, 0 );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config )
    {
        InMemoryIndex index = indexes.get( indexId );
        if ( index == null || index.state != InternalIndexState.ONLINE )
        {
            throw new IllegalStateException( "Index " + indexId + " not online yet" );
        }
        if ( config.isUnique() && !(index instanceof UniqueInMemoryIndex) )
        {
            throw new IllegalStateException( String.format( "The index [%s] was not created as a unique index.",
                                                            indexId ) );
        }
        return index;
    }

    @Override
    public InternalIndexState getInitialState( long indexId )
    {
        InMemoryIndex index = indexes.get( indexId );
        return index != null ? index.state : InternalIndexState.POPULATING;
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexConfiguration config )
    {
        InMemoryIndex populator = config.isUnique() ? new UniqueInMemoryIndex() : new MultiValueInMemoryIndex();
        indexes.put( indexId, populator );
        return populator;
    }

    private static abstract class InMemoryIndex extends IndexAccessor.Adapter implements IndexPopulator
    {
        private InternalIndexState state = InternalIndexState.POPULATING;

        abstract void remove( long nodeId, Object propertyValue );

        @Override
        public void update( Iterable<NodePropertyUpdate> updates ) throws IndexEntryConflictException, IOException
        {
            // TODO: unique indexes need the updates ordered... removes before adds.
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

        @Override
        public void updateAndCommit( Iterable<NodePropertyUpdate> updates ) throws IndexEntryConflictException,
                IOException
        {
            update( updates );
        }

        @Override
        public void recover( Iterable<NodePropertyUpdate> updates ) throws IOException
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

        @Override
        public void force()
        {
        }

        @Override
        public void create()
        {
            clear();
        }

        @Override
        public void drop()
        {
            clear();
        }

        abstract void clear();

        @Override
        public void close( boolean populationCompletedSuccessfully )
        {
            if ( populationCompletedSuccessfully )
            {
                state = InternalIndexState.ONLINE;
            }
        }

        @Override
        public void close()
        {
        }
    }

    private static class UniqueInMemoryIndex extends InMemoryIndex
    {
        private final Map<Object, Long> indexData = new HashMap<Object, Long>();

        @Override
        public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException
        {
            Long previous = indexData.get( propertyValue );
            if ( previous != null )
            {
                throw new IndexEntryConflictException( nodeId, propertyValue, previous );
            }
            indexData.put( propertyValue, nodeId );
        }

        @Override
        void remove( long nodeId, Object propertyValue )
        {
            indexData.remove( propertyValue );
        }

        @Override
        void clear()
        {
            indexData.clear();
        }

        @Override
        public IndexReader newReader()
        {
            return new SingleValueReader( indexData );
        }
    }

    private static class MultiValueInMemoryIndex extends InMemoryIndex
    {
        private final Map<Object, Set<Long>> indexData = new HashMap<Object, Set<Long>>();

        @Override
        public void add( long nodeId, Object propertyValue )
        {
            Set<Long> nodes = getLongs( propertyValue );
            nodes.add( nodeId );
        }

        @Override
        void remove( long nodeId, Object propertyValue )
        {
            Set<Long> nodes = getLongs( propertyValue );
            nodes.remove( nodeId );
        }

        @Override
        void clear()
        {
            indexData.clear();
        }

        private Set<Long> getLongs( Object propertyValue )
        {
            Set<Long> nodes = indexData.get( propertyValue );
            if ( nodes == null )
            {
                nodes = new HashSet<Long>();
                indexData.put( propertyValue, nodes );
            }
            return nodes;
        }

        @Override
        public IndexReader newReader()
        {
            return new MultiValueReader( indexData );
        }
    }

    private static class MultiValueReader implements IndexReader
    {
        private final HashMap<Object, Set<Long>> indexData;

        MultiValueReader( Map<Object, Set<Long>> indexData )
        {
            this.indexData = new HashMap<Object, Set<Long>>( indexData );
        }

        @Override
        public Iterator<Long> lookup( Object value )
        {
            Set<Long> result = indexData.get( value );
            return result != null ? result.iterator() : IteratorUtil.<Long>emptyIterator();
        }

        @Override
        public void close()
        {
        }
    }

    private static class SingleValueReader implements IndexReader
    {
        private final HashMap<Object, Long> indexData;

        SingleValueReader( Map<Object, Long> indexData )
        {
            this.indexData = new HashMap<Object, Long>( indexData );
        }

        @Override
        public Iterator<Long> lookup( Object value )
        {
            Long result = indexData.get( value );
            return result != null ? IteratorUtil.singletonIterator( result ) : IteratorUtil.<Long>emptyIterator();
        }

        @Override
        public void close()
        {
        }
    }
}
