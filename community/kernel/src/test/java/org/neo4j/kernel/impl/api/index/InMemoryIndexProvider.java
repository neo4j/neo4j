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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

@Service.Implementation( InMemoryIndexProvider.class )
public class InMemoryIndexProvider extends SchemaIndexProvider
{
    private final Map<Long, IndexWriter> writers = new CopyOnWriteHashMap<Long, IndexWriter>();

    public InMemoryIndexProvider()
    {
        super("in-memory");
    }

    @Override
    public IndexWriter getOnlineWriter( long indexId )
    {
        IndexWriter populator = new InMemoryIndexWriter();
        writers.put( indexId, populator );
        return populator;
    }

    @Override
    public IndexWriter getPopulatingWriter( long indexId )
    {
        return getOnlineWriter( indexId );
    }

    private static class InMemoryIndexWriter implements IndexWriter
    {
        private final Map<Object, Set<Long>> indexData = new HashMap<Object, Set<Long>>();

        @Override
        public void add( long nodeId, Object propertyValue )
        {
            Set<Long> nodes = getLongs( propertyValue );
            nodes.add( nodeId );
        }

        @Override
        public void remove( long nodeId, Object propertyValue )
        {
            Collection<Long> nodes = indexData.get( propertyValue );
            nodes.remove( nodeId );
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
        public void createIndex()
        {
            indexData.clear();
        }

        @Override
        public void dropIndex()
        {
            throw new UnsupportedOperationException(  );
        }

        @Override
        public void force()
        {
        }
    }
}
