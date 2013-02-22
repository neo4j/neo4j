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

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.api.IndexPopulatorMapper;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

public class InMemoryIndexPopMapper implements IndexPopulatorMapper
{
    private final Map<IndexDefinition, IndexPopulator> populators = new CopyOnWriteHashMap<IndexDefinition, IndexPopulator>();
    
    @Override
    public IndexPopulator getPopulator( IndexDefinition index )
    {
        IndexPopulator populator = new InMemoryIndexPopulator();
        populators.put( index, populator );
        return populator;
    }
    
    private static class InMemoryIndexPopulator implements IndexPopulator
    {
        private final Map<Object, Set<Long>> indexData = new HashMap<Object, Set<Long>>();

        @Override
        public void add( int n, long nodeId, Object propertyValue )
        {
            Set<Long> nodes = getLongs( propertyValue );
            nodes.add( nodeId );
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
        public void remove( int n, long nodeId, Object propertyValue )
        {
            Collection<Long> nodes = indexData.get( propertyValue );
            nodes.remove( nodeId );
        }

        @Override
        public void done()
        {
        }
    }
}
