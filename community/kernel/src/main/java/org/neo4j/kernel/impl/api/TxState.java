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
package org.neo4j.kernel.impl.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TxState
{
    private final Map<Long, NodeState> nodeStates = new HashMap<Long, NodeState>();
    
    public boolean hasChanges()
    {
        return !nodeStates.isEmpty();
    }
    
    public Iterable<NodeState> getNodes()
    {
        return nodeStates.values();
    }
    
    private NodeState getNodeState( long nodeId, boolean create )
    {
        NodeState result = nodeStates.get( nodeId );
        if ( result != null )
            return result;
        
        if ( create )
        {
            result = new NodeState( nodeId );
            nodeStates.put( nodeId, result );
        }
        return result;
    }
    
    public Set<Long> getAddedLabels( long nodeId, boolean create )
    {
        return getNodeState( nodeId, create ).getAddedLabels();
    }
    
    static class EntityState
    {
        private final long id;
        
        EntityState( long id )
        {
            this.id = id;
        }

        public long getId()
        {
            return id;
        }
    }
    
    public static class NodeState extends EntityState
    {
        NodeState( long id )
        {
            super( id );
        }

        private final Set<Long> addedLabels = new HashSet<Long>();
        
        public Set<Long> getAddedLabels()
        {
            return addedLabels;
        }
    }
}
