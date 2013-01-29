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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.Pair;

public class TxState
{
    // Node ID --> NodeState
    private final Map<Long, NodeState> nodeStates = new HashMap<Long, NodeState>();
    
    // Label ID --> Node IDs (added, removed)
    private final Map<Long, Pair<Collection<Long>,Collection<Long>>> labels =
            new HashMap<Long, Pair<Collection<Long>,Collection<Long>>>();
    
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
    
    public boolean addLabelToNode( long labelId, long nodeId )
    {
        Pair<Collection<Long>, Collection<Long>> nodeLabels = getNodeLabels( labelId );
        nodeLabels.first().add( nodeId );
        nodeLabels.other().remove( nodeId );
        
        NodeState nodeState = getNodeState( nodeId, true );
        nodeState.getRemovedLabels().remove( labelId );
        return nodeState.getAddedLabels().add( labelId );
    }
    
    public boolean removeLabelFromNode( long labelId, long nodeId )
    {
        Pair<Collection<Long>, Collection<Long>> nodeLabels = getNodeLabels( labelId );
        nodeLabels.first().remove( nodeId );
        nodeLabels.other().add( nodeId );
        
        NodeState nodeState = getNodeState( nodeId, true );
        nodeState.getAddedLabels().remove( labelId );
        return nodeState.getRemovedLabels().add( labelId );
    }
    
    private Pair<Collection<Long>, Collection<Long>> getNodeLabels( long labelId )
    {
        Pair<Collection<Long>, Collection<Long>> labelNodes = labels.get( labelId );
        if ( labelNodes == null )
        {
            labelNodes = Pair.<Collection<Long>,Collection<Long>>of( new HashSet<Long>(), new HashSet<Long>() );
            labels.put( labelId, labelNodes );
        }
        return labelNodes;
    }

    /**
     * @return
     *      {@code true} if it has been added in this transaction.
     *      {@code false} if it has been removed in this transaction.
     *      {@code null} if it has not been touched in this transaction.
     */
    public Boolean getLabelState( long nodeId, long labelId )
    {
        NodeState nodeState = getNodeState( nodeId, false );
        if ( nodeState != null )
        {
            if ( nodeState.getAddedLabels().contains( labelId ) )
                return Boolean.TRUE;
            if ( nodeState.getRemovedLabels().contains( labelId ) )
                return Boolean.FALSE;
        }
        return null;
    }
    
    public Collection<Long> getAddedLabels( long nodeId )
    {
        NodeState nodeState = getNodeState( nodeId, false );
        return nodeState != null ? nodeState.getAddedLabels() : Collections.<Long>emptySet();
    }
    
    public Collection<Long> getRemovedLabels( long nodeId )
    {
        NodeState nodeState = getNodeState( nodeId, false );
        return nodeState != null ? nodeState.getRemovedLabels() : Collections.<Long>emptySet();
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
        private final Set<Long> removedLabels = new HashSet<Long>();

        public Set<Long> getAddedLabels()
        {
            return addedLabels;
        }

        public Set<Long> getRemovedLabels()
        {
            return removedLabels;
        }
    }

    /**
     * Returns all nodes that, in this tx, has got labelId added.
     */
    public Iterable<Long> getAddedNodesWithLabel( long labelId )
    {
        Pair<Collection<Long>, Collection<Long>> nodeLabels = labels.get( labelId );
        return nodeLabels != null ? nodeLabels.first() : Collections.<Long>emptyList();
    }

    /**
     * Returns all nodes that, in this tx, has got labelId removed.
     */
    public Collection<Long> getRemovedNodesWithLabel( long labelId )
    {
        Pair<Collection<Long>, Collection<Long>> nodeLabels = labels.get( labelId );
        return nodeLabels != null ? nodeLabels.other() : Collections.<Long>emptyList();
    }
}
