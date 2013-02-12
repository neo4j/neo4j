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
import java.util.Map;

public class TxState
{
    // Node ID --> NodeState
    private final Map<Long, NodeState> nodeStates = new HashMap<Long, NodeState>();
    
    // Label ID --> LabelState
    private final Map<Long, LabelState> labelStates = new HashMap<Long, LabelState>();
    
    public boolean hasChanges()
    {
        return !nodeStates.isEmpty() || !labelStates.isEmpty();
    }
    
    public Iterable<NodeState> getNodeStates()
    {
        return nodeStates.values();
    }
    
    public DiffSets<Long> getLabelStateNodeDiffSets( long labelId )
    {
        return getState( labelStates, labelId, LABEL_STATE_CREATOR ).getNodeDiffSets();
    }

    public DiffSets<Long> getNodeStateLabelDiffSets( long nodeId )
    {
        return getState( nodeStates, nodeId, NODE_STATE_CREATOR ).getLabelDiffSets();
    }
    
    public boolean addLabelToNode( long labelId, long nodeId )
    {
        getLabelStateNodeDiffSets( labelId ).add( nodeId );
        return getNodeStateLabelDiffSets( nodeId ).add( labelId );
    }
    
    public boolean removeLabelFromNode( long labelId, long nodeId )
    {
        getLabelStateNodeDiffSets( labelId ).remove( nodeId );
        return getNodeStateLabelDiffSets( nodeId ).remove(  labelId );
    }
    
    /**
     * @return
     *      {@code true} if it has been added in this transaction.
     *      {@code false} if it has been removed in this transaction.
     *      {@code null} if it has not been touched in this transaction.
     */
    public Boolean getLabelState( long nodeId, long labelId )
    {
        NodeState nodeState = getState( nodeStates, nodeId, null );
        if ( nodeState != null )
        {
            DiffSets<Long> labelDiff = nodeState.getLabelDiffSets();
            if ( labelDiff.isAdded( labelId ) )
                return Boolean.TRUE;
            if ( labelDiff.isRemoved( labelId ) )
                return Boolean.FALSE;
        }
        return null;
    }
    
    /**
     * Returns all nodes that, in this tx, has got labelId added.
     */
    public Iterable<Long> getAddedNodesWithLabel( long labelId )
    {
        LabelState state = getState( labelStates, labelId, null );
        return state == null ? Collections.<Long>emptySet() : state.getNodeDiffSets().getAdded();
    }

    /**
     * Returns all nodes that, in this tx, has got labelId removed.
     */
    public Collection<Long> getRemovedNodesWithLabel( long labelId )
    {
        LabelState state = getState( labelStates, labelId, null );
        return state == null ? Collections.<Long>emptySet() : state.getNodeDiffSets().getRemoved();
    }

    public void addIndexRule( long labelId, long propertyKey )
    {
        LabelState labelState = getState( labelStates, labelId, LABEL_STATE_CREATOR );
        labelState.getIndexRuleDiffSets().add( propertyKey );
    }

    public void removeIndexRule( long labelId, long propertyKey )
    {
        LabelState labelState = getState( labelStates, labelId, LABEL_STATE_CREATOR );
        labelState.getIndexRuleDiffSets().remove( propertyKey );
    }

    public Iterable<LabelState> getLabelStates()
    {
        return labelStates.values();
    }
    
    public DiffSets<Long> getIndexRuleDiffSets( long labelId ) {
        LabelState state = getState( labelStates, labelId, null );
        return state == null ? DiffSets.<Long>emptyDiffSets() : state.getIndexRuleDiffSets();
    }

    public boolean hasAddedIndexRule( long labelId, long propertyKey )
    {
        LabelState state = getState( labelStates, labelId, null );
        return state != null ? state.hasAddedIndexRule( propertyKey ) : false;
    }

    public Map<Long,Object> getAddedNodeProperties( long nodeId )
    {
        throw new UnsupportedOperationException();
    }

    public Map<Long,Boolean> getRemovedNodeProperties( long nodeId )
    {
        throw new UnsupportedOperationException();
    }

    public void setNodeProperty( long nodeId, long propertyKeyId, Object value )
    {
        throw new UnsupportedOperationException();
    }

    public void removeNodeProperty( long nodeId, long propertyKeyId )
    {
        throw new UnsupportedOperationException();
    }

    private static interface StateCreator<STATE>
    {
        STATE newState( long id );
    }
    
    private <STATE> STATE getState( Map<Long,STATE> states, long id, StateCreator<STATE> creator )
    {
        STATE result = states.get( id );
        if ( result != null )
            return result;
        
        if ( creator != null )
        {
            result = creator.newState( id );
            states.put( id, result );
        }
        return result;
    }
    
    private static final StateCreator<NodeState> NODE_STATE_CREATOR = new StateCreator<NodeState>()
    {
        @Override
        public NodeState newState( long id )
        {
            return new NodeState( id );
        }
    };
    
    private static final StateCreator<LabelState> LABEL_STATE_CREATOR = new StateCreator<LabelState>()
    {
        @Override
        public LabelState newState( long id )
        {
            return new LabelState( id );
        }
    };
    
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

        private final DiffSets<Long> labelDiffSets = new DiffSets<Long>();

        public DiffSets<Long> getLabelDiffSets()
        {
            return labelDiffSets;
        }
    }
    
    public static class LabelState
    {
        private final long id;
        private final DiffSets<Long> nodeDiffSets = new DiffSets<Long>();
        private final DiffSets<Long> indexRuleDiffSets = new DiffSets<Long>();

        LabelState( long id )
        {
            this.id = id;
        }
        
        public long getId()
        {
            return id;
        }
        
        public DiffSets<Long> getNodeDiffSets()
        {
            return nodeDiffSets;
        }
        
        public DiffSets<Long> getIndexRuleDiffSets()
        {
            return indexRuleDiffSets;
        }

        public boolean hasAddedIndexRule( long propertyKey )
        {
            for ( long addedKey : indexRuleDiffSets.getAdded() )
            {
                if ( addedKey == propertyKey )
                {
                    return true;
                }
            }

            return false;
        }
    }
}
