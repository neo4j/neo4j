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
package org.neo4j.kernel.impl.api.state;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class TxState
{
    // Node ID --> NodeState
    private final Map<Long, NodeState> nodeStates = new HashMap<Long, NodeState>();
    
    // Label ID --> LabelState
    private final Map<Long, LabelState> labelStates = new HashMap<Long, LabelState>();

    // Index rules
    private final DiffSets<IndexRule> indexRuleStates = new DiffSets<IndexRule>();

    private final OldTxStateBridge legacyState;

    public TxState(OldTxStateBridge legacyState)
    {
        this.legacyState = legacyState;
    }

    public boolean hasChanges()
    {
        return !(nodeStates.isEmpty() && labelStates.isEmpty() && indexRuleStates.isEmpty());
    }
    
    public Iterable<NodeState> getNodeStates()
    {
        return nodeStates.values();
    }

    public DiffSets<Long> getNodeStateLabelDiffSets( long nodeId )
    {
        return getState( nodeStates, nodeId, NodeState.FACTORY ).getLabelDiffSets();
    }
    
    public boolean addLabelToNode( long labelId, long nodeId )
    {
        getNodesAddedAndRemovedForLabel( labelId ).add( nodeId );
        return getNodeStateLabelDiffSets( nodeId ).add( labelId );
    }
    
    public boolean removeLabelFromNode( long labelId, long nodeId )
    {
        getNodesAddedAndRemovedForLabel( labelId ).remove( nodeId );
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
     * Returns all nodes that, in this tx, has had some specific label added.
     */
    public Iterable<Long> getNodesWithLabelAdded( long labelId )
    {
        LabelState state = getState( labelStates, labelId, null );
        return state == null ? Collections.<Long>emptySet() : state.getNodeDiffSets().getAdded();
    }

    /**
     * Returns all nodes that, in this tx, has had some specific label removed.
     */
    public Collection<Long> getNodesWithLabelRemoved( long labelId )
    {
        LabelState state = getState( labelStates, labelId, null );
        return state == null ? Collections.<Long>emptySet() : state.getNodeDiffSets().getRemoved();
    }

    public void addIndexRule( IndexRule rule )
    {
        indexRuleStates.add( rule );
        LabelState labelState = getState( labelStates, rule.getLabel(), LabelState.FACTORY );
        labelState.getIndexRuleDiffSets().add( rule );
    }

    public void removeIndexRule( IndexRule rule )
    {
        indexRuleStates.remove( rule );
        LabelState labelState = getState( labelStates, rule.getLabel(), LabelState.FACTORY );
        labelState.getIndexRuleDiffSets().remove( rule );
    }
    
    public DiffSets<IndexRule> getIndexesAddedAndRemovedForLabel( long labelId )
    {
        LabelState labelState = getState( labelStates, labelId, null );
        return labelState != null ? labelState.getIndexRuleDiffSets() : DiffSets.<IndexRule>emptyDiffSets();
    }

    public DiffSets<IndexRule> getIndexRuleDiffSets()
    {
        return indexRuleStates;
    }

    public DiffSets<Long> getNodesWithChangedProperty( long propertyKeyId, Object value )
    {
        return legacyState.getNodesWithChangedProperty( propertyKeyId, value );
    }

    public Iterable<Long> getDeletedNodes() {
        return legacyState.getDeletedNodes();
    }

    private DiffSets<Long> getNodesAddedAndRemovedForLabel( long labelId )
    {
        return getState( labelStates, labelId, LabelState.FACTORY ).getNodeDiffSets();
    }

    private <STATE> STATE getState( Map<Long,STATE> states, long id, StateFactory<STATE> creator )
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

}
