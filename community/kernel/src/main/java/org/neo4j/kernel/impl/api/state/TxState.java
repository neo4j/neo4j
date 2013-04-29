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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

/**
 * This organizes three disjoint containers of state. The goal is to bring that down to one, but for now, it's three.
 * Those three are:
 *
 *  * TxState - this class itself, containing HashMaps and DiffSets for changes
 *  * TransactionState - The legacy transaction state, to be refactored into this class.
 *  * WriteTransaction - More legacy transaction state, accessed through PersistenceManager.
 *
 * TransactionState is used to change the view of the data within a transaction, eg. see your own writes.
 *
 * WriteTransaction contains the changes that will actually be applied to the store, eg. records.
 *
 * TxState should be a common interface for *updating* both kinds of state, and for *reading* the first kind.
 *
 * So, in ascii art, the current implementation is:
 *
 *      StateHandlingTransactionContext-------TransactionStateStatementContext
 *                   \                                      /
 *                    ---------------------|----------------
 *                                         |
 *                                      TxState
 *                                     /      \
 *                       PersistenceManager   TransactionState
 *
 *
 * We want it to look like:
 *
 *      StateHandlingTransactionContext-------TransactionStateStatementContext
 *                   \                                      /
 *                    ---------------------|----------------
 *                                         |
 *                                      TxState
 *
 *
 * Where, in the end implementation, the state inside TxState can be used both to overlay on the graph, eg. read writes,
 * as well as be applied to the graph through the logical log.
 */
public class TxState
{
    public interface IdGeneration
    {
        long newNodeId();

        long newRelationshipId();
    }

    public interface Visitor
    {
        void visitNodeLabelChanges( long id, Set<Long> added, Set<Long> removed );

        void visitAddedIndex( IndexDescriptor element );

        void visitRemovedIndex( IndexDescriptor element );

        void visitAddedConstraint( UniquenessConstraint element );

        void visitRemovedConstraint( UniquenessConstraint element );
    }

    private final Map<Long/*Node ID*/, NodeState> nodeStates = new HashMap<Long, NodeState>();
    private final Map<Long/*Label ID*/, LabelState> labelStates = new HashMap<Long, LabelState>();
    private final DiffSets<IndexDescriptor> indexChanges = new DiffSets<IndexDescriptor>();
    private final DiffSets<UniquenessConstraint> constraintsChanges = new DiffSets<UniquenessConstraint>();
    private final DiffSets<Long> nodes = new DiffSets<Long>();

    private final OldTxStateBridge legacyState;
    private final PersistenceManager persistenceManager;
    private final IdGeneration idGeneration;

    public TxState( OldTxStateBridge legacyState,
                    PersistenceManager legacyTransaction,
                    IdGeneration idGeneration )
    {
        this.legacyState = legacyState;
        this.persistenceManager = legacyTransaction;
        this.idGeneration = idGeneration;
    }

    public void accept( final Visitor visitor )
    {
        for ( NodeState node : nodeStates.values() )
        {
            DiffSets<Long> labelDiff = node.getLabelDiffSets();
            visitor.visitNodeLabelChanges( node.getId(), labelDiff.getAdded(), labelDiff.getRemoved() );
        }
        indexChanges.accept( new DiffSets.Visitor<IndexDescriptor>()
        {
            @Override
            public void visitAdded( IndexDescriptor element )
            {
                visitor.visitAddedIndex( element );
            }

            @Override
            public void visitRemoved( IndexDescriptor element )
            {
                visitor.visitRemovedIndex( element );
            }
        } );
        constraintsChanges.accept( new DiffSets.Visitor<UniquenessConstraint>()
        {
            @Override
            public void visitAdded( UniquenessConstraint element )
            {
                visitor.visitAddedConstraint( element );
            }

            @Override
            public void visitRemoved( UniquenessConstraint element )
            {
                visitor.visitRemovedConstraint( element );
            }
        } );
    }

    public boolean hasChanges()
    {
        return !nodeStates.isEmpty() ||
               !labelStates.isEmpty() ||
               !nodes.isEmpty() ||
               !indexChanges.isEmpty() ||
               !constraintsChanges.isEmpty() ||
               legacyState.hasChanges();
    }

    public Iterable<NodeState> getNodeStates()
    {
        return nodeStates.values();
    }

    public DiffSets<Long> getLabelStateNodeDiffSets( long labelId )
    {
        return getOrCreateLabelState( labelId ).getNodeDiffSets();
    }

    public DiffSets<Long> getNodeStateLabelDiffSets( long nodeId )
    {
        return getOrCreateNodeState( nodeId ).getLabelDiffSets();
    }

    public void deleteNode( long nodeId )
    {
        legacyState.deleteNode( nodeId );
        nodes.remove( nodeId );
    }

    public boolean nodeIsDeletedInThisTx( long nodeId )
    {
        return nodes.isRemoved( nodeId );
    }

    public boolean nodeIsAddedInThisTx( long nodeId )
    {
        return legacyState.nodeIsAddedInThisTx( nodeId );
    }

    public void addLabelToNode( long labelId, long nodeId )
    {
        getLabelStateNodeDiffSets( labelId ).add( nodeId );
        getNodeStateLabelDiffSets( nodeId ).add( labelId );
        persistenceManager.addLabelToNode( labelId, nodeId );
    }

    public void removeLabelFromNode( long labelId, long nodeId )
    {
        getLabelStateNodeDiffSets( labelId ).remove( nodeId );
        getNodeStateLabelDiffSets( nodeId ).remove( labelId );
        persistenceManager.removeLabelFromNode( labelId, nodeId );
    }

    /**
     * @return {@code true} if it has been added in this transaction.
     *         {@code false} if it has been removed in this transaction.
     *         {@code null} if it has not been touched in this transaction.
     */
    public Boolean getLabelState( long nodeId, long labelId )
    {
        NodeState nodeState = getState( nodeStates, nodeId, null );
        if ( nodeState != null )
        {
            DiffSets<Long> labelDiff = nodeState.getLabelDiffSets();
            if ( labelDiff.isAdded( labelId ) )
            {
                return Boolean.TRUE;
            }
            if ( labelDiff.isRemoved( labelId ) )
            {
                return Boolean.FALSE;
            }
        }
        return null;
    }

    /**
     * Returns all nodes that, in this tx, has got labelId added.
     */
    public Set<Long> getNodesWithLabelAdded( long labelId )
    {
        LabelState state = getState( labelStates, labelId, null );
        return state == null ? Collections.<Long>emptySet() : state.getNodeDiffSets().getAdded();
    }

    /**
     * Returns all nodes that, in this tx, has got labelId removed.
     */
    public DiffSets<Long> getNodesWithLabelChanged( long labelId )
    {
        LabelState state = getState( labelStates, labelId, null );
        return state == null ? DiffSets.<Long>emptyDiffSets() : state.getNodeDiffSets();
    }

    public void addIndexRule( IndexDescriptor rule )
    {
        indexChanges.add( rule );
        getOrCreateLabelState( rule.getLabelId() ).indexChanges().add( rule );
    }

    public void dropIndexRule( IndexDescriptor rule )
    {
        indexChanges.remove( rule );
        getOrCreateLabelState( rule.getLabelId() ).indexChanges().remove( rule );
    }

    public DiffSets<IndexDescriptor> getIndexRuleDiffSetsByLabel( long labelId )
    {
        LabelState labelState = getState( labelStates, labelId, null );
        return labelState != null ? labelState.indexChanges() : DiffSets.<IndexDescriptor>emptyDiffSets();
    }

    public DiffSets<IndexDescriptor> getIndexRuleDiffSets()
    {
        return indexChanges;
    }

    public DiffSets<Long> getNodesWithChangedProperty( long propertyKeyId, Object value )
    {
        return legacyState.getNodesWithChangedProperty( propertyKeyId, value );
    }

    public DiffSets<Long> getDeletedNodes()
    {
        return nodes;
    }

    private LabelState getOrCreateLabelState( long labelId )
    {
        return getState( labelStates, labelId, new StateCreator<LabelState>()
        {
            @Override
            public LabelState newState( long id )
            {
                return new LabelState( id );
            }
        } );
    }

    private NodeState getOrCreateNodeState( long nodeId )
    {
        return getState( nodeStates, nodeId, new StateCreator<NodeState>()
        {
            @Override
            public NodeState newState( long id )
            {
                return new NodeState( id );
            }
        } );
    }

    private interface StateCreator<STATE>
    {
        STATE newState( long id );
    }

    private <STATE> STATE getState( Map<Long, STATE> states, long id, StateCreator<STATE> creator )
    {
        STATE result = states.get( id );
        if ( result != null )
        {
            return result;
        }

        if ( creator != null )
        {
            result = creator.newState( id );
            states.put( id, result );
        }
        return result;
    }

    public void addConstraint( UniquenessConstraint constraint )
    {
        constraintsChanges.add( constraint );
        getOrCreateLabelState( constraint.label() ).constraintsChanges().add( constraint );
    }

    public DiffSets<UniquenessConstraint> constraintsForLabel( long labelId )
    {
        return getOrCreateLabelState( labelId ).constraintsChanges();
    }

    public void dropConstraint( UniquenessConstraint constraint )
    {
        constraintsChanges.remove( constraint );
        constraintsForLabel( constraint.label() ).remove( constraint );
    }

    public boolean unRemoveConstraint( UniquenessConstraint constraint )
    {
        return constraintsChanges.unRemove( constraint );
    }
}
