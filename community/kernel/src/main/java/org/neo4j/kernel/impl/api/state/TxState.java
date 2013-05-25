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

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static org.neo4j.helpers.collection.Iterables.map;

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
    /**
     * Ability to generate the leaking id types (node ids and relationship ids).
     */
    public interface IdGeneration
    {
        long newNodeId();

        long newRelationshipId();
    }

    public interface Visitor
    {
        void visitNodeLabelChanges( long id, Set<Long> added, Set<Long> removed );

        void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex );

        void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex );

        void visitAddedConstraint( UniquenessConstraint element, long indexId );

        void visitRemovedConstraint( UniquenessConstraint element );
    }

    private final Map<Long/*Node ID*/, NodeState> nodeStates = new HashMap<Long, NodeState>();
    private final Map<Long/*Relationship ID*/, RelationshipState> relationshipStates = new HashMap<Long, RelationshipState>();
    private final Map<Long/*Label ID*/, LabelState> labelStates = new HashMap<Long, LabelState>();
    private final DiffSets<IndexDescriptor> indexChanges = new DiffSets<IndexDescriptor>();
    private final DiffSets<IndexDescriptor> constraintIndexChanges = new DiffSets<IndexDescriptor>();
    private final DiffSets<UniquenessConstraint> constraintsChanges = new DiffSets<UniquenessConstraint>();
    private final DiffSets<Long> nodes = new DiffSets<Long>();
    private final DiffSets<Long> relationships = new DiffSets<Long>();
    private final Map<UniquenessConstraint, Long> createdConstraintIndexes = new HashMap<UniquenessConstraint, Long>();

    private final OldTxStateBridge legacyState;
    private final PersistenceManager persistenceManager; // should go away dammit!
    private final IdGeneration idGeneration; // needed when we move createNode() and createRelationship() to here...

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
        indexChanges.accept( indexVisitor( visitor, false ) );
        constraintIndexChanges.accept( indexVisitor( visitor, true ) );
        constraintsChanges.accept( new DiffSets.Visitor<UniquenessConstraint>()
        {
            @Override
            public void visitAdded( UniquenessConstraint element )
            {
                visitor.visitAddedConstraint( element, createdConstraintIndexes.get( element ) );
            }

            @Override
            public void visitRemoved( UniquenessConstraint element )
            {
                visitor.visitRemovedConstraint( element );
            }
        } );
    }

    private static DiffSets.Visitor<IndexDescriptor> indexVisitor( final Visitor visitor, final boolean forConstraint )
    {
        return new DiffSets.Visitor<IndexDescriptor>()
        {
            @Override
            public void visitAdded( IndexDescriptor element )
            {
                visitor.visitAddedIndex( element, forConstraint );
            }

            @Override
            public void visitRemoved( IndexDescriptor element )
            {
                visitor.visitRemovedIndex( element, forConstraint );
            }
        };
    }

    public boolean hasChanges()
    {
        return !nodeStates.isEmpty() ||
               !relationshipStates.isEmpty() ||
               !labelStates.isEmpty() ||
               !nodes.isEmpty() ||
               !relationships.isEmpty() ||
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

    public DiffSets<Property> getNodePropertyDiffSets( long nodeId )
    {
        return getOrCreateNodeState( nodeId ).getPropertyDiffSets();
    }

    public DiffSets<Property> getRelationshipPropertyDiffSets( long nodeId )
    {
        return getOrCreateRelationshipState( nodeId ).getPropertyDiffSets();
    }

    public boolean nodeIsAddedInThisTx( long nodeId )
    {
        return legacyState.nodeIsAddedInThisTx( nodeId );
    }

    public boolean relationshipIsAddedInThisTx( long relationshipId )
    {
        return legacyState.relationshipIsAddedInThisTx( relationshipId );
    }

    public void nodeDelete( long nodeId )
    {
        legacyState.deleteNode( nodeId );
        nodes.remove( nodeId );
    }

    public boolean nodeIsDeletedInThisTx( long nodeId )
    {
        return nodes.isRemoved( nodeId );
    }

    public void relationshipDelete( long relationshipId )
    {
        legacyState.deleteRelationship( relationshipId );
        relationships.remove( relationshipId );
    }

    public boolean relationshipIsDeletedInThisTx( long relationshipId )
    {
        return relationships.isRemoved( relationshipId );
    }

    public void nodeReplaceProperty( long nodeId, Property replacedProperty, Property newProperty )
            throws PropertyNotFoundException, EntityNotFoundException
    {
        if ( ! newProperty.isNoProperty() )
        {
            DiffSets<Property> diffSets = getNodePropertyDiffSets( nodeId );
            if ( ! replacedProperty.isNoProperty() )
            {
                diffSets.remove( replacedProperty );
            }
            diffSets.add( newProperty );
        }
    }

    public void relationshipReplaceProperty( long relationshipId, Property replacedProperty, Property newProperty )
            throws PropertyNotFoundException, EntityNotFoundException
    {
        if ( ! newProperty.isNoProperty() )
        {
            DiffSets<Property> diffSets = getRelationshipPropertyDiffSets( relationshipId );
            if ( ! replacedProperty.isNoProperty() )
            {
                diffSets.remove( replacedProperty );
            }
            diffSets.add( newProperty );
        }
    }

    public void nodeRemoveProperty( long nodeId, Property removedProperty )
            throws PropertyNotFoundException, EntityNotFoundException
    {
        if ( ! removedProperty.isNoProperty() )
        {
            getNodePropertyDiffSets( nodeId ).remove( removedProperty );
        }
    }

    public void relationshipRemoveProperty( long relationshipId, Property removedProperty )
            throws PropertyNotFoundException, EntityNotFoundException
    {
        if ( ! removedProperty.isNoProperty() )
        {
            getRelationshipPropertyDiffSets( relationshipId ).remove( removedProperty );
        }
    }

    public void nodeAddLabel( long labelId, long nodeId )
    {
        getLabelStateNodeDiffSets( labelId ).add( nodeId );
        getNodeStateLabelDiffSets( nodeId ).add( labelId );
        persistenceManager.addLabelToNode( labelId, nodeId );
    }

    public void nodeRemoveLabel( long labelId, long nodeId )
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

    public void addIndexRule( IndexDescriptor descriptor )
    {
        indexChanges.add( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).indexChanges().add( descriptor );
    }

    public void addConstraintIndexRule( IndexDescriptor descriptor )
    {
        constraintIndexChanges.add( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).constraintIndexChanges().add( descriptor );
    }

    public void dropIndex( IndexDescriptor descriptor )
    {
        indexChanges.remove( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).indexChanges().remove( descriptor );
    }

    public void dropConstraintIndex( IndexDescriptor descriptor )
    {
        constraintIndexChanges.remove( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).constraintIndexChanges().remove( descriptor );
    }

    public DiffSets<IndexDescriptor> getIndexDiffSetsByLabel( long labelId )
    {
        LabelState labelState = getState( labelStates, labelId, null );
        return labelState != null ? labelState.indexChanges() : DiffSets.<IndexDescriptor>emptyDiffSets();
    }

    public DiffSets<IndexDescriptor> getConstraintIndexDiffSetsByLabel( long labelId )
    {
        LabelState labelState = getState( labelStates, labelId, null );
        return labelState != null ? labelState.constraintIndexChanges() : DiffSets.<IndexDescriptor>emptyDiffSets();
    }

    public DiffSets<IndexDescriptor> getIndexDiffSets()
    {
        return indexChanges;
    }

    public DiffSets<IndexDescriptor> getConstraintIndexDiffSets()
    {
        return constraintIndexChanges;
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

    private RelationshipState getOrCreateRelationshipState( long relationshipId )
    {
        return getState( relationshipStates, relationshipId, new StateCreator<RelationshipState>()
        {
            @Override
            public RelationshipState newState( long id )
            {
                return new RelationshipState( id );
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

    public void addConstraint( UniquenessConstraint constraint, long indexId )
    {
        constraintsChanges.add( constraint );
        createdConstraintIndexes.put( constraint, indexId );
        getOrCreateLabelState( constraint.label() ).constraintsChanges().add( constraint );
    }


    public DiffSets<UniquenessConstraint> constraintsChangesForLabelAndProperty( long labelId, final long propertyKey )
    {
        return getOrCreateLabelState( labelId ).constraintsChanges().filterAdded( new Predicate<UniquenessConstraint>()
        {
            @Override
            public boolean accept( UniquenessConstraint item )
            {
                return item.property() == propertyKey;
            }
        } );
    }
    
    public DiffSets<UniquenessConstraint> constraintsChangesForLabel( long labelId )
    {
        return getOrCreateLabelState( labelId ).constraintsChanges();
    }

    public DiffSets<UniquenessConstraint> constraintsChanges()
    {
        return constraintsChanges;
    }

    public void dropConstraint( UniquenessConstraint constraint )
    {
        if ( constraintsChanges.remove( constraint ) )
        {
            createdConstraintIndexes.remove( constraint );
            // TODO: someone needs to make sure that the index we created gets dropped.
            // I think this can wait until commit/rollback, but we need to be able to know that the index was created...
        }
        constraintsChangesForLabel( constraint.label() ).remove( constraint );
    }

    public boolean unRemoveConstraint( UniquenessConstraint constraint )
    {
        return constraintsChanges.unRemove( constraint );
    }

    public Iterable<IndexDescriptor> createdConstraintIndexes()
    {
        return map( new Function<UniquenessConstraint, IndexDescriptor>()
        {
            @Override
            public IndexDescriptor apply( UniquenessConstraint constraint )
            {
                return new IndexDescriptor( constraint.label(), constraint.property() );
            }
        }, createdConstraintIndexes.keySet() );
    }
}
