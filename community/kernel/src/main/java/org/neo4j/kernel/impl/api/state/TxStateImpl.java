/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.util.DiffSets;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static org.neo4j.helpers.collection.Iterables.map;

/**
 * This organizes three disjoint containers of state. The goal is to bring that down to one, but for now, it's three.
 * Those three are:
 *
 *  * TxState - this class itself, containing HashMaps and DiffSets for changes
 *  * TransactionState - The legacy transaction state, to be refactored into this class.
 *  * WriteTransaction - Maintains changed records and commands for logical log.
 *                       To be refactored into a sub-component of this class.
 *
 * TransactionState is used to change the view of the data within a transaction, eg. see your own writes.
 *
 * WriteTransaction contains the changes that will actually be applied to the store, eg. records and commands.
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
public final class TxStateImpl implements TxState
{
    private static final StateCreator<LabelState> LABEL_STATE_CREATOR = new StateCreator<LabelState>()
    {
        @Override
        public LabelState newState( long id )
        {
            return new LabelState( id );
        }
    };

    private static final StateCreator<NodeState> NODE_STATE_CREATOR = new StateCreator<NodeState>()
    {
        @Override
        public NodeState newState( long id )
        {
            return new NodeState( id );
        }
    };

    private static final StateCreator<RelationshipState> RELATIONSHIP_STATE_CREATOR =
            new StateCreator<RelationshipState>()
    {
        @Override
        public RelationshipState newState( long id )
        {
            return new RelationshipState( id );
        }
    };

    private Map<Long/*Node ID*/, NodeState> nodeStatesMap;
    private Map<Long/*Relationship ID*/, RelationshipState> relationshipStatesMap;
    private Map<Long/*Label ID*/, LabelState> labelStatesMap;

    private GraphState graphState;
    private DiffSets<IndexDescriptor> indexChanges;
    private DiffSets<IndexDescriptor> constraintIndexChanges;
    private DiffSets<UniquenessConstraint> constraintsChanges;
    private DiffSets<Long> deletedNodes;
    private DiffSets<Long> deletedRelationships;
    private Map<UniquenessConstraint, Long> createdConstraintIndexesByConstraint;

    private final OldTxStateBridge legacyState;
    private final PersistenceManager persistenceManager; // should go away dammit!
    private final IdGeneration idGeneration; // needed when we move createNode() and createRelationship() to here...

    private boolean hasChanges;

    public TxStateImpl( OldTxStateBridge legacyState,
                    PersistenceManager legacyTransaction,
                    IdGeneration idGeneration )
    {
        this.legacyState = legacyState;
        this.persistenceManager = legacyTransaction;
        this.idGeneration = idGeneration;
    }

    @Override
    public void accept( final Visitor visitor )
    {
        if ( hasNodeStatesMap() && !nodeStatesMap().isEmpty() )
        {
            for ( NodeState node : nodeStates() )
            {
                DiffSets<Integer> labelDiff = node.labelDiffSets();
                visitor.visitNodeLabelChanges( node.getId(), labelDiff.getAdded(), labelDiff.getRemoved() );
            }
        }

        if ( hasIndexChangesDiffSets() && !indexChanges().isEmpty() )
        {
            indexChanges().accept( indexVisitor( visitor, false ) );
        }

        if ( hasConstraintIndexChangesDiffSets() && !constraintIndexChanges().isEmpty() )
        {
            constraintIndexChanges().accept( indexVisitor( visitor, true ) );
        }

        if ( hasConstraintsChangesDiffSets() && !constraintsChanges().isEmpty() )
        {
            constraintsChanges().accept( new DiffSets.Visitor<UniquenessConstraint>()
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

    @Override
    public boolean hasChanges()
    {
        return hasChanges || legacyState.hasChanges();
    }

    @Override
    public Iterable<NodeState> nodeStates()
    {
        return hasNodeStatesMap() ? nodeStatesMap().values() : Iterables.<NodeState>empty();
    }

    @Override
    public DiffSets<Long> labelStateNodeDiffSets( int labelId )
    {
        return getOrCreateLabelState( labelId ).getNodeDiffSets();
    }

    @Override
    public DiffSets<Integer> nodeStateLabelDiffSets( long nodeId )
    {
        return getOrCreateNodeState( nodeId ).labelDiffSets();
    }

    @Override
    public DiffSets<DefinedProperty> nodePropertyDiffSets( long nodeId )
    {
        return getOrCreateNodeState( nodeId ).propertyDiffSets();
    }

    @Override
    public DiffSets<DefinedProperty> relationshipPropertyDiffSets( long relationshipId )
    {
        return getOrCreateRelationshipState( relationshipId ).propertyDiffSets();
    }

    @Override
    public DiffSets<DefinedProperty> graphPropertyDiffSets()
    {
        return getOrCreateGraphState().propertyDiffSets();
    }

    @Override
    public boolean nodeIsAddedInThisTx( long nodeId )
    {
        return legacyState.nodeIsAddedInThisTx( nodeId );
    }

    @Override
    public boolean relationshipIsAddedInThisTx( long relationshipId )
    {
        return legacyState.relationshipIsAddedInThisTx( relationshipId );
    }

    @Override
    public void nodeDoDelete( long nodeId )
    {
        legacyState.deleteNode( nodeId );
        nodesDeletedInTx().remove( nodeId );
        hasChanges = true;
    }

    @Override
    public boolean nodeIsDeletedInThisTx( long nodeId )
    {
        return hasDeletedNodesDiffSets() && nodesDeletedInTx().isRemoved( nodeId );
    }

    @Override
    public void relationshipDoDelete( long relationshipId )
    {
        legacyState.deleteRelationship( relationshipId );
        deletedRelationships().remove( relationshipId );
        hasChanges = true;
    }

    @Override
    public boolean relationshipIsDeletedInThisTx( long relationshipId )
    {
        return hasDeletedRelationshipsDiffSets() && deletedRelationships().isRemoved( relationshipId );
    }

    @Override
    public void nodeDoReplaceProperty( long nodeId, Property replacedProperty, DefinedProperty newProperty )
    {
        if ( newProperty.isDefined() )
        {
            DiffSets<DefinedProperty> diffSets = nodePropertyDiffSets( nodeId );
            if ( replacedProperty.isDefined() )
            {
                diffSets.replace( (DefinedProperty)replacedProperty, newProperty );
            }
            else
            {
                diffSets.add( newProperty );
            }
            legacyState.nodeSetProperty( nodeId, newProperty );
            hasChanges = true;
        }
    }

    @Override
    public void relationshipDoReplaceProperty( long relationshipId, Property replacedProperty, DefinedProperty newProperty )
    {
        if ( newProperty.isDefined() )
        {
            DiffSets<DefinedProperty> diffSets = relationshipPropertyDiffSets( relationshipId );
            if ( replacedProperty.isDefined() )
            {
                diffSets.replace( (DefinedProperty)replacedProperty, newProperty );
            }
            else
            {
                diffSets.add( newProperty );
            }
            legacyState.relationshipSetProperty( relationshipId, newProperty );
            hasChanges = true;
        }
    }

    @Override
    public void graphDoReplaceProperty( Property replacedProperty, DefinedProperty newProperty )
    {
        if ( newProperty.isDefined() )
        {
            DiffSets<DefinedProperty> diffSets = graphPropertyDiffSets();
            if ( replacedProperty.isDefined() )
            {
                diffSets.replace( (DefinedProperty)replacedProperty, newProperty );
            }
            else
            {
                diffSets.add( newProperty );
            }
            legacyState.graphSetProperty( newProperty );
            hasChanges = true;
        }
    }

    @Override
    public void nodeDoRemoveProperty( long nodeId, Property removedProperty )
    {
        if ( removedProperty.isDefined() )
        {
            nodePropertyDiffSets( nodeId ).remove( (DefinedProperty)removedProperty );
            legacyState.nodeRemoveProperty( nodeId, (DefinedProperty)removedProperty );
            hasChanges = true;
        }
    }

    @Override
    public void relationshipDoRemoveProperty( long relationshipId, Property removedProperty )
    {
        if ( removedProperty.isDefined() )
        {
            relationshipPropertyDiffSets( relationshipId ).remove( (DefinedProperty)removedProperty );
            legacyState.relationshipRemoveProperty( relationshipId, (DefinedProperty)removedProperty );
            hasChanges = true;
        }
    }

    @Override
    public void graphDoRemoveProperty( Property removedProperty )
    {
        if ( removedProperty.isDefined() )
        {
            graphPropertyDiffSets().remove( (DefinedProperty)removedProperty );
            legacyState.graphRemoveProperty( (DefinedProperty)removedProperty );
            hasChanges = true;
        }
    }

    @Override
    public void nodeDoAddLabel( int labelId, long nodeId )
    {
        labelStateNodeDiffSets( labelId ).add( nodeId );
        nodeStateLabelDiffSets( nodeId ).add( labelId );
        persistenceManager.addLabelToNode( labelId, nodeId );
        hasChanges = true;
    }

    @Override
    public void nodeDoRemoveLabel( int labelId, long nodeId )
    {
        labelStateNodeDiffSets( labelId ).remove( nodeId );
        nodeStateLabelDiffSets( nodeId ).remove( labelId );
        persistenceManager.removeLabelFromNode( labelId, nodeId );
        hasChanges = true;
    }

    @Override
    public UpdateTriState labelState( long nodeId, int labelId )
    {
        NodeState nodeState = getState( nodeStatesMap(), nodeId, null );
        if ( nodeState != null )
        {
            DiffSets<Integer> labelDiff = nodeState.labelDiffSets();
            if ( labelDiff.isAdded( labelId ) )
            {
                return UpdateTriState.ADDED;
            }
            if ( labelDiff.isRemoved( labelId ) )
            {
                return UpdateTriState.REMOVED;
            }
        }
        return UpdateTriState.UNTOUCHED;
    }

    @Override
    public Set<Long> nodesWithLabelAdded( int labelId )
    {
        if ( hasLabelStatesMap() )
        {
            LabelState state = getState( labelStatesMap, labelId, null );
            if ( null != state )
            {
                return state.getNodeDiffSets().getAdded();
            }
        }

        return Collections.emptySet();
    }

    @Override
    public DiffSets<Long> nodesWithLabelChanged( int labelId )
    {
        if ( hasLabelStatesMap() )
        {
            LabelState state = getState( labelStatesMap, labelId, null );
            if ( null != state )
            {
                return state.getNodeDiffSets();
            }
        }
        return DiffSets.emptyDiffSets();
    }

    @Override
    public void indexRuleDoAdd( IndexDescriptor descriptor )
    {
        DiffSets<IndexDescriptor> diff = indexChanges();
        if ( diff.unRemove( descriptor ) )
        {
            getOrCreateLabelState( descriptor.getLabelId() ).indexChanges().unRemove( descriptor );
        }
        else
        {
            indexChanges().add( descriptor );
            getOrCreateLabelState( descriptor.getLabelId() ).indexChanges().add( descriptor );
        }
        hasChanges = true;
    }

    @Override
    public void constraintIndexRuleDoAdd( IndexDescriptor descriptor )
    {
        constraintIndexChanges().add( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).constraintIndexChanges().add( descriptor );
        hasChanges = true;
    }

    @Override
    public void indexDoDrop( IndexDescriptor descriptor )
    {
        indexChanges().remove( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).indexChanges().remove( descriptor );
        hasChanges = true;
    }

    @Override
    public void constraintIndexDoDrop( IndexDescriptor descriptor )
    {
        constraintIndexChanges().remove( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).constraintIndexChanges().remove( descriptor );
        hasChanges = true;
    }

    @Override
    public DiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId )
    {
        if ( hasLabelStatesMap() )
        {
            LabelState labelState = getState( labelStatesMap, labelId, null );
            if ( null != labelState )
            {
                return labelState.indexChanges();
            }
        }
        return DiffSets.emptyDiffSets();
    }

    @Override
    public DiffSets<IndexDescriptor> constraintIndexDiffSetsByLabel( int labelId )
    {
        if ( hasLabelStatesMap() )
        {
            LabelState labelState = getState( labelStatesMap(), labelId, null );
            if (labelState != null)
            {
                return labelState.constraintIndexChanges();
            }
        }
        return DiffSets.emptyDiffSets();
    }

    @Override
    public DiffSets<IndexDescriptor> indexChanges()
    {
        if ( !hasIndexChangesDiffSets() )
        {
            indexChanges = new DiffSets<>();
        }
        return indexChanges;
    }

    private boolean hasIndexChangesDiffSets()
    {
        return indexChanges != null;
    }

    @Override
    public DiffSets<IndexDescriptor> constraintIndexChanges()
    {
        if ( !hasConstraintIndexChangesDiffSets() )
        {
            constraintIndexChanges = new DiffSets<>();
        }
        return constraintIndexChanges;
    }

    private boolean hasConstraintIndexChangesDiffSets()
    {
        return constraintIndexChanges != null;
    }

    @Override
    public DiffSets<Long> nodesWithChangedProperty( int propertyKeyId, Object value )
    {
        return legacyState.getNodesWithChangedProperty( propertyKeyId, value );
    }

    @Override
    public Map<Long, Object> nodesWithChangedProperty( int propertyKeyId )
    {
        return legacyState.getNodesWithChangedProperty( propertyKeyId );
    }

    @Override
    public DiffSets<Long> nodesDeletedInTx()
    {
        if ( !hasDeletedNodesDiffSets() )
        {
            deletedNodes = new DiffSets<>();
        }
        return deletedNodes;
    }

    private boolean hasDeletedNodesDiffSets()
    {
        return deletedNodes != null;
    }

    public DiffSets<Long> deletedRelationships()
    {
        if ( !hasDeletedRelationshipsDiffSets() )
        {
            deletedRelationships = new DiffSets<>();
        }
        return deletedRelationships;
    }

    private boolean hasDeletedRelationshipsDiffSets()
    {
        return deletedRelationships != null;
    }

    private LabelState getOrCreateLabelState( int labelId )
    {
        return getState( labelStatesMap(), labelId, LABEL_STATE_CREATOR );
    }

    private NodeState getOrCreateNodeState( long nodeId )
    {
        return getState( nodeStatesMap(), nodeId, NODE_STATE_CREATOR );
    }

    private RelationshipState getOrCreateRelationshipState( long relationshipId )
    {
        return getState( relationshipStatesMap(), relationshipId, RELATIONSHIP_STATE_CREATOR );
    }

    private GraphState getOrCreateGraphState()
    {
        if ( graphState == null )
        {
            graphState = new GraphState();
        }
        return graphState;
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
            hasChanges = true;
        }
        return result;
    }

    @Override
    public void constraintDoAdd( UniquenessConstraint constraint, long indexId )
    {
        constraintsChanges().add( constraint );
        createdConstraintIndexesByConstraint().put( constraint, indexId );
        getOrCreateLabelState( constraint.label() ).constraintsChanges().add( constraint );
        hasChanges = true;
    }


    @Override
    public DiffSets<UniquenessConstraint> constraintsChangesForLabelAndProperty( int labelId, final int propertyKey )
    {
        return getOrCreateLabelState( labelId ).constraintsChanges().filterAdded( new Predicate<UniquenessConstraint>()
        {
            @Override
            public boolean accept( UniquenessConstraint item )
            {
                return item.propertyKeyId() == propertyKey;
            }
        } );
    }

    @Override
    public DiffSets<UniquenessConstraint> constraintsChangesForLabel( int labelId )
    {
        return getOrCreateLabelState( labelId ).constraintsChanges();
    }

    @Override
    public DiffSets<UniquenessConstraint> constraintsChanges()
    {
        if ( !hasConstraintsChangesDiffSets() )
        {
            constraintsChanges = new DiffSets<>();
        }
        return constraintsChanges;
    }

    private boolean hasConstraintsChangesDiffSets()
    {
        return constraintsChanges != null;
    }

    @Override
    public void constraintDoDrop( UniquenessConstraint constraint )
    {
        constraintsChanges().remove( constraint );

        constraintIndexDoDrop( new IndexDescriptor( constraint.label(), constraint.propertyKeyId() ));
        constraintsChangesForLabel( constraint.label() ).remove( constraint );
        hasChanges = true;
    }

    @Override
    public boolean constraintDoUnRemove( UniquenessConstraint constraint )
    {
        if ( constraintsChanges().unRemove( constraint ) )
        {
            constraintsChangesForLabel( constraint.label() ).unRemove( constraint );
            return true;
        }
        return false;
    }

    @Override
    public boolean constraintIndexDoUnRemove( IndexDescriptor index )
    {
        if ( constraintIndexChanges().unRemove( index ) )
        {
            constraintIndexDiffSetsByLabel( index.getLabelId() ).unRemove( index );
            return true;
        }
        return false;
    }

    @Override
    public Iterable<IndexDescriptor> constraintIndexesCreatedInTx()
    {
       if ( hasCreatedConstraintIndexesMap() )
       {
           Map<UniquenessConstraint, Long> constraintMap = createdConstraintIndexesByConstraint();
           if ( !constraintMap.isEmpty() )
           {
               return map( new Function<UniquenessConstraint, IndexDescriptor>()
               {
                   @Override
                   public IndexDescriptor apply( UniquenessConstraint constraint )
                   {
                       return new IndexDescriptor( constraint.label(), constraint.propertyKeyId() );
                   }
               }, constraintMap.keySet() );
           }
       }

       return Iterables.empty();
    }

    public Long indexCreatedForConstraint( UniquenessConstraint constraint )
    {
        return createdConstraintIndexesByConstraint == null ? null :
                createdConstraintIndexesByConstraint.get( constraint );
    }

    private Map<UniquenessConstraint, Long> createdConstraintIndexesByConstraint()
    {
        if ( !hasCreatedConstraintIndexesMap() )
        {
            createdConstraintIndexesByConstraint = new HashMap<>();
        }
        return createdConstraintIndexesByConstraint;
    }

    private boolean hasCreatedConstraintIndexesMap()
    {
        return null != createdConstraintIndexesByConstraint;
    }

    private Map<Long, NodeState> nodeStatesMap()
    {
        if ( !hasNodeStatesMap() )
        {
            nodeStatesMap = new HashMap<>();
        }
        return nodeStatesMap;
    }

    private boolean hasNodeStatesMap()
    {
        return null != nodeStatesMap;
    }

    private Map<Long, RelationshipState> relationshipStatesMap()
    {
        if ( !hasRelationshipsStatesMap() )
        {
            relationshipStatesMap = new HashMap<>();
        }
        return relationshipStatesMap;
    }

    private boolean hasRelationshipsStatesMap()
    {
        return null != relationshipStatesMap;
    }

    private Map<Long, LabelState> labelStatesMap()
    {
        if ( !hasLabelStatesMap() )
        {
            labelStatesMap = new HashMap<>();
        }
        return labelStatesMap;
    }

    private boolean hasLabelStatesMap()
    {
        return null != labelStatesMap;
    }
}
