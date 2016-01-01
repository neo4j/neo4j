/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.TxState;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.util.DiffSets;

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
 *      StateHandlingTransactionContext-------StateHandlingStatementContext
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
 *      StateHandlingTransactionContext-------StateHandlingStatementContext
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

    private PropertyChanges propertyChangesForNodes;

    // Tracks added and removed nodes, not modified nodes
    private DiffSets<Long> nodes;

    // Tracks added and removed relationships, not modified relationships
    private DiffSets<Long> relationships;

    // This is temporary. It is needed until we've removed nodes and rels from the global cache, to tell
    // that they were created and then deleted in the same tx. This is here just to set a save point to
    // get a large set of changes in, and is meant to be removed in the coming days in a follow-up commit.
    private final Set<Long> nodesCreatedAndDeletedInTx = new HashSet<>();
    private final Set<Long> relsCreatedAndDeletedInTx = new HashSet<>();

    private Map<UniquenessConstraint, Long> createdConstraintIndexesByConstraint;

    private PrimitiveIntObjectMap<Map<DefinedProperty, DiffSets<Long>>> indexUpdates;

    private final OldTxStateBridge legacyState;
    private final PersistenceManager persistenceManager; // should go away dammit!
    private final IdGeneration idGeneration; // needed when we move createNode() and createRelationship() to here...

    private boolean hasChanges;

    public TxStateImpl( OldTxStateBridge legacyState, PersistenceManager legacyTransaction, IdGeneration idGeneration )
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
            for ( NodeState node : modifiedNodes() )
            {
                node.accept(nodeVisitor( visitor ));
            }
        }

        if ( hasRelationshipsStatesMap() && !relationshipStatesMap().isEmpty() )
        {
            for ( RelationshipState rel : modifiedRelationships() )
            {
                rel.accept( relVisitor( visitor ) );
            }
        }

        if( graphState != null )
        {
            graphState.accept( graphPropertyVisitor( visitor ) );
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

    private static NodeState.Visitor nodeVisitor( final Visitor visitor  )
    {
        return new NodeState.Visitor()
        {
            @Override
            public void visitLabelChanges( long nodeId, Iterator<Integer> added, Iterator<Integer> removed )
            {
                visitor.visitNodeLabelChanges( nodeId, added, removed );
            }

            @Override
            public void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,
                                              Iterator<DefinedProperty> changed, Iterator<Integer> removed)
            {
                visitor.visitNodePropertyChanges( entityId, added, changed, removed );
            }
        };
    }

    private static PropertyContainerState.Visitor relVisitor( final Visitor visitor  )
    {
        return new PropertyContainerState.Visitor()
        {
            @Override
            public void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,
                                              Iterator<DefinedProperty> changed, Iterator<Integer> removed)
            {
                visitor.visitRelPropertyChanges( entityId, added, changed, removed );
            }
        };
    }

    private static PropertyContainerState.Visitor graphPropertyVisitor( final Visitor visitor  )
    {
        return new PropertyContainerState.Visitor()
        {
            @Override
            public void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,
                                              Iterator<DefinedProperty> changed, Iterator<Integer> removed)
            {
                visitor.visitGraphPropertyChanges( added, changed, removed );
            }
        };
    }

    @Override
    public boolean hasChanges()
    {
        return hasChanges || legacyState.hasChanges();
    }

    @Override
    public Iterable<NodeState> modifiedNodes()
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
    public Iterator<DefinedProperty> augmentNodeProperties( long nodeId, Iterator<DefinedProperty> original )
    {
        NodeState state;
        if(nodeStatesMap != null && (state = nodeStatesMap.get( nodeId )) != null)
        {
            return state.augmentProperties( original );
        }
        return original;
    }

    @Override
    public Iterator<DefinedProperty> augmentRelProperties( long relId, Iterator<DefinedProperty> original )
    {
        RelationshipState state;
        if(relationshipStatesMap != null && (state = relationshipStatesMap.get( relId )) != null)
        {
            return state.augmentProperties( original );
        }
        return original;
    }

    @Override
    public Iterator<DefinedProperty> augmentGraphProperties( Iterator<DefinedProperty> original )
    {
        if(graphState != null)
        {
            return graphState.augmentProperties( original );
        }
        return original;
    }

    @Override
    public Iterator<DefinedProperty> addedAndChangedNodeProperties( long nodeId )
    {
        NodeState state;
        if(nodeStatesMap != null && (state = nodeStatesMap.get( nodeId )) != null)
        {
            return state.addedAndChangedProperties();
        }
        return IteratorUtil.emptyIterator();
    }

    @Override
    public Iterator<DefinedProperty> addedAndChangedRelProperties( long relId )
    {
        RelationshipState state;
        if(relationshipStatesMap != null && (state = relationshipStatesMap.get( relId )) != null)
        {
            return state.addedAndChangedProperties();
        }
        return IteratorUtil.emptyIterator();
    }

    @Override
    public boolean nodeIsAddedInThisTx( long nodeId )
    {
        return hasNodesAddedOrRemoved() && nodes.isAdded( nodeId );
    }

    @Override
    public boolean relationshipIsAddedInThisTx( long relationshipId )
    {
        return hasRelsAddedOrRemoved() && relationships.isAdded( relationshipId );
    }

    @Override
    public long nodeDoCreate()
    {
        long id = legacyState.nodeCreate();
        addedAndRemovedNodes().add( id );
        hasChanges = true;
        return id;
    }

    @Override
    public void nodeDoDelete( long nodeId )
    {
        legacyState.deleteNode( nodeId );
        if(addedAndRemovedNodes().remove( nodeId ))
        {
            nodesCreatedAndDeletedInTx.add(nodeId);
        }

        if(hasNodeStatesMap())
        {
            NodeState nodeState = nodeStatesMap.remove( nodeId );

            if(nodeState != null)
            {
                DiffSets<Integer> diff = nodeState.labelDiffSets();
                for ( Integer label : diff.getAdded() )
                {
                    labelStateNodeDiffSets( label ).remove( nodeId );
                }
                nodeState.clearIndexDiffs(nodeId);
                nodeState.clear();
            }
        }
        hasChanges = true;
    }

    @Override
    public long relationshipDoCreate( int relationshipTypeId, long startNodeId, long endNodeId )
    {
        long id = legacyState.relationshipCreate( relationshipTypeId, startNodeId, endNodeId );

        addedAndRemovedRels().add( id );

        if(startNodeId == endNodeId)
        {
            getOrCreateNodeState( startNodeId ).addRelationship( id, relationshipTypeId, Direction.BOTH );
        }
        else
        {
            getOrCreateNodeState( startNodeId ).addRelationship( id, relationshipTypeId, Direction.OUTGOING );
            getOrCreateNodeState( endNodeId ).addRelationship( id, relationshipTypeId, Direction.INCOMING );
        }

        getOrCreateRelationshipState( id ).setMetaData( startNodeId, endNodeId, relationshipTypeId );

        hasChanges = true;
        return id;
    }

    @Override
    public boolean nodeIsDeletedInThisTx( long nodeId )
    {
        return hasNodesAddedOrRemoved() && addedAndRemovedNodes().isRemoved( nodeId )
                // Temporary until we've stopped adding nodes to the global cache during tx.
                || nodesCreatedAndDeletedInTx.contains( nodeId );
    }

    @Override
    public boolean nodeModifiedInThisTx( long nodeId )
    {
        return nodeIsAddedInThisTx( nodeId ) || nodeIsDeletedInThisTx( nodeId ) || hasNodeState( nodeId );
    }

    @Override
    public void relationshipDoDelete( long id, long startNodeId, long endNodeId, int type )
    {
        legacyState.deleteRelationship( id );
        if(addedAndRemovedRels().remove( id ))
        {
            relsCreatedAndDeletedInTx.add( id );
        }

        if(startNodeId == endNodeId)
        {
            getOrCreateNodeState( startNodeId ).removeRelationship( id, type, Direction.BOTH );
        }
        else
        {
            getOrCreateNodeState( startNodeId ).removeRelationship( id, type, Direction.OUTGOING );
            getOrCreateNodeState( endNodeId ).removeRelationship( id, type, Direction.INCOMING );
        }

        if(hasRelationshipsStatesMap())
        {
            RelationshipState removed = relationshipStatesMap.remove( id );
            if(removed != null)
            {
                removed.clear();
            }
        }

        hasChanges = true;
    }

    @Override
    public void relationshipDoDeleteAddedInThisTx( long relationshipId )
    {
        RelationshipState state = getOrCreateRelationshipState( relationshipId );
        relationshipDoDelete( relationshipId, state.startNode(), state.endNode(), state.type() );
    }

    @Override
    public boolean relationshipIsDeletedInThisTx( long relationshipId )
    {
        return hasDeletedRelationshipsDiffSets() && addedAndRemovedRels().isRemoved( relationshipId )
                // Temporary until we stop adding rels to the global cache during tx
                || relsCreatedAndDeletedInTx.contains( relationshipId );
    }

    @Override
    public void nodeDoReplaceProperty( long nodeId, Property replacedProperty, DefinedProperty newProperty )
    {
        if(replacedProperty.isDefined())
        {
            getOrCreateNodeState( nodeId ).changeProperty( newProperty );
            nodePropertyChanges().changeProperty( nodeId, replacedProperty.propertyKeyId(),
                    ((DefinedProperty)replacedProperty).value(), newProperty.value() );
        }
        else
        {
            NodeState nodeState = getOrCreateNodeState( nodeId );
//            nodeState.
            nodeState.addProperty( newProperty );
            nodePropertyChanges().addProperty(nodeId, newProperty.propertyKeyId(), newProperty.value());
        }
        legacyState.nodeSetProperty( nodeId, newProperty );
        hasChanges = true;
    }

    @Override
    public void relationshipDoReplaceProperty( long relationshipId, Property replacedProperty, DefinedProperty newProperty )
    {
        if(replacedProperty.isDefined())
        {
            getOrCreateRelationshipState( relationshipId ).changeProperty( newProperty );
        }
        else
        {
            getOrCreateRelationshipState( relationshipId ).addProperty( newProperty );
        }
        legacyState.relationshipSetProperty( relationshipId, newProperty );
        hasChanges = true;
    }

    @Override
    public void graphDoReplaceProperty( Property replacedProperty, DefinedProperty newProperty )
    {
        if(replacedProperty.isDefined())
        {
            getOrCreateGraphState().changeProperty( newProperty );
        }
        else
        {
            getOrCreateGraphState().addProperty( newProperty );
        }
        legacyState.graphSetProperty( newProperty );
        hasChanges = true;
    }

    @Override
    public void nodeDoRemoveProperty( long nodeId, DefinedProperty removedProperty )
    {
        getOrCreateNodeState( nodeId ).removeProperty( removedProperty );
        nodePropertyChanges().removeProperty( nodeId, removedProperty.propertyKeyId(),
                removedProperty.value() );
        legacyState.nodeRemoveProperty( nodeId, removedProperty );
        hasChanges = true;
    }

    @Override
    public void relationshipDoRemoveProperty( long relationshipId, DefinedProperty removedProperty )
    {
        getOrCreateRelationshipState( relationshipId ).removeProperty( removedProperty );
        legacyState.relationshipRemoveProperty( relationshipId, removedProperty );
        hasChanges = true;
    }

    @Override
    public void graphDoRemoveProperty( DefinedProperty removedProperty )
    {
        getOrCreateGraphState().removeProperty( removedProperty );
        legacyState.graphRemoveProperty( removedProperty );
        hasChanges = true;
    }

    @Override
    public void nodeDoAddLabel( int labelId, long nodeId )
    {
        labelStateNodeDiffSets( labelId ).add( nodeId );
        nodeStateLabelDiffSets( nodeId ).add( labelId );
        hasChanges = true;
    }

    @Override
    public void nodeDoRemoveLabel( int labelId, long nodeId )
    {
        labelStateNodeDiffSets( labelId ).remove( nodeId );
        nodeStateLabelDiffSets( nodeId ).remove( labelId );
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
        return propertyChangesForNodes != null ? propertyChangesForNodes.changesForProperty( propertyKeyId, value ) :
                DiffSets.<Long>emptyDiffSets();
    }

    @Override
    public DiffSets<Long> addedAndRemovedNodes()
    {
        if ( !hasNodesAddedOrRemoved() )
        {
            nodes = new DiffSets<>();
        }
        return nodes;
    }

    private boolean hasNodesAddedOrRemoved()
    {
        return nodes != null;
    }

    private boolean hasRelsAddedOrRemoved()
    {
        return relationships != null;
    }

    @Override
    public PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, PrimitiveLongIterator rels )
    {
        if(hasNodeState( nodeId ))
        {
            rels = getOrCreateNodeState( nodeId ).augmentRelationships( direction, rels );
            // TODO: This should be handled by the augment call above
            if(hasDeletedRelationshipsDiffSets())
            {
                rels = addedAndRemovedRels().augmentWithRemovals( rels );
            }
        }
        return rels;
    }

    @Override
    public PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, int[] types, PrimitiveLongIterator rels )
    {
        if(hasNodeState( nodeId ))
        {
            rels = getOrCreateNodeState( nodeId ).augmentRelationships( direction, types, rels );
            // TODO: This should be handled by the augment call above
            if(hasDeletedRelationshipsDiffSets())
            {
                rels = addedAndRemovedRels().augmentWithRemovals( rels );
            }
        }
        return rels;
    }

    @Override
    public int augmentNodeDegree( long nodeId, int degree, Direction direction )
    {
        if(hasNodeState( nodeId ))
        {
            return getOrCreateNodeState( nodeId ).augmentDegree( direction, degree );
        }
        return degree;
    }

    @Override
    public int augmentNodeDegree( long nodeId, int degree, Direction direction, int typeId )
    {
        if(hasNodeState( nodeId ))
        {
            return getOrCreateNodeState( nodeId ).augmentDegree( direction, degree, typeId );
        }
        return degree;
    }

    @Override
    public PrimitiveIntIterator nodeRelationshipTypes( long nodeId )
    {
        if ( hasNodeState( nodeId ) )
        {
            return getOrCreateNodeState( nodeId ).relationshipTypes();
        }
        return PrimitiveIntCollections.emptyIterator();
    }

    @Override
    public DiffSets<Long> addedAndRemovedRels()
    {
        if ( !hasDeletedRelationshipsDiffSets() )
        {
            relationships = new DiffSets<>();
        }
        return relationships;
    }

    @Override
    public Iterable<RelationshipState> modifiedRelationships()
    {
        return relationshipStatesMap != null ? relationshipStatesMap.values() : Iterables.<RelationshipState>empty();
    }

    private boolean hasDeletedRelationshipsDiffSets()
    {
        return relationships != null;
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

    @Override
    public Long indexCreatedForConstraint( UniquenessConstraint constraint )
    {
        return createdConstraintIndexesByConstraint == null ? null :
                createdConstraintIndexesByConstraint.get( constraint );
    }

    @Override
    public DiffSets<Long> indexUpdates( IndexDescriptor descriptor, Object value )
    {
        DiffSets<Long> diffs = getIndexUpdates( descriptor.getLabelId(), false, Property.property( descriptor.getPropertyKeyId(), value ) );
        return diffs == null ? DiffSets.<Long>emptyDiffSets() : diffs;
    }

    @Override
    public void indexUpdateProperty( IndexDescriptor descriptor, long nodeId, DefinedProperty propertyBefore, DefinedProperty propertyAfter )
    {
        {
            DiffSets<Long> before = getIndexUpdates( descriptor.getLabelId(), true, propertyBefore );
            if ( before != null )
            {
                before.remove( nodeId );
                //if ( hasNodesAddedOrRemoved() && addedAndRemovedNodes().getAdded().contains( nodeId ) )
                {
                    if ( before.getRemoved().contains( nodeId ) )
                    {
                        getOrCreateNodeState( nodeId ).addIndexDiff( before );
                    }
                    else
                    {
                        getOrCreateNodeState( nodeId ).removeIndexDiff( before );
                    }
                }
            }
        }
        {
            DiffSets<Long> after = getIndexUpdates( descriptor.getLabelId(), true, propertyAfter );
            if ( after != null )
            {
                after.add( nodeId );
                //if ( hasNodesAddedOrRemoved() && addedAndRemovedNodes().getAdded().contains( nodeId ) )
                {
                    if ( after.getAdded().contains( nodeId ) )
                    {
                        getOrCreateNodeState( nodeId ).addIndexDiff( after );
                    }
                    else
                    {
                        getOrCreateNodeState( nodeId ).removeIndexDiff( after );
                    }
                }
            }
        }
    }

    private DiffSets<Long> getIndexUpdates( int label, boolean create, DefinedProperty property )
    {
        if ( property == null )
        {
            return null;
        }
        if ( indexUpdates == null )
        {
            if ( !create )
            {
                return null;
            }
            indexUpdates = Primitive.intObjectMap();
        }
        Map<DefinedProperty, DiffSets<Long>> updates = indexUpdates.get( label );
        if ( updates == null )
        {
            if ( !create )
            {
                return null;
            }
            indexUpdates.put( label, updates = new HashMap<>() );
        }
        DiffSets<Long> diffs = updates.get( property );
        if ( diffs == null && create )
        {
            updates.put( property, diffs = new DiffSets<>() );
        }
        return diffs;
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

    private boolean hasNodeState(long nodeId)
    {
        return hasNodeStatesMap() && nodeStatesMap().containsKey( nodeId );
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

    private PropertyChanges nodePropertyChanges()
    {
        return propertyChangesForNodes == null ?
                propertyChangesForNodes = new PropertyChanges() : propertyChangesForNodes;
    }
}
