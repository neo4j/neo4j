/*
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Function;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.txstate.ReadableTxState;
import org.neo4j.kernel.api.txstate.RelationshipChangeVisitorAdapter;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.api.txstate.UpdateTriState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.kernel.impl.util.diffsets.DiffSetsVisitor;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;

import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.api.properties.Property.property;

/**
 * This class contains transaction-local changes to the graph. These changes can then be used to augment reads from the
 * committed state of the database (to make the local changes appear in local transaction read operations). At commit
 * time a visitor is sent into this class to convert the end result of the tx changes into a physical changeset.
 *
 * See {@link org.neo4j.kernel.impl.api.KernelTransactionImplementation} for how this happens.
 *
 * This class is very large, as it has been used as a gathering point to consolidate all transaction state knowledge
 * into one component. Now that that work is done, this class should be refactored to increase transparency in how it
 * works.
 */
public final class TxState implements TransactionState
{
    private Map<Integer/*Label ID*/, LabelState.Mutable> labelStatesMap;
    private static final LabelState.Defaults LABEL_STATE = new LabelState.Defaults()
    {
        @Override
        Map<Integer, LabelState.Mutable> getMap( TxState state )
        {
            return state.labelStatesMap;
        }

        @Override
        void setMap( TxState state, Map<Integer, LabelState.Mutable> map )
        {
            state.labelStatesMap = map;
        }
    };
    private Map<Long/*Node ID*/, NodeState.Mutable> nodeStatesMap;
    private static final NodeState.Defaults NODE_STATE = new NodeState.Defaults()
    {
        @Override
        Map<Long, NodeState.Mutable> getMap( TxState state )
        {
            return state.nodeStatesMap;
        }

        @Override
        void setMap( TxState state, Map<Long, NodeState.Mutable> map )
        {
            state.nodeStatesMap = map;
        }
    };
    private Map<Long/*Relationship ID*/, RelationshipState.Mutable> relationshipStatesMap;
    private static final RelationshipState.Defaults RELATIONSHIP_STATE = new RelationshipState.Defaults()
    {
        @Override
        Map<Long, RelationshipState.Mutable> getMap( TxState state )
        {
            return state.relationshipStatesMap;
        }

        @Override
        void setMap( TxState state, Map<Long, RelationshipState.Mutable> map )
        {
            state.relationshipStatesMap = map;
        }
    };

    private Map<Integer/*Token ID*/,String> createdLabelTokens;
    private Map<Integer/*Token ID*/,String> createdPropertyKeyTokens;
    private Map<Integer/*Token ID*/,String> createdRelationshipTypeTokens;

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
    private final Set<Long> nodesDeletedInTx = new HashSet<>();
    private final Set<Long> relationshipsDeletedInTx = new HashSet<>();

    private Map<UniquenessConstraint, Long> createdConstraintIndexesByConstraint;

    private Map<String, Map<String, String>> createdNodeLegacyIndexes;
    private Map<String, Map<String, String>> createdRelationshipLegacyIndexes;

    private PrimitiveIntObjectMap<Map<DefinedProperty, DiffSets<Long>>> indexUpdates;

    private boolean hasChanges, hasDataChanges;

    @Override
    public void accept( final TxStateVisitor visitor )
    {
        // Created nodes
        if ( nodes != null )
        {
            nodes.accept( createdNodesVisitor( visitor ) );
        }

        if ( relationships != null )
        {
            // Created relationships
            relationships.accept( createdRelationshipsVisitor( this, visitor ) );

            // Deleted relationships
            relationships.accept( deletedRelationshipsVisitor( visitor ) );
        }

        // Deleted nodes
        if ( nodes != null )
        {
            nodes.accept( deletedNodesVisitor( visitor ) );
        }

        for ( NodeState node : modifiedNodes() )
        {
            node.accept( nodeVisitor( visitor ) );
        }

        for ( RelationshipState rel : modifiedRelationships() )
        {
            rel.accept( relVisitor( visitor ) );
        }

        if ( graphState != null )
        {
            graphState.accept( graphPropertyVisitor( visitor ) );
        }

        if ( indexChanges != null )
        {
            indexChanges.accept( indexVisitor( visitor, false ) );
        }

        if ( constraintIndexChanges != null )
        {
            constraintIndexChanges.accept( indexVisitor( visitor, true ) );
        }

        if ( constraintsChanges != null )
        {
            constraintsChanges.accept( constraintsVisitor( visitor ) );
        }

        if ( createdLabelTokens != null )
        {
            for ( Map.Entry<Integer,String> entry : createdLabelTokens.entrySet() )
            {
                visitor.visitCreatedLabelToken( entry.getValue(), entry.getKey() );
            }
        }

        if ( createdPropertyKeyTokens != null )
        {
            for ( Map.Entry<Integer,String> entry : createdPropertyKeyTokens.entrySet() )
            {
                visitor.visitCreatedPropertyKeyToken( entry.getValue(), entry.getKey() );
            }
        }

        if ( createdRelationshipTypeTokens != null )
        {
            for ( Map.Entry<Integer,String> entry : createdRelationshipTypeTokens.entrySet() )
            {
                visitor.visitCreatedRelationshipTypeToken( entry.getValue(), entry.getKey() );
            }
        }

        if ( createdNodeLegacyIndexes != null )
        {
            for ( Map.Entry<String,Map<String,String>> entry : createdNodeLegacyIndexes.entrySet() )
            {
                visitor.visitCreatedNodeLegacyIndex( entry.getKey(), entry.getValue() );
            }
        }

        if ( createdRelationshipLegacyIndexes != null )
        {
            for ( Map.Entry<String,Map<String,String>> entry : createdRelationshipLegacyIndexes.entrySet() )
            {
                visitor.visitCreatedRelationshipLegacyIndex( entry.getKey(), entry.getValue() );
            }
        }
    }

    private static DiffSetsVisitor<Long> deletedNodesVisitor( final TxStateVisitor visitor )
    {
        return new DiffSetsVisitor.Adapter<Long>()
        {
            @Override
            public void visitRemoved( Long element )
            {
                visitor.visitDeletedNode( element );
            }
        };
    }

    private static DiffSetsVisitor<Long> createdNodesVisitor( final TxStateVisitor visitor )
    {
        return new DiffSetsVisitor.Adapter<Long>()
        {
            @Override
            public void visitAdded( Long element )
            {
                visitor.visitCreatedNode( element );
            }
        };
    }

    private static DiffSetsVisitor<Long> deletedRelationshipsVisitor( final TxStateVisitor visitor )
    {
        return new DiffSetsVisitor.Adapter<Long>()
        {
            @Override
            public void visitRemoved( Long id )
            {
                visitor.visitDeletedRelationship( id );
            }
        };
    }

    private static DiffSetsVisitor<Long> createdRelationshipsVisitor( ReadableTxState tx, final TxStateVisitor visitor )
    {
        return new RelationshipChangeVisitorAdapter( tx )
        {
            @Override
            protected void visitAddedRelationship( long relationshipId, int type, long startNode, long endNode )
            {
                visitor.visitCreatedRelationship( relationshipId, type, startNode, endNode );
            }
        };
    }

    private static DiffSetsVisitor<UniquenessConstraint> constraintsVisitor( final TxStateVisitor visitor )
    {
        return new DiffSetsVisitor<UniquenessConstraint>()
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
        };
    }

    private static DiffSetsVisitor<IndexDescriptor> indexVisitor( final TxStateVisitor visitor, final boolean forConstraint )
    {
        return new DiffSetsVisitor<IndexDescriptor>()
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

    private static NodeState.Visitor nodeVisitor( final TxStateVisitor visitor )
    {
        return new NodeState.Visitor()
        {
            @Override
            public void visitLabelChanges( long nodeId, Set<Integer> added, Set<Integer> removed )
            {
                visitor.visitNodeLabelChanges( nodeId, added, removed );
            }

            @Override
            public void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,
                                              Iterator<DefinedProperty> changed, Iterator<Integer> removed)
            {
                visitor.visitNodePropertyChanges( entityId, added, changed, removed );
            }

            @Override
            public void visitRelationshipChanges( long nodeId, RelationshipChangesForNode added,
                    RelationshipChangesForNode removed )
            {
                visitor.visitNodeRelationshipChanges( nodeId, added, removed );
            }
        };
    }

    private static PropertyContainerState.Visitor relVisitor( final TxStateVisitor visitor  )
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

    private static PropertyContainerState.Visitor graphPropertyVisitor( final TxStateVisitor visitor  )
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
        return hasChanges;
    }

    @Override
    public Iterable<NodeState> modifiedNodes()
    {
        return NODE_STATE.values( this );
    }

    private DiffSets<Long> getOrCreateLabelStateNodeDiffSets( int labelId )
    {
        return LABEL_STATE.getOrCreate( this, labelId ).getOrCreateNodeDiffSets();
    }

    @Override
    public ReadableDiffSets<Integer> nodeStateLabelDiffSets( long nodeId )
    {
        return NODE_STATE.get( this, nodeId ).labelDiffSets();
    }

    private DiffSets<Integer> getOrCreateNodeStateLabelDiffSets( long nodeId )
    {
        return getOrCreateNodeState( nodeId ).getOrCreateLabelDiffSets();
    }

    @Override
    public Iterator<DefinedProperty> augmentNodeProperties( long nodeId, Iterator<DefinedProperty> original )
    {
        return NODE_STATE.get( this, nodeId ).augmentProperties( original );
    }

    @Override
    public Iterator<DefinedProperty> augmentRelationshipProperties( long relId, Iterator<DefinedProperty> original )
    {
        return RELATIONSHIP_STATE.get( this, relId ).augmentProperties( original );
    }

    @Override
    public Iterator<DefinedProperty> augmentGraphProperties( Iterator<DefinedProperty> original )
    {
        if ( graphState != null )
        {
            return graphState.augmentProperties( original );
        }
        return original;
    }

    @Override
    public Iterator<DefinedProperty> addedAndChangedNodeProperties( long nodeId )
    {
        return NODE_STATE.get( this, nodeId ).addedAndChangedProperties();
    }

    @Override
    public Iterator<DefinedProperty> addedAndChangedRelationshipProperties( long relId )
    {
        return RELATIONSHIP_STATE.get( this, relId ).addedAndChangedProperties();
    }

    @Override
    public boolean nodeIsAddedInThisTx( long nodeId )
    {
        return nodes != null && nodes.isAdded( nodeId );
    }

    @Override
    public boolean relationshipIsAddedInThisTx( long relationshipId )
    {
        return relationships != null && relationships.isAdded( relationshipId );
    }

    private void changed()
    {
        hasChanges = true;
    }

    private void dataChanged()
    {
        changed();
        hasDataChanges = true;
    }

    @Override
    public void nodeDoCreate( long id )
    {
        nodes().add( id );
        dataChanged();
    }

    @Override
    public void nodeDoDelete( long nodeId )
    {
        if ( nodes().remove( nodeId ) )
        {
            nodesDeletedInTx.add( nodeId );
        }

        if ( nodeStatesMap != null )
        {
            NodeState.Mutable nodeState = nodeStatesMap.remove( nodeId );
            if ( nodeState != null )
            {
                ReadableDiffSets<Integer> diff = nodeState.labelDiffSets();
                for ( Integer label : diff.getAdded() )
                {
                    getOrCreateLabelStateNodeDiffSets( label ).remove( nodeId );
                }
                nodeState.clearIndexDiffs( nodeId );
                nodeState.clear();
            }
        }
        dataChanged();
    }

    @Override
    public void relationshipDoCreate( long id, int relationshipTypeId, long startNodeId, long endNodeId )
    {
        relationships().add( id );

        if ( startNodeId == endNodeId )
        {
            getOrCreateNodeState( startNodeId ).addRelationship( id, relationshipTypeId, Direction.BOTH );
        }
        else
        {
            getOrCreateNodeState( startNodeId ).addRelationship( id, relationshipTypeId, Direction.OUTGOING );
            getOrCreateNodeState( endNodeId ).addRelationship( id, relationshipTypeId, Direction.INCOMING );
        }

        getOrCreateRelationshipState( id ).setMetaData( startNodeId, endNodeId, relationshipTypeId );

        dataChanged();
    }

    @Override
    public boolean nodeIsDeletedInThisTx( long nodeId )
    {
        return addedAndRemovedNodes().isRemoved( nodeId )
                // Temporary until we've stopped adding nodes to the global cache during tx.
                || nodesDeletedInTx.contains( nodeId );
    }

    @Override
    public boolean nodeModifiedInThisTx( long nodeId )
    {
        return nodeIsAddedInThisTx( nodeId ) || nodeIsDeletedInThisTx( nodeId ) || hasNodeState( nodeId );
    }

    @Override
    public void relationshipDoDelete( long id, int type, long startNodeId, long endNodeId )
    {
        if ( relationships().remove( id ) )
        {
            relationshipsDeletedInTx.add( id );
        }

        if ( startNodeId == endNodeId )
        {
            getOrCreateNodeState( startNodeId ).removeRelationship( id, type, Direction.BOTH );
        }
        else
        {
            getOrCreateNodeState( startNodeId ).removeRelationship( id, type, Direction.OUTGOING );
            getOrCreateNodeState( endNodeId ).removeRelationship( id, type, Direction.INCOMING );
        }

        if ( relationshipStatesMap != null )
        {
            RelationshipState.Mutable removed = relationshipStatesMap.remove( id );
            if ( removed != null )
            {
                removed.clear();
            }
        }

        dataChanged();
    }

    @Override
    public void relationshipDoDeleteAddedInThisTx( long relationshipId )
    {
        RELATIONSHIP_STATE.get( this, relationshipId ).accept( new RelationshipVisitor<RuntimeException>()
        {
            @Override
            public void visit( long relId, int type, long startNode, long endNode )
            {
                relationshipDoDelete( relId, type, startNode, endNode );
            }
        } );
    }

    @Override
    public boolean relationshipIsDeletedInThisTx( long relationshipId )
    {
        return addedAndRemovedRelationships().isRemoved( relationshipId )
                // Temporary until we stop adding rels to the global cache during tx
                || relationshipsDeletedInTx.contains( relationshipId );
    }

    @Override
    public void nodeDoReplaceProperty( long nodeId, Property replacedProperty, DefinedProperty newProperty )
    {
        if ( replacedProperty.isDefined() )
        {
            getOrCreateNodeState( nodeId ).changeProperty( newProperty );
            nodePropertyChanges().changeProperty( nodeId, replacedProperty.propertyKeyId(),
                                                  ((DefinedProperty) replacedProperty).value(), newProperty.value() );
        }
        else
        {
            NodeState.Mutable nodeState = getOrCreateNodeState( nodeId );
            nodeState.addProperty( newProperty );
            nodePropertyChanges().addProperty( nodeId, newProperty.propertyKeyId(), newProperty.value() );
        }
        dataChanged();
    }

    @Override
    public void relationshipDoReplaceProperty( long relationshipId, Property replacedProperty, DefinedProperty newProperty )
    {
        if ( replacedProperty.isDefined() )
        {
            getOrCreateRelationshipState( relationshipId ).changeProperty( newProperty );
        }
        else
        {
            getOrCreateRelationshipState( relationshipId ).addProperty( newProperty );
        }
        dataChanged();
    }

    @Override
    public void graphDoReplaceProperty( Property replacedProperty, DefinedProperty newProperty )
    {
        if ( replacedProperty.isDefined() )
        {
            getOrCreateGraphState().changeProperty( newProperty );
        }
        else
        {
            getOrCreateGraphState().addProperty( newProperty );
        }
        dataChanged();
    }

    @Override
    public void nodeDoRemoveProperty( long nodeId, DefinedProperty removedProperty )
    {
        getOrCreateNodeState( nodeId ).removeProperty( removedProperty );
        nodePropertyChanges().removeProperty( nodeId, removedProperty.propertyKeyId(),
                removedProperty.value() );
        dataChanged();
    }

    @Override
    public void relationshipDoRemoveProperty( long relationshipId, DefinedProperty removedProperty )
    {
        getOrCreateRelationshipState( relationshipId ).removeProperty( removedProperty );
        dataChanged();
    }

    @Override
    public void graphDoRemoveProperty( DefinedProperty removedProperty )
    {
        getOrCreateGraphState().removeProperty( removedProperty );
        dataChanged();
    }

    @Override
    public void nodeDoAddLabel( int labelId, long nodeId )
    {
        getOrCreateLabelStateNodeDiffSets( labelId ).add( nodeId );
        getOrCreateNodeStateLabelDiffSets( nodeId ).add( labelId );
        dataChanged();
    }

    @Override
    public void nodeDoRemoveLabel( int labelId, long nodeId )
    {
        getOrCreateLabelStateNodeDiffSets( labelId ).remove( nodeId );
        getOrCreateNodeStateLabelDiffSets( nodeId ).remove( labelId );
        dataChanged();
    }

    @Override
    public void labelDoCreateForName( String labelName, int id )
    {
        if ( createdLabelTokens == null )
        {
            createdLabelTokens = new HashMap<>();
        }
        createdLabelTokens.put( id, labelName );
        changed();
    }

    @Override
    public void propertyKeyDoCreateForName( String propertyKeyName, int id )
    {
        if ( createdPropertyKeyTokens == null )
        {
            createdPropertyKeyTokens = new HashMap<>();
        }
        createdPropertyKeyTokens.put( id, propertyKeyName );
        changed();
    }

    @Override
    public void relationshipTypeDoCreateForName( String labelName, int id )
    {
        if ( createdRelationshipTypeTokens == null )
        {
            createdRelationshipTypeTokens = new HashMap<>();
        }
        createdRelationshipTypeTokens.put( id, labelName );
        changed();
    }

    @Override
    public UpdateTriState labelState( long nodeId, int labelId )
    {
        return NODE_STATE.get( this, nodeId ).labelState( labelId );
    }

    @Override
    public ReadableDiffSets<Long> nodesWithLabelChanged( int labelId )
    {
        return LABEL_STATE.get( this, labelId ).nodeDiffSets();
    }

    @Override
    public void indexRuleDoAdd( IndexDescriptor descriptor )
    {
        DiffSets<IndexDescriptor> diff = indexChangesDiffSets();
        if ( diff.unRemove( descriptor ) )
        {
            getOrCreateLabelState( descriptor.getLabelId() ).getOrCreateIndexChanges().unRemove( descriptor );
        }
        else
        {
            diff.add( descriptor );
            getOrCreateLabelState( descriptor.getLabelId() ).getOrCreateIndexChanges().add( descriptor );
        }
        changed();
    }

    @Override
    public void constraintIndexRuleDoAdd( IndexDescriptor descriptor )
    {
        constraintIndexChangesDiffSets().add( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).getOrCreateConstraintIndexChanges().add( descriptor );
        changed();
    }

    @Override
    public void indexDoDrop( IndexDescriptor descriptor )
    {
        indexChangesDiffSets().remove( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).getOrCreateIndexChanges().remove( descriptor );
        changed();
    }

    @Override
    public void constraintIndexDoDrop( IndexDescriptor descriptor )
    {
        constraintIndexChangesDiffSets().remove( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).getOrCreateConstraintIndexChanges().remove( descriptor );
        changed();
    }

    @Override
    public ReadableDiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId )
    {
        return LABEL_STATE.get( this, labelId ).indexChanges();
    }

    @Override
    public ReadableDiffSets<IndexDescriptor> constraintIndexDiffSetsByLabel( int labelId )
    {
        return LABEL_STATE.get( this, labelId ).constraintIndexChanges();
    }

    @Override
    public ReadableDiffSets<IndexDescriptor> indexChanges()
    {
        return ReadableDiffSets.Empty.ifNull( indexChanges );
    }

    private DiffSets<IndexDescriptor> indexChangesDiffSets()
    {
        if ( indexChanges == null )
        {
            indexChanges = new DiffSets<>();
        }
        return indexChanges;
    }

    @Override
    public ReadableDiffSets<IndexDescriptor> constraintIndexChanges()
    {
        return ReadableDiffSets.Empty.ifNull( constraintIndexChanges );
    }

    private DiffSets<IndexDescriptor> constraintIndexChangesDiffSets()
    {
        if ( constraintIndexChanges == null )
        {
            constraintIndexChanges = new DiffSets<>();
        }
        return constraintIndexChanges;
    }

    @Override
    public ReadableDiffSets<Long> nodesWithChangedProperty( int propertyKeyId, Object value )
    {
        return propertyChangesForNodes != null
               ? propertyChangesForNodes.changesForProperty( propertyKeyId, value )
               : ReadableDiffSets.Empty.<Long>instance();
    }

    @Override
    public ReadableDiffSets<Long> addedAndRemovedNodes()
    {
        return ReadableDiffSets.Empty.ifNull( nodes );
    }

    private DiffSets<Long> nodes()
    {
        if ( nodes == null )
        {
            nodes = new DiffSets<>();
        }
        return nodes;
    }

    @Override
    public PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, PrimitiveLongIterator rels )
    {
        NodeState state;
        if ( nodeStatesMap != null && (state = nodeStatesMap.get( nodeId )) != null )
        {
            rels = state.augmentRelationships( direction, rels );
            // TODO: This should be handled by the augment call above
            rels = addedAndRemovedRelationships().augmentWithRemovals( rels );
        }
        return rels;
    }

    @Override
    public PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, int[] types, PrimitiveLongIterator rels )
    {
        NodeState state;
        if ( nodeStatesMap != null && (state = nodeStatesMap.get( nodeId )) != null )
        {
            rels = state.augmentRelationships( direction, types, rels );
            // TODO: This should be handled by the augment call above
            rels = addedAndRemovedRelationships().augmentWithRemovals( rels );
        }
        return rels;
    }

    @Override
    public PrimitiveLongIterator addedRelationships( long nodeId, int[] types, Direction direction )
    {
        return NODE_STATE.get( this, nodeId ).addedRelationships( direction, types );
    }

    @Override
    public int augmentNodeDegree( long nodeId, int degree, Direction direction )
    {
        return NODE_STATE.get( this, nodeId ).augmentDegree( direction, degree );
    }

    @Override
    public int augmentNodeDegree( long nodeId, int degree, Direction direction, int typeId )
    {
        return NODE_STATE.get( this, nodeId ).augmentDegree( direction, degree, typeId );
    }

    @Override
    public PrimitiveIntIterator nodeRelationshipTypes( long nodeId )
    {
        return NODE_STATE.get( this, nodeId ).relationshipTypes();
    }

    @Override
    public ReadableDiffSets<Long> addedAndRemovedRelationships()
    {
        return ReadableDiffSets.Empty.ifNull( relationships );
    }

    private DiffSets<Long> relationships()
    {
        if ( relationships == null )
        {
            relationships = new DiffSets<>();
        }
        return relationships;
    }

    @Override
    public Iterable<RelationshipState> modifiedRelationships()
    {
        return RELATIONSHIP_STATE.values( this );
    }

    private LabelState.Mutable getOrCreateLabelState( int labelId )
    {
        return LABEL_STATE.getOrCreate( this, labelId ) ;
    }

    private NodeState.Mutable getOrCreateNodeState( long nodeId )
    {
        return NODE_STATE.getOrCreate( this, nodeId );
    }

    private RelationshipState.Mutable getOrCreateRelationshipState( long relationshipId )
    {
        return RELATIONSHIP_STATE.getOrCreate( this, relationshipId );
    }

    private GraphState getOrCreateGraphState()
    {
        if ( graphState == null )
        {
            graphState = new GraphState();
        }
        return graphState;
    }

    @Override
    public void constraintDoAdd( UniquenessConstraint constraint, long indexId )
    {
        constraintsChangesDiffSets().add( constraint );
        createdConstraintIndexesByConstraint().put( constraint, indexId );
        getOrCreateLabelState( constraint.label() ).getOrCreateConstraintsChanges().add( constraint );
        changed();
    }

    @Override
    public ReadableDiffSets<UniquenessConstraint> constraintsChangesForLabelAndProperty( int labelId, final int propertyKey )
    {
        return LABEL_STATE.get( this, labelId ).constraintsChanges().filterAdded(
            new Predicate<UniquenessConstraint>()
            {
                @Override
                public boolean accept( UniquenessConstraint item )
                {
                    return item.propertyKeyId() == propertyKey;
                }
            } );
    }

    @Override
    public ReadableDiffSets<UniquenessConstraint> constraintsChangesForLabel( int labelId )
    {
        return LABEL_STATE.get( this, labelId ).constraintsChanges();
    }

    @Override
    public ReadableDiffSets<UniquenessConstraint> constraintsChanges()
    {
        return ReadableDiffSets.Empty.ifNull( constraintsChanges );
    }

    private DiffSets<UniquenessConstraint> constraintsChangesDiffSets()
    {
        if ( constraintsChanges == null )
        {
            constraintsChanges = new DiffSets<>();
        }
        return constraintsChanges;
    }

    @Override
    public void constraintDoDrop( UniquenessConstraint constraint )
    {
        constraintsChangesDiffSets().remove( constraint );

        constraintIndexDoDrop( new IndexDescriptor( constraint.label(), constraint.propertyKeyId() ));
        getOrCreateLabelState( constraint.label() ).getOrCreateConstraintsChanges().remove( constraint );
        changed();
    }

    @Override
    public boolean constraintDoUnRemove( UniquenessConstraint constraint )
    {
        if ( constraintsChangesDiffSets().unRemove( constraint ) )
        {
            getOrCreateLabelState( constraint.label() ).getOrCreateConstraintsChanges().unRemove( constraint );
            return true;
        }
        return false;
    }

    @Override
    public boolean constraintIndexDoUnRemove( IndexDescriptor index )
    {
        if ( constraintIndexChangesDiffSets().unRemove( index ) )
        {
            LABEL_STATE.getOrCreate( this, index.getLabelId() ).getOrCreateConstraintIndexChanges().unRemove( index );
            return true;
        }
        return false;
    }

    @Override
    public Iterable<IndexDescriptor> constraintIndexesCreatedInTx()
    {
        if ( createdConstraintIndexesByConstraint != null && !createdConstraintIndexesByConstraint.isEmpty() )
        {
            return map( new Function<UniquenessConstraint,IndexDescriptor>()
            {
                @Override
                public IndexDescriptor apply( UniquenessConstraint constraint )
                {
                    return new IndexDescriptor( constraint.label(), constraint.propertyKeyId() );
                }
            }, createdConstraintIndexesByConstraint.keySet() );
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
    public ReadableDiffSets<Long> indexUpdates( IndexDescriptor descriptor, Object value )
    {
        return ReadableDiffSets.Empty.ifNull(
                getIndexUpdates( descriptor.getLabelId(), /*create=*/false,
                                 property( descriptor.getPropertyKeyId(), value ) ) );
    }

    @Override
    public void indexDoUpdateProperty( IndexDescriptor descriptor, long nodeId,
                                       DefinedProperty propertyBefore, DefinedProperty propertyAfter )
    {
        DiffSets<Long> before = getIndexUpdates( descriptor.getLabelId(), true, propertyBefore );
        if ( before != null )
        {
            before.remove( nodeId );
            if ( before.getRemoved().contains( nodeId ) )
            {
                getOrCreateNodeState( nodeId ).addIndexDiff( before );
            }
            else
            {
                getOrCreateNodeState( nodeId ).removeIndexDiff( before );
            }
        }

        DiffSets<Long> after = getIndexUpdates( descriptor.getLabelId(), true, propertyAfter );
        if ( after != null )
        {
            after.add( nodeId );
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
        if ( createdConstraintIndexesByConstraint == null )
        {
            createdConstraintIndexesByConstraint = new HashMap<>();
        }
        return createdConstraintIndexesByConstraint;
    }

    private boolean hasNodeState( long nodeId )
    {
        return nodeStatesMap != null && nodeStatesMap.containsKey( nodeId );
    }

    private PropertyChanges nodePropertyChanges()
    {
        return propertyChangesForNodes == null ?
                propertyChangesForNodes = new PropertyChanges() : propertyChangesForNodes;
    }

    @Override
    public PrimitiveLongIterator augmentNodesGetAll( PrimitiveLongIterator committed )
    {
        return addedAndRemovedNodes().augment( committed );
    }

    @Override
    public PrimitiveLongIterator augmentRelationshipsGetAll( PrimitiveLongIterator committed )
    {
        return addedAndRemovedRelationships().augment( committed );
    }

    @Override
    public <EX extends Exception> boolean relationshipVisit( long relId, RelationshipVisitor<EX> visitor ) throws EX
    {
        return RELATIONSHIP_STATE.get( this, relId ).accept( visitor );
    }

    @Override
    public void nodeLegacyIndexDoCreate( String indexName, Map<String, String> customConfig )
    {
        assert customConfig != null;

        if ( createdNodeLegacyIndexes == null)
        {
            createdNodeLegacyIndexes = new HashMap<>(  );
        }

        createdNodeLegacyIndexes.put(indexName, customConfig);
        changed();
    }

    @Override
    public void relationshipLegacyIndexDoCreate( String indexName, Map<String, String> customConfig )
    {
        assert customConfig != null;

        if ( createdRelationshipLegacyIndexes == null)
        {
            createdRelationshipLegacyIndexes = new HashMap<>(  );
        }

        createdRelationshipLegacyIndexes.put(indexName, customConfig);
        changed();
    }

    @Override
    public boolean hasDataChanges()
    {
        return hasDataChanges;
    }
}
