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
import java.util.TreeMap;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Function;
import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.constraints.MandatoryPropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.cursor.LabelCursor;
import org.neo4j.kernel.api.cursor.NodeCursor;
import org.neo4j.kernel.api.cursor.PropertyCursor;
import org.neo4j.kernel.api.cursor.RelationshipCursor;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.txstate.ReadableTxState;
import org.neo4j.kernel.api.txstate.RelationshipChangeVisitorAdapter;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.api.txstate.UpdateTriState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.cursor.TxIteratorNodeCursor;
import org.neo4j.kernel.impl.api.cursor.TxIteratorRelationshipCursor;
import org.neo4j.kernel.impl.api.cursor.TxLabelCursor;
import org.neo4j.kernel.impl.api.cursor.TxPropertyCursor;
import org.neo4j.kernel.impl.api.cursor.TxSingleNodeCursor;
import org.neo4j.kernel.impl.api.cursor.TxSingleRelationshipCursor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.kernel.impl.util.diffsets.DiffSetsVisitor;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;
import org.neo4j.kernel.impl.util.diffsets.ReadableRelationshipDiffSets;
import org.neo4j.kernel.impl.util.diffsets.RelationshipDiffSets;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.toPrimitiveIterator;
import static org.neo4j.helpers.collection.Iterables.map;

/**
 * This class contains transaction-local changes to the graph. These changes can then be used to augment reads from the
 * committed state of the database (to make the local changes appear in local transaction read operations). At commit
 * time a visitor is sent into this class to convert the end result of the tx changes into a physical changeset.
 * <p/>
 * See {@link org.neo4j.kernel.impl.api.KernelTransactionImplementation} for how this happens.
 * <p/>
 * This class is very large, as it has been used as a gathering point to consolidate all transaction state knowledge
 * into one component. Now that that work is done, this class should be refactored to increase transparency in how it
 * works.
 */
public final class TxState implements TransactionState, RelationshipVisitor.Home
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

    private Map<Integer/*Token ID*/, String> createdLabelTokens;
    private Map<Integer/*Token ID*/, String> createdPropertyKeyTokens;
    private Map<Integer/*Token ID*/, String> createdRelationshipTypeTokens;

    private GraphState graphState;
    private DiffSets<IndexDescriptor> indexChanges;
    private DiffSets<IndexDescriptor> constraintIndexChanges;
    private DiffSets<PropertyConstraint> constraintsChanges;

    private PropertyChanges propertyChangesForNodes;

    // Tracks added and removed nodes, not modified nodes
    private DiffSets<Long> nodes;

    // Tracks added and removed relationships, not modified relationships
    private RelationshipDiffSets<Long> relationships;

    // This is temporary. It is needed until we've removed nodes and rels from the global cache, to tell
    // that they were created and then deleted in the same tx. This is here just to set a save point to
    // get a large set of changes in, and is meant to be removed in the coming days in a follow-up commit.
    private final Set<Long> nodesDeletedInTx = new HashSet<>();
    private final Set<Long> relationshipsDeletedInTx = new HashSet<>();

    private Map<UniquenessConstraint, Long> createdConstraintIndexesByConstraint;

    private Map<String, Map<String, String>> createdNodeLegacyIndexes;
    private Map<String, Map<String, String>> createdRelationshipLegacyIndexes;

    private PrimitiveIntObjectMap<Map<DefinedProperty, DiffSets<Long>>> indexUpdates;

    private InstanceCache<TxIteratorNodeCursor> iteratorNodeCursor;
    private InstanceCache<TxSingleNodeCursor> singleNodeCursor;
    private InstanceCache<TxIteratorRelationshipCursor> iteratorRelationshipCursor;
    private InstanceCache<TxSingleRelationshipCursor> singleRelationshipCursor;
    private InstanceCache<TxPropertyCursor> propertyCursor;
    private InstanceCache<TxLabelCursor> labelCursor;

    private boolean hasChanges;

    public TxState()
    {
        singleNodeCursor = new InstanceCache<TxSingleNodeCursor>()
        {
            @Override
            protected TxSingleNodeCursor create()
            {
                return new TxSingleNodeCursor( TxState.this, this );
            }
        };
        iteratorNodeCursor = new InstanceCache<TxIteratorNodeCursor>()
        {
            @Override
            protected TxIteratorNodeCursor create()
            {
                return new TxIteratorNodeCursor( TxState.this, this );
            }
        };
        propertyCursor = new InstanceCache<TxPropertyCursor>()
        {
            @Override
            protected TxPropertyCursor create()
            {
                return new TxPropertyCursor( this );
            }
        };
        labelCursor = new InstanceCache<TxLabelCursor>()
        {
            @Override
            protected TxLabelCursor create()
            {
                return new TxLabelCursor( this );
            }
        };
        singleRelationshipCursor = new InstanceCache<TxSingleRelationshipCursor>()
        {
            @Override
            protected TxSingleRelationshipCursor create()
            {
                return new TxSingleRelationshipCursor( TxState.this, this );
            }
        };

        iteratorRelationshipCursor = new InstanceCache<TxIteratorRelationshipCursor>()
        {
            @Override
            protected TxIteratorRelationshipCursor create()
            {
                return new TxIteratorRelationshipCursor( TxState.this, this );
            }
        };
    }

    @Override
    public void accept( final TxStateVisitor visitor ) throws ConstraintValidationKernelException
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
            for ( Map.Entry<Integer, String> entry : createdLabelTokens.entrySet() )
            {
                visitor.visitCreatedLabelToken( entry.getValue(), entry.getKey() );
            }
        }

        if ( createdPropertyKeyTokens != null )
        {
            for ( Map.Entry<Integer, String> entry : createdPropertyKeyTokens.entrySet() )
            {
                visitor.visitCreatedPropertyKeyToken( entry.getValue(), entry.getKey() );
            }
        }

        if ( createdRelationshipTypeTokens != null )
        {
            for ( Map.Entry<Integer, String> entry : createdRelationshipTypeTokens.entrySet() )
            {
                visitor.visitCreatedRelationshipTypeToken( entry.getValue(), entry.getKey() );
            }
        }

        if ( createdNodeLegacyIndexes != null )
        {
            for ( Map.Entry<String, Map<String, String>> entry : createdNodeLegacyIndexes.entrySet() )
            {
                visitor.visitCreatedNodeLegacyIndex( entry.getKey(), entry.getValue() );
            }
        }

        if ( createdRelationshipLegacyIndexes != null )
        {
            for ( Map.Entry<String, Map<String, String>> entry : createdRelationshipLegacyIndexes.entrySet() )
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

    private static DiffSetsVisitor<PropertyConstraint> constraintsVisitor( final TxStateVisitor visitor )
    {
        return new ConstraintDiffSetsVisitor( visitor );
    }

    static class ConstraintDiffSetsVisitor implements PropertyConstraint.ChangeVisitor, DiffSetsVisitor<PropertyConstraint>
    {
        private final TxStateVisitor visitor;

        ConstraintDiffSetsVisitor( TxStateVisitor visitor )
        {
            this.visitor = visitor;
        }

        @Override
        public void visitAdded( PropertyConstraint element )
        {
            element.added( this );
        }

        @Override
        public void visitRemoved( PropertyConstraint element )
        {
            element.removed( this );
        }

        @Override
        public void visitAddedUniquePropertyConstraint( UniquenessConstraint constraint )
        {
            visitor.visitAddedUniquePropertyConstraint( constraint );
        }

        @Override
        public void visitRemovedUniquePropertyConstraint( UniquenessConstraint constraint )
        {
            visitor.visitRemovedUniquePropertyConstraint( constraint );
        }

        @Override
        public void visitAddedMandatoryPropertyConstraint( MandatoryPropertyConstraint constraint )
        {
            visitor.visitAddedMandatoryPropertyConstraint( constraint );
        }

        @Override
        public void visitRemovedMandatoryPropertyConstraint( MandatoryPropertyConstraint constraint )
        {
            visitor.visitRemovedMandatoryPropertyConstraint( constraint );
        }
    }

    private static DiffSetsVisitor<IndexDescriptor> indexVisitor( final TxStateVisitor visitor,
            final boolean forConstraint )
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
                    throws ConstraintValidationKernelException
            {
                visitor.visitNodeLabelChanges( nodeId, added, removed );
            }

            @Override
            public void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,

                                              Iterator<DefinedProperty> changed, Iterator<Integer> removed)
                    throws ConstraintValidationKernelException
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

    private static PropertyContainerState.Visitor relVisitor( final TxStateVisitor visitor )
    {
        return new PropertyContainerState.Visitor()
        {
            @Override
            public void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,
                    Iterator<DefinedProperty> changed, Iterator<Integer> removed )
            {
                visitor.visitRelPropertyChanges( entityId, added, changed, removed );
            }
        };
    }

    private static PropertyContainerState.Visitor graphPropertyVisitor( final TxStateVisitor visitor )
    {
        return new PropertyContainerState.Visitor()
        {
            @Override
            public void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,
                    Iterator<DefinedProperty> changed, Iterator<Integer> removed )
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

    @Override
    public void nodeDoCreate( long id )
    {
        nodes().add( id );
        hasChanges = true;
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
        hasChanges = true;
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

        hasChanges = true;
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

        hasChanges = true;
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
        hasChanges = true;
    }

    @Override
    public void relationshipDoReplaceProperty( long relationshipId,
            Property replacedProperty,
            DefinedProperty newProperty )
    {
        if ( replacedProperty.isDefined() )
        {
            getOrCreateRelationshipState( relationshipId ).changeProperty( newProperty );
        }
        else
        {
            getOrCreateRelationshipState( relationshipId ).addProperty( newProperty );
        }
        hasChanges = true;
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
        hasChanges = true;
    }

    @Override
    public void nodeDoRemoveProperty( long nodeId, DefinedProperty removedProperty )
    {
        getOrCreateNodeState( nodeId ).removeProperty( removedProperty );
        nodePropertyChanges().removeProperty( nodeId, removedProperty.propertyKeyId(),
                removedProperty.value() );
        hasChanges = true;
    }

    @Override
    public void relationshipDoRemoveProperty( long relationshipId, DefinedProperty removedProperty )
    {
        getOrCreateRelationshipState( relationshipId ).removeProperty( removedProperty );
        hasChanges = true;
    }

    @Override
    public void graphDoRemoveProperty( DefinedProperty removedProperty )
    {
        getOrCreateGraphState().removeProperty( removedProperty );
        hasChanges = true;
    }

    @Override
    public void nodeDoAddLabel( int labelId, long nodeId )
    {
        getOrCreateLabelStateNodeDiffSets( labelId ).add( nodeId );
        getOrCreateNodeStateLabelDiffSets( nodeId ).add( labelId );
        hasChanges = true;
    }

    @Override
    public void nodeDoRemoveLabel( int labelId, long nodeId )
    {
        getOrCreateLabelStateNodeDiffSets( labelId ).remove( nodeId );
        getOrCreateNodeStateLabelDiffSets( nodeId ).remove( labelId );
        hasChanges = true;
    }

    @Override
    public void labelDoCreateForName( String labelName, int id )
    {
        if ( createdLabelTokens == null )
        {
            createdLabelTokens = new HashMap<>();
        }
        createdLabelTokens.put( id, labelName );

        hasChanges = true;
    }

    @Override
    public void propertyKeyDoCreateForName( String propertyKeyName, int id )
    {
        if ( createdPropertyKeyTokens == null )
        {
            createdPropertyKeyTokens = new HashMap<>();
        }
        createdPropertyKeyTokens.put( id, propertyKeyName );

        hasChanges = true;
    }

    @Override
    public void relationshipTypeDoCreateForName( String labelName, int id )
    {
        if ( createdRelationshipTypeTokens == null )
        {
            createdRelationshipTypeTokens = new HashMap<>();
        }
        createdRelationshipTypeTokens.put( id, labelName );

        hasChanges = true;
    }

    @Override
    public UpdateTriState labelState( long nodeId, int labelId )
    {
        return NODE_STATE.get( this, nodeId ).labelState( labelId );
    }

    @Override
    public NodeState getNodeState( long id )
    {
        return NODE_STATE.get( this, id );
    }

    @Override
    public RelationshipState getRelationshipState( long id )
    {
        return RELATIONSHIP_STATE.get( this, id );
    }

    public NodeCursor augmentSingleNodeCursor( NodeCursor cursor )
    {
        return hasChanges ? singleNodeCursor.get().init( cursor ) : cursor;
    }

    public PropertyCursor augmentPropertyCursor( PropertyCursor cursor, PropertyContainerState propertyContainerState )
    {
        return propertyContainerState.augmentPropertyCursor( propertyCursor, cursor );
    }

    public LabelCursor augmentLabelCursor( LabelCursor cursor, NodeState nodeState )
    {
        return nodeState.augmentLabelCursor( labelCursor, cursor );
    }

    public RelationshipCursor augmentSingleRelationshipCursor( RelationshipCursor cursor )
    {
        return hasChanges ? singleRelationshipCursor.get().init( cursor ) : cursor;
    }

    @Override
    public RelationshipCursor augmentIteratorRelationshipCursor( RelationshipCursor cursor,
            RelationshipIterator iterator )
    {
        return hasChanges ? iteratorRelationshipCursor.get().init( cursor, iterator ) : cursor;
    }

    public RelationshipCursor augmentNodeRelationshipCursor( RelationshipCursor cursor,
            NodeState nodeState,
            Direction direction,
            int[] relTypes )
    {
        return nodeState.augmentNodeRelationshipCursor( iteratorRelationshipCursor, cursor, direction, relTypes );
    }

    @Override
    public NodeCursor augmentNodesGetAllCursor( NodeCursor cursor )
    {
        return hasChanges && !nodes.isEmpty() ? iteratorNodeCursor.get().init( cursor,
                nodes.getAdded().iterator() ) : cursor;
    }

    @Override
    public RelationshipCursor augmentRelationshipsGetAllCursor( RelationshipCursor cursor )
    {
        return hasChanges && !relationships.isEmpty() ? iteratorRelationshipCursor.get().init( cursor,
                toPrimitiveIterator( relationships.getAdded().iterator() )) : cursor;
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
        hasChanges = true;
    }

    @Override
    public void constraintIndexRuleDoAdd( IndexDescriptor descriptor )
    {
        constraintIndexChangesDiffSets().add( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).getOrCreateConstraintIndexChanges().add( descriptor );
        hasChanges = true;
    }

    @Override
    public void indexDoDrop( IndexDescriptor descriptor )
    {
        indexChangesDiffSets().remove( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).getOrCreateIndexChanges().remove( descriptor );
        hasChanges = true;
    }

    @Override
    public void constraintIndexDoDrop( IndexDescriptor descriptor )
    {
        constraintIndexChangesDiffSets().remove( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).getOrCreateConstraintIndexChanges().remove( descriptor );
        hasChanges = true;
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
    public RelationshipIterator augmentRelationships( long nodeId, Direction direction, RelationshipIterator rels )
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
    public RelationshipIterator augmentRelationships( long nodeId,
            Direction direction,
            int[] types,
            RelationshipIterator rels )
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
    public ReadableRelationshipDiffSets<Long> addedAndRemovedRelationships()
    {
        return ReadableRelationshipDiffSets.Empty.ifNull( relationships );
    }

    private RelationshipDiffSets<Long> relationships()
    {
        if ( relationships == null )
        {
            relationships = new RelationshipDiffSets<>( this );
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
        return LABEL_STATE.getOrCreate( this, labelId );
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
        hasChanges = true;
    }

    @Override
    public void constraintDoAdd( MandatoryPropertyConstraint constraint )
    {
        constraintsChangesDiffSets().add( constraint );
        getOrCreateLabelState( constraint.label() ).getOrCreateConstraintsChanges().add( constraint );
        hasChanges = true;
    }

    @Override
    public ReadableDiffSets<PropertyConstraint> constraintsChangesForLabelAndProperty( int labelId,
            final int propertyKey )
    {
        return LABEL_STATE.get( this, labelId ).constraintsChanges().filterAdded(
                new Predicate<PropertyConstraint>()
                {
                    @Override
                    public boolean test( PropertyConstraint item )
                    {
                        return item.propertyKeyId() == propertyKey;
                    }
                } );
    }

    @Override
    public ReadableDiffSets<PropertyConstraint> constraintsChangesForLabel( int labelId )
    {
        return LABEL_STATE.get( this, labelId ).constraintsChanges();
    }

    @Override
    public ReadableDiffSets<PropertyConstraint> constraintsChanges()
    {
        return ReadableDiffSets.Empty.ifNull( constraintsChanges );
    }

    private DiffSets<PropertyConstraint> constraintsChangesDiffSets()
    {
        if ( constraintsChanges == null )
        {
            constraintsChanges = new DiffSets<>();
        }
        return constraintsChanges;
    }

    @Override
    public void constraintDoDrop( PropertyConstraint constraint )
    {
        constraintsChangesDiffSets().remove( constraint );


        if ( constraint instanceof UniquenessConstraint )
        {
            constraintIndexDoDrop( new IndexDescriptor( constraint.label(), constraint.propertyKeyId() ));
        }
        getOrCreateLabelState( constraint.label() ).getOrCreateConstraintsChanges().remove( constraint );
        hasChanges = true;
    }

    @Override
    public boolean constraintDoUnRemove( PropertyConstraint constraint )
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

            return map( new Function<PropertyConstraint,IndexDescriptor>()
            {
                @Override
                public IndexDescriptor apply( PropertyConstraint constraint )
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
        return ReadableDiffSets.Empty.ifNull( (value == null) ?
                getIndexUpdates( descriptor.getLabelId(), descriptor.getPropertyKeyId() ) :
                getIndexUpdates( descriptor.getLabelId(), /*create=*/false,
                        Property.property( descriptor.getPropertyKeyId(), value ) ) );
    }

    @Override
    public ReadableDiffSets<Long> indexUpdatesForPrefix( IndexDescriptor descriptor, String prefix )
    {
        return ReadableDiffSets.Empty.ifNull( getIndexUpdatesForPrefix( descriptor, prefix ) );
    }

    private ReadableDiffSets<Long> getIndexUpdatesForPrefix( IndexDescriptor descriptor, String prefix )
    {
        if ( indexUpdates == null )
        {
            return null;
        }
        Map<DefinedProperty, DiffSets<Long>> updates = indexUpdates.get( descriptor.getLabelId() );
        if ( updates == null )
        {
            return null;
        }
        TreeMap<DefinedProperty,DiffSets<Long>> sortedUpdates = null;
        if ( updates instanceof TreeMap )
        {
            sortedUpdates = (TreeMap<DefinedProperty,DiffSets<Long>>) updates;
        }
        else
        {
            sortedUpdates = new TreeMap<>();
            sortedUpdates.putAll( updates );
            indexUpdates.put( descriptor.getLabelId(), sortedUpdates );
        }
        DiffSets<Long> diffs = new DiffSets<Long>();
        DefinedProperty floor = DefinedProperty.stringProperty( descriptor.getPropertyKeyId(), prefix );
        for ( Map.Entry<DefinedProperty,DiffSets<Long>> entry : sortedUpdates.tailMap( floor ).entrySet() )
        {
            if ( entry.getKey().value().toString().startsWith( prefix ) )
            {
                DiffSets<Long> diffSets = entry.getValue();
                diffs.addAll( diffSets.getAdded().iterator() );
                diffs.removeAll( diffSets.getRemoved().iterator() );
            }
            else
            {
                break;
            }
        }
        return diffs;
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

    private DiffSets<Long> getIndexUpdates( int label, int propertyKeyId )
    {
        if ( indexUpdates == null )
        {
            return null;
        }
        Map<DefinedProperty, DiffSets<Long>> updates = indexUpdates.get( label );
        if ( updates == null )
        {
            return null;
        }
        DiffSets<Long> diffs = new DiffSets<>();
        for ( Map.Entry<DefinedProperty, DiffSets<Long>> entry : updates.entrySet() )
        {
            if ( entry.getKey().propertyKeyId() == propertyKeyId )
            {
                diffs.addAll( entry.getValue().getAdded().iterator() );
                diffs.removeAll( entry.getValue().getRemoved().iterator() );
            }
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
    public RelationshipIterator augmentRelationshipsGetAll( RelationshipIterator committed )
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

        if ( createdNodeLegacyIndexes == null )
        {
            createdNodeLegacyIndexes = new HashMap<>();
        }

        createdNodeLegacyIndexes.put( indexName, customConfig );

        hasChanges = true;
    }

    @Override
    public void relationshipLegacyIndexDoCreate( String indexName, Map<String, String> customConfig )
    {
        assert customConfig != null;

        if ( createdRelationshipLegacyIndexes == null )
        {
            createdRelationshipLegacyIndexes = new HashMap<>();
        }

        createdRelationshipLegacyIndexes.put( indexName, customConfig );

        hasChanges = true;
    }
}
