/*
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.txstate.RelationshipChangeVisitorAdapter;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.cursor.TxAllPropertyCursor;
import org.neo4j.kernel.impl.api.cursor.TxIteratorNodeCursor;
import org.neo4j.kernel.impl.api.cursor.TxIteratorRelationshipCursor;
import org.neo4j.kernel.impl.api.cursor.TxLabelCursor;
import org.neo4j.kernel.impl.api.cursor.TxSingleLabelCursor;
import org.neo4j.kernel.impl.api.cursor.TxSingleNodeCursor;
import org.neo4j.kernel.impl.api.cursor.TxSinglePropertyCursor;
import org.neo4j.kernel.impl.api.cursor.TxSingleRelationshipCursor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.kernel.impl.util.diffsets.RelationshipDiffSets;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.LabelItem;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.DiffSetsVisitor;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.storageengine.api.txstate.ReadableRelationshipDiffSets;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.RelationshipState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.toPrimitiveIterator;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.api.properties.Property.numberProperty;
import static org.neo4j.kernel.impl.api.PropertyValueComparison.SuperType.NUMBER;
import static org.neo4j.kernel.impl.api.PropertyValueComparison.SuperType.STRING;

/**
 * This class contains transaction-local changes to the graph. These changes can then be used to augment reads from the
 * committed state of the database (to make the local changes appear in local transaction read operations). At commit
 * time a visitor is sent into this class to convert the end result of the tx changes into a physical changeset.
 * <p>
 * See {@link org.neo4j.kernel.impl.api.KernelTransactionImplementation} for how this happens.
 * <p>
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
    private Map<Long/*Node ID*/, NodeStateImpl> nodeStatesMap;
    private static final NodeStateImpl.Defaults NODE_STATE = new NodeStateImpl.Defaults()
    {
        @Override
        Map<Long, NodeStateImpl> getMap( TxState state )
        {
            return state.nodeStatesMap;
        }

        @Override
        void setMap( TxState state, Map<Long, NodeStateImpl> map )
        {
            state.nodeStatesMap = map;
        }
    };
    private Map<Long/*Relationship ID*/, RelationshipStateImpl> relationshipStatesMap;
    private static final RelationshipStateImpl.Defaults RELATIONSHIP_STATE = new RelationshipStateImpl.Defaults()
    {
        @Override
        Map<Long, RelationshipStateImpl> getMap( TxState state )
        {
            return state.relationshipStatesMap;
        }

        @Override
        void setMap( TxState state, Map<Long, RelationshipStateImpl> map )
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

    /**
     * These two sets are needed because create-delete in same transaction is a no-op in {@link DiffSets}
     * but we still need to provide correct answer in {@link #nodeIsDeletedInThisTx(long)} and
     * {@link #relationshipIsDeletedInThisTx(long)} methods.
     */
    private PrimitiveLongSet nodesDeletedInTx;
    private PrimitiveLongSet relationshipsDeletedInTx;

    private Map<UniquenessConstraint, Long> createdConstraintIndexesByConstraint;

    private PrimitiveIntObjectMap<Map<DefinedProperty, DiffSets<Long>>> indexUpdates;
    private PrimitiveIntObjectMap<DiffSets<RelationshipPropertyConstraint>> relationshipConstraintChanges;

    private InstanceCache<TxIteratorNodeCursor> iteratorNodeCursor;
    private InstanceCache<TxSingleNodeCursor> singleNodeCursor;
    private InstanceCache<TxIteratorRelationshipCursor> iteratorRelationshipCursor;
    private InstanceCache<TxSingleRelationshipCursor> singleRelationshipCursor;
    private InstanceCache<TxAllPropertyCursor> propertyCursor;
    private InstanceCache<TxSinglePropertyCursor> singlePropertyCursor;
    private InstanceCache<TxLabelCursor> labelCursor;
    private InstanceCache<TxSingleLabelCursor> singleLabelCursor;

    private boolean hasChanges, hasDataChanges;

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
        propertyCursor = new InstanceCache<TxAllPropertyCursor>()
        {
            @Override
            protected TxAllPropertyCursor create()
            {
                return new TxAllPropertyCursor( (Consumer) this );
            }
        };
        singlePropertyCursor = new InstanceCache<TxSinglePropertyCursor>()
        {
            @Override
            protected TxSinglePropertyCursor create()
            {
                return new TxSinglePropertyCursor( (Consumer) this );
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
        singleLabelCursor = new InstanceCache<TxSingleLabelCursor>()
        {
            @Override
            protected TxSingleLabelCursor create()
            {
                return new TxSingleLabelCursor( this );
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
    public void accept( final TxStateVisitor visitor )
            throws ConstraintValidationKernelException, CreateConstraintFailureException
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

    private static DiffSetsVisitor<Long> createdRelationshipsVisitor( ReadableTransactionState tx, final TxStateVisitor visitor )
    {
        return new RelationshipChangeVisitorAdapter( tx )
        {
            @Override
            protected void visitAddedRelationship( long relationshipId, int type, long startNode, long endNode )
                    throws ConstraintValidationKernelException
            {
                visitor.visitCreatedRelationship( relationshipId, type, startNode, endNode );
            }
        };
    }

    private static DiffSetsVisitor<PropertyConstraint> constraintsVisitor( final TxStateVisitor visitor )
    {
        return new ConstraintDiffSetsVisitor( visitor );
    }

    static class ConstraintDiffSetsVisitor implements PropertyConstraint.ChangeVisitor,
            DiffSetsVisitor<PropertyConstraint>
    {
        private final TxStateVisitor visitor;

        ConstraintDiffSetsVisitor( TxStateVisitor visitor )
        {
            this.visitor = visitor;
        }

        @Override
        public void visitAdded( PropertyConstraint element ) throws CreateConstraintFailureException
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
        public void visitAddedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint constraint )
                throws CreateConstraintFailureException
        {
            visitor.visitAddedNodePropertyExistenceConstraint( constraint );
        }

        @Override
        public void visitRemovedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint constraint )
        {
            visitor.visitRemovedNodePropertyExistenceConstraint( constraint );
        }

        @Override
        public void visitAddedRelationshipPropertyExistenceConstraint(
                RelationshipPropertyExistenceConstraint constraint ) throws CreateConstraintFailureException
        {
            visitor.visitAddedRelationshipPropertyExistenceConstraint( constraint );
        }

        @Override
        public void visitRemovedRelationshipPropertyExistenceConstraint(
                RelationshipPropertyExistenceConstraint constraint )
        {
            visitor.visitRemovedRelationshipPropertyExistenceConstraint( constraint );
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
            public void visitPropertyChanges( long entityId, Iterator<StorageProperty> added,
                    Iterator<StorageProperty> changed, Iterator<Integer> removed )
                    throws ConstraintValidationKernelException
            {
                visitor.visitNodePropertyChanges( entityId, added, changed, removed );
            }
        };
    }

    private static PropertyContainerState.Visitor relVisitor( final TxStateVisitor visitor )
    {
        return visitor::visitRelPropertyChanges;
    }

    private static PropertyContainerState.Visitor graphPropertyVisitor( final TxStateVisitor visitor )
    {
        return ( entityId, added, changed, removed ) -> visitor.visitGraphPropertyChanges( added, changed, removed );
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
    public Iterator<StorageProperty> augmentGraphProperties( Iterator<StorageProperty> original )
    {
        if ( graphState != null )
        {
            return graphState.augmentProperties( original );
        }
        return original;
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
            recordNodeDeleted( nodeId );
        }

        if ( nodeStatesMap != null )
        {
            NodeStateImpl nodeState = nodeStatesMap.remove( nodeId );
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
        return nodesDeletedInTx != null && nodesDeletedInTx.contains( nodeId );
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
            recordRelationshipDeleted( id );
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
            RelationshipStateImpl removed = relationshipStatesMap.remove( id );
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
        return relationshipsDeletedInTx != null && relationshipsDeletedInTx.contains( relationshipId );
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
            NodeStateImpl nodeState = getOrCreateNodeState( nodeId );
            nodeState.addProperty( newProperty );
            nodePropertyChanges().addProperty( nodeId, newProperty.propertyKeyId(), newProperty.value() );
        }
        dataChanged();
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
    public NodeState getNodeState( long id )
    {
        return NODE_STATE.get( this, id );
    }

    @Override
    public RelationshipState getRelationshipState( long id )
    {
        return RELATIONSHIP_STATE.get( this, id );
    }

    @Override
    public Cursor<NodeItem> augmentSingleNodeCursor( Cursor<NodeItem> cursor, long nodeId )
    {
        return hasChanges ? singleNodeCursor.get().init( cursor, nodeId ) : cursor;
    }

    @Override
    public Cursor<PropertyItem> augmentPropertyCursor( Cursor<PropertyItem> cursor,
            PropertyContainerState propertyContainerState )
    {
        return propertyContainerState.hasChanges() ?
                propertyCursor.get().init( cursor, propertyContainerState ) : cursor;
    }

    @Override
    public Cursor<PropertyItem> augmentSinglePropertyCursor( Cursor<PropertyItem> cursor,
            PropertyContainerState propertyContainerState, int propertyKeyId )
    {
        return propertyContainerState.hasChanges() ?
                singlePropertyCursor.get().init( cursor, propertyContainerState, propertyKeyId ) : cursor;
    }

    @Override
    public Cursor<LabelItem> augmentLabelCursor( Cursor<LabelItem> cursor, NodeState nodeState )
    {
        ReadableDiffSets<Integer> labelDiffSets = nodeState.labelDiffSets();

        if ( labelDiffSets.isEmpty() )
        {
            return cursor;
        }
        else
        {
            return labelCursor.get().init( cursor, labelDiffSets );
        }
    }

    @Override
    public Cursor<LabelItem> augmentSingleLabelCursor( Cursor<LabelItem> cursor, NodeState nodeState, int labelId )
    {
        ReadableDiffSets<Integer> labelDiffSets = nodeState.labelDiffSets();

        if ( labelDiffSets.isEmpty() )
        {
            return cursor;
        }
        else
        {
            return singleLabelCursor.get().init( cursor, labelDiffSets, labelId );
        }
    }

    @Override
    public Cursor<RelationshipItem> augmentSingleRelationshipCursor( Cursor<RelationshipItem> cursor,
            long relationshipId )
    {
        return hasChanges ? singleRelationshipCursor.get().init( cursor, relationshipId ) : cursor;
    }

    @Override
    public Cursor<RelationshipItem> augmentIteratorRelationshipCursor( Cursor<RelationshipItem> cursor,
            RelationshipIterator iterator )
    {
        return hasChanges ? iteratorRelationshipCursor.get().init( cursor, iterator ) : cursor;
    }

    @Override
    public Cursor<RelationshipItem> augmentNodeRelationshipCursor( Cursor<RelationshipItem> cursor,
            NodeState nodeState,
            Direction direction,
            int[] relTypes )
    {
        if ( nodeState.hasChanges() )
        {
            if ( relTypes == null )
            {
                return iteratorRelationshipCursor.get().init( cursor, nodeState.getAddedRelationships( direction ) );
            }
            return iteratorRelationshipCursor.get().init( cursor,
                    nodeState.getAddedRelationships( direction, relTypes ) );
        }
        return cursor;
    }

    @Override
    public Cursor<NodeItem> augmentNodesGetAllCursor( Cursor<NodeItem> cursor )
    {
        return hasChanges && nodes != null && !nodes.isEmpty()
               ? iteratorNodeCursor.get().init( cursor, nodes.getAdded().iterator() )
               : cursor;
    }

    @Override
    public Cursor<RelationshipItem> augmentRelationshipsGetAllCursor( Cursor<RelationshipItem> cursor )
    {
        return hasChanges && relationships != null && !relationships.isEmpty()
               ? iteratorRelationshipCursor.get().init( cursor, toPrimitiveIterator( relationships.getAdded().iterator() ) )
               : cursor;
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

    private NodeStateImpl getOrCreateNodeState( long nodeId )
    {
        return NODE_STATE.getOrCreate( this, nodeId );
    }

    private RelationshipStateImpl getOrCreateRelationshipState( long relationshipId )
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
    public void constraintDoAdd( NodePropertyExistenceConstraint constraint )
    {
        constraintsChangesDiffSets().add( constraint );
        getOrCreateLabelState( constraint.label() ).getOrCreateConstraintsChanges().add( constraint );
        hasChanges = true;
    }

    @Override
    public void constraintDoAdd( RelationshipPropertyExistenceConstraint constraint )
    {
        constraintsChangesDiffSets().add( constraint );
        relationshipConstraintChangesByType( constraint.relationshipType() ).add( constraint );
        hasChanges = true;
    }

    @Override
    public ReadableDiffSets<NodePropertyConstraint> constraintsChangesForLabelAndProperty( int labelId,
            final int propertyKey )
    {
        return LABEL_STATE.get( this, labelId ).nodeConstraintsChanges().filterAdded(
                item -> item.propertyKey() == propertyKey );
    }

    @Override
    public ReadableDiffSets<NodePropertyConstraint> constraintsChangesForLabel( int labelId )
    {
        return LABEL_STATE.get( this, labelId ).nodeConstraintsChanges();
    }

    @Override
    public ReadableDiffSets<RelationshipPropertyConstraint> constraintsChangesForRelationshipType( int relTypeId )
    {
        DiffSets<RelationshipPropertyConstraint> changes = null;
        if ( relationshipConstraintChanges != null )
        {
            changes = relationshipConstraintChanges.get( relTypeId );
        }
        return ReadableDiffSets.Empty.ifNull( changes );
    }

    @Override
    public ReadableDiffSets<RelationshipPropertyConstraint> constraintsChangesForRelationshipTypeAndProperty(
            int relTypeId, final int propertyKey )
    {
        return constraintsChangesForRelationshipType( relTypeId ).filterAdded(
                constraint -> constraint.propertyKey() == propertyKey
        );
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
    public void constraintDoDrop( NodePropertyConstraint constraint )
    {
        constraintsChangesDiffSets().remove( constraint );


        if ( constraint instanceof UniquenessConstraint )
        {
            constraintIndexDoDrop( new IndexDescriptor( constraint.label(), constraint.propertyKey() ) );
        }
        getOrCreateLabelState( constraint.label() ).getOrCreateConstraintsChanges().remove( constraint );
        changed();
    }

    @Override
    public void constraintDoDrop( RelationshipPropertyConstraint constraint )
    {
        constraintsChangesDiffSets().remove( constraint );
        relationshipConstraintChangesByType( constraint.relationshipType() ).remove( constraint );
        hasChanges = true;
    }

    private DiffSets<RelationshipPropertyConstraint> relationshipConstraintChangesByType( int relTypeId )
    {
        if ( relationshipConstraintChanges == null )
        {
            relationshipConstraintChanges = Primitive.intObjectMap();
        }
        DiffSets<RelationshipPropertyConstraint> diffSets = relationshipConstraintChanges.get( relTypeId );
        if ( diffSets == null )
        {
            relationshipConstraintChanges.put( relTypeId, diffSets = new DiffSets<>() );
        }
        return diffSets;
    }

    @Override
    public boolean constraintDoUnRemove( NodePropertyConstraint constraint )
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

            return map( constraint -> {
                return new IndexDescriptor( constraint.label(), constraint.propertyKey() );
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
    public ReadableDiffSets<Long> indexUpdatesForScanOrSeek( IndexDescriptor descriptor, Object value )
    {
        return ReadableDiffSets.Empty.ifNull( (value == null) ?
            getIndexUpdatesForScanOrSeek(
                    descriptor.getLabelId(),
                    descriptor.getPropertyKeyId()
            )
            :
            getIndexUpdatesForScanOrSeek(
                    descriptor.getLabelId(), /*create=*/false,
                    Property.property( descriptor.getPropertyKeyId(), value )
            )
        );
    }

    @Override
    public ReadableDiffSets<Long> indexUpdatesForRangeSeekByNumber( IndexDescriptor descriptor,
                                                                    Number lower, boolean includeLower,
                                                                    Number upper, boolean includeUpper )
    {
        return ReadableDiffSets.Empty.ifNull(
            getIndexUpdatesForRangeSeekByNumber( descriptor, lower, includeLower, upper, includeUpper )
        );
    }

    private ReadableDiffSets<Long> getIndexUpdatesForRangeSeekByNumber( IndexDescriptor descriptor,
                                                                        Number lower, boolean includeLower,
                                                                        Number upper, boolean includeUpper )
    {
        TreeMap<DefinedProperty, DiffSets<Long>> sortedUpdates = getSortedIndexUpdates( descriptor );
        if ( sortedUpdates == null )
        {
            return null;
        }

        DefinedProperty selectedLower;
        boolean selectedIncludeLower;

        DefinedProperty selectedUpper;
        boolean selectedIncludeUpper;

        int propertyKeyId = descriptor.getPropertyKeyId();

        if ( lower == null )
        {
            selectedLower = DefinedProperty.numberProperty( propertyKeyId, NUMBER.lowLimit.castValue( Number.class ) );
            selectedIncludeLower = NUMBER.lowLimit.isInclusive;
        }
        else
        {
            selectedLower = numberProperty( propertyKeyId, lower );
            selectedIncludeLower = includeLower;
        }

        if ( upper == null )
        {
            selectedUpper = DefinedProperty.numberProperty( propertyKeyId, NUMBER.highLimit.castValue( Number.class ) );
            selectedIncludeUpper = NUMBER.highLimit.isInclusive;
        }
        else
        {
            selectedUpper = numberProperty( propertyKeyId, upper );
            selectedIncludeUpper = includeUpper;
        }

        DiffSets<Long> diffs = new DiffSets<>();
        for ( Map.Entry<DefinedProperty,DiffSets<Long>> entry : sortedUpdates.subMap( selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper ).entrySet() )
        {
            DiffSets<Long> diffSets = entry.getValue();
            diffs.addAll( diffSets.getAdded().iterator() );
            diffs.removeAll( diffSets.getRemoved().iterator() );
        }
        return diffs;
    }

    @Override
    public ReadableDiffSets<Long> indexUpdatesForRangeSeekByString( IndexDescriptor descriptor,
                                                                    String lower, boolean includeLower,
                                                                    String upper, boolean includeUpper )
    {
        return ReadableDiffSets.Empty.ifNull(
                getIndexUpdatesForRangeSeekByString( descriptor, lower, includeLower, upper, includeUpper )
        );
    }

    private ReadableDiffSets<Long> getIndexUpdatesForRangeSeekByString( IndexDescriptor descriptor,
                                                                        String lower, boolean includeLower,
                                                                        String upper, boolean includeUpper )
    {
        TreeMap<DefinedProperty, DiffSets<Long>> sortedUpdates = getSortedIndexUpdates( descriptor );
        if ( sortedUpdates == null )
        {
            return null;
        }

        DefinedProperty selectedLower;
        boolean selectedIncludeLower;

        DefinedProperty selectedUpper;
        boolean selectedIncludeUpper;

        int propertyKeyId = descriptor.getPropertyKeyId();

        if ( lower == null )
        {
            selectedLower = DefinedProperty.stringProperty( propertyKeyId, STRING.lowLimit.castValue( String.class ) );
            selectedIncludeLower = STRING.lowLimit.isInclusive;
        }
        else
        {
            selectedLower = DefinedProperty.stringProperty( propertyKeyId, lower );
            selectedIncludeLower = includeLower;
        }

        if ( upper == null )
        {
            selectedUpper = DefinedProperty.booleanProperty( propertyKeyId, STRING.highLimit.castValue( Boolean.class ).booleanValue() );
            selectedIncludeUpper = STRING.highLimit.isInclusive;
        }
        else
        {
            selectedUpper = DefinedProperty.stringProperty( propertyKeyId, upper );
            selectedIncludeUpper = includeUpper;
        }

        DiffSets<Long> diffs = new DiffSets<>();
        for ( Map.Entry<DefinedProperty,DiffSets<Long>> entry : sortedUpdates.subMap( selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper ).entrySet() )
        {
            DiffSets<Long> diffSets = entry.getValue();
            diffs.addAll( diffSets.getAdded().iterator() );
            diffs.removeAll( diffSets.getRemoved().iterator() );
        }
        return diffs;
    }

    @Override
    public ReadableDiffSets<Long> indexUpdatesForRangeSeekByPrefix( IndexDescriptor descriptor, String prefix )
    {
        return ReadableDiffSets.Empty.ifNull( getIndexUpdatesForRangeSeekByPrefix( descriptor, prefix ) );
    }

    private ReadableDiffSets<Long> getIndexUpdatesForRangeSeekByPrefix( IndexDescriptor descriptor, String prefix )
    {
        TreeMap<DefinedProperty, DiffSets<Long>> sortedUpdates = getSortedIndexUpdates( descriptor );
        if ( sortedUpdates == null )
        {
            return null;
        }
        int propertyKeyId = descriptor.getPropertyKeyId();
        DefinedProperty floor = Property.stringProperty( propertyKeyId, prefix );
        DiffSets<Long> diffs = new DiffSets<>();
        for ( Map.Entry<DefinedProperty,DiffSets<Long>> entry : sortedUpdates.tailMap( floor ).entrySet() )
        {
            DefinedProperty key = entry.getKey();
            if ( key.propertyKeyId() == propertyKeyId && key.value().toString().startsWith( prefix ) )
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

    // Ensure sorted index updates for a given index. This is needed for range query support and
    // may involve converting the existing hash map first
    //
    private TreeMap<DefinedProperty, DiffSets<Long>> getSortedIndexUpdates( IndexDescriptor descriptor )
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
        TreeMap<DefinedProperty,DiffSets<Long>> sortedUpdates;
        if ( updates instanceof TreeMap )
        {
            sortedUpdates = (TreeMap<DefinedProperty,DiffSets<Long>>) updates;
        }
        else
        {
            sortedUpdates = new TreeMap<>( DefinedProperty.COMPARATOR );
            sortedUpdates.putAll( updates );
            indexUpdates.put( descriptor.getLabelId(), sortedUpdates );
        }
        return sortedUpdates;
    }

    @Override
    public void indexDoUpdateProperty( IndexDescriptor descriptor, long nodeId,
            DefinedProperty propertyBefore, DefinedProperty propertyAfter )
    {
        DiffSets<Long> before = getIndexUpdatesForScanOrSeek( descriptor.getLabelId(), true, propertyBefore );
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

        DiffSets<Long> after = getIndexUpdatesForScanOrSeek( descriptor.getLabelId(), true, propertyAfter );
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

    private DiffSets<Long> getIndexUpdatesForScanOrSeek( int label, boolean create, DefinedProperty property )
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

    private DiffSets<Long> getIndexUpdatesForScanOrSeek( int label, int propertyKeyId )
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
    public boolean hasDataChanges()
    {
        return hasDataChanges;
    }

    private void recordNodeDeleted( long id )
    {
        if ( nodesDeletedInTx == null )
        {
            nodesDeletedInTx = Primitive.longSet();
        }
        nodesDeletedInTx.add( id );
    }

    private void recordRelationshipDeleted( long id )
    {
        if ( relationshipsDeletedInTx == null )
        {
            relationshipsDeletedInTx = Primitive.longSet();
        }
        relationshipsDeletedInTx.add( id );
    }
}
