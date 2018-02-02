/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.schema.constaints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.txstate.RelationshipChangeVisitorAdapter;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.cursor.TxAllPropertyCursor;
import org.neo4j.kernel.impl.api.cursor.TxIteratorRelationshipCursor;
import org.neo4j.kernel.impl.api.cursor.TxSingleNodeCursor;
import org.neo4j.kernel.impl.api.cursor.TxSinglePropertyCursor;
import org.neo4j.kernel.impl.api.cursor.TxSingleRelationshipCursor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.kernel.impl.util.diffsets.EmptyPrimitiveLongReadableDiffSets;
import org.neo4j.kernel.impl.util.diffsets.EmptyRelationshipPrimitiveLongDiffSets;
import org.neo4j.kernel.impl.util.diffsets.PrimitiveLongDiffSets;
import org.neo4j.kernel.impl.util.diffsets.PrimitiveRelationshipDiffSets;
import org.neo4j.kernel.impl.util.diffsets.VersionedPrimitiveLongDiffSets;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.DiffSetsVisitor;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.PrimitiveLongDiffSetsVisitor;
import org.neo4j.storageengine.api.txstate.PrimitiveLongDiffSetsVisitorAdapter;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.RelationshipState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static java.lang.Math.toIntExact;
import static org.neo4j.helpers.collection.Iterables.map;

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
public class TxState implements TransactionState, RelationshipVisitor.Home
{
    private PrimitiveIntObjectMap<VersionedPrimitiveLongDiffSets> labelStatesMap;
    private PrimitiveLongObjectMap<NodeStateImpl> nodeStatesMap;
    private PrimitiveLongObjectMap<RelationshipStateImpl> relationshipStatesMap;

    private PrimitiveIntObjectMap<String> createdLabelTokens;
    private PrimitiveIntObjectMap<String> createdPropertyKeyTokens;
    private PrimitiveIntObjectMap<String> createdRelationshipTypeTokens;

    private GraphState graphState;
    private DiffSets<IndexDescriptor> indexChanges;
    private DiffSets<ConstraintDescriptor> constraintsChanges;

    private PropertyChanges propertyChangesForNodes;

    private RemovalsCountingDiffSets nodes;
    private RemovalsCountingRelationshipsDiffSets relationships;

    private Map<IndexBackedConstraintDescriptor, Long> createdConstraintIndexesByConstraint;

    private Map<LabelSchemaDescriptor,Map<ValueTuple,PrimitiveLongDiffSets>> indexUpdates;

    private InstanceCache<TxSingleNodeCursor> singleNodeCursor;
    private InstanceCache<TxIteratorRelationshipCursor> iteratorRelationshipCursor;
    private InstanceCache<TxSingleRelationshipCursor> singleRelationshipCursor;
    private InstanceCache<TxAllPropertyCursor> propertyCursor;
    private InstanceCache<TxSinglePropertyCursor> singlePropertyCursor;

    private boolean hasChanges;
    private boolean hasDataChanges;

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
            throws ConstraintValidationException, CreateConstraintFailureException
    {
        // Created nodes
        if ( nodes != null )
        {
            nodes.visit( createdNodesVisitor( visitor ) );
        }

        if ( relationships != null )
        {
            // Created relationships
            relationships.visit( createdRelationshipsVisitor( this, visitor ) );

            // Deleted relationships
            relationships.visit( deletedRelationshipsVisitor( visitor ) );
        }

        // Deleted nodes
        if ( nodes != null )
        {
            nodes.visit( deletedNodesVisitor( visitor ) );
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
            indexChanges.accept( indexVisitor( visitor ) );
        }

        if ( constraintsChanges != null )
        {
            constraintsChanges.accept( constraintsVisitor( visitor ) );
        }

        if ( createdLabelTokens != null )
        {
            createdLabelTokens.visitEntries( new LabelTokenStateVisitor( visitor ) );
        }

        if ( createdPropertyKeyTokens != null )
        {
            createdPropertyKeyTokens.visitEntries( new PropertyKeyTokenStateVisitor( visitor ) );
        }

        if ( createdRelationshipTypeTokens != null )
        {
            createdRelationshipTypeTokens.visitEntries( new RelationshipTypeTokenStateVisitor( visitor ) );
        }
    }

    private static PrimitiveLongDiffSetsVisitor deletedNodesVisitor( final TxStateVisitor visitor )
    {
        return new PrimitiveLongDiffSetsVisitorAdapter()
        {
            @Override
            public void visitRemoved( long element )
            {
                visitor.visitDeletedNode( element );
            }
        };
    }

    private static PrimitiveLongDiffSetsVisitor createdNodesVisitor( final TxStateVisitor visitor )
    {
        return new PrimitiveLongDiffSetsVisitorAdapter()
        {
            @Override
            public void visitAdded( long element )
            {
                visitor.visitCreatedNode( element );
            }
        };
    }

    private static PrimitiveLongDiffSetsVisitor deletedRelationshipsVisitor( final TxStateVisitor visitor )
    {
        return new PrimitiveLongDiffSetsVisitorAdapter()
        {
            @Override
            public void visitRemoved( long id )
            {
                visitor.visitDeletedRelationship( id );
            }
        };
    }

    private static PrimitiveLongDiffSetsVisitor createdRelationshipsVisitor( ReadableTransactionState tx, final TxStateVisitor visitor )
    {
        return new RelationshipChangeVisitorAdapter( tx )
        {
            @Override
            protected void visitAddedRelationship( long relationshipId, int type, long startNode, long endNode )
                    throws ConstraintValidationException
            {
                visitor.visitCreatedRelationship( relationshipId, type, startNode, endNode );
            }
        };
    }

    private static DiffSetsVisitor<ConstraintDescriptor> constraintsVisitor( final TxStateVisitor visitor )
    {
        return new ConstraintDiffSetsVisitor( visitor );
    }

    private static DiffSetsVisitor<IndexDescriptor> indexVisitor( final TxStateVisitor visitor )
    {
        return new DiffSetsVisitor<IndexDescriptor>()
        {
            @Override
            public void visitAdded( IndexDescriptor index )
            {
                visitor.visitAddedIndex( index );
            }

            @Override
            public void visitRemoved( IndexDescriptor index )
            {
                visitor.visitRemovedIndex( index );
            }
        };
    }

    private static NodeState.Visitor nodeVisitor( final TxStateVisitor visitor )
    {
        return new NodeState.Visitor()
        {
            @Override
            public void visitLabelChanges( long nodeId, PrimitiveLongSet added, PrimitiveLongSet removed )
                    throws ConstraintValidationException
            {
                visitor.visitNodeLabelChanges( nodeId, added, removed );
            }

            @Override
            public void visitPropertyChanges( long entityId, Iterator<StorageProperty> added,
                    Iterator<StorageProperty> changed, PrimitiveLongIterator removed )
                    throws ConstraintValidationException
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
        return nodeStatesMap == null ? Iterables.empty() : Iterables.cast( nodeStatesMap.values() );
    }

    private PrimitiveLongDiffSets getOrCreateLabelStateNodeDiffSets( int labelId )
    {
        if ( labelStatesMap == null )
        {
            labelStatesMap = Primitive.intObjectMap();
        }
        return labelStatesMap.computeIfAbsent( labelId, unused -> new VersionedPrimitiveLongDiffSets() ).currentView();
    }

    private PrimitiveLongDiffSets getLabelStateNodeDiffSets( int labelId )
    {
        if ( labelStatesMap == null )
        {
            return EmptyPrimitiveLongReadableDiffSets.INSTANCE;
        }
        final VersionedPrimitiveLongDiffSets nodeDiffSets = labelStatesMap.get( labelId );
        return nodeDiffSets == null ? EmptyPrimitiveLongReadableDiffSets.INSTANCE : nodeDiffSets.currentView();
    }

    @Override
    public PrimitiveLongDiffSets nodeStateLabelDiffSets( long nodeId )
    {
        return getNodeState( nodeId ).labelDiffSets();
    }

    private PrimitiveLongDiffSets getOrCreateNodeStateLabelDiffSets( long nodeId )
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
        nodes().remove( nodeId );

        if ( nodeStatesMap != null )
        {
            NodeStateImpl nodeState = nodeStatesMap.remove( nodeId );
            if ( nodeState != null )
            {
                PrimitiveLongDiffSets diff = nodeState.labelDiffSets();
                PrimitiveLongIterator addedIterator = diff.getAdded().iterator();
                while ( addedIterator.hasNext() )
                {
                    getOrCreateLabelStateNodeDiffSets( toIntExact( addedIterator.next() ) ).remove( nodeId );
                }
                // todo dead code? clear() on the next line will clear everything anyway
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
        return nodes != null && nodes.wasRemoved( nodeId );
    }

    @Override
    public boolean nodeModifiedInThisTx( long nodeId )
    {
        return nodeIsAddedInThisTx( nodeId ) || nodeIsDeletedInThisTx( nodeId ) || hasNodeState( nodeId );
    }

    @Override
    public void relationshipDoDelete( long id, int type, long startNodeId, long endNodeId )
    {
        relationships().remove( id );

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
        getRelationshipState( relationshipId ).accept( this::relationshipDoDelete );
    }

    @Override
    public boolean relationshipIsDeletedInThisTx( long relationshipId )
    {
        return relationships != null && relationships.wasRemoved( relationshipId );
    }

    @Override
    public void nodeDoAddProperty( long nodeId, int newPropertyKeyId, Value value )
    {
        NodeStateImpl nodeState = getOrCreateNodeState( nodeId );
        nodeState.addProperty( newPropertyKeyId, value );
        nodePropertyChanges().addProperty( nodeId, newPropertyKeyId, value );
        dataChanged();
    }

    @Override
    public void nodeDoChangeProperty( long nodeId, int propertyKeyId, Value replacedValue, Value newValue )
    {
        getOrCreateNodeState( nodeId ).changeProperty( propertyKeyId, newValue );
        nodePropertyChanges().changeProperty( nodeId, propertyKeyId, replacedValue, newValue );
        dataChanged();
    }

    @Override
    public void relationshipDoReplaceProperty( long relationshipId, int propertyKeyId, Value replacedValue,
            Value newValue )
    {
        if ( replacedValue != Values.NO_VALUE )
        {
            getOrCreateRelationshipState( relationshipId ).changeProperty( propertyKeyId, newValue );
        }
        else
        {
            getOrCreateRelationshipState( relationshipId ).addProperty( propertyKeyId, newValue );
        }
        dataChanged();
    }

    @Override
    public void graphDoReplaceProperty( int propertyKeyId, Value replacedValue, Value newValue )
    {
        if ( replacedValue != Values.NO_VALUE )
        {
            getOrCreateGraphState().changeProperty( propertyKeyId, newValue );
        }
        else
        {
            getOrCreateGraphState().addProperty( propertyKeyId, newValue );
        }
        dataChanged();
    }

    @Override
    public void nodeDoRemoveProperty( long nodeId, int propertyKeyId, Value removedValue )
    {
        getOrCreateNodeState( nodeId ).removeProperty( propertyKeyId, removedValue );
        nodePropertyChanges().removeProperty( nodeId, propertyKeyId, removedValue );
        dataChanged();
    }

    @Override
    public void relationshipDoRemoveProperty( long relationshipId, int propertyKeyId, Value removedValue )
    {
        getOrCreateRelationshipState( relationshipId ).removeProperty( propertyKeyId, removedValue );
        dataChanged();
    }

    @Override
    public void graphDoRemoveProperty( int propertyKeyId, Value removedValue )
    {
        getOrCreateGraphState().removeProperty( propertyKeyId, removedValue );
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
            createdLabelTokens = Primitive.intObjectMap();
        }
        createdLabelTokens.put( id, labelName );
        changed();
    }

    @Override
    public void propertyKeyDoCreateForName( String propertyKeyName, int id )
    {
        if ( createdPropertyKeyTokens == null )
        {
            createdPropertyKeyTokens = Primitive.intObjectMap();
        }
        createdPropertyKeyTokens.put( id, propertyKeyName );
        changed();
    }

    @Override
    public void relationshipTypeDoCreateForName( String labelName, int id )
    {
        if ( createdRelationshipTypeTokens == null )
        {
            createdRelationshipTypeTokens = Primitive.intObjectMap();
        }
        createdRelationshipTypeTokens.put( id, labelName );
        changed();
    }

    @Override
    public NodeState getNodeState( long id )
    {
        if ( nodeStatesMap == null )
        {
            return NodeStateImpl.EMPTY;
        }
        final NodeState nodeState = nodeStatesMap.get( id );
        return nodeState == null ? NodeStateImpl.EMPTY : nodeState;
    }

    @Override
    public RelationshipState getRelationshipState( long id )
    {
        if ( relationshipStatesMap == null )
        {
            return RelationshipStateImpl.EMPTY;
        }
        final RelationshipStateImpl relationshipState = relationshipStatesMap.get( id );
        return relationshipState == null ? RelationshipStateImpl.EMPTY : relationshipState;
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
        return propertyContainerState.hasPropertyChanges() ?
                propertyCursor.get().init( cursor, propertyContainerState ) : cursor;
    }

    @Override
    public Cursor<PropertyItem> augmentSinglePropertyCursor( Cursor<PropertyItem> cursor,
            PropertyContainerState propertyContainerState, int propertyKeyId )
    {
        return propertyContainerState.hasPropertyChanges() ?
                singlePropertyCursor.get().init( cursor, propertyContainerState, propertyKeyId ) : cursor;
    }

    @Override
    public PrimitiveIntSet augmentLabels( PrimitiveIntSet labels, NodeState nodeState )
    {
        PrimitiveLongDiffSets labelDiffSets = nodeState.labelDiffSets();
        if ( !labelDiffSets.isEmpty() )
        {
            labelDiffSets.getRemoved().forEach( value -> labels.remove( Math.toIntExact( value ) ) );
            labelDiffSets.getAdded().forEach( value -> labels.add( Math.toIntExact( value ) ) );
        }
        return labels;
    }

    @Override
    public Cursor<RelationshipItem> augmentSingleRelationshipCursor( Cursor<RelationshipItem> cursor,
            long relationshipId )
    {
        return hasChanges ? singleRelationshipCursor.get().init( cursor, relationshipId ) : cursor;
    }

    @Override
    public Cursor<RelationshipItem> augmentNodeRelationshipCursor( Cursor<RelationshipItem> cursor,
            NodeState nodeState,
            Direction direction )
    {
        return nodeState.hasRelationshipChanges()
               ? iteratorRelationshipCursor.get().init( cursor, nodeState.getAddedRelationships( direction ) )
               : cursor;
    }
    @Override
    public Cursor<RelationshipItem> augmentNodeRelationshipCursor( Cursor<RelationshipItem> cursor,
            NodeState nodeState,
            Direction direction,
            int[] relTypes )
    {
        return nodeState.hasRelationshipChanges()
               ? iteratorRelationshipCursor.get().init( cursor, nodeState.getAddedRelationships( direction, relTypes ) )
               : cursor;
    }

    @Override
    public Cursor<RelationshipItem> augmentRelationshipsGetAllCursor( Cursor<RelationshipItem> cursor )
    {
        return hasChanges && relationships != null && !relationships.isEmpty()
               ? iteratorRelationshipCursor.get().init( cursor, relationships.getAdded().iterator() )
               : cursor;
    }

    @Override
    public PrimitiveLongDiffSets nodesWithLabelChanged( int label )
    {
        return getLabelStateNodeDiffSets( label );
    }

    @Override
    public PrimitiveLongDiffSets nodesWithAnyOfLabelsChanged( int... labels )
    {
        //It is enough that one of the labels is added
        //It is necessary for all the labels are removed
        //TODO: why all of this?
        PrimitiveLongSet added = Primitive.longSet();
        PrimitiveLongSet removed = Primitive.longSet();
        for ( int i = 0; i < labels.length; i++ )
        {
            PrimitiveLongDiffSets nodeDiffSets = getLabelStateNodeDiffSets( labels[i] );
            if ( i == 0 )
            {
                removed.addAll( nodeDiffSets.getRemoved().iterator() );
            }
            else
            {
                removed.visitKeys( (PrimitiveLongVisitor) value -> {
                    if ( !nodeDiffSets.getRemoved().contains( value ) )
                    {
                        removed.remove( value );
                    }
                    return false;
                } );
            }
            added.addAll( nodeDiffSets.getAdded().iterator() );
        }
        return new PrimitiveLongDiffSets( added, removed );
    }

    @Override
    public PrimitiveLongDiffSets nodesWithAllLabelsChanged( int... labels )
    {
        PrimitiveLongDiffSets changes = new PrimitiveLongDiffSets();
        for ( int label : labels )
        {
            final PrimitiveLongDiffSets nodeDiffSets = getLabelStateNodeDiffSets( label );
            changes.addAll( nodeDiffSets.getAdded().iterator() );
            changes.removeAll( nodeDiffSets.getRemoved().iterator() );
        }
        return changes;
    }

    @Override
    public void indexRuleDoAdd( IndexDescriptor descriptor )
    {
        DiffSets<IndexDescriptor> diff = indexChangesDiffSets();
        if ( !diff.unRemove( descriptor ) )
        {
            diff.add( descriptor );
        }
        changed();
    }

    @Override
    public void indexDoDrop( IndexDescriptor descriptor )
    {
        indexChangesDiffSets().remove( descriptor );
        changed();
    }

    @Override
    public boolean indexDoUnRemove( IndexDescriptor descriptor )
    {
        return indexChangesDiffSets().unRemove( descriptor );
    }

    @Override
    public ReadableDiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId )
    {
        return indexChangesDiffSets().filterAdded( SchemaDescriptorPredicates.hasLabel( labelId ) );
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
    public PrimitiveLongReadableDiffSets addedAndRemovedNodes()
    {
        return nodes != null ? nodes : EmptyPrimitiveLongReadableDiffSets.INSTANCE;
    }

    private RemovalsCountingDiffSets nodes()
    {
        if ( nodes == null )
        {
            nodes = new RemovalsCountingDiffSets();
        }
        return nodes;
    }

    @Override
    public int augmentNodeDegree( long nodeId, int degree, Direction direction )
    {
        return getNodeState( nodeId ).augmentDegree( direction, degree );
    }

    @Override
    public int augmentNodeDegree( long nodeId, int degree, Direction direction, int typeId )
    {
        return getNodeState( nodeId ).augmentDegree( direction, degree, typeId );
    }

    @Override
    public PrimitiveIntSet nodeRelationshipTypes( long nodeId )
    {
        return getNodeState( nodeId ).relationshipTypes();
    }

    @Override
    public PrimitiveRelationshipDiffSets addedAndRemovedRelationships()
    {
        return relationships != null ? relationships : EmptyRelationshipPrimitiveLongDiffSets.INSTANCE;
    }

    private RemovalsCountingRelationshipsDiffSets relationships()
    {
        if ( relationships == null )
        {
            relationships = new RemovalsCountingRelationshipsDiffSets( this );
        }
        return relationships;
    }

    @Override
    public Iterable<RelationshipState> modifiedRelationships()
    {
        return relationshipStatesMap == null ? Iterables.empty() : Iterables.cast( relationshipStatesMap.values() );
    }

    private NodeStateImpl getOrCreateNodeState( long nodeId )
    {
        if ( nodeStatesMap == null )
        {
            nodeStatesMap = Primitive.longObjectMap();
        }
        return nodeStatesMap.computeIfAbsent( nodeId, unused -> new NodeStateImpl( nodeId, this ) );
    }

    private RelationshipStateImpl getOrCreateRelationshipState( long relationshipId )
    {
        if ( relationshipStatesMap == null )
        {
            relationshipStatesMap = Primitive.longObjectMap();
        }
        return relationshipStatesMap.computeIfAbsent( relationshipId, unused -> new RelationshipStateImpl( relationshipId ) );
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
    public void constraintDoAdd( IndexBackedConstraintDescriptor constraint, long indexId )
    {
        constraintsChangesDiffSets().add( constraint );
        createdConstraintIndexesByConstraint().put( constraint, indexId );
        changed();
    }

    @Override
    public void constraintDoAdd( ConstraintDescriptor constraint )
    {
        constraintsChangesDiffSets().add( constraint );
        changed();
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChangesForLabel( int labelId )
    {
        return constraintsChangesDiffSets().filterAdded( SchemaDescriptorPredicates.hasLabel( labelId ) );
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChangesForSchema( SchemaDescriptor descriptor )
    {
        return constraintsChangesDiffSets().filterAdded( SchemaDescriptor.equalTo( descriptor ) );
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChangesForRelationshipType( int relTypeId )
    {
        return constraintsChangesDiffSets().filterAdded( SchemaDescriptorPredicates.hasRelType( relTypeId ) );
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChanges()
    {
        return ReadableDiffSets.Empty.ifNull( constraintsChanges );
    }

    private DiffSets<ConstraintDescriptor> constraintsChangesDiffSets()
    {
        if ( constraintsChanges == null )
        {
            constraintsChanges = new DiffSets<>();
        }
        return constraintsChanges;
    }

    @Override
    public void constraintDoDrop( ConstraintDescriptor constraint )
    {
        constraintsChangesDiffSets().remove( constraint );
        if ( constraint.enforcesUniqueness() )
        {
            indexDoDrop( getIndexForIndexBackedConstraint( (IndexBackedConstraintDescriptor) constraint ) );
        }
        changed();
    }

    @Override
    public boolean constraintDoUnRemove( ConstraintDescriptor constraint )
    {
        return constraintsChangesDiffSets().unRemove( constraint );
    }

    @Override
    public Iterable<IndexDescriptor> constraintIndexesCreatedInTx()
    {
        if ( createdConstraintIndexesByConstraint != null && !createdConstraintIndexesByConstraint.isEmpty() )
        {
            return map( this::getIndexForIndexBackedConstraint, createdConstraintIndexesByConstraint.keySet() );
        }
        return Iterables.empty();
    }

    @Override
    public Long indexCreatedForConstraint( ConstraintDescriptor constraint )
    {
        return createdConstraintIndexesByConstraint == null ? null :
                createdConstraintIndexesByConstraint.get( constraint );
    }

    @Override
    public PrimitiveLongReadableDiffSets indexUpdatesForScan( IndexDescriptor descriptor )
    {
        if ( indexUpdates == null )
        {
            return EmptyPrimitiveLongReadableDiffSets.INSTANCE;
        }
        Map<ValueTuple, PrimitiveLongDiffSets> updates = indexUpdates.get( descriptor.schema() );
        if ( updates == null )
        {
            return EmptyPrimitiveLongReadableDiffSets.INSTANCE;
        }
        PrimitiveLongDiffSets diffs = new PrimitiveLongDiffSets();
        for ( PrimitiveLongDiffSets diffSet : updates.values() )
        {
            diffs.addAll( diffSet.getAdded().iterator() );
            diffs.removeAll( diffSet.getRemoved().iterator() );
        }
        return diffs;
    }

    @Override
    public PrimitiveLongReadableDiffSets indexUpdatesForSeek( IndexDescriptor descriptor, ValueTuple values )
    {
        PrimitiveLongDiffSets indexUpdatesForSeek = getIndexUpdatesForSeek( descriptor.schema(), values, /*create=*/false );
        return indexUpdatesForSeek == null ? EmptyPrimitiveLongReadableDiffSets.INSTANCE : indexUpdatesForSeek;
    }

    @Override
    public PrimitiveLongReadableDiffSets indexUpdatesForRangeSeekByNumber( IndexDescriptor descriptor,
                                                                    Number lower, boolean includeLower,
                                                                    Number upper, boolean includeUpper )
    {
        TreeMap<ValueTuple, PrimitiveLongDiffSets> sortedUpdates = getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EmptyPrimitiveLongReadableDiffSets.INSTANCE;
        }

        ValueTuple selectedLower;
        boolean selectedIncludeLower;

        ValueTuple selectedUpper;
        boolean selectedIncludeUpper;

        if ( lower == null )
        {
            selectedLower = ValueTuple.of( Values.MIN_NUMBER );
            selectedIncludeLower = true;
        }
        else
        {
            selectedLower = ValueTuple.of( Values.numberValue( lower ) );
            selectedIncludeLower = includeLower;
        }

        if ( upper == null )
        {
            selectedUpper = ValueTuple.of( Values.MAX_NUMBER );
            selectedIncludeUpper = true;
        }
        else
        {
            selectedUpper = ValueTuple.of( Values.numberValue( upper ) );
            selectedIncludeUpper = includeUpper;
        }

        return indexUpdatesForRangeSeek( sortedUpdates, selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );
    }

    @Override
    public PrimitiveLongReadableDiffSets indexUpdatesForRangeSeekByGeometry( IndexDescriptor descriptor,
                                                                PointValue lower, boolean includeLower,
                                                                PointValue upper, boolean includeUpper )
    {
        TreeMap<ValueTuple, PrimitiveLongDiffSets> sortedUpdates = getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EmptyPrimitiveLongReadableDiffSets.INSTANCE;
        }

        if ( lower == null && upper == null )
        {
            throw new IllegalArgumentException( "Cannot access TxState with invalid GeometryRangePredicate" );
        }

        ValueTuple selectedLower;
        boolean selectedIncludeLower;

        ValueTuple selectedUpper;
        boolean selectedIncludeUpper;

        if ( lower == null )
        {
            selectedLower = ValueTuple.of( Values.minPointValue( upper ) );
            selectedIncludeLower = true;
        }
        else
        {
            selectedLower = ValueTuple.of( lower );
            selectedIncludeLower = includeLower;
        }

        if ( upper == null )
        {
            selectedUpper = ValueTuple.of( Values.maxPointValue( lower ) );
            selectedIncludeUpper = true;
        }
        else
        {
            selectedUpper = ValueTuple.of( upper );
            selectedIncludeUpper = includeUpper;
        }

        return indexUpdatesForRangeSeek( sortedUpdates, selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );
    }

    @Override
    public PrimitiveLongReadableDiffSets indexUpdatesForRangeSeekByString( IndexDescriptor descriptor,
                                                                    String lower, boolean includeLower,
                                                                    String upper, boolean includeUpper )
    {
        TreeMap<ValueTuple, PrimitiveLongDiffSets> sortedUpdates = getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EmptyPrimitiveLongReadableDiffSets.INSTANCE;
        }

        ValueTuple selectedLower;
        boolean selectedIncludeLower;

        ValueTuple selectedUpper;
        boolean selectedIncludeUpper;

        if ( lower == null )
        {
            selectedLower = ValueTuple.of( Values.MIN_STRING );
            selectedIncludeLower = true;
        }
        else
        {
            selectedLower = ValueTuple.of( Values.stringValue( lower ) );
            selectedIncludeLower = includeLower;
        }

        if ( upper == null )
        {
            selectedUpper = ValueTuple.of( Values.MAX_STRING );
            selectedIncludeUpper = false;
        }
        else
        {
            selectedUpper = ValueTuple.of( Values.stringValue( upper ) );
            selectedIncludeUpper = includeUpper;
        }

        return indexUpdatesForRangeSeek( sortedUpdates, selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );
    }

    private PrimitiveLongReadableDiffSets indexUpdatesForRangeSeek( TreeMap<ValueTuple,PrimitiveLongDiffSets> sortedUpdates, ValueTuple lower,
            boolean includeLower, ValueTuple upper, boolean includeUpper )
    {
        PrimitiveLongDiffSets diffs = new PrimitiveLongDiffSets();

        Collection<PrimitiveLongDiffSets> inRange = sortedUpdates.subMap( lower, includeLower, upper, includeUpper ).values();
        for ( PrimitiveLongDiffSets diffForSpecificValue : inRange )
        {
            diffs.addAll( diffForSpecificValue.getAdded().iterator() );
            diffs.removeAll( diffForSpecificValue.getRemoved().iterator() );
        }
        return diffs;
    }

    @Override
    public PrimitiveLongReadableDiffSets indexUpdatesForRangeSeekByPrefix( IndexDescriptor descriptor, String prefix )
    {
        TreeMap<ValueTuple, PrimitiveLongDiffSets> sortedUpdates = getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EmptyPrimitiveLongReadableDiffSets.INSTANCE;
        }
        ValueTuple floor = ValueTuple.of( Values.stringValue( prefix ) );
        PrimitiveLongDiffSets diffs = new PrimitiveLongDiffSets();
        for ( Map.Entry<ValueTuple,PrimitiveLongDiffSets> entry : sortedUpdates.tailMap( floor ).entrySet() )
        {
            ValueTuple key = entry.getKey();
            if ( ((TextValue) key.getOnlyValue()).stringValue().startsWith( prefix ) )
            {
                PrimitiveLongDiffSets diffSets = entry.getValue();
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
    private TreeMap<ValueTuple, PrimitiveLongDiffSets> getSortedIndexUpdates( LabelSchemaDescriptor descriptor )
    {
        if ( indexUpdates == null )
        {
            return null;
        }
        Map<ValueTuple, PrimitiveLongDiffSets> updates = indexUpdates.get( descriptor );
        if ( updates == null )
        {
            return null;
        }
        TreeMap<ValueTuple,PrimitiveLongDiffSets> sortedUpdates;
        if ( updates instanceof TreeMap )
        {
            sortedUpdates = (TreeMap<ValueTuple,PrimitiveLongDiffSets>) updates;
        }
        else
        {
            sortedUpdates = new TreeMap<>( ValueTuple.COMPARATOR );
            sortedUpdates.putAll( updates );
            indexUpdates.put( descriptor, sortedUpdates );
        }
        return sortedUpdates;
    }

    @Override
    public void indexDoUpdateEntry( LabelSchemaDescriptor descriptor, long nodeId,
            ValueTuple propertiesBefore, ValueTuple propertiesAfter )
    {
        NodeStateImpl nodeState = getOrCreateNodeState( nodeId );
        Map<ValueTuple,PrimitiveLongDiffSets> updates = getIndexUpdatesByDescriptor( descriptor, true);
        if ( propertiesBefore != null )
        {
            PrimitiveLongDiffSets before = getIndexUpdatesForSeek( updates, propertiesBefore, true );
            //noinspection ConstantConditions
            before.remove( nodeId );
            if ( before.getRemoved().contains( nodeId ) )
            {
                nodeState.addIndexDiff( before );
            }
            else
            {
                nodeState.removeIndexDiff( before );
            }
        }
        if ( propertiesAfter != null )
        {
            PrimitiveLongDiffSets after = getIndexUpdatesForSeek( updates, propertiesAfter, true );
            //noinspection ConstantConditions
            after.add( nodeId );
            if ( after.getAdded().contains( nodeId ) )
            {
                nodeState.addIndexDiff( after );
            }
            else
            {
                nodeState.removeIndexDiff( after );
            }
        }
    }

    private PrimitiveLongDiffSets getIndexUpdatesForSeek(
            LabelSchemaDescriptor schema, ValueTuple values, boolean create )
    {
        Map<ValueTuple,PrimitiveLongDiffSets> updates = getIndexUpdatesByDescriptor( schema, create );
        if ( updates != null )
        {
            return getIndexUpdatesForSeek( updates, values, create );
        }
        return null;
    }

    private PrimitiveLongDiffSets getIndexUpdatesForSeek( Map<ValueTuple,PrimitiveLongDiffSets> updates,
            ValueTuple values, boolean create )
    {
        return create ? updates.computeIfAbsent( values, value -> new PrimitiveLongDiffSets() ) : updates.get( values );
    }

    private Map<ValueTuple,PrimitiveLongDiffSets> getIndexUpdatesByDescriptor( LabelSchemaDescriptor schema,
            boolean create )
    {
        if ( indexUpdates == null )
        {
            if ( !create )
            {
                return null;
            }
            indexUpdates = new HashMap<>();
        }
        Map<ValueTuple, PrimitiveLongDiffSets> updates = indexUpdates.get( schema );
        if ( updates == null )
        {
            if ( !create )
            {
                return null;
            }
            updates = new HashMap<>();
            indexUpdates.put( schema, updates );
        }
        return updates;
    }

    private Map<IndexBackedConstraintDescriptor, Long> createdConstraintIndexesByConstraint()
    {
        if ( createdConstraintIndexesByConstraint == null )
        {
            createdConstraintIndexesByConstraint = new HashMap<>();
        }
        return createdConstraintIndexesByConstraint;
    }

    private IndexDescriptor getIndexForIndexBackedConstraint( IndexBackedConstraintDescriptor constraint )
    {
        return constraint.ownedIndexDescriptor();
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
    public PrimitiveLongResourceIterator augmentNodesGetAll( PrimitiveLongIterator committed )
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
        return getRelationshipState( relId ).accept( visitor );
    }

    @Override
    public boolean hasDataChanges()
    {
        return hasDataChanges;
    }

    private static class LabelTokenStateVisitor implements PrimitiveIntObjectVisitor<String,RuntimeException>
    {
        private final TxStateVisitor visitor;

        LabelTokenStateVisitor( TxStateVisitor visitor )
        {
            this.visitor = visitor;
        }

        @Override
        public boolean visited( int key, String value )
        {
            visitor.visitCreatedLabelToken( value, key );
            return false;
        }
    }

    private static class PropertyKeyTokenStateVisitor implements PrimitiveIntObjectVisitor<String,RuntimeException>
    {
        private final TxStateVisitor visitor;

        PropertyKeyTokenStateVisitor( TxStateVisitor visitor )
        {
            this.visitor = visitor;
        }

        @Override
        public boolean visited( int key, String value )
        {
            visitor.visitCreatedPropertyKeyToken( value, key );
            return false;
        }
    }

    private static class RelationshipTypeTokenStateVisitor implements PrimitiveIntObjectVisitor<String,RuntimeException>
    {
        private final TxStateVisitor visitor;

        RelationshipTypeTokenStateVisitor( TxStateVisitor visitor )
        {
            this.visitor = visitor;
        }

        @Override
        public boolean visited( int key, String value )
        {
            visitor.visitCreatedRelationshipTypeToken( value, key );
            return false;
        }
    }

    private static class ConstraintDiffSetsVisitor implements DiffSetsVisitor<ConstraintDescriptor>
    {
        private final TxStateVisitor visitor;

        ConstraintDiffSetsVisitor( TxStateVisitor visitor )
        {
            this.visitor = visitor;
        }

        @Override
        public void visitAdded( ConstraintDescriptor constraint ) throws CreateConstraintFailureException
        {
            visitor.visitAddedConstraint( constraint );
        }

        @Override
        public void visitRemoved( ConstraintDescriptor constraint )
        {
            visitor.visitRemovedConstraint( constraint );
        }
    }

    /**
     * This class works around the fact that create-delete in the same transaction is a no-op in {@link DiffSets},
     * whereas we need to know total number of explicit removals.
     */
    private static class RemovalsCountingDiffSets extends PrimitiveLongDiffSets
    {
        private PrimitiveLongSet removedFromAdded;

        @Override
        public boolean remove( long elem )
        {
            if ( getAdded().remove( elem ) )
            {
                if ( removedFromAdded == null )
                {
                    removedFromAdded = Primitive.longSet();
                }
                removedFromAdded.add( elem );
                return true;
            }
            return super.remove( elem );
        }

        private boolean wasRemoved( long id )
        {
            return (removedFromAdded != null && removedFromAdded.contains( id )) || super.isRemoved( id );
        }
    }

    /**
     * This class works around the fact that create-delete in the same transaction is a no-op in {@link DiffSets},
     * whereas we need to know total number of explicit removals.
     */
    private static class RemovalsCountingRelationshipsDiffSets extends PrimitiveRelationshipDiffSets
    {
        private PrimitiveLongSet removedFromAdded;

        private RemovalsCountingRelationshipsDiffSets( RelationshipVisitor.Home txStateRelationshipHome )
        {
            super( txStateRelationshipHome );
        }

        @Override
        public boolean remove( long elem )
        {
            if ( getAdded().remove( elem ) )
            {
                if ( removedFromAdded == null )
                {
                    removedFromAdded = Primitive.longSet();
                }
                removedFromAdded.add( elem );
                return true;
            }
            return super.remove( elem );
        }

        private boolean wasRemoved( long id )
        {
            return (removedFromAdded != null && removedFromAdded.contains( id )) || super.isRemoved( id );
        }
    }
}
