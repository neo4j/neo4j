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

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.schema.constaints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.storageengine.api.txstate.ReadableRelationshipDiffSets;
import org.neo4j.storageengine.api.txstate.RelationshipState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

public class CombinedTxState implements TransactionState, RelationshipVisitor.Home
{
    private final TxState stableTxState;
    private final TxState currentTxState;

    CombinedTxState( TxState stableTxState, TxState currentTxState )
    {
        this.stableTxState = stableTxState;
        this.currentTxState = currentTxState;
    }

    @Override
    public Iterable<NodeState> modifiedNodes()
    {
        return Iterables.concat( currentTxState.modifiedNodes(), stableTxState.modifiedNodes() );
    }

    @Override
    public ReadableDiffSets<Integer> nodeStateLabelDiffSets( long nodeId )
    {
        return currentTxState.nodeStateLabelDiffSets( nodeId );
    }

    @Override
    public Iterator<StorageProperty> augmentGraphProperties( Iterator<StorageProperty> original )
    {
        return stableTxState.augmentGraphProperties( currentTxState.augmentGraphProperties( original ) );
    }

    @Override
    public boolean nodeIsAddedInThisTx( long nodeId )
    {
        return stableTxState.nodeIsAddedInThisTx( nodeId ) || currentTxState.nodeIsAddedInThisTx( nodeId );
    }

    @Override
    public boolean relationshipIsAddedInThisTx( long relationshipId )
    {
        return stableTxState.relationshipIsAddedInThisTx( relationshipId ) || currentTxState.relationshipIsAddedInThisTx( relationshipId );
    }

    @Override
    public void nodeDoCreate( long id )
    {
        currentTxState.nodeDoCreate( id );
    }

    @Override
    public void nodeDoDelete( long nodeId )
    {
        currentTxState.nodeDoDelete( nodeId );
    }

    @Override
    public void relationshipDoCreate( long id, int relationshipTypeId, long startNodeId, long endNodeId )
    {
        currentTxState.relationshipDoCreate( id, relationshipTypeId, startNodeId, endNodeId );
    }

    @Override
    public boolean nodeIsDeletedInThisTx( long nodeId )
    {
        return stableTxState.nodeIsDeletedInThisTx( nodeId ) || currentTxState.nodeIsDeletedInThisTx( nodeId );
    }

    @Override
    public boolean nodeModifiedInThisTx( long nodeId )
    {
        return stableTxState.nodeModifiedInThisTx( nodeId ) || currentTxState.nodeModifiedInThisTx( nodeId );
    }

    @Override
    public void relationshipDoDelete( long id, int type, long startNodeId, long endNodeId )
    {
        currentTxState.relationshipDoDelete( id, type, startNodeId, endNodeId );
    }

    @Override
    public void relationshipDoDeleteAddedInThisTx( long relationshipId )
    {
        //TODO hm...
        currentTxState.relationshipDoDeleteAddedInThisTx( relationshipId );
    }

    @Override
    public boolean relationshipIsDeletedInThisTx( long relationshipId )
    {
        return stableTxState.relationshipIsDeletedInThisTx(relationshipId) ||
               currentTxState.relationshipIsDeletedInThisTx( relationshipId );
    }

    @Override
    public void nodeDoAddProperty( long nodeId, int newPropertyKeyId, Value value )
    {
        currentTxState.nodeDoAddProperty( nodeId, newPropertyKeyId, value );
    }

    @Override
    public void nodeDoChangeProperty( long nodeId, int propertyKeyId, Value replacedValue, Value newValue )
    {
        currentTxState.nodeDoChangeProperty( nodeId, propertyKeyId, replacedValue, newValue );
    }

    @Override
    public void relationshipDoReplaceProperty( long relationshipId, int propertyKeyId, Value replacedValue,
            Value newValue )
    {
        currentTxState.relationshipDoReplaceProperty( relationshipId, propertyKeyId, replacedValue, newValue );
    }

    @Override
    public void graphDoReplaceProperty( int propertyKeyId, Value replacedValue, Value newValue )
    {
        currentTxState.graphDoReplaceProperty( propertyKeyId, replacedValue, newValue );
    }

    @Override
    public void nodeDoRemoveProperty( long nodeId, int propertyKeyId, Value removedValue )
    {
        currentTxState.nodeDoRemoveProperty( nodeId, propertyKeyId, removedValue );
    }

    @Override
    public void relationshipDoRemoveProperty( long relationshipId, int propertyKeyId, Value removedValue )
    {
        currentTxState.relationshipDoRemoveProperty( relationshipId, propertyKeyId, removedValue );
    }

    @Override
    public void graphDoRemoveProperty( int propertyKeyId, Value removedValue )
    {
        currentTxState.graphDoRemoveProperty( propertyKeyId, removedValue );
    }

    @Override
    public void nodeDoAddLabel( int labelId, long nodeId )
    {
        currentTxState.nodeDoAddLabel( labelId, nodeId );
    }

    @Override
    public void nodeDoRemoveLabel( int labelId, long nodeId )
    {
        currentTxState.nodeDoRemoveLabel( labelId, nodeId );
    }

    @Override
    public void labelDoCreateForName( String labelName, int id )
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public void propertyKeyDoCreateForName( String propertyKeyName, int id )
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public void relationshipTypeDoCreateForName( String labelName, int id )
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public NodeState getNodeState( long id )
    {
        return currentTxState.getNodeState( id );
    }

    @Override
    public RelationshipState getRelationshipState( long id )
    {
        return currentTxState.getRelationshipState( id );
    }

    @Override
    public Cursor<NodeItem> augmentSingleNodeCursor( Cursor<NodeItem> cursor, long nodeId )
    {
        return currentTxState.augmentSingleNodeCursor( cursor, nodeId );
    }

    @Override
    public Cursor<PropertyItem> augmentPropertyCursor( Cursor<PropertyItem> cursor,
            PropertyContainerState propertyContainerState )
    {
        return currentTxState.augmentPropertyCursor( cursor, propertyContainerState );
    }

    @Override
    public Cursor<PropertyItem> augmentSinglePropertyCursor( Cursor<PropertyItem> cursor,
            PropertyContainerState propertyContainerState, int propertyKeyId )
    {
        return currentTxState.augmentSinglePropertyCursor( cursor, propertyContainerState, propertyKeyId );
    }

    @Override
    public PrimitiveIntSet augmentLabels( PrimitiveIntSet labels, NodeState nodeState )
    {
        return currentTxState.augmentLabels( labels, nodeState );
    }

    @Override
    public Cursor<RelationshipItem> augmentSingleRelationshipCursor( Cursor<RelationshipItem> cursor,
            long relationshipId )
    {
        return currentTxState.augmentSingleRelationshipCursor( cursor, relationshipId );
    }

    @Override
    public Cursor<RelationshipItem> augmentNodeRelationshipCursor( Cursor<RelationshipItem> cursor, NodeState nodeState,
            Direction direction )
    {
        return currentTxState.augmentNodeRelationshipCursor( cursor, nodeState, direction );
    }

    @Override
    public Cursor<RelationshipItem> augmentNodeRelationshipCursor( Cursor<RelationshipItem> cursor, NodeState nodeState,
            Direction direction, int[] relTypes )
    {
        return currentTxState.augmentNodeRelationshipCursor( cursor, nodeState, direction, relTypes );
    }

    @Override
    public Cursor<RelationshipItem> augmentRelationshipsGetAllCursor( Cursor<RelationshipItem> cursor )
    {
        return stableTxState
                .augmentRelationshipsGetAllCursor( currentTxState.augmentRelationshipsGetAllCursor( cursor ) );
    }

    @Override
    public ReadableDiffSets<Long> nodesWithLabelChanged( int labelId )
    {
        return currentTxState.nodesWithLabelChanged( labelId );
    }

    @Override
    public void indexRuleDoAdd( IndexDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public void indexDoDrop( IndexDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public boolean indexDoUnRemove( IndexDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public ReadableDiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId )
    {
        return currentTxState.indexDiffSetsByLabel( labelId );
    }

    @Override
    public ReadableDiffSets<IndexDescriptor> indexChanges()
    {
        return currentTxState.indexChanges();
    }

    @Override
    public ReadableDiffSets<Long> addedAndRemovedNodes()
    {
        return currentTxState.addedAndRemovedNodes();
    }

    @Override
    public int augmentNodeDegree( long nodeId, int degree, Direction direction )
    {
        return currentTxState.augmentNodeDegree( nodeId, degree, direction );
    }

    @Override
    public int augmentNodeDegree( long nodeId, int degree, Direction direction, int typeId )
    {
        return currentTxState.augmentNodeDegree( nodeId, degree, direction, typeId );
    }

    @Override
    public PrimitiveIntSet nodeRelationshipTypes( long nodeId )
    {
        return currentTxState.nodeRelationshipTypes( nodeId );
    }

    @Override
    public ReadableRelationshipDiffSets<Long> addedAndRemovedRelationships()
    {
        return currentTxState.addedAndRemovedRelationships();
    }

    @Override
    public Iterable<RelationshipState> modifiedRelationships()
    {
        return currentTxState.modifiedRelationships();
    }

    @Override
    public void constraintDoAdd( IndexBackedConstraintDescriptor constraint, long indexId )
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public void constraintDoAdd( ConstraintDescriptor constraint )
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChangesForLabel( int labelId )
    {
        return currentTxState.constraintsChangesForLabel( labelId );
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChangesForSchema( SchemaDescriptor descriptor )
    {
        return currentTxState.constraintsChangesForSchema( descriptor );
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChangesForRelationshipType( int relTypeId )
    {
        return currentTxState.constraintsChangesForRelationshipType( relTypeId );
    }

    @Override
    public ReadableDiffSets<ConstraintDescriptor> constraintsChanges()
    {
        return currentTxState.constraintsChanges();
    }

    @Override
    public void constraintDoDrop( ConstraintDescriptor constraint )
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public boolean constraintDoUnRemove( ConstraintDescriptor constraint )
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public Iterable<IndexDescriptor> constraintIndexesCreatedInTx()
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public Long indexCreatedForConstraint( ConstraintDescriptor constraint )
    {
        throw new UnsupportedOperationException( "Operation not supported." );
    }

    @Override
    public PrimitiveLongReadableDiffSets indexUpdatesForScan( IndexDescriptor descriptor )
    {
        return currentTxState.indexUpdatesForScan( descriptor );
    }

    @Override
    public PrimitiveLongReadableDiffSets indexUpdatesForSeek( IndexDescriptor descriptor, ValueTuple values )
    {
        return currentTxState.indexUpdatesForSeek( descriptor, values );
    }

    @Override
    public PrimitiveLongReadableDiffSets indexUpdatesForRangeSeekByNumber( IndexDescriptor descriptor, Number lower,
            boolean includeLower, Number upper, boolean includeUpper )
    {
        return currentTxState
                .indexUpdatesForRangeSeekByNumber( descriptor, lower, includeLower, upper, includeUpper );
    }

    @Override
    public PrimitiveLongReadableDiffSets indexUpdatesForRangeSeekByString( IndexDescriptor descriptor, String lower,
            boolean includeLower, String upper, boolean includeUpper )
    {
        return currentTxState
                .indexUpdatesForRangeSeekByString( descriptor, lower, includeLower, upper, includeUpper );
    }

    @Override
    public PrimitiveLongReadableDiffSets indexUpdatesForRangeSeekByPrefix( IndexDescriptor descriptor, String prefix )
    {
        return currentTxState.indexUpdatesForRangeSeekByPrefix( descriptor, prefix );
    }

    @Override
    public void indexDoUpdateEntry( LabelSchemaDescriptor descriptor, long nodeId, ValueTuple propertiesBefore,
            ValueTuple propertiesAfter )
    {
        currentTxState.indexDoUpdateEntry( descriptor, nodeId, propertiesBefore, propertiesAfter );
    }

// ----->>>> test arrow is here

    @Override
    public PrimitiveLongResourceIterator augmentNodesGetAll( PrimitiveLongIterator committed )
    {
        return stableTxState.augmentNodesGetAll( currentTxState.augmentNodesGetAll( committed ) );
    }

    @Override
    public RelationshipIterator augmentRelationshipsGetAll( RelationshipIterator committed )
    {
        return stableTxState.augmentRelationshipsGetAll( currentTxState.augmentRelationshipsGetAll( committed ) );
    }

    @Override
    public <EX extends Exception> boolean relationshipVisit( long relId, RelationshipVisitor<EX> visitor )
    {
        throw new UnsupportedOperationException( "Can't visit combined state. Merge states before." );
    }

    @Override
    public void accept( TxStateVisitor visitor )
    {
        throw new UnsupportedOperationException( "Can't visit combined state. Merge states before." );
    }

    @Override
    public boolean hasChanges()
    {
        return stableTxState.hasChanges() || currentTxState.hasChanges();
    }

    @Override
    public boolean hasDataChanges()
    {
        return stableTxState.hasDataChanges() || currentTxState.hasDataChanges();
    }

    public TransactionState merge()
    {
        // TODO: any way we can reuse current visitor mechanism?
        if ( currentTxState.hasDataChanges() )
        {
            ReadableDiffSets<Long> addedRemovedNodes = currentTxState.addedAndRemovedNodes();
            if ( !addedRemovedNodes.isEmpty() )
            {
                for ( Long added : addedRemovedNodes.getAdded() )
                {
                    stableTxState.nodeDoCreate( added );
                }
                for ( Long removed : addedRemovedNodes.getRemoved() )
                {
                    stableTxState.nodeDoDelete( removed );
                }
            }

            for ( NodeState nodeState : currentTxState.modifiedNodes() )
            {
                ReadableDiffSets<Integer> addedRemovedLabels = nodeState.labelDiffSets();
                if ( !addedRemovedLabels.isEmpty() )
                {
                    for ( Integer addedLabel : addedRemovedLabels.getAdded() )
                    {
                        stableTxState.nodeDoAddLabel( addedLabel, nodeState.getId() );
                    }
                    for ( Integer removed : addedRemovedLabels.getRemoved() )
                    {
                        stableTxState.nodeDoAddLabel( removed, nodeState.getId() );
                    }

                }
                if ( nodeState.hasPropertyChanges() )
                {
                    // TODO: only handles newly created properties atm
                    Iterator<StorageProperty> addedProperties = nodeState.addedProperties();
                    while ( addedProperties.hasNext() )
                    {
                        StorageProperty storageProperty = addedProperties.next();
                        stableTxState.nodeDoAddProperty( nodeState.getId(), storageProperty.propertyKeyId(),
                                storageProperty.value() );
                    }
                }
            }
        }
        return stableTxState;
    }
}
