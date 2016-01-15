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
package org.neo4j.storageengine.api.txstate;

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.LabelItem;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;

/**
 * This interface contains the methods for reading transaction state from the transaction state.
 * The implementation of these methods should be free of any side effects (such as initialising lazy state).
 */
public interface ReadableTransactionState
{
    void accept( TxStateVisitor visitor ) throws ConstraintValidationKernelException, CreateConstraintFailureException;

    boolean hasChanges();

    // ENTITY RELATED

    /**
     * Returns all nodes that, in this tx, have had labelId removed.
     */
    ReadableDiffSets<Long> nodesWithLabelChanged( int labelId );

    /**
     * Returns nodes that have been added and removed in this tx.
     */
    ReadableDiffSets<Long> addedAndRemovedNodes();

    /**
     * Returns rels that have been added and removed in this tx.
     */
    ReadableRelationshipDiffSets<Long> addedAndRemovedRelationships();

    /**
     * Nodes that have had labels, relationships, or properties modified in this tx.
     */
    Iterable<NodeState> modifiedNodes();

    /**
     * Rels that have properties modified in this tx.
     */
    Iterable<RelationshipState> modifiedRelationships();

    boolean relationshipIsAddedInThisTx( long relationshipId );

    boolean relationshipIsDeletedInThisTx( long relationshipId );

    ReadableDiffSets<Integer> nodeStateLabelDiffSets( long nodeId );

    Iterator<StorageProperty> augmentGraphProperties( Iterator<StorageProperty> original );

    boolean nodeIsAddedInThisTx( long nodeId );

    boolean nodeIsDeletedInThisTx( long nodeId );

    boolean nodeModifiedInThisTx( long nodeId );

    PrimitiveIntIterator nodeRelationshipTypes( long nodeId );

    int augmentNodeDegree( long node, int committedDegree, Direction direction );

    int augmentNodeDegree( long node, int committedDegree, Direction direction, int relType );

    PrimitiveLongIterator augmentNodesGetAll( PrimitiveLongIterator committed );

    RelationshipIterator augmentRelationshipsGetAll( RelationshipIterator committed );

    /**
     * @return {@code true} if the relationship was visited in this state, i.e. if it was created
     * by this current transaction, otherwise {@code false} where the relationship might need to be
     * visited from the store.
     */
    <EX extends Exception> boolean relationshipVisit( long relId, RelationshipVisitor<EX> visitor ) throws EX;

    // SCHEMA RELATED

    ReadableDiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId );

    ReadableDiffSets<IndexDescriptor> constraintIndexDiffSetsByLabel( int labelId );

    ReadableDiffSets<IndexDescriptor> indexChanges();

    ReadableDiffSets<IndexDescriptor> constraintIndexChanges();

    Iterable<IndexDescriptor> constraintIndexesCreatedInTx();

    ReadableDiffSets<PropertyConstraint> constraintsChanges();

    ReadableDiffSets<NodePropertyConstraint> constraintsChangesForLabel( int labelId );

    ReadableDiffSets<NodePropertyConstraint> constraintsChangesForLabelAndProperty( int labelId, int propertyKey );

    ReadableDiffSets<RelationshipPropertyConstraint> constraintsChangesForRelationshipType( int relTypeId );

    ReadableDiffSets<RelationshipPropertyConstraint> constraintsChangesForRelationshipTypeAndProperty( int relTypeId,
            int propertyKey );

    Long indexCreatedForConstraint( UniquenessConstraint constraint );

    ReadableDiffSets<Long> indexUpdatesForScanOrSeek( IndexDescriptor index, Object value );

    ReadableDiffSets<Long> indexUpdatesForRangeSeekByNumber( IndexDescriptor index,
                                                             Number lower, boolean includeLower,
                                                             Number upper, boolean includeUpper );

    ReadableDiffSets<Long> indexUpdatesForRangeSeekByString( IndexDescriptor index,
                                                             String lower, boolean includeLower,
                                                             String upper, boolean includeUpper );

    ReadableDiffSets<Long> indexUpdatesForRangeSeekByPrefix( IndexDescriptor index, String prefix );

    NodeState getNodeState( long id );

    RelationshipState getRelationshipState( long id );

    Cursor<NodeItem> augmentSingleNodeCursor( Cursor<NodeItem> cursor, long nodeId );

    Cursor<PropertyItem> augmentPropertyCursor( Cursor<PropertyItem> cursor,
            PropertyContainerState propertyContainerState );

    Cursor<PropertyItem> augmentSinglePropertyCursor( Cursor<PropertyItem> cursor,
            PropertyContainerState propertyContainerState,
            int propertyKeyId );

    Cursor<LabelItem> augmentLabelCursor( Cursor<LabelItem> cursor, NodeState nodeState );

    public Cursor<LabelItem> augmentSingleLabelCursor( Cursor<LabelItem> cursor, NodeState nodeState, int labelId );

    Cursor<RelationshipItem> augmentSingleRelationshipCursor( Cursor<RelationshipItem> cursor, long relationshipId );

    Cursor<RelationshipItem> augmentIteratorRelationshipCursor( Cursor<RelationshipItem> cursor,
            RelationshipIterator iterator );

    Cursor<RelationshipItem> augmentNodeRelationshipCursor( Cursor<RelationshipItem> cursor,
            NodeState nodeState,
            Direction direction,
            int[] relTypes );

    Cursor<NodeItem> augmentNodesGetAllCursor( Cursor<NodeItem> cursor );

    Cursor<RelationshipItem> augmentRelationshipsGetAllCursor( Cursor<RelationshipItem> cursor );

    /**
     * The way tokens are created is that the first time a token is needed it gets created in its own little
     * token mini-transaction, separate from the surrounding transaction that creates or modifies data that need it.
     * From the kernel POV it's interesting to know whether or not any tokens have been created in this tx state,
     * because then we know it's a mini-transaction like this and won't have to let transaction event handlers
     * know about it, for example.
     *
     * The same applies to schema changes, such as creating and dropping indexes and constraints.
     */
    boolean hasDataChanges();
}
