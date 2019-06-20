/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.UnmodifiableMap;

import java.util.NavigableMap;
import javax.annotation.Nullable;

import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.values.storable.ValueTuple;

/**
 * This interface contains the methods for reading transaction state from the transaction state.
 * The implementation of these methods should be free of any side effects (such as initialising lazy state).
 */
public interface ReadableTransactionState
{
    void accept( TxStateVisitor visitor ) throws ConstraintValidationException, CreateConstraintFailureException;

    boolean hasChanges();

    // ENTITY RELATED

    /**
     * Returns all nodes that, in this tx, have had the labels changed.
     * @param label
     */
    LongDiffSets nodesWithLabelChanged( long label );

    /**
     * Returns nodes that have been added and removed in this tx.
     */
    LongDiffSets addedAndRemovedNodes();

    /**
     * Returns rels that have been added and removed in this tx.
     */
    LongDiffSets addedAndRemovedRelationships();

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

    LongDiffSets nodeStateLabelDiffSets( long nodeId );

    boolean nodeIsAddedInThisTx( long nodeId );

    boolean nodeIsDeletedInThisTx( long nodeId );

    /**
     * @return {@code true} if the relationship was visited in this state, i.e. if it was created
     * by this current transaction, otherwise {@code false} where the relationship might need to be
     * visited from the store.
     */
    <EX extends Exception> boolean relationshipVisit( long relId, RelationshipVisitor<EX> visitor ) throws EX;

    // SCHEMA RELATED

    DiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId );

    DiffSets<IndexDescriptor> indexDiffSetsByRelationshipType( int relationshipType );

    DiffSets<IndexDescriptor> indexDiffSetsBySchema( SchemaDescriptor schema );

    DiffSets<IndexDescriptor> indexChanges();

    Iterable<IndexDescriptor> constraintIndexesCreatedInTx();

    DiffSets<ConstraintDescriptor> constraintsChanges();

    DiffSets<ConstraintDescriptor> constraintsChangesForLabel( int labelId );

    DiffSets<ConstraintDescriptor> constraintsChangesForSchema( SchemaDescriptor descriptor );

    DiffSets<ConstraintDescriptor> constraintsChangesForRelationshipType( int relTypeId );

    Long indexCreatedForConstraint( ConstraintDescriptor constraint );

    // INDEX UPDATES

    /**
     * A readonly view of all index updates for the provided schema. Returns {@code null}, if the index
     * updates for this schema have not been initialized.
     */
    @Nullable
    UnmodifiableMap<ValueTuple, ? extends LongDiffSets> getIndexUpdates( SchemaDescriptor schema );

    /**
     * A readonly view of all index updates for the provided schema, in sorted order. The returned
     * Map is unmodifiable. Returns {@code null}, if the index updates for this schema have not been initialized.
     *
     * Ensure sorted index updates for a given index. This is needed for range query support and
     * ay involve converting the existing hash map first.
     */
    @Nullable
    NavigableMap<ValueTuple, ? extends LongDiffSets> getSortedIndexUpdates( SchemaDescriptor descriptor );

    // OTHER

    NodeState getNodeState( long id );

    RelationshipState getRelationshipState( long id );

    GraphState getGraphState();

    MutableLongSet augmentLabels( MutableLongSet labels, NodeState nodeState );

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
