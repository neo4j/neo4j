/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api.txstate;

import java.util.Iterator;
import java.util.NavigableMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.UnmodifiableMap;
import org.neo4j.collection.diffset.DiffSets;
import org.neo4j.collection.diffset.LongDiffSets;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.values.storable.ValueTuple;

/**
 * This interface contains the methods for reading transaction state from the transaction state.
 * The implementation of these methods should be free of any side effects (such as initialising lazy state).
 */
public interface ReadableTransactionState {
    void accept(TxStateVisitor visitor) throws KernelException;

    boolean hasChanges();

    // ENTITY RELATED

    /**
     * Returns all nodes that, in this tx, have had the labels changed.
     *
     * @param label The label that has changed.
     */
    LongDiffSets nodesWithLabelChanged(int label);

    /**
     * Returns nodes that have been added and removed in this tx.
     */
    LongDiffSets addedAndRemovedNodes();

    /**
     * Returns all relationships that, in this tx, have had the type changed.
     * This always happens as a result of creating or deleting a relationship.
     *
     * @param type The relationship type that has changed.
     */
    LongDiffSets relationshipsWithTypeChanged(int type);

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

    boolean relationshipIsAddedInThisBatch(long relationshipId);

    boolean relationshipIsDeletedInThisBatch(long relationshipId);

    LongDiffSets nodeStateLabelDiffSets(long nodeId);

    boolean nodeIsAddedInThisBatch(long nodeId);

    boolean nodeIsDeletedInThisBatch(long nodeId);

    boolean hasConstraintIndexesCreatedInTx();

    /**
     * @return {@code true} if the relationship was visited in this state, i.e. if it was created
     * by this current transaction, otherwise {@code false} where the relationship might need to be
     * visited from the store.
     */
    <EX extends Exception> boolean relationshipVisit(long relId, RelationshipVisitor<EX> visitor) throws EX;

    // SCHEMA RELATED

    DiffSets<IndexDescriptor> indexDiffSetsByLabel(int labelId);

    DiffSets<IndexDescriptor> indexDiffSetsByRelationshipType(int relationshipType);

    DiffSets<IndexDescriptor> indexDiffSetsBySchema(SchemaDescriptor schema);

    DiffSets<IndexDescriptor> indexChanges();

    Iterator<IndexDescriptor> constraintIndexesCreatedInTx();

    DiffSets<ConstraintDescriptor> constraintsChanges();

    DiffSets<ConstraintDescriptor> constraintsChangesForLabel(int labelId);

    DiffSets<ConstraintDescriptor> constraintsChangesForSchema(SchemaDescriptor descriptor);

    DiffSets<ConstraintDescriptor> constraintsChangesForRelationshipType(int relTypeId);

    // INDEX UPDATES

    /**
     * A readonly view of all index updates for the provided schema. Returns {@code null}, if the index
     * updates for this schema have not been initialized.
     */
    UnmodifiableMap<ValueTuple, ? extends LongDiffSets> getIndexUpdates(SchemaDescriptor schema);

    /**
     * A readonly view of all index updates for the provided schema, in sorted order. The returned
     * Map is unmodifiable. Returns {@code null}, if the index updates for this schema have not been initialized.
     * <p>
     * Ensure sorted index updates for a given index. This is needed for range query support and
     * ay involve converting the existing hash map first.
     */
    NavigableMap<ValueTuple, ? extends LongDiffSets> getSortedIndexUpdates(SchemaDescriptor descriptor);

    // OTHER

    NodeState getNodeState(long id);

    RelationshipState getRelationshipState(long id);

    MutableIntSet augmentLabels(MutableIntSet labels, NodeState nodeState);

    /**
     * The revision of the data changes in this transaction. This number is opaque, except that it is zero if there have been no data changes in this
     * transaction. And making and then undoing a change does not count as "no data changes." This number will always change when there is a data change in a
     * transaction, however, such that one can reliably tell whether or not there has been any data changes in a transaction since last time the transaction
     * data revision was obtained for the given transaction.
     * <p>
     * This has a number of uses. For instance, the way tokens are created is that the first time a token is needed it gets created in its own little
     * token mini-transaction, separate from the surrounding transaction that creates or modifies data that need it.
     * From the kernel POV it's interesting to know whether or not any tokens have been created in this tx state, because then we know it's a mini-transaction
     * like this and won't have to let transaction event handlers know about it, for example.
     * <p>
     * The same applies to schema changes, such as creating and dropping indexes and constraints.
     * <p>
     * The fulltext schema indexes also use the data revision to determine at query-time, if their internal transaction state needs to be updated.
     *
     * @return The opaque data revision for this transaction, or zero if there has been no data changes in this transaction.
     */
    long getDataRevision();

    /**
     * @return {@code true} if there are any <em>data</em> changes in the transaction.
     */
    boolean hasDataChanges();

    /**
     * @return {@code true} if this transaction state is multi chunked
     */
    boolean isMultiChunk();
}
