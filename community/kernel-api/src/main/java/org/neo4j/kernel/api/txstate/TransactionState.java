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
package org.neo4j.kernel.api.txstate;

import java.util.function.IntPredicate;
import org.neo4j.graphdb.Vector;
import org.neo4j.internal.kernel.api.Upgrade;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.enrichment.EnrichmentMode;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

/**
 * Kernel transaction state, please see {@link org.neo4j.kernel.impl.api.state.TxState} for implementation details.
 *
 * This interface defines the mutating methods for the transaction state, methods for reading are defined in
 * {@link ReadableTransactionState}. These mutating methods follow the rule that they all contain the word "Do" in the name.
 * This naming convention helps deciding where to set {@link #hasChanges()} in the
 * {@link org.neo4j.kernel.impl.api.state.TxState main implementation class}.
 */
public interface TransactionState extends ReadableTransactionState {
    // ENTITY RELATED

    void relationshipDoCreate(long id, int relationshipTypeId, long startNodeId, long endNodeId);

    void reset();

    void nodeDoCreate(long id);

    void relationshipDoDelete(long relationshipId, int type, long startNode, long endNode);

    void relationshipDoDeleteAddedInThisBatch(long relationshipId);

    void nodeDoDelete(long nodeId);

    void nodeDoAddProperty(long nodeId, int newPropertyKeyId, Value value);

    void relationshipDoAddProperty(
            long relationshipId, int type, long startNode, long endNode, int propertyKeyId, Value newValue);

    /**
     * Removes the given property from the specified node.
     * The property must already exist either in the transaction state and/or in the store. The {@link IntPredicate} is
     * used to decide if we should remove the property from the store as well after removing it from the transaction state.
     * Important to only mark the property to be removed from store if it already exists.
     *
     * @param nodeId The node to remove a property from
     * @param propertyKeyId The property key id of the property to remove
     * @param removeFromStore A predicate that decides if the property should be removed from store after it is removed from tx state
     */
    void nodeDoRemoveProperty(long nodeId, int propertyKeyId, IntPredicate removeFromStore);

    /**
     * Removes the given property from the specified relationship.
     * The property must already exist either in the transaction state and/or in the store. The {@link IntPredicate} is
     * used to decide if we should remove the property from the store as well after removing it from the transaction state.
     * Important to only mark the property to be removed from store if it already exists.
     *
     * @param relationshipId The relationship to remove a property from
     * @param type The relationship type id
     * @param startNode The start node id of the relationship
     * @param endNode The end node id of the relationship
     * @param propertyKeyId The property key id of the property to remove
     * @param removeFromStore A predicate that decides if the property should be removed from store after it is removed from tx state
     */
    void relationshipDoRemoveProperty(
            long relationshipId,
            int type,
            long startNode,
            long endNode,
            int propertyKeyId,
            IntPredicate removeFromStore);

    void nodeDoAddLabel(int labelId, long nodeId);

    void nodeDoRemoveLabel(int labelId, long nodeId);

    // TOKEN RELATED

    void labelDoCreateForName(String labelName, boolean internal, long id);

    void propertyKeyDoCreateForName(String propertyKeyName, boolean internal, int id);

    void relationshipTypeDoCreateForName(String relationshipTypeName, boolean internal, int id);

    // SCHEMA RELATED

    void indexDoAdd(IndexDescriptor index);

    void indexDoDrop(IndexDescriptor index);

    void constraintDoAdd(ConstraintDescriptor constraint);

    void constraintDoAdd(IndexBackedConstraintDescriptor constraint, IndexDescriptor index);

    void constraintDoDrop(ConstraintDescriptor constraint);

    void indexDoUpdateEntry(IndexDescriptor descriptor, long nodeId, ValueTuple before, ValueTuple after);

    // Upgrade
    void kernelDoUpgrade(Upgrade.KernelUpgrade kernelUpgrade);

    // MEMORY TRACKING

    /**
     * Return memory tracker that tracks the amount of memory current tx state occupy in memory.
     * Please note that this tracker is not the whole transaction memory tracker and may be measuring only parts
     * that are still in memory in this particular transaction state(it may be backed by the store)
     */
    MemoryTracker memoryTracker();

    /**
     * Create a vector store for the given coordinate type and dimensions.
     * Note that the vector store only gets created during commit of the transaction, not during this call.
     * A transaction is only allowed to create a single vector store. Calling this method more than once in
     * a transaction will throw an Exception.
     */
    void createVectorStore(Vector.CoordinateType coordinateType, int dimensions);

    /**
     * Set the ontology, replacing any value set earlier in this transaction. {@code null} or empty clears it.
     */
    void setOntology(String value);

    boolean hasOntologyChange();

    /**
     * @return the ontology set in this transaction, or {@code null} if it was cleared.
     * Only meaningful when {@link #hasOntologyChange()} is {@code true}.
     */
    String ontologyChange();

    EnrichmentMode enrichmentMode();
}
