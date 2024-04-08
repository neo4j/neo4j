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

import org.neo4j.internal.kernel.api.Upgrade;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.memory.MemoryTracker;
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

    void nodeDoChangeProperty(long nodeId, int propertyKeyId, Value newValue);

    void relationshipDoReplaceProperty(
            long relationshipId,
            int type,
            long startNode,
            long endNode,
            int propertyKeyId,
            Value replacedValue,
            Value newValue);

    void nodeDoRemoveProperty(long nodeId, int propertyKeyId);

    void relationshipDoRemoveProperty(long relationshipId, int type, long startNode, long endNode, int propertyKeyId);

    void nodeDoAddLabel(int labelId, long nodeId);

    void nodeDoRemoveLabel(int labelId, long nodeId);

    // TOKEN RELATED

    void labelDoCreateForName(String labelName, boolean internal, long id);

    void propertyKeyDoCreateForName(String propertyKeyName, boolean internal, int id);

    void relationshipTypeDoCreateForName(String relationshipTypeName, boolean internal, int id);

    // SCHEMA RELATED

    void indexDoAdd(IndexDescriptor index);

    void indexDoDrop(IndexDescriptor index);

    boolean indexDoUnRemove(IndexDescriptor index);

    void constraintDoAdd(ConstraintDescriptor constraint);

    void constraintDoAdd(IndexBackedConstraintDescriptor constraint, IndexDescriptor index);

    void constraintDoDrop(ConstraintDescriptor constraint);

    boolean constraintDoUnRemove(ConstraintDescriptor constraint);

    void indexDoUpdateEntry(SchemaDescriptor descriptor, long nodeId, ValueTuple before, ValueTuple after);

    // Upgrade
    void kernelDoUpgrade(Upgrade.KernelUpgrade kernelUpgrade);

    // MEMORY TRACKING

    /**
     * Return memory tracker that tracks the amount of memory current tx state occupy in memory.
     * Please note that this tracker is not the whole transaction memory tracker and may be measuring only parts
     * that are still in memory in this particular transaction state(it may be backed by the store)
     */
    MemoryTracker memoryTracker();
}
