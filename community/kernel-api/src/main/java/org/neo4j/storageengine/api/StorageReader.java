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
package org.neo4j.storageengine.api;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Abstraction for accessing data from a {@link StorageEngine}.
 * A reader is expected to be scoped to one transaction or query or similar.
 */
public interface StorageReader extends AutoCloseable, StorageSchemaReader {
    /**
     * Closes this reader and all associated resources.
     */
    @Override
    void close();

    /**
     * Check whether or not the given index exists in the store.
     * @param index The index to check.
     * @return {@code true} if the index exists, {@code false} otherwise.
     */
    boolean indexExists(IndexDescriptor index);

    /**
     * Returns all value indexes (including unique) related to a property, any of the tokens (labels or relationship types) and the entity type.
     */
    Collection<IndexDescriptor> valueIndexesGetRelated(int[] tokens, int propertyKeyId, EntityType entityType);

    Collection<IndexDescriptor> valueIndexesGetRelated(int[] tokens, int[] propertyKeyIds, EntityType entityType);

    Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
            int[] tokens, int propertyKeyId, EntityType entityType);

    Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
            int[] tokens, int[] propertyKeyIds, EntityType entityType);

    Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
            int[] changedLabels,
            int[] unchangedLabels,
            int[] propertyKeyIds,
            boolean propertyKeyListIsComplete,
            EntityType entityType);

    boolean hasRelatedSchema(int[] tokens, int propertyKey, EntityType entityType);

    boolean hasRelatedSchema(int token, EntityType entityType);

    /**
     * @param index {@link IndexDescriptor} to get related uniqueness constraint for.
     * @return schema rule id of uniqueness constraint that owns the given {@code index}, or {@code null}
     * if the given index isn't related to a uniqueness constraint.
     */
    Long indexGetOwningUniquenessConstraintId(IndexDescriptor index);

    /**
     * @param descriptor describing the label and property key (or keys) defining the requested constraint.
     * @return node property constraints associated with the label and one or more property keys token ids.
     */
    Iterator<ConstraintDescriptor> constraintsGetForSchema(SchemaDescriptor descriptor);

    boolean constraintExists(ConstraintDescriptor descriptor);

    /**
     * Returns number of stored nodes labeled with the label represented by {@code labelId}.
     *
     * @param labelId label id to match.
     * @param cursorContext underlying page cursor context
     * @return number of stored nodes with this label.
     */
    long countsForNode(int labelId, CursorContext cursorContext);

    long estimateCountsForNode(int labelId, CursorContext cursorContext);

    void visitAllCounts(CountsVisitor visitor, CursorContext cursorContext);

    /**
     * Returns number of stored relationships of a certain {@code typeId} whose start/end nodes are labeled
     * with the {@code startLabelId} and {@code endLabelId} respectively.
     *
     * @param startLabelId label id of start nodes to match.
     * @param typeId relationship type id to match.
     * @param endLabelId label id of end nodes to match.
     * @param cursorContext underlying page cursor context
     * @return number of stored relationships matching these criteria.
     */
    long countsForRelationship(int startLabelId, int typeId, int endLabelId, CursorContext cursorContext);

    long estimateCountsForRelationship(int startLabelId, int typeId, int endLabelId, CursorContext cursorContext);

    long nodesGetCount(CursorContext cursorContext);

    long relationshipsGetCount(CursorContext cursorTracer);

    int labelCount();

    int propertyKeyCount();

    int relationshipTypeCount();

    boolean nodeExists(long id, StoreCursors storeCursors);

    boolean relationshipExists(long id, StoreCursors storeCursors);

    <T> T getOrCreateSchemaDependantState(Class<T> type, Function<StorageReader, T> factory);

    /**
     * Batched all node scan
     * @return a new AllNodeScan maintaining the state of the batched all-node scan
     */
    AllNodeScan allNodeScan();

    /**
     * Batched all relationship scan
     * @return a new AllRelationship maintaining the state of the batched all-relationship scan
     */
    AllRelationshipsScan allRelationshipScan();

    /**
     * @return a new {@link StorageNodeCursor} capable of reading node data from the underlying storage.
     */
    StorageNodeCursor allocateNodeCursor(CursorContext cursorContext, StoreCursors storeCursors);

    /**
     * @return a new {@link StoragePropertyCursor} capable of reading property data from the underlying storage.
     */
    StoragePropertyCursor allocatePropertyCursor(
            CursorContext cursorContext, StoreCursors storeCursors, MemoryTracker memoryTracker);

    /**
     * @return a new {@link StorageRelationshipTraversalCursor} capable of traversing relationships from the underlying storage.
     */
    StorageRelationshipTraversalCursor allocateRelationshipTraversalCursor(
            CursorContext cursorContext, StoreCursors storeCursors);

    /**
     * @return a new {@link StorageRelationshipScanCursor} capable of reading relationship data from the underlying storage.
     */
    StorageRelationshipScanCursor allocateRelationshipScanCursor(
            CursorContext cursorContext, StoreCursors storeCursors);

    /**
     * Get a lock-free snapshot of the current schema, for inspecting the current schema when no mutations are intended.
     * <p>
     * The index states, such as failure messages and population progress, are not captured in the snapshot, but are instead queried "live".
     * This means that if an index in the snapshot is then later deleted, then querying for the state of the index via the snapshot will throw an
     * {@link IndexNotFoundKernelException}.
     *
     * @return a snapshot of the current schema.
     */
    StorageSchemaReader schemaSnapshot();

    /**
     * Get a non-transactional {@link TokenNameLookup} instance.
     * @return a token name lookup instance.
     */
    TokenNameLookup tokenNameLookup();
}
