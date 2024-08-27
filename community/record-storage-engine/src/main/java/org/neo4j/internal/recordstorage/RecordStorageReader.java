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
package org.neo4j.internal.recordstorage;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

import java.util.Collection;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.function.Function;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.counts.CountsStore;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.StorageSchemaReaderSnapshot;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;

/**
 * Default implementation of StorageReader. Delegates to NeoStores and indexes.
 */
public class RecordStorageReader implements StorageReader {
    // These token holders should perhaps move to the cache layer.. not really any reason to have them here?
    private final TokenHolders tokenHolders;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore relationshipGroupStore;
    private final PropertyStore propertyStore;
    private final CountsStore counts;
    private final RelationshipGroupDegreesStore groupDegreesStore;
    private final SchemaCache schemaCache;

    private boolean closed;

    RecordStorageReader(
            TokenHolders tokenHolders,
            NeoStores neoStores,
            CountsStore counts,
            RelationshipGroupDegreesStore groupDegreesStore,
            SchemaCache schemaCache) {
        this.tokenHolders = tokenHolders;
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.relationshipGroupStore = neoStores.getRelationshipGroupStore();
        this.propertyStore = neoStores.getPropertyStore();
        this.counts = counts;
        this.groupDegreesStore = groupDegreesStore;
        this.schemaCache = schemaCache;
    }

    /**
     * All the nulls in this method is a testament to the fact that we probably need to break apart this reader,
     * separating index stuff out from store stuff.
     */
    public RecordStorageReader(NeoStores stores) {
        this(null, stores, null, null, null);
    }

    public RecordStorageReader(NeoStores stores, SchemaCache schemaCache) {
        this(null, stores, null, null, schemaCache);
    }

    @Override
    public Iterator<IndexDescriptor> indexGetForSchema(SchemaDescriptor descriptor) {
        return schemaCache.indexesForSchema(descriptor);
    }

    @Override
    public IndexDescriptor indexGetForSchemaAndType(SchemaDescriptor descriptor, IndexType type) {
        return schemaCache.indexForSchemaAndType(descriptor, type);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel(int labelId) {
        return schemaCache.indexesForLabel(labelId);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType(int relationshipType) {
        return schemaCache.indexesForRelationshipType(relationshipType);
    }

    @Override
    public IndexDescriptor indexGetForName(String name) {
        return schemaCache.indexForName(name);
    }

    @Override
    public ConstraintDescriptor constraintGetForName(String name) {
        return schemaCache.constraintForName(name);
    }

    @Override
    public boolean indexExists(IndexDescriptor index) {
        return schemaCache.hasIndex(index);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll() {
        return schemaCache.indexes().iterator();
    }

    @Override
    public Collection<IndexDescriptor> valueIndexesGetRelated(int[] labels, int propertyKeyId, EntityType entityType) {
        return schemaCache.getValueIndexesRelatedTo(
                EMPTY_INT_ARRAY, labels, new int[] {propertyKeyId}, false, entityType);
    }

    @Override
    public Collection<IndexDescriptor> valueIndexesGetRelated(
            int[] labels, int[] propertyKeyIds, EntityType entityType) {
        return schemaCache.getValueIndexesRelatedTo(labels, EMPTY_INT_ARRAY, propertyKeyIds, true, entityType);
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
            int[] tokens, int propertyKeyId, EntityType entityType) {
        return schemaCache.getUniquenessConstraintsRelatedTo(
                EMPTY_INT_ARRAY, tokens, new int[] {propertyKeyId}, false, entityType);
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
            int[] entityTokens, int[] propertyKeyIds, EntityType entityType) {
        return schemaCache.getUniquenessConstraintsRelatedTo(
                EMPTY_INT_ARRAY, entityTokens, propertyKeyIds, false, entityType);
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
            int[] changedLabels,
            int[] unchangedLabels,
            int[] propertyKeyIds,
            boolean propertyKeyListIsComplete,
            EntityType entityType) {
        return schemaCache.getUniquenessConstraintsRelatedTo(
                changedLabels, unchangedLabels, propertyKeyIds, propertyKeyListIsComplete, entityType);
    }

    @Override
    public boolean hasRelatedSchema(int[] tokens, int propertyKey, EntityType entityType) {
        return schemaCache.hasRelatedSchema(tokens, propertyKey, entityType);
    }

    @Override
    public boolean hasRelatedSchema(int token, EntityType entityType) {
        return schemaCache.hasRelatedSchema(token, entityType);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema(SchemaDescriptor descriptor) {
        return schemaCache.constraintsForSchema(descriptor);
    }

    @Override
    public boolean constraintExists(ConstraintDescriptor descriptor) {
        return schemaCache.hasConstraintRule(descriptor);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel(int labelId) {
        return schemaCache.constraintsForLabel(labelId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType(int typeId) {
        return schemaCache.constraintsForRelationshipType(typeId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll() {
        return schemaCache.constraints().iterator();
    }

    @Override
    public IntSet[] constraintsGetPropertyTokensForLogicalKey(int token, EntityType entityType) {
        return schemaCache.constraintsGetPropertyTokensForLogicalKey(token, entityType);
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId(IndexDescriptor index) {
        if (index == null) {
            return null;
        }
        OptionalLong owningConstraintId = index.getOwningConstraintId();
        if (owningConstraintId.isPresent()) {
            Long constraintId = owningConstraintId.getAsLong();
            if (schemaCache.hasConstraintRule(constraintId)) {
                return constraintId;
            }
        }
        return null;
    }

    @Override
    public long countsForNode(int labelId, CursorContext cursorContext) {
        return counts.nodeCount(labelId, cursorContext);
    }

    @Override
    public long estimateCountsForNode(int labelId, CursorContext cursorContext) {
        return counts.estimateNodeCount(labelId, cursorContext);
    }

    @Override
    public void visitAllCounts(CountsVisitor visitor, CursorContext cursorContext) {
        counts.accept(visitor, cursorContext);
    }

    @Override
    public long countsForRelationship(int startLabelId, int typeId, int endLabelId, CursorContext cursorContext) {
        if (!(startLabelId == ANY_LABEL || endLabelId == ANY_LABEL)) {
            throw new UnsupportedOperationException("not implemented");
        }
        return counts.relationshipCount(startLabelId, typeId, endLabelId, cursorContext);
    }

    @Override
    public long estimateCountsForRelationship(
            int startLabelId, int typeId, int endLabelId, CursorContext cursorContext) {
        if (!(startLabelId == ANY_LABEL || endLabelId == ANY_LABEL)) {
            throw new UnsupportedOperationException("not implemented");
        }
        return counts.estimateRelationshipCount(startLabelId, typeId, endLabelId, cursorContext);
    }

    @Override
    public long nodesGetCount(CursorContext cursorContext) {
        if (counts != null) {
            try {
                return counts.nodeCount(ANY_LABEL, cursorContext);
            } catch (IllegalStateException e) {
                // This can happen if requesting nodes count before the store has been fully recovered.
                // Counts store cannot return values until then, so we'll just have to return an estimate.
                // The only use case here at the time of writing this is index population progress during recovery.
            }
        }
        // else this reader was instantiated as a simpler reader only over a NeoStores

        return nodeStore.getIdGenerator().getHighId();
    }

    @Override
    public long relationshipsGetCount(CursorContext cursorContext) {
        return relationshipStore.getIdGenerator().getHighId();
    }

    @Override
    public int labelCount() {
        return tokenHolders.labelTokens().size();
    }

    @Override
    public int propertyKeyCount() {
        return tokenHolders.propertyKeyTokens().size();
    }

    @Override
    public int relationshipTypeCount() {
        return tokenHolders.relationshipTypeTokens().size();
    }

    @Override
    public boolean nodeExists(long id, StoreCursors storeCursors) {
        return nodeStore.isInUse(id, storeCursors.readCursor(RecordCursorTypes.NODE_CURSOR));
    }

    @Override
    public boolean relationshipExists(long id, StoreCursors storeCursors) {
        return relationshipStore.isInUse(id, storeCursors.readCursor(RecordCursorTypes.RELATIONSHIP_CURSOR));
    }

    @Override
    public <T> T getOrCreateSchemaDependantState(Class<T> type, Function<StorageReader, T> factory) {
        return schemaCache.getOrCreateDependantState(type, factory, this);
    }

    @Override
    public AllNodeScan allNodeScan() {
        return new RecordNodeScan();
    }

    @Override
    public AllRelationshipsScan allRelationshipScan() {
        return new RecordRelationshipScan();
    }

    @Override
    public void close() {
        assert !closed;
        closed = true;
    }

    @Override
    public RecordNodeCursor allocateNodeCursor(
            CursorContext cursorContext, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return new RecordNodeCursor(
                nodeStore,
                relationshipStore,
                relationshipGroupStore,
                groupDegreesStore,
                cursorContext,
                storeCursors,
                memoryTracker);
    }

    @Override
    public RecordRelationshipTraversalCursor allocateRelationshipTraversalCursor(
            CursorContext cursorContext, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return new RecordRelationshipTraversalCursor(
                relationshipStore,
                relationshipGroupStore,
                groupDegreesStore,
                cursorContext,
                storeCursors,
                memoryTracker);
    }

    @Override
    public RecordRelationshipScanCursor allocateRelationshipScanCursor(
            CursorContext cursorContext, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return new RecordRelationshipScanCursor(relationshipStore, cursorContext, storeCursors, memoryTracker);
    }

    @Override
    public StorageSchemaReader schemaSnapshot() {
        return new StorageSchemaReaderSnapshot(schemaCache.snapshot());
    }

    @Override
    public TokenNameLookup tokenNameLookup() {
        return tokenHolders;
    }

    @Override
    public StoragePropertyCursor allocatePropertyCursor(
            CursorContext cursorContext, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return new RecordPropertyCursor(propertyStore, cursorContext, storeCursors, memoryTracker);
    }
}
