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
package org.neo4j.kernel.impl.newapi;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import org.neo4j.collection.diffset.DiffSets;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUsageStats;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageSchemaReader;

public class KernelSchemaRead implements SchemaRead {

    private final SchemaState schemaState;
    private final IndexStatisticsStore indexStatisticsStore;
    private final StorageReader storageReader;
    private final Locks entityLocks;
    protected final TxStateHolder txStateHolder;
    private final IndexingService indexingService;
    private final AssertOpen assertOpen;
    private final AccessModeProvider accessModeProvider;
    private final SchemaReadCoreSnapshot schemaReadCoreImpl;

    public KernelSchemaRead(
            SchemaState schemaState,
            IndexStatisticsStore indexStatisticsStore,
            StorageReader storageReader,
            Locks entityLocks,
            TxStateHolder txStateHolder,
            IndexingService indexingService,
            AssertOpen assertOpen,
            AccessModeProvider accessModeProvider) {
        this.schemaState = schemaState;
        this.indexStatisticsStore = indexStatisticsStore;
        this.storageReader = storageReader;
        this.entityLocks = entityLocks;
        this.txStateHolder = txStateHolder;
        this.indexingService = indexingService;
        this.assertOpen = assertOpen;
        this.accessModeProvider = accessModeProvider;
        this.schemaReadCoreImpl = new SchemaReadCoreSnapshot(
                storageReader, txStateHolder, indexingService, indexStatisticsStore, accessModeProvider);
    }

    static void assertValidIndex(IndexDescriptor index) throws IndexNotFoundKernelException {
        if (index == IndexDescriptor.NO_INDEX) {
            throw new IndexNotFoundKernelException("No index was found");
        }
    }

    private void performCheckBeforeOperation() {
        assertOpen.assertOpen();
    }

    @Override
    public IndexUsageStats indexUsageStats(IndexDescriptor index) throws IndexNotFoundKernelException {
        performCheckBeforeOperation();
        return schemaReadCoreImpl.indexUsageStats(index);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAllNonLocking() {
        performCheckBeforeOperation();
        return schemaReadCoreImpl.constraintsGetAllNonLocking();
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll() {
        performCheckBeforeOperation();
        return lockConstraints(schemaReadCoreImpl.constraintsGetAllNonLocking());
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipTypeNonLocking(int typeId) {
        performCheckBeforeOperation();
        return schemaReadCoreImpl.constraintsGetForRelationshipTypeNonLocking(typeId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType(int typeId) {
        performCheckBeforeOperation();
        entityLocks.acquireSharedRelationshipTypeLock(typeId);
        return schemaReadCoreImpl.constraintsGetForRelationshipTypeNonLocking(typeId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabelNonLocking(int labelId) {
        performCheckBeforeOperation();
        return schemaReadCoreImpl.constraintsGetForLabelNonLocking(labelId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel(int labelId) {
        performCheckBeforeOperation();
        entityLocks.acquireSharedLabelLock(labelId);
        return lockConstraints(schemaReadCoreImpl.constraintsGetForLabelNonLocking(labelId));
    }

    @Override
    public String indexGetFailure(IndexDescriptor index) throws IndexNotFoundKernelException {
        performCheckBeforeOperation();
        return schemaReadCoreImpl.indexGetFailure(index);
    }

    @Override
    public PopulationProgress indexGetPopulationProgress(IndexDescriptor index) throws IndexNotFoundKernelException {
        performCheckBeforeOperation();
        return schemaReadCoreImpl.indexGetPopulationProgress(index);
    }

    @Override
    public InternalIndexState indexGetStateNonLocking(IndexDescriptor index) throws IndexNotFoundKernelException {
        performCheckBeforeOperation();
        return schemaReadCoreImpl.indexGetStateNonLocking(index);
    }

    @Override
    public InternalIndexState indexGetState(IndexDescriptor index) throws IndexNotFoundKernelException {
        performCheckBeforeOperation();
        entityLocks.acquireSharedSchemaLock(index);
        return schemaReadCoreImpl.indexGetStateNonLocking(index);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll() {
        performCheckBeforeOperation();
        return lockIndexes(schemaReadCoreImpl.indexesGetAll());
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType(int relationshipType) {
        performCheckBeforeOperation();
        entityLocks.acquireSharedRelationshipTypeLock(relationshipType);
        return lockIndexes(schemaReadCoreImpl.indexesGetForRelationshipType(relationshipType));
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel(int labelId) {
        performCheckBeforeOperation();
        entityLocks.acquireSharedLabelLock(labelId);
        return lockIndexes(schemaReadCoreImpl.indexesGetForLabel(labelId));
    }

    @Override
    public IndexDescriptor index(SchemaDescriptor schema, IndexType type) {
        performCheckBeforeOperation();
        return lockIndex(schemaReadCoreImpl.index(schema, type));
    }

    @Override
    public Iterator<IndexDescriptor> index(SchemaDescriptor schema) {
        performCheckBeforeOperation();
        return lockIndexes(schemaReadCoreImpl.index(schema));
    }

    @Override
    public ConstraintDescriptor constraintGetForName(String name) {
        performCheckBeforeOperation();
        return lockConstraint(schemaReadCoreImpl.constraintGetForName(name));
    }

    @Override
    public IndexDescriptor indexGetForName(String name) {
        performCheckBeforeOperation();
        return lockIndex(schemaReadCoreImpl.indexGetForName(name));
    }

    @Override
    public Iterator<IndexDescriptor> indexForSchemaNonTransactional(SchemaDescriptor schema) {
        return storageReader.indexGetForSchema(schema);
    }

    @Override
    public IndexDescriptor indexForSchemaAndIndexTypeNonTransactional(SchemaDescriptor schema, IndexType indexType) {
        var index = storageReader.indexGetForSchemaAndType(schema, indexType);
        return index == null ? IndexDescriptor.NO_INDEX : index;
    }

    @Override
    public Iterator<IndexDescriptor> indexForSchemaNonLocking(SchemaDescriptor schema) {
        return schemaReadCoreImpl.index(schema);
    }

    @Override
    public Iterator<IndexDescriptor> getLabelIndexesNonLocking(int labelId) {
        return schemaReadCoreImpl.indexesGetForLabel(labelId);
    }

    @Override
    public Iterator<IndexDescriptor> getRelTypeIndexesNonLocking(int relTypeId) {
        return schemaReadCoreImpl.indexesGetForRelationshipType(relTypeId);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAllNonLocking() {
        return schemaReadCoreImpl.indexesGetAll();
    }

    /**
     * Lock the given index if it is valid and exists.
     * If the given index descriptor does not reference an index that exists, then {@link IndexDescriptor#NO_INDEX} is returned.
     *
     * @param index            committed, transaction-added or even null.
     * @return The validated index descriptor, which is not necessarily the same as the one given as an argument.
     */
    private IndexDescriptor lockIndex(IndexDescriptor index) {
        if (index == null || index == IndexDescriptor.NO_INDEX) {
            return IndexDescriptor.NO_INDEX;
        }
        entityLocks.acquireSharedSchemaLock(index);
        // Since the schema cache gives us snapshots views of the schema, the indexes could be dropped in-between us
        // getting the snapshot, and taking the shared schema locks.
        // Thus, after we take the lock, we need to filter out indexes that no longer exists.
        if (indexNotExists(index)) {
            entityLocks.releaseSharedSchemaLock(index);
            return IndexDescriptor.NO_INDEX;
        }
        return index;
    }

    /**
     * Maps all index descriptors according to {@link KernelSchemaRead#lockIndex(IndexDescriptor)}.
     */
    private Iterator<IndexDescriptor> lockIndexes(Iterator<IndexDescriptor> indexes) {
        Predicate<IndexDescriptor> exists = index -> index != IndexDescriptor.NO_INDEX;
        return Iterators.filter(exists, Iterators.map(this::lockIndex, indexes));
    }

    private boolean indexNotExists(IndexDescriptor index) {
        if (txStateHolder.hasTxStateWithChanges()) {
            DiffSets<IndexDescriptor> changes = txStateHolder.txState().indexChanges();
            return !changes.isAdded(index) && (!storageReader.indexExists(index) || changes.isRemoved(index));
        }
        return !storageReader.indexExists(index);
    }

    public void assertIndexExists(IndexDescriptor index) throws IndexNotFoundKernelException {
        if (indexNotExists(index)) {
            throw new IndexNotFoundKernelException("Index does not exist: ", index);
        }
    }

    private ConstraintDescriptor lockConstraint(ConstraintDescriptor constraint) {
        if (constraint == null) {
            return null;
        }
        entityLocks.acquireSharedSchemaLock(constraint);
        if (!constraintExists(constraint)) {
            entityLocks.releaseSharedSchemaLock(constraint);
            return null;
        }
        return constraint;
    }

    private Iterator<ConstraintDescriptor> lockConstraints(Iterator<ConstraintDescriptor> constraints) {
        return Iterators.filter(Objects::nonNull, Iterators.map(this::lockConstraint, constraints));
    }

    @Override
    public boolean constraintExists(ConstraintDescriptor constraint) {
        performCheckBeforeOperation();
        entityLocks.acquireSharedSchemaLock(constraint);

        if (txStateHolder.hasTxStateWithChanges()) {
            var changes = txStateHolder.txState().constraintsChanges();
            return changes.isAdded(constraint)
                    || (storageReader.constraintExists(constraint) && !changes.isRemoved(constraint));
        }
        return storageReader.constraintExists(constraint);
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId(IndexDescriptor index) {
        entityLocks.acquireSharedSchemaLock(index);
        return indexGetOwningUniquenessConstraintIdNonLocking(index);
    }

    @Override
    public Long indexGetOwningUniquenessConstraintIdNonLocking(IndexDescriptor index) {
        performCheckBeforeOperation();
        return storageReader.indexGetOwningUniquenessConstraintId(storageReader.indexGetForName(index.getName()));
    }

    @Override
    public double indexUniqueValuesSelectivity(IndexDescriptor index) throws IndexNotFoundKernelException {
        performCheckBeforeOperation();
        assertValidIndex(index);
        entityLocks.acquireSharedSchemaLock(index);
        assertIndexExists(index); // Throws if the index has been dropped.
        final IndexSample indexSample = indexStatisticsStore.indexSample(index.getId());
        long unique = indexSample.uniqueValues();
        long size = indexSample.sampleSize();
        return size == 0 ? 1.0d : ((double) unique) / ((double) size);
    }

    @Override
    public long indexSize(IndexDescriptor index) throws IndexNotFoundKernelException {
        performCheckBeforeOperation();
        assertValidIndex(index);
        entityLocks.acquireSharedSchemaLock(index);
        return indexStatisticsStore.indexSample(index.getId()).indexSize();
    }

    @Override
    public IndexSample indexSample(IndexDescriptor index) throws IndexNotFoundKernelException {
        performCheckBeforeOperation();
        assertValidIndex(index);
        return indexStatisticsStore.indexSample(index.getId());
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema(SchemaDescriptor schema) {
        entityLocks.acquireSharedSchemaLock(() -> schema);
        return getConstraintsForSchema(schema);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchemaNonLocking(SchemaDescriptor schema) {
        return getConstraintsForSchema(schema);
    }

    private Iterator<ConstraintDescriptor> getConstraintsForSchema(SchemaDescriptor schema) {
        performCheckBeforeOperation();
        Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetForSchema(schema);
        if (txStateHolder.hasTxStateWithChanges()) {
            return txStateHolder.txState().constraintsChangesForSchema(schema).apply(constraints);
        }
        return constraints;
    }

    @Override
    public SchemaReadCore snapshot() {
        performCheckBeforeOperation();
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        return new SchemaReadCoreSnapshot(
                snapshot, txStateHolder, indexingService, indexStatisticsStore, accessModeProvider);
    }

    @Override
    public <K, V> V schemaStateGetOrCreate(K key, Function<K, V> creator) {
        return schemaState.getOrCreate(key, creator);
    }

    @Override
    public void schemaStateFlush() {
        schemaState.clear();
    }
}
