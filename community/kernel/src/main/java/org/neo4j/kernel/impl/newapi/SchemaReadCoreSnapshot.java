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

import static org.neo4j.internal.helpers.collection.Iterators.singleOrNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Predicate;
import org.neo4j.collection.diffset.DiffSets;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexUsageStats;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.storageengine.api.StorageSchemaReader;

/**
 * Implementation of SchemaReadCore that doesn't check for transaction being open and doesn't take locks
 */
final class SchemaReadCoreSnapshot implements SchemaReadCore {
    private final StorageSchemaReader storageReader;
    private final TxStateHolder txStateHolder;
    private final IndexingService indexingService;
    private final IndexStatisticsStore indexStatisticsStore;
    private final AccessModeProvider accessModeProvider;

    SchemaReadCoreSnapshot(
            StorageSchemaReader storageReader,
            TxStateHolder txStateHolder,
            IndexingService indexingService,
            IndexStatisticsStore indexStatisticsStore,
            AccessModeProvider accessModeProvider) {
        this.storageReader = storageReader;
        this.txStateHolder = txStateHolder;
        this.indexingService = indexingService;
        this.indexStatisticsStore = indexStatisticsStore;
        this.accessModeProvider = accessModeProvider;
    }

    @Override
    public IndexDescriptor indexGetForName(String name) {
        var index = storageReader.indexGetForName(name);
        if (txStateHolder.hasTxStateWithChanges()) {
            Predicate<IndexDescriptor> namePredicate =
                    indexDescriptor -> indexDescriptor.getName().equals(name);
            Iterator<IndexDescriptor> indexes = txStateHolder
                    .txState()
                    .indexChanges()
                    .filterAdded(namePredicate)
                    .apply(Iterators.iterator(index));
            index = singleOrNull(indexes);
        }
        return index != null ? index : IndexDescriptor.NO_INDEX;
    }

    @Override
    public ConstraintDescriptor constraintGetForName(String name) {
        ConstraintDescriptor constraint = storageReader.constraintGetForName(name);
        if (txStateHolder.hasTxStateWithChanges()) {
            Predicate<ConstraintDescriptor> namePredicate =
                    constraintDescriptor -> constraintDescriptor.getName().equals(name);
            Iterator<ConstraintDescriptor> constraints = txStateHolder
                    .txState()
                    .constraintsChanges()
                    .filterAdded(namePredicate)
                    .apply(Iterators.iterator(constraint));
            constraint = singleOrNull(constraints);
        }
        return constraint;
    }

    @Override
    public Iterator<IndexDescriptor> index(SchemaDescriptor schema) {
        var indexes = storageReader.indexGetForSchema(schema);
        if (txStateHolder.hasTxStateWithChanges()) {
            var diffSets = txStateHolder.txState().indexDiffSetsBySchema(schema);
            indexes = diffSets.apply(indexes);
        }
        return indexes;
    }

    @Override
    public IndexDescriptor index(SchemaDescriptor schema, IndexType type) {
        var index = storageReader.indexGetForSchemaAndType(schema, type);
        if (txStateHolder.hasTxStateWithChanges()) {
            var indexChanges = txStateHolder.txState().indexChanges();
            if (index == null) {
                // check if such index was added in this tx
                var added = indexChanges
                        .filterAdded(
                                id -> id.getIndexType() == type && id.schema().equals(schema))
                        .getAdded();
                index = singleOrNull(added.iterator());
            }

            if (indexChanges.isRemoved(index)) {
                // this index was removed in this tx
                return IndexDescriptor.NO_INDEX;
            }
        }
        return index;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel(int labelId) {
        AccessMode accessMode = accessModeProvider.getAccessMode();
        if (accessMode.allowsTraverseNode(labelId) || accessMode.hasApplicableTraverseAllowPropertyRules(labelId)) {
            Iterator<IndexDescriptor> iterator = storageReader.indexesGetForLabel(labelId);

            if (txStateHolder.hasTxStateWithChanges()) {
                iterator = txStateHolder.txState().indexDiffSetsByLabel(labelId).apply(iterator);
            }

            return iterator;
        }
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType(int relationshipType) {
        var iterator = storageReader.indexesGetForRelationshipType(relationshipType);
        if (txStateHolder.hasTxStateWithChanges()) {
            iterator = txStateHolder
                    .txState()
                    .indexDiffSetsByRelationshipType(relationshipType)
                    .apply(iterator);
        }
        return iterator;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll() {
        var iterator = storageReader.indexesGetAll();
        if (txStateHolder.hasTxStateWithChanges()) {
            iterator = txStateHolder.txState().indexChanges().apply(iterator);
        }
        return iterator;
    }

    @Override
    public InternalIndexState indexGetState(IndexDescriptor index) throws IndexNotFoundKernelException {
        return indexGetStateNonLocking(index);
    }

    @Override
    public InternalIndexState indexGetStateNonLocking(IndexDescriptor index) throws IndexNotFoundKernelException {
        AllStoreHolder.assertValidIndex(index);
        if (txStateHolder.hasTxStateWithChanges()) {
            if (checkIndexState(index, txStateHolder.txState().indexDiffSetsBySchema(index.schema()))) {
                return InternalIndexState.POPULATING;
            }
        }

        return indexingService.getIndexProxy(index).getState();
    }

    @Override
    public PopulationProgress indexGetPopulationProgress(IndexDescriptor index) throws IndexNotFoundKernelException {
        AllStoreHolder.assertValidIndex(index);
        if (txStateHolder.hasTxStateWithChanges()) {
            if (checkIndexState(index, txStateHolder.txState().indexDiffSetsBySchema(index.schema()))) {
                return PopulationProgress.NONE;
            }
        }

        return indexingService.getIndexProxy(index).getIndexPopulationProgress();
    }

    private static boolean checkIndexState(IndexDescriptor index, DiffSets<IndexDescriptor> diffSet)
            throws IndexNotFoundKernelException {
        if (diffSet.isAdded(index)) {
            return true;
        }
        if (diffSet.isRemoved(index)) {
            throw new IndexNotFoundKernelException("Index has been dropped in this transaction: ", index);
        }
        return false;
    }

    @Override
    public String indexGetFailure(IndexDescriptor index) throws IndexNotFoundKernelException {
        AllStoreHolder.assertValidIndex(index);
        return indexingService.getIndexProxy(index).getPopulationFailure().asString();
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel(int labelId) {
        return constraintsGetForLabelNonLocking(labelId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabelNonLocking(int labelId) {
        Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetForLabel(labelId);
        if (txStateHolder.hasTxStateWithChanges()) {
            return txStateHolder.txState().constraintsChangesForLabel(labelId).apply(constraints);
        }
        return constraints;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType(int typeId) {
        return constraintsGetForRelationshipTypeNonLocking(typeId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipTypeNonLocking(int typeId) {
        Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetForRelationshipType(typeId);
        if (txStateHolder.hasTxStateWithChanges()) {
            return txStateHolder
                    .txState()
                    .constraintsChangesForRelationshipType(typeId)
                    .apply(constraints);
        }
        return constraints;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll() {
        return constraintsGetAllNonLocking();
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAllNonLocking() {
        var constraints = storageReader.constraintsGetAll();
        if (txStateHolder.hasTxStateWithChanges()) {
            constraints = txStateHolder.txState().constraintsChanges().apply(constraints);
        }
        return constraints;
    }

    @Override
    public IndexUsageStats indexUsageStats(IndexDescriptor index) throws IndexNotFoundKernelException {
        AllStoreHolder.assertValidIndex(index);
        return indexStatisticsStore.usageStats(index.getId());
    }
}
