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
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexUsageStats;
import org.neo4j.storageengine.api.StorageSchemaReader;

class SchemaReadCoreSnapshot implements SchemaReadCore {
    private final StorageSchemaReader snapshot;
    private final AllStoreHolder stores;

    SchemaReadCoreSnapshot(StorageSchemaReader snapshot, AllStoreHolder stores) {
        this.snapshot = snapshot;
        this.stores = stores;
    }

    @Override
    public IndexDescriptor indexGetForName(String name) {
        return stores.indexGetForName(snapshot, name);
    }

    @Override
    public ConstraintDescriptor constraintGetForName(String name) {
        return stores.constraintGetForName(snapshot, name);
    }

    @Override
    public Iterator<IndexDescriptor> index(SchemaDescriptor schema) {
        stores.performCheckBeforeOperation();
        return stores.indexGetForSchema(snapshot, schema);
    }

    @Override
    public IndexDescriptor index(SchemaDescriptor schema, IndexType type) {
        stores.performCheckBeforeOperation();
        return stores.index(schema, type);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel(int labelId) {
        stores.performCheckBeforeOperation();
        return stores.indexesGetForLabel(snapshot, labelId);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType(int relationshipType) {
        stores.performCheckBeforeOperation();
        return stores.indexesGetForRelationshipType(snapshot, relationshipType);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll() {
        stores.performCheckBeforeOperation();
        return stores.indexesGetAll(snapshot);
    }

    @Override
    public InternalIndexState indexGetState(IndexDescriptor index) throws IndexNotFoundKernelException {
        AllStoreHolder.assertValidIndex(index);
        stores.performCheckBeforeOperation();
        return stores.indexGetStateLocked(index);
    }

    @Override
    public InternalIndexState indexGetStateNonLocking(IndexDescriptor index) throws IndexNotFoundKernelException {
        return indexGetState(index);
    }

    @Override
    public PopulationProgress indexGetPopulationProgress(IndexDescriptor index) throws IndexNotFoundKernelException {
        AllStoreHolder.assertValidIndex(index);
        stores.performCheckBeforeOperation();
        return stores.indexGetPopulationProgressLocked(index);
    }

    @Override
    public String indexGetFailure(IndexDescriptor index) throws IndexNotFoundKernelException {
        AllStoreHolder.assertValidIndex(index);
        return stores.indexGetFailure(index);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel(int labelId) {
        stores.performCheckBeforeOperation();
        return stores.constraintsGetForLabel(snapshot, labelId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabelNonLocking(int labelId) {
        return constraintsGetForLabel(labelId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType(int typeId) {
        stores.performCheckBeforeOperation();
        return stores.constraintsGetForRelationshipType(snapshot, typeId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipTypeNonLocking(int typeId) {
        return constraintsGetForRelationshipType(typeId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll() {
        stores.performCheckBeforeOperation();
        return stores.constraintsGetAll(snapshot);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAllNonLocking() {
        return constraintsGetAll();
    }

    @Override
    public IndexUsageStats indexUsageStats(IndexDescriptor index) throws IndexNotFoundKernelException {
        return stores.indexUsageStats(index);
    }
}
