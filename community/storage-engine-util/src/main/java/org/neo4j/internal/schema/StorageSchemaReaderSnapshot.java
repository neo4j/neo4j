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
package org.neo4j.internal.schema;

import java.util.Iterator;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.neo4j.common.EntityType;
import org.neo4j.storageengine.api.StorageSchemaReader;

public class StorageSchemaReaderSnapshot implements StorageSchemaReader {
    private final SchemaCache schema;

    public StorageSchemaReaderSnapshot(SchemaCache schema) {
        this.schema = schema;
    }

    @Override
    public IndexDescriptor indexGetForName(String name) {
        return schema.indexForName(name);
    }

    @Override
    public ConstraintDescriptor constraintGetForName(String name) {
        return schema.constraintForName(name);
    }

    @Override
    public Iterator<IndexDescriptor> indexGetForSchema(SchemaDescriptor descriptor) {
        return schema.indexesForSchema(descriptor);
    }

    @Override
    public IndexDescriptor indexGetForSchemaAndType(SchemaDescriptor descriptor, IndexType type) {
        return schema.indexForSchemaAndType(descriptor, type);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel(int labelId) {
        return schema.indexesForLabel(labelId);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType(int relationshipType) {
        return schema.indexesForRelationshipType(relationshipType);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll() {
        return schema.indexes().iterator();
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel(int labelId) {
        return schema.constraintsForLabel(labelId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType(int typeId) {
        return schema.constraintsForRelationshipType(typeId);
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll() {
        return schema.constraints().iterator();
    }

    @Override
    public IntSet[] constraintsGetPropertyTokensForLogicalKey(int token, EntityType entityType) {
        return schema.constraintsGetPropertyTokensForLogicalKey(token, entityType);
    }
}
