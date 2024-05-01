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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

final class ThrowingConflictDetector<KEY extends NativeIndexKey<KEY>>
        extends ConflictDetectingValueMerger<KEY, Value[]> {
    private final SchemaDescriptor schemaDescriptor;

    ThrowingConflictDetector(boolean compareEntityIds, SchemaDescriptor schemaDescriptor) {
        super(compareEntityIds);
        this.schemaDescriptor = schemaDescriptor;
    }

    @Override
    void doReportConflict(long existingNodeId, long addedNodeId, Value[] values) throws IndexEntryConflictException {
        throw new IndexEntryConflictException(schemaDescriptor, existingNodeId, addedNodeId, ValueTuple.of(values));
    }
}
