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
package org.neo4j.internal.kernel.api;

import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;

/**
 * Methods for acquiring and releasing locks.
 */
public interface Locks {
    void acquireExclusiveNodeLock(long... ids);

    void acquireExclusiveRelationshipLock(long... ids);

    void releaseExclusiveNodeLock(long... ids);

    void releaseExclusiveRelationshipLock(long... ids);

    void acquireSharedNodeLock(long... ids);

    void acquireSharedRelationshipLock(long... ids);

    void acquireSharedRelationshipTypeLock(long... ids);

    void acquireSharedLabelLock(long... ids);

    void releaseSharedNodeLock(long... ids);

    void releaseSharedRelationshipLock(long... ids);

    void releaseSharedLabelLock(long... ids);

    void releaseSharedRelationshipTypeLock(long... ids);

    void acquireSharedLookupLock(EntityType entityType);

    void releaseSharedLookupLock(EntityType entityType);

    void releaseExclusiveIndexEntryLock(long... indexEntries);

    void acquireExclusiveIndexEntryLock(long... indexEntries);

    void releaseSharedIndexEntryLock(long... indexEntries);

    void acquireSharedIndexEntryLock(long... indexEntries);

    void acquireSharedSchemaLock(SchemaDescriptorSupplier schemaLike);

    void releaseSharedSchemaLock(SchemaDescriptorSupplier schemaLike);
}
