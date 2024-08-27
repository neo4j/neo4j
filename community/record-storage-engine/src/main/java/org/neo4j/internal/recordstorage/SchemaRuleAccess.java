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

import java.util.Iterator;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.token.TokenHolders;

public interface SchemaRuleAccess extends org.neo4j.kernel.impl.storemigration.SchemaStorage {

    static SchemaRuleAccess getSchemaRuleAccess(SchemaStore store, TokenHolders tokenHolders) {
        return new SchemaStorage(store, tokenHolders);
    }

    long newRuleId(CursorContext cursorContext);

    SchemaRule loadSingleSchemaRule(long ruleId, StoreCursors storeCursors, MemoryTracker memoryTracker)
            throws MalformedSchemaRuleException;

    Iterator<IndexDescriptor> indexesGetAll(StoreCursors storeCursors, MemoryTracker memoryTracker);

    Iterator<IndexDescriptor> indexesGetAllIgnoreMalformed(StoreCursors storeCursors, MemoryTracker memoryTracker);

    /**
     * Find the IndexRule that matches the given {@link SchemaDescriptorSupplier}.
     *
     * @return an array of all the matching index rules. Empty array if none found.
     * @param memoryTracker
     * @param index the target {@link IndexDescriptor}
     */
    IndexDescriptor[] indexGetForSchema(
            SchemaDescriptorSupplier index, StoreCursors storeCursors, MemoryTracker memoryTracker);

    /**
     * Find the IndexRule that has the given user supplied name.
     *
     * @param memoryTracker
     * @param indexName the user supplied index name to look for.
     * @return the matching IndexRule, or null if no matching index rule was found.
     */
    IndexDescriptor indexGetForName(String indexName, StoreCursors storeCursors, MemoryTracker memoryTracker);

    /**
     * Get the constraint rule that matches the given ConstraintDescriptor
     * @param memoryTracker
     * @param descriptor the ConstraintDescriptor to match
     * @return the matching ConstraintDescriptor
     * @throws SchemaRuleNotFoundException if no ConstraintDescriptor matches the given descriptor
     * @throws DuplicateSchemaRuleException if two or more ConstraintDescriptors match the given descriptor
     */
    ConstraintDescriptor constraintsGetSingle(
            ConstraintDescriptor descriptor, StoreCursors storeCursors, MemoryTracker memoryTracker)
            throws SchemaRuleNotFoundException, DuplicateSchemaRuleException;

    Iterator<ConstraintDescriptor> constraintsGetAllIgnoreMalformed(
            StoreCursors storeCursors, MemoryTracker memoryTracker);

    SchemaRecordChangeTranslator getSchemaRecordChangeTranslator();

    /**
     * Write the given schema rule at the location given by its persistent id, overwriting any data that might be at that location already.
     * This is a non-transactional operation that is used during schema store migration.
     */
    void writeSchemaRule(
            SchemaRule rule,
            IdUpdateListener idUpdateListener,
            DynamicAllocatorProvider allocationProvider,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            StoreCursors storeCursors)
            throws KernelException;

    /**
     * Delete the schema rule with given id.
     * This is a non-transactional operation that is used during schema store migration.
     */
    void deleteSchemaRule(
            long id,
            IdUpdateListener idUpdateListener,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            StoreCursors storeCursors)
            throws KernelException;
}
