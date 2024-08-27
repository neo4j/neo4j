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

import static org.neo4j.io.IOUtils.closeAllUnchecked;

import java.io.IOException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.storageengine.util.IdUpdateListener;

public class SchemaRuleMigrationAccessImpl implements SchemaRuleMigrationAccess {
    private final NeoStores neoStores;
    private final SchemaStorage schemaStorage;
    private final DynamicAllocatorProvider allocationProvider;
    private final CursorContext cursorContext;
    private final MemoryTracker memoryTracker;
    private final StoreCursors storeCursors;

    SchemaRuleMigrationAccessImpl(
            NeoStores neoStores,
            SchemaStorage schemaStorage,
            DynamicAllocatorProvider allocationProvider,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            StoreCursors storeCursors) {
        this.neoStores = neoStores;
        this.schemaStorage = schemaStorage;
        this.allocationProvider = allocationProvider;
        this.cursorContext = cursorContext;
        this.memoryTracker = memoryTracker;
        this.storeCursors = storeCursors;
    }

    @Override
    public Iterable<SchemaRule> getAll() {
        return schemaStorage.getAll(storeCursors, memoryTracker);
    }

    @Override
    public void writeSchemaRule(SchemaRule rule) throws KernelException {
        schemaStorage.writeSchemaRule(
                rule, IdUpdateListener.DIRECT, allocationProvider, cursorContext, memoryTracker, storeCursors);
    }

    @Override
    public void deleteSchemaRule(long id) throws KernelException {
        schemaStorage.deleteSchemaRule(id, IdUpdateListener.DIRECT, cursorContext, memoryTracker, storeCursors);
    }

    @Override
    public long nextId() {
        return schemaStorage.newRuleId(cursorContext);
    }

    @Override
    public void close() throws IOException {
        storeCursors.close();
        neoStores.flush(DatabaseFlushEvent.NULL, cursorContext);
        closeAllUnchecked(storeCursors, neoStores, cursorContext);
    }
}
