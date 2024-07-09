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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;

import java.io.IOException;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.StorageEntityCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;

abstract class StoreScanChunk<T extends StorageEntityCursor> implements InputChunk {
    final StoragePropertyCursor storePropertyCursor;
    protected final T cursor;
    private final boolean requiresPropertyMigration;
    private final CursorContext cursorContext;
    private long id;
    private long endId;

    StoreScanChunk(
            T cursor,
            RecordStorageReader storageReader,
            boolean requiresPropertyMigration,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        this.cursor = cursor;
        this.requiresPropertyMigration = requiresPropertyMigration;
        this.storePropertyCursor = storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker);
        this.cursorContext = cursorContext;
    }

    void visitProperties(T record, InputEntityVisitor visitor) {
        if (!requiresPropertyMigration) {
            visitor.propertyId(((LongReference) record.propertiesReference()).id);
        } else {
            record.properties(storePropertyCursor, ALL_PROPERTIES);
            while (storePropertyCursor.next()) {
                // add key as int here as to have the importer use the token id
                visitor.property(
                        storePropertyCursor.propertyKey(),
                        storePropertyCursor.propertyValue().asObject());
            }
            storePropertyCursor.close();
        }
    }

    @Override
    public void close() {
        IOUtils.closeAllUnchecked(cursor, storePropertyCursor, cursorContext);
    }

    @Override
    public boolean next(InputEntityVisitor visitor) throws IOException {
        if (id < endId) {
            read(cursor, id);
            if (cursor.next()) {
                visitRecord(cursor, visitor);
                visitor.endOfEntity();
            }
            id++;
            return true;
        }
        return false;
    }

    protected abstract void read(T cursor, long id);

    public void initialize(long startId, long endId) {
        this.id = startId;
        this.endId = endId;
    }

    abstract void visitRecord(T record, InputEntityVisitor visitor);
}
