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
package org.neo4j.kernel.impl.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.SCHEMA_CURSOR;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class SchemaStoreConsistentReadTest extends RecordStoreConsistentReadTest<SchemaRecord, SchemaStore> {
    @Override
    protected SchemaStore getStore(NeoStores neoStores) {
        return neoStores.getSchemaStore();
    }

    @Override
    protected PageCursor getCursor(StoreCursors storeCursors) {
        return storeCursors.readCursor(SCHEMA_CURSOR);
    }

    @Override
    protected SchemaRecord createNullRecord(long id) {
        return new SchemaRecord(id); // This is what it looks like when an unused record is force-read from the store.
    }

    @Override
    protected SchemaRecord createExistingRecord(boolean light) {
        SchemaRecord record = new SchemaRecord(ID);
        record.initialize(true, 42);
        return record;
    }

    @Override
    protected SchemaRecord getLight(long id, SchemaStore store, PageCursor pageCursor) {
        return getHeavy(store, id, pageCursor);
    }

    @Override
    protected void assertRecordsEqual(SchemaRecord actualRecord, SchemaRecord expectedRecord) {
        assertNotNull(actualRecord, "actualRecord");
        assertNotNull(expectedRecord, "expectedRecord");
        assertThat(actualRecord.isConstraint()).as("isConstraint").isEqualTo(expectedRecord.isConstraint());
        assertThat(actualRecord.getNextProp()).as("getNextProp").isEqualTo(expectedRecord.getNextProp());
        assertThat(actualRecord.getId()).as("getId").isEqualTo(expectedRecord.getId());
        assertThat(actualRecord.getId()).as("getLongId").isEqualTo(expectedRecord.getId());
    }
}
