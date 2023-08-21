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
package org.neo4j.kernel.impl.store.cursor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.recordstorage.RecordCursorTypes;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.storageengine.api.cursor.CursorType;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
class CachedStoreCursorsIT {
    @Inject
    private RecordStorageEngine storageEngine;

    @Inject
    private CursorContextFactory contextFactory;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.db_format, FormatFamily.ALIGNED.name());
    }

    @Test
    void checkValidationOfClosedReadCursors() {
        try (var cursorContext = contextFactory.create("checkValidationOfClosedReadCursors")) {
            var storageCursors = storageEngine.createStorageCursors(cursorContext);
            storageCursors.readCursor(RecordCursorTypes.NODE_CURSOR).close();
            assertThrows(IllegalStateException.class, storageCursors::close);
        }
    }

    @Test
    void cacheRequestedCursors() {
        DefaultPageCacheTracer pageCacheTracer = new DefaultPageCacheTracer();
        PageCursorTracer cursorTracer = pageCacheTracer.createPageCursorTracer("cacheRequestedCursors");
        try (var cursorContext = contextFactory.create(cursorTracer);
                var storageCursors = storageEngine.createStorageCursors(cursorContext)) {
            Object[] cursors = new Object[RecordCursorTypes.MAX_TYPE + 1];
            for (RecordCursorTypes type : RecordCursorTypes.values()) {
                cursors[type.value()] = storageCursors.readCursor(type);
            }

            for (int i = 0; i < 10; i++) {
                for (RecordCursorTypes type : RecordCursorTypes.values()) {
                    assertEquals(cursors[type.value()], storageCursors.readCursor(type));
                }
            }
        }
    }

    @Test
    void shouldOnlyAcceptRecordStorageTypes() {
        CursorType anotherType = () -> (short) 1;
        try (var cursorContext = contextFactory.create("shouldOnlyAcceptRecordStorageTypes");
                var storageCursors = storageEngine.createStorageCursors(cursorContext)) {
            assertThatThrownBy(() -> storageCursors.readCursor(anotherType))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> storageCursors.writeCursor(anotherType))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
