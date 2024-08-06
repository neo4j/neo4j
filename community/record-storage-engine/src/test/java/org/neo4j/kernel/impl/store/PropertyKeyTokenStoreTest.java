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

import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_KEY_TOKEN_CURSOR;

import java.nio.file.Path;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordCursorTypes;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.api.cursor.CursorType;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.cursor.StoreCursorsAdapter;

class PropertyKeyTokenStoreTest extends TokenStoreTestTemplate<PropertyKeyTokenRecord> {
    @Override
    protected PageCursor storeCursor() {
        return storeCursors.readCursor(PROPERTY_KEY_TOKEN_CURSOR);
    }

    @Override
    protected StoreCursors createCursors(TokenStore<PropertyKeyTokenRecord> store, DynamicStringStore nameStore) {
        return new PropertyKeyTokenStoreCursors(store, nameStore);
    }

    @Override
    protected TokenStore<PropertyKeyTokenRecord> instantiateStore(
            FileSystemAbstraction fileSystem,
            Path file,
            Path idFile,
            IdGeneratorFactory generatorFactory,
            PageCache pageCache,
            InternalLogProvider logProvider,
            DynamicStringStore nameStore,
            RecordFormats formats,
            Config config) {
        return new PropertyKeyTokenStore(
                fileSystem,
                file,
                idFile,
                config,
                generatorFactory,
                pageCache,
                PageCacheTracer.NULL,
                logProvider,
                nameStore,
                formats,
                false,
                DEFAULT_DATABASE_NAME,
                immutable.empty());
    }

    private static class PropertyKeyTokenStoreCursors extends StoreCursorsAdapter {
        private final TokenStore<PropertyKeyTokenRecord> store;
        private final DynamicStringStore nameStore;
        private PageCursor storeCursor;
        private PageCursor dynamicCursor;

        PropertyKeyTokenStoreCursors(TokenStore<PropertyKeyTokenRecord> store, DynamicStringStore nameStore) {
            this.store = store;
            this.nameStore = nameStore;
        }

        @Override
        public PageCursor readCursor(CursorType type) {
            return switch ((RecordCursorTypes) type) {
                case PROPERTY_KEY_TOKEN_CURSOR -> {
                    if (storeCursor == null) {
                        storeCursor = store.openPageCursorForReading(0, CursorContext.NULL_CONTEXT);
                    }
                    yield storeCursor;
                }
                case DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR -> {
                    if (dynamicCursor == null) {
                        dynamicCursor = nameStore.openPageCursorForReading(0, CursorContext.NULL_CONTEXT);
                    }
                    yield dynamicCursor;
                }
                default -> super.readCursor(type);
            };
        }

        @Override
        public PageCursor writeCursor(CursorType type) {
            return switch ((RecordCursorTypes) type) {
                case PROPERTY_KEY_TOKEN_CURSOR -> store.openPageCursorForWriting(0, CursorContext.NULL_CONTEXT);
                case DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR -> nameStore.openPageCursorForWriting(
                        0, CursorContext.NULL_CONTEXT);
                default -> super.writeCursor(type);
            };
        }

        @Override
        public void close() {
            if (dynamicCursor != null) {
                dynamicCursor.close();
                dynamicCursor = null;
            }
            if (storeCursor != null) {
                storeCursor.close();
                storeCursor = null;
            }
            super.close();
        }
    }
}
