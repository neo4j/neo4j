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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.tags.RecordFormatOverrideTag;

@RecordFormatOverrideTag
@PageCacheExtension
@Neo4jLayoutExtension
class StoreIdTest {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    @ParameterizedTest
    @CsvSource({"standard,record-standard-1.1", "aligned,record-aligned-1.1"})
    void testRetrievalOfStoreId(String format, String expectedStoreVersion) throws IOException {
        assertNull(StoreId.retrieveFromStore(fileSystem, databaseLayout, pageCache, NULL_CONTEXT));
        var dbms = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setFileSystem(fileSystem)
                .setConfig(GraphDatabaseSettings.db_format, format)
                .setConfig(GraphDatabaseInternalSettings.include_versions_under_development, false)
                .build();
        dbms.shutdown();

        StoreId storeId = StoreId.retrieveFromStore(fileSystem, databaseLayout, pageCache, NULL_CONTEXT);
        assertNotNull(storeId);
        assertEquals(expectedStoreVersion, storeId.getStoreVersionUserString());
    }

    @ParameterizedTest
    @MethodSource("versionCheckParams")
    void testStoreVersionFullySupportedLocallyCheck(StoreId storeId, boolean supported) {
        assertEquals(supported, storeId.isStoreVersionFullySupportedLocally());
    }

    private static Stream<Arguments> versionCheckParams() {
        return Stream.of(
                arguments(new StoreId(1, 1, "some-engine", "some-format", 1, 1), false),
                arguments(new StoreId(1, 1, RecordStorageEngineFactory.NAME, "some-format", 1, 1), false),
                arguments(
                        new StoreId(
                                1,
                                1,
                                RecordStorageEngineFactory.NAME,
                                PageAligned.LATEST_RECORD_FORMATS
                                        .getFormatFamily()
                                        .name(),
                                PageAligned.LATEST_RECORD_FORMATS.majorVersion() + 10,
                                PageAligned.LATEST_RECORD_FORMATS.minorVersion()),
                        false),
                arguments(
                        new StoreId(
                                1,
                                1,
                                RecordStorageEngineFactory.NAME,
                                PageAligned.LATEST_RECORD_FORMATS
                                        .getFormatFamily()
                                        .name(),
                                PageAligned.LATEST_RECORD_FORMATS.majorVersion(),
                                PageAligned.LATEST_RECORD_FORMATS.minorVersion()),
                        true));
    }
}
