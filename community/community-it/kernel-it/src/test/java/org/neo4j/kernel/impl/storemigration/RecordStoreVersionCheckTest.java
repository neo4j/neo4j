/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.storemigration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.EmptyVersionContextSupplier.EMPTY;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
@Neo4jLayoutExtension
class RecordStoreVersionCheckTest {
    @Inject
    protected TestDirectory testDirectory;

    @Inject
    protected FileSystemAbstraction fileSystem;

    @Inject
    protected PageCache pageCache;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    @Test
    void migrationCheckShouldFailIfMetadataStoreDoesNotExist() {
        // given
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        // when
        StoreVersionCheck.MigrationCheckResult result =
                storeVersionCheck.getAndCheckMigrationTargetVersion("version", NULL_CONTEXT);

        // then
        assertEquals(StoreVersionCheck.MigrationOutcome.STORE_VERSION_RETRIEVAL_FAILURE, result.outcome());
        assertNull(result.versionToMigrateFrom());
        assertNotNull(result.cause());
        assertThat(result.cause()).hasMessageContaining("neostore: Cannot map non-existing file");
    }

    @Test
    void upgradeCheckShouldFailIfMetadataStoreDoesNotExist() {
        // given
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        // when
        StoreVersionCheck.UpgradeCheckResult result = storeVersionCheck.getAndCheckUpgradeTargetVersion(NULL_CONTEXT);

        // then
        assertEquals(StoreVersionCheck.UpgradeOutcome.STORE_VERSION_RETRIEVAL_FAILURE, result.outcome());
        assertNull(result.versionToUpgradeFrom());
        assertNotNull(result.cause());
        assertThat(result.cause()).hasMessageContaining("neostore: Cannot map non-existing file");
    }

    @Test
    void migrationCheckShouldFailWithCorruptedMetadataStore() throws IOException {
        // given
        metaDataFileContaining(databaseLayout, fileSystem, "nothing interesting");
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        // when
        StoreVersionCheck.MigrationCheckResult result =
                storeVersionCheck.getAndCheckMigrationTargetVersion(null, NULL_CONTEXT);

        // then
        assertEquals(StoreVersionCheck.MigrationOutcome.STORE_VERSION_RETRIEVAL_FAILURE, result.outcome());
        assertNull(result.versionToMigrateFrom());
        assertNotNull(result.cause());
        assertThat(result.cause()).hasMessageContaining("Uninitialized version field in");
    }

    @Test
    void upgradeCheckShouldFailWithCorruptedMetadataStore() throws IOException {
        // given
        metaDataFileContaining(databaseLayout, fileSystem, "nothing interesting");
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        // when
        StoreVersionCheck.UpgradeCheckResult result = storeVersionCheck.getAndCheckUpgradeTargetVersion(NULL_CONTEXT);

        // then
        assertEquals(StoreVersionCheck.UpgradeOutcome.STORE_VERSION_RETRIEVAL_FAILURE, result.outcome());
        assertNull(result.versionToUpgradeFrom());
        assertNotNull(result.cause());
        assertThat(result.cause()).hasMessageContaining("Uninitialized version field in");
    }

    @Test
    void migrationCheckShouldFailWithUnknownVersion() throws IOException {
        // given
        Path neoStore = emptyFile(fileSystem);
        long v1 = StoreVersion.versionStringToLong("V1");
        MetaDataStore.setRecord(
                pageCache,
                neoStore,
                MetaDataStore.Position.STORE_VERSION,
                v1,
                databaseLayout.getDatabaseName(),
                NULL_CONTEXT);
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        // when
        StoreVersionCheck.MigrationCheckResult result =
                storeVersionCheck.getAndCheckMigrationTargetVersion(null, NULL_CONTEXT);

        // then
        assertEquals(StoreVersionCheck.MigrationOutcome.STORE_VERSION_RETRIEVAL_FAILURE, result.outcome());
        assertNull(result.versionToMigrateFrom());
        assertNotNull(result.cause());
        assertThat(result.cause()).hasMessageContaining("Unknown store version 'V1'");
    }

    @Test
    void upgradeCheckShouldFailWithUnknownVersion() throws IOException {
        // given
        Path neoStore = emptyFile(fileSystem);
        long v1 = StoreVersion.versionStringToLong("V1");
        MetaDataStore.setRecord(
                pageCache,
                neoStore,
                MetaDataStore.Position.STORE_VERSION,
                v1,
                databaseLayout.getDatabaseName(),
                NULL_CONTEXT);
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        // when
        StoreVersionCheck.UpgradeCheckResult result = storeVersionCheck.getAndCheckUpgradeTargetVersion(NULL_CONTEXT);

        // then
        assertEquals(StoreVersionCheck.UpgradeOutcome.STORE_VERSION_RETRIEVAL_FAILURE, result.outcome());
        assertNull(result.versionToUpgradeFrom());
        assertNotNull(result.cause());
        assertThat(result.cause()).hasMessageContaining("Unknown store version 'V1'");
    }

    @Test
    void tracePageCacheAccessOnMigrationCheck() throws IOException {
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        doTestTraceOnCheck(cursorContext -> {
            StoreVersionCheck.MigrationCheckResult result =
                    storeVersionCheck.getAndCheckMigrationTargetVersion(null, cursorContext);
            assertEquals(StoreVersionCheck.MigrationOutcome.NO_OP, result.outcome());
        });
    }

    @Test
    void tracePageCacheAccessOnUpgradeCheck() throws IOException {
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        doTestTraceOnCheck(cursorContext -> {
            StoreVersionCheck.UpgradeCheckResult result =
                    storeVersionCheck.getAndCheckUpgradeTargetVersion(cursorContext);
            assertEquals(StoreVersionCheck.UpgradeOutcome.NO_OP, result.outcome());
        });
    }

    private void doTestTraceOnCheck(Consumer<CursorContext> performCheck) throws IOException {
        Path neoStore = emptyFile(fileSystem);
        String storeVersion = PageAligned.LATEST_NAME;
        long v1 = StoreVersion.versionStringToLong(storeVersion);
        MetaDataStore.setRecord(
                pageCache,
                neoStore,
                MetaDataStore.Position.STORE_VERSION,
                v1,
                databaseLayout.getDatabaseName(),
                NULL_CONTEXT);

        var pageCacheTracer = new DefaultPageCacheTracer();
        CursorContextFactory contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY);
        var cursorContext = contextFactory.create("tracePageCacheAccessOnCheckUpgradable");

        performCheck.accept(cursorContext);

        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isOne();
        assertThat(cursorTracer.unpins()).isOne();
        assertThat(cursorTracer.faults()).isOne();
    }

    @Test
    void tracePageCacheAccessOnStoreVersionAccess() throws IOException {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY);
        var cursorContext = contextFactory.create("tracePageCacheAccessOnStoreVersionAccessConstruction");

        Path neoStore = emptyFile(fileSystem);
        long v1 = StoreVersion.versionStringToLong(PageAligned.LATEST_NAME);
        MetaDataStore.setRecord(
                pageCache,
                neoStore,
                MetaDataStore.Position.RANDOM_NUMBER,
                1234,
                databaseLayout.getDatabaseName(),
                NULL_CONTEXT);
        MetaDataStore.setRecord(
                pageCache, neoStore, MetaDataStore.Position.TIME, 1234, databaseLayout.getDatabaseName(), NULL_CONTEXT);
        MetaDataStore.setRecord(
                pageCache,
                neoStore,
                MetaDataStore.Position.STORE_VERSION,
                v1,
                databaseLayout.getDatabaseName(),
                NULL_CONTEXT);

        StoreId storeId = StoreId.retrieveFromStore(fileSystem, databaseLayout, pageCache, cursorContext);
        assertNotNull(storeId);
        assertEquals("record-aligned-1-1", storeId.getStoreVersionUserString());

        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isEqualTo(3);
        assertThat(cursorTracer.unpins()).isEqualTo(3);
        assertThat(cursorTracer.faults()).isEqualTo(3);
    }

    private Path emptyFile(FileSystemAbstraction fs) throws IOException {
        Path shortFile = databaseLayout.metadataStore();
        fs.deleteFile(shortFile);
        fs.write(shortFile).close();
        return shortFile;
    }

    private static void metaDataFileContaining(RecordDatabaseLayout layout, FileSystemAbstraction fs, String content)
            throws IOException {
        Path shortFile = layout.metadataStore();
        fs.deleteFile(shortFile);
        try (OutputStream outputStream = fs.openAsOutputStream(shortFile, false)) {
            outputStream.write(UTF8.encode(content));
        }
    }

    private RecordStoreVersionCheck newStoreVersionCheck() {
        return new RecordStoreVersionCheck(pageCache, databaseLayout, Config.defaults());
    }
}
