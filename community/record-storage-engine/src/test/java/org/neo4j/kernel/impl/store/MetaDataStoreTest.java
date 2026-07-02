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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.test.utils.PageCacheConfig.config;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.DelegatingPageCursor;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.DatabaseCreationOptions;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.StoreFileClosedException;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;

@EphemeralNeo4jLayoutExtension
public class MetaDataStoreTest {
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension =
            new PageCacheSupportExtension(config().withInconsistentReads(new AtomicBoolean()));

    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private PageCache pageCache;
    private boolean fakePageCursorOverflow;
    private PageCache pageCacheWithFakeOverflow;
    private final CursorContextFactory contextFactory =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    public static Stream<Arguments> recordFormats() {
        return Stream.of(Arguments.of(PageAligned.LATEST_RECORD_FORMATS));
    }

    @BeforeEach
    void setUp() {
        pageCache = pageCacheExtension.getPageCache(fs);
        fakePageCursorOverflow = false;
        pageCacheWithFakeOverflow = new DelegatingPageCache(pageCache) {
            @Override
            public PagedFile map(
                    StoreFile storeFile,
                    int pageSize,
                    String databaseName,
                    ImmutableSet<OpenOption> openOptions,
                    IOController ioController)
                    throws IOException {
                return new DelegatingPagedFile(
                        super.map(storeFile, pageSize, databaseName, openOptions, ioController)) {
                    @Override
                    public PageCursor io(long pageId, int pf_flags, CursorContext context) throws IOException {
                        return new DelegatingPageCursor(super.io(pageId, pf_flags, context)) {
                            @Override
                            public boolean checkAndClearBoundsFlag() {
                                return fakePageCursorOverflow | super.checkAndClearBoundsFlag();
                            }
                        };
                    }
                };
            }
        };
    }

    @AfterEach
    void tearDown() {
        pageCache.close();
    }

    @Test
    void getStoreIdShouldFailWhenStoreIsClosed() {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        assertThrows(StoreFileClosedException.class, metaDataStore::getStoreId);
    }

    @Test
    void generateExternalStoreUUIDOnCreation() {
        try (MetaDataStore metaDataStore = newMetaDataStore()) {
            var externalStoreId = metaDataStore.getExternalStoreId();
            var externalUUID = externalStoreId.id();
            assertThat(externalUUID.getLeastSignificantBits()).isNotZero();
            assertThat(externalUUID.getMostSignificantBits()).isNotZero();
        }
    }

    @Test
    void logRecordsMustIgnorePageOverflow() {
        try (MetaDataStore store = newMetaDataStore()) {
            fakePageCursorOverflow = true;
            store.logRecords(s -> {});
            fakePageCursorOverflow = false;
        }
    }

    @ParameterizedTest
    @MethodSource("recordFormats")
    void canReadStoreVersionWithDifferentEndiannessFormats(RecordFormats recordFormats) throws IOException {
        try (var metaDataStore = newMetaDataStore(recordFormats)) {
            var access = MetaDataStore.getFieldAccess(
                    pageCache, databaseLayout.metadataStore(), databaseLayout.getDatabaseName(), NULL_CONTEXT);
            var storeId = access.readStoreId();
            assertThat(storeId.getFormatName())
                    .isEqualTo(recordFormats.getFormatFamily().name());
            assertThat(storeId.getMajorVersion()).isEqualTo(recordFormats.majorVersion());
            assertThat(storeId.getMinorVersion()).isEqualTo(recordFormats.minorVersion());
        }
    }

    @Test
    void regenerateSetStoreId() {
        // given
        var format = defaultFormat();
        StoreId storeId = StoreId.generateNew(
                RecordStorageEngineFactory.NAME,
                format.getFormatFamily().name(),
                format.majorVersion(),
                format.minorVersion());

        UUID externalStoreUUID = UUID.randomUUID();
        try (MetaDataStore store = newMetaDataStore()) {
            store.regenerateMetadata(storeId, externalStoreUUID, NULL_CONTEXT);
        }

        // then
        try (MetaDataStore store = newMetaDataStore()) {
            assertEquals(storeId, store.getStoreId());
        }
    }

    @Test
    void regenerateSetExternalStoreId() {
        UUID externalStoreId = UUID.randomUUID();
        try (MetaDataStore store = newMetaDataStore()) {
            store.regenerateMetadata(store.getStoreId(), externalStoreId, NULL_CONTEXT);
        }

        try (MetaDataStore store = newMetaDataStore()) {
            var retrievedExternalStoreId = store.getExternalStoreId();
            assertEquals(externalStoreId, retrievedExternalStoreId.id());
        }
    }

    @Test
    void tracePageCacheAccessOnStoreInitialisation() {
        var pageCacheTracer = new DefaultPageCacheTracer();
        CursorContextFactory contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (MetaDataStore ignored = newMetaDataStore(contextFactory)) {
            assertThat(pageCacheTracer.faults()).isOne();
            assertThat(pageCacheTracer.pins()).isEqualTo(2);
            assertThat(pageCacheTracer.unpins()).isEqualTo(2);
            assertThat(pageCacheTracer.hits()).isEqualTo(1);
        }
    }

    @Test
    void tracePageCacheAccessOnSetRecord() throws IOException {
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        var cursorContext = contextFactory.create("tracePageCacheAccessOnSetRecord");
        try (var metaDataStore = newMetaDataStore()) {
            var fieldAccess = MetaDataStore.getFieldAccess(
                    pageCache, databaseLayout.metadataStore(), databaseLayout.getDatabaseName(), cursorContext);
            fieldAccess.writeStoreId(StoreId.generateNew("engine-1", "format-1", 1, 1));

            PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.pins()).isOne();
            assertThat(cursorTracer.unpins()).isOne();
            assertThat(cursorTracer.hits()).isOne();
        }
    }

    @Test
    void tracePageCacheAccessOnGetRecord() throws IOException {
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        var cursorContext = contextFactory.create("tracePageCacheAccessOnGetRecord");
        try (var metaDataStore = newMetaDataStore()) {
            var fieldAccess = MetaDataStore.getFieldAccess(
                    pageCache, databaseLayout.metadataStore(), databaseLayout.getDatabaseName(), cursorContext);
            fieldAccess.readStoreId();

            PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.pins()).isOne();
            assertThat(cursorTracer.unpins()).isOne();
            assertThat(cursorTracer.hits()).isOne();
        }
    }

    @Test
    void tracePageCacheAssessOnRegenerate() {
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        var cursorContext = contextFactory.create("tracePageCacheAssessOnSetStoreId");
        try (var metaDataStore = newMetaDataStore()) {
            var storeId = StoreId.generateNew("engine-1", "format-1", 1, 1);
            metaDataStore.regenerateMetadata(
                    storeId, metaDataStore.getExternalStoreId().id(), cursorContext);
            PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.pins()).isEqualTo(2);
            assertThat(cursorTracer.unpins()).isEqualTo(2);
            assertThat(cursorTracer.hits()).isEqualTo(2);
        }
    }

    @Test
    void shouldBeAbleToReadAndWriteDatabaseIdUuid() {
        // given
        var databaseIdUuid = UUID.randomUUID();

        // when
        try (MetaDataStore store = newMetaDataStore()) {
            store.setDatabaseIdUuid(databaseIdUuid, NULL_CONTEXT);
        }

        // then
        try (MetaDataStore store = newMetaDataStore()) {
            var storedDatabaseId = store.getDatabaseIdUuid(NULL_CONTEXT);
            assertThat(storedDatabaseId).hasValue(databaseIdUuid);
        }
    }

    @Test
    void shouldReturnEmptyIfDatabaseIdHasNeverBeenSet() {
        // given
        try (MetaDataStore store = newMetaDataStore()) {
            // when
            var storeDatabaseId = store.getDatabaseIdUuid(NULL_CONTEXT);

            // then
            assertThat(storeDatabaseId).isEmpty();
        }
    }

    @Test
    void shouldLoadAllFieldsOnOpen() {
        // given
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (var store = newMetaDataStore(contextFactory)) {
            var pinsBefore = pageCacheTracer.pins();
            readAllFields(store);
            var pinsAfter = pageCacheTracer.pins();
            assertThat(pinsAfter).isEqualTo(pinsBefore);
        }
    }

    private void readAllFields(MetaDataStore store) {
        store.getStoreId();
        store.getExternalStoreId();
        // getDatabaseIdUuid actually reads from store, but must not refresh fields
        store.getDatabaseIdUuid(NULL_CONTEXT);
    }

    private MetaDataStore newMetaDataStore() {
        return newMetaDataStore(contextFactory);
    }

    private MetaDataStore newMetaDataStore(CursorContextFactory contextFactory) {
        InternalLogProvider logProvider = NullLogProvider.getInstance();
        PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;
        StoreFactory storeFactory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCacheWithFakeOverflow,
                pageCacheTracer,
                fs,
                logProvider,
                contextFactory,
                false,
                DatabaseCreationOptions.EMPTY_CREATION_OPTIONS);
        return storeFactory.openNeoStores(StoreType.META_DATA).getMetaDataStore();
    }

    private MetaDataStore newMetaDataStore(RecordFormats recordFormats) {
        InternalLogProvider logProvider = NullLogProvider.getInstance();
        PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;
        StoreFactory storeFactory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCacheWithFakeOverflow,
                pageCacheTracer,
                fs,
                recordFormats,
                logProvider,
                contextFactory,
                false,
                DatabaseCreationOptions.EMPTY_CREATION_OPTIONS);
        return storeFactory.openNeoStores(StoreType.META_DATA).getMetaDataStore();
    }
}
