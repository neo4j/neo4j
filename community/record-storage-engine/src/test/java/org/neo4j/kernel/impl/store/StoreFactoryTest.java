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

import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfigForNewDbs;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class StoreFactoryTest {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    private NeoStores neoStores;
    private IdGeneratorFactory idGeneratorFactory;

    @BeforeEach
    void setUp() {
        idGeneratorFactory = new DefaultIdGeneratorFactory(
                fileSystem, immediate(), PageCacheTracer.NULL, databaseLayout.getDatabaseName());
    }

    private StoreFactory storeFactory(Config config) {
        return storeFactory(config, new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER));
    }

    private StoreFactory storeFactory(Config config, CursorContextFactory contextFactory) {
        return storeFactory(config, contextFactory, immutable.empty(), false);
    }

    private StoreFactory storeFactory(
            Config config,
            CursorContextFactory contextFactory,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly) {
        InternalLogProvider logProvider = NullLogProvider.getInstance();
        RecordFormats recordFormats = selectForStoreOrConfigForNewDbs(
                config, databaseLayout, fileSystem, pageCache, logProvider, contextFactory);
        return new StoreFactory(
                databaseLayout,
                config,
                idGeneratorFactory,
                pageCache,
                PageCacheTracer.NULL,
                fileSystem,
                recordFormats,
                logProvider,
                contextFactory,
                readOnly,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                openOptions);
    }

    @AfterEach
    void tearDown() {
        if (neoStores != null) {
            neoStores.close();
        }
    }

    @Test
    void tracePageCacheAccessOnOpenStores() {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        neoStores = storeFactory(defaults(), contextFactory).openAllNeoStores();

        assertThat(pageCacheTracer.pins()).isNotZero();
        assertThat(pageCacheTracer.unpins()).isNotZero();
    }

    @Test
    void shouldThrowWhenOpeningNonExistingNeoStores() {
        assertThrows(StoreNotFoundException.class, () -> {
            try (NeoStores neoStores = storeFactory(defaults(), NULL_CONTEXT_FACTORY, immutable.empty(), true)
                    .openAllNeoStores()) {
                neoStores.getMetaDataStore();
            }
        });
    }

    @Test
    void shouldDelegateDeletionOptionToStores() throws IOException {
        // GIVEN
        StoreFactory storeFactory = storeFactory(
                defaults(),
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                immutable.of(DELETE_ON_CLOSE),
                false);

        // WHEN
        neoStores = storeFactory.openAllNeoStores();
        assertTrue(fileSystem.listFiles(databaseLayout.databaseDirectory()).length >= StoreType.STORE_TYPES.length);

        // THEN
        neoStores.close();
        assertEquals(0, fileSystem.listFiles(databaseLayout.databaseDirectory()).length);
    }

    @Test
    void shouldHandleStoreConsistingOfOneEmptyFile() throws Exception {
        StoreFactory storeFactory = storeFactory(defaults());
        fileSystem.write(databaseLayout.file("neostore.nodestore.db.labels"));
        storeFactory.openAllNeoStores().close();
    }

    @Test
    void shouldCompleteInitializationOfStoresWithIncompleteHeaders() throws Exception {
        StoreFactory storeFactory = storeFactory(defaults());
        storeFactory.openAllNeoStores().close();
        for (Path f : fileSystem.listFiles(databaseLayout.databaseDirectory())) {
            if (!f.getFileName().toString().endsWith(".id") && !f.equals(databaseLayout.metadataStore())) {
                fileSystem.truncate(f, 0);
            }
        }
        storeFactory = storeFactory(defaults());
        storeFactory.openAllNeoStores().close();
    }
}
