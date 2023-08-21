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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.store.StoreType.STORE_TYPES;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatPropertyConfigurator;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.DisabledForRoot;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

@PageCacheExtension
@Neo4jLayoutExtension
class NeoStoreOpenFailureTest {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    @Test
    @DisabledForRoot
    void mustCloseAllStoresIfNeoStoresFailToOpen() {
        var pageCacheTracer = NULL;
        Config config = Config.defaults();
        IdGeneratorFactory idGenFactory = new DefaultIdGeneratorFactory(
                fileSystem, immediate(), pageCacheTracer, databaseLayout.getDatabaseName());
        InternalLogProvider logProvider = NullLogProvider.getInstance();
        RecordFormats formats = defaultFormat();
        RecordFormatPropertyConfigurator.configureRecordFormat(formats, config);
        StoreType[] storeTypes = StoreType.STORE_TYPES;
        ImmutableSet<OpenOption> openOptions = immutable.empty();
        CursorContextFactory contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        LogTailLogVersionsMetadata logTail = LogTailLogVersionsMetadata.EMPTY_LOG_TAIL;
        NeoStores neoStores = new NeoStores(
                fileSystem,
                databaseLayout,
                config,
                idGenFactory,
                pageCache,
                pageCacheTracer,
                logProvider,
                formats,
                contextFactory,
                false,
                logTail,
                STORE_TYPES,
                openOptions);
        Path schemaStore = neoStores.getSchemaStore().getStorageFile();
        neoStores.close();

        // Make the schema store inaccessible, to sabotage the next initialisation we'll do.
        assumeTrue(schemaStore.toFile().setReadable(false));
        assumeTrue(schemaStore.toFile().setWritable(false));

        assertThrows(
                RuntimeException.class,
                () ->
                        // This should fail due to the permissions we changed above.
                        // And when it fails, the already-opened stores should be closed.
                        new NeoStores(
                                fileSystem,
                                databaseLayout,
                                config,
                                idGenFactory,
                                pageCache,
                                pageCacheTracer,
                                logProvider,
                                formats,
                                contextFactory,
                                false,
                                logTail,
                                STORE_TYPES,
                                openOptions));

        // We verify that the successfully opened stores were closed again by the failed NeoStores open,
        // by closing the page cache, which will throw if not all files have been unmapped.
        pageCache.close();
    }
}
