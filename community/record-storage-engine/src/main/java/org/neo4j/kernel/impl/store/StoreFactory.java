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
import static org.neo4j.io.pagecache.PageCacheOpenOptions.DIRECT;
import static org.neo4j.kernel.impl.store.format.RecordFormatPropertyConfigurator.configureRecordFormat;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfigForNewDbs;

import java.io.IOException;
import java.nio.file.OpenOption;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.PageCacheOptionsSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.InternalLogProvider;

/**
 * Factory for Store implementations. Can also be used to create empty stores.
 */
public class StoreFactory {
    private final RecordDatabaseLayout databaseLayout;
    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final InternalLogProvider logProvider;
    private final PageCache pageCache;
    private final PageCacheTracer pageCacheTracer;
    private final RecordFormats recordFormats;
    private final CursorContextFactory contextFactory;
    private final boolean readOnly;
    private final LogTailLogVersionsMetadata logTailMetadata;
    private final ImmutableSet<OpenOption> openOptions;

    public StoreFactory(
            DatabaseLayout directoryStructure,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            FileSystemAbstraction fileSystemAbstraction,
            InternalLogProvider logProvider,
            CursorContextFactory contextFactory,
            boolean readOnly,
            LogTailLogVersionsMetadata logTailMetadata) {
        this(
                directoryStructure,
                config,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                fileSystemAbstraction,
                selectForStoreOrConfigForNewDbs(
                        config,
                        RecordDatabaseLayout.convert(directoryStructure),
                        fileSystemAbstraction,
                        pageCache,
                        logProvider,
                        contextFactory),
                logProvider,
                contextFactory,
                readOnly,
                logTailMetadata,
                immutable.empty());
    }

    public StoreFactory(
            DatabaseLayout databaseLayout,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            FileSystemAbstraction fileSystemAbstraction,
            RecordFormats recordFormats,
            InternalLogProvider logProvider,
            CursorContextFactory contextFactory,
            boolean readOnly,
            LogTailLogVersionsMetadata logTailMetadata,
            ImmutableSet<OpenOption> openOptions) {
        this.databaseLayout = RecordDatabaseLayout.convert(databaseLayout);
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.recordFormats = recordFormats;
        this.contextFactory = contextFactory;
        this.readOnly = readOnly;
        this.logTailMetadata = logTailMetadata;
        this.openOptions = buildOpenOptions(config, recordFormats, openOptions);
        this.logProvider = logProvider;
        this.pageCache = pageCache;
        this.pageCacheTracer = pageCacheTracer;
        configureRecordFormat(recordFormats, config);
    }

    /**
     * Open {@link NeoStores} with all possible stores. If some store does not exist it will <b>not</b> be created.
     * @return container with all opened stores
     */
    public NeoStores openAllNeoStores() {
        return openNeoStores(StoreType.STORE_TYPES);
    }

    /**
     * Open {@link NeoStores} for requested and store types. If requested store depend from non request store,
     * it will be automatically opened as well.
     * @param storeTypes - types of stores to be opened.
     * @return container with opened stores
     */
    public NeoStores openNeoStores(StoreType... storeTypes) {
        if (!readOnly) {
            try {
                fileSystemAbstraction.mkdirs(databaseLayout.databaseDirectory());
            } catch (IOException e) {
                throw new UnderlyingStorageException(
                        "Could not create database directory: " + databaseLayout.databaseDirectory(), e);
            }
        }
        return new NeoStores(
                fileSystemAbstraction,
                databaseLayout,
                config,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                logProvider,
                recordFormats,
                contextFactory,
                readOnly,
                logTailMetadata,
                storeTypes,
                openOptions);
    }

    private static ImmutableSet<OpenOption> buildOpenOptions(
            Config config, RecordFormats recordFormats, ImmutableSet<OpenOption> openOptions) {
        openOptions = openOptions.newWithAll(PageCacheOptionsSelector.select(recordFormats));

        // we need to modify options only for aligned format and avoid passing direct io option in all other cases
        if (!recordFormats.getFormatFamily().equals(FormatFamily.ALIGNED)) {
            return openOptions;
        }
        if (!config.get(GraphDatabaseSettings.pagecache_direct_io)) {
            return openOptions;
        }
        if (!UnsafeUtil.unsafeByteBufferAccessAvailable()) {
            return openOptions;
        }
        if (openOptions.contains(DIRECT)) {
            return openOptions;
        }
        return openOptions.newWith(DIRECT);
    }
}
