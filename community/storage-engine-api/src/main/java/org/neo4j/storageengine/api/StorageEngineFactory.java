/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.Services;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.token.TokenHolders;

/**
 * A factory suitable for something like service-loading to load {@link StorageEngine} instances.
 * Also migration logic is provided by this factory.
 */
@Service
public interface StorageEngineFactory
{
    /**
     * Returns a {@link StoreVersionCheck} which can provide both configured and existing store versions
     * and means of checking upgradability between them.
     * @return StoreVersionCheck to check store version as well as upgradability to other versions.
     */
    StoreVersionCheck versionCheck( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, LogService logService,
            PageCacheTracer pageCacheTracer );

    StoreVersion versionInformation( String storeVersion );

    /**
     * Returns a {@link StoreMigrationParticipant} which will be able to participate in a store migration.
     * @return StoreMigrationParticipant for migration.
     */
    List<StoreMigrationParticipant> migrationParticipants( FileSystemAbstraction fs, Config config, PageCache pageCache,
            JobScheduler jobScheduler, LogService logService, PageCacheTracer cacheTracer, MemoryTracker memoryTracker );

    /**
     * Instantiates a {@link StorageEngine} where all dependencies can be retrieved from the supplied {@code dependencyResolver}.
     *
     * @return the instantiated {@link StorageEngine}.
     */
    StorageEngine instantiate( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, TokenHolders tokenHolders,
            SchemaState schemaState, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter, LockService lockService,
            IdGeneratorFactory idGeneratorFactory, IdController idController, DatabaseHealth databaseHealth,
            LogProvider logProvider, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, PageCacheTracer cacheTracer, boolean createStoreIfNotExists,
            MemoryTracker memoryTracker );

    /**
     * Lists files of a specific storage location.
     * @param fileSystem {@link FileSystemAbstraction} this storage is on.
     * @param databaseLayout {@link DatabaseLayout} pointing out its location.
     * @return a {@link List} of {@link Path} instances for the storage files.
     * @throws IOException if there was no storage in this location.
     */
    List<Path> listStorageFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException;

    /**
     * Check if a store described by provided database layout exists in provided file system
     * @param fileSystem store file system
     * @param databaseLayout store database layout
     * @param pageCache page cache to open store with
     * @return true of store exist, false otherwise
     */
    boolean storageExists( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache );

    /**
     * Instantiates a read-only {@link TransactionIdStore} to be used outside of a {@link StorageEngine}.
     * @return the read-only {@link TransactionIdStore}.
     * @throws IOException on I/O error or if the store doesn't exist.
     */
    TransactionIdStore readOnlyTransactionIdStore( FileSystemAbstraction filySystem, DatabaseLayout databaseLayout,
            PageCache pageCache, PageCursorTracer pageCursorTracer ) throws IOException;

    /**
     * Instantiates a read-only {@link LogVersionRepository} to be used outside of a {@link StorageEngine}.
     * @return the read-only {@link LogVersionRepository}.
     * @throws IOException on I/O error or if the store doesn't exist.
     */
    LogVersionRepository readOnlyLogVersionRepository( DatabaseLayout databaseLayout, PageCache pageCache, PageCursorTracer cursorTracer ) throws IOException;

    /**
     * Instantiates a fully functional {@link MetadataProvider}, which is a union of {@link TransactionIdStore}
     * and {@link LogVersionRepository}.
     * @return a fully functional {@link MetadataProvider}.
     * @throws IOException on I/O error or if the store doesn't exist.
     */
    MetadataProvider transactionMetaDataStore( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache,
            PageCacheTracer cacheTracer ) throws IOException;

    StoreId storeId( DatabaseLayout databaseLayout, PageCache pageCache, PageCursorTracer cursorTracer ) throws IOException;

    SchemaRuleMigrationAccess schemaRuleMigrationAccess( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout databaseLayout,
            LogService logService, String recordFormats, PageCacheTracer cacheTracer, PageCursorTracer cursorTracer, MemoryTracker memoryTracker );

    /**
     * Asks this storage engine about the state of a specific store before opening it. If this specific store is missing optional or
     * even perhaps mandatory files in order to properly open it, this is the place to report that.
     *
     * @param fs {@link FileSystemAbstraction} to use for file operations.
     * @param databaseLayout {@link DatabaseLayout} for the location of the database in the file system.
     * @param pageCache {@link PageCache} for any data reading needs.
     * @return the state of the storage files.
     */
    StorageFilesState checkRecoveryRequired( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache );

    /**
     * @return a {@link CommandReaderFactory} capable of handing out {@link CommandReader} for specific versions. Generally kernel will take care
     * of most of the log entry parsing, i.e. the START, COMMIT, CHECKPOINT commands and their contents (they may be versioned). For COMMAND log entries
     * this returned factory will be used to parse the actual command contents, which are storage-specific. For maximum flexibility the structure should
     * be something like this:
     * <ol>
     *     <li>1B log entry version - managed by kernel</li>
     *     <li>1B log entry type - managed by kernel</li>
     *     <li>For COMMAND log entries: 1B command version - managed by storage</li>
     *     <li>For COMMAND log entries: 1B command type - managed by storage</li>
     *     <li>For COMMAND log entries: command data... - managed by storage</li>
     * </ol>
     *
     * Although currently it's more like this:
     *
     * <ol>
     *     <li>1B log entry version - dictating both log entry version AND command version</li>
     *     <li>1B log entry type - managed by kernel</li>
     *     <li>For COMMAND log entries: command data... - managed by storage, although versioned the same as log entry version</li>
     * </ol>
     */
    CommandReaderFactory commandReaderFactory();

    /**
     * Selects a {@link StorageEngineFactory} among the candidates. How it's done or which it selects isn't important a.t.m.
     * @return the selected {@link StorageEngineFactory}.
     * @throws IllegalStateException if there were no candidates.
     */
    static StorageEngineFactory selectStorageEngine()
    {
        return Iterables.single( Services.loadAll( StorageEngineFactory.class ) );
    }
}
