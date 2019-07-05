/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
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
    StoreVersionCheck versionCheck( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, LogService logService );

    StoreVersion versionInformation( String storeVersion );

    /**
     * Returns a {@link StoreMigrationParticipant} which will be able to participate in a store migration.
     * @return StoreMigrationParticipant for migration.
     */
    List<StoreMigrationParticipant> migrationParticipants( FileSystemAbstraction fs, Config config, PageCache pageCache,
            JobScheduler jobScheduler, LogService logService );

    /**
     * Instantiates a {@link StorageEngine} where all dependencies can be retrieved from the supplied {@code dependencyResolver}.
     *
     * @return the instantiated {@link StorageEngine}.
     */
    StorageEngine instantiate( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, TokenHolders tokenHolders,
            SchemaState schemaState, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter, LockService lockService,
            IdGeneratorFactory idGeneratorFactory, IdController idController, DatabaseHealth databaseHealth, VersionContextSupplier versionContextSupplier,
            LogProvider logProvider, boolean createStoreIfNotExists );

    /**
     * Lists files of a specific storage location.
     * @param fileSystem {@link FileSystemAbstraction} this storage is on.
     * @param databaseLayout {@link DatabaseLayout} pointing out its location.
     * @return a {@link Stream} of {@link File} instances for the storage files.
     * @throws IOException if there was no storage in this location.
     */
    List<File> listStorageFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException;

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
    TransactionIdStore readOnlyTransactionIdStore( FileSystemAbstraction filySystem, DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException;

    /**
     * Instantiates a read-only {@link LogVersionRepository} to be used outside of a {@link StorageEngine}.
     * @return the read-only {@link LogVersionRepository}.
     * @throws IOException on I/O error or if the store doesn't exist.
     */
    LogVersionRepository readOnlyLogVersionRepository( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException;

    /**
     * Instantiates a fully functional {@link TransactionMetaDataStore}, which is a union of {@link TransactionIdStore}
     * and {@link LogVersionRepository}.
     * @return a fully functional {@link TransactionMetaDataStore}.
     * @throws IOException on I/O error or if the store doesn't exist.
     */
    TransactionMetaDataStore transactionMetaDataStore( FileSystemAbstraction fs, DatabaseLayout databaseLayout,
            Config config, PageCache pageCache ) throws IOException;

    StoreId storeId( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException;

    SchemaRuleMigrationAccess schemaRuleMigrationAccess( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout databaseLayout,
            LogService logService, String recordFormats );

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
