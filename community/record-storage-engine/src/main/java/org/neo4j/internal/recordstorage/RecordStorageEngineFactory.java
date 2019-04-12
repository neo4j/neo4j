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
package org.neo4j.internal.recordstorage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.IndexProviderMigrator;
import org.neo4j.kernel.impl.storemigration.RecordStorageMigrator;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersion;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersionCheck;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.TransactionMetaDataStore;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.token.TokenHolders;

import static org.neo4j.kernel.impl.store.StoreType.META_DATA;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;

@ServiceProvider
public class RecordStorageEngineFactory implements StorageEngineFactory
{
    @Override
    public StoreVersionCheck versionCheck( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache,
            LogService logService )
    {
        return new RecordStoreVersionCheck( fs, pageCache, databaseLayout, logService.getInternalLogProvider(), config );
    }

    @Override
    public StoreVersion versionInformation( String storeVersion )
    {
        return new RecordStoreVersion( RecordFormatSelector.selectForVersion( storeVersion ) );
    }

    @Override
    public List<StoreMigrationParticipant> migrationParticipants( FileSystemAbstraction fs, Config config, PageCache pageCache,
            JobScheduler jobScheduler, LogService logService )
    {
        RecordStorageMigrator recordStorageMigrator = new RecordStorageMigrator( fs, pageCache, config, logService, jobScheduler );
        IndexProviderMigrator indexProviderMigrator = new IndexProviderMigrator( fs, config, pageCache, logService );
        // Make sure that we migrate the store before we update the schema store with index providers.
        return Arrays.asList( recordStorageMigrator, indexProviderMigrator );
    }

    @Override
    public StorageEngine instantiate( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, TokenHolders tokenHolders,
            SchemaState schemaState, ConstraintRuleAccessor constraintSemantics, LockService lockService, IdGeneratorFactory idGeneratorFactory,
            IdController idController, DatabaseHealth databaseHealth, VersionContextSupplier versionContextSupplier, LogProvider logProvider )
    {
        return new RecordStorageEngine( databaseLayout, config, pageCache, fs, logProvider,
                tokenHolders, schemaState, constraintSemantics, lockService, databaseHealth, idGeneratorFactory, idController, versionContextSupplier );
    }

    @Override
    public ReadableStorageEngine instantiateReadable( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache,
            LogProvider logProvider )
    {
        return new ReadableRecordStorageEngine( databaseLayout, config, pageCache, fs, logProvider );
    }

    @Override
    public List<File> listStorageFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException
    {
        if ( !fileSystem.fileExists( databaseLayout.file( META_DATA.getDatabaseFile() ).findFirst().get() ) )
        {
            throw new IOException( "No storage present at " + databaseLayout + " on " + fileSystem );
        }

        List<File> files = new ArrayList<>();
        for ( StoreType type : StoreType.values() )
        {
            databaseLayout.file( type.getDatabaseFile() ).filter( fileSystem::fileExists ).forEach( files::add );
        }
        return files;
    }

    @Override
    public boolean storageExists( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache )
    {
        return NeoStores.isStorePresent( pageCache, databaseLayout );
    }

    @Override
    public TransactionIdStore readOnlyTransactionIdStore( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        return new ReadOnlyTransactionIdStore( pageCache, databaseLayout );
    }

    @Override
    public LogVersionRepository readOnlyLogVersionRepository( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        return new ReadOnlyLogVersionRepository( pageCache, databaseLayout );
    }

    @Override
    public TransactionMetaDataStore transactionMetaDataStore( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache )
    {
        RecordFormats recordFormats = selectForStoreOrConfig( Config.defaults(), databaseLayout, fs, pageCache, NullLogProvider.getInstance() );
        return new StoreFactory( databaseLayout, config, new DefaultIdGeneratorFactory( fs ), pageCache, fs, recordFormats, NullLogProvider.getInstance() )
                .openNeoStores( META_DATA ).getMetaDataStore();
    }

    @Override
    public StoreId storeId( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        File neoStoreFile = databaseLayout.metadataStore();
        return MetaDataStore.getStoreId( pageCache, neoStoreFile );
    }
}
