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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseConfig;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.transaction.log.LogVersionUpgradeChecker;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_corrupted_log_files;

public class DatabaseMigratorFactory
{
    private final FileSystemAbstraction fs;
    private final Config config;
    private final LogService logService;
    private final PageCache pageCache;
    private final JobScheduler jobScheduler;
    private final DatabaseId databaseId;

    public DatabaseMigratorFactory( FileSystemAbstraction fs, Config config, LogService logService, PageCache pageCache, JobScheduler jobScheduler,
            DatabaseId databaseId )
    {
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.pageCache = pageCache;
        this.jobScheduler = jobScheduler;
        this.databaseId = databaseId;
    }

    public DatabaseMigrator createDatabaseMigrator( DatabaseLayout databaseLayout, StorageEngineFactory storageEngineFactory,
            DependencyResolver dependencyResolver )
    {
        final DatabaseConfig dbConfig = new DatabaseConfig( config, databaseId );
        final IndexProviderMap indexProviderMap = dependencyResolver.resolveDependency( IndexProviderMap.class );
        final Monitors monitors = dependencyResolver.resolveDependency( Monitors.class );
        final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        final LegacyTransactionLogsLocator logsLocator = new LegacyTransactionLogsLocator( dbConfig, databaseLayout );
        final LogFiles logFiles;
        try
        {
            // we should not use provided database layout here since transaction log location is different compare to previous versions
            // and that's why we need to use custom transaction logs locator and database layout
            DatabaseLayout oldDatabaseLayout = new LegacyDatabaseLayout( databaseLayout.getStoreLayout(), databaseLayout.getDatabaseName(),
                    logsLocator );
            logFiles = LogFilesBuilder.builder( oldDatabaseLayout, fs )
                .withLogEntryReader( logEntryReader )
                .withConfig( dbConfig )
                .withDependencies( dependencyResolver ).build();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        final LogTailScanner tailScanner = new LogTailScanner( logFiles, logEntryReader, monitors, dbConfig.get( fail_on_corrupted_log_files ) );
        LogVersionUpgradeChecker.check( tailScanner, dbConfig );
        return new DatabaseMigrator( fs, dbConfig, logService, indexProviderMap, pageCache, tailScanner, jobScheduler, databaseLayout, logsLocator,
                storageEngineFactory );
    }

    private static class LegacyDatabaseLayout extends DatabaseLayout
    {
        private final LegacyTransactionLogsLocator logsLocator;

        LegacyDatabaseLayout( StoreLayout storeLayout, String databaseName, LegacyTransactionLogsLocator logsLocator )
        {
            super( storeLayout, databaseName );
            this.logsLocator = logsLocator;
        }

        @Override
        public File getTransactionLogsDirectory()
        {
            return logsLocator.getTransactionLogsDirectory();
        }
    }
}
