/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

package org.neo4j.graphdb.internal;

import java.io.IOException;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.storemigration.DatabaseMigratorImpl;
import org.neo4j.kernel.impl.transaction.log.LogVersionUpgradeChecker;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFileCreationMonitor;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.migration.DatabaseMigrator;

public class DatabaseMigratorFactoryImpl implements org.neo4j.storageengine.migration.DatabaseMigratorFactory
{
    private final FileSystemAbstraction fs;
    private final Config config;
    private final LogService logService;
    private final PageCache pageCache;
    private final JobScheduler jobScheduler;

    public DatabaseMigratorFactoryImpl( FileSystemAbstraction fs, Config config, LogService logService, PageCache pageCache, JobScheduler jobScheduler )
    {
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.pageCache = pageCache;
        this.jobScheduler = jobScheduler;
    }

    @Override
    public DatabaseMigrator createDatabaseMigrator( DatabaseLayout databaseLayout, DependencyResolver dependencyResolver )
    {
        final IndexProviderMap indexProviderMap = dependencyResolver.resolveDependency( IndexProviderMap.class );
        final Monitors monitors = dependencyResolver.resolveDependency( Monitors.class );
        final LogFileCreationMonitor logFileCreationMonitor = monitors.newMonitor( LogFileCreationMonitor.class );
        final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        final LogFiles logFiles;
        try
        {
            logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withLogEntryReader( logEntryReader )
                .withLogFileMonitor( logFileCreationMonitor )
                .withConfig( config )
                .withDependencies( dependencyResolver ).build();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        final LogTailScanner tailScanner = new LogTailScanner(
            logFiles, logEntryReader, monitors, config.get( GraphDatabaseSettings.fail_on_corrupted_log_files ) );
        LogVersionUpgradeChecker.check( tailScanner, config );
        return new DatabaseMigratorImpl(
            fs, config, logService, indexProviderMap, pageCache, tailScanner, jobScheduler, databaseLayout );
    }
}
