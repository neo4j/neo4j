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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.LogVersionUpgradeChecker;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionMetaDataStore;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;

import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_corrupted_log_files;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;
import static org.neo4j.io.fs.FileSystemAbstraction.EMPTY_COPY_OPTIONS;

public class LogsUpgrader
{
    private final FileSystemAbstraction fs;
    private final StorageEngineFactory storageEngineFactory;
    private final DatabaseLayout databaseLayout;
    private final PageCache pageCache;
    private final LegacyTransactionLogsLocator legacyLogsLocator;
    private final Config config;
    private final DependencyResolver dependencyResolver;

    public LogsUpgrader( FileSystemAbstraction fs, StorageEngineFactory storageEngineFactory, DatabaseLayout databaseLayout, PageCache pageCache,
                         LegacyTransactionLogsLocator legacyLogsLocator, Config config, DependencyResolver dependencyResolver )
    {
        this.fs = fs;
        this.storageEngineFactory = storageEngineFactory;
        this.databaseLayout = databaseLayout;
        this.pageCache = pageCache;
        this.legacyLogsLocator = legacyLogsLocator;
        this.config = config;
        this.dependencyResolver = dependencyResolver;
    }

    public void assertCleanlyShutDownByCheckPoint( DatabaseLayout layout )
    {
        Throwable suppressibleException = null;
        try
        {
            // we should not use provided database layout here since transaction log location is different compare to previous versions
            // and that's why we need to use custom transaction logs locator and database layout
            DatabaseLayout oldDatabaseLayout = buildLegacyLogsLayout( layout );
            LogTailScanner logTailScanner = buildLogTailScanner( oldDatabaseLayout );
            LogVersionUpgradeChecker.check( logTailScanner, config );

            LogTailScanner.LogTailInformation tail = logTailScanner.getTailInformation();
            if ( !tail.isRecoveryRequired() )
            {
                // All good
                return;
            }
            if ( tail.logsMissing() )
            {
                // There are no log files in the legacy logs location.
                // Either log files are missing entirely, or they are already in their correct place.
                logTailScanner = buildLogTailScanner( layout );
                LogVersionUpgradeChecker.check( logTailScanner, config );
                tail = logTailScanner.getTailInformation();

                if ( !tail.isRecoveryRequired() )
                {
                    // Log file is already in its new location, and looks good.
                    return;
                }
                if ( tail.logsMissing() && !config.get( fail_on_missing_files ) )
                {
                    // We don't have any log files, but we were told to ignore this.
                    return;
                }
            }
        }
        catch ( Throwable throwable )
        {
            // ignore exception and throw db not cleanly shutdown
            suppressibleException = throwable;
        }
        StoreUpgrader.DatabaseNotCleanlyShutDownException exception = new StoreUpgrader.DatabaseNotCleanlyShutDownException();
        if ( suppressibleException != null )
        {
            exception.addSuppressed( suppressibleException );
        }
        throw exception;
    }

    private DatabaseLayout buildLegacyLogsLayout( DatabaseLayout databaseLayout )
    {
        return new LegacyDatabaseLayout( databaseLayout.getNeo4jLayout(), databaseLayout.getDatabaseName(), legacyLogsLocator );
    }

    private LogTailScanner buildLogTailScanner( DatabaseLayout layout )
    {
        final LogEntryReader logEntryReader = new VersionAwareLogEntryReader();
        final LogFiles logFiles;
        try
        {
            logFiles = LogFilesBuilder.builder( layout, fs )
                                      .withLogEntryReader( logEntryReader )
                                      .withConfig( config )
                                      .withDependencies( dependencyResolver ).build();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        Monitors monitors = dependencyResolver.resolveDependency( Monitors.class );
        return new LogTailScanner( logFiles, logEntryReader, monitors, config.get( fail_on_corrupted_log_files ) );
    }

    public void upgrade( DatabaseLayout layout )
    {
        try ( TransactionMetaDataStore store = getMetaDataStore() )
        {
            TransactionLogInitializer logInitializer = new TransactionLogInitializer( fs, store );

            File transactionLogsDirectory = layout.getTransactionLogsDirectory();
            File legacyLogsDirectory = legacyLogsLocator.getTransactionLogsDirectory();
            boolean filesNeedsToMove = !transactionLogsDirectory.equals( legacyLogsDirectory );

            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( legacyLogsDirectory, fs ).build();
            // Move log files to their intended directory, if they are not there already.
            File[] legacyFiles = logFiles.logFiles();
            if ( legacyFiles != null && legacyFiles.length > 0 )
            {
                if ( filesNeedsToMove )
                {
                    for ( File legacyFile : legacyFiles )
                    {
                        fs.copyFile( legacyFile, new File( transactionLogsDirectory, legacyFile.getName() ), EMPTY_COPY_OPTIONS );
                    }
                }
                logInitializer.initializeExistingLogFiles( layout, transactionLogsDirectory );
                if ( filesNeedsToMove )
                {
                    for ( File legacyFile : legacyFiles )
                    {
                        fs.deleteFile( legacyFile );
                    }
                }
            }
            else
            {
                // We didn't find any files in the legacy location.
                // If the legacy location is the same as the intended location, then the log files are missing entirely.
                // Otherwise, we will have to check if the log files are already present in the intended location and try to initialize them there.
                logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( transactionLogsDirectory, fs ).build();
                legacyFiles = logFiles.logFiles();
                if ( legacyFiles != null && legacyFiles.length > 0 )
                {
                    // The log files are already at their intended location, so initialize them there.
                    logInitializer.initializeExistingLogFiles( layout, transactionLogsDirectory );
                }
                else if ( config.get( fail_on_missing_files ) )
                {
                    // The log files are missing entirely.
                    // By default, we should avoid modifying stores that have no log files,
                    // since we log files are the only thing that can tell us if the store is in a
                    // recovered state or not.
                    throw new UpgradeNotAllowedException();
                }
                else
                {
                    // The log files are missing entirely, but we were told to not think of this as an error condition,
                    // so we instead initialize an empty log file.
                    logInitializer.initializeEmptyLogFile( layout, transactionLogsDirectory );
                }
            }
        }
        catch ( Exception exception )
        {
            throw new StoreUpgrader.TransactionLogsRelocationException(
                    "Failure on attempt to move transaction logs into new location.", exception );
        }
    }

    private TransactionMetaDataStore getMetaDataStore() throws IOException
    {
        // Make sure to create the TransactionMetaDataStore with a `read_only` config,
        // to avoid relying on the persistent id generators.
        // We can't use those id files because at this point they haven't been migrated yet.
        Config readOnlyConfig = Config.defaults( GraphDatabaseSettings.read_only, true );
        return storageEngineFactory.transactionMetaDataStore( fs, databaseLayout, readOnlyConfig, pageCache );
    }
}
