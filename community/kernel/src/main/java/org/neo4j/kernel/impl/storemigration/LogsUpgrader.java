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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionMetaDataStore;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;

import static org.neo4j.io.fs.FileSystemAbstraction.EMPTY_COPY_OPTIONS;

public class LogsUpgrader
{
    private final FileSystemAbstraction fs;
    private final StorageEngineFactory storageEngineFactory;
    private final DatabaseLayout databaseLayout;
    private final PageCache pageCache;
    private final LegacyTransactionLogsLocator legacyLogsLocator;
    private final Config config;

    public LogsUpgrader( FileSystemAbstraction fs, StorageEngineFactory storageEngineFactory, DatabaseLayout databaseLayout, PageCache pageCache,
                         LegacyTransactionLogsLocator legacyLogsLocator, Config config )
    {
        this.fs = fs;
        this.storageEngineFactory = storageEngineFactory;
        this.databaseLayout = databaseLayout;
        this.pageCache = pageCache;
        this.legacyLogsLocator = legacyLogsLocator;
        this.config = config;
    }

    public void upgrade( DatabaseLayout dbDirectoryLayout )
    {
        try ( TransactionMetaDataStore store = getMetaDataStore() )
        {
            TransactionLogInitializer logInitializer = new TransactionLogInitializer( fs, store );

            File transactionLogsDirectory = dbDirectoryLayout.getTransactionLogsDirectory();
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
                logInitializer.initializeExistingLogFiles( dbDirectoryLayout, transactionLogsDirectory );
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
                if ( config.get( GraphDatabaseSettings.fail_on_missing_files ) )
                {
                    // By default, we should avoid modifying stores that have no log files,
                    // since we log files are the only thing that can tell us if the store is in a
                    // recovered state or not.
                    throw new UpgradeNotAllowedException();
                }
                else
                {
                    logInitializer.initializeEmptyLogFile( dbDirectoryLayout, transactionLogsDirectory );
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
