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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionMetaDataStore;

import static org.neo4j.io.fs.FileSystemAbstraction.EMPTY_COPY_OPTIONS;

public class LogsUpgrader
{
    private final FileSystemAbstraction fs;
    private final StorageEngineFactory storageEngineFactory;
    private final DatabaseLayout databaseLayout;
    private final PageCache pageCache;
    private final LegacyTransactionLogsLocator legacyLogsLocator;

    public LogsUpgrader( FileSystemAbstraction fs, StorageEngineFactory storageEngineFactory, DatabaseLayout databaseLayout, PageCache pageCache,
                         LegacyTransactionLogsLocator legacyLogsLocator )
    {
        this.fs = fs;
        this.storageEngineFactory = storageEngineFactory;
        this.databaseLayout = databaseLayout;
        this.pageCache = pageCache;
        this.legacyLogsLocator = legacyLogsLocator;
    }

    public void upgrade( DatabaseLayout dbDirectoryLayout )
    {
        Config config = Config.defaults( GraphDatabaseSettings.read_only, true );
        try ( TransactionMetaDataStore txStore = storageEngineFactory.transactionMetaDataStore( fs, databaseLayout, config, pageCache ) )
        {
            StoreId storeId = storageEngineFactory.storeId( databaseLayout, pageCache );
            TransactionLogsMigrator logMigrator = new TransactionLogsMigrator( fs, txStore, storeId, txStore );

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
                logMigrator.migrateLogFile( dbDirectoryLayout, transactionLogsDirectory );
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
                logMigrator.createEmptyLogFile( dbDirectoryLayout, transactionLogsDirectory );
            }
        }
        catch ( Exception exception )
        {
            throw new StoreUpgrader.TransactionLogsRelocationException(
                    "Failure on attempt to move transaction logs into new location.", exception );
        }
    }
}
