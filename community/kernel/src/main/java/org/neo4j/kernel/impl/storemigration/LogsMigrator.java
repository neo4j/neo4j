/*
 * Copyright (c) "Neo4j"
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;

class LogsMigrator
{
    private static final String MIGRATION_CHECKPOINT = "Migration checkpoint.";
    private final FileSystemAbstraction fs;
    private final StorageEngineFactory storageEngineFactory;
    private final DatabaseLayout databaseLayout;
    private final PageCache pageCache;
    private final Config config;
    private final CursorContextFactory contextFactory;
    private final Supplier<LogTailMetadata> logTailSupplier;

    LogsMigrator(
            FileSystemAbstraction fs,
            StorageEngineFactory storageEngineFactory,
            DatabaseLayout databaseLayout,
            PageCache pageCache,
            Config config,
            CursorContextFactory contextFactory,
            Supplier<LogTailMetadata> logTailSupplier )
    {
        this.fs = fs;
        this.storageEngineFactory = storageEngineFactory;
        this.databaseLayout = databaseLayout;
        this.pageCache = pageCache;
        this.config = config;
        this.contextFactory = contextFactory;
        this.logTailSupplier = logTailSupplier;
    }

    CheckResult assertCleanlyShutDown()
    {
        LogTailMetadata logTail;

        try
        {
            logTail = logTailSupplier.get();
        }
        catch ( Throwable throwable )
        {
            throw new UnableToMigrateException( "Failed to verify the transaction logs. This most likely means that the transaction logs are corrupted.",
                    throwable );
        }
        if ( logTail.logsMissing() )
        {
            if ( config.get( fail_on_missing_files ) )
            {
                // The log files are missing entirely.
                // By default, we should avoid modifying stores that have no log files,
                // since the log files are the only thing that can tell us if the store is in a
                // recovered state or not.
                throw new UnableToMigrateException( "Transaction logs not found" );
            }
            return new CheckResult( true );
        }
        if ( logTail.isRecoveryRequired() )
        {
            throw new UnableToMigrateException( "The database is not cleanly shutdown. The database needs recovery, in order to recover the database, "
                    + "please run the version of the DBMS you are migrating from on this store." );
        }
        // all good
        return new CheckResult( false );
    }

    /**
     * Refer to {@link StoreMigrator} for an explanation of the difference between migration and upgrade.
     */
    class CheckResult
    {
        private final boolean logsMissing;

        private CheckResult( boolean logsMissing )
        {
            this.logsMissing = logsMissing;
        }

        void migrate()
        {
            try ( MetadataProvider store = getMetaDataStore() )
            {
                TransactionLogInitializer logInitializer = new TransactionLogInitializer( fs, store, storageEngineFactory );
                Path transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory();

                if ( logsMissing )
                {
                    // The log files are missing entirely, but since we made it through the check,
                    // we were told to not think of this as an error condition,
                    // so we instead initialize an empty log file.
                    logInitializer.initializeEmptyLogFile( databaseLayout, transactionLogsDirectory, MIGRATION_CHECKPOINT );
                }
                else
                {
                    logInitializer.migrateExistingLogFiles( databaseLayout, transactionLogsDirectory, MIGRATION_CHECKPOINT );
                }
            }
            catch ( Exception exception )
            {
                throw new UnableToMigrateException( "Failure on attempt to migrate transaction logs to new version.", exception );
            }
        }

        void upgrade()
        {
            if ( !logsMissing )
            {
                return;
            }

            // The log files are missing entirely, but since we made it through the check,
            // we were told to not think of this as an error condition,
            // so we instead initialize an empty log file.
            try ( MetadataProvider store = getMetaDataStore() )
            {
                TransactionLogInitializer logInitializer = new TransactionLogInitializer( fs, store, storageEngineFactory );
                Path transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory();
                logInitializer.initializeEmptyLogFile( databaseLayout, transactionLogsDirectory, MIGRATION_CHECKPOINT );
            }
            catch ( Exception exception )
            {
                throw new UnableToMigrateException( "Failure on attempt to upgrade transaction logs to new version.", exception );
            }
        }
    }

    private MetadataProvider getMetaDataStore() throws IOException
    {
        return storageEngineFactory.transactionMetaDataStore( fs, databaseLayout, config, pageCache, DatabaseReadOnlyChecker.readOnly(),
                contextFactory, logTailSupplier.get() );
    }
}
