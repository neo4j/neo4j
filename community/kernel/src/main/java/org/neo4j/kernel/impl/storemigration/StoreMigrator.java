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
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.migration.MigrationProgressMonitor;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

import static org.neo4j.storageengine.migration.StoreMigrationParticipant.NOT_PARTICIPATING;
import static org.neo4j.util.Preconditions.checkState;

public class StoreMigrator
{
    private static final String STORE_UPGRADE_TAG = "storeUpgrade";
    // TODO: Do something about the name of the directory to reflect the migration process,
    // but let's leave it compatible with StoreUpgrader for now.
    public static final String MIGRATION_DIRECTORY = "upgrade";
    private static final String MIGRATION_STATUS_FILE = "_status";

    private final MigrationProgressMonitor progressMonitor;
    private final CursorContextFactory contextFactory;
    private final DatabaseLayout databaseLayout;
    private final FileSystemAbstraction fs;
    private final Config config;
    private final PageCache pageCache;
    private final LogService logService;
    private final JobScheduler jobScheduler;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;
    private final IndexProviderMap indexProviderMap;
    private final StorageEngineFactory storageEngineFactoryToMigrateFrom;
    private final InternalLog userLog;
    private final Supplier<LogTailMetadata> logTailSupplier;

    public StoreMigrator(
            FileSystemAbstraction fs, Config config, LogService logService, PageCache pageCache,
            PageCacheTracer pageCacheTracer, JobScheduler jobScheduler, DatabaseLayout databaseLayout, StorageEngineFactory storageEngineFactoryToMigrateFrom,
            IndexProviderMap indexProviderMap, CursorContextFactory contextFactory, MemoryTracker memoryTracker,
            Supplier<LogTailMetadata> logTailSupplier )
    {
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.pageCache = pageCache;
        this.databaseLayout = databaseLayout;
        this.storageEngineFactoryToMigrateFrom = storageEngineFactoryToMigrateFrom;
        this.contextFactory = contextFactory;
        this.jobScheduler = jobScheduler;
        this.pageCacheTracer = pageCacheTracer;
        this.memoryTracker = memoryTracker;
        this.indexProviderMap = indexProviderMap;

        this.userLog = logService.getUserLog( getClass() );
        this.logTailSupplier = logTailSupplier;
        progressMonitor = new VisibleMigrationProgressMonitor( userLog );
    }

    public void migrateIfNeeded( StorageEngineFactory storageEngineFactoryToMigrateTo, String formatFamily ) throws UnableToMigrateException, IOException
    {
        // Let's do the check to make sure the logic in this class does not blow up.
        // However, since this class was provided a storage engine factory representing the current store,
        // the check should have been also done when the storage engine factory was obtained.
        checkStoreExists();

        try ( var cursorContext = contextFactory.create( STORE_UPGRADE_TAG ) )
        {
            DatabaseLayout migrationStructure = DatabaseLayout.ofFlat( databaseLayout.file( MIGRATION_DIRECTORY ) );
            Path migrationStateFile = migrationStructure.file( MIGRATION_STATUS_FILE );
            String versionToMigrateTo = getVersionToMigrateTo( storageEngineFactoryToMigrateTo, formatFamily, cursorContext );
            String versionToMigrateFrom = checkMigrationCompatibilityAndGetVersion( versionToMigrateTo, cursorContext );

            if ( isOnRequestedVersion( versionToMigrateFrom, versionToMigrateTo, migrationStateFile ) )
            {
                return;
            }

            doMigrate( migrationStructure, migrationStateFile, versionToMigrateFrom, storageEngineFactoryToMigrateTo, versionToMigrateTo );
        }
    }

    private String getVersionToMigrateTo( StorageEngineFactory storageEngineFactoryToMigrateTo, String formatFamily, CursorContext cursorContext )
    {
        try
        {
            StoreVersionCheck storeVersionCheck =
                    storageEngineFactoryToMigrateTo.versionCheck( fs, databaseLayout, config, pageCache, logService, contextFactory );
            String versionToMigrateTo = storeVersionCheck.getLatestAvailableVersion( formatFamily, cursorContext );
            userLog.info( "'" + versionToMigrateTo + "' has been identified as the target version of the store migration" );
            return versionToMigrateTo;
        }
        catch ( Exception e )
        {
            throw new UnableToMigrateException( "Failed to determine the target version", e );
        }
    }

    private void doMigrate( DatabaseLayout migrationLayout, Path migrationStateFile, String versionToMigrateFrom,
            StorageEngineFactory storageEngineFactoryToMigrateTo, String versionToMigrateTo ) throws IOException
    {
        var participants = getStoreMigrationParticipants( storageEngineFactoryToMigrateTo );
        // One or more participants would like to do migration
        progressMonitor.started( participants.size() );

        var logsUpgrader = new LogsMigrator( fs, storageEngineFactoryToMigrateFrom, storageEngineFactoryToMigrateTo, databaseLayout, pageCache, config,
                contextFactory, logTailSupplier );
        logsUpgrader.assertCleanlyShutDown();

        MigrationStatus migrationStatus = MigrationStatus.readMigrationStatus( fs, migrationStateFile );
        // We don't need to migrate if we're at the phase where we have migrated successfully
        // and it's just a matter of moving over the files to the storeDir.
        // TODO: we need to make sure that the the migration that happens to be in progress is to 'versionToMigrateTo'
        if ( MigrationStatus.migrating.isNeededFor( migrationStatus ) )
        {
            StoreVersion fromVersion = storageEngineFactoryToMigrateFrom.versionInformation( versionToMigrateFrom );
            StoreVersion toVersion = storageEngineFactoryToMigrateTo.versionInformation( versionToMigrateTo );
            cleanMigrationDirectory( migrationLayout.databaseDirectory() );
            MigrationStatus.migrating.setMigrationStatus( fs, migrationStateFile, versionToMigrateFrom );
            migrateToIsolatedDirectory( participants, databaseLayout, migrationLayout, fromVersion, toVersion );
            MigrationStatus.moving.setMigrationStatus( fs, migrationStateFile, versionToMigrateFrom );
        }

        if ( MigrationStatus.moving.isNeededFor( migrationStatus ) )
        {
            moveMigratedFilesToStoreDirectory( participants, migrationLayout, databaseLayout, versionToMigrateFrom, versionToMigrateTo );
        }

        progressMonitor.startTransactionLogsMigration();
        logsUpgrader.upgrade( databaseLayout );
        progressMonitor.completeTransactionLogsMigration();

        cleanup( participants, migrationLayout );

        progressMonitor.completed();
    }

    private String checkMigrationCompatibilityAndGetVersion( String versionToMigrateTo, CursorContext cursorContext )
    {
        StoreVersionCheck storeVersionCheck =
                storageEngineFactoryToMigrateFrom.versionCheck( fs, databaseLayout, config, pageCache, logService, contextFactory );
        StoreVersionCheck.Result result = storeVersionCheck.checkUpgrade( versionToMigrateTo, cursorContext );
        String versionToMigrateFrom = switch ( result.outcome() )
                {
                    case ok -> result.actualVersion();
                    case unexpectedUpgradingVersion -> throw new UnableToMigrateException(
                            String.format( "Store migration from '%s' to '%s' not supported", result.actualVersion(), versionToMigrateTo ) );
                    case storeVersionNotFound -> throw new UnableToMigrateException(
                            "Failed to read current store version. This usually indicate a store corruption" );
                    // The rest of the cases cannot really happen,
                    // they are here mostly to have somewhere to write the explanation why they cannot happen.

                    // The existence of the database and  metadata store is the first thing the migration logic in this class does
                    case missingStoreFile -> throw new UnableToMigrateException(
                            "Database '" + databaseLayout.getDatabaseName() + "' either does not exists or it has not been initialised" );
                    // The version 'versionToMigrateTo' is chosen as the latest known version of the provided format family
                    // or the format family the database is currently on. Since the latest known is always chosen,
                    // we cannot really downgrade.
                    case attemptedStoreDowngrade -> throw new UnableToMigrateException( "Downgrading stores is not supported." );
                    // Since 'versionToMigrateTo' was not directly provided by the user, but selected using the logic described above,
                    // 'versionToMigrateTo' being an unexpected version would mean a serious bug.
                    case unexpectedStoreVersion -> throw new UnableToMigrateException(
                            String.format( "Not possible to upgrade a store with version '%s' to current store version '%s'",
                                    result.actualVersion(),
                                    versionToMigrateTo )
                    );
                };
        userLog.info( "'" + versionToMigrateFrom + "' has been identified as the current version of the store" );
        return versionToMigrateFrom;
    }

    private List<StoreMigrationParticipant> getStoreMigrationParticipants( StorageEngineFactory storageEngineFactoryToMigrateTo )
    {
        List<StoreMigrationParticipant> participants = new ArrayList<>();
        // Get all the participants from the storage engine and add them where they want to be
        var storeParticipants = storageEngineFactoryToMigrateTo.migrationParticipants(
                fs, config, pageCache, jobScheduler, logService, memoryTracker, pageCacheTracer, contextFactory );
        participants.addAll( storeParticipants );

        // Do individual index provider migration last because they may delete files that we need in earlier steps.
        indexProviderMap.accept(
                provider -> participants.add( provider.storeMigrationParticipant( fs, pageCache, storageEngineFactoryToMigrateTo, contextFactory ) ) );

        Set<String> participantNames = new HashSet<>();
        participants.forEach( participant ->
        {
            if ( !NOT_PARTICIPATING.equals( participant ) )
            {
                var newParticipantName = participant.getName();
                checkState( !participantNames.contains( newParticipantName ),
                        "Migration participants should have unique names. Participant with name: '%s' is already registered.", newParticipantName );
                participantNames.add( newParticipantName );
            }
        } );

        return participants;
    }

    private void migrateToIsolatedDirectory( List<StoreMigrationParticipant> participants, DatabaseLayout directoryLayout, DatabaseLayout migrationLayout,
            StoreVersion fromVersion, StoreVersion toVersion )
    {
        try
        {
            for ( StoreMigrationParticipant participant : participants )
            {
                ProgressReporter progressReporter = progressMonitor.startSection( participant.getName() );
                IndexImporterFactory indexImporterFactory = new IndexImporterFactoryImpl();
                participant.migrate( directoryLayout, migrationLayout, progressReporter, fromVersion, toVersion, indexImporterFactory,
                        logTailSupplier.get() );
                progressReporter.completed();
            }
        }
        catch ( IOException | UncheckedIOException | KernelException e )
        {
            throw new UnableToMigrateException( "A critical failure during migration has occurred", e );
        }
    }

    private static void moveMigratedFilesToStoreDirectory( Iterable<StoreMigrationParticipant> participants, DatabaseLayout migrationLayout,
            DatabaseLayout directoryLayout, String versionToMigrateFrom, String versionToMigrateTo )
    {
        try
        {
            for ( StoreMigrationParticipant participant : participants )
            {
                participant.moveMigratedFiles( migrationLayout, directoryLayout, versionToMigrateFrom,
                        versionToMigrateTo );
            }
        }
        catch ( IOException e )
        {
            throw new UnableToMigrateException( "A critical failure during migration has occurred. Failed to move migrated files into place", e );
        }
    }

    private void checkStoreExists()
    {
        if ( !storageEngineFactoryToMigrateFrom.storageExists( fs, databaseLayout, pageCache ) )
        {
            throw new UnableToMigrateException( "Database '" + databaseLayout.getDatabaseName() + "' ether does not exists or it has not been initialised" );
        }
    }

    private boolean isOnRequestedVersion( String versionToMigrateFrom, String versionToMigrateTo, Path migrationStateFile )
    {
        if ( versionToMigrateFrom.equals( versionToMigrateTo ) && !fs.fileExists( migrationStateFile ) )
        {
            userLog.info( "The current store version and the migration target version are the same, so there is nothing to do." );
            return true;
        }
        return false;
    }

    private static void cleanup( Iterable<StoreMigrationParticipant> participants, DatabaseLayout migrationStructure )
    {
        try
        {
            for ( StoreMigrationParticipant participant : participants )
            {
                participant.cleanup( migrationStructure );
            }
        }
        catch ( IOException e )
        {
            throw new UnableToMigrateException(
                    "A critical failure during migration has occurred. Failed to clean up after migration", e );
        }
    }

    private void cleanMigrationDirectory( Path migrationDirectory )
    {
        try
        {
            if ( fs.fileExists( migrationDirectory ) )
            {
                fs.deleteRecursively( migrationDirectory );
            }
        }
        catch ( IOException | UncheckedIOException e )
        {
            throw new UnableToMigrateException(
                    "A critical failure during migration has occurred. Failed to delete a migration directory " + migrationDirectory, e );
        }
        try
        {
            fs.mkdir( migrationDirectory );
        }
        catch ( IOException e )
        {
            throw new UnableToMigrateException(
                    "A critical failure during migration has occurred. Failed to create a migration directory " + migrationDirectory, e );
        }
    }
}
