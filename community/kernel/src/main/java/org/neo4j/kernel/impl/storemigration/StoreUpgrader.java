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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.internal.Version;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.IndexCapabilities;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.migration.MigrationProgressMonitor;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;

import static org.neo4j.storageengine.migration.StoreMigrationParticipant.NOT_PARTICIPATING;
import static org.neo4j.util.Preconditions.checkState;

/**
 * A migration process to migrate {@link StoreMigrationParticipant migration participants}, if there's
 * need for it, before the database fully starts. Participants can
 * {@link #addParticipant(StoreMigrationParticipant) register} and will be notified when it's time for migration.
 * The migration will happen to a separate, isolated directory so that an incomplete migration will not affect
 * the original database. Only when a successful migration has taken place the migrated store will replace
 * the original database.
 * <p/>
 * Migration process at a glance:
 * <ol>
 * <li>Participants are asked whether or not there's a need for migration</li>
 * <li>Those that need are asked to migrate into a separate /upgrade directory. Regardless of who actually
 * performs migration all participants are asked to satisfy dependencies for downstream participants</li>
 * <li>Migration is marked as migrated</li>
 * <li>Participants are asked to move their migrated files into the source directory,
 * replacing only the existing files, so that if only some store files needed migration the others are left intact</li>
 * <li>Migration is completed and participant resources are closed</li>
 * </ol>
 * <p/>
 *
 * @see StoreMigrationParticipant
 */
public class StoreUpgrader
{
    private static final String STORE_UPGRADE_TAG = "storeUpgrade";
    public static final String MIGRATION_DIRECTORY = "upgrade";
    public static final String MIGRATION_LEFT_OVERS_DIRECTORY = "upgrade_backup";
    private static final String MIGRATION_STATUS_FILE = "_status";
    private static final Pattern MIGRATION_LEFTOVERS_PATTERN = Pattern.compile( MIGRATION_LEFT_OVERS_DIRECTORY + "(_\\d*)?" );

    private final StoreVersionCheck storeVersionCheck;
    private final MigrationProgressMonitor progressMonitor;
    private final LinkedHashMap<String, StoreMigrationParticipant> participants = new LinkedHashMap<>();
    private final Config config;
    private final FileSystemAbstraction fileSystem;
    private final Log log;
    private final LogsUpgrader logsUpgrader;
    private final String configuredFormat;
    private final PageCacheTracer pageCacheTracer;

    public StoreUpgrader( StoreVersionCheck storeVersionCheck, MigrationProgressMonitor progressMonitor, Config config,
                          FileSystemAbstraction fileSystem, LogProvider logProvider, LogsUpgrader logsUpgrader,
                          PageCacheTracer pageCacheTracer )
    {
        this.storeVersionCheck = storeVersionCheck;
        this.progressMonitor = progressMonitor;
        this.fileSystem = fileSystem;
        this.config = config;
        this.logsUpgrader = logsUpgrader;
        this.log = logProvider.getLog( getClass() );
        this.configuredFormat = storeVersionCheck.configuredVersion();
        this.pageCacheTracer = pageCacheTracer;
    }

    /**
     * Add migration participant into a participants list.
     * Participant will be added into the end of a list and will be executed only after all predecessors.
     * @param participant - participant to add into migration
     */
    public void addParticipant( StoreMigrationParticipant participant )
    {
        assert participant != null;
        if ( !NOT_PARTICIPATING.equals( participant ) )
        {
            var newParticipantName = participant.getName();
            checkState( !participants.containsKey( newParticipantName ),
                    "Migration participants should have unique names. Participant with name: `%s` is already registered.", newParticipantName );
            this.participants.put( newParticipantName, participant );
        }
    }

    /**
     * Upgrade the store format, if it is not the latest version or is different from the configured desired format.
     *
     * @param layout The layout of the existing database store.
     * @param forceUpgrade If {@code true}, the value of the {@link GraphDatabaseSettings#allow_upgrade} setting is ignored.
     */
    public void migrateIfNeeded( DatabaseLayout layout, boolean forceUpgrade ) throws IOException
    {
        // nothing to migrate
        if ( !Files.exists( layout.databaseDirectory() ) )
        {
            return;
        }
        if ( layout.getDatabaseName().equals( GraphDatabaseSettings.SYSTEM_DATABASE_NAME ) )
        {
            // TODO: System database does not (yet) support migration, remove this when it does!
            return;
        }

        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( STORE_UPGRADE_TAG ) )
        {
            DatabaseLayout migrationStructure = DatabaseLayout.ofFlat( layout.file( MIGRATION_DIRECTORY ) );

            cleanupLegacyLeftOverDirsIn( layout.databaseDirectory() );

            Path migrationStateFile = migrationStructure.file( MIGRATION_STATUS_FILE );
            // if migration directory exists than we might have failed to move files into the store dir so do it again
            if ( hasCurrentVersion( storeVersionCheck, cursorTracer ) && !fileSystem.fileExists( migrationStateFile ) )
            {
                // No migration needed
                return;
            }

            if ( isUpgradeAllowed() || forceUpgrade )
            {
                migrate( layout, migrationStructure, migrationStateFile, cursorTracer );
            }
            else
            {
                Optional<String> storeVersion = storeVersionCheck.storeVersion( cursorTracer );
                if ( storeVersion.isPresent() )
                {
                    StoreVersion version = storeVersionCheck.versionInformation( storeVersion.get() );
                    if ( version.hasCapability( IndexCapabilities.LuceneCapability.LUCENE_5 ) )
                    {
                        throw new UpgradeNotAllowedException( "Upgrade is required to migrate store to new major version." );
                    }
                    else
                    {
                        String configuredVersion = storeVersionCheck.configuredVersion();
                        if ( configuredVersion != null && !version.isCompatibleWith( storeVersionCheck.versionInformation( configuredVersion ) ) )
                        {
                            throw new UpgradeNotAllowedException();
                        }
                    }
                }
            }
        }
    }

    private static boolean hasCurrentVersion( StoreVersionCheck storeVersionCheck, PageCursorTracer cursorTracer )
    {
        String configuredVersion = storeVersionCheck.configuredVersion();
        StoreVersionCheck.Result versionResult = storeVersionCheck.checkUpgrade( configuredVersion, cursorTracer );
        if ( versionResult.outcome == StoreVersionCheck.Outcome.missingStoreFile )
        {
            // New store so will be of the current version
            return true;
        }
        return versionResult.outcome.isSuccessful() && versionResult.actualVersion.equals( configuredVersion );
    }

    private void migrate( DatabaseLayout dbDirectoryLayout, DatabaseLayout migrationLayout, Path migrationStateFile, PageCursorTracer cursorTracer )
    {
        // One or more participants would like to do migration
        progressMonitor.started( participants.size() );

        MigrationStatus migrationStatus = MigrationStatus.readMigrationStatus( fileSystem, migrationStateFile );
        String versionToMigrateFrom = null;
        // We don't need to migrate if we're at the phase where we have migrated successfully
        // and it's just a matter of moving over the files to the storeDir.
        if ( MigrationStatus.migrating.isNeededFor( migrationStatus ) )
        {
            StoreVersionCheck.Result upgradeCheck = storeVersionCheck.checkUpgrade( storeVersionCheck.configuredVersion(), cursorTracer );
            versionToMigrateFrom = getVersionFromResult( upgradeCheck );
            logsUpgrader.assertCleanlyShutDown( dbDirectoryLayout );
            cleanMigrationDirectory( migrationLayout.databaseDirectory() );
            MigrationStatus.migrating.setMigrationStatus( fileSystem, migrationStateFile, versionToMigrateFrom );
            migrateToIsolatedDirectory( dbDirectoryLayout, migrationLayout, versionToMigrateFrom );
            MigrationStatus.moving.setMigrationStatus( fileSystem, migrationStateFile, versionToMigrateFrom );
        }

        if ( MigrationStatus.moving.isNeededFor( migrationStatus ) )
        {
            versionToMigrateFrom = MigrationStatus.moving.maybeReadInfo( fileSystem, migrationStateFile, versionToMigrateFrom );
            String versionToMigrateTo = storeVersionCheck.configuredVersion();
            moveMigratedFilesToStoreDirectory( participants.values(), migrationLayout, dbDirectoryLayout, versionToMigrateFrom, versionToMigrateTo );
        }

        progressMonitor.startTransactionLogsMigration();
        logsUpgrader.upgrade( dbDirectoryLayout );
        progressMonitor.completeTransactionLogsMigration();

        cleanup( participants.values(), migrationLayout );

        progressMonitor.completed();
    }

    private String getVersionFromResult( StoreVersionCheck.Result result )
    {
        switch ( result.outcome )
        {
        case ok:
            return result.actualVersion;
        case missingStoreFile:
            throw new StoreUpgrader.UpgradeMissingStoreFilesException( result.storeFilename );
        case storeVersionNotFound:
            throw new StoreUpgrader.UpgradingStoreVersionNotFoundException( result.storeFilename );
        case attemptedStoreDowngrade:
            throw new StoreUpgrader.AttemptedDowngradeException();
        case unexpectedStoreVersion:
            throw new StoreUpgrader.UnexpectedUpgradingStoreVersionException( result.actualVersion, configuredFormat );
        case storeNotCleanlyShutDown:
            throw new StoreUpgrader.DatabaseNotCleanlyShutDownException();
        case unexpectedUpgradingVersion:
            throw new StoreUpgrader.UnexpectedUpgradingStoreFormatException();
        default:
            throw new IllegalArgumentException( "Unexpected outcome: " + result.outcome.name() );
        }
    }

    List<StoreMigrationParticipant> getParticipants()
    {
        return List.copyOf( participants.values() );
    }

    private boolean isUpgradeAllowed()
    {
        return config.get( GraphDatabaseSettings.allow_upgrade );
    }

    private void cleanupLegacyLeftOverDirsIn( Path databaseDirectory ) throws IOException
    {

        try ( DirectoryStream<Path> paths = Files.newDirectoryStream( databaseDirectory ) )
        {
            for ( Path path : paths )
            {
                if ( MIGRATION_LEFTOVERS_PATTERN.matcher( path.getFileName().toString() ).matches() )
                {
                    deleteSilently( path );
                }
            }
        }
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
            throw new UnableToUpgradeException( "Failure cleaning up after migration", e );
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
            throw new UnableToUpgradeException( "Unable to move migrated files into place", e );
        }
    }

    private void migrateToIsolatedDirectory( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, String versionToMigrateFrom )
    {
        try
        {
            for ( Map.Entry<String, StoreMigrationParticipant> participantEntry : participants.entrySet() )
            {
                ProgressReporter progressReporter = progressMonitor.startSection( participantEntry.getKey() );
                String versionToMigrateTo = storeVersionCheck.configuredVersion();
                participantEntry.getValue().migrate( directoryLayout, migrationLayout, progressReporter, versionToMigrateFrom, versionToMigrateTo );
                progressReporter.completed();
            }
        }
        catch ( IOException | UncheckedIOException | KernelException e )
        {
            throw new UnableToUpgradeException( "Failure doing migration", e );
        }
    }

    private void cleanMigrationDirectory( Path migrationDirectory )
    {
        try
        {
            if ( fileSystem.fileExists( migrationDirectory ) )
            {
                fileSystem.deleteRecursively( migrationDirectory );
            }
        }
        catch ( IOException | UncheckedIOException e )
        {
            throw new UnableToUpgradeException( "Failure deleting upgrade directory " + migrationDirectory, e );
        }
        fileSystem.mkdir( migrationDirectory );
    }

    private void deleteSilently( Path dir )
    {
        try
        {
            fileSystem.deleteRecursively( dir );
        }
        catch ( IOException e )
        {
            log.error( "Unable to delete directory: " + dir, e );
        }
    }

    static class TransactionLogsRelocationException extends RuntimeException
    {
        TransactionLogsRelocationException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

    public static class UnableToUpgradeException extends RuntimeException
    {
        public UnableToUpgradeException( String message, Throwable cause )
        {
            super( message, cause );
        }

        UnableToUpgradeException( String message )
        {
            super( message );
        }
    }

    static class UpgradeMissingStoreFilesException extends UnableToUpgradeException
    {
        private static final String MESSAGE = "Missing required store file '%s'.";

        UpgradeMissingStoreFilesException( String filenameExpectedToExist )
        {
            super( String.format( MESSAGE, filenameExpectedToExist ) );
        }
    }

    static class UpgradingStoreVersionNotFoundException extends UnableToUpgradeException
    {
        private static final String MESSAGE =
                "'%s' does not contain a store version, please ensure that the original database was shut down in a " +
                "clean state.";

        UpgradingStoreVersionNotFoundException( String filenameWithoutStoreVersion )
        {
            super( String.format( MESSAGE, filenameWithoutStoreVersion ) );
        }
    }

    public static class UnexpectedUpgradingStoreVersionException extends UnableToUpgradeException
    {
        static final String MESSAGE =
                "Not possible to upgrade a store with version '%s' to current store version `%s` (Neo4j %s).";

        UnexpectedUpgradingStoreVersionException( String fileVersion, String currentVersion )
        {
            super( String.format( MESSAGE, fileVersion, currentVersion, Version.getNeo4jVersion() ) );
        }
    }

    public static class AttemptedDowngradeException extends UnableToUpgradeException
    {
        static final String MESSAGE = "Downgrading stores are not supported.";

        AttemptedDowngradeException()
        {
            super( MESSAGE );
        }
    }

    public static class UnexpectedUpgradingStoreFormatException extends UnableToUpgradeException
    {
        static final String MESSAGE =
                "This is an enterprise-only store. Please configure '%s' to open.";

        UnexpectedUpgradingStoreFormatException()
        {
            super( String.format( MESSAGE, GraphDatabaseSettings.record_format.name() ) );
        }
    }

    public static class DatabaseNotCleanlyShutDownException extends UnableToUpgradeException
    {
        private static final String MESSAGE =
                "The database is not cleanly shutdown. The database needs recovery, in order to recover the database, "
                + "please run the old version of the database on this store.";

        DatabaseNotCleanlyShutDownException()
        {
            super( MESSAGE );
        }
    }
}
