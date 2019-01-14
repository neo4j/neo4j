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
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.internal.Version;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

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
 * TODO walk through crash scenarios and how they are handled.
 *
 * @see StoreMigrationParticipant
 */
public class StoreUpgrader
{
    public static final String MIGRATION_DIRECTORY = "upgrade";
    public static final String MIGRATION_LEFT_OVERS_DIRECTORY = "upgrade_backup";
    private static final String MIGRATION_STATUS_FILE = "_status";

    private final UpgradableDatabase upgradableDatabase;
    private final MigrationProgressMonitor progressMonitor;
    private final List<StoreMigrationParticipant> participants = new ArrayList<>();
    private final Config config;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final Log log;
    private final LogProvider logProvider;

    public StoreUpgrader( UpgradableDatabase upgradableDatabase, MigrationProgressMonitor progressMonitor, Config
            config, FileSystemAbstraction fileSystem, PageCache pageCache, LogProvider logProvider )
    {
        this.upgradableDatabase = upgradableDatabase;
        this.progressMonitor = progressMonitor;
        this.fileSystem = fileSystem;
        this.config = config;
        this.pageCache = pageCache;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    /**
     * Add migration participant into a participants list.
     * Participant will be added into the end of a list and will be executed only after all predecessors.
     * @param participant - participant to add into migration
     */
    public void addParticipant( StoreMigrationParticipant participant )
    {
        assert participant != null;
        if ( !StoreMigrationParticipant.NOT_PARTICIPATING.equals( participant ) )
        {
            this.participants.add( participant );
        }
    }

    public void migrateIfNeeded( File storeDirectory )
    {
        File migrationDirectory = new File( storeDirectory, MIGRATION_DIRECTORY );

        cleanupLegacyLeftOverDirsIn( storeDirectory );

        File migrationStateFile = new File( migrationDirectory, MIGRATION_STATUS_FILE );
        // if migration directory exists than we might have failed to move files into the store dir so do it again
        if ( upgradableDatabase.hasCurrentVersion( storeDirectory ) && !fileSystem.fileExists( migrationStateFile ) )
        {
            // No migration needed
            return;
        }

        if ( isUpgradeAllowed() )
        {
            migrateStore( storeDirectory, migrationDirectory, migrationStateFile );
        }
        else if ( !RecordFormatSelector.isStoreAndConfigFormatsCompatible( config, storeDirectory, pageCache, logProvider ) )
        {
            throw new UpgradeNotAllowedByConfigurationException();
        }
    }

    private void migrateStore( File storeDirectory, File migrationDirectory, File migrationStateFile )
    {
        // One or more participants would like to do migration
        progressMonitor.started( participants.size() );

        MigrationStatus migrationStatus = MigrationStatus.readMigrationStatus( fileSystem, migrationStateFile );
        String versionToMigrateFrom = null;
        // We don't need to migrate if we're at the phase where we have migrated successfully
        // and it's just a matter of moving over the files to the storeDir.
        if ( MigrationStatus.migrating.isNeededFor( migrationStatus ) )
        {
            versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory ).storeVersion();
            cleanMigrationDirectory( migrationDirectory );
            MigrationStatus.migrating.setMigrationStatus( fileSystem, migrationStateFile, versionToMigrateFrom );
            migrateToIsolatedDirectory( storeDirectory, migrationDirectory, versionToMigrateFrom );
            MigrationStatus.moving.setMigrationStatus( fileSystem, migrationStateFile, versionToMigrateFrom );
        }

        if ( MigrationStatus.moving.isNeededFor( migrationStatus ) )
        {
            versionToMigrateFrom =
                    MigrationStatus.moving.maybeReadInfo( fileSystem, migrationStateFile, versionToMigrateFrom );
            moveMigratedFilesToStoreDirectory( participants, migrationDirectory, storeDirectory,
                    versionToMigrateFrom, upgradableDatabase.currentVersion() );
        }

        cleanup( participants, migrationDirectory );

        progressMonitor.completed();
    }

    List<StoreMigrationParticipant> getParticipants()
    {
        return participants;
    }

    private boolean isUpgradeAllowed()
    {
        return config.get( GraphDatabaseSettings.allow_upgrade );
    }

    private void cleanupLegacyLeftOverDirsIn( File storeDir )
    {
        final Pattern leftOverDirsPattern = Pattern.compile( MIGRATION_LEFT_OVERS_DIRECTORY + "(_\\d*)?" );
        File[] leftOverDirs = storeDir.listFiles(
                ( file, name ) -> file.isDirectory() && leftOverDirsPattern.matcher( name ).matches() );
        if ( leftOverDirs != null )
        {
            for ( File leftOverDir : leftOverDirs )
            {
                deleteSilently( leftOverDir );
            }
        }
    }

    private void cleanup( Iterable<StoreMigrationParticipant> participants, File migrationDirectory )
    {
        try
        {
            for ( StoreMigrationParticipant participant : participants )
            {
                participant.cleanup( migrationDirectory );
            }
        }
        catch ( IOException e )
        {
            throw new UnableToUpgradeException( "Failure cleaning up after migration", e );
        }
    }

    private void moveMigratedFilesToStoreDirectory( Iterable<StoreMigrationParticipant> participants,
            File migrationDirectory, File storeDirectory, String versionToMigrateFrom, String versionToMigrateTo )
    {
        try
        {
            for ( StoreMigrationParticipant participant : participants )
            {
                participant.moveMigratedFiles( migrationDirectory, storeDirectory, versionToMigrateFrom,
                        versionToMigrateTo );
            }
        }
        catch ( IOException e )
        {
            throw new UnableToUpgradeException( "Unable to move migrated files into place", e );
        }
    }

    private void migrateToIsolatedDirectory( File storeDir, File migrationDirectory, String versionToMigrateFrom )
    {
        try
        {
            for ( StoreMigrationParticipant participant : participants )
            {
                ProgressReporter progressReporter = progressMonitor.startSection( participant.getName() );
                participant.migrate( storeDir, migrationDirectory, progressReporter, versionToMigrateFrom,
                        upgradableDatabase.currentVersion() );
                progressReporter.completed();
            }
        }
        catch ( IOException | UncheckedIOException e )
        {
            throw new UnableToUpgradeException( "Failure doing migration", e );
        }
    }

    private void cleanMigrationDirectory( File migrationDirectory )
    {
        try
        {
            if ( fileSystem.fileExists( migrationDirectory ) )
            {
                fileSystem.deleteRecursively( migrationDirectory );
            }
            // We use the file system from the page cache here to make sure that the migration directory is clean
            // even if we are using a block device.
            try
            {
                pageCache.getCachedFileSystem().streamFilesRecursive( migrationDirectory )
                        .forEach( FileHandle.HANDLE_DELETE );
            }
            catch ( NoSuchFileException e )
            {
                // This means that we had no files to clean, this is fine.
            }
        }
        catch ( IOException | UncheckedIOException e )
        {
            throw new UnableToUpgradeException( "Failure deleting upgrade directory " + migrationDirectory, e );
        }
        fileSystem.mkdir( migrationDirectory );
    }

    private void deleteSilently( File dir )
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
        protected static final String MESSAGE =
                "This is an enterprise-only store. Please configure '%s' to open.";

        UnexpectedUpgradingStoreFormatException()
        {
            super( String.format( MESSAGE, GraphDatabaseSettings.record_format.name() ) );
        }
    }

    static class DatabaseNotCleanlyShutDownException extends UnableToUpgradeException
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
