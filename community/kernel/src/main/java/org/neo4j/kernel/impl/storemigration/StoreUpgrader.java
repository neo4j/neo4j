/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * A migration process to migrate {@link StoreMigrationParticipant migration participants}, if there's
 * need for it, before the database fully starts. Participants can
 * {@link #addParticipant(StoreMigrationParticipant) register} and will be notified when it's time for migration.
 * The migration will happen to a separate, isolated directory so that an incomplete migration will not affect
 * the original database. Only when a successful migration has taken place the migrated store will replace
 * the original database.
 * <p>
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
 * <p>
 * TODO walk through crash scenarios and how they are handled.
 *
 * @see StoreMigrationParticipant
 */
public class StoreUpgrader
{
    public static final String MIGRATION_DIRECTORY = "upgrade";
    public static final String MIGRATION_LEFT_OVERS_DIRECTORY = "upgrade_backup";

    private static final String MIGRATION_STATUS_FILE = "_status";

    private enum MigrationStatus
    {
        migrating,
        moving,
        completed;
    }

    public interface Monitor
    {
        void migrationNeeded();

        void migrationNotAllowed();

        void migrationCompleted();
    }

    public static abstract class MonitorAdapter implements Monitor
    {
        @Override
        public void migrationNeeded()
        {   // Do nothing
        }

        @Override
        public void migrationNotAllowed()
        {   // Do nothing
        }

        @Override
        public void migrationCompleted()
        {   // Do nothing
        }
    }

    public static final Monitor NO_MONITOR = new MonitorAdapter()
    {
    };

    private final List<StoreMigrationParticipant> participants = new ArrayList<>();
    private final UpgradeConfiguration upgradeConfiguration;
    private final FileSystemAbstraction fileSystem;
    private final Monitor monitor;
    private final Log log;

    public StoreUpgrader( UpgradeConfiguration upgradeConfiguration, FileSystemAbstraction fileSystem,
            Monitor monitor, LogProvider logProvider )
    {
        this.fileSystem = fileSystem;
        this.upgradeConfiguration = upgradeConfiguration;
        this.monitor = monitor;
        this.log = logProvider.getLog( getClass() );
    }

    public void addParticipant( StoreMigrationParticipant participant )
    {
        assert participant != null;
        this.participants.add( participant );
    }

    public void migrateIfNeeded( File storeDirectory, SchemaIndexProvider schemaIndexProvider )
    {
        File migrationDirectory = new File( storeDirectory, MIGRATION_DIRECTORY );

        cleanupLegacyLeftOverDirsIn( storeDirectory );

        List<StoreMigrationParticipant> participantsNeedingMigration =
                getParticipantsEagerToMigrate( storeDirectory );
        if ( participantsNeedingMigration.isEmpty() )
        {   // No migration needed
            deleteSilently( migrationDirectory ); // delete stale dir if present
            return;
        }

        // One or more participants would like to do migration
        monitor.migrationNeeded();
        try
        {
            upgradeConfiguration.checkConfigurationAllowsAutomaticUpgrade();
        }
        catch ( UpgradeNotAllowedException e )
        {
            monitor.migrationNotAllowed();
            throw e;
        }

        try
        {
            File migrationStateFile = new File( migrationDirectory, MIGRATION_STATUS_FILE );

            // We don't need to migrate if we're at the phase where we have migrated successfully
            // and it's just a matter of moving over the files to the storeDir.
            if ( !migrationStatusIs( migrationStateFile, MigrationStatus.moving ) )
            {
                cleanMigrationDirectory( migrationDirectory );
                setMigrationStatus( migrationStateFile, MigrationStatus.migrating );
                migrateToIsolatedDirectory(
                        participantsNeedingMigration, storeDirectory, migrationDirectory, schemaIndexProvider );
                setMigrationStatus( migrationStateFile, MigrationStatus.moving );
            }

            closeParticipants();
            moveMigratedFilesToWorkingDirectory( participantsNeedingMigration, migrationDirectory, storeDirectory );

            setMigrationStatus( migrationStateFile, MigrationStatus.completed );
            cleanup( participantsNeedingMigration, migrationDirectory );
            monitor.migrationCompleted();
        }
        finally
        {   // Safety net
            closeParticipants();
        }
    }

    private void cleanupLegacyLeftOverDirsIn( File storeDir )
    {
        final Pattern leftOverDirsPattern = Pattern.compile( MIGRATION_LEFT_OVERS_DIRECTORY + "(_\\d*)?" );
        File[] leftOverDirs = storeDir.listFiles( new FilenameFilter()
        {
            @Override
            public boolean accept( File file, String name )
            {
                return file.isDirectory() && leftOverDirsPattern.matcher( name ).matches();
            }
        } );
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

    private void closeParticipants()
    {
        for ( StoreMigrationParticipant participant : participants )
        {
            participant.close();
        }
    }

    private boolean migrationStatusIs( File stateFile, MigrationStatus state )
    {
        return state.name().equals( readFromFile( stateFile ) );
    }

    private void setMigrationStatus( File stateFile, MigrationStatus status )
    {
        writeToFile( stateFile, status.name() );
    }

    private void moveMigratedFilesToWorkingDirectory(
            Iterable<StoreMigrationParticipant> participantsNeedingMigration,
            File migrationDirectory,
            File workingDirectory )
    {
        try
        {
            for ( StoreMigrationParticipant participant : participantsNeedingMigration )
            {
                participant.moveMigratedFiles( migrationDirectory, workingDirectory );
            }
        }
        catch ( IOException e )
        {
            throw new UnableToUpgradeException( "Unable to move migrated files into place", e );
        }
    }

    private List<StoreMigrationParticipant> getParticipantsEagerToMigrate( File storeDirectory )
    {
        List<StoreMigrationParticipant> participantsNeedingUpgrade = new ArrayList<>();
        for ( StoreMigrationParticipant participant : participants )
        {
            try
            {
                if ( participant.needsMigration( storeDirectory ) )
                {
                    participantsNeedingUpgrade.add( participant );
                }
            }
            catch ( IOException e )
            {
                throw new UnableToCheckForUpgradeException( participant.toString() + " for " + storeDirectory, e );
            }
        }
        return participantsNeedingUpgrade;
    }

    private void migrateToIsolatedDirectory( List<StoreMigrationParticipant> participantsNeedingMigration,
                                             File storeDir, File migrationDirectory,
                                             SchemaIndexProvider schemaIndexProvider )
    {
        try
        {
            for ( StoreMigrationParticipant participant : participants )
            {
                if ( participantsNeedingMigration.contains( participant ) )
                {   // This participant needs migration, do it
                    participant.migrate( storeDir, migrationDirectory, schemaIndexProvider );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnableToUpgradeException( "Failure doing migration", e );
        }
        catch ( Exception e )
        {
            throw Exceptions.launderedException( e );
        }
    }

    private void cleanMigrationDirectory( File migrationDirectory )
    {
        if ( migrationDirectory.exists() )
        {
            try
            {
                fileSystem.deleteRecursively( migrationDirectory );
            }
            catch ( IOException e )
            {
                throw new UnableToUpgradeException( "Failure deleting upgrade directory " + migrationDirectory, e );
            }
        }
        fileSystem.mkdir( migrationDirectory );
    }

    private void writeToFile( File file, String string )
    {
        if ( fileSystem.fileExists( file ) )
        {
            fileSystem.deleteFile( file );
        }

        try ( Writer writer = fileSystem.openAsWriter( file, "utf-8", false ) )
        {
            writer.write( string );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private String readFromFile( File file )
    {
        try ( BufferedReader reader = new BufferedReader( fileSystem.openAsReader( file, "utf-8" ) ) )
        {
            String readLine = reader.readLine();
            return readLine;
        }
        catch ( FileNotFoundException e )
        {
            return null;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
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

        public UnableToUpgradeException( String message )
        {
            super( message );
        }
    }

    public static class UpgradeMissingStoreFilesException extends UnableToUpgradeException
    {
        private static final String MESSAGE = "Missing required store file '%s'.";

        public UpgradeMissingStoreFilesException( String filenameExpectedToExist )
        {
            super( String.format( MESSAGE, filenameExpectedToExist ) );
        }
    }

    public static class UpgradingStoreVersionNotFoundException extends UnableToUpgradeException
    {
        private static final String MESSAGE =
                "'%s' does not contain a store version, please ensure that the original database was shut down in a " +
                        "clean state.";

        public UpgradingStoreVersionNotFoundException( String filenameWithoutStoreVersion )
        {
            super( String.format( MESSAGE, filenameWithoutStoreVersion ) );
        }
    }

    public static class UnexpectedUpgradingStoreVersionException extends UnableToUpgradeException
    {
        private static final String MESSAGE =
                "'%s' has a store version number that we cannot upgrade from. Expected '%s' but file is version '%s'.";

        public UnexpectedUpgradingStoreVersionException( String filename, String expectedVersion, String actualVersion )
        {
            super( String.format( MESSAGE, filename, expectedVersion, actualVersion ) );
        }
    }

    public static class UnableToCheckForUpgradeException extends UnableToUpgradeException
    {
        private static final String MESSAGE =
                "'%s' failed to check whether or not migration was required";

        public UnableToCheckForUpgradeException( String participant, Throwable cause )
        {
            super( String.format( MESSAGE, participant ), cause );
        }
    }
}
